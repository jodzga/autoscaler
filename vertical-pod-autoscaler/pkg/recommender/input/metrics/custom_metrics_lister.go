/*
Helper for querying custom per-container usage metrics from M3.
See metrics_source.go for usage.
*/
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	v1lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

// customPodMetricsLister implements the PodMetricsLister interface in metrics_source.go,
// rather than the resourceclient.PodMetricsesGetter interface, as only LIST is needed.
type customPodMetricsLister struct {
	client       *http.Client
	baseQueryUrl string
	queries      []nsQueryBuilder
	podLister    v1lister.PodLister
}

// nsQueryResponse is for unmarshaling the response from M3.
type nsQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			// First value is the unix timestamp, second value is the metric value.
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// containerUsages maps containers to their resource usages.
type containerUsages map[string]resource.Quantity

// nsQueryResult holds the parsed result (from nsQueryResponse) of a custom resource query for a namespace.
type nsQueryResult struct {
	// Maps pods to their containers' resource usages.
	podUsages map[string]containerUsages
	nsQuery   nsQuery
	err       error
}

func newCustomPodMetricsLister(baseQueryUrl string, queries []nsQueryBuilder, podLister v1lister.PodLister) PodMetricsLister {
	return &customPodMetricsLister{
		client:       &http.Client{},
		baseQueryUrl: baseQueryUrl,
		queries:      queries,
		podLister:    podLister,
	}
}

// List returns pod custom resource metrics from M3.
// M3 queries are dispatched concurrently by namespace, and parsed after all write responses to the results channel.
func (c *customPodMetricsLister) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	// Index pods by namespace.
	podsByNamespace := map[string][]string{}
	for _, pod := range pods {
		podsByNamespace[pod.Namespace] = append(podsByNamespace[pod.Namespace], pod.Name)
	}

	// Build M3 queries for each custom resource query (per-namespace, batched by 500 pods).
	allQueries := []nsQuery{}
	for _, query := range c.queries {
		for namespace := range podsByNamespace {
			batchQueries := query.buildBatch(podsByNamespace[namespace], namespace)
			allQueries = append(allQueries, batchQueries...)
		}
	}

	// Dispatch custom resource queries concurrently by namespace.
	var wg sync.WaitGroup
	resChan := make(chan nsQueryResult, len(allQueries))
	for _, query := range allQueries {
		wg.Add(1)

		go func(query nsQuery) {
			defer wg.Done()
			resChan <- c.query(query)
		}(query)
	}

	wg.Wait()
	close(resChan)

	// Index the results by namespace.
	resultsByNs := make(map[string][]nsQueryResult, len(allQueries))
	for res := range resChan {
		if res.err != nil {
			// TODO(leekathy): Emit metrics and setup alerting.
			klog.ErrorS(res.err, "Failed to query custom resource metrics", "query", res.nsQuery.query, "namespace", res.nsQuery.namespace, "resource", res.nsQuery.resource, "pods", res.nsQuery.pods)
			continue
		}

		resultsByNs[res.nsQuery.namespace] = append(resultsByNs[res.nsQuery.namespace], res)
	}

	// Create a PodMetricsList with the custom resource metrics.
	podMetrics := &v1beta1.PodMetricsList{}
	for i, pod := range pods {
		results, ok := resultsByNs[pod.Namespace]
		if !ok {
			continue
		}

		for _, result := range results {
			containerUsages, ok := result.podUsages[pod.Name]
			if !ok {
				continue
			}

			for j, container := range pod.Spec.Containers {
				containerUsage, ok := containerUsages[container.Name]
				if !ok {
					continue
				}

				podMetrics.Items[i].Containers[j].Usage[result.nsQuery.resource] = containerUsage
			}
		}
	}

	return podMetrics, nil
}

// query queries M3 for the specified custom resource metric and returns the result.
func (c *customPodMetricsLister) query(query nsQuery) nsQueryResult {
	params := url.Values{}
	params.Add("query", query.query)
	baseQueryUrl, err := url.Parse(c.baseQueryUrl)
	if err != nil {
		return nsQueryResult{nsQuery: query, err: err}
	}
	baseQueryUrl.RawQuery = params.Encode()

	resp, err := c.client.Get(baseQueryUrl.String())
	if err != nil {
		return nsQueryResult{nsQuery: query, err: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nsQueryResult{nsQuery: query, err: fmt.Errorf("Failed to get valid response (status: %s)", resp.Status)}
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nsQueryResult{nsQuery: query, err: err}
	}

	var response nsQueryResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nsQueryResult{nsQuery: query, err: err}
	}

	nsQueryResult := nsQueryResult{nsQuery: query, podUsages: make(map[string]containerUsages)}
	for _, result := range response.Data.Result {
		podName, ok := result.Metric[query.podNameLabel]
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value of pod name label", "targetLabel", query.podNameLabel, "allLabels", result.Metric)
			continue
		}

		containerName, ok := result.Metric[query.containerNameLabel]
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value of container name label", "targetLabel", query.containerNameLabel, "allLabels", result.Metric)
			continue
		}

		if len(result.Value) < 2 {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get result in expected [timestamp, value] format", "result", result.Value)
			continue
		}

		value, ok := result.Value[1].(string)
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value as resource quantity string", "result", result.Value[1])
			continue
		}

		resourceQuantity, err := resource.ParseQuantity(value)
		if err != nil {
			klog.ErrorS(err, "Failed to parse resource quantity", "value", value, "resource", query.resource, "namespace", query.namespace, "pod", podName, "container", containerName)
			continue
		}

		if _, ok := nsQueryResult.podUsages[podName]; !ok {
			nsQueryResult.podUsages[podName] = make(containerUsages)
		}
		nsQueryResult.podUsages[podName][containerName] = resourceQuantity
	}

	return nsQueryResult
}
