/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"

	k8sapiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/external_metrics"
	"time"
)

const batchSize = 500

// PodMetricsLister wraps both metrics-client and External Metrics
type PodMetricsLister interface {
	List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error)
}

// podMetricsSource is the metrics-client source of metrics.
type podMetricsSource struct {
	metricsGetter resourceclient.PodMetricsesGetter
	m3Url         *url.URL
}

// nsQuery wraps the func to construct a custom resource query.
// Parameters are list of pod names, namespace, container name label, and pod name label.
type nsQuery struct {
	f                  func([]string, string, string, string) string
	resource           k8sapiv1.ResourceName
	containerNameLabel string
	podNameLabel       string
}

// containerUsages maps containers to their resource usages.
type containerUsages map[string]resource.Quantity

// nsQueryResults holds the results of a custom resource query for a namespace.
type nsQueryResults struct {
	namespace string
	resource  k8sapiv1.ResourceName
	// Maps pods to their containers' resource usages.
	podUsages map[string]containerUsages
	err       error
}

// m3Response is for unmarshaling the response from M3.
type m3Response struct {
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

// NewPodMetricsesSource Returns a Source-wrapper around PodMetricsesGetter.
func NewPodMetricsesSource(source resourceclient.PodMetricsesGetter, m3UrlBase string) PodMetricsLister {
	var m3Url *url.URL
	if m3UrlBase != "" {
		m3Url, _ = url.Parse(m3UrlBase)
		m3Url.Path += "/api/v1/query"
	}

	return podMetricsSource{
		metricsGetter: source,
		m3Url:         m3Url,
	}
}

// getRSSQuery returns a query string for the RSS metric for the specified pods in the namespace.
func getRSSQuery(podNames []string, namespace string, containerNameLabel string, podNameLabel string) string {
	return fmt.Sprintf("max_over_time(container_memory_rss{%s!='', %s=~'%s', namespace='%s'}[5m])", containerNameLabel, podNameLabel, strings.Join(podNames, "|"), namespace)
}

// getJVMHeapCommittedQuery returns a query string for the JVM Heap Committed metric for the specified pods in the namespace.
func getJVMHeapCommittedQuery(podNames []string, namespace string, containerNameLabel string, podNameLabel string) string {
	return fmt.Sprintf("max_over_time(jmx_Memory_HeapMemoryUsage_committed{%s!='', %s=~'%s', kubernetes_namespace='%s'}[5m])", containerNameLabel, podNameLabel, strings.Join(podNames, "|"), namespace)
}

// batchPodsByNs batches pods by namespace to avoid exploding DFA states in the queries.
// It returns a map of namespace to batches of pod names. Batch size is hardcoded to 500.
func (s podMetricsSource) batchPodsByNs(podMetrics *v1beta1.PodMetricsList) map[string][][]string {
	podsByNsBatched := map[string][][]string{}
	for _, pod := range podMetrics.Items {
		_, exists := podsByNsBatched[pod.Namespace]
		if !exists {
			podsByNsBatched[pod.Namespace] = [][]string{{}}
		}

		// Get the last batch for this namespace
		lastBatchIdx := len(podsByNsBatched[pod.Namespace]) - 1
		lastBatch := podsByNsBatched[pod.Namespace][lastBatchIdx]
		// Create a new batch if the last one is full
		if len(lastBatch) >= batchSize {
			podsByNsBatched[pod.Namespace] = append(podsByNsBatched[pod.Namespace], []string{})
		}

		// Get the current batch for this namespace and add the pod
		currentBatchIdx := len(podsByNsBatched[pod.Namespace]) - 1
		currentBatch := podsByNsBatched[pod.Namespace][currentBatchIdx]
		currentBatch = append(currentBatch, pod.Name)

		// Wire the update back into the map
		podsByNsBatched[pod.Namespace][currentBatchIdx] = currentBatch
	}

	return podsByNsBatched
}

// queryCustomResourceMetric queries M3 for a custom resource metric for the specified pods in the namespace.
func (s podMetricsSource) queryCustomResourceMetric(ns string, podNames []string, customQuery nsQuery) nsQueryResults {
	params := url.Values{}
	params.Add("query", customQuery.f(podNames, ns, customQuery.containerNameLabel, customQuery.podNameLabel))
	s.m3Url.RawQuery = params.Encode()

	resp, err := http.Get(s.m3Url.String())
	if err != nil {
		return nsQueryResults{namespace: ns, resource: customQuery.resource, err: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nsQueryResults{namespace: ns, resource: customQuery.resource, err: fmt.Errorf("Failed to get valid response (status: %s)", resp.Status)}
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nsQueryResults{namespace: ns, resource: customQuery.resource, err: err}
	}

	var response m3Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nsQueryResults{namespace: ns, resource: customQuery.resource, err: err}
	}

	nsQueryResults := nsQueryResults{
		namespace: ns,
		resource:  customQuery.resource,
		podUsages: make(map[string]containerUsages),
	}
	for _, result := range response.Data.Result {
		podName, ok := result.Metric[customQuery.podNameLabel]
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value of pod name label", "label", customQuery.podNameLabel, result.Metric, result.Metric)
			continue
		}

		containerName, ok := result.Metric[customQuery.containerNameLabel]
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value of container name label", "label", customQuery.containerNameLabel, result.Metric, result.Metric)
			continue
		}

		if len(result.Value) < 2 {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value of metric", "value", result.Value)
			continue
		}

		value, ok := result.Value[1].(string)
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value of metric", "value", result.Value)
			continue
		}

		resourceQuantity, err := resource.ParseQuantity(value)
		if err != nil {
			klog.ErrorS(err, "Failed to parse resource quantity", "value", value, "resource", customQuery.resource, "namespace", ns, "pod", podName, "container", containerName)
			continue
		}

		if _, ok := nsQueryResults.podUsages[podName]; !ok {
			nsQueryResults.podUsages[podName] = make(containerUsages)
		}
		nsQueryResults.podUsages[podName][containerName] = resourceQuantity
	}

	return nsQueryResults
}

// withCustomResourceMetrics augments and returns pod metrics with custom resource metrics from M3.
// M3 queries are dispatched concurrently by namespace, and parsed after all write responses to the results channel.
func (s podMetricsSource) withCustomResourceMetrics(podMetrics *v1beta1.PodMetricsList, customQueries []nsQuery) (*v1beta1.PodMetricsList, error) {
	// Group pods by namespace and batch them by 500 pods each.
	podsByNsBatched := s.batchPodsByNs(podMetrics)
	// Create a buffered channel to hold all results.
	buffSize := 0
	for _, batches := range podsByNsBatched {
		buffSize += len(batches) * len(customQueries)
	}
	resChan := make(chan nsQueryResults, buffSize)

	// Dispatch custom resource queries concurrently by namespace.
	var wg sync.WaitGroup
	for ns, batches := range podsByNsBatched {
		for _, podNames := range batches {
			for _, customQuery := range customQueries {
				wg.Add(1)

				go func(ns string, podNames []string, customQuery nsQuery) {
					defer wg.Done()
					resChan <- s.queryCustomResourceMetric(ns, podNames, customQuery)
				}(ns, podNames, customQuery)
			}
		}
	}

	wg.Wait()
	close(resChan)

	// Index the results by namespace.
	resultsByNs := make(map[string][]nsQueryResults, len(podsByNsBatched))
	for res := range resChan {
		if res.err != nil {
			klog.ErrorS(res.err, "Failed to query custom resource metrics", "namespace", res.namespace, "resource", res.resource, "pods", res.podUsages)
			continue
		}

		resultsByNs[res.namespace] = append(resultsByNs[res.namespace], res)
	}

	// Augment the PodMetricsList with the custom resource metrics.
	for i, pod := range podMetrics.Items {
		nsQueryResults, ok := resultsByNs[pod.Namespace]
		if !ok {
			continue
		}

		for _, nsQueryResult := range nsQueryResults {
			containerUsages, ok := nsQueryResult.podUsages[pod.Name]
			if !ok {
				continue
			}

			for j, container := range pod.Containers {
				containerUsage, ok := containerUsages[container.Name]
				if !ok {
					continue
				}

				podMetrics.Items[i].Containers[j].Usage[nsQueryResult.resource] = containerUsage
			}
		}
	}

	return podMetrics, nil
}

func (s podMetricsSource) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	podMetricsInterface := s.metricsGetter.PodMetricses(namespace)
	podsMetrics, err := podMetricsInterface.List(ctx, opts)
	if err != nil {
		klog.ErrorS(err, "Failed to query pod usage metrics from the Metrics API")
		return nil, err
	}

	if s.m3Url == nil {
		klog.Info("No M3 URL provided - skipping custom pod usage metrics")
		return podsMetrics, nil
	}

	customQueries := []nsQuery{
		{
			f:                  getRSSQuery,
			resource:           k8sapiv1.ResourceName(model.ResourceRSS),
			containerNameLabel: "container_name",
			podNameLabel:       "pod_name",
		},
		{
			f:                  getJVMHeapCommittedQuery,
			resource:           k8sapiv1.ResourceName(model.ResourceJVMHeapCommitted),
			containerNameLabel: "kubernetes_container_name",
			podNameLabel:       "kubernetes_pod_name",
		},
	}
	return s.withCustomResourceMetrics(podsMetrics, customQueries)
}

// externalMetricsClient is the External Metrics source of metrics.
type externalMetricsClient struct {
	externalClient external_metrics.ExternalMetricsClient
	options        ExternalClientOptions
	clusterState   *model.ClusterState
}

// ExternalClientOptions specifies parameters for using an External Metrics Client.
type ExternalClientOptions struct {
	ResourceMetrics map[k8sapiv1.ResourceName]string
	// Label to use for the container name.
	ContainerNameLabel string
}

// NewExternalClient returns a Source for an External Metrics Client.
func NewExternalClient(c *rest.Config, clusterState *model.ClusterState, options ExternalClientOptions) PodMetricsLister {
	extClient, err := external_metrics.NewForConfig(c)
	if err != nil {
		klog.Fatalf("Failed initializing external metrics client: %v", err)
	}
	return &externalMetricsClient{
		externalClient: extClient,
		options:        options,
		clusterState:   clusterState,
	}
}

func (s *externalMetricsClient) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	result := v1beta1.PodMetricsList{}

	for _, vpa := range s.clusterState.Vpas {
		if vpa.PodCount == 0 {
			continue
		}

		if namespace != "" && vpa.ID.Namespace != namespace {
			continue
		}

		nsClient := s.externalClient.NamespacedMetrics(vpa.ID.Namespace)
		pods := s.clusterState.GetMatchingPods(vpa)

		for _, pod := range pods {
			podNameReq, err := labels.NewRequirement("pod", selection.Equals, []string{pod.PodName})
			if err != nil {
				return nil, err
			}
			selector := vpa.PodSelector.Add(*podNameReq)
			podMets := v1beta1.PodMetrics{
				TypeMeta:   v1.TypeMeta{},
				ObjectMeta: v1.ObjectMeta{Namespace: vpa.ID.Namespace, Name: pod.PodName},
				Window:     v1.Duration{},
				Containers: make([]v1beta1.ContainerMetrics, 0),
			}
			// Query each resource in turn, then assemble back to a single []ContainerMetrics.
			containerMetrics := make(map[string]k8sapiv1.ResourceList)
			for resourceName, metricName := range s.options.ResourceMetrics {
				m, err := nsClient.List(metricName, selector)
				if err != nil {
					return nil, err
				}
				if m == nil || len(m.Items) == 0 {
					klog.V(4).Infof("External Metrics Query for VPA %+v: resource %+v, metric %+v, No items,", vpa.ID, resourceName, metricName)
					continue
				}
				klog.V(4).Infof("External Metrics Query for VPA %+v: resource %+v, metric %+v, %d items, item[0]: %+v", vpa.ID, resourceName, metricName, len(m.Items), m.Items[0])
				podMets.Timestamp = m.Items[0].Timestamp
				if m.Items[0].WindowSeconds != nil {
					podMets.Window = v1.Duration{Duration: time.Duration(*m.Items[0].WindowSeconds) * time.Second}
				}
				for _, val := range m.Items {
					ctrName, hasCtrName := val.MetricLabels[s.options.ContainerNameLabel]
					if !hasCtrName {
						continue
					}
					if containerMetrics[ctrName] == nil {
						containerMetrics[ctrName] = make(k8sapiv1.ResourceList)
					}
					containerMetrics[ctrName][resourceName] = val.Value
				}

			}
			for cname, res := range containerMetrics {
				podMets.Containers = append(podMets.Containers, v1beta1.ContainerMetrics{Name: cname, Usage: res})
			}
			result.Items = append(result.Items, podMets)

		}
	}
	return &result, nil
}
