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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/external_metrics"
	"time"
)

const BatchSize = 500

// PodMetricsLister wraps both metrics-client and External Metrics
type PodMetricsLister interface {
	List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error)
}

// podMetricsSource is the metrics-client source of metrics.
type podMetricsSource struct {
	metricsGetter resourceclient.PodMetricsesGetter
	m3Url         string
}

// nsQueryFunc is an alias for a func to construct a custom resource query.
// Parameters are list of pod names and namespace.
type nsQueryFunc func([]string, string) string

// containerQueryResults is a struct to hold the results of a custom resource query and metadata.
type containerQueryResults map[k8sapiv1.ResourceName]resource.Quantity

type podQueryResults struct {
	pod types.NamespacedName
	// Maps container name to its resource usages.
	containerQueryResults map[string]containerQueryResults
}

type nsQueryResults struct {
	namespace string
	// Maps pod name to its containers' resource usages.
	podQueryResults map[types.NamespacedName]podQueryResults
	err             error
}

type m3Response struct {
	status string `json:"status"`
	data   struct {
		resultType string `json:"resultType"`
		result     []struct {
			metric map[string]string `json:"metric"`
			value  []string          `json:"value"`
		} `json:"results"`
	} `json:"data"`
}

// NewPodMetricsesSource Returns a Source-wrapper around PodMetricsesGetter.
func NewPodMetricsesSource(source resourceclient.PodMetricsesGetter, m3Url string) PodMetricsLister {
	return podMetricsSource{
		metricsGetter: source,
		m3Url:         m3Url,
	}
}

// getRSSQuery returns a query string for the RSS metric for the specified pods in the namespace.
func getRSSQuery(podNames []string, namespace string) string {
	return fmt.Sprintf("max_over_time(container_memory_rss{container_name!='', pod_name=~'%s', namespace='%s'}[5m])", strings.Join(podNames, "|"), namespace)
}

// getJVMHeapCommittedQuery returns a query string for the JVM Heap Committed metric for the specified pods in the namespace.
func getJVMHeapCommittedQuery(podNames []string, namespace string) string {
	return fmt.Sprintf("max_over_time(jmx_Memory_HeapMemoryUsage_committed{kubernetes_container_name!='', kubernetes_pod_name=~'%s', kubernetes_namespace='%s'}[5m])", strings.Join(podNames, "|"), namespace)
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
		if len(lastBatch) >= BatchSize {
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

func (s podMetricsSource) queryM3CustomMetric(ns string, podNames []string, query string, resourceName k8sapiv1.ResourceName, baseM3Url *url.URL) nsQueryResults {
	params := url.Values{}
	params.Add("query", query)
	baseM3Url.RawQuery = params.Encode()

	resp, err := http.Get(baseM3Url.String())
	if err != nil {
		return nsQueryResults{namespace: ns, err: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get valid response (status: %s)", resp.Status)}
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nsQueryResults{namespace: ns, err: err}
	}

	var m3Response m3Response
	if err := json.Unmarshal(body, &m3Response); err != nil {
		return nsQueryResults{namespace: ns, err: err}
	}
	klog.InfoS("Response from M3", "namespace", ns, "resource", resourceName, "query", query, "response", m3Response)
	// data, ok := (responseBody["data"]).(map[string]interface{})
	// if !ok {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to parse .data from response")}
	// }
	// klog.InfoS("Data from M3", "namespace", ns, "resource", resourceName, "query", query, "data", data)
	// result, ok := (data["result"]).([]interface{})
	// if !ok {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get .data.result from response")}
	// }
	// klog.InfoS("Result from M3", "namespace", ns, "resource", resourceName, "query", query, "result", result)
	// if len(result) == 0 {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get any query results in .data.result")}
	// }
	// if len(result) > 1 {
	// 	klog.InfoS("More than one query result in .data.result", "namespace", ns, "resource", resourceName, "query", query, "result", result)
	// 	// Proceed for now and just use the first result. Will need to fine-tune the query if so.
	// }
	// firstResult, ok := (result[0]).(map[string]interface{})
	// if !ok {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get first element from .data.result from response")}
	// }
	// value, ok := (firstResult["value"]).([]interface{})
	// if !ok {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get .data.result[0].value from response")}
	// }
	// if len(value) < 2 {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get .data.result[0].value[1] from response")}
	// }
	// resourceValue, ok := (value[1]).(string)
	// if !ok {
	// 	return nsQueryResults{namespace: ns, err: fmt.Errorf("Failed to get .data.result[0].value[1] from response")}
	// }
	// resourceQuantity, err := resource.ParseQuantity(resourceValue)
	// if err != nil {
	// 	return nsQueryResults{namespace: ns, err: err}
	// }
	// klog.InfoS("Resource value from M3", "namespace", ns, "resource", resourceName, "query", query, "value", resourceQuantity)
	return nsQueryResults{namespace: ns, err: nil}
}

// withM3CustomMetrics augments and returns pod metrics with custom resource metrics from M3.
// M3 queries are dispatched concurrently by namespace, and parsed after all write responses to the results channel.
// For efficient queries, follows a similar custom strategy to Prometheus Adapter for GetPodMetrics (used to get podMetrics):
// https://github.com/jodzga/prometheus-adapter/blob/db-release-0.10/pkg/resourceprovider/provider.go#L182C1-L211C2.
func (s podMetricsSource) withM3CustomMetrics(podMetrics *v1beta1.PodMetricsList, customResourceQueryFuncs map[k8sapiv1.ResourceName]nsQueryFunc) (*v1beta1.PodMetricsList, error) {
	m3Url, err := url.Parse(s.m3Url)
	if err != nil {
		klog.ErrorS(err, "Failed to parse M3 URL")
		return nil, err
	}
	m3Url.Path += "/api/v1/query"

	// Group pods by namespace and batch them by 500 pods each.
	podsByNsBatched := s.batchPodsByNs(podMetrics)
	// Create a buffered channel to hold all results.
	buffSize := 0
	for _, batches := range podsByNsBatched {
		buffSize += len(batches) * len(customResourceQueryFuncs)
	}
	resChan := make(chan nsQueryResults, buffSize)

	var wg sync.WaitGroup
	for ns, batches := range podsByNsBatched {
		for _, podNames := range batches {
			queries := make(map[string]k8sapiv1.ResourceName)
			for resourceName, customResourceQueryFunc := range customResourceQueryFuncs {
				queries[customResourceQueryFunc(podNames, ns)] = resourceName
			}

			for query, resourceName := range queries {
				wg.Add(1)

				go func(ns string, podNames []string, query string, resourceName k8sapiv1.ResourceName) {
					defer wg.Done()
					resChan <- s.queryM3CustomMetric(ns, podNames, query, resourceName, m3Url)
				}(ns, podNames, query, resourceName)
			}
		}
	}

	wg.Wait()
	close(resChan)

	// Index the results by namespace.
	resultsByNs := make(map[string][]nsQueryResults, len(podsByNsBatched))
	for res := range resChan {
		if res.err != nil {
			klog.ErrorS(res.err, "Failed to query custom resource metrics", "namespace", res.namespace)
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
			podQueryResults, ok := nsQueryResult.podQueryResults[types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}]
			if !ok {
				continue
			}

			for j, container := range pod.Containers {
				containerQueryResults, ok := podQueryResults.containerQueryResults[container.Name]
				if !ok {
					continue
				}

				for resourceName := range customResourceQueryFuncs {
					containerQueryResult, ok := containerQueryResults[resourceName]
					if !ok {
						continue
					}

					podMetrics.Items[i].Containers[j].Usage[resourceName] = containerQueryResult
				}
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

	if s.m3Url == "" {
		klog.Info("No M3 URL provided - skipping custom pod usage metrics")
		return podsMetrics, nil
	}

	customResourceQueryFuncs := map[k8sapiv1.ResourceName]nsQueryFunc{
		k8sapiv1.ResourceName(model.ResourceRSS):              getRSSQuery,
		k8sapiv1.ResourceName(model.ResourceJVMHeapCommitted): getJVMHeapCommittedQuery,
	}
	return s.withM3CustomMetrics(podsMetrics, customResourceQueryFuncs)
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
