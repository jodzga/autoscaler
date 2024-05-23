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
	"sync"
	"time"

	k8sapiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	v1lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/external_metrics"
)

// PodMetricsLister wraps both metrics-client and External Metrics
type PodMetricsLister interface {
	List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error)
}

// podMetricsSource is the metrics-client source of metrics.
type podMetricsSource struct {
	metricsGetter       resourceclient.PodMetricsesGetter
	customMetricsLister PodMetricsLister
}

// customQueries holds the custom resource query builders.
var customQueries = []nsQueryBuilder{
	getRSSQuery("container_name", "pod_name"),
	getJVMHeapCommittedQuery("kubernetes_container_name", "kubernetes_pod_name"),
}

// NewPodMetricsesSource Returns a Source-wrapper around PodMetricsesGetter.
// Added custom metrics lister for M3 queries.
func NewPodMetricsesSource(source resourceclient.PodMetricsesGetter, podLister v1lister.PodLister, m3Url string) PodMetricsLister {
	if m3Url == "" {
		klog.Info("No M3 URL provided - skipping pod custom resource usage metrics")
		return podMetricsSource{
			metricsGetter:       source,
			customMetricsLister: nil,
		}
	}

	klog.Infof("Using M3 URL %s for pod custom resource usage metrics", m3Url)
	return podMetricsSource{
		metricsGetter:       source,
		customMetricsLister: newCustomPodMetricsLister(m3Url, customQueries, podLister),
	}
}

func (s podMetricsSource) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	podMetricsInterface := s.metricsGetter.PodMetricses(namespace)

	// Dispatch queries to Metrics API (for CPU/memory usage) and queries to M3 (for custom resource usage) concurrently.
	var wg sync.WaitGroup
	resChan := make(chan *v1beta1.PodMetricsList, 1)
	customResChan := make(chan *v1beta1.PodMetricsList, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()

		podsMetrics, err := podMetricsInterface.List(ctx, opts)
		if err != nil {
			// TODO(leekathy): Emit metrics and setup alerting.
			klog.ErrorS(err, "Failed to query pod usage metrics from the Metrics API")
			resChan <- nil
			return
		}

		resChan <- podsMetrics
	}()
	if s.customMetricsLister != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			podsCustomMetrics, err := s.customMetricsLister.List(ctx, namespace, opts)
			if err != nil {
				// TODO(leekathy): Emit metrics and setup alerting.
				klog.ErrorS(err, "Failed to query pod custom usage metrics from M3")
				resChan <- nil
				return
			}

			customResChan <- podsCustomMetrics
		}()
	}

	wg.Wait()
	close(resChan)
	close(customResChan)

	podMetrics := <-resChan
	customPodMetrics := <-customResChan
	if s.customMetricsLister == nil || customPodMetrics == nil {
		return podMetrics, nil
	}

	// Index the custom query results by namespace then pod name then container name then resource.
	indexedCustomPodMetrics := make(map[string]map[string]map[string]v1beta1.ContainerMetrics)
	for _, customPodMetric := range customPodMetrics.Items {
		if _, ok := indexedCustomPodMetrics[customPodMetric.Namespace]; !ok {
			indexedCustomPodMetrics[customPodMetric.Namespace] = make(map[string]map[string]v1beta1.ContainerMetrics)
		}
		if _, ok := indexedCustomPodMetrics[customPodMetric.Namespace][customPodMetric.Name]; !ok {
			indexedCustomPodMetrics[customPodMetric.Namespace][customPodMetric.Name] = make(map[string]v1beta1.ContainerMetrics)
		}

		for _, containerMetrics := range customPodMetric.Containers {
			if _, ok := indexedCustomPodMetrics[customPodMetric.Namespace][customPodMetric.Name]; !ok {
				indexedCustomPodMetrics[customPodMetric.Namespace][customPodMetric.Name] = make(map[string]v1beta1.ContainerMetrics)
			}

			indexedCustomPodMetrics[customPodMetric.Namespace][customPodMetric.Name][containerMetrics.Name] = containerMetrics
		}
	}

	// Augment the query results from the Metrics API with the custom query results from M3.
	for i, podMetric := range podMetrics.Items {
		for j, containerMetrics := range podMetric.Containers {
			customNamespaceMetrics, ok := indexedCustomPodMetrics[podMetric.Namespace]
			if !ok {
				continue
			}
			customPodMetrics, ok := customNamespaceMetrics[podMetric.Name]
			if !ok {
				continue
			}
			customContainerMetrics, ok := customPodMetrics[containerMetrics.Name]
			if !ok {
				continue
			}

			for resource, quantity := range customContainerMetrics.Usage {
				podMetrics.Items[i].Containers[j].Usage[resource] = quantity
			}
		}
	}

	klog.InfoS("ALL POD METRICS", "podMetrics", podMetrics)
	return podMetrics, nil
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
