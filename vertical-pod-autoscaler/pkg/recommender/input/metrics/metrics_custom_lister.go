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
	"fmt"
	"sync"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"

	k8sapiv1 "k8s.io/api/core/v1"
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
	client    prometheusv1.API
	queries   []nsQueryBuilder
	podLister v1lister.PodLister
}

// containerUsages maps containers to their resource usages.
type containerUsages map[prommodel.LabelValue]resource.Quantity

// nsQueryResult holds the parsed result of a custom resource query for a namespace.
type nsQueryResult struct {
	// Maps pods to their containers' resource usages.
	podUsages map[prommodel.LabelValue]containerUsages
	nsQuery   nsQuery
	err       error
}

// TODO(leekathy): Add unit tests.
func newCustomPodMetricsLister(promClient prometheusv1.API, queries []nsQueryBuilder, podLister v1lister.PodLister) PodMetricsLister {
	return &customPodMetricsLister{
		client:    promClient,
		queries:   queries,
		podLister: podLister,
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

	// Index the results by namespace then pod name then container name then resource.
	indexedCustomPodMetrics := make(map[string]map[prommodel.LabelValue]map[prommodel.LabelValue]v1beta1.ContainerMetrics)
	for res := range resChan {
		if res.err != nil {
			// TODO(leekathy): Emit metrics and setup alerting.
			klog.ErrorS(res.err, "Failed to query custom resource metrics", "query", res.nsQuery.query, "namespace", res.nsQuery.namespace, "resource", res.nsQuery.resource, "pods", res.nsQuery.pods)
			continue
		}

		if _, ok := indexedCustomPodMetrics[res.nsQuery.namespace]; !ok {
			indexedCustomPodMetrics[res.nsQuery.namespace] = make(map[prommodel.LabelValue]map[prommodel.LabelValue]v1beta1.ContainerMetrics)
		}

		for pod, containerUsages := range res.podUsages {
			if _, ok := indexedCustomPodMetrics[res.nsQuery.namespace][pod]; !ok {
				indexedCustomPodMetrics[res.nsQuery.namespace][pod] = make(map[prommodel.LabelValue]v1beta1.ContainerMetrics)
			}

			for container, usage := range containerUsages {
				if _, ok := indexedCustomPodMetrics[res.nsQuery.namespace][pod][container]; !ok {
					indexedCustomPodMetrics[res.nsQuery.namespace][pod][container] = v1beta1.ContainerMetrics{
						Name:  string(container),
						Usage: make(k8sapiv1.ResourceList),
					}
				}

				indexedCustomPodMetrics[res.nsQuery.namespace][pod][container].Usage[res.nsQuery.resource] = usage
			}
		}
	}

	// Convert the indexed results back to a PodMetricsList.
	podsCustomMetrics := &v1beta1.PodMetricsList{}
	for namespace, pods := range indexedCustomPodMetrics {
		for pod, containers := range pods {
			podMetrics := v1beta1.PodMetrics{
				TypeMeta:   v1.TypeMeta{},
				ObjectMeta: v1.ObjectMeta{Namespace: namespace, Name: string(pod)},
				Window:     v1.Duration{5 * time.Minute},
				Containers: make([]v1beta1.ContainerMetrics, 0),
			}
			for _, container := range containers {
				podMetrics.Containers = append(podMetrics.Containers, container)
			}

			podsCustomMetrics.Items = append(podsCustomMetrics.Items, podMetrics)
		}
	}

	return podsCustomMetrics, nil
}

// query queries M3 for the specified custom resource metric and returns the result.
func (c *customPodMetricsLister) query(query nsQuery) nsQueryResult {
	res, _, err := c.client.Query(context.Background(), query.query, time.Now())
	if err != nil {
		return nsQueryResult{nsQuery: query, err: err}
	}

	samples, ok := res.(prommodel.Vector)
	if !ok {
		return nsQueryResult{nsQuery: query, err: fmt.Errorf("expected vector response type but got %s", res.Type())}
	}

	nsQueryResult := nsQueryResult{nsQuery: query, podUsages: make(map[prommodel.LabelValue]containerUsages)}
	for _, sample := range samples {
		podName, ok := sample.Metric[query.podNameLabel]
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get pod name from labels", "podNameLabel", query.podNameLabel, "labels", sample.Metric)
			continue
		}

		containerName, ok := sample.Metric[query.containerNameLabel]
		if !ok {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get container name from labels", "containerNameLabel", query.containerNameLabel, "labels", sample.Metric)
			continue
		}

		value := sample.Value.String()
		resourceQuantity, err := resource.ParseQuantity(value)
		if err != nil {
			klog.ErrorS(fmt.Errorf("Not found"), "Failed to get value as resource quantity string", "value", value)
			continue
		}

		if _, ok := nsQueryResult.podUsages[podName]; !ok {
			nsQueryResult.podUsages[podName] = make(containerUsages)
		}
		nsQueryResult.podUsages[podName][containerName] = resourceQuantity
	}

	return nsQueryResult
}
