/*
Copyright 2017 The Kubernetes Authors.

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

package model

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metrics_quality "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/metrics/quality"
	"k8s.io/klog/v2"
)

// ContainerUsageSample is a measure of resource usage of a container over some
// interval.
type ContainerUsageSample struct {
	// Start of the measurement interval.
	MeasureStart time.Time
	// Average CPU usage in cores or memory usage in bytes.
	Usage ResourceAmount
	// If this is an artificial sample created because of an OOM.
	isOOM bool
	// CPU or memory request at the time of measurement.
	Request ResourceAmount
	// Which resource is this sample for.
	Resource ResourceName
}

// ContainerState stores information about a single container instance.
// Each ContainerState has a pointer to the aggregation that is used for
// aggregating its usage samples.
// It holds the recent history of CPU and memory utilization.
//
//	Note: samples are added to intervals based on their start timestamps.
type ContainerState struct {
	// Current request.
	Request Resources
	// Start of the latest CPU usage sample that was aggregated.
	LastCPUSampleStart time.Time
	// Max memory usage observed in the current aggregation interval.
	memoryPeak ResourceAmount
	// Max memory usage estimated from an OOM event in the current aggregation interval.
	oomPeak ResourceAmount
	// End time of the current memory aggregation interval (not inclusive).
	MemoryWindowEnd time.Time
	// Start of the latest memory usage sample that was aggregated.
	lastMemorySampleStart time.Time
	// Start of the latest RSS sample that was aggregated.
	lastRSSSampleStart time.Time
	// Start of the latest JVM Heap sample that was aggregated.
	lastJVMHeapCommittedSampleStart time.Time
	// Aggregation to add usage samples to.
	aggregator ContainerStateAggregator
}

// NewContainerState returns a new ContainerState.
func NewContainerState(request Resources, aggregator ContainerStateAggregator) *ContainerState {
	return &ContainerState{
		Request:                         request,
		LastCPUSampleStart:              time.Time{},
		MemoryWindowEnd:                 time.Time{},
		lastMemorySampleStart:           time.Time{},
		lastRSSSampleStart:              time.Time{},
		lastJVMHeapCommittedSampleStart: time.Time{},
		aggregator:                      aggregator,
	}
}

func (sample *ContainerUsageSample) isValid(expectedResource ResourceName) bool {
	return sample.Usage >= 0 && sample.Resource == expectedResource
}

func (container *ContainerState) addCPUSample(sample *ContainerUsageSample) bool {
	// Order should not matter for the histogram, other than deduplication.
	if !sample.isValid(ResourceCPU) || !sample.MeasureStart.After(container.LastCPUSampleStart) {
		return false // Discard invalid, duplicate or out-of-order samples.
	}
	container.observeQualityMetrics(sample.Usage, false, corev1.ResourceCPU)
	container.aggregator.AddSample(sample)
	container.LastCPUSampleStart = sample.MeasureStart
	return true
}

func (container *ContainerState) observeQualityMetrics(usage ResourceAmount, isOOM bool, resource corev1.ResourceName) {
	if !container.aggregator.NeedsRecommendation() {
		return
	}
	updateMode := container.aggregator.GetUpdateMode()
	var usageValue float64
	switch resource {
	case corev1.ResourceCPU:
		usageValue = CoresFromCPUAmount(usage)
	case corev1.ResourceMemory:
		usageValue = BytesFromMemoryAmount(usage)
	case corev1.ResourceName(ResourceRSS):
		usageValue = BytesFromMemoryAmount(usage)
	case corev1.ResourceName(ResourceJVMHeapCommitted):
		usageValue = BytesFromMemoryAmount(usage)
	}
	if container.aggregator.GetLastRecommendation() == nil {
		metrics_quality.ObserveQualityMetricsRecommendationMissing(usageValue, isOOM, resource, updateMode)
		return
	}
	recommendation := container.aggregator.GetLastRecommendation()[resource]
	if recommendation.IsZero() {
		metrics_quality.ObserveQualityMetricsRecommendationMissing(usageValue, isOOM, resource, updateMode)
		return
	}
	var recommendationValue float64
	switch resource {
	case corev1.ResourceCPU:
		recommendationValue = float64(recommendation.MilliValue()) / 1000.0
	case corev1.ResourceMemory:
		recommendationValue = float64(recommendation.Value())
	case corev1.ResourceName(ResourceRSS):
		recommendationValue = float64(recommendation.Value())
	case corev1.ResourceName(ResourceJVMHeapCommitted):
		recommendationValue = float64(recommendation.Value())
	default:
		klog.Warningf("Unknown resource: %v", resource)
		return
	}
	metrics_quality.ObserveQualityMetrics(usageValue, recommendationValue, isOOM, resource, updateMode)
}

// GetMaxMemoryPeak returns maximum memory usage in the sample, possibly estimated from OOM
func (container *ContainerState) GetMaxMemoryPeak() ResourceAmount {
	return ResourceAmountMax(container.memoryPeak, container.oomPeak)
}

func (container *ContainerState) addMemorySample(sample *ContainerUsageSample) bool {
	ts := sample.MeasureStart
	// We always process OOM samples.
	if !sample.isValid(ResourceMemory) ||
		(!sample.isOOM && ts.Before(container.lastMemorySampleStart)) {
		return false // Discard invalid or outdated samples.
	}
	container.lastMemorySampleStart = ts
	if container.MemoryWindowEnd.IsZero() { // This is the first sample.
		container.MemoryWindowEnd = ts
	}

	// Each container aggregates one peak per aggregation interval. If the timestamp of the
	// current sample is earlier than the end of the current interval (MemoryWindowEnd) and is larger
	// than the current peak, the peak is updated in the aggregation by subtracting the old value
	// and adding the new value.
	addNewPeak := false
	if ts.Before(container.MemoryWindowEnd) {
		oldMaxMem := container.GetMaxMemoryPeak()
		if oldMaxMem != 0 && sample.Usage > oldMaxMem {
			// Remove the old peak.
			oldPeak := ContainerUsageSample{
				MeasureStart: container.MemoryWindowEnd,
				Usage:        oldMaxMem,
				Request:      sample.Request,
				Resource:     ResourceMemory,
			}
			container.aggregator.SubtractSample(&oldPeak)
			addNewPeak = true
		}
	} else {
		// Shift the memory aggregation window to the next interval.
		memoryAggregationInterval := GetAggregationsConfig().MemoryAggregationInterval
		shift := ts.Sub(container.MemoryWindowEnd).Truncate(memoryAggregationInterval) + memoryAggregationInterval
		container.MemoryWindowEnd = container.MemoryWindowEnd.Add(shift)
		container.memoryPeak = 0
		container.oomPeak = 0
		addNewPeak = true
	}
	container.observeQualityMetrics(sample.Usage, sample.isOOM, corev1.ResourceMemory)
	if addNewPeak {
		newPeak := ContainerUsageSample{
			MeasureStart: container.MemoryWindowEnd,
			Usage:        sample.Usage,
			Request:      sample.Request,
			Resource:     ResourceMemory,
			isOOM:        sample.isOOM,
		}
		container.aggregator.AddSample(&newPeak)
		if sample.isOOM {
			container.oomPeak = sample.Usage
		} else {
			container.memoryPeak = sample.Usage
		}
	}
	return true
}

func (container *ContainerState) addRSSSample(sample *ContainerUsageSample) bool {
	ts := sample.MeasureStart
	if !sample.isValid(ResourceRSS) || (!sample.isOOM && ts.Before(container.lastRSSSampleStart)) {
		return false // Discard invalid or outdated samples.
	}
	container.observeQualityMetrics(sample.Usage, sample.isOOM, corev1.ResourceName(ResourceRSS))
	container.aggregator.AddSample(sample)
	container.lastRSSSampleStart = ts
	return true
}

func (container *ContainerState) addJVMHeapCommittedSample(sample *ContainerUsageSample) bool {
	ts := sample.MeasureStart
	if !sample.isValid(ResourceJVMHeapCommitted) || (!sample.isOOM && ts.Before(container.lastJVMHeapCommittedSampleStart)) {
		return false // Discard invalid or outdated samples.
	}
	container.observeQualityMetrics(sample.Usage, sample.isOOM, corev1.ResourceName(ResourceJVMHeapCommitted))
	container.aggregator.AddSample(sample)
	container.lastJVMHeapCommittedSampleStart = ts
	return true
}

// RecordOOM adds info regarding OOM event in the model as an artificial memory sample.
func (container *ContainerState) RecordOOM(timestamp time.Time, resource ResourceName, memory ResourceAmount) error {
	// Discard old OOM
	if resource == ResourceMemory && timestamp.Before(container.MemoryWindowEnd.Add(-1*GetAggregationsConfig().MemoryAggregationInterval)) {
		return fmt.Errorf("OOM event will be discarded - it is too old (%v)", timestamp)
	}

	if resource == ResourceMemory {
		// Get max of the request and the recent usage-based memory peak.
		// Omitting oomPeak here to protect against recommendation running too high on subsequent OOMs.
		memoryUsed := ResourceAmountMax(memory, container.memoryPeak)
		memory = ResourceAmountMax(memoryUsed+MemoryAmountFromBytes(GetAggregationsConfig().OOMMinBumpUp),
			ScaleResource(memoryUsed, GetAggregationsConfig().OOMBumpUpRatio))
	}
	// Otherwise, for RSS and JVMHeapCommitted, memory is the memory limit.
	oomMemorySample := ContainerUsageSample{
		MeasureStart: timestamp,
		Usage:        memory,
		Resource:     resource,
		isOOM:        true,
	}
	if !container.AddSample(&oomMemorySample) {
		return fmt.Errorf("adding OOM sample failed")
	}
	return nil
}

// AddSample adds a usage sample to the given ContainerState. Requires samples
// for a single resource to be passed in chronological order (i.e. in order of
// growing MeasureStart). Invalid samples (out of order or measure out of legal
// range) are discarded. Returns true if the sample was aggregated, false if it
// was discarded.
// Note: usage samples don't hold their end timestamp / duration. They are
// implicitly assumed to be disjoint when aggregating.
func (container *ContainerState) AddSample(sample *ContainerUsageSample) bool {
	switch sample.Resource {
	case ResourceCPU:
		return container.addCPUSample(sample)
	case ResourceMemory:
		return container.addMemorySample(sample)
	case ResourceRSS:
		return container.addRSSSample(sample)
	case ResourceJVMHeapCommitted:
		return container.addJVMHeapCommittedSample(sample)
	default:
		return false
	}
}
