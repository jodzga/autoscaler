/*
Copyright 2018 The Kubernetes Authors.

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
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/util"
)

var (
	testPodID1  = PodID{"namespace-1", "pod-1"}
	testPodID2  = PodID{"namespace-1", "pod-2"}
	testRequest = Resources{
		ResourceCPU:    CPUAmountFromCores(3.14),
		ResourceMemory: MemoryAmountFromBytes(3.14e9),
	}
)

func addTestCPUSample(cluster *ClusterState, container ContainerID, cpuCores float64) error {
	sample := ContainerUsageSampleWithKey{
		Container: container,
		ContainerUsageSample: ContainerUsageSample{
			MeasureStart: testTimestamp,
			Usage:        CPUAmountFromCores(cpuCores),
			Request:      testRequest[ResourceCPU],
			Resource:     ResourceCPU,
		},
	}
	return cluster.AddSample(&sample)
}

func addTestMemorySample(cluster *ClusterState, container ContainerID, memoryBytes float64) error {
	sample := ContainerUsageSampleWithKey{
		Container: container,
		ContainerUsageSample: ContainerUsageSample{
			MeasureStart: testTimestamp,
			Usage:        MemoryAmountFromBytes(memoryBytes),
			Request:      testRequest[ResourceMemory],
			Resource:     ResourceMemory,
		},
	}
	return cluster.AddSample(&sample)
}

// Creates two pods, each having two containers:
//
//	testPodID1: { 'app-A', 'app-B' }
//	testPodID2: { 'app-A', 'app-C' }
//
// Adds a few usage samples to the containers.
// Verifies that AggregateStateByContainerName() properly aggregates
// container CPU and memory peak histograms, grouping the two containers
// with the same name ('app-A') together.
func TestAggregateStateByContainerName(t *testing.T) {
	cluster := NewClusterState(testGcPeriod)
	cluster.AddOrUpdatePod(testPodID1, testLabels, apiv1.PodRunning)
	otherLabels := labels.Set{"label-2": "value-2"}
	cluster.AddOrUpdatePod(testPodID2, otherLabels, apiv1.PodRunning)

	// Create 4 containers: 2 with the same name and 2 with different names.
	containers := []ContainerID{
		{testPodID1, "app-A"},
		{testPodID1, "app-B"},
		{testPodID2, "app-A"},
		{testPodID2, "app-C"},
	}
	for _, c := range containers {
		assert.NoError(t, cluster.AddOrUpdateContainer(c, testRequest))
	}

	// Add CPU usage samples to all containers.
	assert.NoError(t, addTestCPUSample(cluster, containers[0], 1.0)) // app-A
	assert.NoError(t, addTestCPUSample(cluster, containers[1], 5.0)) // app-B
	assert.NoError(t, addTestCPUSample(cluster, containers[2], 3.0)) // app-A
	assert.NoError(t, addTestCPUSample(cluster, containers[3], 5.0)) // app-C
	// Add Memory usage samples to all containers.
	assert.NoError(t, addTestMemorySample(cluster, containers[0], 2e9))  // app-A
	assert.NoError(t, addTestMemorySample(cluster, containers[1], 10e9)) // app-B
	assert.NoError(t, addTestMemorySample(cluster, containers[2], 4e9))  // app-A
	assert.NoError(t, addTestMemorySample(cluster, containers[3], 10e9)) // app-C

	// Build the AggregateContainerStateMap.
	aggregateResources := AggregateStateByContainerName(cluster.aggregateStateMap)
	assert.Contains(t, aggregateResources, "app-A")
	assert.Contains(t, aggregateResources, "app-B")
	assert.Contains(t, aggregateResources, "app-C")

	// Expect samples from all containers to be grouped by the container name.
	assert.Equal(t, 2, aggregateResources["app-A"].TotalSamplesCount)
	assert.Equal(t, 1, aggregateResources["app-B"].TotalSamplesCount)
	assert.Equal(t, 1, aggregateResources["app-C"].TotalSamplesCount)

	config := GetAggregationsConfig()
	// Compute the expected histograms for the "app-A" containers.
	expectedCPUHistogram := util.NewDecayingHistogram(config.CPUHistogramOptions, config.CPUHistogramDecayHalfLife)
	expectedCPUHistogram.Merge(cluster.findOrCreateAggregateContainerState(containers[0]).AggregateCPUUsage)
	expectedCPUHistogram.Merge(cluster.findOrCreateAggregateContainerState(containers[2]).AggregateCPUUsage)
	actualCPUHistogram := aggregateResources["app-A"].AggregateCPUUsage

	expectedMemoryHistogram := util.NewDecayingHistogram(config.MemoryHistogramOptions, config.MemoryHistogramDecayHalfLife)
	expectedMemoryHistogram.AddSample(2e9, 1.0, cluster.GetContainer(containers[0]).MemoryWindowEnd)
	expectedMemoryHistogram.AddSample(4e9, 1.0, cluster.GetContainer(containers[2]).MemoryWindowEnd)
	actualMemoryHistogram := aggregateResources["app-A"].AggregateMemoryPeaks

	assert.True(t, expectedCPUHistogram.Equals(actualCPUHistogram), "Expected:\n%s\nActual:\n%s", expectedCPUHistogram, actualCPUHistogram)
	assert.True(t, expectedMemoryHistogram.Equals(actualMemoryHistogram), "Expected:\n%s\nActual:\n%s", expectedMemoryHistogram, actualMemoryHistogram)
}

func TestAggregateContainerStateSaveToCheckpoint(t *testing.T) {
	location, _ := time.LoadLocation("UTC")
	cs := NewAggregateContainerState()
	t1, t2 := time.Date(2018, time.January, 1, 2, 3, 4, 0, location), time.Date(2018, time.February, 1, 2, 3, 4, 0, location)
	cs.FirstSampleStart = t1
	cs.LastSampleStart = t2
	cs.TotalSamplesCount = 10

	cs.AggregateCPUUsage.AddSample(1, 33, t2)
	cs.AggregateMemoryPeaks.AddSample(1, 55, t1)
	cs.AggregateMemoryPeaks.AddSample(10000000, 55, t1)
	checkpoint, err := cs.SaveToCheckpoint()

	assert.NoError(t, err)

	assert.True(t, time.Since(checkpoint.LastUpdateTime.Time) < 10*time.Second)
	assert.Equal(t, t1, checkpoint.FirstSampleStart.Time)
	assert.Equal(t, t2, checkpoint.LastSampleStart.Time)
	assert.Equal(t, 10, checkpoint.TotalSamplesCount)

	assert.Equal(t, SupportedCheckpointVersion, checkpoint.Version)

	// Basic check that serialization of histograms happened.
	// Full tests are part of the Histogram.
	assert.Len(t, checkpoint.CPUHistogram.BucketWeights, 1)
	assert.Len(t, checkpoint.MemoryHistogram.BucketWeights, 2)
}

func TestAggregateContainerStateLoadFromCheckpointFailsForVersionMismatch(t *testing.T) {
	checkpoint := vpa_types.VerticalPodAutoscalerCheckpointStatus{
		Version: "foo",
	}
	cs := NewAggregateContainerState()
	err := cs.LoadFromCheckpoint(&checkpoint)
	assert.Error(t, err)
}

func TestAggregateContainerStateLoadFromCheckpoint(t *testing.T) {
	location, _ := time.LoadLocation("UTC")
	t1, t2 := time.Date(2018, time.January, 1, 2, 3, 4, 0, location), time.Date(2018, time.February, 1, 2, 3, 4, 0, location)

	checkpoint := vpa_types.VerticalPodAutoscalerCheckpointStatus{
		Version:           SupportedCheckpointVersion,
		LastUpdateTime:    metav1.NewTime(time.Now()),
		FirstSampleStart:  metav1.NewTime(t1),
		LastSampleStart:   metav1.NewTime(t2),
		TotalSamplesCount: 20,
		MemoryHistogram: vpa_types.HistogramCheckpoint{
			BucketWeights: map[int]uint32{
				0: 10,
			},
			TotalWeight: 33.0,
		},
		CPUHistogram: vpa_types.HistogramCheckpoint{
			BucketWeights: map[int]uint32{
				0: 10,
			},
			TotalWeight: 44.0,
		},
	}

	cs := NewAggregateContainerState()
	err := cs.LoadFromCheckpoint(&checkpoint)
	assert.NoError(t, err)

	assert.Equal(t, t1, cs.FirstSampleStart)
	assert.Equal(t, t2, cs.LastSampleStart)
	assert.Equal(t, 20, cs.TotalSamplesCount)
	assert.False(t, cs.AggregateCPUUsage.IsEmpty())
	assert.False(t, cs.AggregateMemoryPeaks.IsEmpty())
}

func TestAggregateContainerStateIsExpired(t *testing.T) {
	cs := NewAggregateContainerState()
	cs.LastSampleStart = testTimestamp
	cs.TotalSamplesCount = 1
	assert.False(t, cs.isExpired(testTimestamp.Add(7*24*time.Hour)))
	assert.True(t, cs.isExpired(testTimestamp.Add(8*24*time.Hour)))

	csEmpty := NewAggregateContainerState()
	csEmpty.TotalSamplesCount = 0
	csEmpty.CreationTime = testTimestamp
	assert.False(t, csEmpty.isExpired(testTimestamp.Add(7*24*time.Hour)))
	assert.True(t, csEmpty.isExpired(testTimestamp.Add(8*24*time.Hour)))
}

func TestUpdateFromPolicyScalingMode(t *testing.T) {
	scalingModeAuto := vpa_types.ContainerScalingModeAuto
	scalingModeOff := vpa_types.ContainerScalingModeOff
	testCases := []struct {
		name     string
		policy   *vpa_types.ContainerResourcePolicy
		expected *vpa_types.ContainerScalingMode
	}{
		{
			name: "Explicit auto scaling mode",
			policy: &vpa_types.ContainerResourcePolicy{
				Mode: &scalingModeAuto,
			},
			expected: &scalingModeAuto,
		}, {
			name: "Off scaling mode",
			policy: &vpa_types.ContainerResourcePolicy{
				Mode: &scalingModeOff,
			},
			expected: &scalingModeOff,
		}, {
			name:     "No mode specified - default to Auto",
			policy:   &vpa_types.ContainerResourcePolicy{},
			expected: &scalingModeAuto,
		}, {
			name:     "Nil policy - default to Auto",
			policy:   nil,
			expected: &scalingModeAuto,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cs := NewAggregateContainerState()
			cs.UpdateFromPolicy(tc.policy)
			assert.Equal(t, tc.expected, cs.GetScalingMode())
		})
	}
}

func TestUpdateFromPolicyControlledResources(t *testing.T) {
	testCases := []struct {
		name     string
		policy   *vpa_types.ContainerResourcePolicy
		expected []ResourceName
	}{
		{
			name: "Explicit ControlledResources",
			policy: &vpa_types.ContainerResourcePolicy{
				ControlledResources: &[]apiv1.ResourceName{apiv1.ResourceCPU, apiv1.ResourceMemory},
			},
			expected: []ResourceName{ResourceCPU, ResourceMemory},
		}, {
			name: "Empty ControlledResources",
			policy: &vpa_types.ContainerResourcePolicy{
				ControlledResources: &[]apiv1.ResourceName{},
			},
			expected: []ResourceName{},
		}, {
			name: "ControlledResources with one resource",
			policy: &vpa_types.ContainerResourcePolicy{
				ControlledResources: &[]apiv1.ResourceName{apiv1.ResourceMemory},
			},
			expected: []ResourceName{ResourceMemory},
		}, {
			name:     "No ControlledResources specified - used default",
			policy:   &vpa_types.ContainerResourcePolicy{},
			expected: []ResourceName{ResourceCPU, ResourceMemory, ResourceRSS, ResourceJVMHeapCommitted},
		}, {
			name:     "Nil policy - use default",
			policy:   nil,
			expected: []ResourceName{ResourceCPU, ResourceMemory, ResourceRSS, ResourceJVMHeapCommitted},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cs := NewAggregateContainerState()
			cs.UpdateFromPolicy(tc.policy)
			assert.Equal(t, tc.expected, cs.GetControlledResources())
		})
	}
}

// Flags chosen for DB memory recommendation algorithm
var (
	DBMemoryAggregationInterval      = flag.Duration("memory-aggregation-interval", DefaultMemoryAggregationInterval, `The length of a single interval, for which the peak memory usage is computed. Memory usage peaks are aggregated in multiples of this interval. In other words there is one memory usage sample per interval (the maximum usage over that interval)`)
	DBMemoryAggregationIntervalCount = flag.Int64("memory-aggregation-interval-count", DefaultMemoryAggregationIntervalCount, `The number of consecutive memory-aggregation-intervals which make up the MemoryAggregationWindowLength which in turn is the period for memory usage aggregation by VPA. In other words, MemoryAggregationWindowLength = memory-aggregation-interval * memory-aggregation-interval-count.`)
	DBMemoryHistogramDecayHalfLife   = flag.Duration("memory-histogram-decay-half-life", DefaultMemoryHistogramDecayHalfLife, `The amount of time it takes a historical memory usage sample to lose half of its weight. In other words, a fresh usage sample is twice as 'important' as one with age equal to the half life period.`)
	// TODO: add a flag for memory histogram target percentile or other custom flags
	DBCpuHistogramDecayHalfLife = flag.Duration("cpu-histogram-decay-half-life", DefaultCPUHistogramDecayHalfLife, `The amount of time it takes a historical CPU usage sample to lose half of its weight.`)
	DBOomBumpUpRatio            = flag.Float64("oom-bump-up-ratio", DefaultOOMBumpUpRatio, `The memory bump up ratio when OOM occurred, default is 1.2.`)
	DBOomMinBumpUp              = flag.Float64("oom-min-bump-up-bytes", DefaultOOMMinBumpUp, `The minimal increase of memory when OOM occurred in bytes, default is 100 * 1024 * 1024`)
)

func newDBAggregateContainerState() *AggregateContainerState {
	state := NewAggregateContainerState()
	state.IsUnderVPA = true
	state.ControlledResources = &DefaultControlledResources
	scalingModeAuto := vpa_types.ContainerScalingModeAuto
	state.ScalingMode = &scalingModeAuto
	updteModeInitial := vpa_types.UpdateModeInitial
	state.UpdateMode = &updteModeInitial
	return state
}

func TestDBPeakRememberedForAMonth(t *testing.T) {
	// Initialization
	InitializeAggregationsConfig(NewAggregationsConfig(*DBMemoryAggregationInterval, *DBMemoryAggregationIntervalCount, *DBMemoryHistogramDecayHalfLife, time.Hour*24*1, *DBOomBumpUpRatio, *DBOomMinBumpUp))
	cs := newDBAggregateContainerState()
	timestamp := time.Now()

	// Add peak at time 0
	memoryPeak := float64(2 * 1024 * 1024 * 1024) // 2 Gi peak
	memorySample := ContainerUsageSample{
		MeasureStart: timestamp,
		Usage:        MemoryAmountFromBytes(memoryPeak),
		Request:      testRequest[ResourceMemory],
		Resource:     ResourceMemory,
	}
	cs.AddSample(&memorySample)

	// Add "low" memory usage for a month and verify that peak is remembered
	for i := 0; i < 60*24*30; i++ {
		lowMemoryUsage := float64((i%10 + 1) * 100 * 1024 * 1024)
		memorySample = ContainerUsageSample{
			MeasureStart: timestamp,
			Usage:        MemoryAmountFromBytes(lowMemoryUsage),
			Request:      testRequest[ResourceMemory],
			Resource:     ResourceMemory,
		}
		cs.AddSample(&memorySample)
		peakFromHistogram := MemoryAmountFromBytes(cs.AggregateMemoryPeaks.Percentile(1.0))
		// Peak from histogram can be larger than observed peak due to histogram bucketing
		assert.GreaterOrEqual(t, peakFromHistogram, MemoryAmountFromBytes(memoryPeak))
		timestamp = timestamp.Add(time.Minute)
	}

	// Add "low" memory usage for one more day
	for i := 0; i < 60*24; i++ {
		lowMemoryUsage := float64((i%10 + 1) * 100 * 1024 * 1024)
		memorySample = ContainerUsageSample{
			MeasureStart: timestamp,
			Usage:        MemoryAmountFromBytes(lowMemoryUsage),
			Request:      testRequest[ResourceMemory],
			Resource:     ResourceMemory,
		}
		cs.AddSample(&memorySample)
		timestamp = timestamp.Add(time.Minute)
	}

	// Verify that peak is forgotten
	peakFromHistogram := MemoryAmountFromBytes(cs.AggregateMemoryPeaks.Percentile(1.0))
	assert.Greater(t, MemoryAmountFromBytes(memoryPeak), peakFromHistogram)

}
