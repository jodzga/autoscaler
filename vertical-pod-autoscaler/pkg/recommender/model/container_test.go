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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/util"
)

var (
	timeLayout       = "2006-01-02 15:04:05"
	testTimestamp, _ = time.Parse(timeLayout, "2017-04-18 17:35:05")

	TestRequest = Resources{
		ResourceCPU:              CPUAmountFromCores(2.3),
		ResourceMemory:           MemoryAmountFromBytes(5e8),
		ResourceRSS:              MemoryAmountFromBytes(4e8),
		ResourceJVMHeapCommitted: MemoryAmountFromBytes(3e8),
	}
)

const (
	kb = 1024
	mb = 1024 * kb
)

func newUsageSample(timestamp time.Time, usage int64, resource ResourceName) *ContainerUsageSample {
	return &ContainerUsageSample{
		MeasureStart: timestamp,
		Usage:        ResourceAmount(usage),
		Request:      TestRequest[resource],
		Resource:     resource,
	}
}

type ContainerTest struct {
	mockCPUHistogram        *util.MockHistogram
	mockMemoryHistogram     *util.MockHistogram
	mockRSSHistogram        *util.MockHistogram
	mockJVMHeapHistogram    *util.MockHistogram
	aggregateContainerState *AggregateContainerState
	container               *ContainerState
}

func newContainerTest() ContainerTest {
	mockCPUHistogram := new(util.MockHistogram)
	mockMemoryHistogram := new(util.MockHistogram)
	mockRSSHistogram := new(util.MockHistogram)
	mockJVMHeapHistogram := new(util.MockHistogram)
	aggregateContainerState := &AggregateContainerState{
		AggregateCPUUsage:              mockCPUHistogram,
		AggregateMemoryPeaks:           mockMemoryHistogram,
		AggregateRSSPeaks:              mockRSSHistogram,
		AggregateJVMHeapCommittedPeaks: mockJVMHeapHistogram,
	}
	container := &ContainerState{
		Request:    TestRequest,
		aggregator: aggregateContainerState,
	}
	return ContainerTest{
		mockCPUHistogram:        mockCPUHistogram,
		mockMemoryHistogram:     mockMemoryHistogram,
		mockRSSHistogram:        mockRSSHistogram,
		mockJVMHeapHistogram:    mockJVMHeapHistogram,
		aggregateContainerState: aggregateContainerState,
		container:               container,
	}
}

// Add 6 usage samples (3 valid, 3 invalid) to a container. Verifies that for
// valid samples the CPU measures are aggregated in the CPU histogram and
// the memory measures are aggregated in the memory peaks sliding window.
// Verifies that invalid samples (out-of-order or negative usage) are ignored.
func TestAggregateContainerUsageSamples(t *testing.T) {
	test := newContainerTest()
	c := test.container
	memoryAggregationInterval := GetAggregationsConfig().MemoryAggregationInterval
	// Verify that CPU measures are added to the CPU histogram.
	// The weight should be equal to the current request.
	timeStep := memoryAggregationInterval / 2
	test.mockCPUHistogram.On("AddSample", 3.14, 2.3, testTimestamp)
	test.mockCPUHistogram.On("AddSample", 6.28, 2.3, testTimestamp.Add(timeStep))
	test.mockCPUHistogram.On("AddSample", 1.57, 2.3, testTimestamp.Add(2*timeStep))
	// Verify that memory peaks are added to the memory peaks histogram.
	memoryAggregationWindowEnd := testTimestamp.Add(memoryAggregationInterval)
	test.mockMemoryHistogram.On("AddSample", 5.0, 1.0, memoryAggregationWindowEnd)
	test.mockMemoryHistogram.On("SubtractSample", 5.0, 1.0, memoryAggregationWindowEnd)
	test.mockMemoryHistogram.On("AddSample", 10.0, 1.0, memoryAggregationWindowEnd)
	memoryAggregationWindowEnd = memoryAggregationWindowEnd.Add(memoryAggregationInterval)
	test.mockMemoryHistogram.On("AddSample", 2.0, 1.0, memoryAggregationWindowEnd)
	// Verify that RSS peaks are added to the RSS peaks histogram.
	test.mockRSSHistogram.On("AddSample", 4.0, 1.0, testTimestamp)
	test.mockRSSHistogram.On("AddSample", 8.0, 1.0, testTimestamp.Add(timeStep))
	test.mockRSSHistogram.On("AddSample", 2.0, 1.0, testTimestamp.Add(2*timeStep))
	// Verify that JVM Heap peaks are added to the JVM Heap peaks histogram.
	test.mockJVMHeapHistogram.On("AddSample", 3.0, 1.0, testTimestamp)
	test.mockJVMHeapHistogram.On("AddSample", 6.0, 1.0, testTimestamp.Add(timeStep))
	test.mockJVMHeapHistogram.On("AddSample", 2.0, 1.0, testTimestamp.Add(2*timeStep))

	// Add three CPU, memory, RSS and JVM Heap Committed usage samples.
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 3140, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 5, ResourceMemory)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 4, ResourceRSS)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 3, ResourceJVMHeapCommitted)))

	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 6280, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 10, ResourceMemory)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 8, ResourceRSS)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 6, ResourceJVMHeapCommitted)))

	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 1570, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 2, ResourceMemory)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 2, ResourceRSS)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 2, ResourceJVMHeapCommitted)))

	// Discard invalid samples.
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceCPU)))
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceMemory)))
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceRSS)))
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceJVMHeapCommitted)))
	assert.False(t, c.AddSample(newUsageSample( // Negative CPU usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceCPU)))
	assert.False(t, c.AddSample(newUsageSample( // Negative memory usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceMemory)))
	assert.False(t, c.AddSample(newUsageSample( // Negative RSS usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceRSS)))
	assert.False(t, c.AddSample(newUsageSample( // Negative JVM Heap usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceJVMHeapCommitted)))
}

func TestRecordOOMIncreasedByBumpUp(t *testing.T) {
	test := newContainerTest()
	memoryAggregationWindowEnd := testTimestamp.Add(GetAggregationsConfig().MemoryAggregationInterval)
	// Bump Up factor is 20%.
	test.mockMemoryHistogram.On("AddSample", 1200.0*mb, 1.0, memoryAggregationWindowEnd)

	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(1000*mb)))
}

func TestRecordOOMDontRunAway(t *testing.T) {
	test := newContainerTest()
	memoryAggregationWindowEnd := testTimestamp.Add(GetAggregationsConfig().MemoryAggregationInterval)

	// Bump Up factor is 20%.
	test.mockMemoryHistogram.On("AddSample", 1200.0*mb, 1.0, memoryAggregationWindowEnd)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(1000*mb)))

	// new smaller OOMs don't influence the sample value (oomPeak)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(999*mb)))
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(999*mb)))

	test.mockMemoryHistogram.On("SubtractSample", 1200.0*mb, 1.0, memoryAggregationWindowEnd)
	test.mockMemoryHistogram.On("AddSample", 2400.0*mb, 1.0, memoryAggregationWindowEnd)
	// a larger OOM should increase the sample value
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(2000*mb)))
}

func TestRecordOOMIncreasedByMin(t *testing.T) {
	test := newContainerTest()
	memoryAggregationWindowEnd := testTimestamp.Add(GetAggregationsConfig().MemoryAggregationInterval)
	// Min grow by 100Mb.
	test.mockMemoryHistogram.On("AddSample", 101.0*mb, 1.0, memoryAggregationWindowEnd)

	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(1*mb)))
}

func TestRecordOOMMaxedWithKnownSample(t *testing.T) {
	test := newContainerTest()
	memoryAggregationWindowEnd := testTimestamp.Add(GetAggregationsConfig().MemoryAggregationInterval)

	test.mockMemoryHistogram.On("AddSample", 3000.0*mb, 1.0, memoryAggregationWindowEnd)
	assert.True(t, test.container.AddSample(newUsageSample(testTimestamp, 3000*mb, ResourceMemory)))

	// Last known sample is higher than request, so it is taken.
	test.mockMemoryHistogram.On("SubtractSample", 3000.0*mb, 1.0, memoryAggregationWindowEnd)
	test.mockMemoryHistogram.On("AddSample", 3600.0*mb, 1.0, memoryAggregationWindowEnd)

	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceMemory, ResourceAmount(1000*mb)))
}

func TestRecordOOMDiscardsOldSample(t *testing.T) {
	test := newContainerTest()
	memoryAggregationWindowEnd := testTimestamp.Add(GetAggregationsConfig().MemoryAggregationInterval)

	test.mockMemoryHistogram.On("AddSample", 1000.0*mb, 1.0, memoryAggregationWindowEnd)
	assert.True(t, test.container.AddSample(newUsageSample(testTimestamp, 1000*mb, ResourceMemory)))

	// OOM is stale, mem not changed.
	assert.Error(t, test.container.RecordOOM(testTimestamp.Add(-30*time.Hour), ResourceMemory, ResourceAmount(1000*mb)))
}

func TestRecordOOMInNewWindow(t *testing.T) {
	test := newContainerTest()
	memoryAggregationInterval := GetAggregationsConfig().MemoryAggregationInterval
	memoryAggregationWindowEnd := testTimestamp.Add(memoryAggregationInterval)

	test.mockMemoryHistogram.On("AddSample", 2000.0*mb, 1.0, memoryAggregationWindowEnd)
	assert.True(t, test.container.AddSample(newUsageSample(testTimestamp, 2000*mb, ResourceMemory)))

	memoryAggregationWindowEnd = memoryAggregationWindowEnd.Add(2 * memoryAggregationInterval)
	test.mockMemoryHistogram.On("AddSample", 2400.0*mb, 1.0, memoryAggregationWindowEnd)
	assert.NoError(t, test.container.RecordOOM(testTimestamp.Add(2*memoryAggregationInterval), ResourceMemory, ResourceAmount(1000*mb)))
}

func TestRecordOOMRSSConsecutive(t *testing.T) {
	test := newContainerTest()

	test.mockRSSHistogram.On("AddOomSample", 1000.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(1000*mb)))

	// Smaller OOMs are also recorded.
	test.mockRSSHistogram.On("AddOomSample", 900.0*mb, 1.0, testTimestamp)
	test.mockRSSHistogram.On("AddOomSample", 800.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(900*mb)))
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(800*mb)))

	// Larger OOMs are recorded.
	test.mockRSSHistogram.On("AddOomSample", 2000.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(2000*mb)))
}

func TestRecordOOMJvmHeapCommittedConsecutive(t *testing.T) {
	test := newContainerTest()

	test.mockRSSHistogram.On("Percentile", 1.0).Return(1000.0 * mb)
	test.mockJVMHeapHistogram.On("AddOomSample", 900.0*mb, 1.0, testTimestamp)
	test.mockRSSHistogram.On("AddOomSample", 1000.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceJVMHeapCommitted, ResourceAmount(900*mb)))

	// Smaller OOMs are also recorded.
	test.mockJVMHeapHistogram.On("AddOomSample", 900.0*mb, 1.0, testTimestamp)
	test.mockJVMHeapHistogram.On("AddOomSample", 800.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceJVMHeapCommitted, ResourceAmount(900*mb)))
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceJVMHeapCommitted, ResourceAmount(800*mb)))

	// Larger OOMs are recorded.
	test.mockJVMHeapHistogram.On("AddOomSample", 2000.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceJVMHeapCommitted, ResourceAmount(2000*mb)))
}
