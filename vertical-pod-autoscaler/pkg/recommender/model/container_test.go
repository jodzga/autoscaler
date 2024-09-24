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
		ResourceCPU:    CPUAmountFromCores(2.3),
		ResourceMemory: MemoryAmountFromBytes(5e8),
		ResourceRSS:    MemoryAmountFromBytes(4e8),
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
	aggregateContainerState *AggregateContainerState
	container               *ContainerState
}

func newContainerTest() ContainerTest {
	mockCPUHistogram := new(util.MockHistogram)
	mockMemoryHistogram := new(util.MockHistogram)
	mockRSSHistogram := new(util.MockHistogram)
	aggregateContainerState := &AggregateContainerState{
		AggregateCPUUsage:    mockCPUHistogram,
		AggregateMemoryPeaks: mockMemoryHistogram,
		AggregateRSSPeaks:    mockRSSHistogram,
	}
	container := &ContainerState{
		Request:    TestRequest,
		aggregator: aggregateContainerState,
	}
	return ContainerTest{
		mockCPUHistogram:        mockCPUHistogram,
		mockMemoryHistogram:     mockMemoryHistogram,
		mockRSSHistogram:        mockRSSHistogram,
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

	// Add three CPU, memory, and RSS usage samples.
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 3140, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 5, ResourceMemory)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 4, ResourceRSS)))

	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 6280, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 10, ResourceMemory)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 8, ResourceRSS)))

	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 1570, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 2, ResourceMemory)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 2, ResourceRSS)))

	// Discard invalid samples.
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceCPU)))
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceMemory)))
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(1*timeStep), 1000, ResourceRSS)))
	assert.False(t, c.AddSample(newUsageSample( // Negative CPU usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceCPU)))
	assert.False(t, c.AddSample(newUsageSample( // Negative memory usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceMemory)))
	assert.False(t, c.AddSample(newUsageSample( // Negative RSS usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceRSS)))
}

func TestRecordOOMConsecutive(t *testing.T) {
	test := newContainerTest()

	test.mockRSSHistogram.On("AddSample", 1100.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(1000*mb)))

	// Smaller OOMs are also recorded.
	test.mockRSSHistogram.On("AddSample", 990.0*mb, 1.0, testTimestamp)
	test.mockRSSHistogram.On("AddSample", 880.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(900*mb)))
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(800*mb)))

	// Larger OOMs are recorded.
	test.mockRSSHistogram.On("AddSample", 2200.0*mb, 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(2000*mb)))
}

func TestRecordOOMDiscardsOldSample(t *testing.T) {
	test := newContainerTest()

	test.mockRSSHistogram.On("AddSample", 1000.0*mb, 1.0, testTimestamp)
	assert.True(t, test.container.AddSample(newUsageSample(testTimestamp, 1000*mb, ResourceRSS)))

	// OOM is stale.
	assert.Error(t, test.container.RecordOOM(testTimestamp.Add(-30*time.Hour), ResourceRSS, ResourceAmount(1000*mb)))
}
