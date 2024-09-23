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
		ResourceCPU: CPUAmountFromCores(2.3),
		ResourceRSS: MemoryAmountFromBytes(5e8),
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
		// We should add limit here and we should say resource instead of oomtype in the other
		Resource:     resource,
	}
}

type ContainerTest struct {
	mockCPUHistogram        *util.MockHistogram
	mockRSSHistogram        *util.MockHistogram
	aggregateContainerState *AggregateContainerState
	container               *ContainerState
}

func newContainerTest() ContainerTest {
	mockCPUHistogram := new(util.MockHistogram)
	mockRSSHistogram := new(util.MockHistogram)
	aggregateContainerState := &AggregateContainerState{
		AggregateCPUUsage:    mockCPUHistogram,
		AggregateRSSPeaks: mockRSSHistogram,
	}
	container := &ContainerState{
		Request:    TestRequest,
		aggregator: aggregateContainerState,
	}
	return ContainerTest{
		mockCPUHistogram:        mockCPUHistogram,
		mockRSSHistogram:        mockRSSHistogram,
		aggregateContainerState: aggregateContainerState,
		container:               container,
	}
}

// Add 6 usage samples (3 valid, 3 invalid) to a container. Verifies that for
// valid samples the CPU measures are aggregated in the CPU histogram and
// the memory measures are AGGREGATED FIX THIS.
// Verifies that invalid samples (out-of-order or negative usage) are ignored.
func TestAggregateContainerUsageSamples(t *testing.T) {
	test := newContainerTest()
	c := test.container
	// Verify that CPU measures are added to the CPU histogram.
	// The weight should be equal to the current request.
	timeStep := 10 * time.Minute
	test.mockCPUHistogram.On("AddSample", 3.14, 2.3, testTimestamp)
	test.mockCPUHistogram.On("AddSample", 6.28, 2.3, testTimestamp.Add(timeStep))
	test.mockCPUHistogram.On("AddSample", 1.57, 2.3, testTimestamp.Add(2*timeStep))
	// Verify that memory peaks are added to the memory peaks histogram.
	test.mockRSSHistogram.On("AddSample", 5.0, 1.0, testTimestamp)
	test.mockRSSHistogram.On("AddSample", 10.0, 1.0, testTimestamp.Add(timeStep))
	test.mockRSSHistogram.On("AddSample", 2.0, 1.0, testTimestamp.Add(2*timeStep))

	// Add three CPU and memory usage samples.
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 3140, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp, 5, ResourceRSS)))

	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 6280, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(timeStep), 10, ResourceRSS)))

	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 1570, ResourceCPU)))
	assert.True(t, c.AddSample(newUsageSample(
		testTimestamp.Add(2*timeStep), 2, ResourceRSS)))

	// Discard invalid samples.
	assert.False(t, c.AddSample(newUsageSample( // Out of order sample.
		testTimestamp.Add(2*timeStep), 1000, ResourceCPU)))
	assert.False(t, c.AddSample(newUsageSample( // Negative CPU usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceCPU)))
	assert.False(t, c.AddSample(newUsageSample( // Negative memory usage.
		testTimestamp.Add(4*timeStep), -1000, ResourceRSS)))
}

func TestRecordOOMAddsMemoryLimitWithSmallConst(t *testing.T) {
	test := newContainerTest()
	// Artificial memory sample would be memory limit plus small const of 10MiB.
	test.mockRSSHistogram.On("AddSample", 2000.0*mb+(10.0*mb), 1.0, testTimestamp)

	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(2000.0*mb)))
}

func TestRecordOOMDontRunAway(t *testing.T) {
	test := newContainerTest()

	// Bump Up factor is 20%.
	test.mockRSSHistogram.On("AddSample", 1000.0*mb + (10.0*mb), 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(1000.0*mb)))

	// new smaller OOMs don't influence the sample value (oomPeak)
	test.mockRSSHistogram.On("AddSample", 999.0*mb + (10.0*mb), 1.0, testTimestamp)
	test.mockRSSHistogram.On("AddSample", 999.0*mb + (10.0*mb), 1.0, testTimestamp)
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(999.0*mb)))
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(999.0*mb)))

	test.mockRSSHistogram.On("AddSample", 2000.0*mb + (10.0*mb), 1.0, testTimestamp)
	// a larger OOM should increase the sample value
	assert.NoError(t, test.container.RecordOOM(testTimestamp, ResourceRSS, ResourceAmount(2000.0*mb)))
}

func TestRecordOOMDiscardsOldSample(t *testing.T) {
	test := newContainerTest()

	test.mockRSSHistogram.On("AddSample", 1000.0*mb, 1.0, testTimestamp)
	assert.True(t, test.container.AddSample(newUsageSample(testTimestamp, 1000.0*mb, ResourceRSS)))

	// OOM is stale, mem not changed.
	assert.Error(t, test.container.RecordOOM(testTimestamp.Add(-30*time.Hour), ResourceRSS, ResourceAmount(1200.0*mb)))
}
