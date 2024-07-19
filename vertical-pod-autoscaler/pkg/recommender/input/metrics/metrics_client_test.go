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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetContainersMetricsReturnsEmptyList(t *testing.T) {
	tc := newEmptyMetricsClientTestCase()
	emptyMetricsClient := tc.createFakeMetricsClient()

	containerMetricsSnapshots, err := emptyMetricsClient.GetContainersMetrics()

	assert.NoError(t, err)
	assert.Empty(t, containerMetricsSnapshots, "should be empty for empty MetricsGetter")
}

func TestGetContainersMetricsReturnsResults(t *testing.T) {
	tc := newMetricsClientTestCase()
	fakeMetricsClient := tc.createFakeMetricsClient()

	snapshots, err := fakeMetricsClient.GetContainersMetrics()

	assert.NoError(t, err)
	assert.Len(t, snapshots, len(tc.getAllSnaps()), "It should return right number of snapshots")
	for _, snap := range snapshots {
		assert.Contains(t, tc.getAllSnaps(), snap, "One of returned ContainerMetricsSnapshot is different then expected ")
	}
}

func TestCalculateUsageReturnsResults(t *testing.T) {
	containerUsage := []k8sapiv1.ResourceList{
		k8sapiv1.ResourceCPU:    resource.MustParse("100m"),
		k8sapiv1.ResourceMemory: resource.MustParse("100Mi"),
		k8sapiv1.ResourceRSS:    resource.MustParse("100Mi"),
		// Missing k8sapiv1.ResourceJVMHeapCommitted
	}

	result := calculateUsage(containerUsage)
	assert.Len(t, result, 3, "It should return 3 results")
	assert.Equal(t, int64(100), result[ResourceCPU], "CPU usage should be 100 millicores")
	assert.Equal(t, int64(104857600), result[ResourceMemory], "Memory usage should be 104857600 bytes")
	assert.Equal(t, int64(104857600), result[ResourceRSS], "RSS usage should be 104857600 bytes")
}
