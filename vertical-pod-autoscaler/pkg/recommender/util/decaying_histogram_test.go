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

package util

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
)

var (
	startTime = time.Unix(1234567890, 0) // Arbitrary timestamp.
)

// Verifies that Percentile() returns 0.0 when called on an empty decaying histogram
// for any percentile.
func TestPercentilesEmptyDecayingHistogram(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	for p := -0.5; p <= 1.5; p += 0.5 {
		assert.Equal(t, 0.0, h.Percentile(p))
	}
}

func TestMax(t *testing.T) {
	histogramOptions, _ := NewLinearHistogramOptions(10.0, 1.0, 0.0001)
	h := NewDecayingHistogram(histogramOptions, time.Hour)
	// Add a sample with a very large weight.
	h.AddSample(2, 1, startTime)
	h.AddSample(1, 1, startTime.Add(time.Hour*2))
	assert.InEpsilon(t, 3, h.Max(h.Epsilon(), startTime), valueEpsilon)

	// Solution to 1/(2^n-1) = 0.0001 is n = 13.2877
	// So, after 14 half life periods, the weight of the first sample
	// should be lower than the epsilon 0.0001.
	assert.InEpsilon(t, 3, h.Max(h.Epsilon(), startTime.Add(time.Hour*13)), valueEpsilon)
	assert.InEpsilon(t, 2, h.Max(h.Epsilon(), startTime.Add(time.Hour*14)), valueEpsilon)
}

func TestMax30d(t *testing.T) {
	histogramOptions, _ := NewLinearHistogramOptions(10.0, 1.0, 0.0001)
	// 60*24*30/13.2877 = 3251
	h := NewDecayingHistogram(histogramOptions, time.Minute*3251)
	// Add a sample with a very large weight.
	h.AddSample(2, 1, startTime)
	h.AddSample(1, 1, startTime.Add(time.Hour*24*2))
	assert.InEpsilon(t, 3, h.Max(h.Epsilon(), startTime), valueEpsilon)
	assert.InEpsilon(t, 2, h.Max(h.Epsilon(), startTime.Add(time.Hour*24*30)), valueEpsilon)
	assert.InEpsilon(t, 2, h.Max(h.Epsilon(), startTime.Add(time.Hour*24*31)), valueEpsilon)
	assert.InEpsilon(t, 1, h.Max(h.Epsilon(), startTime.Add(time.Hour*24*32)), valueEpsilon)
}

// Verify that a sample with a large weight is almost entirely (but not 100%)
// decayed after sufficient amount of time elapses.
func TestSimpleDecay(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	// Add a sample with a very large weight.
	h.AddSample(2, 1000, startTime)
	// Add another sample 20 half life periods later. Its relative weight is
	// expected to be 2^20 * 0.001 > 1000 times larger than the first sample.
	h.AddSample(1, 1, startTime.Add(time.Hour*20))
	assert.InEpsilon(t, 2, h.Percentile(0.999), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(1.0), valueEpsilon)
}

// Verify that the decaying histogram behaves correctly after the decaying
// factor grows by more than 2^maxDecayExponent.
func TestLongtermDecay(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	// Add a sample with a very large weight.
	h.AddSample(2, 1, startTime)
	// Add another sample later, such that the relative decay factor of the
	// two samples will exceed 2^maxDecayExponent.
	h.AddSample(1, 1, startTime.Add(time.Hour*101))
	assert.InEpsilon(t, 2, h.Percentile(1.0), valueEpsilon)
}

// Verify specific values of percentiles on an example decaying histogram with
// 4 samples added with different timestamps.
func TestDecayingHistogramPercentiles(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	timestamp := startTime
	// Add four samples with both values and weights equal to 1, 2, 3, 4,
	// each separated by one half life period from the previous one.
	for i := 1; i <= 4; i++ {
		h.AddSample(float64(i), float64(i), timestamp)
		timestamp = timestamp.Add(time.Hour)
	}
	// The expected distribution is:
	// bucket = [1..2], weight = 1 * 2^(-3), percentiles ~  0% ... 2%
	// bucket = [2..3], weight = 2 * 2^(-2), percentiles ~  3% ... 10%
	// bucket = [3..4], weight = 3 * 2^(-1), percentiles ~ 11% ... 34%
	// bucket = [4..5], weight = 4 * 2^(-0), percentiles ~ 35% ... 100%
	assert.InEpsilon(t, 2, h.Percentile(0.00), valueEpsilon)
	assert.InEpsilon(t, 2, h.Percentile(0.02), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.03), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.10), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.11), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.34), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.35), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(1.00), valueEpsilon)
}

// Verifies that the decaying histogram behaves the same way as a regular
// histogram if the time is fixed and no decaying happens.
func TestNoDecay(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	for i := 1; i <= 4; i++ {
		h.AddSample(float64(i), float64(i), startTime)
	}
	assert.InEpsilon(t, 2, h.Percentile(0.0), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.2), valueEpsilon)
	assert.InEpsilon(t, 2, h.Percentile(0.1), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.3), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.4), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.5), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.6), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.7), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.8), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.9), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(1.0), valueEpsilon)
}

// Verifies that Merge() works as expected on two sample decaying histograms.
func TestDecayingHistogramMerge(t *testing.T) {
	h1 := NewDecayingHistogram(testHistogramOptions, time.Hour)
	h1.AddSample(1, 1, startTime)
	h1.AddSample(2, 1, startTime.Add(time.Hour))

	h2 := NewDecayingHistogram(testHistogramOptions, time.Hour)
	h2.AddSample(2, 1, startTime.Add(time.Hour*2))
	h2.AddSample(3, 1, startTime.Add(time.Hour))

	expected := NewDecayingHistogram(testHistogramOptions, time.Hour)
	expected.AddSample(2, 1, startTime.Add(time.Hour*2))
	expected.AddSample(2, 1, startTime.Add(time.Hour))
	expected.AddSample(3, 1, startTime.Add(time.Hour))
	expected.AddSample(1, 1, startTime)

	h1.Merge(h2)
	assert.True(t, h1.Equals(expected))
}

func TestDecayingHistogramSaveToCheckpoint(t *testing.T) {
	d := &decayingHistogram{
		histogram:          *NewHistogram(testHistogramOptions).(*histogram),
		halfLife:           time.Hour,
		referenceTimestamp: time.Time{},
	}
	d.AddSample(2, 1, startTime.Add(time.Hour*100))
	assert.NotEqual(t, d.referenceTimestamp, time.Time{})

	checkpoint, err := d.SaveToChekpoint()
	assert.NoError(t, err)
	assert.Equal(t, checkpoint.ReferenceTimestamp.Time, d.referenceTimestamp)
	// Just check that buckets are not empty, actual testing of bucketing
	// belongs to Histogram
	assert.NotEmpty(t, checkpoint.BucketWeights)
	assert.NotZero(t, checkpoint.TotalWeight)
}

func TestDecayingHistogramLoadFromCheckpoint(t *testing.T) {
	location, _ := time.LoadLocation("UTC")
	timestamp := time.Date(2018, time.January, 2, 3, 4, 5, 0, location)

	checkpoint := vpa_types.HistogramCheckpoint{
		TotalWeight: 6.0,
		BucketWeights: map[int]uint32{
			0: 1,
		},
		ReferenceTimestamp: metav1.NewTime(timestamp),
	}
	d := &decayingHistogram{
		histogram:          *NewHistogram(testHistogramOptions).(*histogram),
		halfLife:           time.Hour,
		referenceTimestamp: time.Time{},
	}
	err := d.LoadFromCheckpoint(&checkpoint)
	assert.NoError(t, err)

	assert.False(t, d.histogram.IsEmpty())
	assert.Equal(t, timestamp, d.referenceTimestamp)
}

func TestDecayingHistogramLoadFromDBCheckpoint(t *testing.T) {
	/*
	   apiVersion: autoscaling.k8s.io/v1
	   kind: VerticalPodAutoscalerCheckpoint
	   metadata:
	     creationTimestamp: '2023-10-25T11:41:45Z'
	     generation: 354639
	     managedFields:
	       - apiVersion: autoscaling.k8s.io/v1
	         fieldsType: FieldsV1
	         fieldsV1:
	           f:spec:
	             .: {}
	             f:containerName: {}
	             f:vpaObjectName: {}
	           f:status:
	             .: {}
	             f:cpuHistogram:
	               .: {}
	               f:bucketWeights:
	                 .: {}
	                 f:10: {}
	                 f:11: {}
	                 f:12: {}
	                 f:13: {}
	                 f:14: {}
	                 f:15: {}
	                 f:16: {}
	                 f:17: {}
	                 f:18: {}
	                 f:19: {}
	                 f:2: {}
	                 f:20: {}
	                 f:21: {}
	                 f:22: {}
	                 f:23: {}
	                 f:24: {}
	                 f:25: {}
	                 f:26: {}
	                 f:27: {}
	                 f:28: {}
	                 f:29: {}
	                 f:3: {}
	                 f:30: {}
	                 f:31: {}
	                 f:32: {}
	                 f:33: {}
	                 f:34: {}
	                 f:35: {}
	                 f:36: {}
	                 f:37: {}
	                 f:38: {}
	                 f:39: {}
	                 f:4: {}
	                 f:40: {}
	                 f:41: {}
	                 f:42: {}
	                 f:43: {}
	                 f:5: {}
	                 f:6: {}
	                 f:7: {}
	                 f:8: {}
	                 f:9: {}
	               f:referenceTimestamp: {}
	               f:totalWeight: {}
	             f:firstSampleStart: {}
	             f:jvmHeapCommittedHistogram:
	               .: {}
	               f:referenceTimestamp: {}
	             f:lastSampleStart: {}
	             f:lastUpdateTime: {}
	             f:memoryHistogram:
	               .: {}
	               f:bucketWeights:
	                 .: {}
	                 f:76: {}
	                 f:77: {}
	                 f:78: {}
	                 f:79: {}
	                 f:80: {}
	               f:referenceTimestamp: {}
	               f:totalWeight: {}
	             f:rssHistogram:
	               .: {}
	               f:bucketWeights:
	                 .: {}
	                 f:76: {}
	                 f:77: {}
	                 f:78: {}
	                 f:79: {}
	               f:referenceTimestamp: {}
	               f:totalWeight: {}
	             f:totalSamplesCount: {}
	             f:version: {}
	         manager: recommender-amd64
	         operation: Update
	         time: '2024-07-26T16:46:05Z'
	     name: cmapiserver-deployment-high-vpa-cmapiserver
	     namespace: cmapiserver
	     resourceVersion: '2904247470'
	     uid: 02b92c5d-cc88-4376-a5d7-e007484a46a7
	     selfLink: >-
	       /apis/autoscaling.k8s.io/v1/namespaces/cmapiserver/verticalpodautoscalercheckpoints/cmapiserver-deployment-high-vpa-cmapiserver
	   status:
	     cpuHistogram:
	       bucketWeights:
	         '2': 1
	         '3': 4
	         '4': 504
	         '5': 7507
	         '6': 10000
	         '7': 7139
	         '8': 4339
	         '9': 3155
	         '10': 3546
	         '11': 2243
	         '12': 1404
	         '13': 879
	         '14': 772
	         '15': 649
	         '16': 573
	         '17': 501
	         '18': 486
	         '19': 397
	         '20': 301
	         '21': 258
	         '22': 191
	         '23': 100
	         '24': 84
	         '25': 53
	         '26': 41
	         '27': 32
	         '28': 27
	         '29': 25
	         '30': 22
	         '31': 23
	         '32': 25
	         '33': 19
	         '34': 10
	         '35': 8
	         '36': 8
	         '37': 3
	         '38': 5
	         '39': 1
	         '40': 1
	         '41': 3
	         '42': 1
	         '43': 1
	       referenceTimestamp: '2024-07-29T00:00:00Z'
	       totalWeight: 2678.9068695782944
	     firstSampleStart: '2023-10-25T04:09:53Z'
	     jvmHeapCommittedHistogram:
	       referenceTimestamp: null
	     lastSampleStart: '2024-07-28T04:30:44Z'
	     lastUpdateTime: '2024-07-28T04:31:12Z'
	     memoryHistogram:
	       bucketWeights:
	         '76': 130
	         '77': 1837
	         '78': 10000
	         '79': 1399
	         '80': 617
	       referenceTimestamp: '2024-07-26T00:00:00Z'
	       totalWeight: 320.04235388263606
	     rssHistogram:
	       bucketWeights:
	         '76': 148
	         '77': 2493
	         '78': 10000
	         '79': 775
	       referenceTimestamp: '2024-07-26T00:00:00Z'
	       totalWeight: 309.41187082249957
	     totalSamplesCount: 819544
	     version: v3
	   spec:
	     containerName: cmapiserver
	     vpaObjectName: cmapiserver-deployment-high-vpa

	*/

	location, _ := time.LoadLocation("UTC")
	timestamp := time.Date(2024, time.July, 29, 0, 0, 0, 0, location)

	checkpoint := vpa_types.HistogramCheckpoint{
		TotalWeight: 2678.9068695782944,
		BucketWeights: map[int]uint32{
			2:  1,
			3:  4,
			4:  504,
			5:  7507,
			6:  10000,
			7:  7139,
			8:  4339,
			9:  3155,
			10: 3546,
			11: 2243,
			12: 1404,
			13: 879,
			14: 772,
			15: 649,
			16: 573,
			17: 501,
			18: 486,
			19: 397,
			20: 301,
			21: 258,
			22: 191,
			23: 100,
			24: 84,
			25: 53,
			26: 41,
			27: 32,
			28: 27,
			29: 25,
			30: 22,
			31: 23,
			32: 25,
			33: 19,
			34: 10,
			35: 8,
			36: 8,
			37: 3,
			38: 5,
			39: 1,
			40: 1,
			41: 3,
			42: 1,
			43: 1,
		},
		ReferenceTimestamp: metav1.NewTime(timestamp),
	}
	options, err := NewExponentialHistogramOptions(1000.0, 0.01, 1.+0.05, epsilon)
	assert.NoError(t, err)
	d := &decayingHistogram{
		histogram:          *NewHistogram(options).(*histogram),
		halfLife:           time.Hour * 24 * 7,
		referenceTimestamp: time.Time{},
	}
	err = d.LoadFromCheckpoint(&checkpoint)
	assert.NoError(t, err)

	assert.False(t, d.histogram.IsEmpty())
	assert.Equal(t, timestamp, d.referenceTimestamp)

	fmt.Println(d.String())
}
