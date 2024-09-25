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

package util

import (
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
)

// NewBinaryDecayingHistogram returns a new Histogram instance using given options.
func NewBinaryDecayingHistogram(options HistogramOptions, retentionDays int) Histogram {
	if retentionDays < 3 {
		panic("at least 3 days retention is required")
	}
	return &binaryDecayingHistogram{
		options:       options,
		bucketForDay:  make([]uint16, retentionDays),
		lastDayIndex:  0,
		retentionDays: retentionDays}
}

// A histogram that only supports returning the maximum value (Percentile(1.0)) of the samples in the last retentionDays days.
// This histogram does not support SubtractSample function.
//
// An example diagram of the binaryDecayingHistogram structure:
//
//	bucketForDay: [ 3 ][ 5 ][ 2 ][ 0 ][ 4 ][ 1 ]  (assuming retentionDays = 6)
//	                              ^
//	                         lastDayIndex
//
// In this diagram:
// - Each bracket [ ] represents a day in the bucketForDay slice
// - The numbers inside the brackets are the maximum bucket indices for each day
// - The ^ pointer shows where lastDayIndex is currently pointing
//
// How it works:
// 1. When a new sample is added:
//   - The day index is calculated based on the sample's timestamp
//   - If the day index is newer than lastDayIndex, the buffer is rotated:
//   - Days between lastDayIndex and the new day are cleared (set to 0)
//   - lastDayIndex is updated to the new day
//
// 2. The appropriate bucket for the sample value is determined
// 3. The bucket index is stored in bucketForDay[lastDayIndex % retentionDays]
// 4. When retrieving the maximum value (Percentile(1.0)):
//   - The maximum bucket index from all days in the retention period is found
//   - The corresponding value for that bucket is returned
//
// This structure allows for efficient storage and retrieval of maximum values
// over the specified retention period, with O(1) time complexity for adding samples
// and O(retentionDays) for retrieving the maximum value.
type binaryDecayingHistogram struct {
	// Bucketing scheme.
	options HistogramOptions
	// Index of a bucket for each day. Buckets are indexed starting with 1. 0 means no sample, 1 means the first bucket, etc.
	bucketForDay []uint16
	// Last recorded day index. If the value is greater than 0 then the bucketForDay holds retentionDays days of data. If lastDayIndex is 0, then the histogram is empty.
	// The bucket number (max for the day) for lastDayIndex is stored at bucketForDay[lastDayIndex%retentionDays]. The bucket number for the day before is
	// stored at bucketForDay[(lastDayIndex-1)%retentionDays] etc.
	lastDayIndex int
	// Retention days.
	retentionDays int
}

func (h *binaryDecayingHistogram) dayIndex(ts time.Time) int {
	return int(ts.Unix() / (60 * 60 * 24))
}

func (h *binaryDecayingHistogram) addSampleToBucket(bucket uint16, dayIndex int) {
	if h.lastDayIndex > 0 {
		// Ignore samples too far in the past
		if dayIndex <= h.lastDayIndex-h.retentionDays {
			return
		}
		// If dayIndex not in the future, we will set bucket to max of current and new value
		if dayIndex <= h.lastDayIndex && h.bucketForDay[dayIndex%h.retentionDays] > bucket {
			bucket = h.bucketForDay[dayIndex%h.retentionDays]
		}
		// Move forward if necessary
		if dayIndex > h.lastDayIndex {
			i := h.lastDayIndex + 1
			for i < dayIndex {
				h.bucketForDay[i%h.retentionDays] = 0
				// Break if we cleared out all days
				if i%h.retentionDays == h.lastDayIndex%h.retentionDays {
					break
				}
				i++
			}
		}
	}
	h.bucketForDay[dayIndex%h.retentionDays] = bucket
	if dayIndex > h.lastDayIndex {
		h.lastDayIndex = dayIndex
	}
}

func (h *binaryDecayingHistogram) addSample(value float64, dayIndex int, isOOM bool) {
	bucket := h.options.FindBucket(value) + 1
	if isOOM {
		// OOM samples are stored in the next bucket to differentiate them
		// from real memory samples taken at the memory limit.
		bucket += 1
	}
	h.addSampleToBucket(uint16(bucket), dayIndex)
}

func (h *binaryDecayingHistogram) AddSample(value float64, weight float64, time time.Time, isOOM bool) {
	dayIndex := h.dayIndex(time)
	h.addSample(value, dayIndex, isOOM)
}

func (h *binaryDecayingHistogram) SubtractSample(value float64, weight float64, time time.Time) {
	panic("SubtractSample function is not implemented for binary decaying histogram")
}

func (h *binaryDecayingHistogram) Merge(other Histogram) {
	o := other.(*binaryDecayingHistogram)
	if h.options != o.options {
		panic("can't merge histograms with different options")
	}
	if h.retentionDays != o.retentionDays {
		panic("can't merge histograms with different retention days")
	}
	for day := o.lastDayIndex; day > o.lastDayIndex-o.retentionDays && day >= 0; day-- {
		bucket := o.bucketForDay[day%o.retentionDays]
		if day <= h.lastDayIndex && day > h.lastDayIndex-h.retentionDays {
			if h.bucketForDay[day%h.retentionDays] > bucket {
				bucket = h.bucketForDay[day%h.retentionDays]
			}
		}
		if bucket > 0 {
			h.addSampleToBucket(bucket, day)
		}
	}
}

func (h *binaryDecayingHistogram) Percentile(percentile float64) float64 {
	if percentile != 1.0 {
		panic("Only Percentile(1.0) function is implemented for binary decaying histogram, got: " + fmt.Sprintf("%f", percentile))
	}
	maxBucket := uint16(0)
	for day := 0; day < h.retentionDays; day++ {
		if h.bucketForDay[day] > maxBucket {
			maxBucket = h.bucketForDay[day]
		}
	}
	value := 0.0
	if maxBucket > 0 {
		value = h.valueForBucket(int(maxBucket) - 1)
	}
	return value
}

func (h *binaryDecayingHistogram) IsEmpty() bool {
	for day := 0; day < h.retentionDays; day++ {
		if h.bucketForDay[day] != 0 {
			return false
		}
	}
	return true
}

func (h *binaryDecayingHistogram) valueForBucket(bucket int) float64 {
	if bucket < h.options.NumBuckets()-1 {
		// Return the end of the bucket.
		return h.options.GetBucketStart(bucket + 1)
	}
	// Return the start of the last bucket (note that the last bucket
	// doesn't have an upper bound).
	return h.options.GetBucketStart(bucket)
}

func (h *binaryDecayingHistogram) String() string {
	lines := []string{
		fmt.Sprintf("retentionDays: %d", h.retentionDays),
		"day\tbucket\tvalue",
	}
	for day := h.lastDayIndex; day > h.lastDayIndex-h.retentionDays && day >= 0; day-- {
		bucket := h.bucketForDay[day%h.retentionDays]
		value := 0.0
		if bucket > 0 {
			value = h.valueForBucket(int(bucket) - 1)
		}
		lines = append(lines, fmt.Sprintf("%d\t%d\t%.3f", day, bucket, value))
	}
	lines = append(lines, "\n")
	return strings.Join(lines, "\n")
}

func (h *binaryDecayingHistogram) Equals(other Histogram) bool {
	h2, typesMatch := other.(*binaryDecayingHistogram)
	if !typesMatch || h.options != h2.options || h.retentionDays != h2.retentionDays || h.lastDayIndex != h2.lastDayIndex {
		return false
	}
	for day := 0; day < h.retentionDays; day++ {
		if h.bucketForDay[day] != h2.bucketForDay[day] {
			return false
		}
	}
	return true
}

// Saving to checkpoint is done by converting in memory representation (list of days) to list of bucket weights.
// One day requires 1 bit of storage, so we can store 32 days in one uint32. Since number of days can be arbitrary,
// we may need multiple uint32s to store all days for one bucket. The bits for bucket 0 are stored in checkpoint with
// index 0, h.options.NumBuckets(), 2*h.options.NumBuckets() etc. The bits for bucket 1 are stored in checkpoint with
// index 1, h.options.NumBuckets()+1, 2*h.options.NumBuckets()+1 etc.
// The resulting checkpoint leaves out the total weight, as it is not used by this histogram type.
// This allows us to detect if checkpoint was saved by decaying histogram (positive total weight) or by this histogram type (total weight 0).
func (h *binaryDecayingHistogram) SaveToChekpoint() (*vpa_types.HistogramCheckpoint, error) {
	result := vpa_types.HistogramCheckpoint{
		BucketWeights:      make(map[int]uint32),
		ReferenceTimestamp: metav1.NewTime(time.Unix(int64(h.lastDayIndex*60*60*24), 0)),
	}
	for day := h.lastDayIndex; day > h.lastDayIndex-h.retentionDays && day >= 0; day-- {
		bucket := h.bucketForDay[day%h.retentionDays]
		if bucket > 0 {
			intIndex := (h.lastDayIndex - day) / 32
			bucketBase := intIndex * h.options.NumBuckets()
			bucketIndex := bucketBase + int(bucket) - 1
			bitIndex := (h.lastDayIndex - day) % 32
			result.BucketWeights[bucketIndex] |= 1 << bitIndex
		}
	}
	return &result, nil
}

// Loading from checkpoint supports loading from checkpoints saved by decaying histogram and by this histogram type.
// It supports loading from checkpoint with different retention days.
func (h *binaryDecayingHistogram) LoadFromCheckpoint(checkpoint *vpa_types.HistogramCheckpoint) error {
	if checkpoint == nil {
		return fmt.Errorf("cannot load from empty checkpoint")
	}
	h.bucketForDay = make([]uint16, h.retentionDays)
	if checkpoint.TotalWeight > 0 {
		// Loading from checkpoint saved by a different histogram type.
		// In this case we only extract max.
		basicHistogram := NewHistogram(h.options)
		basicHistogram.LoadFromCheckpoint(checkpoint)
		h.AddSample(basicHistogram.Percentile(1.0), 1, time.Now(), false)
	} else {
		if checkpoint.ReferenceTimestamp.Time.IsZero() {
			h.lastDayIndex = 0
		} else {
			h.lastDayIndex = h.dayIndex(checkpoint.ReferenceTimestamp.Time)
			for day := h.lastDayIndex; day > h.lastDayIndex-h.retentionDays && day >= 0; day-- {
				intIndex := (h.lastDayIndex - day) / 32
				bucketBase := intIndex * h.options.NumBuckets()
				bitIndex := (h.lastDayIndex - day) % 32
				for bucket := 1; bucket <= h.options.NumBuckets(); bucket++ {
					bucketIndex := bucketBase + bucket - 1
					if (checkpoint.BucketWeights[bucketIndex] & (1 << bitIndex)) != 0 {
						h.bucketForDay[day%h.retentionDays] = uint16(bucket)
						break
					}
				}
			}
		}
	}
	return nil
}
