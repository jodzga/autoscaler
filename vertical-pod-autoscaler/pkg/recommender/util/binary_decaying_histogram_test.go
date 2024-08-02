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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	bucketGrowth = 0.05
	// Test histogram options.
	testBinaryDecayingHistogramOptions, _ = NewExponentialHistogramOptions(1e12, 1e7, 1.+bucketGrowth, epsilon)

	retentionsToTest = []int{30, 32, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360}

	v128Mib        = float64(128 * 1024 * 1024)
	v128MibEpsilon = bucketLength(testBinaryDecayingHistogramOptions, v128Mib) / v128Mib
	v256Mib        = float64(128 * 1024 * 1024)
	v256MibEpsilon = bucketLength(testBinaryDecayingHistogramOptions, v256Mib) / v256Mib
)

func bucketLength(options HistogramOptions, value float64) float64 {
	bucket := options.FindBucket(value)
	// special handling of the last bucket
	if bucket == options.NumBuckets()-1 {
		return options.GetBucketStart(bucket) - options.GetBucketStart(bucket-1)
	} else {
		return options.GetBucketStart(bucket+1) - options.GetBucketStart(bucket)
	}
}

// Verifies that Percentile(1.0) returns 0.0 when called on an empty histogram for
// any percentile.
func TestPercentilesEmptyBinaryDecayingHistogram(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			assert.Equal(t, 0.0, h.Percentile(1.0))
		})
	}
}

func TestPercentilesBinaryDecayingHistogram(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h.AddSample(v128Mib, 1.0, startTime)
			assert.InEpsilon(t, v128Mib, h.Percentile(1.0), v128MibEpsilon)

			// Adding more samples should not change the result.
			h.AddSample(1024*1024, 1.0, startTime)
			h.AddSample(1024*1024, 1.0, startTime)
			assert.InEpsilon(t, v128Mib, h.Percentile(1.0), v128MibEpsilon)

			// Adding smaller samples should not change the result.
			h.AddSample(512*1024, 1.0, startTime)
			h.AddSample(128*1024, 1.0, startTime)
			assert.InEpsilon(t, v128Mib, h.Percentile(1.0), v128MibEpsilon)

			// Adding higher samples should change the result.
			h.AddSample(v256Mib, 1.0, startTime)
			assert.InEpsilon(t, v256Mib, h.Percentile(1.0), v256MibEpsilon)
		})
	}
}

// Verifies that IsEmpty() returns true on an empty histogram and false otherwise.
func TestEmptyBinaryDecayingHistogram(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			assert.True(t, h.IsEmpty())
			h.AddSample(v128Mib, 1.0, startTime)
			assert.False(t, h.IsEmpty())
		})
	}
}

func TestBinaryDecayingHistogramRetention(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h.AddSample(v128Mib, 1.0, startTime)
			assert.InEpsilon(t, v128Mib, h.Percentile(1.0), v128MibEpsilon)

			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			firstDayBeoyondRetention := dayIndex + retentionDays
			firstDayBeoyondRetentionWindowStart := time.Unix(int64(firstDayBeoyondRetention*60*60*24), 0)

			currentTime := startTime.Add(time.Minute)
			// Iterate all minutes starting from the currentTime until the firstDayBeoyondRetentionWindowStart.
			for currentTime.Before(firstDayBeoyondRetentionWindowStart) {
				// low number between 10 and 69
				lowNumber := float64(currentTime.Minute() + 10)

				h.AddSample(lowNumber*1024*1024, 1.0, currentTime)
				assert.InEpsilon(t, v128Mib, h.Percentile(1.0), v128MibEpsilon)

				currentTime = currentTime.Add(time.Minute)
			}

			lastDay := firstDayBeoyondRetention + retentionDays
			lastDayWindowStart := time.Unix(int64(lastDay*60*60*24), 0)

			currentTime = currentTime.Add(time.Minute)
			// Iterate all minutes starting from the firstDayBeoyondRetentionWindowStart until the lastDayWindowStart.
			for currentTime.Before(lastDayWindowStart) {

				h.AddSample(1, 1.0, currentTime)
				assert.InEpsilon(t, 69*1024*1024, h.Percentile(1.0), bucketLength(testBinaryDecayingHistogramOptions, 69*1024*1024)/69*1024*1024)

				currentTime = currentTime.Add(time.Minute)
			}
		})
	}
}

func TestBinaryDecayingHistogramRetentionChange(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			shorter := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays/2)
			currentTime := startTime
			lastTime := startTime.AddDate(2, 0, 0)
			for currentTime.Before(lastTime) {
				seconds := currentTime.Unix()
				mb := float64(seconds%1000000) * 1024 * 1024
				h.AddSample(mb, 1.0, currentTime)
				shorter.AddSample(mb, 1.0, currentTime)
				s, err := h.SaveToChekpoint()
				assert.NoError(t, err)
				loaded := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
				loaded.LoadFromCheckpoint(s)
				assert.True(t, h.Equals(loaded))

				loadedShorter := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays/2)
				loadedShorter.LoadFromCheckpoint(s)
				assert.True(t, shorter.Equals(loadedShorter))

				longer := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays*2)
				ct := currentTime
				for i := 0; i < retentionDays && !ct.Before(startTime); i++ {
					seconds := ct.Unix()
					mb := float64(seconds%1000000) * 1024 * 1024
					longer.AddSample(mb, 1.0, ct)
					ct = ct.AddDate(0, 0, -1)
				}
				loadedLonger := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays*2)
				loadedLonger.LoadFromCheckpoint(s)
				assert.True(t, longer.Equals(loadedLonger))

				currentTime = currentTime.AddDate(0, 0, 1)
			}

		})
	}
}

func TestBinaryDecayingHistogramMergeNonOverlappingLarger(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h1 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h2 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			expected := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			for i := 0; i < retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h1.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
			}
			for i := retentionDays; i < 2*retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h2.AddSample(float64((i+1)*1024*1024), 1.0, ts)
				expected.AddSample(float64((i+1)*1024*1024), 1.0, ts)
			}
			h1.Merge(h2)
			assert.True(t, h1.Equals(expected))
		})
	}
}

func TestBinaryDecayingHistogramMergeNonOverlappingSmaller(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h1 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h2 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			expected := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			for i := 0; i < retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h2.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
			}
			for i := retentionDays; i < 2*retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h1.AddSample(float64((i+1)*1024*1024), 1.0, ts)
				expected.AddSample(float64((i+1)*1024*1024), 1.0, ts)
			}
			h1.Merge(h2)
			assert.True(t, h1.Equals(expected))
		})
	}
}

func TestBinaryDecayingHistogramMergeOverlappingLarger(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h1 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h2 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			expected := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			for i := 0; i < retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h1.AddSample(float64((i+1)*1024*1024), 1.0, ts)
			}
			for i := retentionDays - retentionDays/2; i < 2*retentionDays-retentionDays/2; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h2.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
				expected.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
			}
			h1.Merge(h2)
			assert.True(t, h1.Equals(expected))
		})
	}
}

func TestBinaryDecayingHistogramMergeOverlappingSmaller(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h1 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h2 := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			expected := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			for i := 0; i < retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h1.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
			}
			for i := retentionDays - retentionDays/2; i < 2*retentionDays-retentionDays/2; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h2.AddSample(float64((i+1)*1024*1024), 1.0, ts)
				expectedValue := float64((i + 1) * 1024 * 1024)
				if i < retentionDays {
					expectedValue = float64((i + 1) * 1024 * 1024 * 1024)
				}
				expected.AddSample(expectedValue, 1.0, ts)
			}
			h1.Merge(h2)
			assert.True(t, h1.Equals(expected))
		})
	}
}

func TestBinaryDecayingHistogramMergeSame(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			expected := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			for i := 0; i < retentionDays; i++ {
				ts := time.Unix(int64((dayIndex+i)*60*60*24), 0)
				h.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
				expected.AddSample(float64((i+1)*1024*1024*1024), 1.0, ts)
			}
			h.Merge(expected)
			assert.True(t, h.Equals(expected))
		})
	}
}

func TestBinaryDecayingHistogramSaveToCheckpointEmpty(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			s, err := h.SaveToChekpoint()
			assert.NoError(t, err)
			assert.Equal(t, 0., s.TotalWeight)
			assert.Len(t, s.BucketWeights, 0)
			assert.Equal(t, time.Unix(0, 0), s.ReferenceTimestamp.Time)
		})
	}
}

func TestBinaryDecayingHistogramSaveToCheckpoint(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			h.AddSample(v128Mib, 1.0, startTime)
			s, err := h.SaveToChekpoint()
			assert.NoError(t, err)
			bucket := testBinaryDecayingHistogramOptions.FindBucket(v128Mib)
			assert.Len(t, s.BucketWeights, 1)
			assert.Contains(t, s.BucketWeights, bucket)
			assert.Equal(t, uint32(1), s.BucketWeights[bucket])
			assert.Equal(t, 0., s.TotalWeight)
			dayIndex := int(startTime.Unix() / (60 * 60 * 24))
			referenceTimestamp := time.Unix(int64(dayIndex*60*60*24), 0)
			assert.Equal(t, referenceTimestamp, s.ReferenceTimestamp.Time)

			// Perfect overlap, the same result is expected except for the reference timestamp.
			h.AddSample(v128Mib, 1.0, startTime.AddDate(0, 0, retentionDays))
			s, err = h.SaveToChekpoint()
			assert.NoError(t, err)
			bucket = testBinaryDecayingHistogramOptions.FindBucket(v128Mib)
			assert.Len(t, s.BucketWeights, 1)
			assert.Contains(t, s.BucketWeights, bucket)
			assert.Equal(t, uint32(1), s.BucketWeights[bucket])
			referenceTimestamp = time.Unix(int64((dayIndex+retentionDays)*60*60*24), 0)
			assert.Equal(t, referenceTimestamp, s.ReferenceTimestamp.Time)
			assert.Equal(t, 0., s.TotalWeight)

			// Set highest bit
			h.AddSample(v128Mib, 1.0, startTime.AddDate(0, 0, 2*retentionDays-1))
			s, err = h.SaveToChekpoint()
			assert.NoError(t, err)
			fullInts := (retentionDays - 1) / 32
			bucket = testBinaryDecayingHistogramOptions.FindBucket(v128Mib) + (fullInts * testBinaryDecayingHistogramOptions.NumBuckets())
			if retentionDays <= 32 {
				assert.Len(t, s.BucketWeights, 1)
			} else {
				assert.Len(t, s.BucketWeights, 2)
			}
			assert.Contains(t, s.BucketWeights, bucket)
			if retentionDays == 30 {
				assert.Equal(t, uint32(1<<29+1), s.BucketWeights[bucket])
			} else if retentionDays == 32 {
				assert.Equal(t, uint32(1<<31+1), s.BucketWeights[bucket])
			} else {
				// Only highest bit set
				assert.Equal(t, uint32(1<<((retentionDays-1)%32)), s.BucketWeights[bucket])
			}
			referenceTimestamp = time.Unix(int64((dayIndex+2*retentionDays-1)*60*60*24), 0)
			assert.Equal(t, referenceTimestamp, s.ReferenceTimestamp.Time)
			assert.Equal(t, 0., s.TotalWeight)
		})
	}
}

func TestBinaryDecayingHistogramCheckpointing(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			currentTime := startTime
			lastTime := startTime.AddDate(3, 0, 0)
			for currentTime.Before(lastTime) {
				seconds := currentTime.Unix()
				mb := float64(seconds%1000000) * 1024 * 1024
				h.AddSample(mb, 1.0, currentTime)
				s, err := h.SaveToChekpoint()
				assert.NoError(t, err)
				loaded := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
				loaded.LoadFromCheckpoint(s)
				assert.True(t, h.Equals(loaded))
				currentTime = currentTime.AddDate(0, 0, 1)
			}

		})
	}
}

func TestBinaryDecayingHistogramLoadFromDecayingHistogramCheckpoint(t *testing.T) {
	for _, retentionDays := range retentionsToTest {
		t.Run(fmt.Sprintf("retentionDays: %d", retentionDays), func(t *testing.T) {
			h := NewDecayingHistogram(testBinaryDecayingHistogramOptions, time.Hour*24)
			h.AddSample(v128Mib, 1.0, startTime)
			h.AddSample(v256Mib, 1.0, startTime.AddDate(0, 0, 1))
			s, err := h.SaveToChekpoint()
			assert.NoError(t, err)
			loaded := NewBinaryDecayingHistogram(testBinaryDecayingHistogramOptions, retentionDays)
			loaded.LoadFromCheckpoint(s)
			assert.InEpsilon(t, v256Mib, loaded.Percentile(1.0), v256MibEpsilon)
		})
	}
}
