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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func main() {
	// Read VPA checkpoint JSON from standard input
	checkpointData, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("Error reading from stdin: %v", err)
	}
	// Parse the JSON into a VPA checkpoint structure
	var checkpoint vpa_types.VerticalPodAutoscalerCheckpoint
	err = json.Unmarshal(checkpointData, &checkpoint)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	err = input.SetNumBucketsFromAnnotations(&checkpoint)
	if err != nil {
		log.Fatalf("Error loading from checkpoint: %v", err)
	}

	// Create a new AggregateContainerState
	aggregateState := model.NewAggregateContainerState()
	// Load data into the AggregateContainerState using LoadFromCheckpoint
	err = aggregateState.LoadFromCheckpoint(&checkpoint.Status)
	if checkpoint.Namespace == "vpa-test-service" {
		layout := "2006-01-02 15:04:05.999999999 -0700 MST"
		parsedTime, err := time.Parse(layout, checkpoint.Annotations["LastSampleStart"])
		if err != nil {
			fmt.Printf("Error parsing time: %v\n", err)
		}
		aggregateState.LastSampleStart = parsedTime
		parsedTime, err = time.Parse(layout, checkpoint.Annotations["LastMemorySampleStart"])
		aggregateState.LastMemorySampleStart = parsedTime
		parsedTime, err = time.Parse(layout, checkpoint.Annotations["LastRSSSampleStart"])
		aggregateState.LastRSSSampleStart = parsedTime
		parsedTime, err = time.Parse(layout, checkpoint.Annotations["LastJVMHeapCommittedSampleStart"])
		aggregateState.LastJVMHeapCommittedSampleStart = parsedTime
		if err != nil {
			fmt.Printf("Error parsing time: %v\n", err)
		}
	}
	if err != nil {
		log.Fatalf("Error loading from checkpoint: %v", err)
	}

	fmt.Printf("CPU histogram:\n%v\n", aggregateState.AggregateCPUUsage)
	fmt.Printf("\nRSS Usage:\n%v\n", aggregateState.AggregateRSSPeaks)
	fmt.Printf("\nJVM Heap Committed Usage\n%v\n", aggregateState.AggregateJVMHeapCommittedPeaks)
}
