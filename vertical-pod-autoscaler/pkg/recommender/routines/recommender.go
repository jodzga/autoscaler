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

package routines

import (
	"context"
	"flag"
	"time"

	"k8s.io/klog/v2"

	vpa_api "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/typed/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/checkpoint"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/controller_fetcher"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	metrics_recommender "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/metrics/recommender"
	vpa_utils "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

var (
	checkpointsWriteTimeout = flag.Duration("checkpoints-timeout", 2 * time.Minute, `Timeout for writing checkpoints since the start of the recommender's main loop`)
	minCheckpointsPerRun    = flag.Int("min-checkpoints", 10, "Minimum number of checkpoints to write per recommender's main loop")
)

// Recommender recommend resources for certain containers, based on utilization periodically got from metrics api.
type Recommender interface {
	// RunOnce performs one iteration of recommender duties followed by update of recommendations in VPA objects.
	RunOnce()
	// GetClusterState returns ClusterState used by Recommender
	GetClusterState() *model.ClusterState
	// GetClusterStateFeeder returns ClusterStateFeeder used by Recommender
	GetClusterStateFeeder() input.ClusterStateFeeder
	// UpdateVPAs computes recommendations and sends VPAs status updates to API Server
	UpdateVPAs()
	// MaintainCheckpoints stores current checkpoints in API Server and garbage collect old ones
	// MaintainCheckpoints writes at least minCheckpoints if there are more checkpoints to write.
	// Checkpoints are written until ctx permits or all checkpoints are written.
	MaintainCheckpoints(ctx context.Context, minCheckpoints int)
}

type recommender struct {
	clusterState                  *model.ClusterState
	clusterStateFeeder            input.ClusterStateFeeder
	checkpointWriter              checkpoint.CheckpointWriter
	checkpointsGCInterval         time.Duration
	controllerFetcher             controllerfetcher.ControllerFetcher
	lastCheckpointGC              time.Time
	vpaClient                     vpa_api.VerticalPodAutoscalersGetter
	podResourceRecommender        logic.PodResourceRecommender
	useCheckpoints                bool
	lastAggregateContainerStateGC time.Time
	recommendationPostProcessor   []RecommendationPostProcessor
}

func (r *recommender) GetClusterState() *model.ClusterState {
	return r.clusterState
}

func (r *recommender) GetClusterStateFeeder() input.ClusterStateFeeder {
	return r.clusterStateFeeder
}

// Updates VPA CRD objects' statuses.
func (r *recommender) UpdateVPAs() {
	cnt := metrics_recommender.NewObjectCounter()
	defer cnt.Observe()

	for _, observedVpa := range r.clusterState.ObservedVpas {
		key := model.VpaID{
			Namespace: observedVpa.Namespace,
			VpaName:   observedVpa.Name,
		}

		vpa, found := r.clusterState.Vpas[key]
		if !found {
			continue
		}
		resources := r.podResourceRecommender.GetRecommendedPodResources(GetContainerNameToAggregateStateMap(vpa))
		had := vpa.HasRecommendation()

		listOfResourceRecommendation := logic.MapToListOfRecommendedContainerResources(resources)

		for _, postProcessor := range r.recommendationPostProcessor {
			listOfResourceRecommendation = postProcessor.Process(observedVpa, listOfResourceRecommendation)
		}

		vpa.UpdateRecommendation(listOfResourceRecommendation)
		if vpa.HasRecommendation() && !had {
			metrics_recommender.ObserveRecommendationLatency(vpa.Created)
		}
		hasMatchingPods := vpa.PodCount > 0
		vpa.UpdateConditions(hasMatchingPods)
		if err := r.clusterState.RecordRecommendation(vpa, time.Now()); err != nil {
			klog.Warningf("%v", err)
			if klog.V(4).Enabled() {
				klog.Infof("VPA dump")
				klog.Infof("%+v", vpa)
				klog.Infof("HasMatchingPods: %v", hasMatchingPods)
				klog.Infof("PodCount: %v", vpa.PodCount)
				pods := r.clusterState.GetMatchingPods(vpa)
				klog.Infof("MatchingPods: %+v", pods)
				if len(pods) != vpa.PodCount {
					klog.Errorf("ClusterState pod count and matching pods disagree for vpa %v/%v", vpa.ID.Namespace, vpa.ID.VpaName)
				}
			}
		}
		cnt.Add(vpa)

		_, err := vpa_utils.UpdateVpaStatusIfNeeded(
			r.vpaClient.VerticalPodAutoscalers(vpa.ID.Namespace), vpa.ID.VpaName, vpa.AsStatus(), &observedVpa.Status)
		if err != nil {
			klog.Errorf(
				"Cannot update VPA %v/%v object. Reason: %+v", vpa.ID.Namespace, vpa.ID.VpaName, err)
		}
	}
}

func (r *recommender) MaintainCheckpoints(ctx context.Context, minCheckpointsPerRun int) {
	now := time.Now()
	if r.useCheckpoints {
		if err := r.checkpointWriter.StoreCheckpoints(ctx, now, minCheckpointsPerRun); err != nil {
			klog.Warningf("Failed to store checkpoints. Reason: %+v", err)
		}
		if time.Since(r.lastCheckpointGC) > r.checkpointsGCInterval {
			r.lastCheckpointGC = now
			r.clusterStateFeeder.GarbageCollectCheckpoints()
		}
	}
}

func (r *recommender) RunOnce() {
	timer := metrics_recommender.NewExecutionTimer()
	defer timer.ObserveTotal()

	ctx := context.Background()
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(*checkpointsWriteTimeout))
	defer cancelFunc()

	klog.V(3).Infof("Recommender Run")

	r.clusterStateFeeder.LoadVPAs()
	timer.ObserveStep("LoadVPAs")

	r.clusterStateFeeder.LoadPods()
	timer.ObserveStep("LoadPods")

	r.clusterStateFeeder.LoadRealTimeMetrics()
	timer.ObserveStep("LoadMetrics")
	klog.V(3).Infof("ClusterState is tracking %v PodStates and %v VPAs", len(r.clusterState.Pods), len(r.clusterState.Vpas))

	for _, vpa := range r.clusterState.Vpas {
		if vpa.ID.Namespace == "vpa-test-service" {
		aggregateContainerStateMap := buildAggregateContainerStateMap(vpa, r.clusterState, time.Now())
		klog.Infof("HELLO after load metrics aggregateContainerStateMap %+v", aggregateContainerStateMap["vpa-test-service"].AggregateRSSPeaks)
		klog.Infof("HELLO after load metrics vpacheckpoint %+v", aggregateContainerStateMap["vpa-test-service"].AggregateRSSPeaks.SaveToChekpoint())
		break
		}
	}

	r.UpdateVPAs()
	timer.ObserveStep("UpdateVPAs")
	for _, vpa := range r.clusterState.Vpas {
		if vpa.ID.Namespace == "vpa-test-service" {
		aggregateContainerStateMap := buildAggregateContainerStateMap(vpa, r.clusterState, time.Now())
		klog.Infof("HELLO after update vpas aggregateContainerStateMap %+v", aggregateContainerStateMap["vpa-test-service"].AggregateRSSPeaks)
		klog.Infof("HELLO after update vpas vpacheckpoint %+v", aggregateContainerStateMap["vpa-test-service"].AggregateRSSPeaks.SaveToChekpoint())
		break
		}
	}

	timer.ObserveStep("MaintainCheckpoints")
	for _, vpa := range r.clusterState.Vpas {
		if vpa.ID.Namespace == "vpa-test-service" {
		aggregateContainerStateMap := buildAggregateContainerStateMap(vpa, r.clusterState, time.Now())
		klog.Infof("HELLO after maintain checkpoints aggregateContainerStateMap %+v", aggregateContainerStateMap["vpa-test-service"].AggregateRSSPeaks)
		klog.Infof("HELLO after maintain checkpoints vpacheckpoint %+v", aggregateContainerStateMap["vpa-test-service"].AggregateRSSPeaks.SaveToChekpoint())
		break
		}
	}

	r.clusterState.RateLimitedGarbageCollectAggregateCollectionStates(time.Now(), r.controllerFetcher)
	timer.ObserveStep("GarbageCollect")
	klog.V(3).Infof("ClusterState is tracking %d aggregated container states", r.clusterState.StateMapSize())
}


func buildAggregateContainerStateMap(vpa *model.Vpa, cluster *model.ClusterState, now time.Time) map[string]*model.AggregateContainerState {
	aggregateContainerStateMap := vpa.AggregateStateByContainerName()
	// Note: the memory peak from the current (ongoing) aggregation interval is not included in the
	// checkpoint to avoid having multiple peaks in the same interval after the state is restored from
	// the checkpoint. Therefore we are extracting the current peak from all containers.
	// TODO: Avoid the nested loop over all containers for each VPA.
	for _, pod := range cluster.Pods {
		for containerName, container := range pod.Containers {
			aggregateKey := cluster.MakeAggregateStateKey(pod, containerName)
			if vpa.UsesAggregation(aggregateKey) {
				if aggregateContainerState, exists := aggregateContainerStateMap[containerName]; exists {
					subtractCurrentContainerMemoryPeak(aggregateContainerState, container, now)
				}
			}
		}
	}
	return aggregateContainerStateMap
}

func subtractCurrentContainerMemoryPeak(a *model.AggregateContainerState, container *model.ContainerState, now time.Time) {
	if now.Before(container.WindowEnd) {
		a.AggregateMemoryPeaks.SubtractSample(model.BytesFromMemoryAmount(container.GetMaxMemoryPeak()), 1.0, container.WindowEnd)
		a.AggregateRSSPeaks.SubtractSample(model.BytesFromMemoryAmount(container.GetMaxRSSPeak()), 1.0, container.WindowEnd)
		a.AggregateJVMHeapCommittedPeaks.SubtractSample(model.BytesFromMemoryAmount(container.GetMaxJVMHeapCommittedPeak()), 1.0, container.WindowEnd)
	}
}

// RecommenderFactory makes instances of Recommender.
type RecommenderFactory struct {
	ClusterState *model.ClusterState

	ClusterStateFeeder     input.ClusterStateFeeder
	ControllerFetcher      controllerfetcher.ControllerFetcher
	CheckpointWriter       checkpoint.CheckpointWriter
	PodResourceRecommender logic.PodResourceRecommender
	VpaClient              vpa_api.VerticalPodAutoscalersGetter

	RecommendationPostProcessors []RecommendationPostProcessor

	CheckpointsGCInterval time.Duration
	UseCheckpoints        bool
}

// Make creates a new recommender instance,
// which can be run in order to provide continuous resource recommendations for containers.
func (c RecommenderFactory) Make() Recommender {
	recommender := &recommender{
		clusterState:                  c.ClusterState,
		clusterStateFeeder:            c.ClusterStateFeeder,
		checkpointWriter:              c.CheckpointWriter,
		checkpointsGCInterval:         c.CheckpointsGCInterval,
		controllerFetcher:             c.ControllerFetcher,
		useCheckpoints:                c.UseCheckpoints,
		vpaClient:                     c.VpaClient,
		podResourceRecommender:        c.PodResourceRecommender,
		recommendationPostProcessor:   c.RecommendationPostProcessors,
		lastAggregateContainerStateGC: time.Now(),
		lastCheckpointGC:              time.Now(),
	}
	klog.V(3).Infof("New Recommender created %+v", recommender)
	return recommender
}
