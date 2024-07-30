package routines

import (
	"context"
	"testing"
	"time"

	vpa_api "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/typed/autoscaling.k8s.io/v1"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/controller_fetcher"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/history"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// Mock implementations of dependencies
type mockClusterStateFeeder struct{}

func (m *mockClusterStateFeeder) LoadVPAs()                                                       {}
func (m *mockClusterStateFeeder) LoadPods()                                                       {}
func (m *mockClusterStateFeeder) LoadRealTimeMetrics()                                            {}
func (m *mockClusterStateFeeder) GarbageCollectCheckpoints()                                      {}
func (m *mockClusterStateFeeder) InitFromCheckpoints()                                            {}
func (m *mockClusterStateFeeder) InitFromHistoryProvider(historyProvider history.HistoryProvider) {}

type mockCheckpointWriter struct{}

func (m *mockCheckpointWriter) StoreCheckpoints(ctx context.Context, now time.Time, minCheckpoints int) error {
	return nil
}

type mockControllerFetcher struct{}

func (m *mockControllerFetcher) FindTopMostWellKnownOrScalable(controller *controllerfetcher.ControllerKeyWithAPIVersion) (*controllerfetcher.ControllerKeyWithAPIVersion, error) {
	return nil, nil
}

type mockVPAClient struct{}

func (m *mockVPAClient) VerticalPodAutoscalers(namespace string) vpa_api.VerticalPodAutoscalerInterface {
	return nil
}

func TestRunOnce(t *testing.T) {
	// Create a mock recommender with stubbed dependencies
	mockRecommender := &recommender{
		clusterState:                  model.NewClusterState(1 * time.Hour), // 1hour constant taken from main.go
		clusterStateFeeder:            &mockClusterStateFeeder{},
		checkpointWriter:              &mockCheckpointWriter{},
		checkpointsGCInterval:         10 * time.Minute, // 10min constant taken from main.go
		controllerFetcher:             &mockControllerFetcher{},
		lastCheckpointGC:              time.Now(),
		vpaClient:                     &mockVPAClient{},
		podResourceRecommender:        logic.CreatePodResourceRecommender(),
		useCheckpoints:                true,
		lastAggregateContainerStateGC: time.Now(),
		recommendationPostProcessor:   []RecommendationPostProcessor{},
	}

	// Run the RunOnce function
	mockRecommender.RunOnce()

}
