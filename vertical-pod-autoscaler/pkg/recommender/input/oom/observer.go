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

package oom

import (
	"strings"
	"time"
	"unicode"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"
)

// OomInfo contains data of the OOM event occurrence
type OomInfo struct {
	Timestamp   time.Time
	Memory      model.ResourceAmount
	Resource    model.ResourceName
	ContainerID model.ContainerID
}

// Observer can observe pod resource update and collect OOM events.
type Observer interface {
	GetObservedOomsChannel() chan OomInfo
	OnEvent(*apiv1.Event)
	cache.ResourceEventHandler
}

// observer can observe pod resource update and collect OOM events.
type observer struct {
	observedOomsChannel chan OomInfo
}

// NewObserver returns new instance of the observer.
func NewObserver() *observer {
	return &observer{
		observedOomsChannel: make(chan OomInfo, 5000),
	}
}

func (o *observer) GetObservedOomsChannel() chan OomInfo {
	return o.observedOomsChannel
}

func parseEvictionEvent(event *apiv1.Event) []OomInfo {
	if event.Reason != "Evicted" ||
		event.InvolvedObject.Kind != "Pod" {
		return []OomInfo{}
	}
	extractArray := func(annotationsKey string) []string {
		str, found := event.Annotations[annotationsKey]
		if !found {
			return []string{}
		}
		return strings.Split(str, ",")
	}
	offendingContainers := extractArray("offending_containers")
	offendingContainersUsage := extractArray("offending_containers_usage")
	starvedResource := extractArray("starved_resource")
	if len(offendingContainers) != len(offendingContainersUsage) ||
		len(offendingContainers) != len(starvedResource) {
		return []OomInfo{}
	}

	result := make([]OomInfo, 0, len(offendingContainers))

	for i, container := range offendingContainers {
		if starvedResource[i] != "memory" {
			continue
		}
		memory, err := resource.ParseQuantity(offendingContainersUsage[i])
		if err != nil {
			klog.Errorf("Cannot parse resource quantity in eviction event %v. Error: %v", offendingContainersUsage[i], err)
			continue
		}
		oomInfo := OomInfo{
			Timestamp: event.CreationTimestamp.Time.UTC(),
			Memory:    model.ResourceAmount(memory.Value()),
			Resource:  model.ResourceMemory,
			ContainerID: model.ContainerID{
				PodID: model.PodID{
					Namespace: event.InvolvedObject.Namespace,
					PodName:   event.InvolvedObject.Name,
				},
				ContainerName: container,
			},
		}
		result = append(result, oomInfo)
	}
	return result
}

// OnEvent inspects Node MemoryPressure k8s eviction events and translates them to OomInfo.
func (o *observer) OnEvent(event *apiv1.Event) {
	klog.V(1).Infof("OOM Observer processing event: %+v", event)
	for _, oomInfo := range parseEvictionEvent(event) {
		o.observedOomsChannel <- oomInfo
	}
}

func findStatus(name string, containerStatuses []apiv1.ContainerStatus) *apiv1.ContainerStatus {
	for _, containerStatus := range containerStatuses {
		if containerStatus.Name == name {
			return &containerStatus
		}
	}
	return nil
}

func findSpec(name string, containers []apiv1.Container) *apiv1.Container {
	for _, containerSpec := range containers {
		if containerSpec.Name == name {
			return &containerSpec
		}
	}
	return nil
}

func findEnvVar(name string, envVars []apiv1.EnvVar) *apiv1.EnvVar {
	for _, envVar := range envVars {
		if envVar.Name == name {
			return &envVar
		}
	}
	return nil
}

func findContainerOverrideJvmHeapSizeEnv(envVars []apiv1.EnvVar) *resource.Quantity {
	for _, envVar := range envVars {
		if envVar.Name == "OVERRIDE_JVM_HEAP_SIZE" {
			jvmHeapSize, err := resource.ParseQuantity(envVar.Value)
			jvmHeapSizeStr := envVar.Value
			if jvmHeapSizeStr == "" {
				return nil
			}

			// JVM heap size is specified as k/K to indicate KiB, m/M to indicate MiB, g/G to indicate GiB.
			// https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html > -Xmx size
			if unicode.IsLetter(rune(jvmHeapSizeStr[len(jvmHeapSizeStr)-1])) {
				// If not a raw byte value, must capitalize and then add an 'i' to indicate binary units.
				jvmHeapSizeStr = strings.ToUpper(jvmHeapSizeStr) + "i"
			}

			jvmHeapSize, err = resource.ParseQuantity(jvmHeapSizeStr)
			if err != nil {
				return nil
			}
			return &jvmHeapSize
		}
	}
	return nil
}

// OnAdd is Noop
func (*observer) OnAdd(obj interface{}) {}

// OnUpdate inspects if the update contains oom information and
// passess it to the ObservedOomsChannel
func (o *observer) OnUpdate(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*apiv1.Pod)
	if !ok {
		klog.Errorf("OOM observer received invalid oldObj: %v", oldObj)
	}
	newPod, ok := newObj.(*apiv1.Pod)
	if !ok {
		klog.Errorf("OOM observer received invalid newObj: %v", newObj)
	}

	for _, containerStatus := range newPod.Status.ContainerStatuses {
		if containerStatus.RestartCount > 0 &&
			containerStatus.LastTerminationState.Terminated != nil {
			if containerStatus.LastTerminationState.Terminated.Reason == "OOMKilled" {

				oldStatus := findStatus(containerStatus.Name, oldPod.Status.ContainerStatuses)
				if oldStatus != nil && containerStatus.RestartCount > oldStatus.RestartCount {
					oldSpec := findSpec(containerStatus.Name, oldPod.Spec.Containers)
					if oldSpec != nil {
						// Artificial memory sample is created based on the memory request.
						memory := oldSpec.Resources.Requests[apiv1.ResourceMemory]
						oomInfo := OomInfo{
							Timestamp: containerStatus.LastTerminationState.Terminated.FinishedAt.Time.UTC(),
							Memory:    model.ResourceAmount(memory.Value()),
							Resource:  model.ResourceMemory,
							ContainerID: model.ContainerID{
								PodID: model.PodID{
									Namespace: newPod.ObjectMeta.Namespace,
									PodName:   newPod.ObjectMeta.Name,
								},
								ContainerName: containerStatus.Name,
							},
						}
						o.observedOomsChannel <- oomInfo

						// Artificial RSS sample is created based on the memory limit.
						// The generated RSS recommendation will then be higher than the memory limit because the
						// RSS binary decaying histogram recommends the max RSS observed in some past period.
						memoryLimit := oldSpec.Resources.Limits[apiv1.ResourceMemory]
						oomInfoRSS := OomInfo{
							Timestamp: containerStatus.LastTerminationState.Terminated.FinishedAt.Time.UTC(),
							Memory:    model.ResourceAmount(memoryLimit.Value()),
							Resource:  model.ResourceRSS,
							ContainerID: model.ContainerID{
								PodID: model.PodID{
									Namespace: newPod.ObjectMeta.Namespace,
									PodName:   newPod.ObjectMeta.Name,
								},
								ContainerName: containerStatus.Name,
							},
						}
						o.observedOomsChannel <- oomInfoRSS
					}
				}
			} else if containerStatus.LastTerminationState.Terminated.Reason == "JVM Heap OOM" {

				oldStatus := findStatus(containerStatus.Name, oldPod.Status.ContainerStatuses)
				if oldStatus != nil && containerStatus.RestartCount > oldStatus.RestartCount {
					oldSpec := findSpec(containerStatus.Name, oldPod.Spec.Containers)
					if oldSpec != nil {
						jvmHeapSize := findContainerOverrideJvmHeapSizeEnv(oldSpec.Env)
						if jvmHeapSize == nil {
							klog.Errorf("OOM observer received JVM OOM event without JVM Heap Size override: %v", oldSpec.Env)
							continue
						}
						oomInfoJVMHeapComitted := OomInfo{
							Timestamp: containerStatus.LastTerminationState.Terminated.FinishedAt.Time.UTC(),
							Memory:    model.ResourceAmount(jvmHeapSize.Value()),
							Resource:  model.ResourceJVMHeapCommitted,
							ContainerID: model.ContainerID{
								PodID: model.PodID{
									Namespace: newPod.ObjectMeta.Namespace,
									PodName:   newPod.ObjectMeta.Name,
								},
								ContainerName: containerStatus.Name,
							},
						}
						o.observedOomsChannel <- oomInfoJVMHeapComitted
					}
				}
			}
		}
	}
}

// OnDelete is Noop
func (*observer) OnDelete(obj interface{}) {}
