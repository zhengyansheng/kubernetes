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

package noderesources

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// scorer is decorator for resourceAllocationScorer
type scorer func(args *config.NodeResourcesFitArgs) *resourceAllocationScorer

// resourceAllocationScorer contains information to calculate resource allocation score.
type resourceAllocationScorer struct {
	Name string
	// used to decide whether to use Requested or NonZeroRequested for cpu and memory.
	// 用于决定是使用请求的资源还是非零请求的资源。
	useRequested bool
	scorer       func(requested, allocable []int64) int64
	resources    []config.ResourceSpec
}

// score will use `scorer` function to calculate the score.
func (r *resourceAllocationScorer) score(pod *v1.Pod, nodeInfo *framework.NodeInfo) (int64, *framework.Status) {

	node := nodeInfo.Node()
	if node == nil {
		return 0, framework.NewStatus(framework.Error, "node not found")
	}
	// resources not set, nothing scheduled,
	// 资源未设置，没有安排任何东西，
	if len(r.resources) == 0 {
		return 0, framework.NewStatus(framework.Error, "resources not found")
	}
	klog.Infof("pod: %v, node: %v, resources: %+v", pod.Name, node.Name, r.resources)

	requested := make([]int64, len(r.resources))
	allocatable := make([]int64, len(r.resources))
	for i := range r.resources {
		alloc, req := r.calculateResourceAllocatableRequest(nodeInfo, pod, v1.ResourceName(r.resources[i].Name))
		// Only fill the extended resource entry when it's non-zero.
		// 仅在扩展资源不为零时填充扩展资源条目。
		if alloc == 0 {
			continue
		}
		klog.Infof("=====idx: %v, resource: %v, pod: %v, node: %v, allocatable: %d, requested: %d", i, v1.ResourceName(r.resources[i].Name), pod.Name, nodeInfo.Node().Name, alloc, req)
		allocatable[i] = alloc
		requested[i] = req
	}

	score := r.scorer(requested, allocatable) // balancedResourceScorer

	if klogV := klog.V(10); klogV.Enabled() { // Serializing these maps is costly. // 序列化这些映射是昂贵的。
		klogV.InfoS("Listing internal info for allocatable resources, requested resources and score", "pod",
			klog.KObj(pod), "node", klog.KObj(node), "resourceAllocationScorer", r.Name,
			"allocatableResource", allocatable, "requestedResource", requested, "resourceScore", score,
		)
	}

	return score, nil
}

// calculateResourceAllocatableRequest returns 2 parameters:
// - 1st param: quantity of allocatable resource on the node.
// - 2nd param: aggregated quantity of requested resource on the node.
// Note: if it's an extended resource, and the pod doesn't request it, (0, 0) is returned.
// calculateResourceAllocatableRequest返回2个参数：
// - 第一个参数：节点上可分配资源的数量。
// - 第二个参数：节点上请求资源的数量的总和。
// 注意：如果它是扩展资源，并且pod没有请求它，则返回(0, 0)。
func (r *resourceAllocationScorer) calculateResourceAllocatableRequest(nodeInfo *framework.NodeInfo, pod *v1.Pod, resource v1.ResourceName) (int64, int64) {
	requested := nodeInfo.NonZeroRequested
	if r.useRequested {
		requested = nodeInfo.Requested
	}

	podRequest := r.calculatePodResourceRequest(pod, resource)
	//klog.InfoS("->calculate ", "pod", pod.Name, "node", nodeInfo.Node().Name, "resource", resource.String(), "podRequest", podRequest)

	// If it's an extended resource, and the pod doesn't request it. We return (0, 0)
	// as an implication to bypass scoring on this resource.
	// 如果它是扩展资源，并且pod没有请求它。我们返回(0, 0)
	// 作为绕过此资源评分的暗示。
	if podRequest == 0 && schedutil.IsScalarResourceName(resource) {
		return 0, 0
	}
	/*
		nodeInfo.Allocatable.MilliCPU: 表示节点上可用的CPU资源，单位为毫核  计算公式 .status.allocatable * 1000
		requested.MilliCPU: 表示
	*/
	switch resource {
	case v1.ResourceCPU:
		klog.InfoS("--------->", "node", nodeInfo.Node().Name, "millicpu", nodeInfo.Allocatable.MilliCPU, "requested millcpu", requested.MilliCPU, "pod request", podRequest)
		return nodeInfo.Allocatable.MilliCPU, (requested.MilliCPU + podRequest)
	case v1.ResourceMemory:
		return nodeInfo.Allocatable.Memory, (requested.Memory + podRequest)
	case v1.ResourceEphemeralStorage: // EphemeralStorage 是一个扩展存储资源
		return nodeInfo.Allocatable.EphemeralStorage, (nodeInfo.Requested.EphemeralStorage + podRequest)
	default:
		if _, exists := nodeInfo.Allocatable.ScalarResources[resource]; exists {
			return nodeInfo.Allocatable.ScalarResources[resource], (nodeInfo.Requested.ScalarResources[resource] + podRequest)
		}
	}
	klog.V(10).InfoS("Requested resource is omitted for node score calculation", "resourceName", resource)
	return 0, 0
}

// calculatePodResourceRequest returns the total non-zero requests. If Overhead is defined for the pod
// the Overhead is added to the result.
// podResourceRequest = max(sum(podSpec.Containers), podSpec.InitContainers) + overHead
// calculatePodResourceRequest 返回总的非零请求。如果为pod定义了Overhead
// 则将Overhead添加到结果中。
// podResourceRequest = max(sum(podSpec.Containers), podSpec.InitContainers) + overHead
func (r *resourceAllocationScorer) calculatePodResourceRequest(pod *v1.Pod, resource v1.ResourceName) int64 {
	var podRequest int64
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		value := schedutil.GetRequestForResource(resource, &container.Resources.Requests, !r.useRequested)
		//klog.Infof("idx: %v, resource: %v, pod: %v, container: %v, requested: %d， useRequested: %v", i, resource, pod.Name, container.Name, value, !r.useRequested)
		podRequest += value
	}

	for i := range pod.Spec.InitContainers {
		initContainer := &pod.Spec.InitContainers[i]
		value := schedutil.GetRequestForResource(resource, &initContainer.Resources.Requests, !r.useRequested)
		if podRequest < value {
			podRequest = value
		}
	}

	// If Overhead is being utilized, add to the total requests for the pod
	// 如果正在使用Overhead，则将其添加到pod的总请求中
	if pod.Spec.Overhead != nil {
		if quantity, found := pod.Spec.Overhead[resource]; found {
			podRequest += quantity.Value()
		}
	}

	return podRequest
}
