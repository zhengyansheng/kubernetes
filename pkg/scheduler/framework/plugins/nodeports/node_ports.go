/*
Copyright 2019 The Kubernetes Authors.

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

package nodeports

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// NodePorts is a plugin that checks if a node has free ports for the requested pod ports.
// NodePorts 是一个插件，用于检查节点是否有空闲端口以供请求的 pod 使用。
type NodePorts struct{}

var _ framework.PreFilterPlugin = &NodePorts{}
var _ framework.FilterPlugin = &NodePorts{}
var _ framework.EnqueueExtensions = &NodePorts{}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	// Name 是插件在插件注册表和配置中使用的名称。
	Name = names.NodePorts

	// preFilterStateKey is the key in CycleState to NodePorts pre-computed data.
	// Using the name of the plugin will likely help us avoid collisions with other plugins.
	// preFilterStateKey 是 CycleState 中 NodePorts 预计算数据的键。
	// 使用插件的名称可能有助于我们避免与其他插件发生冲突。
	preFilterStateKey = "PreFilter" + Name

	// ErrReason when node ports aren't available.
	// 当节点端口不可用时的错误原因。
	ErrReason = "node(s) didn't have free ports for the requested pod ports"
)

type preFilterState []*v1.ContainerPort

// Clone the prefilter state.
func (s preFilterState) Clone() framework.StateData {
	// The state is not impacted by adding/removing existing pods, hence we don't need to make a deep copy.
	// 这个状态不受添加/删除现有 pod 的影响，因此我们不需要进行深度复制。
	return s
}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *NodePorts) Name() string {
	return Name
}

// getContainerPorts returns the used host ports of Pods: if 'port' was used, a 'port:true' pair
// will be in the result; but it does not resolve port conflict.
// getContainerPorts 返回 Pods 使用的主机端口：如果 'port' 被使用，结果中将有 'port:true' 对；但它不解决端口冲突。
func getContainerPorts(pods ...*v1.Pod) []*v1.ContainerPort {
	ports := []*v1.ContainerPort{}
	for _, pod := range pods {
		for j := range pod.Spec.Containers {
			container := &pod.Spec.Containers[j]
			for k := range container.Ports {
				ports = append(ports, &container.Ports[k])
			}
		}
	}
	return ports
}

// PreFilter invoked at the prefilter extension point.
func (pl *NodePorts) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	s := getContainerPorts(pod)
	// 保存 pod 的端口信息
	cycleState.Write(preFilterStateKey, preFilterState(s))
	return nil, nil
}

// PreFilterExtensions do not exist for this plugin.
// PreFilterExtensions 此插件没有预过滤器扩展。
func (pl *NodePorts) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func getPreFilterState(cycleState *framework.CycleState) (preFilterState, error) {
	// 读取保存的 pod 的端口信息
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		// preFilterState 不存在，可能没有调用 PreFilter。
		return nil, fmt.Errorf("reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to nodeports.preFilterState error", c)
	}
	return s, nil
}

// EventsToRegister returns the possible events that may make a Pod failed by this plugin schedulable.
// EventsToRegister 返回可能使 Pod 无法调度的事件。
func (pl *NodePorts) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		// Due to immutable fields `spec.containers[*].ports`, pod update events are ignored.
		{Resource: framework.Pod, ActionType: framework.Delete},
		{Resource: framework.Node, ActionType: framework.Add | framework.Update},
	}
}

// Filter invoked at the filter extension point.
func (pl *NodePorts) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	// 获取 pod 的端口信息
	wantPorts, err := getPreFilterState(cycleState)
	if err != nil {
		return framework.AsStatus(err)
	}

	// 检查节点是否有空闲端口
	fits := fitsPorts(wantPorts, nodeInfo)
	if !fits {
		return framework.NewStatus(framework.Unschedulable, ErrReason)
	}

	return nil
}

// Fits checks if the pod fits the node.
func Fits(pod *v1.Pod, nodeInfo *framework.NodeInfo) bool {
	return fitsPorts(getContainerPorts(pod), nodeInfo)
}

func fitsPorts(wantPorts []*v1.ContainerPort, nodeInfo *framework.NodeInfo) bool {
	// try to see whether existingPorts and wantPorts will conflict or not
	// 尝试查看 existingPorts 和 wantPorts 是否会发生冲突
	existingPorts := nodeInfo.UsedPorts
	for _, cp := range wantPorts {
		if existingPorts.CheckConflict(cp.HostIP, string(cp.Protocol), cp.HostPort) {
			return false
		}
	}
	return true
}

// New initializes a new plugin and returns it.
// New 初始化一个新的插件并返回它。
func New(_ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &NodePorts{}, nil
}
