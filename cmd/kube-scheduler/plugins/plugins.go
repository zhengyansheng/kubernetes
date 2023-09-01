package plugins

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Name 插件名称
const Name = "sample-scheduler"

type MyPlugin struct {
	handle framework.Handle
}

// Name 实现 Plugin 插件，即实现 Name 方法
func (s *MyPlugin) Name() string {
	return Name
}

func (s *MyPlugin) PreFilter(f *framework.Plugin, pod *v1.Pod) *framework.Status {
	klog.Infof("prefilter pod: %v", pod.Name)
	return framework.NewStatus(framework.Success, "")
}

// Filter 3. 实现 Filter 函数
func (s *MyPlugin) Filter(f *framework.Plugin, pod *v1.Pod, nodeName string) *framework.Status {
	klog.Infof("filter pod: %v, node: %v", pod.Name, nodeName)
	return framework.NewStatus(framework.Success, "")
}

func (s *MyPlugin) PreBind(f *framework.Plugin, pod *v1.Pod, nodeName string) *framework.Status {
	nodeInfo, err := s.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("prebind get node info error: %+v", nodeName))
	}
	klog.Infof("pre bind node info: %+v", nodeInfo.Node())
	return framework.NewStatus(framework.Success, "")
}

// New 实现 New 函数，返回该自定义插件对象，类似下面代码
func New(configuration runtime.Object, f framework.Handle) (framework.Plugin, error) {
	return &MyPlugin{}, nil
}
