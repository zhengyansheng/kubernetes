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

package imagelocality

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// The two thresholds are used as bounds for the image score range.
// They correspond to a reasonable size range for container images compressed and stored in registries;
// 90%ile of images on dockerhub drops into this range.
// 这2个阈值用作镜像分数范围的边界。
// 它们对应于压缩并存储在注册表中的容器镜像的合理大小范围；
// dockerhub上90%的镜像都在这个范围内。
const (
	mb                    int64 = 1024 * 1024
	minThreshold          int64 = 23 * mb
	maxContainerThreshold int64 = 1000 * mb
)

// ImageLocality is a score plugin that favors nodes that already have requested pod container's images.
type ImageLocality struct {
	handle framework.Handle
}

var _ framework.ScorePlugin = &ImageLocality{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.ImageLocality

// Name returns name of the plugin. It is used in logs, etc.
func (pl *ImageLocality) Name() string {
	return Name
}

// Score invoked at the score extension point.
// Score favors nodes that already have requested pod container's images.
func (pl *ImageLocality) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	// 从快照中查找 node
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("getting node %q from Snapshot: %w", nodeName, err))
	}

	//for key, image := range nodeInfo.ImageStates {
	//	klog.Infof("--->name: %v, key: %v, size: %v, nodes: %+v", nodeInfo.Node().Name, key, image.Size, image.NumNodes)
	//}

	// 从快照中获取所有的node
	nodeInfos, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, framework.AsStatus(err)
	}
	totalNumNodes := len(nodeInfos)

	// 计算分数
	// sumImageScores: 计算pod中多个容器的镜像大小的和
	sumScores := sumImageScores(nodeInfo, pod.Spec.Containers, totalNumNodes)
	klog.Infof("----->Pod %s/%s sumScores: %d", pod.Namespace, pod.Name, sumScores)
	score := calculatePriority(sumScores, len(pod.Spec.Containers))
	//score := calculatePriority(sumImageScores(nodeInfo, pod.Spec.Containers, totalNumNodes), len(pod.Spec.Containers))

	return score, nil
}

// ScoreExtensions of the Score plugin.
func (pl *ImageLocality) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &ImageLocality{handle: h}, nil
}

// calculatePriority returns the priority of a node.
// Given the sumScores of requested images on the node,
// the node's priority is obtained by scaling the maximum priority value with a ratio proportional to the sumScores.
func calculatePriority(sumScores int64, numContainers int) int64 {
	// maxThreshold =（ 1000 * 1024 * 1024 ） * 1
	maxThreshold := maxContainerThreshold * int64(numContainers)
	if sumScores < minThreshold { // 23 * 1024 * 1024
		sumScores = minThreshold // 如果sumScores < 23M 那么得分就直接为0 {sumScores - minThreshold <0}
	} else if sumScores > maxThreshold { // 1000 * containerNum * 1024 * 1024
		sumScores = maxThreshold
	}

	//
	return int64(framework.MaxNodeScore) * (sumScores - minThreshold) / (maxThreshold - minThreshold)
}

// sumImageScores returns the sum of image scores of all the containers that are already on the node.
// Each image receives a raw score of its size, scaled by scaledImageScore. The raw scores are later used to calculate
// the final score. Note that the init containers are not considered for it's rare for users to deploy huge init containers.
// 求一个pod中多个容器镜像大小的和
func sumImageScores(nodeInfo *framework.NodeInfo, containers []v1.Container, totalNumNodes int) int64 {
	/*
	   - docker.io/library/nginx:stable-perl
	   sizeBytes: 79205939
	*/
	var sum int64
	for _, container := range containers {
		// normalizedImageName -> xx/xx/xx:{version}
		state, ok := nodeInfo.ImageStates[normalizedImageName(container.Image)]
		if ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
		klog.Infof("state: %+v, image: %v, ok: %v", *state, normalizedImageName(container.Image), ok)
	}
	return sum
}

// scaledImageScore returns an adaptively scaled score for the given state of an image.
// The size of the image is used as the base score, scaled by a factor which considers how much nodes the image has "spread" to.
// This heuristic aims to mitigate the undesirable "node heating problem", i.e., pods get assigned to the same or
// a few nodes due to image locality.
func scaledImageScore(imageState *framework.ImageStateSummary, totalNumNodes int) int64 {
	//klog.Infof("NumNodes: %v totalNumNodes: %v", imageState.NumNodes, float64(totalNumNodes))
	spread := float64(imageState.NumNodes) / float64(totalNumNodes)
	return int64(float64(imageState.Size) * spread)
}

// normalizedImageName returns the CRI compliant name for a given image.
// TODO: cover the corner cases of missed matches, e.g,
// 1. Using Docker as runtime and docker.io/library/test:tag in pod spec, but only test:tag will present in node status
// 2. Using the implicit registry, i.e., test:tag or library/test:tag in pod spec but only docker.io/library/test:tag
// in node status; note that if users consistently use one registry format, this should not happen.
// 如果没有指定tag,则默认为latest，否则直接返回
func normalizedImageName(name string) string {
	if strings.LastIndex(name, ":") <= strings.LastIndex(name, "/") {
		name = name + ":latest"
	}
	return name
}
