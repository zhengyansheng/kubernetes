/*
Copyright 2014 The Kubernetes Authors.

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

package config

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/pkg/util/config"
)

// PodConfigNotificationMode describes how changes are sent to the update channel.
type PodConfigNotificationMode int

const (
	// PodConfigNotificationUnknown is the default value for
	// PodConfigNotificationMode when uninitialized.
	// PodConfigNotificationUnknown 是未初始化时 PodConfigNotificationMode 的默认值。
	PodConfigNotificationUnknown PodConfigNotificationMode = iota
	// PodConfigNotificationSnapshot delivers the full configuration as a SET whenever
	// any change occurs.
	// PodConfigNotificationSnapshot 在发生任何更改时将完整配置作为 SET 传递。
	PodConfigNotificationSnapshot
	// PodConfigNotificationSnapshotAndUpdates delivers an UPDATE and DELETE message whenever pods are
	// changed, and a SET message if there are any additions or removals.
	// PodConfigNotificationSnapshotAndUpdates 在更改 pod 时传递 UPDATE 和 DELETE 消息，如果有任何添加或删除，则传递 SET 消息。
	PodConfigNotificationSnapshotAndUpdates
	// PodConfigNotificationIncremental delivers ADD, UPDATE, DELETE, REMOVE, RECONCILE to the update channel.
	// PodConfigNotificationIncremental 将 ADD、UPDATE、DELETE、REMOVE、RECONCILE 传递到更新通道。
	PodConfigNotificationIncremental
)

type podStartupSLIObserver interface {
	ObservedPodOnWatch(pod *v1.Pod, when time.Time)
}

// PodConfig is a configuration mux that merges many sources of pod configuration into a single
// consistent structure, and then delivers incremental change notifications to listeners
// in order.
type PodConfig struct {
	pods *podStorage
	mux  *config.Mux

	// the channel of denormalized changes passed to listeners
	updates chan kubetypes.PodUpdate

	// contains the list of all configured sources
	sourcesLock sync.Mutex
	// sources 是一个字符串集合，包含所有配置的源
	sources sets.String
}

// NewPodConfig creates an object that can merge many configuration sources into a stream
// of normalized updates to a pod configuration.
func NewPodConfig(mode PodConfigNotificationMode, recorder record.EventRecorder, startupSLIObserver podStartupSLIObserver) *PodConfig {
	updates := make(chan kubetypes.PodUpdate, 50)
	storage := newPodStorage(updates, mode, recorder, startupSLIObserver)
	podConfig := &PodConfig{
		pods:    storage,
		mux:     config.NewMux(storage),
		updates: updates,
		sources: sets.String{},
	}
	return podConfig
}

// Channel creates or returns a config source channel.  The channel only accepts PodUpdates
// Channel创建或返回一个配置源通道。 该通道仅接受PodUpdates
func (c *PodConfig) Channel(ctx context.Context, source string) chan<- interface{} {
	c.sourcesLock.Lock()
	defer c.sourcesLock.Unlock()
	c.sources.Insert(source)
	return c.mux.ChannelWithContext(ctx, source)
}

// SeenAllSources returns true if seenSources contains all sources in the
// config, and also this config has received a SET message from each source.
func (c *PodConfig) SeenAllSources(seenSources sets.String) bool {
	if c.pods == nil {
		return false
	}
	c.sourcesLock.Lock()
	defer c.sourcesLock.Unlock()
	klog.V(5).InfoS("Looking for sources, have seen", "sources", c.sources.List(), "seenSources", seenSources)
	return seenSources.HasAll(c.sources.List()...) && c.pods.seenSources(c.sources.List()...)
}

// Updates returns a channel of updates to the configuration, properly denormalized.
func (c *PodConfig) Updates() <-chan kubetypes.PodUpdate {
	return c.updates
}

// Sync requests the full configuration be delivered to the update channel.
func (c *PodConfig) Sync() {
	c.pods.Sync()
}

// podStorage manages the current pod state at any point in time and ensures updates
// to the channel are delivered in order.  Note that this object is an in-memory source of
// "truth" and on creation contains zero entries.  Once all previously read sources are
// available, then this object should be considered authoritative.
type podStorage struct {
	podLock sync.RWMutex
	// map of source name to pod uid to pod reference
	// 从源名称到pod uid到pod引用的映射
	pods map[string]map[types.UID]*v1.Pod
	mode PodConfigNotificationMode

	// ensures that updates are delivered in strict order
	// on the updates channel
	updateLock sync.Mutex
	updates    chan<- kubetypes.PodUpdate

	// contains the set of all sources that have sent at least one SET
	sourcesSeenLock sync.RWMutex
	sourcesSeen     sets.String

	// the EventRecorder to use
	recorder record.EventRecorder

	startupSLIObserver podStartupSLIObserver
}

// TODO: PodConfigNotificationMode could be handled by a listener to the updates channel
// in the future, especially with multiple listeners.
// TODO: allow initialization of the current state of the store with snapshotted version.
func newPodStorage(updates chan<- kubetypes.PodUpdate, mode PodConfigNotificationMode, recorder record.EventRecorder, startupSLIObserver podStartupSLIObserver) *podStorage {
	return &podStorage{
		pods:               make(map[string]map[types.UID]*v1.Pod),
		mode:               mode,
		updates:            updates,
		sourcesSeen:        sets.String{},
		recorder:           recorder,
		startupSLIObserver: startupSLIObserver,
	}
}

// Merge normalizes a set of incoming changes from different sources into a map of all Pods
// and ensures that redundant changes are filtered out, and then pushes zero or more minimal
// updates onto the update channel.  Ensures that updates are delivered in order.
func (s *podStorage) Merge(source string, change interface{}) error {
	// 枷锁
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	seenBefore := s.sourcesSeen.Has(source)
	adds, updates, deletes, removes, reconciles := s.merge(source, change)
	firstSet := !seenBefore && s.sourcesSeen.Has(source)

	// deliver update notifications // 传递更新通知
	switch s.mode {
	case PodConfigNotificationIncremental:
		if len(removes.Pods) > 0 {
			s.updates <- *removes
		}
		if len(adds.Pods) > 0 {
			s.updates <- *adds
		}
		if len(updates.Pods) > 0 {
			s.updates <- *updates
		}
		if len(deletes.Pods) > 0 {
			s.updates <- *deletes
		}
		if firstSet && len(adds.Pods) == 0 && len(updates.Pods) == 0 && len(deletes.Pods) == 0 {
			// Send an empty update when first seeing the source and there are
			// no ADD or UPDATE or DELETE pods from the source. This signals kubelet that
			// the source is ready.
			s.updates <- *adds
		}
		// Only add reconcile support here, because kubelet doesn't support Snapshot update now.
		if len(reconciles.Pods) > 0 {
			s.updates <- *reconciles
		}

	case PodConfigNotificationSnapshotAndUpdates:
		if len(removes.Pods) > 0 || len(adds.Pods) > 0 || firstSet {
			s.updates <- kubetypes.PodUpdate{Pods: s.MergedState().([]*v1.Pod), Op: kubetypes.SET, Source: source}
		}
		if len(updates.Pods) > 0 {
			s.updates <- *updates
		}
		if len(deletes.Pods) > 0 {
			s.updates <- *deletes
		}

	case PodConfigNotificationSnapshot:
		if len(updates.Pods) > 0 || len(deletes.Pods) > 0 || len(adds.Pods) > 0 || len(removes.Pods) > 0 || firstSet {
			s.updates <- kubetypes.PodUpdate{Pods: s.MergedState().([]*v1.Pod), Op: kubetypes.SET, Source: source}
		}

	case PodConfigNotificationUnknown:
		fallthrough
	default:
		panic(fmt.Sprintf("unsupported PodConfigNotificationMode: %#v", s.mode))
	}

	return nil
}

func (s *podStorage) merge(source string, change interface{}) (adds, updates, deletes, removes, reconciles *kubetypes.PodUpdate) {
	s.podLock.Lock()
	defer s.podLock.Unlock()

	addPods := []*v1.Pod{}
	updatePods := []*v1.Pod{}
	deletePods := []*v1.Pod{}
	removePods := []*v1.Pod{}
	reconcilePods := []*v1.Pod{}

	pods := s.pods[source] // source file http api
	if pods == nil {
		pods = make(map[types.UID]*v1.Pod)
	}

	// updatePodFunc is the local function which updates the pod cache *oldPods* with new pods *newPods*.
	// After updated, new pod will be stored in the pod cache *pods*.
	// Notice that *pods* and *oldPods* could be the same cache.
	// updatePodFunc 是 local 函数，用于将 pod 缓存 *oldPods* 更新为新的 pod *newPods*。
	// 更新后，新的 pod 将存储在 pod 缓存 *pods* 中。
	// 请注意，*pods* 和 *oldPods* 可能是相同的缓存。
	updatePodsFunc := func(newPods []*v1.Pod, oldPods, pods map[types.UID]*v1.Pod) {
		// 过滤掉无效的Pods 进行去重
		filtered := filterInvalidPods(newPods, source, s.recorder)
		// 遍历过滤后的Pods 逐个更新 oldPods 中的 Pod 对象 或者 添加到 oldPods 中 或者删除 oldPods 中的 Pod 对象 或者更新 oldPods 中的 Pod 对象
		for _, ref := range filtered {
			// Annotate the pod with the source before any comparison.
			// 在任何比较之前，使用源注释 Pod。
			if ref.Annotations == nil {
				ref.Annotations = make(map[string]string)
			}
			ref.Annotations[kubetypes.ConfigSourceAnnotationKey] = source
			// ignore static pods
			if !kubetypes.IsStaticPod(ref) {
				s.startupSLIObserver.ObservedPodOnWatch(ref, time.Now())
			}
			if existing, found := oldPods[ref.UID]; found {
				pods[ref.UID] = existing
				/*
					checkAndUpdatePod；1. 更新 2. 删除 3. 协调
				*/
				needUpdate, needReconcile, needGracefulDelete := checkAndUpdatePod(existing, ref)
				if needUpdate {
					updatePods = append(updatePods, existing)
				} else if needReconcile {
					reconcilePods = append(reconcilePods, existing)
				} else if needGracefulDelete {
					deletePods = append(deletePods, existing)
				}
				continue
			}
			recordFirstSeenTime(ref)
			pods[ref.UID] = ref
			addPods = append(addPods, ref)
		}
	}

	update := change.(kubetypes.PodUpdate)
	switch update.Op {
	case kubetypes.ADD, kubetypes.UPDATE, kubetypes.DELETE:
		if update.Op == kubetypes.ADD {
			klog.V(4).InfoS("Adding new pods from source", "source", source, "pods", klog.KObjSlice(update.Pods))
		} else if update.Op == kubetypes.DELETE {
			klog.V(4).InfoS("Gracefully deleting pods from source", "source", source, "pods", klog.KObjSlice(update.Pods))
		} else {
			klog.V(4).InfoS("Updating pods from source", "source", source, "pods", klog.KObjSlice(update.Pods))
		}
		updatePodsFunc(update.Pods, pods, pods)

	case kubetypes.REMOVE:
		klog.V(4).InfoS("Removing pods from source", "source", source, "pods", klog.KObjSlice(update.Pods))
		for _, value := range update.Pods {
			if existing, found := pods[value.UID]; found {
				// this is a delete
				delete(pods, value.UID)
				removePods = append(removePods, existing)
				continue
			}
			// this is a no-op
		}

	case kubetypes.SET:
		klog.V(4).InfoS("Setting pods for source", "source", source)
		s.markSourceSet(source)
		// Clear the old map entries by just creating a new map // 通过创建新映射来清除旧映射条目
		oldPods := pods
		pods = make(map[types.UID]*v1.Pod)
		updatePodsFunc(update.Pods, oldPods, pods)
		for uid, existing := range oldPods {
			if _, found := pods[uid]; !found {
				// this is a delete // 这是一个删除
				removePods = append(removePods, existing)
			}
		}

	default:
		klog.InfoS("Received invalid update type", "type", update)

	}

	s.pods[source] = pods

	adds = &kubetypes.PodUpdate{Op: kubetypes.ADD, Pods: copyPods(addPods), Source: source}
	updates = &kubetypes.PodUpdate{Op: kubetypes.UPDATE, Pods: copyPods(updatePods), Source: source}
	deletes = &kubetypes.PodUpdate{Op: kubetypes.DELETE, Pods: copyPods(deletePods), Source: source}
	removes = &kubetypes.PodUpdate{Op: kubetypes.REMOVE, Pods: copyPods(removePods), Source: source}
	reconciles = &kubetypes.PodUpdate{Op: kubetypes.RECONCILE, Pods: copyPods(reconcilePods), Source: source}

	return adds, updates, deletes, removes, reconciles
}

func (s *podStorage) markSourceSet(source string) {
	s.sourcesSeenLock.Lock()
	defer s.sourcesSeenLock.Unlock()
	s.sourcesSeen.Insert(source)
}

func (s *podStorage) seenSources(sources ...string) bool {
	s.sourcesSeenLock.RLock()
	defer s.sourcesSeenLock.RUnlock()
	return s.sourcesSeen.HasAll(sources...)
}

func filterInvalidPods(pods []*v1.Pod, source string, recorder record.EventRecorder) (filtered []*v1.Pod) {
	names := sets.String{}
	for i, pod := range pods {
		// Pods from each source are assumed to have passed validation individually.
		// This function only checks if there is any naming conflict.
		// pods 从每个 source 都被假定为单独通过验证。
		// 此函数仅检查是否存在任何命名冲突。
		name := kubecontainer.GetPodFullName(pod)
		if names.Has(name) {
			klog.InfoS("Pod failed validation due to duplicate pod name, ignoring", "index", i, "pod", klog.KObj(pod), "source", source)
			recorder.Eventf(pod, v1.EventTypeWarning, events.FailedValidation, "Error validating pod %s from %s due to duplicate pod name %q, ignoring", format.Pod(pod), source, pod.Name)
			continue
		} else {
			names.Insert(name)
		}

		filtered = append(filtered, pod)
	}
	return
}

// Annotations that the kubelet adds to the pod.
var localAnnotations = []string{
	kubetypes.ConfigSourceAnnotationKey,
	kubetypes.ConfigMirrorAnnotationKey,
	kubetypes.ConfigFirstSeenAnnotationKey,
}

func isLocalAnnotationKey(key string) bool {
	for _, localKey := range localAnnotations {
		if key == localKey {
			return true
		}
	}
	return false
}

// isAnnotationMapEqual returns true if the existing annotation Map is equal to candidate except
// for local annotations.
func isAnnotationMapEqual(existingMap, candidateMap map[string]string) bool {
	if candidateMap == nil {
		candidateMap = make(map[string]string)
	}
	for k, v := range candidateMap {
		if isLocalAnnotationKey(k) {
			continue
		}
		if existingValue, ok := existingMap[k]; ok && existingValue == v {
			continue
		}
		return false
	}
	for k := range existingMap {
		if isLocalAnnotationKey(k) {
			continue
		}
		// stale entry in existing map.
		if _, exists := candidateMap[k]; !exists {
			return false
		}
	}
	return true
}

// recordFirstSeenTime records the first seen time of this pod.
func recordFirstSeenTime(pod *v1.Pod) {
	klog.V(4).InfoS("Receiving a new pod", "pod", klog.KObj(pod))
	pod.Annotations[kubetypes.ConfigFirstSeenAnnotationKey] = kubetypes.NewTimestamp().GetString()
}

// updateAnnotations returns an Annotation map containing the api annotation map plus
// locally managed annotations
func updateAnnotations(existing, ref *v1.Pod) {
	annotations := make(map[string]string, len(ref.Annotations)+len(localAnnotations))
	for k, v := range ref.Annotations {
		annotations[k] = v
	}
	for _, k := range localAnnotations {
		if v, ok := existing.Annotations[k]; ok {
			annotations[k] = v
		}
	}
	existing.Annotations = annotations
}

// podsDifferSemantically 返回 true，如果 existing 和 ref 在语义上不同
func podsDifferSemantically(existing, ref *v1.Pod) bool {
	if reflect.DeepEqual(existing.Spec, ref.Spec) &&
		reflect.DeepEqual(existing.Labels, ref.Labels) &&
		reflect.DeepEqual(existing.DeletionTimestamp, ref.DeletionTimestamp) &&
		reflect.DeepEqual(existing.DeletionGracePeriodSeconds, ref.DeletionGracePeriodSeconds) &&
		isAnnotationMapEqual(existing.Annotations, ref.Annotations) {
		return false
	}
	return true
}

// checkAndUpdatePod updates existing, and:
//   - if ref makes a meaningful change, returns needUpdate=true
//   - if ref makes a meaningful change, and this change is graceful deletion, returns needGracefulDelete=true
//   - if ref makes no meaningful change, but changes the pod status, returns needReconcile=true
//   - else return all false
//     Now, needUpdate, needGracefulDelete and needReconcile should never be both true
//
// checkAndUpdatePod 更新 existing，并：
//   - 如果 ref 进行了有意义的更改，则返回 needUpdate=true
//   - 如果 ref 进行了有意义的更改，并且此更改是优雅的删除，则返回 needGracefulDelete=true
//   - 如果 ref 没有进行有意义的更改，但更改了 pod 状态，则返回 needReconcile=true
//   - 否则返回所有 false
//     现在，needUpdate、needGracefulDelete 和 needReconcile 不应同时为 true
func checkAndUpdatePod(existing, ref *v1.Pod) (needUpdate, needReconcile, needGracefulDelete bool) {

	// 1. this is a reconcile
	// TODO: it would be better to update the whole object and only preserve certain things
	//       like the source annotation or the UID (to ensure safety)
	if !podsDifferSemantically(existing, ref) {
		// this is not an update Only check reconcile when it is not an update,
		// because if the pod is going to be updated, an extra reconcile is unnecessary
		// 这是不是一个更新，只有在不是更新时才检查协调，因为如果要更新 pod，则额外的协调是不必要的
		if !reflect.DeepEqual(existing.Status, ref.Status) {
			// Pod with changed pod status needs reconcile, because kubelet should be the source of truth of pod status.
			// Pod 的状态发生变化需要协调，因为 kubelet 应该是 pod 状态的真实来源。
			existing.Status = ref.Status
			needReconcile = true
		}
		return
	}

	// Overwrite the first-seen time with the existing one. This is our own internal annotation, there is no need to update.
	// 覆盖现有的第一次看到的时间。 这是我们自己的内部注释，没有必要更新。
	ref.Annotations[kubetypes.ConfigFirstSeenAnnotationKey] = existing.Annotations[kubetypes.ConfigFirstSeenAnnotationKey]

	existing.Spec = ref.Spec
	existing.Labels = ref.Labels
	existing.DeletionTimestamp = ref.DeletionTimestamp
	existing.DeletionGracePeriodSeconds = ref.DeletionGracePeriodSeconds
	existing.Status = ref.Status
	updateAnnotations(existing, ref)

	// 2. this is an graceful delete 这是一个优雅的删除
	if ref.DeletionTimestamp != nil {
		needGracefulDelete = true
	} else {
		// 3. this is an update 这是一个更新
		needUpdate = true
	}

	return
}

// Sync sends a copy of the current state through the update channel.
func (s *podStorage) Sync() {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	s.updates <- kubetypes.PodUpdate{Pods: s.MergedState().([]*v1.Pod), Op: kubetypes.SET, Source: kubetypes.AllSource}
}

// Object implements config.Accessor
func (s *podStorage) MergedState() interface{} {
	s.podLock.RLock()
	defer s.podLock.RUnlock()
	pods := make([]*v1.Pod, 0)
	for _, sourcePods := range s.pods {
		for _, podRef := range sourcePods {
			pods = append(pods, podRef.DeepCopy())
		}
	}
	return pods
}

func copyPods(sourcePods []*v1.Pod) []*v1.Pod {
	pods := []*v1.Pod{}
	for _, source := range sourcePods {
		// Use a deep copy here just in case
		pods = append(pods, source.DeepCopy())
	}
	return pods
}
