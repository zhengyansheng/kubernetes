/*
Copyright 2016 The Kubernetes Authors.

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

package garbagecollector

import (
	"context"
	goerrors "errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	clientset "k8s.io/client-go/kubernetes" // import known versions
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/controller"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog/v2"
	c "k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/apis/config/scheme"
	"k8s.io/kubernetes/pkg/controller/garbagecollector/metrics"
)

// ResourceResyncTime defines the resync period of the garbage collector's informers.
// ResourceResyncTime 定义垃圾收集器的informers的重新同步周期。
const ResourceResyncTime time.Duration = 0

// GarbageCollector runs reflectors to watch for changes of managed API
// objects, funnels the results to a single-threaded dependencyGraphBuilder,
// which builds a graph caching the dependencies among objects. Triggered by the
// graph changes, the dependencyGraphBuilder enqueues objects that can
// potentially be garbage-collected to the `attemptToDelete` queue, and enqueues
// objects whose dependents need to be orphaned to the `attemptToOrphan` queue.
// The GarbageCollector has workers who consume these two queues, send requests
// to the API server to delete/update the objects accordingly.
// Note that having the dependencyGraphBuilder notify the garbage collector
// ensures that the garbage collector operates with a graph that is at least as
// up to date as the notification is sent.

// GarbageCollector 运行反射器来监视托管API对象的更改，将结果汇总到单线程 dependencyGraphBuilder，构建一个缓存对象之间依赖关系的图形。
// 由图变化触发，dependencyGraphBuilder将可能被垃圾收集的对象 排队到`attemptToDelete`队列，并将其依赖项需要孤立的对象排队到`attemptToOrphan`队列。
// GarbageCollector具有使用这两个队列的工作人员，向API服务器发送请求以相应地删除更新对象。
// 请注意，让dependencyGraphBuilder通知垃圾收集器确保垃圾收集器使用至少与发送通知一样最新的图形进行操作。
type GarbageCollector struct {
	// resettableRESTMapper是一个RESTMapper，它能够在discovery资源类型时重置自己
	restMapper meta.ResettableRESTMapper
	// metadataClient 元数据客户端
	metadataClient metadata.Interface
	// garbage collector attempts to delete the items in attemptToDelete queue when the time is ripe.
	// 垃圾收集器尝试在时间成熟时 删除attemptToDelete队列中的item
	attemptToDelete workqueue.RateLimitingInterface

	// garbage collector attempts to orphan the dependents of the items in the attemptToOrphan queue, then deletes the items.
	// 垃圾收集器尝试孤立attemptToOrphan队列中item的依赖项，然后删除item
	attemptToOrphan workqueue.RateLimitingInterface

	// 依赖图的构建
	dependencyGraphBuilder *GraphBuilder

	// GC caches the owners that do not exist according to the API server.
	// GC根据API服务器缓存不存在的所有者
	// 有owner的资源对象,才会给absentOwnerCache填充不存在的Owner信息
	absentOwnerCache *ReferenceCache

	// clientSet
	kubeClient clientset.Interface

	// 事件
	eventBroadcaster record.EventBroadcaster

	// 互斥锁
	workerLock sync.RWMutex
}

var _ controller.Interface = (*GarbageCollector)(nil)
var _ controller.Debuggable = (*GarbageCollector)(nil)

// NewGarbageCollector creates a new GarbageCollector.
func NewGarbageCollector(
	kubeClient clientset.Interface,
	metadataClient metadata.Interface,
	mapper meta.ResettableRESTMapper,
	ignoredResources map[schema.GroupResource]struct{},
	sharedInformers informerfactory.InformerFactory,
	informersStarted <-chan struct{},
) (*GarbageCollector, error) {

	// event
	eventBroadcaster := record.NewBroadcaster()
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "garbage-collector-controller"})

	// queue
	attemptToDelete := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_attempt_to_delete")
	attemptToOrphan := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_attempt_to_orphan")
	// cache
	absentOwnerCache := NewReferenceCache(500)

	gc := &GarbageCollector{
		metadataClient:   metadataClient,
		restMapper:       mapper,
		attemptToDelete:  attemptToDelete,
		attemptToOrphan:  attemptToOrphan,
		absentOwnerCache: absentOwnerCache,
		kubeClient:       kubeClient,
		eventBroadcaster: eventBroadcaster,
	}
	gc.dependencyGraphBuilder = &GraphBuilder{
		eventRecorder:    eventRecorder,
		metadataClient:   metadataClient,
		informersStarted: informersStarted,
		restMapper:       mapper,
		graphChanges:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_graph_changes"),
		uidToNode: &concurrentUIDToNode{
			uidToNode: make(map[types.UID]*node),
		},
		attemptToDelete:  attemptToDelete,
		attemptToOrphan:  attemptToOrphan,
		absentOwnerCache: absentOwnerCache,
		sharedInformers:  sharedInformers,
		ignoredResources: ignoredResources,
	}

	// metrics
	metrics.Register()

	return gc, nil
}

// resyncMonitors starts or stops resource monitors as needed to ensure that all
// (and only) those resources present in the map are monitored.
func (gc *GarbageCollector) resyncMonitors(deletableResources map[schema.GroupVersionResource]struct{}) error {

	if err := gc.dependencyGraphBuilder.syncMonitors(deletableResources); err != nil {
		return err
	}
	gc.dependencyGraphBuilder.startMonitors()
	return nil
}

// Run starts garbage collector workers.
func (gc *GarbageCollector) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()
	defer gc.attemptToDelete.ShutDown()
	defer gc.attemptToOrphan.ShutDown()
	defer gc.dependencyGraphBuilder.graphChanges.ShutDown()

	// Start events processing pipeline.
	gc.eventBroadcaster.StartStructuredLogging(0)
	gc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: gc.kubeClient.CoreV1().Events("")})
	defer gc.eventBroadcaster.Shutdown()

	klog.Infof("Starting garbage collector controller")
	defer klog.Infof("Shutting down garbage collector controller")

	// 依赖关系图形生成器
	go gc.dependencyGraphBuilder.Run(ctx.Done())

	// 等待 dependencyGraphBuilder 缓存同步完成
	if !cache.WaitForNamedCacheSync("garbage collector", ctx.Done(), gc.dependencyGraphBuilder.IsSynced) {
		return
	}

	// 垃圾收集器：所有资源监视器都已同步。继续收集垃圾
	klog.Infof("Garbage collector: all resource monitors have synced. Proceeding to collect garbage")

	// gc workers
	for i := 0; i < workers; i++ {
		// 并发20个goroutine，间隔1秒运行，从attemptToDelete队列中删除work
		go wait.UntilWithContext(ctx, gc.runAttemptToDeleteWorker, 1*time.Second)
		// 并发20个goroutine，间隔1秒运行，从attemptToOrphanWorker队列中xxx
		go wait.Until(gc.runAttemptToOrphanWorker, 1*time.Second, ctx.Done())
	}

	<-ctx.Done()
}

// Sync periodically resyncs the garbage collector when new resources are
// observed from discovery. When new resources are detected, Sync will stop all
// GC workers, reset gc.restMapper, and resync the monitors.
//
// Note that discoveryClient should NOT be shared with gc.restMapper, otherwise
// the mapper's underlying discovery client will be unnecessarily reset during
// the course of detecting new resources.
func (gc *GarbageCollector) Sync(discoveryClient discovery.ServerResourcesInterface, period time.Duration, stopCh <-chan struct{}) {
	// 定义版本资源组
	oldResources := make(map[schema.GroupVersionResource]struct{})

	wait.Until(func() {
		// Get the current resource list from discovery.
		// 从 discoveryClient 获取当前可删除的资源列表 (有这三个权限"delete", "list", "watch")
		newResources := GetDeletableResources(discoveryClient)

		// This can occur if there is an internal error in GetDeletableResources.
		if len(newResources) == 0 { // 内部错误
			klog.V(2).Infof("no resources reported by discovery, skipping garbage collector sync")
			metrics.GarbageCollectorResourcesSyncError.Inc()
			return
		}

		// Decide whether discovery has reported a change.
		if reflect.DeepEqual(oldResources, newResources) { // 没有变化
			klog.V(5).Infof("no resource updates from discovery, skipping garbage collector sync")
			return
		}

		// Ensure workers are paused to avoid processing events before informers have resynced.
		gc.workerLock.Lock()
		defer gc.workerLock.Unlock()

		// Once we get here, we should not unpause workers until we've successfully synced
		attempt := 0
		wait.PollImmediateUntil(100*time.Millisecond, func() (bool, error) {
			attempt++

			// On a reattempt, check if available resources have changed
			if attempt > 1 {
				newResources = GetDeletableResources(discoveryClient)
				if len(newResources) == 0 {
					klog.V(2).Infof("no resources reported by discovery (attempt %d)", attempt)
					metrics.GarbageCollectorResourcesSyncError.Inc()
					return false, nil
				}
			}

			klog.V(2).Infof("syncing garbage collector with updated resources from discovery (attempt %d): %s", attempt, printDiff(oldResources, newResources))

			// Resetting the REST mapper will also invalidate the underlying discovery
			// client. This is a leaky abstraction and assumes behavior about the REST
			// mapper, but we'll deal with it for now.
			gc.restMapper.Reset()
			klog.V(4).Infof("reset restmapper")

			// Perform the monitor resync and wait for controllers to report cache sync.
			//
			// NOTE: It's possible that newResources will diverge from the resources
			// discovered by restMapper during the call to Reset, since they are
			// distinct discovery clients invalidated at different times. For example,
			// newResources may contain resources not returned in the restMapper's
			// discovery call if the resources appeared in-between the calls. In that
			// case, the restMapper will fail to map some of newResources until the next
			// attempt.
			if err := gc.resyncMonitors(newResources); err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to sync resource monitors (attempt %d): %v", attempt, err))
				metrics.GarbageCollectorResourcesSyncError.Inc()
				return false, nil
			}
			klog.V(4).Infof("resynced monitors")

			// wait for caches to fill for a while (our sync period) before attempting to rediscover resources and retry syncing.
			// this protects us from deadlocks where available resources changed and one of our informer caches will never fill.
			// informers keep attempting to sync in the background, so retrying doesn't interrupt them.
			// the call to resyncMonitors on the reattempt will no-op for resources that still exist.
			// note that workers stay paused until we successfully resync.
			if !cache.WaitForNamedCacheSync("garbage collector", waitForStopOrTimeout(stopCh, period), gc.dependencyGraphBuilder.IsSynced) {
				utilruntime.HandleError(fmt.Errorf("timed out waiting for dependency graph builder sync during GC sync (attempt %d)", attempt))
				metrics.GarbageCollectorResourcesSyncError.Inc()
				return false, nil
			}

			// success, break out of the loop
			return true, nil
		}, stopCh)

		// Finally, keep track of our new state. Do this after all preceding steps
		// have succeeded to ensure we'll retry on subsequent syncs if an error
		// occurred.
		oldResources = newResources
		klog.V(2).Infof("synced garbage collector")
	}, period, stopCh)
}

// printDiff returns a human-readable summary of what resources were added and removed
func printDiff(oldResources, newResources map[schema.GroupVersionResource]struct{}) string {
	removed := sets.NewString()
	for oldResource := range oldResources {
		if _, ok := newResources[oldResource]; !ok {
			removed.Insert(fmt.Sprintf("%+v", oldResource))
		}
	}
	added := sets.NewString()
	for newResource := range newResources {
		if _, ok := oldResources[newResource]; !ok {
			added.Insert(fmt.Sprintf("%+v", newResource))
		}
	}
	return fmt.Sprintf("added: %v, removed: %v", added.List(), removed.List())
}

// waitForStopOrTimeout returns a stop channel that closes when the provided stop channel closes or when the specified timeout is reached
func waitForStopOrTimeout(stopCh <-chan struct{}, timeout time.Duration) <-chan struct{} {
	stopChWithTimeout := make(chan struct{})
	go func() {
		select {
		case <-stopCh:
		case <-time.After(timeout):
		}
		close(stopChWithTimeout)
	}()
	return stopChWithTimeout
}

// IsSynced returns true if dependencyGraphBuilder is synced.
func (gc *GarbageCollector) IsSynced() bool {
	return gc.dependencyGraphBuilder.IsSynced()
}

func (gc *GarbageCollector) runAttemptToDeleteWorker(ctx context.Context) {
	for gc.processAttemptToDeleteWorker(ctx) {
	}
}

var enqueuedVirtualDeleteEventErr = goerrors.New("enqueued virtual delete event")

var namespacedOwnerOfClusterScopedObjectErr = goerrors.New("cluster-scoped objects cannot refer to namespaced owners")

func (gc *GarbageCollector) processAttemptToDeleteWorker(ctx context.Context) bool {
	item, quit := gc.attemptToDelete.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	// 删除 item
	defer gc.attemptToDelete.Done(item)

	action := gc.attemptToDeleteWorker(ctx, item)
	switch action {
	case forgetItem: // 忘记元素，删除元素
		gc.attemptToDelete.Forget(item)
	case requeueItem: // 元素重新排队，限速队列中
		gc.attemptToDelete.AddRateLimited(item)
	}

	return true
}

type workQueueItemAction int

const (
	// item 重新入队列
	requeueItem = iota
	// 忘记 item
	forgetItem
)

// attemptToDeleteWorker attempts to delete the given item from the cluster.
func (gc *GarbageCollector) attemptToDeleteWorker(ctx context.Context, item interface{}) workQueueItemAction {
	// 断言node类型
	n, ok := item.(*node)
	if !ok {
		// 断言失败直接返回 forgetItem
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return forgetItem
	}

	if !n.isObserved() {
		nodeFromGraph, existsInGraph := gc.dependencyGraphBuilder.uidToNode.Read(n.identity.UID)
		if !existsInGraph {
			// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
			// and in the meantime a deletion of the real object associated with that uid was observed
			klog.V(5).Infof("item %s no longer in the graph, skipping attemptToDeleteItem", n)
			return forgetItem
		}
		if nodeFromGraph.isObserved() {
			// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
			// and in the meantime the real object associated with that uid was observed
			klog.V(5).Infof("item %s no longer virtual in the graph, skipping attemptToDeleteItem on virtual node", n)
			return forgetItem
		}
	}

	err := gc.attemptToDeleteItem(ctx, n)

	if err == enqueuedVirtualDeleteEventErr {
		// a virtual event was produced and will be handled by processGraphChanges, no need to requeue this node
		return forgetItem
	} else if err == namespacedOwnerOfClusterScopedObjectErr {
		// a cluster-scoped object referring to a namespaced owner is an error that will not resolve on retry, no need to requeue this node
		return forgetItem
	} else if err != nil {
		if _, ok := err.(*restMappingError); ok {
			// There are at least two ways this can happen:
			// 1. The reference is to an object of a custom type that has not yet been
			//    recognized by gc.restMapper (this is a transient error).
			// 2. The reference is to an invalid group/version. We don't currently
			//    have a way to distinguish this from a valid type we will recognize
			//    after the next discovery sync.
			// For now, record the error and retry.
			klog.V(5).Infof("error syncing item %s: %v", n, err)
		} else {
			utilruntime.HandleError(fmt.Errorf("error syncing item %s: %v", n, err))
		}
		// retry if garbage collection of an object failed.
		return requeueItem
	} else if !n.isObserved() {
		// requeue if item hasn't been observed via an informer event yet.
		// otherwise a virtual node for an item added AND removed during watch reestablishment can get stuck in the graph and never removed.
		// see https://issue.k8s.io/56121
		klog.V(5).Infof("item %s hasn't been observed via informer yet", n.identity)
		return requeueItem
	}

	return forgetItem
}

// isDangling check if a reference is pointing to an object that doesn't exist.
// If isDangling looks up the referenced object at the API server, it also returns its latest state.
// isDangling检查引用是否指向不存在的对象。
// 如果isDangling在API服务器上查找引用的对象，它也会返回其最新状态。
func (gc *GarbageCollector) isDangling(ctx context.Context, reference metav1.OwnerReference, item *node) (dangling bool, owner *metav1.PartialObjectMetadata, err error) {

	// check for recorded absent cluster-scoped parent
	// 检查记录的缺少的集群范围的父项 (非名称空间范围内）
	absentOwnerCacheKey := objectReference{OwnerReference: ownerReferenceCoordinates(reference)}
	if gc.absentOwnerCache.Has(absentOwnerCacheKey) {
		klog.V(5).Infof("according to the absentOwnerCache, object %s's owner %s/%s, %s does not exist", item.identity.UID, reference.APIVersion, reference.Kind, reference.Name)
		return true, nil, nil
	}
	// check for recorded absent namespaced parent （名称空间范围内）
	absentOwnerCacheKey.Namespace = item.identity.Namespace
	if gc.absentOwnerCache.Has(absentOwnerCacheKey) {
		klog.V(5).Infof("according to the absentOwnerCache, object %s's owner %s/%s, %s does not exist in namespace %s", item.identity.UID, reference.APIVersion, reference.Kind, reference.Name, item.identity.Namespace)
		return true, nil, nil
	}

	// TODO: we need to verify the reference resource is supported by the
	// system. If it's not a valid resource, the garbage collector should i)
	// ignore the reference when decide if the object should be deleted, and
	// ii) should update the object to remove such references. This is to
	// prevent objects having references to an old resource from being
	// deleted during a cluster upgrade.
	resource, namespaced, err := gc.apiResource(reference.APIVersion, reference.Kind)
	if err != nil {
		return false, nil, err
	}
	if !namespaced {
		absentOwnerCacheKey.Namespace = ""
	}

	if len(item.identity.Namespace) == 0 && namespaced {
		// item is a cluster-scoped object referring to a namespace-scoped owner, which is not valid.
		// return a marker error, rather than retrying on the lookup failure forever.
		klog.V(2).Infof("object %s is cluster-scoped, but refers to a namespaced owner of type %s/%s", item.identity, reference.APIVersion, reference.Kind)
		return false, nil, namespacedOwnerOfClusterScopedObjectErr
	}

	// TODO: It's only necessary to talk to the API server if the owner node
	// is a "virtual" node. The local graph could lag behind the real status, but in practice, the difference is small.
	// 是一个“虚拟”节点。局部图可能落后于实际状态，但在实践中，差异很小。
	owner, err = gc.metadataClient.Resource(resource).Namespace(resourceDefaultNamespace(namespaced, item.identity.Namespace)).Get(ctx, reference.Name, metav1.GetOptions{})
	switch {
	case errors.IsNotFound(err):
		gc.absentOwnerCache.Add(absentOwnerCacheKey)
		klog.V(5).Infof("object %s's owner %s/%s, %s is not found", item.identity.UID, reference.APIVersion, reference.Kind, reference.Name)
		return true, nil, nil
	case err != nil:
		return false, nil, err
	}

	if owner.GetUID() != reference.UID {
		klog.V(5).Infof("object %s's owner %s/%s, %s is not found, UID mismatch", item.identity.UID, reference.APIVersion, reference.Kind, reference.Name)
		gc.absentOwnerCache.Add(absentOwnerCacheKey)
		return true, nil, nil
	}
	return false, owner, nil
}

// classify the latestReferences to three categories:
// solid: the owner exists, and is not "waitingForDependentsDeletion"
// dangling: the owner does not exist
// waitingForDependentsDeletion: the owner exists, its deletionTimestamp is non-nil, and it has
// FinalizerDeletingDependents
// This function communicates with the server.
func (gc *GarbageCollector) classifyReferences(ctx context.Context, item *node, latestReferences []metav1.OwnerReference) (
	solid, dangling, waitingForDependentsDeletion []metav1.OwnerReference, err error) {

	for _, reference := range latestReferences { // latestReferences -> owner
		isDangling, owner, err := gc.isDangling(ctx, reference, item) // false, owner, nil
		if err != nil {
			return nil, nil, nil, err
		}
		if isDangling {
			dangling = append(dangling, reference)
			continue
		}

		ownerAccessor, err := meta.Accessor(owner)
		if err != nil {
			return nil, nil, nil, err
		}
		if ownerAccessor.GetDeletionTimestamp() != nil && hasDeleteDependentsFinalizer(ownerAccessor) {
			// 要删除的依赖项
			waitingForDependentsDeletion = append(waitingForDependentsDeletion, reference)
		} else {
			// 有效的
			solid = append(solid, reference)
		}
	}
	return solid, dangling, waitingForDependentsDeletion, nil
}

func ownerRefsToUIDs(refs []metav1.OwnerReference) []types.UID {
	var ret []types.UID
	for _, ref := range refs {
		ret = append(ret, ref.UID)
	}
	return ret
}

// attemptToDeleteItem looks up the live API object associated with the node,
// and issues a delete IFF the uid matches, the item is not blocked on deleting dependents,
// and all owner references are dangling.
//
// if the API get request returns a NotFound error, or the retrieved item's uid does not match,
// a virtual delete event for the node is enqueued and enqueuedVirtualDeleteEventErr is returned.
func (gc *GarbageCollector) attemptToDeleteItem(ctx context.Context, item *node) error {
	klog.V(2).InfoS("Processing object", "object", klog.KRef(item.identity.Namespace, item.identity.Name),
		"objectUID", item.identity.UID, "kind", item.identity.Kind, "virtual", !item.isObserved())

	// "being deleted" is an one-way trip to the final deletion. We'll just wait for the final deletion, and then process the object's dependents.
	if item.isBeingDeleted() && !item.isDeletingDependents() {
		klog.V(5).Infof("processing item %s returned at once, because its DeletionTimestamp is non-nil", item.identity)
		return nil
	}
	// TODO: It's only necessary to talk to the API server if this is a
	// "virtual" node. The local graph could lag behind the real status, but in practice, the difference is small.
	//klog.Infof("-----> attempting to delete item %+v", item)
	klog.InfoS("----> Processing object", "object", klog.KRef(item.identity.Namespace, item.identity.Name), "objectUID", item.identity.UID, "kind", item.identity.Kind, "virtual", !item.isObserved())
	klog.Infof("-----> attempting to delete item identity %+v", item.identity)
	latest, err := gc.getObject(item.identity)
	klog.Infof("-----> attempting to delete item latest %+v", latest)
	switch {
	case errors.IsNotFound(err):
		// the GraphBuilder can add "virtual" node for an owner that doesn't
		// exist yet, so we need to enqueue a virtual Delete event to remove
		// the virtual node from GraphBuilder.uidToNode.
		klog.V(5).Infof("item %v not found, generating a virtual delete event", item.identity)
		gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
		return enqueuedVirtualDeleteEventErr
	case err != nil:
		return err
	}

	if latest.GetUID() != item.identity.UID {
		klog.V(5).Infof("UID doesn't match, item %v not found, generating a virtual delete event", item.identity)
		gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
		return enqueuedVirtualDeleteEventErr
	}

	// TODO: attemptToOrphanWorker() routine is similar. Consider merging
	// attemptToOrphanWorker() into attemptToDeleteItem() as well.
	if item.isDeletingDependents() {
		return gc.processDeletingDependentsItem(item)
	}

	// compute if we should delete the item
	ownerReferences := latest.GetOwnerReferences() // 获取owner (rs的owner是deployment，pod的owner是rs）
	if len(ownerReferences) == 0 {                 // 如果没有owner，直接返回
		klog.V(2).Infof("object %s's doesn't have an owner, continue on next item", item.identity)
		return nil
	}
	klog.Infof("-----> ownerReferences: %+v", ownerReferences)

	// 核心逻辑 对引用分类处理
	/*
		solid: owner存在 或者 终结器不为 foregroundDeletion的owner集合
		dangling: 不存在的owner集群
		waitingForDependentsDeletion: owner存在，deletionTimestamp为非nil(删除)，终结器为foregroundDeletion的owner集合
	*/
	solid, dangling, waitingForDependentsDeletion, err := gc.classifyReferences(ctx, item, ownerReferences) // 分类引用 (rs)
	if err != nil {
		return err
	}
	klog.V(5).Infof("classify references of %s.\nsolid: %#v\ndangling: %#v\nwaitingForDependentsDeletion: %#v\n", item.identity, solid, dangling, waitingForDependentsDeletion)

	klog.Infof("-----> solid: %+v", solid)
	klog.Infof("-----> dangling: %+v", dangling)
	klog.Infof("-----> waitingForDependentsDeletion: %+v", waitingForDependentsDeletion)
	switch {
	case len(solid) != 0: // solid 不为空，即item存在没被删除的owner

		klog.V(2).Infof("object %#v has at least one existing owner: %#v, will not garbage collect", item.identity, solid)

		// 当dangling和waitingForDependentsDeletion都为空，则直接返回
		if len(dangling) == 0 && len(waitingForDependentsDeletion) == 0 {
			return nil
		}
		klog.V(2).Infof("remove dangling references %#v and waiting references %#v for object %s", dangling, waitingForDependentsDeletion, item.identity)
		// waitingForDependentsDeletion needs to be deleted from the
		// ownerReferences, otherwise the referenced objects will be stuck with
		// the FinalizerDeletingDependents and never get deleted.

		// 合并两个集合owner uid
		ownerUIDs := append(ownerRefsToUIDs(dangling), ownerRefsToUIDs(waitingForDependentsDeletion)...)
		p, err := c.GenerateDeleteOwnerRefStrategicMergeBytes(item.identity.UID, ownerUIDs)
		if err != nil {
			return err
		}
		// 执行patch请求，将这些uid对应的ownerReferences从item中删除
		_, err = gc.patch(item, p, func(n *node) ([]byte, error) {
			return gc.deleteOwnerRefJSONMergePatch(n, ownerUIDs...)
		})
		return err
	case len(waitingForDependentsDeletion) != 0 && item.dependentsLength() != 0: // waitingForDependentsDeletion集合不为空，且item有从资源
		deps := item.getDependents()

		for _, dep := range deps {
			// isDeletingDependents 判断是否正在删除从属关系
			if dep.isDeletingDependents() {
				// this circle detection has false positives, we need to
				// apply a more rigorous detection if this turns out to be a problem.
				// there are multiple workers run attemptToDeleteItem in
				// parallel, the circle detection can fail in a race condition.
				klog.V(2).Infof("processing object %s, some of its owners and its dependent [%s] have FinalizerDeletingDependents, to prevent potential cycle, its ownerReferences are going to be modified to be non-blocking, then the object is going to be deleted with Foreground", item.identity, dep.identity)
				patch, err := item.unblockOwnerReferencesStrategicMergePatch()
				if err != nil {
					return err
				}
				// 执行patch请求，将FinalizerDeletingDependents从item中删除
				if _, err := gc.patch(item, patch, gc.unblockOwnerReferencesJSONMergePatch); err != nil {
					return err
				}
				break
			}
		}
		klog.V(2).Infof("at least one owner of object %s has FinalizerDeletingDependents, and the object itself has dependents, so it is going to be deleted in Foreground", item.identity)
		// the deletion event will be observed by the graphBuilder, so the item
		// will be processed again in processDeletingDependentsItem. If it
		// doesn't have dependents, the function will remove the
		// FinalizerDeletingDependents from the item, resulting in the final
		// deletion of the item.

		// 后台删除策略
		policy := metav1.DeletePropagationForeground
		// 执行删除
		return gc.deleteObject(item.identity, &policy)
	default:
		// item doesn't have any solid owner, so it needs to be garbage
		// collected. Also, none of item's owners is waiting for the deletion of
		// the dependents, so set propagationPolicy based on existing finalizers.
		// 删除策略
		var policy metav1.DeletionPropagation
		switch {
		case hasOrphanFinalizer(latest):
			// if an existing orphan finalizer is already on the object, honor it.
			// 如果对象的metadata.finalizer中有 "orphan" 孤儿 则返回true
			policy = metav1.DeletePropagationOrphan
		case hasDeleteDependentsFinalizer(latest): // "foregroundDeletion"
			// if an existing foreground finalizer is already on the object, honor it.
			// 如果对象的metadata.finalizer中有 "foregroundDeletion" 前台删除 则返回true，同步删除逻辑
			policy = metav1.DeletePropagationForeground
		default:
			// otherwise, default to background.
			// 默认后台删除，异步删除逻辑
			policy = metav1.DeletePropagationBackground
		}
		klog.V(2).InfoS("Deleting object", "object", klog.KRef(item.identity.Namespace, item.identity.Name),
			"objectUID", item.identity.UID, "kind", item.identity.Kind, "propagationPolicy", policy)

		// 删除对象
		return gc.deleteObject(item.identity, &policy)
	}
}

// process item that's waiting for its dependents to be deleted
func (gc *GarbageCollector) processDeletingDependentsItem(item *node) error {
	// blockingDependents item对象阻塞的从资源
	blockingDependents := item.blockingDependents()

	// 如果为0 直接删除
	if len(blockingDependents) == 0 {
		klog.V(2).Infof("remove DeleteDependents finalizer for item %s", item.identity)
		return gc.removeFinalizer(item, metav1.FinalizerDeleteDependents) // "foregroundDeletion"
	}
	// 从资源添加到 attemptToDelete 队列中
	for _, dep := range blockingDependents {
		if !dep.isDeletingDependents() {
			klog.V(2).Infof("adding %s to attemptToDelete, because its owner %s is deletingDependents", dep.identity, item.identity)
			gc.attemptToDelete.Add(dep)
		}
	}
	return nil
}

// dependents are copies of pointers to the owner's dependents, they don't need to be locked.
func (gc *GarbageCollector) orphanDependents(owner objectReference, dependents []*node) error {
	errCh := make(chan error, len(dependents))
	wg := sync.WaitGroup{}
	wg.Add(len(dependents))
	for i := range dependents {
		go func(dependent *node) {
			defer wg.Done()
			// the dependent.identity.UID is used as precondition
			p, err := c.GenerateDeleteOwnerRefStrategicMergeBytes(dependent.identity.UID, []types.UID{owner.UID})
			if err != nil {
				errCh <- fmt.Errorf("orphaning %s failed, %v", dependent.identity, err)
				return
			}
			_, err = gc.patch(dependent, p, func(n *node) ([]byte, error) {
				return gc.deleteOwnerRefJSONMergePatch(n, owner.UID)
			})
			// note that if the target ownerReference doesn't exist in the
			// dependent, strategic merge patch will NOT return an error.
			if err != nil && !errors.IsNotFound(err) {
				errCh <- fmt.Errorf("orphaning %s failed, %v", dependent.identity, err)
			}
		}(dependents[i])
	}
	wg.Wait()
	close(errCh)

	var errorsSlice []error
	for e := range errCh {
		errorsSlice = append(errorsSlice, e)
	}

	if len(errorsSlice) != 0 {
		return fmt.Errorf("failed to orphan dependents of owner %s, got errors: %s", owner, utilerrors.NewAggregate(errorsSlice).Error())
	}
	klog.V(5).Infof("successfully updated all dependents of owner %s", owner)
	return nil
}

func (gc *GarbageCollector) runAttemptToOrphanWorker() {
	for gc.processAttemptToOrphanWorker() {
	}
}

// processAttemptToOrphanWorker dequeues a node from the attemptToOrphan, then finds its
// dependents based on the graph maintained by the GC, then removes it from the
// OwnerReferences of its dependents, and finally updates the owner to remove
// the "Orphan" finalizer. The node is added back into the attemptToOrphan if any of
// these steps fail.
func (gc *GarbageCollector) processAttemptToOrphanWorker() bool {
	item, quit := gc.attemptToOrphan.Get()

	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	defer gc.attemptToOrphan.Done(item)

	action := gc.attemptToOrphanWorker(item)
	switch action {
	case forgetItem:
		// 删除item
		gc.attemptToOrphan.Forget(item)
	case requeueItem:
		// 延迟队列
		gc.attemptToOrphan.AddRateLimited(item)
	}

	return true
}

func (gc *GarbageCollector) attemptToOrphanWorker(item interface{}) workQueueItemAction {
	owner, ok := item.(*node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return forgetItem
	}
	// we don't need to lock each element, because they never get updated
	owner.dependentsLock.RLock()
	dependents := make([]*node, 0, len(owner.dependents))
	for dependent := range owner.dependents {
		dependents = append(dependents, dependent)
	}
	owner.dependentsLock.RUnlock()

	// orphanDependents will remove the owner from the OwnerReferences of its dependents
	err := gc.orphanDependents(owner.identity, dependents)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("orphanDependents for %s failed with %v", owner.identity, err))
		return requeueItem
	}
	// update the owner, remove "orphaningFinalizer" from its finalizers list
	err = gc.removeFinalizer(owner, metav1.FinalizerOrphanDependents) // "orphan"
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("removeOrphanFinalizer for %s failed with %v", owner.identity, err))
		return requeueItem
	}
	return forgetItem
}

// *FOR TEST USE ONLY*
// GraphHasUID returns if the GraphBuilder has a particular UID store in its
// uidToNode graph. It's useful for debugging.
// This method is used by integration tests.
func (gc *GarbageCollector) GraphHasUID(u types.UID) bool {
	_, ok := gc.dependencyGraphBuilder.uidToNode.Read(u)
	return ok
}

// GetDeletableResources returns all resources from discoveryClient that the
// garbage collector should recognize and work with. More specifically, all
// preferred resources which support the 'delete', 'list', and 'watch' verbs.
//
// All discovery errors are considered temporary. Upon encountering any error,
// GetDeletableResources will log and return any discovered resources it was
// able to process (which may be none).
// 获取可删除的资源( 因为如果没有删除操作，就不需要再次同步）
// 这个函数可以直接运行，传递下参数即可；
func GetDeletableResources(discoveryClient discovery.ServerResourcesInterface) map[schema.GroupVersionResource]struct{} {
	preferredResources, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		if discovery.IsGroupDiscoveryFailedError(err) {
			klog.Warningf("failed to discover some groups: %v", err.(*discovery.ErrGroupDiscoveryFailed).Groups)
		} else {
			klog.Warningf("failed to discover preferred resources: %v", err)
		}
	}
	if preferredResources == nil {
		return map[schema.GroupVersionResource]struct{}{}
	}

	// This is extracted from discovery.GroupVersionResources to allow tolerating failures on a per-resource basis.
	deletableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"delete", "list", "watch"}}, preferredResources)
	deletableGroupVersionResources := map[schema.GroupVersionResource]struct{}{}
	for _, rl := range deletableResources {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			klog.Warningf("ignoring invalid discovered resource %q: %v", rl.GroupVersion, err)
			continue
		}
		for i := range rl.APIResources {
			deletableGroupVersionResources[schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: rl.APIResources[i].Name}] = struct{}{}
		}
	}

	return deletableGroupVersionResources
}

func (gc *GarbageCollector) Name() string {
	return "garbagecollector"
}
