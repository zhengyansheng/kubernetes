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
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	apps "k8s.io/api/apps/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/core"

	v1 "k8s.io/api/core/v1"
	eventv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"

	"k8s.io/kubernetes/pkg/controller/garbagecollector/metaonly"
)

type eventType int

func (e eventType) String() string {
	switch e {
	case addEvent:
		return "add"
	case updateEvent:
		return "update"
	case deleteEvent:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

const (
	addEvent eventType = iota
	updateEvent
	deleteEvent
)

type event struct {
	// virtual indicates this event did not come from an informer, but was constructed artificially
	virtual   bool
	eventType eventType
	obj       interface{}
	// the update event comes with an old object, but it's not used by the garbage collector.
	oldObj interface{}
	gvk    schema.GroupVersionKind
}

// GraphBuilder processes events supplied by the informers, updates uidToNode,
// a graph that caches the dependencies as we know, and enqueues
// items to the attemptToDelete and attemptToOrphan.
type GraphBuilder struct {
	restMapper meta.RESTMapper

	// each monitor list/watches a resource, the results are funneled to the
	// dependencyGraphBuilder
	// 每个监视器列表/监视资源，结果汇集到dependencyGraphBuilder
	monitors monitors

	// 互斥锁
	monitorLock sync.RWMutex

	// informersStarted is closed after after all of the controllers have been initialized and are running.
	// After that it is safe to start them here, before that it is not.
	// informersStarted在所有控制器初始化并运行后关闭。之后在这里启动它们是安全的，在此之前它不是。
	informersStarted <-chan struct{}

	// stopCh drives shutdown. When a receive from it unblocks, monitors will shut down.
	// This channel is also protected by monitorLock.
	stopCh <-chan struct{}

	// running tracks whether Run() has been called.
	// it is protected by monitorLock.
	// 运行轨道是否已调用Run()它受monitorLock保护。
	running bool

	// event
	eventRecorder record.EventRecorder

	metadataClient metadata.Interface

	// monitors are the producer of the graphChanges queue, graphBuilder alters
	// the in-memory graph according to the changes.
	graphChanges workqueue.RateLimitingInterface

	// uidToNode doesn't require a lock to protect, because only the
	// single-threaded GraphBuilder.processGraphChanges() reads/writes it.
	// uidToNode 不需要锁保护，因为只有单线程 GraphBuilder.processGraphChanges() 读写它。
	uidToNode *concurrentUIDToNode

	// GraphBuilder is the producer of attemptToDelete and attemptToOrphan, GC is the consumer.
	// GraphBuilder是attemptToDelete和attemptTo Orphan的生产者，GC是消费者
	attemptToDelete workqueue.RateLimitingInterface
	attemptToOrphan workqueue.RateLimitingInterface

	// GraphBuilder and GC share the absentOwnerCache.
	// Objects that are known to be non-existent are added to the cached.
	//GraphBuilder和GC共享absentOwnerCache, 将已知不存在的对象添加到缓存中。
	absentOwnerCache *ReferenceCache

	// 所有k8s资源对象集的informer
	sharedInformers informerfactory.InformerFactory

	// 监视器忽略的资源对象集
	ignoredResources map[schema.GroupResource]struct{}
}

// monitor runs a Controller with a local stop channel.
type monitor struct {
	controller cache.Controller
	store      cache.Store

	// stopCh stops Controller. If stopCh is nil, the monitor is considered to be not yet started.
	// stopCh 停止控制器。如果stopCh为nil，则认为监视器尚未启动。
	stopCh chan struct{}
}

// Run is intended to be called in a goroutine. Multiple calls of this is an
// error.
func (m *monitor) Run() {
	m.controller.Run(m.stopCh)
}

type monitors map[schema.GroupVersionResource]*monitor

func (gb *GraphBuilder) controllerFor(resource schema.GroupVersionResource, kind schema.GroupVersionKind) (cache.Controller, cache.Store, error) {

	// 1. 将 新增、更改、删除的资源对象构建为event结构体，放入GraphBuilder的graphChanges队列里
	handlers := cache.ResourceEventHandlerFuncs{
		// add the event to the dependencyGraphBuilder's graphChanges.
		// 添加事件到dependencyGraphBuilder的graphChanges。
		AddFunc: func(obj interface{}) {
			event := &event{
				eventType: addEvent,
				obj:       obj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// TODO: check if there are differences in the ownerRefs,
			// finalizers, and DeletionTimestamp; if not, ignore the update.
			event := &event{
				eventType: updateEvent,
				obj:       newObj,
				oldObj:    oldObj,
				gvk:       kind,
			}
			if pod, ok := newObj.(*core.Pod); ok && pod.Namespace == "default" {
				klog.Infof("-----> [controllerFor] UpdateFunc pod: %v", pod.Name)
			}
			gb.graphChanges.Add(event)
		},
		DeleteFunc: func(obj interface{}) { // 执行 delete 操作会被监听到
			// delta fifo may wrap the object in a cache.DeletedFinalStateUnknown, unwrap it
			// delta fifo可以将对象包装在cache.DeletedFinalStateUnknown中，解包它
			if deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = deletedFinalStateUnknown.Obj
			}
			event := &event{
				eventType: deleteEvent,
				obj:       obj,
				gvk:       kind,
			}
			klog.Infof("-----> [controllerFor] DeleteFunc delete gvk: %v", event.gvk)
			if deployment, ok := obj.(*apps.Deployment); ok {
				klog.Infof("-----> [controllerFor] DeleteFunc delete obj: %+v", deployment.ObjectMeta)
				klog.Infof("-----> [controllerFor] DeleteFunc delete obj deletionTimestamp: %v", deployment.ObjectMeta.GetDeletionTimestamp())
			}
			gb.graphChanges.Add(event)
		},
	}
	// 2. 从sharedInformers中获取资源对象的informer，添加事件处理函数
	shared, err := gb.sharedInformers.ForResource(resource)
	if err != nil {
		// 获取资源对象时出错会到这里,比如非k8s内置RedisCluster、clusterbases、clusters、esclusters、volumeproviders、stsmasters、appapps、mysqlclusters、brokerclusters、clustertemplates;
		// 内置的networkPolicies、apiservices、customresourcedefinitions
		klog.V(4).Infof("unable to use a shared informer for resource %q, kind %q: %v", resource.String(), kind.String(), err)
		return nil, nil, err
	}

	klog.V(4).Infof("using a shared informer for resource %q, kind %q", resource.String(), kind.String())

	// need to clone because it's from a shared cache
	// 不需要克隆，因为它不是来自共享缓存
	shared.Informer().AddEventHandlerWithResyncPeriod(handlers, ResourceResyncTime) // 0

	return shared.Informer().GetController(), shared.Informer().GetStore(), nil
}

// syncMonitors rebuilds the monitor set according to the supplied resources,
// creating or deleting monitors as necessary. It will return any error
// encountered, but will make an attempt to create a monitor for each resource
// instead of immediately exiting on an error. It may be called before or after
// Run. Monitors are NOT started as part of the sync. To ensure all existing
// monitors are started, call startMonitors.
func (gb *GraphBuilder) syncMonitors(resources map[schema.GroupVersionResource]struct{}) error {
	// 加锁
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	toRemove := gb.monitors
	if toRemove == nil {
		toRemove = monitors{}
	}
	current := monitors{}
	errs := []error{}
	kept := 0
	added := 0
	for resource := range resources {
		// 如果是忽略的资源对象，直接跳过
		if _, ok := gb.ignoredResources[resource.GroupResource()]; ok {
			continue
		}
		// 如果已经存在，直接跳过
		if m, ok := toRemove[resource]; ok {
			current[resource] = m
			delete(toRemove, resource)
			kept++
			continue
		}
		// 获取资源对象的kind
		kind, err := gb.restMapper.KindFor(resource)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't look up resource %q: %v", resource, err))
			continue
		}
		klog.Infof("-----> [syncMonitors] starting monitor for resource %+v, kind %v", resource, kind)
		// controllerFor 核心逻辑
		c, s, err := gb.controllerFor(resource, kind)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't start monitor for resource %q: %v", resource, err))
			continue
		}
		current[resource] = &monitor{store: s, controller: c}
		added++
	}
	// 赋值
	gb.monitors = current

	for _, monitor := range toRemove {
		if monitor.stopCh != nil {
			close(monitor.stopCh)
		}
	}

	klog.V(4).Infof("synced monitors; added %d, kept %d, removed %d", added, kept, len(toRemove))
	// NewAggregate returns nil if errs is 0-length
	return utilerrors.NewAggregate(errs)
}

// startMonitors ensures the current set of monitors are running. Any newly
// started monitors will also cause shared informers to be started.
//
// If called before Run, startMonitors does nothing (as there is no stop channel
// to support monitor/informer execution).
func (gb *GraphBuilder) startMonitors() {
	// 加锁
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	// 如果没有运行，直接返回
	if !gb.running {
		return
	}

	// we're waiting until after the informer start that happens once all the controllers are initialized.
	// This ensures that they don't get unexpected events on their work queues.
	//我们一直等到informer启动后，所有控制器初始化后才会发生这种情况。这确保了他们的工作队列中不会出现意外事件。
	<-gb.informersStarted

	monitors := gb.monitors
	started := 0
	var gvrs []string
	for gvr, monitor := range monitors {
		if monitor.stopCh == nil {
			monitor.stopCh = make(chan struct{})
			// 启动 informer
			gb.sharedInformers.Start(gb.stopCh)
			//klog.Infof("-----> start monitor gvr: %+v,", gvr.String())
			gvrs = append(gvrs, gvr.String())

			// 启动 controller
			go monitor.Run()
			started++
		}
	}
	printIndent(gvrs)

	klog.V(4).Infof("started %d new monitors, %d currently running", started, len(monitors))
}

/*
[
	"apps/v1, Resource=deployments",
	"networking.istio.io/v1beta1, Resource=workloadentries",
	"/v1, Resource=endpoints",
	"crd.projectcalico.org/v1, Resource=felixconfigurations",
	"networking.istio.io/v1beta1, Resource=virtualservices",
	"extensions.istio.io/v1alpha1, Resource=wasmplugins",
	"networking.k8s.io/v1, Resource=ingresses",
	"/v1, Resource=limitranges",
	"batch/v1, Resource=jobs",
	"networking.k8s.io/v1, Resource=networkpolicies",
	"/v1, Resource=replicationcontrollers",
	"certificates.k8s.io/v1, Resource=certificatesigningrequests",
	"/v1, Resource=persistentvolumeclaims",
	"coordination.k8s.io/v1, Resource=leases",
	"crd.projectcalico.org/v1, Resource=kubecontrollersconfigurations",
	"rbac.authorization.k8s.io/v1, Resource=clusterrolebindings",
	"networking.istio.io/v1beta1, Resource=sidecars",
	"install.istio.io/v1alpha1, Resource=istiooperators",
	"crd.projectcalico.org/v1, Resource=globalnetworkpolicies",
	"scheduling.k8s.io/v1, Resource=priorityclasses",
	"apiregistration.k8s.io/v1, Resource=apiservices",
	"admissionregistration.k8s.io/v1, Resource=mutatingwebhookconfigurations",
	"/v1, Resource=namespaces",
	"networking.k8s.io/v1, Resource=ingressclasses",
	"admissionregistration.k8s.io/v1, Resource=validatingwebhookconfigurations",
	"/v1, Resource=services",
	"crd.projectcalico.org/v1, Resource=networksets",
	"crd.projectcalico.org/v1, Resource=bgpconfigurations",
	"rbac.authorization.k8s.io/v1, Resource=roles",
	"crd.projectcalico.org/v1, Resource=ipamhandles",
	"/v1, Resource=pods",
	"apiextensions.k8s.io/v1, Resource=customresourcedefinitions",
	"/v1, Resource=persistentvolumes",
	"/v1, Resource=nodes",
	"apps/v1, Resource=daemonsets",
	"crd.projectcalico.org/v1, Resource=clusterinformations",
	"rbac.authorization.k8s.io/v1, Resource=rolebindings",
	"crd.projectcalico.org/v1, Resource=globalnetworksets",
	"autoscaling/v2, Resource=horizontalpodautoscalers",
	"crd.projectcalico.org/v1, Resource=ippools",
	"crd.projectcalico.org/v1, Resource=caliconodestatuses",
	"flowcontrol.apiserver.k8s.io/v1beta3, Resource=flowschemas",
	"crd.projectcalico.org/v1, Resource=ipamconfigs",
	"apps/v1, Resource=controllerrevisions",
	"crd.projectcalico.org/v1, Resource=ipamblocks",
	"crd.projectcalico.org/v1, Resource=hostendpoints",
	"networking.istio.io/v1beta1, Resource=gateways",
	"storage.k8s.io/v1, Resource=volumeattachments",
	"security.istio.io/v1, Resource=requestauthentications",
	"crd.projectcalico.org/v1, Resource=blockaffinities",
	"/v1, Resource=serviceaccounts",
	"/v1, Resource=secrets",
	"storage.k8s.io/v1, Resource=csistoragecapacities",
	"/v1, Resource=resourcequotas",
	"apps/v1, Resource=statefulsets",
	"node.k8s.io/v1, Resource=runtimeclasses",
	"networking.istio.io/v1beta1, Resource=workloadgroups",
	"crd.projectcalico.org/v1, Resource=bgppeers",
	"rbac.authorization.k8s.io/v1, Resource=clusterroles",
	"telemetry.istio.io/v1alpha1, Resource=telemetries",
	"storage.k8s.io/v1, Resource=storageclasses",
	"storage.k8s.io/v1, Resource=csinodes",
	"networking.istio.io/v1beta1, Resource=destinationrules",
	"flowcontrol.apiserver.k8s.io/v1beta3, Resource=prioritylevelconfigurations",
	"batch/v1, Resource=cronjobs",
	"policy/v1, Resource=poddisruptionbudgets",
	"storage.k8s.io/v1, Resource=csidrivers",
	"/v1, Resource=podtemplates",
	"discovery.k8s.io/v1, Resource=endpointslices",
	"apps/v1, Resource=replicasets",
	"crd.projectcalico.org/v1, Resource=ipreservations",
	"/v1, Resource=configmaps",
	"security.istio.io/v1, Resource=authorizationpolicies",
	"crd.projectcalico.org/v1, Resource=networkpolicies",
	"networking.istio.io/v1beta1, Resource=proxyconfigs",
	"networking.istio.io/v1alpha3, Resource=envoyfilters",
	"security.istio.io/v1beta1, Resource=peerauthentications",
	"networking.istio.io/v1beta1, Resource=serviceentries",
	"apps.hh.org/v1, Resource=appsets"
]
*/

func printIndent(s interface{}) {
	bs, err := json.Marshal(s)
	if err != nil {
		klog.Errorf("-----> printIndent: %v", err)
	}
	var out bytes.Buffer
	json.Indent(&out, bs, "", "\t")
	fmt.Printf("-----> interface: %v\n", out.String())
}

// IsSynced returns true if any monitors exist AND all those monitors'
// controllers HasSynced functions return true. This means IsSynced could return
// true at one time, and then later return false if all monitors were
// reconstructed.
func (gb *GraphBuilder) IsSynced() bool {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	if len(gb.monitors) == 0 {
		klog.V(4).Info("garbage controller monitor not synced: no monitors")
		return false
	}

	for resource, monitor := range gb.monitors {
		if !monitor.controller.HasSynced() {
			klog.V(4).Infof("garbage controller monitor not yet synced: %+v", resource)
			return false
		}
	}
	return true
}

// Run sets the stop channel and starts monitor execution until stopCh is
// closed. Any running monitors will be stopped before Run returns.
func (gb *GraphBuilder) Run(stopCh <-chan struct{}) {
	klog.Infof("GraphBuilder running")
	defer klog.Infof("GraphBuilder stopping")

	// Set up the stop channel.
	gb.monitorLock.Lock()
	gb.stopCh = stopCh
	gb.running = true
	gb.monitorLock.Unlock()

	// Start monitors and begin change processing until the stop channel is closed.
	// 启动 informer
	gb.startMonitors()

	// 开启消费者
	wait.Until(gb.runProcessGraphChanges, 1*time.Second, stopCh)

	// Stop any running monitors.
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	monitors := gb.monitors
	stopped := 0
	for _, monitor := range monitors {
		if monitor.stopCh != nil {
			stopped++
			close(monitor.stopCh)
		}
	}

	// reset monitors so that the graph builder can be safely re-run/synced.
	gb.monitors = nil
	klog.Infof("stopped %d of %d monitors", stopped, len(monitors))
}

var ignoredResources = map[schema.GroupResource]struct{}{
	{Group: "", Resource: "events"}:                {},
	{Group: eventv1.GroupName, Resource: "events"}: {},
}

// DefaultIgnoredResources returns the default set of resources that the garbage collector controller
// should ignore. This is exposed so downstream integrators can have access to the defaults, and add
// to them as necessary when constructing the controller.
func DefaultIgnoredResources() map[schema.GroupResource]struct{} {
	return ignoredResources
}

// enqueueVirtualDeleteEvent is used to add a virtual delete event to be processed for virtual nodes
// once it is determined they do not have backing objects in storage
func (gb *GraphBuilder) enqueueVirtualDeleteEvent(ref objectReference) {
	gv, _ := schema.ParseGroupVersion(ref.APIVersion)
	gb.graphChanges.Add(&event{
		virtual:   true,
		eventType: deleteEvent,
		gvk:       gv.WithKind(ref.Kind),
		obj: &metaonly.MetadataOnlyObject{
			TypeMeta:   metav1.TypeMeta{APIVersion: ref.APIVersion, Kind: ref.Kind},
			ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, UID: ref.UID, Name: ref.Name},
		},
	})
}

// addDependentToOwners adds n to owners' dependents list. If the owner does not
// exist in the gb.uidToNode yet, a "virtual" node will be created to represent
// the owner. The "virtual" node will be enqueued to the attemptToDelete, so that
// attemptToDeleteItem() will verify if the owner exists according to the API server.
func (gb *GraphBuilder) addDependentToOwners(n *node, owners []metav1.OwnerReference) {
	// track if some of the referenced owners already exist in the graph and have been observed,
	// and the dependent's ownerRef does not match their observed coordinates
	hasPotentiallyInvalidOwnerReference := false
	klog.Infof("-----> owners: %+v", owners)
	for _, owner := range owners {
		ownerNode, ok := gb.uidToNode.Read(owner.UID)
		if !ok {
			// Create a "virtual" node in the graph for the owner if it doesn't
			// exist in the graph yet.
			ownerNode = &node{
				identity: objectReference{
					OwnerReference: ownerReferenceCoordinates(owner),
					Namespace:      n.identity.Namespace,
				},
				dependents: make(map[*node]struct{}),
				virtual:    true,
			}
			klog.V(5).Infof("add virtual node.identity: %s\n\n", ownerNode.identity)
			gb.uidToNode.Write(ownerNode)
		}
		ownerNode.addDependent(n)
		if !ok {
			// Enqueue the virtual node into attemptToDelete.
			// The garbage processor will enqueue a virtual delete
			// event to delete it from the graph if API server confirms this
			// owner doesn't exist.
			gb.attemptToDelete.Add(ownerNode)
		} else if !hasPotentiallyInvalidOwnerReference {
			ownerIsNamespaced := len(ownerNode.identity.Namespace) > 0
			if ownerIsNamespaced && ownerNode.identity.Namespace != n.identity.Namespace {
				if ownerNode.isObserved() {
					// The owner node has been observed via an informer
					// the dependent's namespace doesn't match the observed owner's namespace, this is definitely wrong.
					// cluster-scoped owners can be referenced as an owner from any namespace or cluster-scoped object.
					klog.V(2).Infof("node %s references an owner %s but does not match namespaces", n.identity, ownerNode.identity)
					gb.reportInvalidNamespaceOwnerRef(n, owner.UID)
				}
				hasPotentiallyInvalidOwnerReference = true
			} else if !ownerReferenceMatchesCoordinates(owner, ownerNode.identity.OwnerReference) {
				if ownerNode.isObserved() {
					// The owner node has been observed via an informer
					// n's owner reference doesn't match the observed identity, this might be wrong.
					klog.V(2).Infof("node %s references an owner %s with coordinates that do not match the observed identity", n.identity, ownerNode.identity)
				}
				hasPotentiallyInvalidOwnerReference = true
			} else if !ownerIsNamespaced && ownerNode.identity.Namespace != n.identity.Namespace && !ownerNode.isObserved() {
				// the ownerNode is cluster-scoped and virtual, and does not match the child node's namespace.
				// the owner could be a missing instance of a namespaced type incorrectly referenced by a cluster-scoped child (issue #98040).
				// enqueue this child to attemptToDelete to verify parent references.
				hasPotentiallyInvalidOwnerReference = true
			}
		}
	}

	if hasPotentiallyInvalidOwnerReference {
		// Enqueue the potentially invalid dependent node into attemptToDelete.
		// The garbage processor will verify whether the owner references are dangling
		// and delete the dependent if all owner references are confirmed absent.
		klog.Infof("-----> enqueue potentially invalid node %+v into attemptToDelete", n.identity)
		gb.attemptToDelete.Add(n)
	}
}

func (gb *GraphBuilder) reportInvalidNamespaceOwnerRef(n *node, invalidOwnerUID types.UID) {
	var invalidOwnerRef metav1.OwnerReference
	var found = false
	for _, ownerRef := range n.owners {
		if ownerRef.UID == invalidOwnerUID {
			invalidOwnerRef = ownerRef
			found = true
			break
		}
	}
	if !found {
		return
	}
	ref := &v1.ObjectReference{
		Kind:       n.identity.Kind,
		APIVersion: n.identity.APIVersion,
		Namespace:  n.identity.Namespace,
		Name:       n.identity.Name,
		UID:        n.identity.UID,
	}
	invalidIdentity := objectReference{
		OwnerReference: metav1.OwnerReference{
			Kind:       invalidOwnerRef.Kind,
			APIVersion: invalidOwnerRef.APIVersion,
			Name:       invalidOwnerRef.Name,
			UID:        invalidOwnerRef.UID,
		},
		Namespace: n.identity.Namespace,
	}
	gb.eventRecorder.Eventf(ref, v1.EventTypeWarning, "OwnerRefInvalidNamespace", "ownerRef %s does not exist in namespace %q", invalidIdentity, n.identity.Namespace)
}

// insertNode insert the node to gb.uidToNode; then it finds all owners as listed
// in n.owners, and adds the node to their dependents list.
func (gb *GraphBuilder) insertNode(n *node) {
	gb.uidToNode.Write(n)

	// 添加依赖关系
	gb.addDependentToOwners(n, n.owners)
}

// removeDependentFromOwners remove n from owners' dependents list.
func (gb *GraphBuilder) removeDependentFromOwners(n *node, owners []metav1.OwnerReference) {
	for _, owner := range owners {
		ownerNode, ok := gb.uidToNode.Read(owner.UID)
		if !ok {
			continue
		}
		ownerNode.deleteDependent(n)
	}
}

// removeNode removes the node from gb.uidToNode, then finds all
// owners as listed in n.owners, and removes n from their dependents list.
func (gb *GraphBuilder) removeNode(n *node) {
	gb.uidToNode.Delete(n.identity.UID)
	gb.removeDependentFromOwners(n, n.owners)
}

type ownerRefPair struct {
	oldRef metav1.OwnerReference
	newRef metav1.OwnerReference
}

// TODO: profile this function to see if a naive N^2 algorithm performs better
// when the number of references is small.
func referencesDiffs(old []metav1.OwnerReference, new []metav1.OwnerReference) (added []metav1.OwnerReference, removed []metav1.OwnerReference, changed []ownerRefPair) {
	oldUIDToRef := make(map[string]metav1.OwnerReference)
	for _, value := range old {
		oldUIDToRef[string(value.UID)] = value
	}
	oldUIDSet := sets.StringKeySet(oldUIDToRef)
	for _, value := range new {
		newUID := string(value.UID)
		if oldUIDSet.Has(newUID) {
			if !reflect.DeepEqual(oldUIDToRef[newUID], value) {
				changed = append(changed, ownerRefPair{oldRef: oldUIDToRef[newUID], newRef: value})
			}
			oldUIDSet.Delete(newUID)
		} else {
			added = append(added, value)
		}
	}
	for oldUID := range oldUIDSet {
		removed = append(removed, oldUIDToRef[oldUID])
	}

	return added, removed, changed
}

func deletionStartsWithFinalizer(oldObj interface{}, newAccessor metav1.Object, matchingFinalizer string) bool {
	// if the new object isn't being deleted, or doesn't have the finalizer we're interested in, return false
	if !beingDeleted(newAccessor) || !hasFinalizer(newAccessor, matchingFinalizer) {
		return false
	}

	// if the old object is nil, or wasn't being deleted, or didn't have the finalizer, return true
	if oldObj == nil {
		return true
	}
	oldAccessor, err := meta.Accessor(oldObj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access oldObj: %v", err))
		return false
	}
	return !beingDeleted(oldAccessor) || !hasFinalizer(oldAccessor, matchingFinalizer)
}

func beingDeleted(accessor metav1.Object) bool {
	return accessor.GetDeletionTimestamp() != nil
}

func hasDeleteDependentsFinalizer(accessor metav1.Object) bool {
	return hasFinalizer(accessor, metav1.FinalizerDeleteDependents) // "foregroundDeletion"
}

func hasOrphanFinalizer(accessor metav1.Object) bool {
	return hasFinalizer(accessor, metav1.FinalizerOrphanDependents) // "orphan"
}

func hasFinalizer(accessor metav1.Object, matchingFinalizer string) bool {
	// 获取对象的 finalizers
	finalizers := accessor.GetFinalizers()
	for _, finalizer := range finalizers {
		if finalizer == matchingFinalizer { // "orphan"
			return true
		}
	}
	return false
}

// this function takes newAccessor directly because the caller already
// instantiates an accessor for the newObj.
func startsWaitingForDependentsDeleted(oldObj interface{}, newAccessor metav1.Object) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, metav1.FinalizerDeleteDependents)
}

// this function takes newAccessor directly because the caller already
// instantiates an accessor for the newObj.
func startsWaitingForDependentsOrphaned(oldObj interface{}, newAccessor metav1.Object) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, metav1.FinalizerOrphanDependents)
}

// if an blocking ownerReference points to an object gets removed, or gets set to
// "BlockOwnerDeletion=false", add the object to the attemptToDelete queue.
func (gb *GraphBuilder) addUnblockedOwnersToDeleteQueue(removed []metav1.OwnerReference, changed []ownerRefPair) {
	for _, ref := range removed {
		if ref.BlockOwnerDeletion != nil && *ref.BlockOwnerDeletion {
			node, found := gb.uidToNode.Read(ref.UID)
			if !found {
				klog.V(5).Infof("cannot find %s in uidToNode", ref.UID)
				continue
			}
			gb.attemptToDelete.Add(node)
		}
	}
	for _, c := range changed {
		wasBlocked := c.oldRef.BlockOwnerDeletion != nil && *c.oldRef.BlockOwnerDeletion
		isUnblocked := c.newRef.BlockOwnerDeletion == nil || (c.newRef.BlockOwnerDeletion != nil && !*c.newRef.BlockOwnerDeletion)
		if wasBlocked && isUnblocked {
			node, found := gb.uidToNode.Read(c.newRef.UID)
			if !found {
				klog.V(5).Infof("cannot find %s in uidToNode", c.newRef.UID)
				continue
			}
			gb.attemptToDelete.Add(node)
		}
	}
}

// processTransitions
func (gb *GraphBuilder) processTransitions(oldObj interface{}, newAccessor metav1.Object, n *node) {
	if newAccessor.GetName() == "nginx" {
		klog.Info("--------------> 4")
	}
	// 直接删除pod不会进入这个逻辑
	if startsWaitingForDependentsOrphaned(oldObj, newAccessor) {
		klog.V(5).Infof("add %s to the attemptToOrphan", n.identity)
		klog.Infof("-----> add %s to the attemptToOrphan, node: %+v", n.identity, n)
		gb.attemptToOrphan.Add(n)
		return
	}
	if startsWaitingForDependentsDeleted(oldObj, newAccessor) {
		klog.V(2).Infof("add %s to the attemptToDelete, because it's waiting for its dependents to be deleted", n.identity)
		klog.Infof("-----> add %s to the attemptToDelete, because it's waiting for its dependents to be deleted", n.identity)

		// if the n is added as a "virtual" node, its deletingDependents field is not properly set, so always set it here.
		n.markDeletingDependents()

		klog.Infof("-----> n.dependents: %+v", n.dependents)
		for dep := range n.dependents {
			gb.attemptToDelete.Add(dep)
		}
		gb.attemptToDelete.Add(n)
	}
}

// runProcessGraphChanges 运行processGraphChanges直到graphChanges队列为空
func (gb *GraphBuilder) runProcessGraphChanges() {
	for gb.processGraphChanges() {
	}
}

func identityFromEvent(event *event, accessor metav1.Object) objectReference {
	return objectReference{
		OwnerReference: metav1.OwnerReference{
			APIVersion: event.gvk.GroupVersion().String(),
			Kind:       event.gvk.Kind,
			UID:        accessor.GetUID(),
			Name:       accessor.GetName(),
		},
		Namespace: accessor.GetNamespace(),
	}
}

// Dequeueing an event from graphChanges, updating graph, populating dirty_queue.
// 从graphChanges中获取事件，更新图形，填充dirty_queue
// (graphChanges队列里数据来源于各个资源的monitors监听资源变化回调addFunc、updateFunc、deleteFunc)
func (gb *GraphBuilder) processGraphChanges() bool {
	// 从graphChanges队列中获取事件
	item, quit := gb.graphChanges.Get()
	if quit {
		return false
	}
	// 事件处理完成后，调用Done方法，将事件从graphChanges队列中移除
	defer gb.graphChanges.Done(item)

	// 事件类型
	event, ok := item.(*event)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect a *event, got %v", item))
		return true
	}
	obj := event.obj
	// 获取对象的accessor
	accessor, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
		return true
	}
	klog.V(5).Infof("GraphBuilder process object: %s/%s, namespace %s, name %s, uid %s, event type %v, virtual=%v", event.gvk.GroupVersion().String(), event.gvk.Kind, accessor.GetNamespace(), accessor.GetName(), string(accessor.GetUID()), event.eventType, event.virtual)

	// Check if the node already exists
	// 检查节点是否已经存在
	existingNode, found := gb.uidToNode.Read(accessor.GetUID())
	if accessor.GetName() == "nginx" {
		klog.Infof("-----> GraphBuilder process object: %s/%s, namespace %s, name %s, uid %s, event type %v, virtual=%v", event.gvk.GroupVersion().String(), event.gvk.Kind, accessor.GetNamespace(), accessor.GetName(), string(accessor.GetUID()), event.eventType, event.virtual)
		klog.Infof("-----> existingNode: %v, found: %v", existingNode, found)
	}
	if found && !event.virtual && !existingNode.isObserved() {

		// this marks the node as having been observed via an informer event
		// 1. this depends on graphChanges only containing add/update events from the actual informer
		// 2. this allows things tracking virtual nodes' existence to stop polling and rely on informer events
		observedIdentity := identityFromEvent(event, accessor)
		if observedIdentity != existingNode.identity {
			// find dependents that don't match the identity we observed
			_, potentiallyInvalidDependents := partitionDependents(existingNode.getDependents(), observedIdentity)
			if accessor.GetName() == "nginx" {
				klog.Infof("--------------> 0, len(potentiallyInvalidDependents): %v", len(potentiallyInvalidDependents))
			}
			// add those potentially invalid dependents to the attemptToDelete queue.
			// if their owners are still solid the attemptToDelete will be a no-op.
			// this covers the bad child -> good parent observation sequence.
			// the good parent -> bad child observation sequence is handled in addDependentToOwners
			for _, dep := range potentiallyInvalidDependents {

				if len(observedIdentity.Namespace) > 0 && dep.identity.Namespace != observedIdentity.Namespace {
					// Namespace mismatch, this is definitely wrong
					klog.V(2).Infof("node %s references an owner %s but does not match namespaces", dep.identity, observedIdentity)
					gb.reportInvalidNamespaceOwnerRef(dep, observedIdentity.UID)
				}
				klog.Infof("-----> dep: %+v", dep)
				gb.attemptToDelete.Add(dep)
			}

			// make a copy (so we don't modify the existing node in place), store the observed identity, and replace the virtual node
			klog.V(2).Infof("replacing virtual node %s with observed node %s", existingNode.identity, observedIdentity)
			klog.Infof("replacing virtual node %s with observed node %s", existingNode.identity, observedIdentity)
			existingNode = existingNode.clone()
			existingNode.identity = observedIdentity
			gb.uidToNode.Write(existingNode)
		}
		if accessor.GetNamespace() == "default" {
			klog.Info("---> markObserved")
		}
		existingNode.markObserved()
	}

	switch {
	// gc 第一次运行时，uidToNode尚且没有初始化资源对象依赖关系图表结构，所以found为false，会新增节点
	case (event.eventType == addEvent || event.eventType == updateEvent) && !found:
		if accessor.GetName() == "nginx" {
			klog.Info("---> addEvent, updateEvent, found=false")
		}
		// 构建新的节点
		newNode := &node{
			identity:           identityFromEvent(event, accessor),
			dependents:         make(map[*node]struct{}),
			owners:             accessor.GetOwnerReferences(),
			deletingDependents: beingDeleted(accessor) && hasDeleteDependentsFinalizer(accessor),
			beingDeleted:       beingDeleted(accessor),
		}
		gb.insertNode(newNode)
		// the underlying delta_fifo may combine a creation and a deletion into
		// one event, so we need to further process the event.
		// 底层delta_fifo可以将创建和删除组合成一个事件，因此我们需要进一步处理事件。
		gb.processTransitions(event.oldObj, accessor, newNode)

	// uidToNode已经初始化资源对象依赖关系图表结构，所以found为true
	case (event.eventType == addEvent || event.eventType == updateEvent) && found:
		// handle changes in ownerReferences
		added, removed, changed := referencesDiffs(existingNode.owners, accessor.GetOwnerReferences())
		if accessor.GetName() == "nginx" {
			klog.Info("--------------> 1")
			klog.Infof("---> addEvent:%v, updateEvent:%v, found=true", addEvent.String(), updateEvent.String())
			klog.Infof("---> added:%v, removed:%v, changed:%v", added, removed, changed)
		}
		if len(added) != 0 || len(removed) != 0 || len(changed) != 0 { // 直接删除pod 无法进入这个逻辑
			if accessor.GetName() == "nginx" {
				klog.Info("--------------> 2")
			}
			// check if the changed dependency graph unblock owners that are waiting for the deletion of their dependents.
			// 检查更改后的依赖关系图是否取消阻止正在等待删除其依赖关系的所有者
			gb.addUnblockedOwnersToDeleteQueue(removed, changed)
			// update the node itself
			// 更新节点本身
			existingNode.owners = accessor.GetOwnerReferences()

			// Add the node to its new owners' dependent lists.
			// 添加节点到其新所有者的依赖列表中
			gb.addDependentToOwners(existingNode, added)

			// remove the node from the dependent list of node that are no longer in the node's owners list.
			// 移除节点从不再在节点的所有者列表中的节点的依赖列表中
			gb.removeDependentFromOwners(existingNode, removed)
		}

		if beingDeleted(accessor) {
			if accessor.GetName() == "nginx" {
				klog.Info("--------------> 3")
			}
			existingNode.markBeingDeleted()
		}
		// 直接删除pod会进入这个逻辑
		gb.processTransitions(event.oldObj, accessor, existingNode)

	case event.eventType == deleteEvent:
		if !found {
			klog.V(5).Infof("%v doesn't exist in the graph, this shouldn't happen", accessor.GetUID())
			return true
		}

		removeExistingNode := true
		if event.virtual {
			if accessor.GetName() == "nginx" {
				klog.Info("--------------> 5")
			}
			// this is a virtual delete event, not one observed from an informer
			deletedIdentity := identityFromEvent(event, accessor)
			if existingNode.virtual {

				// our existing node is also virtual, we're not sure of its coordinates.
				// see if any dependents reference this owner with coordinates other than the one we got a virtual delete event for.
				if matchingDependents, nonmatchingDependents := partitionDependents(existingNode.getDependents(), deletedIdentity); len(nonmatchingDependents) > 0 {

					// some of our dependents disagree on our coordinates, so do not remove the existing virtual node from the graph
					removeExistingNode = false

					if len(matchingDependents) > 0 {
						// mark the observed deleted identity as absent
						gb.absentOwnerCache.Add(deletedIdentity)
						// attempt to delete dependents that do match the verified deleted identity
						for _, dep := range matchingDependents {
							gb.attemptToDelete.Add(dep)
						}
					}

					// if the delete event verified existingNode.identity doesn't exist...
					if existingNode.identity == deletedIdentity {
						// find an alternative identity our nonmatching dependents refer to us by
						replacementIdentity := getAlternateOwnerIdentity(nonmatchingDependents, deletedIdentity)
						if replacementIdentity != nil {
							// replace the existing virtual node with a new one with one of our other potential identities
							replacementNode := existingNode.clone()
							replacementNode.identity = *replacementIdentity
							gb.uidToNode.Write(replacementNode)
							// and add the new virtual node back to the attemptToDelete queue
							gb.attemptToDelete.AddRateLimited(replacementNode)
						}
					}
				}

			} else if existingNode.identity != deletedIdentity {
				if accessor.GetName() == "nginx" {
					klog.Info("--------------> 6")
				}
				// do not remove the existing real node from the graph based on a virtual delete event
				removeExistingNode = false

				// our existing node which was observed via informer disagrees with the virtual delete event's coordinates
				matchingDependents, _ := partitionDependents(existingNode.getDependents(), deletedIdentity)

				if len(matchingDependents) > 0 {
					// mark the observed deleted identity as absent
					gb.absentOwnerCache.Add(deletedIdentity)
					// attempt to delete dependents that do match the verified deleted identity
					for _, dep := range matchingDependents {
						gb.attemptToDelete.Add(dep)
					}
				}
			}
		}

		if removeExistingNode {
			if accessor.GetName() == "nginx" {
				klog.Info("--------------> 7")
			}
			// removeNode updates the graph
			gb.removeNode(existingNode)

			// 加锁
			existingNode.dependentsLock.RLock()
			defer existingNode.dependentsLock.RUnlock()
			if len(existingNode.dependents) > 0 {
				gb.absentOwnerCache.Add(identityFromEvent(event, accessor))
			}

			klog.Infof("-----> existingNode dependents: %+v", existingNode.dependents)
			for dep := range existingNode.dependents {
				klog.Infof("-----> dep: %+v", dep)
				gb.attemptToDelete.Add(dep)
			}
			klog.Infof("-----> existingNode owners: %+v", existingNode.owners)
			for _, owner := range existingNode.owners {
				ownerNode, found := gb.uidToNode.Read(owner.UID)
				if !found || !ownerNode.isDeletingDependents() {
					continue
				}
				// this is to let attempToDeleteItem check if all the owner's
				// dependents are deleted, if so, the owner will be deleted.
				klog.Infof("-----> ownerNode: %+v", ownerNode)
				gb.attemptToDelete.Add(ownerNode)
			}
		}
	}
	return true
}

// partitionDependents divides the provided dependents into a list which have an ownerReference matching the provided identity,
// and ones which have an ownerReference for the given uid that do not match the provided identity.
// Note that a dependent with multiple ownerReferences for the target uid can end up in both lists.
func partitionDependents(dependents []*node, matchOwnerIdentity objectReference) (matching, nonmatching []*node) {
	ownerIsNamespaced := len(matchOwnerIdentity.Namespace) > 0
	for i := range dependents {
		dep := dependents[i]
		foundMatch := false
		foundMismatch := false
		// if the dep namespace matches or the owner is cluster scoped ...
		if ownerIsNamespaced && matchOwnerIdentity.Namespace != dep.identity.Namespace {
			// all references to the parent do not match, since the dependent namespace does not match the owner
			foundMismatch = true
		} else {
			for _, ownerRef := range dep.owners {
				// ... find the ownerRef with a matching uid ...
				if ownerRef.UID == matchOwnerIdentity.UID {
					// ... and check if it matches all coordinates
					if ownerReferenceMatchesCoordinates(ownerRef, matchOwnerIdentity.OwnerReference) {
						foundMatch = true
					} else {
						foundMismatch = true
					}
				}
			}
		}

		if foundMatch {
			matching = append(matching, dep)
		}
		if foundMismatch {
			nonmatching = append(nonmatching, dep)
		}
	}
	return matching, nonmatching
}

func referenceLessThan(a, b objectReference) bool {
	// kind/apiVersion are more significant than namespace,
	// so that we get coherent ordering between kinds
	// regardless of whether they are cluster-scoped or namespaced
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.APIVersion != b.APIVersion {
		return a.APIVersion < b.APIVersion
	}
	// namespace is more significant than name
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	// name is more significant than uid
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	// uid is included for completeness, but is expected to be identical
	// when getting alternate identities for an owner since they are keyed by uid
	if a.UID != b.UID {
		return a.UID < b.UID
	}
	return false
}

// getAlternateOwnerIdentity searches deps for owner references which match
// verifiedAbsentIdentity.UID but differ in apiVersion/kind/name or namespace.
// The first that follows verifiedAbsentIdentity (according to referenceLessThan) is returned.
// If none follow verifiedAbsentIdentity, the first (according to referenceLessThan) is returned.
// If no alternate identities are found, nil is returned.
func getAlternateOwnerIdentity(deps []*node, verifiedAbsentIdentity objectReference) *objectReference {
	absentIdentityIsClusterScoped := len(verifiedAbsentIdentity.Namespace) == 0

	seenAlternates := map[objectReference]bool{verifiedAbsentIdentity: true}

	// keep track of the first alternate reference (according to referenceLessThan)
	var first *objectReference
	// keep track of the first reference following verifiedAbsentIdentity (according to referenceLessThan)
	var firstFollowing *objectReference

	for _, dep := range deps {
		for _, ownerRef := range dep.owners {
			if ownerRef.UID != verifiedAbsentIdentity.UID {
				// skip references that aren't the uid we care about
				continue
			}

			if ownerReferenceMatchesCoordinates(ownerRef, verifiedAbsentIdentity.OwnerReference) {
				if absentIdentityIsClusterScoped || verifiedAbsentIdentity.Namespace == dep.identity.Namespace {
					// skip references that exactly match verifiedAbsentIdentity
					continue
				}
			}

			ref := objectReference{OwnerReference: ownerReferenceCoordinates(ownerRef), Namespace: dep.identity.Namespace}
			if absentIdentityIsClusterScoped && ref.APIVersion == verifiedAbsentIdentity.APIVersion && ref.Kind == verifiedAbsentIdentity.Kind {
				// we know this apiVersion/kind is cluster-scoped because of verifiedAbsentIdentity,
				// so clear the namespace from the alternate identity
				ref.Namespace = ""
			}

			if seenAlternates[ref] {
				// skip references we've already seen
				continue
			}
			seenAlternates[ref] = true

			if first == nil || referenceLessThan(ref, *first) {
				// this alternate comes first lexically
				first = &ref
			}
			if referenceLessThan(verifiedAbsentIdentity, ref) && (firstFollowing == nil || referenceLessThan(ref, *firstFollowing)) {
				// this alternate is the first following verifiedAbsentIdentity lexically
				firstFollowing = &ref
			}
		}
	}

	// return the first alternate identity following the verified absent identity, if there is one
	if firstFollowing != nil {
		return firstFollowing
	}
	// otherwise return the first alternate identity
	return first
}
