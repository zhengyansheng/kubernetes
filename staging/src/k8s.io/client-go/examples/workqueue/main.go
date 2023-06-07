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

package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// Controller demonstrates how to implement a controller with client-go.
type Controller struct {
	indexer  cache.Indexer
	queue    workqueue.RateLimitingInterface
	informer cache.Controller // cache.Controller 是 interface
}

// NewController creates a new Controller.
func NewController(queue workqueue.RateLimitingInterface, indexer cache.Indexer, informer cache.Controller) *Controller {
	return &Controller{
		informer: informer,
		indexer:  indexer,
		queue:    queue,
	}
}

func (c *Controller) processNextItem() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two pods with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	// Invoke the method containing the business logic
	err := c.syncToStdout(key.(string))
	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, key)
	return true
}

// syncToStdout is the business logic of the controller. In this controller it simply prints
// information about the pod to stdout. In case an error happened, it has to simply return the error.
// The retry logic should not be part of the business logic.
func (c *Controller) syncToStdout(key string) error {
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		klog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}

	if !exists {
		// Below we will warm up our cache with a Pod, so that we will see a delete for one pod
		fmt.Printf("Pod %s does not exist anymore\n", key)
	} else {
		// Note that you also have to check the uid if you have a local controlled resource, which
		// is dependent on the actual instance, to detect that a Pod was recreated with the same name
		//fmt.Printf("Sync/Add/Update for Pod %s\n", obj.(*v1.Event).GetName())
		e := obj.(*v1.Event)
		fmt.Println(e.Name)
		var eventName string
		if ok := strings.Contains(e.Name, "."); ok {
			eventName = strings.Split(e.Name, ".")[0]
		}
		// Type     Reason   Age                      From     Message
		fmt.Println(eventName, e.Namespace, e.Type, e.Reason, e.CreationTimestamp, e.Message)
	}
	return nil
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it stops trying.
	if c.queue.NumRequeues(key) < 5 {
		klog.Infof("Error syncing pod %v: %v", key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// Report to an external entity that, even after several retries, we could not successfully process this key
	runtime.HandleError(err)
	klog.Infof("Dropping pod %q out of the queue: %v", key, err)
}

// Run begins watching and syncing.
func (c *Controller) Run(workers int, stopCh chan struct{}) {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	klog.Info("Starting Pod controller")

	// 启动 informer (执行 ListAndWatch 和 processLoop)
	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	klog.Info("Stopping Pod controller")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func main1() {
	var kubeconfig string
	var master string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.Parse()

	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatal(err)
	}

	// create the pod watcher
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "pods", v1.NamespaceDefault, fields.Everything())

	// create the workqueue
	// 创建一个队列，用于存放需要处理的pod的key workqueue.RateLimitingInterface是一个接口，实现了Add,Done,Forget,NumRequeues方法
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Bind the workqueue to a cache with the help of an informer. This way we make sure that
	// whenever the cache is updated, the pod key is added to the workqueue.
	// Note that when we finally process the item from the workqueue, we might see a newer version
	// of the Pod than the version which was responsible for triggering the update.
	// NewIndexerInformer 这个地方是创建一个informer，这个informer会去监听podListWatcher，当podListWatcher发生变化时，会将变化的pod放入队列
	indexer, informer := cache.NewIndexerInformer(podListWatcher, &v1.Pod{}, time.Second*5, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// obj 是一个pod对象，这里的key是namespace/name
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				// 将key放入队列
				queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			// 同 AddFunc
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this key function.
			// DeletionHandlingMetaNamespaceKeyFunc is a wrapper around MetaNamespaceKeyFunc which handles
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}, cache.Indexers{})

	/*
		queue: RateLimitingQueue
		indexer: 在map的基础上封装了一些方法
		informer: Controller interface
	*/
	controller := NewController(queue, indexer, informer)

	//// We can now warm up the cache for initial synchronization.
	//// Let's suppose that we knew about a pod "mypod" on our last run, therefore add it to the cache.
	//// If this pod is not there anymore, the controller will be notified about the removal after the
	//// cache has synchronized.
	//indexer.Add(&v1.Pod{
	//	ObjectMeta: meta_v1.ObjectMeta{
	//		Name:      "mypod",
	//		Namespace: v1.NamespaceDefault,
	//	},
	//})

	// Now let's start the controller
	stop := make(chan struct{})
	defer close(stop)
	go controller.Run(1, stop)

	// Wait forever
	select {}
}

func main() {
	var kubeconfig string
	var master string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.Parse()

	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatal(err)
	}

	// create the pod watcher
	eventListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "events", v1.NamespaceDefault, fields.Everything())

	// create the workqueue
	// 创建一个队列，用于存放需要处理的pod的key workqueue.RateLimitingInterface是一个接口，实现了Add,Done,Forget,NumRequeues方法
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Bind the workqueue to a cache with the help of an informer. This way we make sure that
	// whenever the cache is updated, the pod key is added to the workqueue.
	// Note that when we finally process the item from the workqueue, we might see a newer version
	// of the Pod than the version which was responsible for triggering the update.
	// NewIndexerInformer 这个地方是创建一个informer，这个informer会去监听podListWatcher，当podListWatcher发生变化时，会将变化的pod放入队列
	indexer, informer := cache.NewIndexerInformer(eventListWatcher, &v1.Event{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// obj 是一个pod对象，这里的key是namespace/name
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				// 将key放入队列
				queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			// 同 AddFunc
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this key function.
			// DeletionHandlingMetaNamespaceKeyFunc is a wrapper around MetaNamespaceKeyFunc which handles
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}, cache.Indexers{})

	/*
		queue: RateLimitingQueue
		indexer: 在map的基础上封装了一些方法
		informer: Controller interface
	*/
	controller := NewController(queue, indexer, informer)

	//// We can now warm up the cache for initial synchronization.
	//// Let's suppose that we knew about a pod "mypod" on our last run, therefore add it to the cache.
	//// If this pod is not there anymore, the controller will be notified about the removal after the
	//// cache has synchronized.
	//indexer.Add(&v1.Pod{
	//	ObjectMeta: meta_v1.ObjectMeta{
	//		Name:      "mypod",
	//		Namespace: v1.NamespaceDefault,
	//	},
	//})

	// Now let's start the controller
	stop := make(chan struct{})
	defer close(stop)
	go controller.Run(1, stop)

	// Wait forever
	select {}
}

func main2() {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	queue.Add("one")
	queue.Add("two")

	// 非阻塞
	fmt.Println(queue.Get())
	fmt.Println(queue.Get())

	go func() {
		for i := 1; i < 5; i++ {
			// 限速添加 call .When() to add an item to the queue with a delay
			queue.AddRateLimited(i)
		}
		fmt.Println("add rate limited done")
	}()
	for {
		item, shutdown := queue.Get()
		if shutdown {
			return
		}
		//_ = item
		fmt.Println(time.Now(), item)
	}

}
