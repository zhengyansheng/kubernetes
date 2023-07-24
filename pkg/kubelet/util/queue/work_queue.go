/*
Copyright 2015 The Kubernetes Authors.

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

package queue

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
)

// WorkQueue allows queuing items with a timestamp. An item is
// considered ready to process if the timestamp has expired.
type WorkQueue interface {
	// GetWork dequeues and returns all ready items.
	GetWork() []types.UID
	// Enqueue inserts a new item or overwrites an existing item.
	Enqueue(item types.UID, delay time.Duration)
}

type basicWorkQueue struct {
	clock clock.Clock
	lock  sync.Mutex
	queue map[types.UID]time.Time
}

var _ WorkQueue = &basicWorkQueue{}

// NewBasicWorkQueue returns a new basic WorkQueue with the provided clock
func NewBasicWorkQueue(clock clock.Clock) WorkQueue {
	queue := make(map[types.UID]time.Time)
	return &basicWorkQueue{queue: queue, clock: clock}
}

func (q *basicWorkQueue) GetWork() []types.UID {
	// 加锁
	q.lock.Lock()
	defer q.lock.Unlock()

	// 当前时间
	now := q.clock.Now()
	
	// 遍历队列，如果队列中的时间小于当前时间，则将其添加到items中
	var items []types.UID
	for k, v := range q.queue {
		if v.Before(now) {
			items = append(items, k)
			delete(q.queue, k)
		}
	}
	return items
}

// Enqueue inserts a new item or overwrites an existing item.
func (q *basicWorkQueue) Enqueue(item types.UID, delay time.Duration) {
	// 加锁
	q.lock.Lock()
	defer q.lock.Unlock()

	// 添加到队列中
	q.queue[item] = q.clock.Now().Add(delay)
}
