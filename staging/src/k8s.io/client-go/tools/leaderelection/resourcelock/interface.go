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

package resourcelock

import (
	"context"
	"fmt"
	"time"

	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	LeaderElectionRecordAnnotationKey = "control-plane.alpha.kubernetes.io/leader" // 这个字段在 lease 基本用不到
	endpointsResourceLock             = "endpoints"                                // 已经废弃
	configMapsResourceLock            = "configmaps"                               // 已经废弃
	LeasesResourceLock                = "leases"
	// When using EndpointsLeasesResourceLock, you need to ensure that
	// API Priority & Fairness is configured with non-default flow-schema
	// that will catch the necessary operations on leader-election related
	// endpoint objects.
	//
	// The example of such flow scheme could look like this:
	//   apiVersion: flowcontrol.apiserver.k8s.io/v1beta2
	//   kind: FlowSchema
	//   metadata:
	//     name: my-leader-election
	//   spec:
	//     distinguisherMethod:
	//       type: ByUser
	//     matchingPrecedence: 200
	//     priorityLevelConfiguration:
	//       name: leader-election   # reference the <leader-election> PL
	//     rules:
	//     - resourceRules:
	//       - apiGroups:
	//         - ""
	//         namespaces:
	//         - '*'
	//         resources:
	//         - endpoints
	//         verbs:
	//         - get
	//         - create
	//         - update
	//       subjects:
	//       - kind: ServiceAccount
	//         serviceAccount:
	//           name: '*'
	//           namespace: kube-system
	EndpointsLeasesResourceLock = "endpointsleases"
	// When using EndpointsLeasesResourceLock, you need to ensure that
	// API Priority & Fairness is configured with non-default flow-schema
	// that will catch the necessary operations on leader-election related
	// configmap objects.
	//
	// The example of such flow scheme could look like this:
	//   apiVersion: flowcontrol.apiserver.k8s.io/v1beta2
	//   kind: FlowSchema
	//   metadata:
	//     name: my-leader-election
	//   spec:
	//     distinguisherMethod:
	//       type: ByUser
	//     matchingPrecedence: 200
	//     priorityLevelConfiguration:
	//       name: leader-election   # reference the <leader-election> PL
	//     rules:
	//     - resourceRules:
	//       - apiGroups:
	//         - ""
	//         namespaces:
	//         - '*'
	//         resources:
	//         - configmaps
	//         verbs:
	//         - get
	//         - create
	//         - update
	//       subjects:
	//       - kind: ServiceAccount
	//         serviceAccount:
	//           name: '*'
	//           namespace: kube-system
	ConfigMapsLeasesResourceLock = "configmapsleases"
)

// LeaderElectionRecord is the record that is stored in the leader election annotation.
// This information should be used for observational purposes only and could be replaced
// with a random string (e.g. UUID) with only slight modification of this code.
// TODO(mikedanese): this should potentially be versioned
/*
//LeaderElectionRecord是存储在领导人选举注释中的记录。
//这些信息只能用于观测目的，可以替换
//带有一个随机字符串（例如UUID），只对该代码进行了轻微修改。
*/
type LeaderElectionRecord struct {
	// HolderIdentity is the ID that owns the lease. If empty, no one owns this lease and
	// all callers may acquire. Versions of this library prior to Kubernetes 1.14 will not
	// attempt to acquire leases with empty identities and will wait for the full lease
	// interval to expire before attempting to reacquire. This value is set to empty when
	// a client voluntarily steps down.
	HolderIdentity       string      `json:"holderIdentity"`       // 身份的唯一标识
	LeaseDurationSeconds int         `json:"leaseDurationSeconds"` // 租约周期
	AcquireTime          metav1.Time `json:"acquireTime"`          // 获取锁的时间
	RenewTime            metav1.Time `json:"renewTime"`            // 续订锁的时间
	LeaderTransitions    int         `json:"leaderTransitions"`    // leader 转换的次数
}

// EventRecorder records a change in the ResourceLock.
type EventRecorder interface {
	Eventf(obj runtime.Object, eventType, reason, message string, args ...interface{})
}

// ResourceLockConfig common data that exists across different
// resource locks
type ResourceLockConfig struct {
	// Identity is the unique string identifying a lease holder across
	// all participants in an election.
	Identity string
	// EventRecorder is optional.
	EventRecorder EventRecorder
}

// Interface offers a common interface for locking on arbitrary
// resources used in leader election.  The Interface is used
// to hide the details on specific implementations in order to allow
// them to change over time.  This interface is strictly for use
// by the leaderelection code.
type Interface interface {
	// Get returns the LeaderElectionRecord
	Get(ctx context.Context) (*LeaderElectionRecord, []byte, error)

	// Create attempts to create a LeaderElectionRecord
	Create(ctx context.Context, ler LeaderElectionRecord) error

	// Update will update and existing LeaderElectionRecord
	Update(ctx context.Context, ler LeaderElectionRecord) error

	// RecordEvent is used to record events
	RecordEvent(string)

	// Identity will return the locks Identity
	Identity() string

	// Describe is used to convert details on current resource lock
	// into a string
	Describe() string
}

// New Manufacture will create a lock of a given type according to the input parameters
// New 制造商将根据输入参数创建给定类型的锁
func New(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig) (Interface, error) {
	endpointsLock := &endpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coreClient,
		LockConfig: rlc,
	}
	configmapLock := &configMapLock{
		ConfigMapMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coreClient,
		LockConfig: rlc,
	}
	leaseLock := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coordinationClient,
		LockConfig: rlc,
	}
	switch lockType {
	case endpointsResourceLock:
		// migrate 迁移
		return nil, fmt.Errorf("endpoints lock is removed, migrate to %s", EndpointsLeasesResourceLock)
	case configMapsResourceLock:
		return nil, fmt.Errorf("configmaps lock is removed, migrate to %s", ConfigMapsLeasesResourceLock)
	case LeasesResourceLock:
		return leaseLock, nil
	case EndpointsLeasesResourceLock:
		return &MultiLock{
			Primary:   endpointsLock,
			Secondary: leaseLock,
		}, nil
	case ConfigMapsLeasesResourceLock:
		return &MultiLock{
			Primary:   configmapLock,
			Secondary: leaseLock,
		}, nil
	default:
		return nil, fmt.Errorf("Invalid lock-type %s", lockType)
	}
}

// NewFromKubeconfig will create a lock of a given type according to the input parameters.
// Timeout set for a client used to contact to Kubernetes should be lower than
// RenewDeadline to keep a single hung request from forcing a leader loss.
// Setting it to max(time.Second, RenewDeadline/2) as a reasonable heuristic.
func NewFromKubeconfig(lockType string, ns string, name string, rlc ResourceLockConfig, kubeconfig *restclient.Config, renewDeadline time.Duration) (Interface, error) {
	// shallow copy, do not modify the kubeconfig
	config := *kubeconfig
	// 设置Timeout, 范围在 max(time.Second, RenewDeadline/2)
	timeout := renewDeadline / 2
	if timeout < time.Second {
		timeout = time.Second
	}
	config.Timeout = timeout

	// 初始化 clientSet -> leaderElectionClient
	leaderElectionClient := clientset.NewForConfigOrDie(restclient.AddUserAgent(&config, "leader-election"))

	// 创建锁
	return New(lockType, ns, name, leaderElectionClient.CoreV1(), leaderElectionClient.CoordinationV1(), rlc)
}
