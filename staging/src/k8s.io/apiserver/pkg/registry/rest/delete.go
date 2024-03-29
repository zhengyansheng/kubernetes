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

package rest

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	utilpointer "k8s.io/utils/pointer"
)

// RESTDeleteStrategy defines deletion behavior on an object that follows Kubernetes
// API conventions.
type RESTDeleteStrategy interface {
	runtime.ObjectTyper
}

type GarbageCollectionPolicy string

const (
	DeleteDependents GarbageCollectionPolicy = "DeleteDependents"
	OrphanDependents GarbageCollectionPolicy = "OrphanDependents"
	// Unsupported means that the resource knows that it cannot be GC'd, so the finalizers
	// should never be set in storage.
	Unsupported GarbageCollectionPolicy = "Unsupported"
)

// GarbageCollectionDeleteStrategy must be implemented by the registry that wants to
// orphan dependents by default.
type GarbageCollectionDeleteStrategy interface {
	// DefaultGarbageCollectionPolicy returns the default garbage collection behavior.
	DefaultGarbageCollectionPolicy(ctx context.Context) GarbageCollectionPolicy
}

// RESTGracefulDeleteStrategy must be implemented by the registry that supports
// graceful deletion.
type RESTGracefulDeleteStrategy interface {
	// CheckGracefulDelete should return true if the object can be gracefully deleted and set
	// any default values on the DeleteOptions.
	// NOTE: if return true, `options.GracePeriodSeconds` must be non-nil (nil will fail),
	// that's what tells the deletion how "graceful" to be.
	CheckGracefulDelete(ctx context.Context, obj runtime.Object, options *metav1.DeleteOptions) bool
}

// BeforeDelete tests whether the object can be gracefully deleted.
// If graceful is set, the object should be gracefully deleted.  If gracefulPending
// is set, the object has already been gracefully deleted (and the provided grace
// period is longer than the time to deletion). An error is returned if the
// condition cannot be checked or the gracePeriodSeconds is invalid. The options
// argument may be updated with default values if graceful is true. Second place
// where we set deletionTimestamp is pkg/registry/generic/registry/store.go.
// This function is responsible for setting deletionTimestamp during gracefulDeletion,
// other one for cascading deletions.
// BeforeDelete 测试对象是否可以正常删除。
// 如果设置了优雅，则应该优雅地删除对象。如果设置了 gracefulPending，则表示对象已被正常删除（并且提供的宽限期比删除时间长）。
// 如果无法检查条件或gracePeriodSeconds无效，则返回错误。如果优雅为true，则选项参数可能会更新为默认值。我们设置deletionTimestamp的第二个地方是pkg/registry/general/registry/store.go。
// 此函数负责在gracefulDelete期间设置deletionTimestamp，另一个用于级联删除。

func BeforeDelete(strategy RESTDeleteStrategy, ctx context.Context, obj runtime.Object, options *metav1.DeleteOptions) (graceful, gracefulPending bool, err error) {
	// gracefulPending 表示对象已经被删除
	objectMeta, gvk, kerr := objectMetaAndKind(strategy, obj)
	if kerr != nil {
		return false, false, kerr
	}
	// options ->  {TypeMeta:{Kind:DeleteOptions APIVersion:meta.k8s.io/v1} GracePeriodSeconds:<nil> Preconditions:&Preconditions{UID:*7bb04c95-b811-437c-bd4d-aa03573eff46,ResourceVersion:nil,} OrphanDependents:<nil> PropagationPolicy:<nil> DryRun:[]}
	if errs := validation.ValidateDeleteOptions(options); len(errs) > 0 {
		return false, false, errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "DeleteOptions"}, "", errs)
	}
	// Checking the Preconditions here to fail early. They'll be enforced later on when we actually do the deletion, too.
	if options.Preconditions != nil {
		if options.Preconditions.UID != nil && *options.Preconditions.UID != objectMeta.GetUID() {
			return false, false, errors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, objectMeta.GetName(), fmt.Errorf("the UID in the precondition (%s) does not match the UID in record (%s). The object might have been deleted and then recreated", *options.Preconditions.UID, objectMeta.GetUID()))
		}
		if options.Preconditions.ResourceVersion != nil && *options.Preconditions.ResourceVersion != objectMeta.GetResourceVersion() {
			return false, false, errors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, objectMeta.GetName(), fmt.Errorf("the ResourceVersion in the precondition (%s) does not match the ResourceVersion in record (%s). The object might have been modified", *options.Preconditions.ResourceVersion, objectMeta.GetResourceVersion()))
		}
	}

	// Negative values will be treated as the value `1s` on the delete path.
	// 如果gracePeriodSeconds为负，则将其设置为1s。
	if gracePeriodSeconds := options.GracePeriodSeconds; gracePeriodSeconds != nil && *gracePeriodSeconds < 0 {
		options.GracePeriodSeconds = utilpointer.Int64(1)
	}
	// 如果 deletionGracePeriodSeconds 为负，则将其设置为1s。
	if deletionGracePeriodSeconds := objectMeta.GetDeletionGracePeriodSeconds(); deletionGracePeriodSeconds != nil && *deletionGracePeriodSeconds < 0 {
		objectMeta.SetDeletionGracePeriodSeconds(utilpointer.Int64(1))
	}

	gracefulStrategy, ok := strategy.(RESTGracefulDeleteStrategy)
	if !ok {
		// If we're not deleting gracefully there's no point in updating Generation, as we won't update
		// the obcject before deleting it.
		return false, false, nil
	}
	// if the object is already being deleted, no need to update generation.
	// 如果对象已经被删除，则不需要更新Generation。
	if objectMeta.GetDeletionTimestamp() != nil {
		// if we are already being deleted, we may only shorten the deletion grace period
		// this means the object was gracefully deleted previously but deletionGracePeriodSeconds was not set,
		// so we force deletion immediately
		// IMPORTANT:
		// The deletion operation happens in two phases.
		// 1. Update to set DeletionGracePeriodSeconds and DeletionTimestamp
		// 2. Delete the object from storage.
		// If the update succeeds, but the delete fails (network error, internal storage error, etc.),
		// a resource was previously left in a state that was non-recoverable.  We
		// check if the existing stored resource has a grace period as 0 and if so
		// attempt to delete immediately in order to recover from this scenario.
		/*
			//如果我们已经被删除，我们可能只会缩短删除宽限期
			//这意味着该对象之前已被正常删除，但未设置deletionGracePeriodSeconds，
			//所以我们立即强制删除
			//重要：
			//删除操作分两个阶段进行。
			//1。更新以设置DeletionGracePeriodSeconds和DeletionTimestamp
			//2。从存储中删除对象。
			//如果更新成功，但删除失败（网络错误、内部存储错误等），
			//资源以前处于不可恢复的状态。我们检查现有存储资源的宽限期是否为0，如果是尝试立即删除以便从此场景中恢复。
		*/
		if objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() == 0 {
			return false, false, nil
		}
		// only a shorter grace period may be provided by a user
		// 用中文翻译下面这段代码
		// 只有用户可以提供更短的宽限期 ？
		// 如果用户提供的宽限期大于当前宽限期，那么就不需要更新宽限期
		// 如果用户提供的宽限期小于当前宽限期，那么就需要更新宽限期
		// 如果用户没有提供宽限期，那么就不需要更新宽限期
		// 如果用户提供的宽限期等于当前宽限期，那么就不需要更新宽限期
		if options.GracePeriodSeconds != nil {
			period := int64(*options.GracePeriodSeconds)
			if period >= *objectMeta.GetDeletionGracePeriodSeconds() {
				return false, true, nil
			}
			newDeletionTimestamp := metav1.NewTime(
				objectMeta.GetDeletionTimestamp().Add(-time.Second * time.Duration(*objectMeta.GetDeletionGracePeriodSeconds())).
					Add(time.Second * time.Duration(*options.GracePeriodSeconds)))
			objectMeta.SetDeletionTimestamp(&newDeletionTimestamp)
			objectMeta.SetDeletionGracePeriodSeconds(&period)
			return true, false, nil
		}
		// graceful deletion is pending, do nothing
		// 优雅的删除正在等待，什么都不做
		options.GracePeriodSeconds = objectMeta.GetDeletionGracePeriodSeconds()
		return false, true, nil
	}

	// `CheckGracefulDelete` will be implemented by specific strategy
	// 其实就是设置 options.GracePeriodSeconds 的值
	if !gracefulStrategy.CheckGracefulDelete(ctx, obj, options) {
		return false, false, nil
	}

	if options.GracePeriodSeconds == nil {
		return false, false, errors.NewInternalError(fmt.Errorf("options.GracePeriodSeconds should not be nil"))
	}

	now := metav1.NewTime(metav1.Now().Add(time.Second * time.Duration(*options.GracePeriodSeconds))) // 当前时间+宽限期

	// 设置当前对象的 metadata 属性 DeletionTimestamp, GracePeriodSeconds, Generation
	objectMeta.SetDeletionTimestamp(&now)                                // 删除时间戳
	objectMeta.SetDeletionGracePeriodSeconds(options.GracePeriodSeconds) // 删除宽限期
	// If it's the first graceful deletion we are going to set the DeletionTimestamp to non-nil.
	// Controllers of the object that's being deleted shouldn't take any nontrivial actions, hence its behavior changes.
	// Thus we need to bump object's Generation (if set). This handles generation bump during graceful deletion.
	// The bump for objects that don't support graceful deletion is handled in pkg/registry/generic/registry/store.go.
	if objectMeta.GetGeneration() > 0 {
		objectMeta.SetGeneration(objectMeta.GetGeneration() + 1)
	}

	return true, false, nil
}

// AdmissionToValidateObjectDeleteFunc returns a admission validate func for object deletion
func AdmissionToValidateObjectDeleteFunc(admit admission.Interface, staticAttributes admission.Attributes, objInterfaces admission.ObjectInterfaces) ValidateObjectFunc {
	mutatingAdmission, isMutatingAdmission := admit.(admission.MutationInterface)
	validatingAdmission, isValidatingAdmission := admit.(admission.ValidationInterface)

	mutating := isMutatingAdmission && mutatingAdmission.Handles(staticAttributes.GetOperation())
	validating := isValidatingAdmission && validatingAdmission.Handles(staticAttributes.GetOperation())

	return func(ctx context.Context, old runtime.Object) error {
		if !mutating && !validating {
			return nil
		}
		finalAttributes := admission.NewAttributesRecord(
			nil,
			// Deep copy the object to avoid accidentally changing the object.
			old.DeepCopyObject(),
			staticAttributes.GetKind(),
			staticAttributes.GetNamespace(),
			staticAttributes.GetName(),
			staticAttributes.GetResource(),
			staticAttributes.GetSubresource(),
			staticAttributes.GetOperation(),
			staticAttributes.GetOperationOptions(),
			staticAttributes.IsDryRun(),
			staticAttributes.GetUserInfo(),
		)
		if mutating {
			if err := mutatingAdmission.Admit(ctx, finalAttributes, objInterfaces); err != nil {
				return err
			}
		}
		if validating {
			if err := validatingAdmission.Validate(ctx, finalAttributes, objInterfaces); err != nil {
				return err
			}
		}
		return nil
	}
}
