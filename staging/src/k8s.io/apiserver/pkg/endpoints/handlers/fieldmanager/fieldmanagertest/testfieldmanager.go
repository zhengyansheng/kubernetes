/*
Copyright 2022 The Kubernetes Authors.

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

package fieldmanagertest

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager/internal/testing"
)

// TestFieldManager is a FieldManager that can be used in test to
// simulate the behavior of Server-Side Apply and field tracking. This
// also has a few methods to get a sense of the state of the object.
//
// This TestFieldManager uses a series of "fake" objects to simulate
// some behavior which come with the limitation that you can only use
// one version since there is no version conversion logic.
//
// You can use this rather than NewDefaultTestFieldManager if you want
// to specify either a sub-resource, or a set of modified Manager to
// test them specifically.
type TestFieldManager interface {
	// APIVersion of the object that we're tracking.
	APIVersion() string
	// Reset resets the state of the liveObject by resetting it to an empty object.
	Reset()
	// Live returns a copy of the current liveObject.
	Live() runtime.Object
	// Apply applies the given object on top of the current liveObj, for the
	// given manager and force flag.
	Apply(obj runtime.Object, manager string, force bool) error
	// Update will updates the managed fields in the liveObj based on the
	// changes performed by the update.
	Update(obj runtime.Object, manager string) error
	// ManagedFields returns the list of existing managed fields for the
	// liveObj.
	ManagedFields() []metav1.ManagedFieldsEntry
}

// NewTestFieldManager returns a new TestFieldManager built for the
// given gvk, on the main resource.
func NewTestFieldManager(typeConverter fieldmanager.TypeConverter, gvk schema.GroupVersionKind) TestFieldManager {
	return testing.NewTestFieldManagerImpl(typeConverter, gvk, "", nil)
}

// NewTestFieldManagerSubresource returns a new TestFieldManager built
// for the given gvk, on the given sub-resource.
func NewTestFieldManagerSubresource(typeConverter fieldmanager.TypeConverter, gvk schema.GroupVersionKind, subresource string) TestFieldManager {
	return testing.NewTestFieldManagerImpl(typeConverter, gvk, subresource, nil)
}
