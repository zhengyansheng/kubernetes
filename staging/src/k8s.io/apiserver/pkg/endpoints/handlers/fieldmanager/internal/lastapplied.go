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

package internal

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime"
)

// SetLastApplied sets the last-applied annotation the given value in
// the object.
func SetLastApplied(obj runtime.Object, value string) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}
	var annotations = accessor.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[corev1.LastAppliedConfigAnnotation] = value
	if err := apimachineryvalidation.ValidateAnnotationsSize(annotations); err != nil {
		delete(annotations, corev1.LastAppliedConfigAnnotation)
	}
	accessor.SetAnnotations(annotations)
	return nil
}
