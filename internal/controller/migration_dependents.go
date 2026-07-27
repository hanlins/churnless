/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	referenceSpecField       = "spec"
	referenceAPIVersionField = "apiVersion"
	referenceKindField       = "kind"
	referenceNameField       = "name"
	scaleTargetRefField      = "scaleTargetRef"
)

type migrationDependentReference struct {
	listGVK           schema.GroupVersionKind
	referencePath     []string
	defaultAPIVersion string
	defaultKind       string
}

var migrationDependentReferences = []migrationDependentReference{
	{
		listGVK: schema.GroupVersionKind{
			Group:   "keda.sh",
			Version: "v1alpha1",
			Kind:    "ScaledObjectList",
		},
		referencePath:     []string{referenceSpecField, scaleTargetRefField},
		defaultAPIVersion: nativeDeploymentAPIVersion,
		defaultKind:       deploymentKind,
	},
	{
		listGVK: schema.GroupVersionKind{
			Group:   "autoscaling.k8s.io",
			Version: "v1",
			Kind:    "VerticalPodAutoscalerList",
		},
		referencePath: []string{referenceSpecField, "targetRef"},
	},
	{
		listGVK: schema.GroupVersionKind{
			Group:   "autoscaling",
			Version: "v2",
			Kind:    "HorizontalPodAutoscalerList",
		},
		referencePath: []string{referenceSpecField, scaleTargetRefField},
	},
}

// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=autoscaling.k8s.io,resources=verticalpodautoscalers,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;patch

func (r *MigrationReconciler) retargetMigrationDependents(
	ctx context.Context,
	namespace, workload, sourceAPIVersion, targetAPIVersion string,
) (bool, error) {
	changed := false
	for _, reference := range migrationDependentReferences {
		var dependents unstructured.UnstructuredList
		dependents.SetGroupVersionKind(reference.listGVK)
		if err := r.reader().List(
			ctx,
			&dependents,
			client.InNamespace(namespace),
		); meta.IsNoMatchError(err) {
			continue
		} else if err != nil {
			return false, fmt.Errorf("list %s dependents: %w", reference.listGVK.Kind, err)
		}

		for i := range dependents.Items {
			dependent := &dependents.Items[i]
			before := dependent.DeepCopy()
			retargeted, err := retargetMigrationDependent(
				dependent,
				reference,
				workload,
				sourceAPIVersion,
				targetAPIVersion,
			)
			if err != nil {
				return false, fmt.Errorf(
					"read %s %s/%s target reference: %w",
					dependent.GetKind(),
					dependent.GetNamespace(),
					dependent.GetName(),
					err,
				)
			}
			if !retargeted {
				continue
			}
			if err := r.Patch(ctx, dependent, client.MergeFrom(before)); err != nil {
				return false, fmt.Errorf(
					"retarget %s %s/%s: %w",
					dependent.GetKind(),
					dependent.GetNamespace(),
					dependent.GetName(),
					err,
				)
			}
			changed = true
		}
	}
	return changed, nil
}

func retargetMigrationDependent(
	dependent *unstructured.Unstructured,
	reference migrationDependentReference,
	workload, sourceAPIVersion, targetAPIVersion string,
) (bool, error) {
	name, found, err := unstructured.NestedString(
		dependent.Object,
		append(reference.referencePath, referenceNameField)...,
	)
	if err != nil || !found || name != workload {
		return false, err
	}
	apiVersion, found, err := unstructured.NestedString(
		dependent.Object,
		append(reference.referencePath, referenceAPIVersionField)...,
	)
	if err != nil {
		return false, err
	}
	if !found {
		apiVersion = reference.defaultAPIVersion
	}
	kind, found, err := unstructured.NestedString(
		dependent.Object,
		append(reference.referencePath, referenceKindField)...,
	)
	if err != nil {
		return false, err
	}
	if !found {
		kind = reference.defaultKind
	}
	if apiVersion != sourceAPIVersion || kind != deploymentKind {
		return false, nil
	}
	if err := unstructured.SetNestedField(
		dependent.Object,
		targetAPIVersion,
		append(reference.referencePath, referenceAPIVersionField)...,
	); err != nil {
		return false, err
	}
	if err := unstructured.SetNestedField(
		dependent.Object,
		deploymentKind,
		append(reference.referencePath, referenceKindField)...,
	); err != nil {
		return false, err
	}
	return true, nil
}
