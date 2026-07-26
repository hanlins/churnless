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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

// ReplicaSetReconciler reconciles a ReplicaSet.
type ReplicaSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch

// Reconcile keeps a native ReplicaSet in sync and applies image updates to its Pods.
func (r *ReplicaSetReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	var workload appsv1alpha1.ReplicaSet
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		return ignoreNotFound(err)
	}
	if workload.Spec.Replicas == nil {
		before := workload.DeepCopy()
		replicas := int32(1)
		workload.Spec.Replicas = &replicas
		return ctrl.Result{Requeue: true}, r.Patch(ctx, &workload, client.MergeFrom(before))
	}

	selector, err := selectorString(workload.Spec.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("parse selector: %w", err)
	}
	shadow, created, err := r.syncReplicaSet(ctx, &workload)
	if err != nil {
		return ctrl.Result{}, err
	}
	if created {
		return ctrl.Result{Requeue: true}, nil
	}

	pods, err := listOwnedPods(ctx, r.Client, &workload)
	if err != nil {
		return ctrl.Result{}, err
	}
	inPlace := !imagesEqual(&shadow.Spec.Template, &workload.Spec.Template)
	if inPlace {
		if err := updatePods(
			ctx,
			r.Client,
			pods,
			&workload.Spec.Template,
			allPods,
			1,
			nil,
		); err != nil {
			return ctrl.Result{}, err
		}
	}
	progress := calculateProgress(pods, &workload.Spec.Template, allPods)
	if err := r.updateReplicaSetStatus(ctx, &workload, shadow, selector, progress); err != nil {
		return ctrl.Result{}, err
	}
	if inPlace {
		return requeueWhileUpdating(progress), nil
	}
	return ctrl.Result{}, nil
}

func (r *ReplicaSetReconciler) syncReplicaSet(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
) (*appsv1.ReplicaSet, bool, error) {
	var shadow appsv1.ReplicaSet
	key := client.ObjectKeyFromObject(workload)
	if err := r.Get(ctx, key, &shadow); apierrors.IsNotFound(err) {
		shadow = appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: workload.Name, Namespace: workload.Namespace},
			Spec:       *workload.Spec.ReplicaSetSpec.DeepCopy(),
		}
		injectOwnership(&shadow.Spec.Template, workload, "ReplicaSet")
		r.Scheme.Default(&shadow)
		if err := controllerutil.SetControllerReference(workload, &shadow, r.Scheme); err != nil {
			return nil, false, err
		}
		if err := r.Create(ctx, &shadow); err != nil {
			return nil, false, fmt.Errorf("create native ReplicaSet: %w", err)
		}
		return &shadow, true, nil
	} else if err != nil {
		return nil, false, err
	}
	if !metav1.IsControlledBy(&shadow, workload) {
		return nil, false, fmt.Errorf(
			"native ReplicaSet %s/%s already exists and is not controlled by this resource",
			shadow.Namespace,
			shadow.Name,
		)
	}

	candidate := shadow.DeepCopy()
	candidate.Spec = *workload.Spec.ReplicaSetSpec.DeepCopy()
	if err := r.Update(ctx, candidate, client.DryRunAll); err != nil {
		return nil, false, fmt.Errorf("validate native ReplicaSet update: %w", err)
	}
	candidate.Spec.Template = *prepareShadowTemplate(
		&shadow.Spec.Template,
		&candidate.Spec.Template,
		workload,
		"ReplicaSet",
		true,
	)
	if apiequality.Semantic.DeepEqual(shadow.Spec, candidate.Spec) {
		return &shadow, false, nil
	}
	before := shadow.DeepCopy()
	shadow.Spec = candidate.Spec
	if err := r.Patch(ctx, &shadow, client.MergeFrom(before)); err != nil {
		return nil, false, fmt.Errorf("update native ReplicaSet: %w", err)
	}
	return &shadow, false, nil
}

func (r *ReplicaSetReconciler) updateReplicaSetStatus(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
	shadow *appsv1.ReplicaSet,
	selector string,
	progress podProgress,
) error {
	status := appsv1alpha1.ReplicaSetStatus{
		ReplicaSetStatus: *shadow.Status.DeepCopy(),
		Selector:         selector,
		InPlace:          inPlaceStatus(progress),
	}
	if shadow.Status.ObservedGeneration == shadow.Generation {
		status.ObservedGeneration = workload.Generation
	}
	if apiequality.Semantic.DeepEqual(workload.Status, status) {
		return nil
	}
	before := workload.DeepCopy()
	workload.Status = status
	return r.Status().Patch(ctx, workload, client.MergeFrom(before))
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.ReplicaSet{}).
		Owns(&appsv1.ReplicaSet{}).
		Watches(&corev1.Pod{}, podWatch("ReplicaSet")).
		Named("replicaset").
		Complete(r)
}
