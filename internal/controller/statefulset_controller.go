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

// StatefulSetReconciler reconciles a StatefulSet.
type StatefulSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.churnless.io,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.churnless.io,resources=statefulsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.churnless.io,resources=statefulsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch

// Reconcile keeps a native StatefulSet in sync and applies rolling image updates to its Pods.
func (r *StatefulSetReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	var workload appsv1alpha1.StatefulSet
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
	shadow, created, err := r.syncStatefulSet(ctx, &workload)
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
	partition := int32(0)
	if shadow.Spec.UpdateStrategy.RollingUpdate != nil &&
		shadow.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
		partition = *shadow.Spec.UpdateStrategy.RollingUpdate.Partition
	}
	eligible := statefulSetEligible(partition)
	inPlace := !imagesEqual(&shadow.Spec.Template, &workload.Spec.Template)
	if inPlace {
		if err := updatePods(
			ctx,
			r.Client,
			pods,
			&workload.Spec.Template,
			eligible,
			1,
			statefulSetOrder,
		); err != nil {
			return ctrl.Result{}, err
		}
	}
	progress := calculateProgress(pods, &workload.Spec.Template, eligible)
	if err := r.updateStatefulSetStatus(
		ctx,
		&workload,
		shadow,
		selector,
		progress,
		inPlace,
		partition,
	); err != nil {
		return ctrl.Result{}, err
	}
	if inPlace {
		return requeueWhileUpdating(progress), nil
	}
	return ctrl.Result{}, nil
}

func (r *StatefulSetReconciler) syncStatefulSet(
	ctx context.Context,
	workload *appsv1alpha1.StatefulSet,
) (*appsv1.StatefulSet, bool, error) {
	var shadow appsv1.StatefulSet
	key := client.ObjectKeyFromObject(workload)
	if err := r.Get(ctx, key, &shadow); apierrors.IsNotFound(err) {
		shadow = appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: workload.Name, Namespace: workload.Namespace},
			Spec:       *workload.Spec.StatefulSetSpec.DeepCopy(),
		}
		injectOwnership(&shadow.Spec.Template, workload, "StatefulSet")
		r.Scheme.Default(&shadow)
		if err := controllerutil.SetControllerReference(workload, &shadow, r.Scheme); err != nil {
			return nil, false, err
		}
		if err := r.Create(ctx, &shadow); err != nil {
			return nil, false, fmt.Errorf("create native StatefulSet: %w", err)
		}
		return &shadow, true, nil
	} else if err != nil {
		return nil, false, err
	}
	if !metav1.IsControlledBy(&shadow, workload) {
		return nil, false, fmt.Errorf(
			"native StatefulSet %s/%s already exists and is not controlled by this resource",
			shadow.Namespace,
			shadow.Name,
		)
	}

	candidate := shadow.DeepCopy()
	candidate.Spec = *workload.Spec.StatefulSetSpec.DeepCopy()
	if err := r.Update(ctx, candidate, client.DryRunAll); err != nil {
		return nil, false, fmt.Errorf("validate native StatefulSet update: %w", err)
	}
	preserveImages := workload.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType
	candidate.Spec.Template = *prepareShadowTemplate(
		&shadow.Spec.Template,
		&candidate.Spec.Template,
		workload,
		"StatefulSet",
		preserveImages,
	)
	if apiequality.Semantic.DeepEqual(shadow.Spec, candidate.Spec) {
		return &shadow, false, nil
	}
	before := shadow.DeepCopy()
	shadow.Spec = candidate.Spec
	if err := r.Patch(ctx, &shadow, client.MergeFrom(before)); err != nil {
		return nil, false, fmt.Errorf("update native StatefulSet: %w", err)
	}
	return &shadow, false, nil
}

func (r *StatefulSetReconciler) updateStatefulSetStatus(
	ctx context.Context,
	workload *appsv1alpha1.StatefulSet,
	shadow *appsv1.StatefulSet,
	selector string,
	progress podProgress,
	inPlace bool,
	partition int32,
) error {
	status := appsv1alpha1.StatefulSetStatus{
		StatefulSetStatus: *shadow.Status.DeepCopy(),
		Selector:          selector,
		InPlace:           inPlaceStatus(progress),
	}
	if shadow.Status.ObservedGeneration == shadow.Generation {
		status.ObservedGeneration = workload.Generation
	}
	if inPlace {
		status.UpdatedReplicas = progress.Updated
		status.UpdateRevision = progress.Revision
		if partition == 0 && progress.Total == shadow.Status.Replicas &&
			progress.Ready == progress.Total {
			status.CurrentReplicas = progress.Total
			status.CurrentRevision = progress.Revision
		}
	}
	if apiequality.Semantic.DeepEqual(workload.Status, status) {
		return nil
	}
	before := workload.DeepCopy()
	workload.Status = status
	return r.Status().Patch(ctx, workload, client.MergeFrom(before))
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.StatefulSet{}).
		Owns(&appsv1.StatefulSet{}).
		Watches(&corev1.Pod{}, podWatch("StatefulSet")).
		Named("statefulset").
		Complete(r)
}
