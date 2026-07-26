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
	"maps"
	"slices"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const inPlaceParallelismAnnotation = "apps.churnless.io/in-place-parallelism"

// ReplicaSetReconciler reconciles a ReplicaSet.
type ReplicaSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;patch;delete

// Reconcile makes the custom ReplicaSet the sole controller of its Pods.
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

	if removed, err := r.removeLegacyShadow(ctx, &workload); err != nil || removed {
		return ctrl.Result{Requeue: removed}, err
	}
	selector, err := metav1.LabelSelectorAsSelector(workload.Spec.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("parse selector: %w", err)
	}
	pods, changed, err := r.claimPods(ctx, &workload, selector)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}

	active, terminating := activeReplicaSetPods(pods)
	if changed, err := r.reconcileReplicas(ctx, &workload, active); err != nil || changed {
		return ctrl.Result{Requeue: changed}, err
	}
	if err := updatePods(
		ctx,
		r.Client,
		active,
		&workload.Spec.Template,
		allPods,
		replicaSetParallelism(&workload),
		nil,
	); err != nil {
		return ctrl.Result{}, err
	}

	progress := calculateProgress(active, &workload.Spec.Template, allPods)
	selectorText, err := selectorString(workload.Spec.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("format selector: %w", err)
	}
	if err := r.updateReplicaSetStatus(
		ctx,
		&workload,
		active,
		terminating,
		selectorText,
		progress,
	); err != nil {
		return ctrl.Result{}, err
	}
	result := requeueWhileUpdating(progress)
	if result.RequeueAfter == 0 &&
		availableReplicas(active, workload.Spec.MinReadySeconds) < readyReplicas(active) {
		result.RequeueAfter = time.Second
	}
	return result, nil
}

func (r *ReplicaSetReconciler) removeLegacyShadow(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
) (bool, error) {
	var shadow appsv1.ReplicaSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(workload), &shadow); apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !metav1.IsControlledBy(&shadow, workload) {
		return false, nil
	}
	if err := r.Delete(ctx, &shadow); client.IgnoreNotFound(err) != nil {
		return false, fmt.Errorf("delete legacy native ReplicaSet: %w", err)
	}
	return true, nil
}

func (r *ReplicaSetReconciler) claimPods(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
	selector labels.Selector,
) ([]corev1.Pod, bool, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(workload.Namespace)); err != nil {
		return nil, false, err
	}

	claimed := make([]corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		controlled := metav1.IsControlledBy(pod, workload)
		matches := selector.Matches(labels.Set(pod.Labels))
		switch {
		case controlled && !matches:
			before := pod.DeepCopy()
			pod.OwnerReferences = slices.DeleteFunc(
				pod.OwnerReferences,
				func(ref metav1.OwnerReference) bool {
					return ref.Controller != nil && *ref.Controller && ref.UID == workload.UID
				},
			)
			if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
				return nil, false, fmt.Errorf("release Pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			return nil, true, nil
		case controlled && matches:
			claimed = append(claimed, *pod)
		case !matches || metav1.GetControllerOf(pod) != nil || !workload.DeletionTimestamp.IsZero():
			continue
		default:
			before := pod.DeepCopy()
			if err := controllerutil.SetControllerReference(workload, pod, r.Scheme); err != nil {
				return nil, false, err
			}
			if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
				return nil, false, fmt.Errorf("adopt Pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			return nil, true, nil
		}
	}
	return claimed, false, nil
}

func activeReplicaSetPods(pods []corev1.Pod) (active []corev1.Pod, terminating int32) {
	active = make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		switch {
		case pods[i].Status.Phase == corev1.PodSucceeded ||
			pods[i].Status.Phase == corev1.PodFailed:
			continue
		case !pods[i].DeletionTimestamp.IsZero():
			terminating++
		default:
			active = append(active, pods[i])
		}
	}
	return active, terminating
}

func (r *ReplicaSetReconciler) reconcileReplicas(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
	pods []corev1.Pod,
) (bool, error) {
	diff := int(desiredReplicas(workload.Spec.Replicas)) - len(pods)
	switch {
	case diff > 0:
		for range diff {
			if err := r.createPod(ctx, workload); err != nil {
				return false, err
			}
		}
		return true, nil
	case diff < 0:
		slices.SortStableFunc(pods, compareScaleDownPods)
		for i := range -diff {
			if err := r.Delete(ctx, &pods[i]); client.IgnoreNotFound(err) != nil {
				return false, fmt.Errorf("delete Pod %s/%s: %w", pods[i].Namespace, pods[i].Name, err)
			}
		}
		return true, nil
	default:
		return false, nil
	}
}

func (r *ReplicaSetReconciler) createPod(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
) error {
	template := workload.Spec.Template.DeepCopy()
	annotations := maps.Clone(template.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[revisionAnnotation] = imageRevision(template)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: workload.Name + "-",
			Namespace:    workload.Namespace,
			Labels:       maps.Clone(template.Labels),
			Annotations:  annotations,
			Finalizers:   slices.Clone(template.Finalizers),
		},
		Spec: *template.Spec.DeepCopy(),
	}
	if err := controllerutil.SetControllerReference(workload, &pod, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, &pod); err != nil {
		return fmt.Errorf("create Pod for ReplicaSet %s/%s: %w", workload.Namespace, workload.Name, err)
	}
	return nil
}

func compareScaleDownPods(left, right corev1.Pod) int {
	leftReady, rightReady := isPodReady(&left), isPodReady(&right)
	switch {
	case leftReady != rightReady && !leftReady:
		return -1
	case leftReady != rightReady:
		return 1
	case left.CreationTimestamp.After(right.CreationTimestamp.Time):
		return -1
	case right.CreationTimestamp.After(left.CreationTimestamp.Time):
		return 1
	default:
		return 0
	}
}

func replicaSetParallelism(workload *appsv1alpha1.ReplicaSet) int {
	value := workload.Annotations[inPlaceParallelismAnnotation]
	if value == "" {
		return 1
	}
	parallelism, err := strconv.Atoi(value)
	if err != nil || parallelism < 0 {
		return 1
	}
	return parallelism
}

func (r *ReplicaSetReconciler) updateReplicaSetStatus(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
	pods []corev1.Pod,
	terminating int32,
	selector string,
	progress podProgress,
) error {
	status := appsv1alpha1.ReplicaSetStatus{
		ReplicaSetStatus: appsv1.ReplicaSetStatus{
			ObservedGeneration:   workload.Generation,
			Replicas:             int32(len(pods)),
			FullyLabeledReplicas: fullyLabeledReplicas(pods, &workload.Spec.Template),
			ReadyReplicas:        readyReplicas(pods),
			AvailableReplicas:    availableReplicas(pods, workload.Spec.MinReadySeconds),
			TerminatingReplicas:  &terminating,
		},
		Selector: selector,
		InPlace:  inPlaceStatus(progress),
	}
	if terminating == 0 {
		status.TerminatingReplicas = nil
	}
	if apiequality.Semantic.DeepEqual(workload.Status, status) {
		return nil
	}
	before := workload.DeepCopy()
	workload.Status = status
	return r.Status().Patch(ctx, workload, client.MergeFrom(before))
}

func fullyLabeledReplicas(pods []corev1.Pod, template *corev1.PodTemplateSpec) int32 {
	selector := labels.SelectorFromSet(template.Labels)
	var count int32
	for i := range pods {
		if selector.Matches(labels.Set(pods[i].Labels)) {
			count++
		}
	}
	return count
}

func readyReplicas(pods []corev1.Pod) int32 {
	var count int32
	for i := range pods {
		if isPodReady(&pods[i]) {
			count++
		}
	}
	return count
}

func availableReplicas(pods []corev1.Pod, minReadySeconds int32) int32 {
	var count int32
	now := time.Now()
	for i := range pods {
		for _, condition := range pods[i].Status.Conditions {
			if condition.Type != corev1.PodReady || condition.Status != corev1.ConditionTrue {
				continue
			}
			if minReadySeconds == 0 ||
				condition.LastTransitionTime.Add(time.Duration(minReadySeconds)*time.Second).Before(now) {
				count++
			}
			break
		}
	}
	return count
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.ReplicaSet{}).
		Owns(&corev1.Pod{}).
		Named("replicaset").
		Complete(r)
}
