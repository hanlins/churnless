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
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/kubecompat"
)

const (
	inPlaceParallelismAnnotation = "churnless.io/in-place-parallelism"
	podControllerUIDIndex        = ".metadata.controllerUID"
)

// ReplicaSetReconciler reconciles a ReplicaSet.
type ReplicaSetReconciler struct {
	client.Client
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	podOwnerIndexed bool
}

// +kubebuilder:rbac:groups=churnless.io,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=churnless.io,resources=replicasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=churnless.io,resources=replicasets/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/resize,verbs=update

// Reconcile makes the custom ReplicaSet the sole controller of its Pods.
func (r *ReplicaSetReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	var workload appsv1alpha1.ReplicaSet
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		return ignoreNotFound(err)
	}
	if !workload.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
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
	policy := mutablePodPolicyFor(workload.Annotations)
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
		policy,
	); err != nil {
		return ctrl.Result{}, err
	}

	progress := calculateProgress(
		active,
		&workload.Spec.Template,
		allPods,
		policy,
	)
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
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(workload), &shadow); apierrors.IsNotFound(err) {
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
	pods, err := r.podCandidates(ctx, workload, selector)
	if err != nil {
		return nil, false, err
	}

	claimed := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
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
			if err := r.Patch(
				ctx,
				pod,
				client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
			); err != nil {
				return nil, false, fmt.Errorf("release Pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			return nil, true, nil
		case controlled && matches:
			claimed = append(claimed, *pod)
		case !matches || metav1.GetControllerOf(pod) != nil || !workload.DeletionTimestamp.IsZero():
			continue
		default:
			if err := r.canAdopt(ctx, workload); err != nil {
				return nil, false, err
			}
			before := pod.DeepCopy()
			if err := controllerutil.SetControllerReference(workload, pod, r.Scheme); err != nil {
				return nil, false, err
			}
			if err := r.Patch(
				ctx,
				pod,
				client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
			); err != nil {
				return nil, false, fmt.Errorf("adopt Pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			return nil, true, nil
		}
	}
	return claimed, false, nil
}

func (r *ReplicaSetReconciler) podCandidates(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
	selector labels.Selector,
) ([]corev1.Pod, error) {
	if !r.podOwnerIndexed {
		var list corev1.PodList
		if err := r.reader().List(
			ctx,
			&list,
			client.InNamespace(workload.Namespace),
		); err != nil {
			return nil, err
		}
		return list.Items, nil
	}

	var matching corev1.PodList
	if err := r.APIReader.List(
		ctx,
		&matching,
		client.InNamespace(workload.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return nil, err
	}
	candidates := make(map[string]corev1.Pod, len(matching.Items))
	for i := range matching.Items {
		candidates[podKey(&matching.Items[i])] = matching.Items[i]
	}

	controlled, err := r.cachedControlledPods(ctx, workload.Namespace, workload.UID)
	if err != nil {
		return nil, err
	}
	for i := range controlled {
		key := podKey(&controlled[i])
		if _, ok := candidates[key]; ok {
			continue
		}
		var fresh corev1.Pod
		if err := r.APIReader.Get(
			ctx,
			client.ObjectKeyFromObject(&controlled[i]),
			&fresh,
		); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		candidates[key] = fresh
	}

	result := make([]corev1.Pod, 0, len(candidates))
	for _, pod := range candidates {
		result = append(result, pod)
	}
	slices.SortFunc(result, func(left, right corev1.Pod) int {
		return strings.Compare(podKey(&left), podKey(&right))
	})
	return result, nil
}

func (r *ReplicaSetReconciler) cachedControlledPods(
	ctx context.Context,
	namespace string,
	uid types.UID,
) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(
		ctx,
		&list,
		client.InNamespace(namespace),
		client.MatchingFields{podControllerUIDIndex: string(uid)},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func podControllerUID(object client.Object) []string {
	owner := metav1.GetControllerOf(object)
	if owner == nil {
		return nil
	}
	return []string{string(owner.UID)}
}

func (r *ReplicaSetReconciler) canAdopt(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
) error {
	var fresh appsv1alpha1.ReplicaSet
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(workload), &fresh); err != nil {
		return fmt.Errorf("recheck ReplicaSet before adoption: %w", err)
	}
	if fresh.UID != workload.UID {
		return fmt.Errorf(
			"ReplicaSet %s/%s was replaced: got UID %s, expected %s",
			workload.Namespace,
			workload.Name,
			fresh.UID,
			workload.UID,
		)
	}
	if !fresh.DeletionTimestamp.IsZero() {
		return fmt.Errorf("ReplicaSet %s/%s is being deleted", workload.Namespace, workload.Name)
	}
	return nil
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
		diff = min(diff, kubecompat.BurstReplicas)
		created, err := kubecompat.SlowStartBatch(
			diff,
			kubecompat.SlowStartInitialBatchSize,
			func() error {
				return r.createPod(ctx, workload)
			},
		)
		if err != nil {
			return created > 0, err
		}
		return true, nil
	case diff < 0:
		diff = max(diff, -kubecompat.BurstReplicas)
		related, err := r.relatedPods(ctx, workload)
		if err != nil {
			return false, err
		}
		ranks := podRanks(related)
		now := metav1.Now()
		slices.SortStableFunc(pods, func(left, right corev1.Pod) int {
			return kubecompat.ComparePodsForDeletion(
				&left,
				&right,
				ranks[podKey(&left)],
				ranks[podKey(&right)],
				now,
			)
		})
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

func (r *ReplicaSetReconciler) relatedPods(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
) ([]corev1.Pod, error) {
	relatedReplicaSets := map[types.UID]struct{}{workload.UID: {}}
	if owner := metav1.GetControllerOf(workload); owner != nil &&
		owner.APIVersion == appsv1alpha1.GroupVersion.String() &&
		owner.Kind == "Deployment" {
		var replicaSets appsv1alpha1.ReplicaSetList
		if err := r.reader().List(
			ctx,
			&replicaSets,
			client.InNamespace(workload.Namespace),
		); err != nil {
			return nil, err
		}
		for i := range replicaSets.Items {
			replicaSetOwner := metav1.GetControllerOf(&replicaSets.Items[i])
			if replicaSetOwner != nil && replicaSetOwner.UID == owner.UID {
				relatedReplicaSets[replicaSets.Items[i].UID] = struct{}{}
			}
		}
	}

	var candidates []corev1.Pod
	if !r.podOwnerIndexed {
		var list corev1.PodList
		if err := r.reader().List(ctx, &list, client.InNamespace(workload.Namespace)); err != nil {
			return nil, err
		}
		candidates = list.Items
	} else {
		for uid := range relatedReplicaSets {
			pods, err := r.cachedControlledPods(ctx, workload.Namespace, uid)
			if err != nil {
				return nil, err
			}
			candidates = append(candidates, pods...)
		}
	}

	related := make([]corev1.Pod, 0, len(candidates))
	for i := range candidates {
		pod := &candidates[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil {
			continue
		}
		if _, ok := relatedReplicaSets[owner.UID]; !ok {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded ||
			pod.Status.Phase == corev1.PodFailed ||
			!pod.DeletionTimestamp.IsZero() {
			continue
		}
		related = append(related, *pod)
	}
	return related, nil
}

func podRanks(pods []corev1.Pod) map[string]int {
	onNode := make(map[string]int)
	for i := range pods {
		onNode[pods[i].Spec.NodeName]++
	}
	ranks := make(map[string]int, len(pods))
	for i := range pods {
		ranks[podKey(&pods[i])] = onNode[pods[i].Spec.NodeName]
	}
	return ranks
}

func podKey(pod *corev1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

func (r *ReplicaSetReconciler) createPod(
	ctx context.Context,
	workload *appsv1alpha1.ReplicaSet,
) error {
	template := workload.Spec.Template.DeepCopy()
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: workload.Name + "-",
			Namespace:    workload.Namespace,
			Labels:       maps.Clone(template.Labels),
			Annotations:  maps.Clone(template.Annotations),
			Finalizers:   slices.Clone(template.Finalizers),
		},
		Spec: *template.Spec.DeepCopy(),
	}
	setPodMetadata(&pod, template)
	pod.Annotations[revisionAnnotation] = mutablePodPolicyFor(workload.Annotations).revision(template)
	if err := controllerutil.SetControllerReference(workload, &pod, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, &pod); err != nil {
		return fmt.Errorf("create Pod for ReplicaSet %s/%s: %w", workload.Namespace, workload.Name, err)
	}
	return nil
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
		if kubecompat.IsPodReady(&pods[i]) {
			count++
		}
	}
	return count
}

func availableReplicas(pods []corev1.Pod, minReadySeconds int32) int32 {
	var count int32
	now := metav1.Now()
	for i := range pods {
		if kubecompat.IsPodAvailable(&pods[i], minReadySeconds, now) {
			count++
		}
	}
	return count
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		podControllerUIDIndex,
		podControllerUID,
	); err != nil {
		return fmt.Errorf("index Pods by controller UID: %w", err)
	}
	r.podOwnerIndexed = true
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.ReplicaSet{}).
		Owns(&corev1.Pod{}).
		Named("replicaset").
		Complete(r)
}

func (r *ReplicaSetReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
