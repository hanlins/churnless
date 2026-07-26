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
	"crypto/sha256"
	"encoding/json"
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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/kubecompat"
)

const structuralRevisionLabel = "apps.churnless.io/structural-revision"

// DeploymentReconciler reconciles a Deployment.
type DeploymentReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.churnless.io,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.churnless.io,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.churnless.io,resources=deployments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps.churnless.io,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;delete

// Reconcile delegates Pod ownership to Churnless ReplicaSets.
func (r *DeploymentReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	var workload appsv1alpha1.Deployment
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

	replicaSets, err := r.listReplicaSets(ctx, &workload)
	if err != nil {
		return ctrl.Result{}, err
	}
	revision := structuralRevision(&workload.Spec.Template)
	current := replicaSetForRevision(replicaSets, revision)
	if workload.Spec.Paused && current == nil && len(replicaSets) > 0 {
		current = newestReplicaSet(replicaSets)
	}
	if current == nil {
		_, err = r.createReplicaSet(ctx, &workload, revision)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	parallelism, err := deploymentParallelism(&workload, replicaSets)
	if err != nil {
		return ctrl.Result{}, err
	}
	if workload.Spec.Paused {
		parallelism = 0
	}
	changed, err := r.syncReplicaSet(ctx, &workload, current, revision, parallelism)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.updateDeploymentStatus(ctx, &workload, current, replicaSets); err != nil {
		return ctrl.Result{}, err
	}
	if workload.Spec.Paused {
		return ctrl.Result{}, nil
	}
	changed, err = r.rollout(ctx, &workload, current, replicaSets)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if !deploymentComplete(&workload, current, replicaSets) {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *DeploymentReconciler) removeLegacyShadow(
	ctx context.Context,
	workload *appsv1alpha1.Deployment,
) (bool, error) {
	var shadow appsv1.Deployment
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(workload), &shadow); apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !metav1.IsControlledBy(&shadow, workload) {
		return false, nil
	}
	if err := r.Delete(ctx, &shadow); client.IgnoreNotFound(err) != nil {
		return false, fmt.Errorf("delete legacy native Deployment: %w", err)
	}
	return true, nil
}

func (r *DeploymentReconciler) listReplicaSets(
	ctx context.Context,
	workload *appsv1alpha1.Deployment,
) ([]*appsv1alpha1.ReplicaSet, error) {
	var list appsv1alpha1.ReplicaSetList
	if err := r.reader().List(ctx, &list, client.InNamespace(workload.Namespace)); err != nil {
		return nil, err
	}
	result := make([]*appsv1alpha1.ReplicaSet, 0, len(list.Items))
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], workload) {
			result = append(result, &list.Items[i])
		}
	}
	return result, nil
}

func structuralRevision(template *corev1.PodTemplateSpec) string {
	copy := cleanTemplate(template)
	clearImages(copy)
	data, _ := json.Marshal(copy)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:5])
}

func replicaSetForRevision(
	replicaSets []*appsv1alpha1.ReplicaSet,
	revision string,
) *appsv1alpha1.ReplicaSet {
	for i := range replicaSets {
		if replicaSets[i].Labels[structuralRevisionLabel] == revision {
			return replicaSets[i]
		}
	}
	return nil
}

func newestReplicaSet(replicaSets []*appsv1alpha1.ReplicaSet) *appsv1alpha1.ReplicaSet {
	return slices.MaxFunc(replicaSets, func(left, right *appsv1alpha1.ReplicaSet) int {
		return left.CreationTimestamp.Compare(right.CreationTimestamp.Time)
	})
}

func replicaSetName(deploymentName, revision string) string {
	const suffix = 1 + 10
	maxPrefix := 63 - suffix
	prefix := strings.TrimRight(deploymentName[:min(len(deploymentName), maxPrefix)], "-")
	return prefix + "-" + revision
}

func (r *DeploymentReconciler) createReplicaSet(
	ctx context.Context,
	workload *appsv1alpha1.Deployment,
	revision string,
) (*appsv1alpha1.ReplicaSet, error) {
	if workload.Spec.Selector == nil {
		return nil, fmt.Errorf("create Churnless ReplicaSet: spec.selector is required")
	}
	replicas := int32(0)
	selector := workload.Spec.Selector.DeepCopy()
	if selector.MatchLabels == nil {
		selector.MatchLabels = map[string]string{}
	}
	selector.MatchLabels[structuralRevisionLabel] = revision
	template := workload.Spec.Template.DeepCopy()
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	template.Labels[structuralRevisionLabel] = revision

	replicaSet := &appsv1alpha1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaSetName(workload.Name, revision),
			Namespace: workload.Namespace,
			Labels: map[string]string{
				structuralRevisionLabel: revision,
			},
			Annotations: map[string]string{inPlaceParallelismAnnotation: "0"},
		},
		Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
			Replicas:        &replicas,
			MinReadySeconds: workload.Spec.MinReadySeconds,
			Selector:        selector,
			Template:        *template,
		}},
	}
	if err := controllerutil.SetControllerReference(workload, replicaSet, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, replicaSet); err != nil {
		return nil, fmt.Errorf("create Churnless ReplicaSet: %w", err)
	}
	return replicaSet, nil
}

func (r *DeploymentReconciler) syncReplicaSet(
	ctx context.Context,
	workload *appsv1alpha1.Deployment,
	replicaSet *appsv1alpha1.ReplicaSet,
	revision string,
	parallelism int,
) (bool, error) {
	before := replicaSet.DeepCopy()
	if !workload.Spec.Paused {
		template := workload.Spec.Template.DeepCopy()
		if template.Labels == nil {
			template.Labels = map[string]string{}
		}
		template.Labels[structuralRevisionLabel] = revision
		replicaSet.Spec.Template = *template
	}
	replicaSet.Spec.MinReadySeconds = workload.Spec.MinReadySeconds
	if replicaSet.Annotations == nil {
		replicaSet.Annotations = map[string]string{}
	}
	replicaSet.Annotations[inPlaceParallelismAnnotation] = strconv.Itoa(parallelism)
	if apiequality.Semantic.DeepEqual(before.Spec, replicaSet.Spec) &&
		maps.Equal(before.Annotations, replicaSet.Annotations) {
		return false, nil
	}
	if err := r.Patch(ctx, replicaSet, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("update Churnless ReplicaSet: %w", err)
	}
	return true, nil
}

func deploymentParallelism(
	deployment *appsv1alpha1.Deployment,
	replicaSets []*appsv1alpha1.ReplicaSet,
) (int, error) {
	replicas := int(desiredReplicas(deployment.Spec.Replicas))
	if replicas == 0 {
		return 0, nil
	}
	if deployment.Spec.Strategy.Type == appsv1.RecreateDeploymentStrategyType {
		return replicas, nil
	}
	maxSurge, maxUnavailable := deploymentFenceposts(deployment)
	_, maximum, err := kubecompat.ResolveFenceposts(maxSurge, maxUnavailable, int32(replicas))
	if err != nil {
		return 0, fmt.Errorf("calculate maxUnavailable: %w", err)
	}
	available := int32(0)
	for i := range replicaSets {
		available += replicaSets[i].Status.AvailableReplicas
	}
	unavailable := max(replicas-int(available), 0)
	if maximum == 0 && unavailable == 0 {
		return 1, nil
	}
	return max(int(maximum)-unavailable, 0), nil
}

func (r *DeploymentReconciler) rollout(
	ctx context.Context,
	workload *appsv1alpha1.Deployment,
	current *appsv1alpha1.ReplicaSet,
	replicaSets []*appsv1alpha1.ReplicaSet,
) (bool, error) {
	if workload.Spec.Strategy.Type == appsv1.RecreateDeploymentStrategyType {
		for i := range replicaSets {
			if replicaSets[i].UID != current.UID && desiredReplicas(replicaSets[i].Spec.Replicas) > 0 {
				return r.scaleReplicaSet(ctx, replicaSets[i], 0)
			}
		}
		if totalOldReplicas(current, replicaSets) == 0 {
			return r.scaleReplicaSet(ctx, current, desiredReplicas(workload.Spec.Replicas))
		}
		return false, nil
	}

	desired := desiredReplicas(workload.Spec.Replicas)
	maxSurge, maxUnavailable, err := rollingLimits(workload, desired)
	if err != nil {
		return false, err
	}
	total := totalReplicaSpec(replicaSets)
	currentReplicas := desiredReplicas(current.Spec.Replicas)
	if currentReplicas < desired && total < desired+maxSurge {
		increase := min(desired-currentReplicas, desired+maxSurge-total)
		return r.scaleReplicaSet(ctx, current, currentReplicas+increase)
	}

	minAvailable := max(desired-maxUnavailable, 0)
	totalAvailable := totalAvailableReplicas(replicaSets)
	for i := range replicaSets {
		old := replicaSets[i]
		oldReplicas := desiredReplicas(old.Spec.Replicas)
		if old.UID == current.UID || oldReplicas == 0 {
			continue
		}
		unavailableOld := max(oldReplicas-old.Status.AvailableReplicas, 0)
		availableBudget := max(totalAvailable-minAvailable, 0)
		excess := max(total-desired, 0)
		needed := max(desired-currentReplicas, 0)
		decrease := min(oldReplicas, unavailableOld+availableBudget)
		if excess > 0 {
			decrease = min(decrease, excess)
		} else {
			decrease = min(decrease, needed)
		}
		if decrease > 0 {
			return r.scaleReplicaSet(ctx, old, oldReplicas-decrease)
		}
	}
	if currentReplicas != desired && totalOldReplicas(current, replicaSets) == 0 {
		return r.scaleReplicaSet(ctx, current, desired)
	}
	return false, nil
}

func rollingLimits(
	deployment *appsv1alpha1.Deployment,
	replicas int32,
) (int32, int32, error) {
	maxSurgeValue, maxUnavailableValue := deploymentFenceposts(deployment)
	maxSurge, maxUnavailable, err := kubecompat.ResolveFenceposts(
		maxSurgeValue,
		maxUnavailableValue,
		replicas,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve rolling update fenceposts: %w", err)
	}
	return maxSurge, min(maxUnavailable, replicas), nil
}

func deploymentFenceposts(
	deployment *appsv1alpha1.Deployment,
) (*intstr.IntOrString, *intstr.IntOrString) {
	surge := intstr.FromString("25%")
	unavailable := intstr.FromString("25%")
	if deployment.Spec.Strategy.RollingUpdate != nil {
		if deployment.Spec.Strategy.RollingUpdate.MaxSurge != nil {
			surge = *deployment.Spec.Strategy.RollingUpdate.MaxSurge
		}
		if deployment.Spec.Strategy.RollingUpdate.MaxUnavailable != nil {
			unavailable = *deployment.Spec.Strategy.RollingUpdate.MaxUnavailable
		}
	}
	return &surge, &unavailable
}

func (r *DeploymentReconciler) scaleReplicaSet(
	ctx context.Context,
	replicaSet *appsv1alpha1.ReplicaSet,
	replicas int32,
) (bool, error) {
	if desiredReplicas(replicaSet.Spec.Replicas) == replicas {
		return false, nil
	}
	before := replicaSet.DeepCopy()
	replicaSet.Spec.Replicas = &replicas
	if err := r.Patch(ctx, replicaSet, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("scale Churnless ReplicaSet %s/%s: %w", replicaSet.Namespace, replicaSet.Name, err)
	}
	return true, nil
}

func totalReplicaSpec(replicaSets []*appsv1alpha1.ReplicaSet) int32 {
	var total int32
	for i := range replicaSets {
		total += desiredReplicas(replicaSets[i].Spec.Replicas)
	}
	return total
}

func totalAvailableReplicas(replicaSets []*appsv1alpha1.ReplicaSet) int32 {
	var total int32
	for i := range replicaSets {
		total += replicaSets[i].Status.AvailableReplicas
	}
	return total
}

func totalOldReplicas(
	current *appsv1alpha1.ReplicaSet,
	replicaSets []*appsv1alpha1.ReplicaSet,
) int32 {
	var total int32
	for i := range replicaSets {
		if replicaSets[i].UID != current.UID {
			total += desiredReplicas(replicaSets[i].Spec.Replicas)
		}
	}
	return total
}

func (r *DeploymentReconciler) updateDeploymentStatus(
	ctx context.Context,
	workload *appsv1alpha1.Deployment,
	current *appsv1alpha1.ReplicaSet,
	replicaSets []*appsv1alpha1.ReplicaSet,
) error {
	selector, err := selectorString(workload.Spec.Selector)
	if err != nil {
		return fmt.Errorf("format selector: %w", err)
	}
	var status appsv1.DeploymentStatus
	status.ObservedGeneration = workload.Generation
	status.CollisionCount = workload.Status.CollisionCount
	var terminating int32
	for i := range replicaSets {
		status.Replicas += replicaSets[i].Status.Replicas
		status.ReadyReplicas += replicaSets[i].Status.ReadyReplicas
		status.AvailableReplicas += replicaSets[i].Status.AvailableReplicas
		if replicaSets[i].Status.TerminatingReplicas != nil {
			terminating += *replicaSets[i].Status.TerminatingReplicas
		}
	}
	if terminating > 0 {
		status.TerminatingReplicas = &terminating
	}
	progress := observedInPlaceStatus(current)
	status.UpdatedReplicas = progress.UpdatedReplicas
	status.UnavailableReplicas = max(status.Replicas-status.AvailableReplicas, 0)
	status.Conditions = deploymentConditions(workload, status)
	result := appsv1alpha1.DeploymentStatus{
		DeploymentStatus: status,
		Selector:         selector,
		InPlace:          progress,
	}
	if apiequality.Semantic.DeepEqual(workload.Status, result) {
		return nil
	}
	before := workload.DeepCopy()
	workload.Status = result
	return r.Status().Patch(ctx, workload, client.MergeFrom(before))
}

func deploymentConditions(
	workload *appsv1alpha1.Deployment,
	status appsv1.DeploymentStatus,
) []appsv1.DeploymentCondition {
	desired := desiredReplicas(workload.Spec.Replicas)
	minAvailable := desired
	if workload.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		_, maxUnavailable, err := rollingLimits(workload, desired)
		if err == nil {
			minAvailable = max(desired-maxUnavailable, 0)
		}
	}
	available := status.AvailableReplicas >= minAvailable
	availableCondition := appsv1.DeploymentCondition{
		Type:   appsv1.DeploymentAvailable,
		Status: corev1.ConditionFalse,
		Reason: "MinimumReplicasUnavailable",
		Message: fmt.Sprintf(
			"Deployment has %d available replicas, requires at least %d",
			status.AvailableReplicas,
			minAvailable,
		),
	}
	if available {
		availableCondition.Status = corev1.ConditionTrue
		availableCondition.Reason = "MinimumReplicasAvailable"
		availableCondition.Message = "Deployment has minimum availability"
	}

	progressingCondition := appsv1.DeploymentCondition{
		Type:   appsv1.DeploymentProgressing,
		Status: corev1.ConditionTrue,
		Reason: "ReplicaSetUpdated",
		Message: fmt.Sprintf(
			"Deployment is updating %d of %d replicas",
			status.UpdatedReplicas,
			desired,
		),
	}
	switch {
	case workload.Spec.Paused:
		progressingCondition.Status = corev1.ConditionUnknown
		progressingCondition.Reason = "DeploymentPaused"
		progressingCondition.Message = "Deployment is paused"
	case status.UpdatedReplicas == desired && status.AvailableReplicas == desired:
		progressingCondition.Reason = "NewReplicaSetAvailable"
		progressingCondition.Message = "Deployment rollout is complete"
	}
	result := make([]appsv1.DeploymentCondition, 0, len(workload.Status.Conditions)+2)
	for i := range workload.Status.Conditions {
		if workload.Status.Conditions[i].Type != appsv1.DeploymentAvailable &&
			workload.Status.Conditions[i].Type != appsv1.DeploymentProgressing {
			result = append(result, workload.Status.Conditions[i])
		}
	}
	return append(
		result,
		preserveConditionTime(workload.Status.Conditions, availableCondition),
		preserveConditionTime(workload.Status.Conditions, progressingCondition),
	)
}

func preserveConditionTime(
	existing []appsv1.DeploymentCondition,
	condition appsv1.DeploymentCondition,
) appsv1.DeploymentCondition {
	now := metav1.Now()
	condition.LastUpdateTime = now
	condition.LastTransitionTime = now
	for i := range existing {
		if existing[i].Type != condition.Type {
			continue
		}
		if existing[i].Status == condition.Status {
			condition.LastTransitionTime = existing[i].LastTransitionTime
		}
		if existing[i].Status == condition.Status &&
			existing[i].Reason == condition.Reason &&
			existing[i].Message == condition.Message {
			condition.LastUpdateTime = existing[i].LastUpdateTime
		}
		break
	}
	return condition
}

func deploymentComplete(
	workload *appsv1alpha1.Deployment,
	current *appsv1alpha1.ReplicaSet,
	replicaSets []*appsv1alpha1.ReplicaSet,
) bool {
	desired := desiredReplicas(workload.Spec.Replicas)
	progress := observedInPlaceStatus(current)
	return totalOldReplicas(current, replicaSets) == 0 &&
		desiredReplicas(current.Spec.Replicas) == desired &&
		progress.UpdatedReplicas == desired &&
		progress.ReadyUpdatedReplicas == desired &&
		current.Status.AvailableReplicas == desired
}

func observedInPlaceStatus(
	replicaSet *appsv1alpha1.ReplicaSet,
) *appsv1alpha1.InPlaceUpdateStatus {
	revision := imageRevision(&replicaSet.Spec.Template)
	if replicaSet.Status.ObservedGeneration < replicaSet.Generation ||
		replicaSet.Status.InPlace == nil ||
		replicaSet.Status.InPlace.Revision != revision {
		return &appsv1alpha1.InPlaceUpdateStatus{Revision: revision}
	}
	return replicaSet.Status.InPlace.DeepCopy()
}

// SetupWithManager sets up the controller with the Manager.
func (r *DeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.Deployment{}).
		Owns(&appsv1alpha1.ReplicaSet{}).
		Named("deployment").
		Complete(r)
}

func (r *DeploymentReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
