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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const (
	takeoverAnnotation = "churnless.io/takeover"
	handoffAnnotation  = "churnless.io/handoff"

	migrationIDAnnotation               = "churnless.io/migration-id"
	migrationSourceAnnotation           = "churnless.io/migration-source"
	migrationSourceGenerationAnnotation = "churnless.io/migration-source-generation"
	migrationOriginalPausedAnnotation   = "churnless.io/migration-original-paused"
	migrationPhaseAnnotation            = "churnless.io/migration-phase"
	migrationRoleAnnotation             = "churnless.io/migration-role"

	nativeDeploymentAPIVersion    = "apps/v1"
	churnlessDeploymentAPIVersion = "churnless.io/v1alpha1"
	deploymentKind                = "Deployment"
	nativeDeploymentSource        = nativeDeploymentAPIVersion + "," + deploymentKind
	churnlessDeploymentSource     = churnlessDeploymentAPIVersion + "," + deploymentKind
	migrationPhaseWarming         = "warming"
	migrationPhaseCutover         = "cutover"
	migrationRoleSource           = "source"
	migrationRoleTarget           = "target"
	annotationEnabledValue        = "true"

	sourceDeletionCost = "2147483647"
	targetDeletionCost = "-2147483647"
)

// MigrationReconciler transfers a stable Deployment between native Kubernetes
// and Churnless controllers while retaining the source Pods when possible.
type MigrationReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=churnless.io,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=churnless.io,resources=replicasets,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch

// Reconcile observes same-name native and Churnless Deployments. The target
// Deployment records durable migration state so a transfer resumes after a
// controller restart even if the source request annotation is removed.
func (r *MigrationReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	native, err := r.getNativeDeployment(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	churnless, err := r.getChurnlessDeployment(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case churnless != nil && churnless.Annotations[migrationSourceAnnotation] == nativeDeploymentSource:
		return r.reconcileTakeover(ctx, native, churnless)
	case native != nil && native.Annotations[migrationSourceAnnotation] == churnlessDeploymentSource:
		return r.reconcileHandoff(ctx, churnless, native)
	case native != nil && native.Annotations[takeoverAnnotation] == annotationEnabledValue:
		if churnless != nil {
			return ctrl.Result{}, fmt.Errorf(
				"take over native Deployment %s/%s: same-name Churnless Deployment already exists",
				native.Namespace,
				native.Name,
			)
		}
		return r.startTakeover(ctx, native)
	case churnless != nil && churnless.Annotations[handoffAnnotation] == annotationEnabledValue:
		if native != nil {
			return ctrl.Result{}, fmt.Errorf(
				"hand off Churnless Deployment %s/%s: same-name native Deployment already exists",
				churnless.Namespace,
				churnless.Name,
			)
		}
		return r.startHandoff(ctx, churnless)
	default:
		return ctrl.Result{}, nil
	}
}

func (r *MigrationReconciler) startTakeover(
	ctx context.Context,
	source *appsv1.Deployment,
) (ctrl.Result, error) {
	if !nativeDeploymentComplete(source) {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	target := &appsv1alpha1.Deployment{
		ObjectMeta: migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			nativeDeploymentSource,
		),
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: *source.Spec.DeepCopy()},
	}
	target.Spec.Paused = false
	if err := r.Create(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("create Churnless migration target: %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *MigrationReconciler) startHandoff(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
) (ctrl.Result, error) {
	complete, err := r.churnlessDeploymentComplete(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !complete {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	target := &appsv1.Deployment{
		ObjectMeta: migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			churnlessDeploymentSource,
		),
		Spec: *source.Spec.DeploymentSpec.DeepCopy(),
	}
	target.Spec.Paused = false
	if err := r.Create(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("create native migration target: %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}

func migrationTargetMetadata(
	source metav1.ObjectMeta,
	uid types.UID,
	generation int64,
	paused bool,
	sourceKind string,
) metav1.ObjectMeta {
	annotations := maps.Clone(source.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	for _, key := range []string{
		takeoverAnnotation,
		handoffAnnotation,
		migrationIDAnnotation,
		migrationSourceAnnotation,
		migrationSourceGenerationAnnotation,
		migrationOriginalPausedAnnotation,
		migrationPhaseAnnotation,
		migrationRoleAnnotation,
	} {
		delete(annotations, key)
	}
	annotations[migrationIDAnnotation] = string(uid)
	annotations[migrationSourceAnnotation] = sourceKind
	annotations[migrationSourceGenerationAnnotation] = strconv.FormatInt(generation, 10)
	annotations[migrationOriginalPausedAnnotation] = strconv.FormatBool(paused)
	annotations[migrationPhaseAnnotation] = migrationPhaseWarming
	return metav1.ObjectMeta{
		Name:        source.Name,
		Namespace:   source.Namespace,
		Labels:      maps.Clone(source.Labels),
		Annotations: annotations,
	}
}

func (r *MigrationReconciler) reconcileTakeover(
	ctx context.Context,
	source *appsv1.Deployment,
	target *appsv1alpha1.Deployment,
) (ctrl.Result, error) {
	if source != nil && source.DeletionTimestamp.IsZero() {
		if err := validateMigrationSource(target, source.UID, source.Generation); err != nil {
			return ctrl.Result{}, fmt.Errorf("continue takeover: %w", err)
		}
		if !nativeDeploymentComplete(source) {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}
	changed, err := r.retargetMigrationDependents(
		ctx,
		target.Namespace,
		target.Name,
		appsv1.SchemeGroupVersion.String(),
		appsv1alpha1.GroupVersion.String(),
	)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("retarget takeover dependents: %w", err)
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if source == nil {
		return r.finishTakeover(ctx, target)
	}
	if !source.DeletionTimestamp.IsZero() {
		return r.waitForNativeSourceDeletion(ctx, target)
	}

	targetReplicaSet, ready, err := r.takeoverTargetReplicaSet(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	sourceReplicaSets, err := r.nativeReplicaSetsControlledBy(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed, err = r.prepareMigration(
		ctx,
		string(source.UID),
		source.Namespace,
		nativeObjects(sourceReplicaSets),
		uidSetForNativeReplicaSets(sourceReplicaSets),
		targetReplicaSet.Spec.Selector,
		targetReplicaSet.UID,
	)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("prepare takeover: %w", err)
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if target.Annotations[migrationPhaseAnnotation] != migrationPhaseCutover {
		return ctrl.Result{Requeue: true}, r.setMigrationPhase(ctx, target, migrationPhaseCutover)
	}
	if err := r.deleteNativeSource(ctx, source, sourceReplicaSets); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *MigrationReconciler) reconcileHandoff(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
	target *appsv1.Deployment,
) (ctrl.Result, error) {
	if source != nil && source.DeletionTimestamp.IsZero() {
		if err := validateMigrationSource(target, source.UID, source.Generation); err != nil {
			return ctrl.Result{}, fmt.Errorf("continue handoff: %w", err)
		}
		complete, err := r.churnlessDeploymentComplete(ctx, source)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !complete {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}
	changed, err := r.retargetMigrationDependents(
		ctx,
		target.Namespace,
		target.Name,
		appsv1alpha1.GroupVersion.String(),
		appsv1.SchemeGroupVersion.String(),
	)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("retarget handoff dependents: %w", err)
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if source == nil {
		return r.finishHandoff(ctx, target)
	}
	if !source.DeletionTimestamp.IsZero() {
		return r.waitForChurnlessSourceDeletion(ctx, target)
	}

	targetReplicaSet, ready, err := r.handoffTargetReplicaSet(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	sourceReplicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed, err = r.prepareMigration(
		ctx,
		string(source.UID),
		source.Namespace,
		churnlessObjects(sourceReplicaSets),
		uidSetForChurnlessReplicaSets(sourceReplicaSets),
		targetReplicaSet.Spec.Selector,
		targetReplicaSet.UID,
	)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("prepare handoff: %w", err)
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if target.Annotations[migrationPhaseAnnotation] != migrationPhaseCutover {
		return ctrl.Result{Requeue: true}, r.setMigrationPhase(ctx, target, migrationPhaseCutover)
	}
	if err := r.deleteChurnlessSource(ctx, source, sourceReplicaSets); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func validateMigrationSource(target client.Object, uid types.UID, generation int64) error {
	if target.GetAnnotations()[migrationIDAnnotation] != string(uid) {
		return fmt.Errorf(
			"source UID changed: got %s, expected %s",
			uid,
			target.GetAnnotations()[migrationIDAnnotation],
		)
	}
	expectedGeneration := target.GetAnnotations()[migrationSourceGenerationAnnotation]
	if expectedGeneration != strconv.FormatInt(generation, 10) {
		return fmt.Errorf(
			"source generation changed: got %d, expected %s; remove the migration target and retry",
			generation,
			expectedGeneration,
		)
	}
	return nil
}

func (r *MigrationReconciler) prepareMigration(
	ctx context.Context,
	migrationID string,
	namespace string,
	sourceReplicaSets []client.Object,
	sourceReplicaSetUIDs map[types.UID]struct{},
	targetSelector *metav1.LabelSelector,
	targetReplicaSetUID types.UID,
) (bool, error) {
	for _, replicaSet := range sourceReplicaSets {
		if replicaSet.GetAnnotations()[migrationIDAnnotation] == migrationID {
			continue
		}
		before := replicaSet.DeepCopyObject().(client.Object)
		annotations := maps.Clone(replicaSet.GetAnnotations())
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[migrationIDAnnotation] = migrationID
		replicaSet.SetAnnotations(annotations)
		if err := r.Patch(ctx, replicaSet, client.MergeFrom(before)); err != nil {
			return false, fmt.Errorf("mark source ReplicaSet %s: %w", replicaSet.GetName(), err)
		}
		return true, nil
	}

	selector, err := metav1.LabelSelectorAsSelector(targetSelector)
	if err != nil {
		return false, fmt.Errorf("parse target ReplicaSet selector: %w", err)
	}
	pods, err := r.listPods(ctx, namespace)
	if err != nil {
		return false, err
	}
	sourcePods := podsControlledBy(pods, sourceReplicaSetUIDs)
	targetPods := podsControlledBy(pods, map[types.UID]struct{}{targetReplicaSetUID: {}})
	for i := range sourcePods {
		changed, err := r.preparePod(
			ctx,
			&sourcePods[i],
			migrationID,
			migrationRoleSource,
			sourceDeletionCost,
			targetSelector.MatchLabels,
		)
		if err != nil {
			return false, err
		}
		if changed {
			return true, nil
		}
		if !selector.Matches(labels.Set(sourcePods[i].Labels)) {
			return false, fmt.Errorf(
				"source Pod %s/%s cannot match target ReplicaSet selector",
				sourcePods[i].Namespace,
				sourcePods[i].Name,
			)
		}
	}
	for i := range targetPods {
		changed, err := r.preparePod(
			ctx,
			&targetPods[i],
			migrationID,
			migrationRoleTarget,
			targetDeletionCost,
			nil,
		)
		if err != nil {
			return false, err
		}
		if changed {
			return true, nil
		}
	}
	return false, nil
}

func (r *MigrationReconciler) preparePod(
	ctx context.Context,
	pod *corev1.Pod,
	migrationID, role, deletionCost string,
	requiredLabels map[string]string,
) (bool, error) {
	before := pod.DeepCopy()
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	maps.Copy(pod.Labels, requiredLabels)
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[migrationIDAnnotation] = migrationID
	pod.Annotations[migrationRoleAnnotation] = role
	pod.Annotations[corev1.PodDeletionCost] = deletionCost
	if apiequality.Semantic.DeepEqual(before.Labels, pod.Labels) &&
		maps.Equal(before.Annotations, pod.Annotations) {
		return false, nil
	}
	if err := r.Patch(
		ctx,
		pod,
		client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
	); err != nil {
		return false, fmt.Errorf("prepare Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return true, nil
}

func (r *MigrationReconciler) finishTakeover(
	ctx context.Context,
	target *appsv1alpha1.Deployment,
) (ctrl.Result, error) {
	migrationID := target.Annotations[migrationIDAnnotation]
	remaining, err := r.nativeReplicaSetsForMigration(ctx, target.Namespace, migrationID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(remaining) > 0 {
		if err := r.deleteNativeReplicaSets(ctx, remaining); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	complete, err := r.churnlessDeploymentComplete(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !complete {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	replicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	settled, changed, err := r.cleanupMigrationPods(
		ctx,
		target.Namespace,
		migrationID,
		uidSetForChurnlessReplicaSets(replicaSets),
		&target.Spec.Template,
		true,
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !settled || changed {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, r.finishMigrationTarget(ctx, target)
}

func (r *MigrationReconciler) waitForNativeSourceDeletion(
	ctx context.Context,
	target *appsv1alpha1.Deployment,
) (ctrl.Result, error) {
	remaining, err := r.nativeReplicaSetsForMigration(
		ctx,
		target.Namespace,
		target.Annotations[migrationIDAnnotation],
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.deleteNativeReplicaSets(ctx, remaining); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *MigrationReconciler) finishHandoff(
	ctx context.Context,
	target *appsv1.Deployment,
) (ctrl.Result, error) {
	migrationID := target.Annotations[migrationIDAnnotation]
	remaining, err := r.churnlessReplicaSetsForMigration(ctx, target.Namespace, migrationID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(remaining) > 0 {
		if err := r.deleteChurnlessReplicaSets(ctx, remaining); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if !nativeDeploymentComplete(target) {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	replicaSets, err := r.nativeReplicaSetsControlledBy(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	settled, changed, err := r.cleanupMigrationPods(
		ctx,
		target.Namespace,
		migrationID,
		uidSetForNativeReplicaSets(replicaSets),
		&target.Spec.Template,
		false,
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !settled || changed {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, r.finishMigrationTarget(ctx, target)
}

func (r *MigrationReconciler) waitForChurnlessSourceDeletion(
	ctx context.Context,
	target *appsv1.Deployment,
) (ctrl.Result, error) {
	remaining, err := r.churnlessReplicaSetsForMigration(
		ctx,
		target.Namespace,
		target.Annotations[migrationIDAnnotation],
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.deleteChurnlessReplicaSets(ctx, remaining); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *MigrationReconciler) cleanupMigrationPods(
	ctx context.Context,
	namespace, migrationID string,
	targetReplicaSetUIDs map[types.UID]struct{},
	template *corev1.PodTemplateSpec,
	takeover bool,
) (settled, changed bool, err error) {
	pods, err := r.listPods(ctx, namespace)
	if err != nil {
		return false, false, err
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Annotations[migrationIDAnnotation] != migrationID {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if !pod.DeletionTimestamp.IsZero() || owner == nil {
			return false, false, nil
		}
		if _, ok := targetReplicaSetUIDs[owner.UID]; !ok {
			return false, false, nil
		}
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Annotations[migrationIDAnnotation] != migrationID {
			continue
		}
		before := pod.DeepCopy()
		delete(pod.Annotations, migrationIDAnnotation)
		delete(pod.Annotations, migrationRoleAnnotation)
		if value, ok := template.Annotations[corev1.PodDeletionCost]; ok {
			pod.Annotations[corev1.PodDeletionCost] = value
		} else {
			delete(pod.Annotations, corev1.PodDeletionCost)
		}
		if takeover {
			delete(pod.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
		} else {
			delete(pod.Labels, structuralRevisionLabel)
			delete(pod.Annotations, revisionAnnotation)
			delete(pod.Annotations, managedLabelKeysAnnotation)
			delete(pod.Annotations, managedAnnotationKeysAnnotation)
		}
		if len(pod.Annotations) == 0 {
			pod.Annotations = nil
		}
		if err := r.Patch(
			ctx,
			pod,
			client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
		); err != nil {
			return false, false, fmt.Errorf("clean up Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return true, true, nil
	}
	return true, false, nil
}

func (r *MigrationReconciler) finishMigrationTarget(
	ctx context.Context,
	target client.Object,
) error {
	before := target.DeepCopyObject().(client.Object)
	annotations := maps.Clone(target.GetAnnotations())
	paused, err := strconv.ParseBool(annotations[migrationOriginalPausedAnnotation])
	if err != nil {
		return fmt.Errorf("parse original paused state: %w", err)
	}
	for _, key := range []string{
		migrationIDAnnotation,
		migrationSourceAnnotation,
		migrationSourceGenerationAnnotation,
		migrationOriginalPausedAnnotation,
		migrationPhaseAnnotation,
	} {
		delete(annotations, key)
	}
	target.SetAnnotations(annotations)
	switch deployment := target.(type) {
	case *appsv1.Deployment:
		deployment.Spec.Paused = paused
	case *appsv1alpha1.Deployment:
		deployment.Spec.Paused = paused
	default:
		return fmt.Errorf("unsupported migration target %T", target)
	}
	if err := r.Patch(ctx, target, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("finish migration target: %w", err)
	}
	return nil
}

func (r *MigrationReconciler) setMigrationPhase(
	ctx context.Context,
	target client.Object,
	phase string,
) error {
	before := target.DeepCopyObject().(client.Object)
	annotations := maps.Clone(target.GetAnnotations())
	annotations[migrationPhaseAnnotation] = phase
	target.SetAnnotations(annotations)
	return r.Patch(ctx, target, client.MergeFrom(before))
}

func (r *MigrationReconciler) takeoverTargetReplicaSet(
	ctx context.Context,
	target *appsv1alpha1.Deployment,
) (*appsv1alpha1.ReplicaSet, bool, error) {
	replicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, target)
	if err != nil {
		return nil, false, err
	}
	current := replicaSetForRevision(replicaSets, structuralRevision(target))
	if current == nil {
		return nil, false, nil
	}
	desired := desiredReplicas(target.Spec.Replicas)
	return current,
		desiredReplicas(current.Spec.Replicas) == desired &&
			current.Status.Replicas == desired,
		nil
}

func (r *MigrationReconciler) handoffTargetReplicaSet(
	ctx context.Context,
	target *appsv1.Deployment,
) (*appsv1.ReplicaSet, bool, error) {
	replicaSets, err := r.nativeReplicaSetsControlledBy(ctx, target)
	if err != nil {
		return nil, false, err
	}
	current := currentNativeReplicaSet(target, replicaSets)
	if current == nil {
		return nil, false, nil
	}
	desired := desiredReplicas(target.Spec.Replicas)
	return current,
		desiredReplicas(current.Spec.Replicas) == desired &&
			current.Status.Replicas == desired,
		nil
}

func currentNativeReplicaSet(
	deployment *appsv1.Deployment,
	replicaSets []*appsv1.ReplicaSet,
) *appsv1.ReplicaSet {
	matches := make([]*appsv1.ReplicaSet, 0, len(replicaSets))
	for _, replicaSet := range replicaSets {
		template := replicaSet.Spec.Template.DeepCopy()
		delete(template.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
		if apiequality.Semantic.DeepEqual(template, &deployment.Spec.Template) {
			matches = append(matches, replicaSet)
		}
	}
	if len(matches) == 0 {
		return nil
	}
	return slices.MaxFunc(matches, func(left, right *appsv1.ReplicaSet) int {
		return left.CreationTimestamp.Compare(right.CreationTimestamp.Time)
	})
}

func nativeDeploymentComplete(deployment *appsv1.Deployment) bool {
	desired := desiredReplicas(deployment.Spec.Replicas)
	return deployment.Status.ObservedGeneration >= deployment.Generation &&
		deployment.Status.UpdatedReplicas == desired &&
		deployment.Status.Replicas == desired &&
		deployment.Status.AvailableReplicas == desired
}

func (r *MigrationReconciler) churnlessDeploymentComplete(
	ctx context.Context,
	deployment *appsv1alpha1.Deployment,
) (bool, error) {
	replicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, deployment)
	if err != nil {
		return false, err
	}
	current := replicaSetForRevision(replicaSets, structuralRevision(deployment))
	if current == nil {
		return false, nil
	}
	return deploymentComplete(deployment, current, replicaSets), nil
}

func (r *MigrationReconciler) nativeReplicaSetsControlledBy(
	ctx context.Context,
	deployment *appsv1.Deployment,
) ([]*appsv1.ReplicaSet, error) {
	var list appsv1.ReplicaSetList
	if err := r.reader().List(ctx, &list, client.InNamespace(deployment.Namespace)); err != nil {
		return nil, err
	}
	result := make([]*appsv1.ReplicaSet, 0, len(list.Items))
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], deployment) {
			result = append(result, &list.Items[i])
		}
	}
	return result, nil
}

func (r *MigrationReconciler) churnlessReplicaSetsControlledBy(
	ctx context.Context,
	deployment *appsv1alpha1.Deployment,
) ([]*appsv1alpha1.ReplicaSet, error) {
	var list appsv1alpha1.ReplicaSetList
	if err := r.reader().List(ctx, &list, client.InNamespace(deployment.Namespace)); err != nil {
		return nil, err
	}
	result := make([]*appsv1alpha1.ReplicaSet, 0, len(list.Items))
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], deployment) {
			result = append(result, &list.Items[i])
		}
	}
	return result, nil
}

func (r *MigrationReconciler) nativeReplicaSetsForMigration(
	ctx context.Context,
	namespace, migrationID string,
) ([]*appsv1.ReplicaSet, error) {
	var list appsv1.ReplicaSetList
	if err := r.reader().List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	result := make([]*appsv1.ReplicaSet, 0)
	for i := range list.Items {
		if list.Items[i].Annotations[migrationIDAnnotation] == migrationID {
			result = append(result, &list.Items[i])
		}
	}
	return result, nil
}

func (r *MigrationReconciler) churnlessReplicaSetsForMigration(
	ctx context.Context,
	namespace, migrationID string,
) ([]*appsv1alpha1.ReplicaSet, error) {
	var list appsv1alpha1.ReplicaSetList
	if err := r.reader().List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	result := make([]*appsv1alpha1.ReplicaSet, 0)
	for i := range list.Items {
		if list.Items[i].Annotations[migrationIDAnnotation] == migrationID {
			result = append(result, &list.Items[i])
		}
	}
	return result, nil
}

func (r *MigrationReconciler) deleteNativeSource(
	ctx context.Context,
	source *appsv1.Deployment,
	replicaSets []*appsv1.ReplicaSet,
) error {
	if err := r.Delete(ctx, source, orphanDeleteOptions(source, true)); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("orphan native Deployment: %w", err)
	}
	return r.deleteNativeReplicaSets(ctx, replicaSets)
}

func (r *MigrationReconciler) deleteChurnlessSource(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
	replicaSets []*appsv1alpha1.ReplicaSet,
) error {
	if err := r.Delete(ctx, source, orphanDeleteOptions(source, true)); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("orphan Churnless Deployment: %w", err)
	}
	return r.deleteChurnlessReplicaSets(ctx, replicaSets)
}

func (r *MigrationReconciler) deleteNativeReplicaSets(
	ctx context.Context,
	replicaSets []*appsv1.ReplicaSet,
) error {
	for _, replicaSet := range replicaSets {
		if err := r.Delete(
			ctx,
			replicaSet,
			orphanDeleteOptions(replicaSet, false),
		); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("orphan native ReplicaSet %s/%s: %w", replicaSet.Namespace, replicaSet.Name, err)
		}
	}
	return nil
}

func (r *MigrationReconciler) deleteChurnlessReplicaSets(
	ctx context.Context,
	replicaSets []*appsv1alpha1.ReplicaSet,
) error {
	for _, replicaSet := range replicaSets {
		if err := r.Delete(
			ctx,
			replicaSet,
			orphanDeleteOptions(replicaSet, false),
		); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf(
				"orphan Churnless ReplicaSet %s/%s: %w",
				replicaSet.Namespace,
				replicaSet.Name,
				err,
			)
		}
	}
	return nil
}

func orphanDeleteOptions(object client.Object, includeResourceVersion bool) *client.DeleteOptions {
	policy := metav1.DeletePropagationOrphan
	uid := object.GetUID()
	preconditions := &metav1.Preconditions{UID: &uid}
	if includeResourceVersion {
		resourceVersion := object.GetResourceVersion()
		preconditions.ResourceVersion = &resourceVersion
	}
	return &client.DeleteOptions{
		PropagationPolicy: &policy,
		Preconditions:     preconditions,
	}
}

func (r *MigrationReconciler) listPods(
	ctx context.Context,
	namespace string,
) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.reader().List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func podsControlledBy(
	pods []corev1.Pod,
	controllerUIDs map[types.UID]struct{},
) []corev1.Pod {
	result := make([]corev1.Pod, 0)
	for i := range pods {
		owner := metav1.GetControllerOf(&pods[i])
		if owner == nil || !pods[i].DeletionTimestamp.IsZero() {
			continue
		}
		if _, ok := controllerUIDs[owner.UID]; ok {
			result = append(result, pods[i])
		}
	}
	return result
}

func nativeObjects(replicaSets []*appsv1.ReplicaSet) []client.Object {
	result := make([]client.Object, len(replicaSets))
	for i := range replicaSets {
		result[i] = replicaSets[i]
	}
	return result
}

func churnlessObjects(replicaSets []*appsv1alpha1.ReplicaSet) []client.Object {
	result := make([]client.Object, len(replicaSets))
	for i := range replicaSets {
		result[i] = replicaSets[i]
	}
	return result
}

func uidSetForNativeReplicaSets(replicaSets []*appsv1.ReplicaSet) map[types.UID]struct{} {
	result := make(map[types.UID]struct{}, len(replicaSets))
	for _, replicaSet := range replicaSets {
		result[replicaSet.UID] = struct{}{}
	}
	return result
}

func uidSetForChurnlessReplicaSets(replicaSets []*appsv1alpha1.ReplicaSet) map[types.UID]struct{} {
	result := make(map[types.UID]struct{}, len(replicaSets))
	for _, replicaSet := range replicaSets {
		result[replicaSet.UID] = struct{}{}
	}
	return result
}

func (r *MigrationReconciler) getNativeDeployment(
	ctx context.Context,
	key client.ObjectKey,
) (*appsv1.Deployment, error) {
	var deployment appsv1.Deployment
	if err := r.reader().Get(ctx, key, &deployment); apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &deployment, nil
}

func (r *MigrationReconciler) getChurnlessDeployment(
	ctx context.Context,
	key client.ObjectKey,
) (*appsv1alpha1.Deployment, error) {
	var deployment appsv1alpha1.Deployment
	if err := r.reader().Get(ctx, key, &deployment); apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &deployment, nil
}

// SetupWithManager sets up the migration controller with both Deployment GVKs.
func (r *MigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.Deployment{}).
		Watches(&appsv1.Deployment{}, &handler.EnqueueRequestForObject{}).
		Named("migration").
		Complete(r)
}

func (r *MigrationReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
