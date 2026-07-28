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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const (
	controllerAnnotation     = "churnless.io/controller"
	churnlessControllerValue = "churnless"
	nativeControllerValue    = "native"

	migrationIDAnnotation               = "churnless.io/migration-id"
	migrationModeAnnotation             = "churnless.io/migration-mode"
	migrationStateVersionAnnotation     = "churnless.io/migration-state-version"
	migrationSourceAnnotation           = "churnless.io/migration-source"
	migrationSourceGenerationAnnotation = "churnless.io/migration-source-generation"
	migrationOriginalPausedAnnotation   = "churnless.io/migration-original-paused"
	migrationOriginalDeletionCost       = "churnless.io/migration-original-deletion-cost"
	migrationPhaseAnnotation            = "churnless.io/migration-phase"
	migrationRoleAnnotation             = "churnless.io/migration-role"

	nativeDeploymentAPIVersion    = "apps/v1"
	churnlessDeploymentAPIVersion = "churnless.io/v1alpha1"
	deploymentKind                = "Deployment"
	nativeDeploymentSource        = nativeDeploymentAPIVersion + "," + deploymentKind
	churnlessDeploymentSource     = churnlessDeploymentAPIVersion + "," + deploymentKind
	migrationPhaseWarming         = "warming"
	migrationPhaseCutover         = "cutover"
	migrationModePreserve         = "preserve"
	migrationModeRecovery         = "recovery"
	migrationRoleSource           = "source"
	migrationRoleTarget           = "target"
	migrationStateVersion         = "1"
	annotationEnabledValue        = "true"

	sourceDeletionCost = "2147483647"
	targetDeletionCost = "-2147483647"

	originalDeletionCostAbsent        = "absent"
	originalDeletionCostPresentPrefix = "present:"

	migrationPollInterval        = time.Second
	migrationRolloutPollInterval = 2 * time.Second
)

// MigrationReconciler transfers a stable Deployment between native Kubernetes
// and Churnless controllers while retaining the source Pods when possible.
type MigrationReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
}

type podAdoptionTarget struct {
	object   client.Object
	selector *metav1.LabelSelector
}

type migrationCancellation struct {
	source            client.Object
	target            client.Object
	sourceReplicaSets []client.Object
	sourceController  string
	sourceAPIVersion  string
	targetAPIVersion  string
	targetDescription string
	addedPodLabel     string
	sourceTemplate    *corev1.PodTemplateSpec
	completionMessage string
}

// +kubebuilder:rbac:groups=churnless.io,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=churnless.io,resources=replicasets,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

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
	case native != nil && native.Annotations[controllerAnnotation] == churnlessControllerValue:
		if churnless != nil {
			r.event(
				native,
				corev1.EventTypeWarning,
				"MigrationBlocked",
				"Same-name Churnless Deployment already exists",
			)
			return ctrl.Result{}, fmt.Errorf(
				"take over native Deployment %s/%s: same-name Churnless Deployment already exists",
				native.Namespace,
				native.Name,
			)
		}
		return r.startTakeover(ctx, native)
	case churnless != nil && churnless.Annotations[controllerAnnotation] == nativeControllerValue:
		if native != nil {
			r.event(
				churnless,
				corev1.EventTypeWarning,
				"MigrationBlocked",
				"Same-name native Deployment already exists",
			)
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
	if !source.DeletionTimestamp.IsZero() {
		r.event(
			source,
			corev1.EventTypeWarning,
			"MigrationBlocked",
			"Cannot start takeover from a deleting native Deployment",
		)
		return ctrl.Result{}, nil
	}
	if !nativeDeploymentComplete(source) {
		r.event(
			source,
			corev1.EventTypeNormal,
			"MigrationPending",
			"Waiting for the native Deployment rollout to complete before takeover",
		)
		return ctrl.Result{RequeueAfter: migrationRolloutPollInterval}, nil
	}
	target := &appsv1alpha1.Deployment{
		ObjectMeta: migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			nativeDeploymentSource,
			migrationModePreserve,
		),
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: *source.Spec.DeepCopy()},
	}
	target.Spec.Paused = false
	created, err := r.createMigrationTarget(ctx, target)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create Churnless migration target: %w", err)
	}
	if !created {
		return ctrl.Result{Requeue: true}, nil
	}
	r.event(
		target,
		corev1.EventTypeNormal,
		"MigrationStarted",
		"Started migration from native Kubernetes to Churnless",
	)
	return ctrl.Result{Requeue: true}, nil
}

func (r *MigrationReconciler) startHandoff(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
) (ctrl.Result, error) {
	if !source.DeletionTimestamp.IsZero() {
		r.event(
			source,
			corev1.EventTypeWarning,
			"MigrationBlocked",
			"Cannot start handoff from a deleting Churnless Deployment",
		)
		return ctrl.Result{}, nil
	}
	complete, err := r.churnlessDeploymentComplete(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	mode := migrationModePreserve
	if !complete {
		mode = migrationModeRecovery
	}
	target := &appsv1.Deployment{
		ObjectMeta: migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			churnlessDeploymentSource,
			mode,
		),
		Spec: *source.Spec.DeploymentSpec.DeepCopy(),
	}
	target.Spec.Paused = false
	created, err := r.createMigrationTarget(ctx, target)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create native migration target: %w", err)
	}
	if !created {
		return ctrl.Result{Requeue: true}, nil
	}
	reason := "MigrationStarted"
	message := "Started identity-preserving migration from Churnless to native Kubernetes"
	if mode == migrationModeRecovery {
		reason = "RecoveryHandoffStarted"
		message = "Started recovery handoff to native Kubernetes without waiting for Churnless rollout completion"
	}
	r.event(target, corev1.EventTypeNormal, reason, message)
	return ctrl.Result{Requeue: true}, nil
}

// createMigrationTarget treats an existing deterministic target as progress by
// another driver. The next reconciliation validates its durable migration
// metadata before using it, so an unrelated same-name object still fails closed.
func (r *MigrationReconciler) createMigrationTarget(
	ctx context.Context,
	target client.Object,
) (bool, error) {
	err := r.Create(ctx, target)
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsAlreadyExists(err):
		return false, nil
	default:
		return false, err
	}
}

func migrationTargetMetadata(
	source metav1.ObjectMeta,
	uid types.UID,
	generation int64,
	paused bool,
	sourceKind string,
	mode string,
) metav1.ObjectMeta {
	annotations := maps.Clone(source.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	for _, key := range []string{
		migrationIDAnnotation,
		migrationModeAnnotation,
		migrationStateVersionAnnotation,
		migrationSourceAnnotation,
		migrationSourceGenerationAnnotation,
		migrationOriginalPausedAnnotation,
		migrationPhaseAnnotation,
		migrationRoleAnnotation,
	} {
		delete(annotations, key)
	}
	annotations[migrationIDAnnotation] = string(uid)
	annotations[migrationModeAnnotation] = mode
	annotations[migrationStateVersionAnnotation] = migrationStateVersion
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
	if err := validateMigrationTarget(target, nativeDeploymentSource, false); err != nil {
		return ctrl.Result{}, fmt.Errorf("continue takeover: %w", err)
	}
	if source != nil && source.DeletionTimestamp.IsZero() {
		if controllerRequested(nativeControllerValue, source, target) {
			return r.cancelTakeover(ctx, source, target)
		}
		if !target.DeletionTimestamp.IsZero() {
			return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
		}
		if err := validateMigrationSource(target, source.UID, source.Generation); err != nil {
			return ctrl.Result{}, fmt.Errorf("continue takeover: %w", err)
		}
		if !nativeDeploymentComplete(source) {
			return ctrl.Result{RequeueAfter: migrationRolloutPollInterval}, nil
		}
	} else if source != nil &&
		source.Annotations[controllerAnnotation] == nativeControllerValue &&
		target.Annotations[controllerAnnotation] != nativeControllerValue {
		if err := r.setAnnotation(
			ctx,
			target,
			controllerAnnotation,
			nativeControllerValue,
		); err != nil {
			return ctrl.Result{}, fmt.Errorf("record requested native fallback: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
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
		return ctrl.Result{RequeueAfter: migrationRolloutPollInterval}, nil
	}
	sourceReplicaSets, err := r.nativeReplicaSetsControlledBy(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed, err = r.prepareMigration(
		ctx,
		string(source.UID),
		source.Namespace,
		asClientObjects(sourceReplicaSets),
		uidSetFor(sourceReplicaSets),
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
		if err := r.setMigrationPhase(ctx, target, migrationPhaseCutover); err != nil {
			return ctrl.Result{}, err
		}
		r.event(
			target,
			corev1.EventTypeNormal,
			"MigrationCutover",
			"Starting Pod ownership cutover to Churnless",
		)
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.deleteNativeSource(ctx, source, sourceReplicaSets); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
}

func (r *MigrationReconciler) cancelTakeover(
	ctx context.Context,
	source *appsv1.Deployment,
	target *appsv1alpha1.Deployment,
) (ctrl.Result, error) {
	replicaSets, err := r.nativeReplicaSetsControlledBy(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.cancelMigration(ctx, migrationCancellation{
		source:            source,
		target:            target,
		sourceReplicaSets: asClientObjects(replicaSets),
		sourceController:  nativeControllerValue,
		sourceAPIVersion:  appsv1.SchemeGroupVersion.String(),
		targetAPIVersion:  appsv1alpha1.GroupVersion.String(),
		targetDescription: "Churnless migration target",
		addedPodLabel:     structuralRevisionLabel,
		sourceTemplate:    &source.Spec.Template,
		completionMessage: "Cancelled Churnless takeover; native Kubernetes remains authoritative",
	})
}

func (r *MigrationReconciler) cancelHandoff(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
	target *appsv1.Deployment,
) (ctrl.Result, error) {
	replicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.cancelMigration(ctx, migrationCancellation{
		source:            source,
		target:            target,
		sourceReplicaSets: asClientObjects(replicaSets),
		sourceController:  churnlessControllerValue,
		sourceAPIVersion:  appsv1alpha1.GroupVersion.String(),
		targetAPIVersion:  appsv1.SchemeGroupVersion.String(),
		targetDescription: "native migration target",
		addedPodLabel:     appsv1.DefaultDeploymentUniqueLabelKey,
		sourceTemplate:    &source.Spec.Template,
		completionMessage: "Cancelled native handoff; Churnless remains authoritative",
	})
}

func (r *MigrationReconciler) cancelMigration(
	ctx context.Context,
	cancellation migrationCancellation,
) (ctrl.Result, error) {
	changed, err := r.retargetMigrationDependents(
		ctx,
		cancellation.target.GetNamespace(),
		cancellation.target.GetName(),
		cancellation.targetAPIVersion,
		cancellation.sourceAPIVersion,
	)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("restore migration dependents: %w", err)
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if cancellation.source.GetAnnotations()[controllerAnnotation] != cancellation.sourceController {
		if err := r.setAnnotation(
			ctx,
			cancellation.source,
			controllerAnnotation,
			cancellation.sourceController,
		); err != nil {
			return ctrl.Result{}, fmt.Errorf("restore migration source controller: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	migrationID := cancellation.target.GetAnnotations()[migrationIDAnnotation]
	for _, replicaSet := range cancellation.sourceReplicaSets {
		if replicaSet.GetAnnotations()[migrationIDAnnotation] != migrationID {
			continue
		}
		before := replicaSet.DeepCopyObject().(client.Object)
		annotations := maps.Clone(replicaSet.GetAnnotations())
		delete(annotations, migrationIDAnnotation)
		replicaSet.SetAnnotations(annotations)
		if err := r.Patch(
			ctx,
			replicaSet,
			client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
		); err != nil {
			return ctrl.Result{}, fmt.Errorf(
				"restore source ReplicaSet %s/%s: %w",
				replicaSet.GetNamespace(),
				replicaSet.GetName(),
				err,
			)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	pods, err := r.listPods(ctx, cancellation.source.GetNamespace())
	if err != nil {
		return ctrl.Result{}, err
	}
	sourcePods := podsControlledBy(pods, uidSetFor(cancellation.sourceReplicaSets))
	for i := range sourcePods {
		if sourcePods[i].Annotations[migrationIDAnnotation] != migrationID {
			continue
		}
		if err := r.restoreCancelledMigrationPod(
			ctx,
			&sourcePods[i],
			cancellation.sourceTemplate,
			cancellation.addedPodLabel,
		); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if cancellation.target.GetDeletionTimestamp().IsZero() {
		if err := r.Delete(
			ctx,
			cancellation.target,
			foregroundDeleteOptions(cancellation.target, true),
		); err != nil {
			return ctrl.Result{}, fmt.Errorf(
				"delete cancelled %s: %w",
				cancellation.targetDescription,
				err,
			)
		}
	}
	r.event(
		cancellation.source,
		corev1.EventTypeNormal,
		"MigrationCancelled",
		cancellation.completionMessage,
	)
	return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
}

func (r *MigrationReconciler) restoreCancelledMigrationPod(
	ctx context.Context,
	pod *corev1.Pod,
	sourceTemplate *corev1.PodTemplateSpec,
	addedPodLabel string,
) error {
	before := pod.DeepCopy()
	restorePodTemplateLabel(pod, sourceTemplate, addedPodLabel)
	delete(pod.Annotations, migrationIDAnnotation)
	delete(pod.Annotations, migrationRoleAnnotation)
	restoreMigrationDeletionCost(pod, sourceTemplate)
	if len(pod.Annotations) == 0 {
		pod.Annotations = nil
	}
	if len(pod.Labels) == 0 {
		pod.Labels = nil
	}
	if err := r.Patch(
		ctx,
		pod,
		client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
	); err != nil {
		return fmt.Errorf("restore source Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

func (r *MigrationReconciler) reconcileHandoff(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
	target *appsv1.Deployment,
) (ctrl.Result, error) {
	if err := validateMigrationTarget(target, churnlessDeploymentSource, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("continue handoff: %w", err)
	}
	if result, proceed, err := r.prepareHandoffSource(ctx, source, target); err != nil || !proceed {
		return result, err
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
		if migrationModeFor(target) == migrationModeRecovery {
			return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
		}
		return r.waitForChurnlessSourceDeletion(ctx, target)
	}

	targetReplicaSet, ready, err := r.handoffTargetReplicaSet(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		return ctrl.Result{RequeueAfter: migrationRolloutPollInterval}, nil
	}
	if migrationModeFor(target) == migrationModeRecovery {
		if target.Annotations[migrationPhaseAnnotation] != migrationPhaseCutover {
			if err := r.setMigrationPhase(ctx, target, migrationPhaseCutover); err != nil {
				return ctrl.Result{}, err
			}
			r.event(
				target,
				corev1.EventTypeNormal,
				"MigrationCutover",
				"Starting recovery cutover to native Kubernetes",
			)
			return ctrl.Result{Requeue: true}, nil
		}
		if err := r.deleteChurnlessSourceForRecovery(ctx, source); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	sourceReplicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed, err = r.prepareMigration(
		ctx,
		string(source.UID),
		source.Namespace,
		asClientObjects(sourceReplicaSets),
		uidSetFor(sourceReplicaSets),
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
		if err := r.setMigrationPhase(ctx, target, migrationPhaseCutover); err != nil {
			return ctrl.Result{}, err
		}
		r.event(
			target,
			corev1.EventTypeNormal,
			"MigrationCutover",
			"Starting Pod ownership cutover to native Kubernetes",
		)
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.deleteChurnlessSource(ctx, source, sourceReplicaSets); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
}

func (r *MigrationReconciler) prepareHandoffSource(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
	target *appsv1.Deployment,
) (ctrl.Result, bool, error) {
	if source != nil && source.DeletionTimestamp.IsZero() {
		if controllerRequested(churnlessControllerValue, source, target) {
			result, err := r.cancelHandoff(ctx, source, target)
			return result, false, err
		}
		if !target.DeletionTimestamp.IsZero() {
			return ctrl.Result{RequeueAfter: migrationPollInterval}, false, nil
		}
		if err := validateMigrationSource(target, source.UID, source.Generation); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("continue handoff: %w", err)
		}
		if migrationModeFor(target) == migrationModePreserve {
			complete, err := r.churnlessDeploymentComplete(ctx, source)
			if err != nil {
				return ctrl.Result{}, false, err
			}
			if !complete {
				if err := r.setAnnotation(
					ctx,
					target,
					migrationModeAnnotation,
					migrationModeRecovery,
				); err != nil {
					return ctrl.Result{}, false, fmt.Errorf(
						"switch handoff to recovery mode: %w",
						err,
					)
				}
				r.event(
					target,
					corev1.EventTypeNormal,
					"RecoveryHandoffStarted",
					"Switched to recovery handoff after the Churnless rollout became incomplete",
				)
				return ctrl.Result{Requeue: true}, false, nil
			}
		}
	} else if source != nil &&
		source.Annotations[controllerAnnotation] == churnlessControllerValue &&
		target.Annotations[controllerAnnotation] != churnlessControllerValue {
		if err := r.setAnnotation(
			ctx,
			target,
			controllerAnnotation,
			churnlessControllerValue,
		); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("record requested Churnless return: %w", err)
		}
		return ctrl.Result{Requeue: true}, false, nil
	}
	return ctrl.Result{}, true, nil
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

func validateMigrationTarget(
	target client.Object,
	expectedSource string,
	allowRecovery bool,
) error {
	annotations := target.GetAnnotations()
	if annotations[migrationStateVersionAnnotation] != migrationStateVersion {
		return fmt.Errorf(
			"unsupported migration state version %q",
			annotations[migrationStateVersionAnnotation],
		)
	}
	if annotations[migrationSourceAnnotation] != expectedSource {
		return fmt.Errorf(
			"unexpected migration source %q",
			annotations[migrationSourceAnnotation],
		)
	}
	switch annotations[controllerAnnotation] {
	case nativeControllerValue, churnlessControllerValue:
	default:
		return fmt.Errorf(
			"unsupported desired controller %q",
			annotations[controllerAnnotation],
		)
	}
	if annotations[migrationIDAnnotation] == "" {
		return fmt.Errorf("migration source UID is missing")
	}
	if _, err := strconv.ParseInt(
		annotations[migrationSourceGenerationAnnotation],
		10,
		64,
	); err != nil {
		return fmt.Errorf("parse migration source generation: %w", err)
	}
	if _, err := strconv.ParseBool(annotations[migrationOriginalPausedAnnotation]); err != nil {
		return fmt.Errorf("parse original paused state: %w", err)
	}
	switch annotations[migrationPhaseAnnotation] {
	case migrationPhaseWarming, migrationPhaseCutover:
	default:
		return fmt.Errorf(
			"unsupported migration phase %q",
			annotations[migrationPhaseAnnotation],
		)
	}
	switch annotations[migrationModeAnnotation] {
	case migrationModePreserve:
	case migrationModeRecovery:
		if !allowRecovery {
			return fmt.Errorf("recovery mode is not supported for takeover")
		}
	default:
		return fmt.Errorf(
			"unsupported migration mode %q",
			annotations[migrationModeAnnotation],
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
		if err := r.Patch(
			ctx,
			replicaSet,
			client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
		); err != nil {
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
	if _, recorded := pod.Annotations[migrationOriginalDeletionCost]; !recorded {
		pod.Annotations[migrationOriginalDeletionCost] =
			migrationDeletionCostSnapshot(before.Annotations)
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
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	replicaSets, err := r.churnlessReplicaSetsControlledBy(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(replicaSets) == 0 {
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	changed, err := r.adoptMigrationPods(
		ctx,
		target.Namespace,
		migrationID,
		churnlessPodAdoptionTargets(replicaSets),
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	returningToNative := target.Annotations[controllerAnnotation] == nativeControllerValue
	if !returningToNative {
		complete, err := r.churnlessDeploymentComplete(ctx, target)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !complete {
			return ctrl.Result{RequeueAfter: migrationRolloutPollInterval}, nil
		}
	}
	settled, changed, err := r.cleanupMigrationPods(
		ctx,
		target.Namespace,
		migrationID,
		uidSetFor(replicaSets),
		&target.Spec.Template,
		true,
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !settled || changed {
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	if err := r.finishMigrationTarget(ctx, target); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: returningToNative}, nil
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
	return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
}

func (r *MigrationReconciler) finishHandoff(
	ctx context.Context,
	target *appsv1.Deployment,
) (ctrl.Result, error) {
	returningToChurnless :=
		target.Annotations[controllerAnnotation] == churnlessControllerValue
	if migrationModeFor(target) == migrationModeRecovery {
		if err := r.finishMigrationTarget(ctx, target); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: returningToChurnless}, nil
	}
	migrationID := target.Annotations[migrationIDAnnotation]
	remaining, err := r.churnlessReplicaSetsForMigration(ctx, target.Namespace, migrationID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(remaining) > 0 {
		if err := r.deleteChurnlessReplicaSets(ctx, remaining); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	replicaSets, err := r.nativeReplicaSetsControlledBy(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(replicaSets) == 0 {
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	changed, err := r.adoptMigrationPods(
		ctx,
		target.Namespace,
		migrationID,
		nativePodAdoptionTargets(replicaSets),
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	if !returningToChurnless && !nativeDeploymentComplete(target) {
		return ctrl.Result{RequeueAfter: migrationRolloutPollInterval}, nil
	}
	settled, changed, err := r.cleanupMigrationPods(
		ctx,
		target.Namespace,
		migrationID,
		uidSetFor(replicaSets),
		&target.Spec.Template,
		false,
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !settled || changed {
		return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
	}
	if err := r.finishMigrationTarget(ctx, target); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: returningToChurnless}, nil
}

func (r *MigrationReconciler) adoptMigrationPods(
	ctx context.Context,
	namespace, migrationID string,
	targets []podAdoptionTarget,
) (bool, error) {
	targetUIDs := make(map[types.UID]struct{}, len(targets))
	selectors := make([]labels.Selector, len(targets))
	for i := range targets {
		targetUIDs[targets[i].object.GetUID()] = struct{}{}
		selector, err := metav1.LabelSelectorAsSelector(targets[i].selector)
		if err != nil {
			return false, fmt.Errorf(
				"parse target ReplicaSet %s/%s selector for adoption: %w",
				targets[i].object.GetNamespace(),
				targets[i].object.GetName(),
				err,
			)
		}
		selectors[i] = selector
	}
	pods, err := r.listPods(ctx, namespace)
	if err != nil {
		return false, err
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Annotations[migrationIDAnnotation] != migrationID ||
			pod.Annotations[migrationRoleAnnotation] != migrationRoleSource ||
			!pod.DeletionTimestamp.IsZero() {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if owner != nil {
			if _, ok := targetUIDs[owner.UID]; ok {
				continue
			}
			return false, nil
		}
		targetIndex := slices.IndexFunc(selectors, func(selector labels.Selector) bool {
			return selector.Matches(labels.Set(pod.Labels))
		})
		if targetIndex < 0 {
			return false, fmt.Errorf(
				"source Pod %s/%s cannot match any target ReplicaSet selector",
				pod.Namespace,
				pod.Name,
			)
		}
		target := targets[targetIndex].object

		freshTarget := target.DeepCopyObject().(client.Object)
		if err := r.reader().Get(ctx, client.ObjectKeyFromObject(target), freshTarget); err != nil {
			return false, fmt.Errorf("re-read target ReplicaSet before Pod adoption: %w", err)
		}
		if freshTarget.GetUID() != target.GetUID() || !freshTarget.GetDeletionTimestamp().IsZero() {
			return false, fmt.Errorf(
				"target ReplicaSet %s/%s changed or is deleting before Pod adoption",
				target.GetNamespace(),
				target.GetName(),
			)
		}

		before := pod.DeepCopy()
		if err := controllerutil.SetControllerReference(freshTarget, pod, r.Scheme); err != nil {
			return false, fmt.Errorf("adopt Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if err := r.Patch(
			ctx,
			pod,
			client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
		); err != nil {
			return false, fmt.Errorf("adopt Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return true, nil
	}
	return false, nil
}

func nativePodAdoptionTargets(replicaSets []*appsv1.ReplicaSet) []podAdoptionTarget {
	result := make([]podAdoptionTarget, len(replicaSets))
	for i := range replicaSets {
		result[i] = podAdoptionTarget{
			object:   replicaSets[i],
			selector: replicaSets[i].Spec.Selector,
		}
	}
	return result
}

func churnlessPodAdoptionTargets(
	replicaSets []*appsv1alpha1.ReplicaSet,
) []podAdoptionTarget {
	result := make([]podAdoptionTarget, len(replicaSets))
	for i := range replicaSets {
		result[i] = podAdoptionTarget{
			object:   replicaSets[i],
			selector: replicaSets[i].Spec.Selector,
		}
	}
	return result
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
	return ctrl.Result{RequeueAfter: migrationPollInterval}, nil
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
		restoreMigrationDeletionCost(pod, template)
		if takeover {
			restorePodTemplateLabel(
				pod,
				template,
				appsv1.DefaultDeploymentUniqueLabelKey,
			)
		} else {
			restorePodTemplateLabel(pod, template, structuralRevisionLabel)
			delete(pod.Annotations, revisionAnnotation)
			delete(pod.Annotations, managedLabelKeysAnnotation)
			delete(pod.Annotations, managedAnnotationKeysAnnotation)
		}
		if len(pod.Annotations) == 0 {
			pod.Annotations = nil
		}
		if len(pod.Labels) == 0 {
			pod.Labels = nil
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

func restorePodTemplateLabel(
	pod *corev1.Pod,
	template *corev1.PodTemplateSpec,
	key string,
) {
	if value, ok := template.Labels[key]; ok {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[key] = value
		return
	}
	delete(pod.Labels, key)
}

func restoreMigrationDeletionCost(
	pod *corev1.Pod,
	template *corev1.PodTemplateSpec,
) {
	if original, recorded := pod.Annotations[migrationOriginalDeletionCost]; recorded {
		delete(pod.Annotations, migrationOriginalDeletionCost)
		switch {
		case original == originalDeletionCostAbsent:
			delete(pod.Annotations, corev1.PodDeletionCost)
		case strings.HasPrefix(original, originalDeletionCostPresentPrefix):
			pod.Annotations[corev1.PodDeletionCost] = strings.TrimPrefix(
				original,
				originalDeletionCostPresentPrefix,
			)
		default:
			// State written before presence-aware snapshots stored the raw value.
			pod.Annotations[corev1.PodDeletionCost] = original
		}
		return
	}
	if value, ok := template.Annotations[corev1.PodDeletionCost]; ok {
		pod.Annotations[corev1.PodDeletionCost] = value
	} else {
		delete(pod.Annotations, corev1.PodDeletionCost)
	}
}

func migrationDeletionCostSnapshot(annotations map[string]string) string {
	if value, ok := annotations[corev1.PodDeletionCost]; ok {
		return originalDeletionCostPresentPrefix + value
	}
	return originalDeletionCostAbsent
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
		migrationModeAnnotation,
		migrationStateVersionAnnotation,
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
	if err := r.Patch(
		ctx,
		target,
		client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
	); err != nil {
		return fmt.Errorf("finish migration target: %w", err)
	}
	managedBy := "Churnless"
	if _, native := target.(*appsv1.Deployment); native {
		managedBy = "native Kubernetes"
	}
	message := "Deployment is now managed by " + managedBy
	if _, churnless := target.(*appsv1alpha1.Deployment); churnless &&
		target.GetAnnotations()[controllerAnnotation] == nativeControllerValue {
		message = "Completed Churnless takeover cutover; continuing with requested native fallback"
	}
	if _, native := target.(*appsv1.Deployment); native &&
		target.GetAnnotations()[controllerAnnotation] == churnlessControllerValue {
		message = "Completed native handoff cutover; continuing with requested Churnless return"
	}
	r.event(
		target,
		corev1.EventTypeNormal,
		"MigrationCompleted",
		message,
	)
	return nil
}

func migrationModeFor(target client.Object) string {
	return target.GetAnnotations()[migrationModeAnnotation]
}

func controllerRequested(value string, objects ...client.Object) bool {
	for _, object := range objects {
		if object.GetAnnotations()[controllerAnnotation] == value {
			return true
		}
	}
	return false
}

func (r *MigrationReconciler) setMigrationPhase(
	ctx context.Context,
	target client.Object,
	phase string,
) error {
	return r.setAnnotation(ctx, target, migrationPhaseAnnotation, phase)
}

func (r *MigrationReconciler) setAnnotation(
	ctx context.Context,
	object client.Object,
	key, value string,
) error {
	if object.GetAnnotations()[key] == value {
		return nil
	}
	before := object.DeepCopyObject().(client.Object)
	annotations := maps.Clone(object.GetAnnotations())
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[key] = value
	object.SetAnnotations(annotations)
	return r.Patch(
		ctx,
		object,
		client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
	)
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

func (r *MigrationReconciler) deleteChurnlessSourceForRecovery(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
) error {
	if err := r.Delete(
		ctx,
		source,
		foregroundDeleteOptions(source, true),
	); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete Churnless Deployment for recovery: %w", err)
	}
	return nil
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
	return deleteOptions(object, policy, includeResourceVersion)
}

func foregroundDeleteOptions(
	object client.Object,
	includeResourceVersion bool,
) *client.DeleteOptions {
	policy := metav1.DeletePropagationForeground
	return deleteOptions(object, policy, includeResourceVersion)
}

func deleteOptions(
	object client.Object,
	policy metav1.DeletionPropagation,
	includeResourceVersion bool,
) *client.DeleteOptions {
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

func (r *MigrationReconciler) event(
	object client.Object,
	eventType, reason, message string,
) {
	if r.Recorder != nil {
		r.Recorder.Eventf(object, nil, eventType, reason, reason, message)
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

func asClientObjects[T client.Object](objects []T) []client.Object {
	result := make([]client.Object, len(objects))
	for i := range objects {
		result[i] = objects[i]
	}
	return result
}

func uidSetFor[T client.Object](objects []T) map[types.UID]struct{} {
	result := make(map[types.UID]struct{}, len(objects))
	for _, object := range objects {
		result[object.GetUID()] = struct{}{}
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
