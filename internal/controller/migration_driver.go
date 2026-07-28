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
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

// MigrationDestination identifies the desired Deployment controller.
type MigrationDestination string

const (
	// MigrationDestinationChurnless transfers a native Deployment to Churnless.
	MigrationDestinationChurnless MigrationDestination = churnlessControllerValue
	// MigrationDestinationNative transfers a Churnless Deployment to Kubernetes.
	MigrationDestinationNative MigrationDestination = nativeControllerValue
)

// MigrationProgress is a stable, user-facing view of a resumable migration.
type MigrationProgress struct {
	Destination         MigrationDestination
	RequestedController MigrationDestination
	CurrentController   MigrationDestination
	Phase               string
	Mode                string
	Complete            bool
	Superseded          bool
	Message             string
}

// MigrationStep is the controller-neutral scheduling result of one idempotent
// migration step.
type MigrationStep struct {
	RetryAfter time.Duration
}

// MigrationEngine is the shared migration surface used by the controller loop
// and synchronous clients such as kubectl-churnless.
type MigrationEngine interface {
	RequestMigration(context.Context, types.NamespacedName, MigrationDestination) error
	InspectMigration(
		context.Context,
		types.NamespacedName,
		MigrationDestination,
	) (MigrationProgress, error)
	StepMigration(context.Context, types.NamespacedName) (MigrationStep, error)
}

// MigrationDriver synchronously drives the same idempotent state machine used
// by MigrationReconciler. Interrupted commands leave durable state in the API;
// running the same transfer again resumes it.
type MigrationDriver struct {
	Engine       MigrationEngine
	PollInterval time.Duration
	Observe      func(MigrationProgress)
}

// Transfer requests the destination controller and drives reconciliation until
// that controller is authoritative or the context ends.
func (d *MigrationDriver) Transfer(
	ctx context.Context,
	key types.NamespacedName,
	destination MigrationDestination,
) error {
	if d.Engine == nil {
		return fmt.Errorf("migration engine is required")
	}
	if err := validateMigrationDestination(destination); err != nil {
		return err
	}
	pollInterval := d.PollInterval
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	lastProgress := MigrationProgress{
		Destination: destination,
		Message:     "requesting the migration",
	}
	for {
		err := d.Engine.RequestMigration(ctx, key, destination)
		if err == nil {
			break
		}
		if !retryableMigrationError(err) {
			return migrationOperationError(ctx, lastProgress, "request migration", err)
		}
		if err := waitForMigrationRetry(ctx, pollInterval); err != nil {
			return migrationOperationError(ctx, lastProgress, "request migration", err)
		}
	}

	progress, err := d.Engine.InspectMigration(ctx, key, destination)
	if err != nil {
		return migrationOperationError(ctx, lastProgress, "inspect migration", err)
	}
	d.observe(progress)
	if progress.Superseded {
		return migrationSupersededError(progress)
	}
	if progress.Complete {
		return nil
	}

	lastProgress = progress
	for {
		step, reconcileErr := d.Engine.StepMigration(ctx, key)
		if reconcileErr != nil && !retryableMigrationError(reconcileErr) {
			return migrationOperationError(
				ctx,
				lastProgress,
				"drive migration",
				reconcileErr,
			)
		}

		progress, err = d.Engine.InspectMigration(ctx, key, destination)
		if err != nil {
			if !retryableMigrationError(err) {
				return migrationOperationError(
					ctx,
					lastProgress,
					"inspect migration",
					err,
				)
			}
		} else {
			if progress != lastProgress {
				d.observe(progress)
				lastProgress = progress
			}
			if progress.Superseded {
				return migrationSupersededError(progress)
			}
			if progress.Complete {
				return nil
			}
		}

		delay := step.RetryAfter
		if delay <= 0 {
			delay = pollInterval
		}
		if err := waitForMigrationRetry(ctx, delay); err != nil {
			return migrationOperationError(ctx, lastProgress, "wait for migration", err)
		}
	}
}

func (d *MigrationDriver) observe(progress MigrationProgress) {
	if d.Observe != nil {
		d.Observe(progress)
	}
}

// StepMigration advances the reconciler once without exposing
// controller-runtime request and scheduling types to synchronous clients.
func (r *MigrationReconciler) StepMigration(
	ctx context.Context,
	key types.NamespacedName,
) (MigrationStep, error) {
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	return MigrationStep{RetryAfter: result.RequeueAfter}, err
}

// RequestMigration records the desired controller on the authoritative source,
// or on the surviving target when cutover has already removed that source.
func (r *MigrationReconciler) RequestMigration(
	ctx context.Context,
	key types.NamespacedName,
	destination MigrationDestination,
) error {
	if err := validateMigrationDestination(destination); err != nil {
		return err
	}

	const attempts = 2
	for attempt := range attempts {
		native, err := r.getNativeDeployment(ctx, key)
		if err != nil {
			return err
		}
		churnless, err := r.getChurnlessDeployment(ctx, key)
		if err != nil {
			return err
		}

		requestTarget, err := migrationRequestTarget(native, churnless)
		if err != nil {
			return err
		}
		err = r.setAnnotation(
			ctx,
			requestTarget,
			controllerAnnotation,
			string(destination),
		)
		if !apierrors.IsNotFound(err) || attempt == attempts-1 {
			return err
		}
	}
	return nil
}

func migrationRequestTarget(
	native *appsv1.Deployment,
	churnless *appsv1alpha1.Deployment,
) (client.Object, error) {
	switch {
	case churnless != nil &&
		churnless.GetAnnotations()[migrationSourceAnnotation] == nativeDeploymentSource:
		if native != nil {
			return native, nil
		}
		return churnless, nil
	case native != nil &&
		native.GetAnnotations()[migrationSourceAnnotation] == churnlessDeploymentSource:
		if churnless != nil {
			return churnless, nil
		}
		return native, nil
	case native != nil && churnless != nil:
		return nil, fmt.Errorf(
			"same-name native and Churnless Deployments exist without valid migration state",
		)
	case native != nil:
		return native, nil
	case churnless != nil:
		return churnless, nil
	default:
		return nil, fmt.Errorf("deployment does not exist")
	}
}

// InspectMigration reports persisted migration state without mutating it.
func (r *MigrationReconciler) InspectMigration(
	ctx context.Context,
	key types.NamespacedName,
	destination MigrationDestination,
) (MigrationProgress, error) {
	if err := validateMigrationDestination(destination); err != nil {
		return MigrationProgress{}, err
	}
	native, err := r.getNativeDeployment(ctx, key)
	if err != nil {
		return MigrationProgress{}, err
	}
	churnless, err := r.getChurnlessDeployment(ctx, key)
	if err != nil {
		return MigrationProgress{}, err
	}

	progress := MigrationProgress{Destination: destination}
	switch {
	case churnless != nil &&
		churnless.Annotations[migrationSourceAnnotation] == nativeDeploymentSource:
		if err := validateMigrationTarget(churnless, nativeDeploymentSource, false); err != nil {
			return MigrationProgress{}, err
		}
		progress.CurrentController = MigrationDestinationNative
		if native == nil || !native.DeletionTimestamp.IsZero() {
			progress.CurrentController = MigrationDestinationChurnless
		}
		progress.Phase = churnless.Annotations[migrationPhaseAnnotation]
		progress.Mode = churnless.Annotations[migrationModeAnnotation]
	case native != nil &&
		native.Annotations[migrationSourceAnnotation] == churnlessDeploymentSource:
		if err := validateMigrationTarget(native, churnlessDeploymentSource, true); err != nil {
			return MigrationProgress{}, err
		}
		progress.CurrentController = MigrationDestinationChurnless
		if churnless == nil || !churnless.DeletionTimestamp.IsZero() {
			progress.CurrentController = MigrationDestinationNative
		}
		progress.Phase = native.Annotations[migrationPhaseAnnotation]
		progress.Mode = native.Annotations[migrationModeAnnotation]
	case native != nil && churnless != nil:
		return MigrationProgress{}, fmt.Errorf(
			"same-name native and Churnless Deployments exist without valid migration state",
		)
	case native != nil:
		progress.CurrentController = MigrationDestinationNative
		progress.Complete = destination == MigrationDestinationNative
	case churnless != nil:
		progress.CurrentController = MigrationDestinationChurnless
		progress.Complete = destination == MigrationDestinationChurnless
	default:
		return MigrationProgress{}, fmt.Errorf("deployment does not exist")
	}

	requestTarget, err := migrationRequestTarget(native, churnless)
	if err != nil {
		return MigrationProgress{}, err
	}
	requested := MigrationDestination(
		requestTarget.GetAnnotations()[controllerAnnotation],
	)
	if err := validateMigrationDestination(requested); err != nil {
		return MigrationProgress{}, err
	}
	progress.RequestedController = requested
	if requested != destination {
		progress.Superseded = true
	}
	progress.Message = migrationProgressMessage(progress)
	return progress, nil
}

func migrationProgressMessage(progress MigrationProgress) string {
	if progress.Superseded {
		return fmt.Sprintf(
			"request to %s was superseded by desired controller %s",
			progress.Destination,
			progress.RequestedController,
		)
	}
	if progress.Complete {
		if progress.Destination == MigrationDestinationNative {
			return "handoff complete; native Kubernetes is authoritative"
		}
		return "takeover complete; Churnless is authoritative"
	}
	if progress.Phase == "" {
		if progress.Destination == MigrationDestinationNative {
			return "waiting to start handoff to native Kubernetes"
		}
		return "waiting to start Churnless takeover"
	}
	action := "takeover"
	if progress.Destination == MigrationDestinationNative {
		action = "handoff"
	}
	return fmt.Sprintf("%s %s in %s phase", progress.Mode, action, progress.Phase)
}

func migrationSupersededError(progress MigrationProgress) error {
	return fmt.Errorf(
		"migration request to %s was superseded by desired controller %s",
		progress.Destination,
		progress.RequestedController,
	)
}

func migrationOperationError(
	ctx context.Context,
	progress MigrationProgress,
	action string,
	err error,
) error {
	cause := err
	if ctx.Err() != nil {
		cause = ctx.Err()
	}
	if errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf(
			"migration interrupted while %s; cluster state may have advanced; "+
				"run the same command to resume: %w",
			progress.Message,
			cause,
		)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func validateMigrationDestination(destination MigrationDestination) error {
	switch destination {
	case MigrationDestinationChurnless, MigrationDestinationNative:
		return nil
	default:
		return fmt.Errorf("unsupported migration destination %q", destination)
	}
}

func retryableMigrationError(err error) bool {
	return apierrors.IsConflict(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err)
}

func waitForMigrationRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
