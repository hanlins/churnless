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
	Mode                string
	Complete            bool
	Superseded          bool
	Message             string
}

// MigrationStep is how long the synchronous driver should wait before the
// next idempotent step. Zero drives the next step immediately.
type MigrationStep time.Duration

// MigrationEngine is the reusable transfer surface driven by kubectl-churnless.
// RequestMigration records intent once; AdvanceMigration observes that intent
// before running one idempotent step.
type MigrationEngine interface {
	RequestMigration(context.Context, types.NamespacedName, MigrationDestination) error
	AdvanceMigration(
		context.Context,
		types.NamespacedName,
		MigrationDestination,
	) (MigrationProgress, MigrationStep, error)
}

// MigrationDriver synchronously drives an idempotent migration engine.
// Interrupted commands leave durable state in the API; running the same
// transfer again resumes it.
type MigrationDriver struct {
	Engine       MigrationEngine
	PollInterval time.Duration
	Observe      func(MigrationProgress)
}

// Transfer requests the destination controller and drives transfer steps until
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

	for {
		progress, step, err := d.Engine.AdvanceMigration(ctx, key, destination)
		if progress.Destination != "" && progress != lastProgress {
			d.observe(progress)
			lastProgress = progress
		}
		if err != nil {
			if !retryableMigrationError(err) {
				return migrationOperationError(
					ctx,
					lastProgress,
					"advance migration",
					err,
				)
			}
			if err := waitForMigrationRetry(ctx, pollInterval); err != nil {
				return migrationOperationError(ctx, lastProgress, "advance migration", err)
			}
			continue
		}
		if progress.Superseded {
			return migrationSupersededError(progress)
		}
		if progress.Complete {
			return nil
		}

		delay := time.Duration(step)
		if delay <= 0 {
			if err := ctx.Err(); err != nil {
				return migrationOperationError(ctx, lastProgress, "advance migration", err)
			}
			continue
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

// RequestMigration records the desired controller on the authoritative source,
// or on the surviving target when cutover has already removed that source.
func (r *DeploymentMigrationEngine) RequestMigration(
	ctx context.Context,
	key types.NamespacedName,
	destination MigrationDestination,
) error {
	if err := validateMigrationDestination(destination); err != nil {
		return err
	}

	const attempts = 2
	for attempt := range attempts {
		pair, err := r.loadMigrationPair(ctx, key)
		if err != nil {
			return err
		}

		err = r.setAnnotation(
			ctx,
			pair.requestTarget(),
			controllerAnnotation,
			string(destination),
		)
		if !apierrors.IsNotFound(err) || attempt == attempts-1 {
			return err
		}
	}
	return nil
}

// AdvanceMigration reports current progress and, unless the request is already
// complete or superseded, advances one idempotent transfer step.
func (r *DeploymentMigrationEngine) AdvanceMigration(
	ctx context.Context,
	key types.NamespacedName,
	destination MigrationDestination,
) (MigrationProgress, MigrationStep, error) {
	if err := validateMigrationDestination(destination); err != nil {
		return MigrationProgress{}, 0, err
	}
	pair, err := r.loadMigrationPair(ctx, key)
	if err != nil {
		return MigrationProgress{}, 0, err
	}

	progress, err := migrationProgressForPair(pair, destination)
	if err != nil {
		return MigrationProgress{}, 0, err
	}
	if progress.Complete || progress.Superseded {
		return progress, 0, nil
	}
	step, err := r.advanceMigrationPair(ctx, pair)
	return progress, step, err
}

func migrationProgressForPair(
	pair migrationPair,
	destination MigrationDestination,
) (MigrationProgress, error) {
	progress := MigrationProgress{Destination: destination}
	switch pair.destination {
	case MigrationDestinationChurnless:
		if err := validateMigrationTarget(pair.churnless, false); err != nil {
			return MigrationProgress{}, err
		}
		progress.CurrentController = MigrationDestinationNative
		if pair.native == nil || !pair.native.DeletionTimestamp.IsZero() {
			progress.CurrentController = MigrationDestinationChurnless
		}
		progress.Mode = pair.churnless.Annotations[migrationModeAnnotation]
	case MigrationDestinationNative:
		if err := validateMigrationTarget(pair.native, true); err != nil {
			return MigrationProgress{}, err
		}
		progress.CurrentController = MigrationDestinationChurnless
		if pair.churnless == nil || !pair.churnless.DeletionTimestamp.IsZero() {
			progress.CurrentController = MigrationDestinationNative
		}
		progress.Mode = pair.native.Annotations[migrationModeAnnotation]
	case "":
		switch {
		case pair.native != nil:
			progress.CurrentController = MigrationDestinationNative
			progress.Complete = destination == MigrationDestinationNative
		case pair.churnless != nil:
			progress.CurrentController = MigrationDestinationChurnless
			progress.Complete = destination == MigrationDestinationChurnless
		}
	}

	requested := MigrationDestination(
		pair.requestTarget().GetAnnotations()[controllerAnnotation],
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
	if progress.Mode == "" {
		if progress.Destination == MigrationDestinationNative {
			return "waiting to start handoff to native Kubernetes"
		}
		return "waiting to start Churnless takeover"
	}
	action := "takeover"
	if progress.Destination == MigrationDestinationNative {
		action = "handoff"
	}
	if progress.CurrentController == progress.Destination {
		return fmt.Sprintf("finalizing %s %s", progress.Mode, action)
	}
	return fmt.Sprintf("%s %s in progress", progress.Mode, action)
}

// migrationPair is the single classification of the same-name native and
// Churnless Deployments used by requests and transfer advances.
type migrationPair struct {
	native      *appsv1.Deployment
	churnless   *appsv1alpha1.Deployment
	destination MigrationDestination
}

func (r *DeploymentMigrationEngine) loadMigrationPair(
	ctx context.Context,
	key types.NamespacedName,
) (migrationPair, error) {
	native, err := r.getNativeDeployment(ctx, key)
	if err != nil {
		return migrationPair{}, err
	}
	churnless, err := r.getChurnlessDeployment(ctx, key)
	if err != nil {
		return migrationPair{}, err
	}

	pair := migrationPair{native: native, churnless: churnless}
	nativeTarget := native != nil && hasMigrationState(native)
	churnlessTarget := churnless != nil && hasMigrationState(churnless)
	switch {
	case nativeTarget && churnlessTarget:
		return migrationPair{}, fmt.Errorf(
			"same-name native and Churnless Deployments both contain migration state",
		)
	case churnlessTarget:
		pair.destination = MigrationDestinationChurnless
	case nativeTarget:
		pair.destination = MigrationDestinationNative
	case native != nil && churnless != nil:
		return migrationPair{}, fmt.Errorf(
			"same-name native and Churnless Deployments exist without valid migration state",
		)
	case native == nil && churnless == nil:
		return migrationPair{}, fmt.Errorf("deployment does not exist")
	}
	return pair, nil
}

func hasMigrationState(object client.Object) bool {
	_, found := object.GetAnnotations()[migrationStateVersionAnnotation]
	return found
}

func (p migrationPair) requestTarget() client.Object {
	switch p.destination {
	case MigrationDestinationChurnless:
		if p.native != nil {
			return p.native
		}
		return p.churnless
	case MigrationDestinationNative:
		if p.churnless != nil {
			return p.churnless
		}
		return p.native
	default:
		if p.native != nil {
			return p.native
		}
		return p.churnless
	}
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
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
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
