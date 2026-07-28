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

package kubectlplugin

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/workloadmeta"
)

type restartAPIVersion int

const (
	restartAPIVersionAny restartAPIVersion = iota
	restartAPIVersionNative
	restartAPIVersionChurnless
)

type restartResource struct {
	name       string
	apiVersion restartAPIVersion
}

type restartTarget struct {
	object    client.Object
	canonical string
}

// deploymentRestarter resolves a Deployment GVK and records a native-style
// restart token on its Pod template.
type deploymentRestarter struct {
	client client.Client
	now    func() time.Time
}

func newDeploymentRestarter(
	k8sClient client.Client,
	now func() time.Time,
) *deploymentRestarter {
	if now == nil {
		now = time.Now
	}
	return &deploymentRestarter{client: k8sClient, now: now}
}

// Restart requests a rollout restart and returns the canonical resource name.
func (r *deploymentRestarter) Restart(
	ctx context.Context,
	namespace, resource string,
) (string, error) {
	if r == nil || r.client == nil {
		return "", fmt.Errorf("restart client is required")
	}
	parsed, err := parseRestartResource(resource)
	if err != nil {
		return "", err
	}
	initial, err := r.resolve(ctx, namespace, parsed)
	if err != nil {
		return "", err
	}

	restartedAt := r.now().UTC().Format(time.RFC3339Nano)
	initialUID := initial.object.GetUID()
	target := initial
	err = retryRestartConflict(ctx, func() error {
		if target.object == nil {
			target, err = r.resolve(ctx, namespace, parsed)
			if err != nil {
				return err
			}
			if target.canonical != initial.canonical ||
				initialUID != "" && target.object.GetUID() != initialUID {
				return fmt.Errorf(
					"restart target changed from %s while retrying",
					initial.canonical,
				)
			}
		}

		paused, pauseErr := deploymentPaused(target.object)
		if pauseErr != nil {
			return pauseErr
		}
		if paused {
			return fmt.Errorf(
				"%s is paused; set spec.paused=false before restarting",
				target.canonical,
			)
		}
		before := target.object.DeepCopyObject().(client.Object)
		if err := setRestartAnnotation(target.object, restartedAt); err != nil {
			return err
		}
		patchErr := r.client.Patch(
			ctx,
			target.object,
			client.MergeFromWithOptions(
				before,
				client.MergeFromWithOptimisticLock{},
			),
		)
		target = restartTarget{}
		return patchErr
	})
	if err != nil {
		if ctx.Err() != nil ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			apierrors.IsTimeout(err) ||
			apierrors.IsServerTimeout(err) {
			return "", fmt.Errorf(
				"restart request for %s was interrupted; cluster state may have advanced; "+
					"inspect the %s annotation before retrying: %w",
				initial.canonical,
				workloadmeta.KubectlRestartedAtAnnotation,
				err,
			)
		}
		return "", fmt.Errorf("restart %s: %w", initial.canonical, err)
	}
	return initial.canonical, nil
}

func retryRestartConflict(ctx context.Context, attempt func() error) error {
	var lastConflict, terminalError error
	err := wait.ExponentialBackoffWithContext(
		ctx,
		retry.DefaultBackoff,
		func(context.Context) (bool, error) {
			err := attempt()
			switch {
			case err == nil:
				return true, nil
			case apierrors.IsConflict(err):
				lastConflict = err
				return false, nil
			default:
				terminalError = err
				return false, err
			}
		},
	)
	if terminalError != nil {
		return terminalError
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil && lastConflict != nil {
		return lastConflict
	}
	return err
}

func parseRestartResource(resource string) (restartResource, error) {
	kind, name, found := strings.Cut(resource, "/")
	if !found || name == "" || strings.Contains(name, "/") {
		return restartResource{}, fmt.Errorf(
			"expected deployment/NAME, got %q",
			resource,
		)
	}

	var apiVersion restartAPIVersion
	switch strings.ToLower(kind) {
	case "deployment", "deployments":
		apiVersion = restartAPIVersionAny
	case "deploy", "deployment.apps", "deployments.apps":
		apiVersion = restartAPIVersionNative
	case "cdeploy", "deployment.churnless.io", "deployments.churnless.io":
		apiVersion = restartAPIVersionChurnless
	default:
		return restartResource{}, fmt.Errorf(
			"rollout restart supports Deployments only, got %q",
			kind,
		)
	}
	return restartResource{name: name, apiVersion: apiVersion}, nil
}

func (r *deploymentRestarter) resolve(
	ctx context.Context,
	namespace string,
	resource restartResource,
) (restartTarget, error) {
	key := types.NamespacedName{Namespace: namespace, Name: resource.name}
	switch resource.apiVersion {
	case restartAPIVersionNative:
		return r.getNative(ctx, key)
	case restartAPIVersionChurnless:
		return r.getChurnless(ctx, key)
	case restartAPIVersionAny:
		native, nativeErr := r.getNative(ctx, key)
		if nativeErr != nil && !restartResourceAbsent(nativeErr) {
			return restartTarget{}, fmt.Errorf("resolve native Deployment: %w", nativeErr)
		}
		churnless, churnlessErr := r.getChurnless(ctx, key)
		if churnlessErr != nil && !restartResourceAbsent(churnlessErr) {
			return restartTarget{}, fmt.Errorf("resolve Churnless Deployment: %w", churnlessErr)
		}
		switch {
		case nativeErr == nil && churnlessErr == nil:
			return restartTarget{}, fmt.Errorf(
				"deployment/%s is ambiguous in namespace %q: both %s and %s exist; "+
					"choose one explicitly",
				resource.name,
				namespace,
				native.canonical,
				churnless.canonical,
			)
		case nativeErr == nil:
			return native, nil
		case churnlessErr == nil:
			return churnless, nil
		default:
			return restartTarget{}, fmt.Errorf(
				"deployment/%s was not found in namespace %q "+
					"(checked deployment.apps/%s and deployment.churnless.io/%s)",
				resource.name,
				namespace,
				resource.name,
				resource.name,
			)
		}
	default:
		return restartTarget{}, fmt.Errorf("unsupported restart API version")
	}
}

func restartResourceAbsent(err error) bool {
	return apierrors.IsNotFound(err) || meta.IsNoMatchError(err)
}

func (r *deploymentRestarter) getNative(
	ctx context.Context,
	key types.NamespacedName,
) (restartTarget, error) {
	var deployment appsv1.Deployment
	if err := r.client.Get(ctx, key, &deployment); err != nil {
		return restartTarget{}, err
	}
	return restartTarget{
		object:    &deployment,
		canonical: "deployment.apps/" + key.Name,
	}, nil
}

func (r *deploymentRestarter) getChurnless(
	ctx context.Context,
	key types.NamespacedName,
) (restartTarget, error) {
	var deployment appsv1alpha1.Deployment
	if err := r.client.Get(ctx, key, &deployment); err != nil {
		return restartTarget{}, err
	}
	return restartTarget{
		object:    &deployment,
		canonical: "deployment.churnless.io/" + key.Name,
	}, nil
}

func deploymentPaused(object client.Object) (bool, error) {
	switch deployment := object.(type) {
	case *appsv1.Deployment:
		return deployment.Spec.Paused, nil
	case *appsv1alpha1.Deployment:
		return deployment.Spec.Paused, nil
	default:
		return false, fmt.Errorf("unsupported restart target %T", object)
	}
}

func setRestartAnnotation(object client.Object, restartedAt string) error {
	switch deployment := object.(type) {
	case *appsv1.Deployment:
		deployment.Spec.Template.Annotations = restartAnnotations(
			deployment.Spec.Template.Annotations,
			restartedAt,
		)
	case *appsv1alpha1.Deployment:
		deployment.Spec.Template.Annotations = restartAnnotations(
			deployment.Spec.Template.Annotations,
			restartedAt,
		)
	default:
		return fmt.Errorf("unsupported restart target %T", object)
	}
	return nil
}

func restartAnnotations(current map[string]string, restartedAt string) map[string]string {
	annotations := maps.Clone(current)
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[workloadmeta.KubectlRestartedAtAnnotation] = restartedAt
	return annotations
}
