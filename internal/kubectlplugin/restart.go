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

type restartTarget struct {
	object    client.Object
	spec      *appsv1.DeploymentSpec
	canonical string
}

// deploymentRestarter records the standard kubectl restart token on either
// Deployment API through one shared code path.
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

// Restart requests a rollout and returns the target's canonical resource name.
func (r *deploymentRestarter) Restart(
	ctx context.Context,
	namespace string,
	reference deploymentReference,
) (string, error) {
	if r == nil || r.client == nil {
		return "", fmt.Errorf("restart client is required")
	}
	initial, err := r.resolve(ctx, namespace, reference)
	if err != nil {
		return "", err
	}

	restartedAt := r.now().UTC().Format(time.RFC3339Nano)
	initialUID := initial.object.GetUID()
	target := initial
	err = retryRestartConflict(ctx, func() error {
		if target.object == nil {
			target, err = r.resolve(ctx, namespace, reference)
			if err != nil {
				return err
			}
			if target.canonical != initial.canonical ||
				(initialUID != "" && target.object.GetUID() != initialUID) {
				return fmt.Errorf(
					"restart target changed from %s while retrying",
					initial.canonical,
				)
			}
		}
		if target.spec.Paused {
			return fmt.Errorf(
				"%s is paused; set spec.paused=false before restarting",
				target.canonical,
			)
		}

		before := target.object.DeepCopyObject().(client.Object)
		if target.spec.Template.Annotations == nil {
			target.spec.Template.Annotations = map[string]string{}
		}
		target.spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation] = restartedAt
		patchErr := r.client.Patch(
			ctx,
			target.object,
			client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
		)
		target = restartTarget{}
		return patchErr
	})
	if err == nil {
		return initial.canonical, nil
	}
	if restartWasInterrupted(ctx, err) {
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

func retryRestartConflict(ctx context.Context, attempt func() error) error {
	var last error
	err := wait.ExponentialBackoffWithContext(
		ctx,
		retry.DefaultBackoff,
		func(context.Context) (bool, error) {
			last = attempt()
			if last == nil {
				return true, nil
			}
			if apierrors.IsConflict(last) {
				return false, nil
			}
			return false, last
		},
	)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil && last != nil {
		return last
	}
	return err
}

func restartWasInterrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err)
}

func (r *deploymentRestarter) resolve(
	ctx context.Context,
	namespace string,
	reference deploymentReference,
) (restartTarget, error) {
	key := types.NamespacedName{Namespace: namespace, Name: reference.name}
	if reference.api != deploymentAPIAny {
		return r.get(ctx, key, reference.api)
	}

	native, nativeErr := r.get(ctx, key, deploymentAPINative)
	if nativeErr != nil && !restartResourceAbsent(nativeErr) {
		return restartTarget{}, fmt.Errorf("resolve native Deployment: %w", nativeErr)
	}
	churnless, churnlessErr := r.get(ctx, key, deploymentAPIChurnless)
	if churnlessErr != nil && !restartResourceAbsent(churnlessErr) {
		return restartTarget{}, fmt.Errorf("resolve Churnless Deployment: %w", churnlessErr)
	}
	switch {
	case nativeErr == nil && churnlessErr == nil:
		return restartTarget{}, fmt.Errorf(
			"deployment/%s is ambiguous in namespace %q: both %s and %s exist; "+
				"choose one explicitly",
			reference.name,
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
			reference.name,
			namespace,
			reference.name,
			reference.name,
		)
	}
}

func (r *deploymentRestarter) get(
	ctx context.Context,
	key types.NamespacedName,
	api deploymentAPI,
) (restartTarget, error) {
	var target restartTarget
	switch api {
	case deploymentAPINative:
		deployment := &appsv1.Deployment{}
		target = restartTarget{
			object:    deployment,
			spec:      &deployment.Spec,
			canonical: "deployment.apps/" + key.Name,
		}
	case deploymentAPIChurnless:
		deployment := &appsv1alpha1.Deployment{}
		target = restartTarget{
			object:    deployment,
			spec:      &deployment.Spec.DeploymentSpec,
			canonical: "deployment.churnless.io/" + key.Name,
		}
	default:
		return restartTarget{}, fmt.Errorf("unsupported restart API version")
	}
	if err := r.client.Get(ctx, key, target.object); err != nil {
		return restartTarget{}, err
	}
	return target, nil
}

func restartResourceAbsent(err error) bool {
	return apierrors.IsNotFound(err) || meta.IsNoMatchError(err)
}
