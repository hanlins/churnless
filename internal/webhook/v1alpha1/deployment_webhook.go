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

package v1alpha1

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/kubecompat"
)

// SetupDeploymentWebhookWithManager registers the webhook for Deployment in the manager.
func SetupDeploymentWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &appsv1alpha1.Deployment{}).
		WithValidator(&DeploymentCustomValidator{}).
		WithDefaulter(&DeploymentCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-churnless-io-v1alpha1-deployment,mutating=true,failurePolicy=fail,sideEffects=None,groups=churnless.io,resources=deployments,verbs=create;update,versions=v1alpha1,name=mdeployment-v1alpha1.kb.io,admissionReviewVersions=v1

// DeploymentCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Deployment when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type DeploymentCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Deployment.
func (d *DeploymentCustomDefaulter) Default(_ context.Context, obj *appsv1alpha1.Deployment) error {
	kubecompat.DefaultDeployment(&obj.Spec.DeploymentSpec)
	return nil
}

// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-churnless-io-v1alpha1-deployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=churnless.io,resources=deployments,verbs=create;update,versions=v1alpha1,name=vdeployment-v1alpha1.kb.io,admissionReviewVersions=v1

// DeploymentCustomValidator struct is responsible for validating the Deployment resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type DeploymentCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Deployment.
func (v *DeploymentCustomValidator) ValidateCreate(ctx context.Context, obj *appsv1alpha1.Deployment) (admission.Warnings, error) {
	return kubecompat.DeploymentWarnings(ctx, obj.Name, &obj.Spec.DeploymentSpec, nil),
		deploymentValidationError(obj, nil)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Deployment.
func (v *DeploymentCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *appsv1alpha1.Deployment) (admission.Warnings, error) {
	return kubecompat.DeploymentWarnings(
			ctx,
			newObj.Name,
			&newObj.Spec.DeploymentSpec,
			&oldObj.Spec.DeploymentSpec,
		),
		deploymentValidationError(newObj, oldObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Deployment.
func (v *DeploymentCustomValidator) ValidateDelete(_ context.Context, obj *appsv1alpha1.Deployment) (admission.Warnings, error) {
	return nil, nil
}

func deploymentValidationError(
	obj, oldObj *appsv1alpha1.Deployment,
) error {
	var oldSpec *appsv1.DeploymentSpec
	if oldObj != nil {
		oldSpec = &oldObj.Spec.DeploymentSpec
	}
	errs := kubecompat.ValidateDeployment(&obj.Spec.DeploymentSpec, oldSpec)
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		appsv1alpha1.GroupVersion.WithKind("Deployment").GroupKind(),
		obj.Name,
		errs,
	)
}
