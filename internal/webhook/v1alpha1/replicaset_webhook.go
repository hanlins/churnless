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

// SetupReplicaSetWebhookWithManager registers the webhook for ReplicaSet in the manager.
func SetupReplicaSetWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &appsv1alpha1.ReplicaSet{}).
		WithValidator(&ReplicaSetCustomValidator{}).
		WithDefaulter(&ReplicaSetCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-apps-churnless-io-v1alpha1-replicaset,mutating=true,failurePolicy=fail,sideEffects=None,groups=apps.churnless.io,resources=replicasets,verbs=create;update,versions=v1alpha1,name=mreplicaset-v1alpha1.kb.io,admissionReviewVersions=v1

// ReplicaSetCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind ReplicaSet when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type ReplicaSetCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind ReplicaSet.
func (d *ReplicaSetCustomDefaulter) Default(_ context.Context, obj *appsv1alpha1.ReplicaSet) error {
	kubecompat.DefaultReplicaSet(&obj.Spec.ReplicaSetSpec)
	return nil
}

// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-apps-churnless-io-v1alpha1-replicaset,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps.churnless.io,resources=replicasets,verbs=create;update,versions=v1alpha1,name=vreplicaset-v1alpha1.kb.io,admissionReviewVersions=v1

// ReplicaSetCustomValidator struct is responsible for validating the ReplicaSet resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type ReplicaSetCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type ReplicaSet.
func (v *ReplicaSetCustomValidator) ValidateCreate(ctx context.Context, obj *appsv1alpha1.ReplicaSet) (admission.Warnings, error) {
	return kubecompat.ReplicaSetWarnings(ctx, obj.Name, &obj.Spec.ReplicaSetSpec, nil),
		replicaSetValidationError(obj, nil)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type ReplicaSet.
func (v *ReplicaSetCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *appsv1alpha1.ReplicaSet) (admission.Warnings, error) {
	return kubecompat.ReplicaSetWarnings(
			ctx,
			newObj.Name,
			&newObj.Spec.ReplicaSetSpec,
			&oldObj.Spec.ReplicaSetSpec,
		),
		replicaSetValidationError(newObj, oldObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type ReplicaSet.
func (v *ReplicaSetCustomValidator) ValidateDelete(_ context.Context, obj *appsv1alpha1.ReplicaSet) (admission.Warnings, error) {
	return nil, nil
}

func replicaSetValidationError(
	obj, oldObj *appsv1alpha1.ReplicaSet,
) error {
	var oldSpec *appsv1.ReplicaSetSpec
	if oldObj != nil {
		oldSpec = &oldObj.Spec.ReplicaSetSpec
	}
	errs := kubecompat.ValidateReplicaSet(&obj.Spec.ReplicaSetSpec, oldSpec)
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		appsv1alpha1.GroupVersion.WithKind("ReplicaSet").GroupKind(),
		obj.Name,
		errs,
	)
}
