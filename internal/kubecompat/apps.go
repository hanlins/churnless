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

// Package kubecompat isolates Churnless from Kubernetes's non-staging Go
// packages. Keep k8s.io/kubernetes and all staging modules on the same release.
package kubecompat

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	internalapps "k8s.io/kubernetes/pkg/apis/apps"
	upstreamappsv1 "k8s.io/kubernetes/pkg/apis/apps/v1"
	appsvalidation "k8s.io/kubernetes/pkg/apis/apps/validation"
	internalcore "k8s.io/kubernetes/pkg/apis/core"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
)

const UpstreamVersion = "v1.36.0"

// DefaultDeployment delegates to the covering apps/v1 defaulter generated and
// maintained by Kubernetes. It includes nested Pod-template defaults.
func DefaultDeployment(spec *appsv1.DeploymentSpec) {
	deployment := &appsv1.Deployment{Spec: *spec}
	upstreamappsv1.SetObjectDefaults_Deployment(deployment)
	*spec = deployment.Spec
}

// DefaultReplicaSet delegates to the covering apps/v1 defaulter generated and
// maintained by Kubernetes. It includes nested Pod-template defaults.
func DefaultReplicaSet(spec *appsv1.ReplicaSetSpec) {
	replicaSet := &appsv1.ReplicaSet{Spec: *spec}
	upstreamappsv1.SetObjectDefaults_ReplicaSet(replicaSet)
	*spec = replicaSet.Spec
}

// ValidateDeployment delegates spec and Pod-template validation to
// Kubernetes. Selector immutability normally belongs to the registry update
// strategy, so the CRD webhook applies the same upstream machinery check here.
func ValidateDeployment(
	spec, oldSpec *appsv1.DeploymentSpec,
) field.ErrorList {
	current, errs := deploymentSpecToInternal(spec)
	if len(errs) != 0 {
		return errs
	}

	var previous *internalapps.DeploymentSpec
	var oldTemplate *internalcore.PodTemplateSpec
	if oldSpec != nil {
		previous, errs = deploymentSpecToInternal(oldSpec)
		if len(errs) != 0 {
			return errs
		}
		oldTemplate = &previous.Template
	}

	options := podutil.GetValidationOptionsFromPodTemplate(
		&current.Template,
		oldTemplate,
	)
	errs = appsvalidation.ValidateDeploymentSpec(
		current,
		previous,
		field.NewPath("spec"),
		options,
	)
	if previous != nil {
		errs = append(errs, apivalidation.ValidateImmutableField(
			current.Selector,
			previous.Selector,
			field.NewPath("spec", "selector"),
		)...)
	}
	return errs
}

// ValidateReplicaSet delegates spec and Pod-template validation to Kubernetes.
func ValidateReplicaSet(
	spec, oldSpec *appsv1.ReplicaSetSpec,
) field.ErrorList {
	current, errs := replicaSetSpecToInternal(spec)
	if len(errs) != 0 {
		return errs
	}

	var previous *internalapps.ReplicaSetSpec
	var oldTemplate *internalcore.PodTemplateSpec
	if oldSpec != nil {
		previous, errs = replicaSetSpecToInternal(oldSpec)
		if len(errs) != 0 {
			return errs
		}
		oldTemplate = &previous.Template
	}

	options := podutil.GetValidationOptionsFromPodTemplate(
		&current.Template,
		oldTemplate,
	)
	errs = appsvalidation.ValidateReplicaSetSpec(
		current,
		previous,
		field.NewPath("spec"),
		options,
	)
	if previous != nil {
		errs = append(errs, apivalidation.ValidateImmutableField(
			current.Selector,
			previous.Selector,
			field.NewPath("spec", "selector"),
		)...)
	}
	return errs
}

// DeploymentWarnings delegates Pod-template warnings to Kubernetes and
// preserves its workload-name warning.
func DeploymentWarnings(
	ctx context.Context,
	name string,
	spec, oldSpec *appsv1.DeploymentSpec,
) []string {
	current, errs := deploymentSpecToInternal(spec)
	if len(errs) != 0 {
		return nil
	}
	var oldTemplate *internalcore.PodTemplateSpec
	if oldSpec != nil {
		previous, conversionErrors := deploymentSpecToInternal(oldSpec)
		if len(conversionErrors) != 0 {
			return nil
		}
		oldTemplate = &previous.Template
	}
	return workloadWarnings(ctx, name, &current.Template, oldTemplate)
}

// ReplicaSetWarnings delegates Pod-template warnings to Kubernetes and
// preserves its workload-name warning.
func ReplicaSetWarnings(
	ctx context.Context,
	name string,
	spec, oldSpec *appsv1.ReplicaSetSpec,
) []string {
	current, errs := replicaSetSpecToInternal(spec)
	if len(errs) != 0 {
		return nil
	}
	var oldTemplate *internalcore.PodTemplateSpec
	if oldSpec != nil {
		previous, conversionErrors := replicaSetSpecToInternal(oldSpec)
		if len(conversionErrors) != 0 {
			return nil
		}
		oldTemplate = &previous.Template
	}
	return workloadWarnings(ctx, name, &current.Template, oldTemplate)
}

// ResolveFenceposts delegates the coupled percentage rounding behavior to the
// native Deployment controller helper.
func ResolveFenceposts(
	maxSurge, maxUnavailable *intstr.IntOrString,
	desired int32,
) (int32, int32, error) {
	return deploymentutil.ResolveFenceposts(maxSurge, maxUnavailable, desired)
}

func deploymentSpecToInternal(
	spec *appsv1.DeploymentSpec,
) (*internalapps.DeploymentSpec, field.ErrorList) {
	result := &internalapps.DeploymentSpec{}
	if err := upstreamappsv1.Convert_v1_DeploymentSpec_To_apps_DeploymentSpec(
		spec,
		result,
		nil,
	); err != nil {
		return nil, field.ErrorList{field.InternalError(field.NewPath("spec"), err)}
	}
	return result, nil
}

func workloadWarnings(
	ctx context.Context,
	name string,
	template, oldTemplate *internalcore.PodTemplateSpec,
) []string {
	var warnings []string
	if messages := validation.IsDNS1123Label(name); len(messages) != 0 {
		warnings = append(warnings, fmt.Sprintf(
			"metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: %v",
			messages,
		))
	}
	return append(
		warnings,
		podutil.GetWarningsForPodTemplate(
			ctx,
			field.NewPath("spec", "template"),
			template,
			oldTemplate,
		)...,
	)
}
func replicaSetSpecToInternal(
	spec *appsv1.ReplicaSetSpec,
) (*internalapps.ReplicaSetSpec, field.ErrorList) {
	result := &internalapps.ReplicaSetSpec{}
	if err := upstreamappsv1.Convert_v1_ReplicaSetSpec_To_apps_ReplicaSetSpec(
		spec,
		result,
		nil,
	); err != nil {
		return nil, field.ErrorList{field.InternalError(field.NewPath("spec"), err)}
	}
	return result, nil
}
