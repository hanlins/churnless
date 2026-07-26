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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const (
	webhookAppLabel = "app"
	webhookOther    = "other"
)

var _ = Describe("Deployment webhook", func() {
	defaulter := DeploymentCustomDefaulter{}
	validator := DeploymentCustomValidator{}

	It("applies native workload defaults", func() {
		workload := validDeployment("defaulted")
		Expect(defaulter.Default(ctx, workload)).To(Succeed())
		Expect(*workload.Spec.Replicas).To(Equal(int32(1)))
		Expect(workload.Spec.Strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
		Expect(workload.Spec.Strategy.RollingUpdate.MaxSurge.String()).To(Equal("25%"))
		Expect(workload.Spec.Strategy.RollingUpdate.MaxUnavailable.String()).To(Equal("25%"))
		Expect(*workload.Spec.RevisionHistoryLimit).To(Equal(int32(10)))
		Expect(*workload.Spec.ProgressDeadlineSeconds).To(Equal(int32(600)))
		Expect(workload.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyAlways))
	})

	It("rejects selector mismatches and an impossible rolling strategy", func() {
		workload := validDeployment("invalid")
		Expect(defaulter.Default(ctx, workload)).To(Succeed())
		workload.Spec.Template.Labels[webhookAppLabel] = webhookOther
		_, err := validator.ValidateCreate(ctx, workload)
		Expect(err).To(MatchError(ContainSubstring("selector")))

		workload = validDeployment("invalid-fenceposts")
		Expect(defaulter.Default(ctx, workload)).To(Succeed())
		zero := intstr.FromInt32(0)
		workload.Spec.Strategy.RollingUpdate.MaxSurge = &zero
		workload.Spec.Strategy.RollingUpdate.MaxUnavailable = &zero
		_, err = validator.ValidateCreate(ctx, workload)
		Expect(err).To(MatchError(ContainSubstring("may not be 0")))
	})

	It("rejects selector mutation", func() {
		oldWorkload := validDeployment("immutable")
		Expect(defaulter.Default(ctx, oldWorkload)).To(Succeed())
		workload := oldWorkload.DeepCopy()
		workload.Spec.Selector.MatchLabels[webhookAppLabel] = webhookOther
		workload.Spec.Template.Labels[webhookAppLabel] = webhookOther
		_, err := validator.ValidateUpdate(ctx, oldWorkload, workload)
		Expect(err).To(MatchError(ContainSubstring("field is immutable")))
	})

	It("returns upstream workload warnings", func() {
		workload := validDeployment("web.example")
		Expect(defaulter.Default(ctx, workload)).To(Succeed())
		warnings, err := validator.ValidateCreate(ctx, workload)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("DNS label is recommended")))
	})
})

func validDeployment(name string) *appsv1alpha1.Deployment {
	return &appsv1alpha1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{webhookAppLabel: name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{webhookAppLabel: name}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "web",
					Image: "nginx:1.28-alpine",
				}}},
			},
		}},
	}
}
