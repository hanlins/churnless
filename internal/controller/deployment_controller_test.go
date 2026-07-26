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
	"maps"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const appLabel = "app"

var _ = Describe("Deployment Controller", func() {
	const (
		name      = "deployment-test"
		namespace = "default"
		oldImage  = "registry.k8s.io/pause:3.9"
		newImage  = "registry.k8s.io/pause:3.10"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: name, Namespace: namespace}
	replicas := int32(1)
	reconciler := &DeploymentReconciler{}

	BeforeEach(func() {
		reconciler = &DeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		workload := &appsv1alpha1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType},
				Template: podTemplate(name, oldImage),
			}},
		}
		Expect(k8sClient.Create(ctx, workload)).To(Succeed())
	})

	AfterEach(func() {
		deleteIfPresent(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: namespace}})
		deleteIfPresent(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
		deleteIfPresent(
			ctx,
			&appsv1alpha1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}},
		)
	})

	It("preserves the Pod identity for an image-only update and supports /scale", func() {
		By("creating a native shadow with the complete embedded Deployment spec")
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var shadow appsv1.Deployment
		Expect(k8sClient.Get(ctx, key, &shadow)).To(Succeed())
		Expect(shadow.Spec.Template.Spec.Containers[0].Image).To(Equal(oldImage))
		Expect(shadow.Spec.ProgressDeadlineSeconds).NotTo(BeNil())
		Expect(metav1.IsControlledBy(&shadow, currentDeployment(ctx, key))).To(BeTrue())

		shadow.Status.AvailableReplicas = 1
		shadow.Status.ReadyReplicas = 1
		shadow.Status.Replicas = 1
		shadow.Status.ObservedGeneration = shadow.Generation
		Expect(k8sClient.Status().Update(ctx, &shadow)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name + "-pod",
				Namespace:   namespace,
				Labels:      copyMap(shadow.Spec.Template.Labels),
				Annotations: copyMap(shadow.Spec.Template.Annotations),
			},
			Spec: *shadow.Spec.Template.Spec.DeepCopy(),
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.42"
		pod.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: name, Image: oldImage, Ready: true,
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		uid := pod.UID

		By("changing only the custom workload image")
		workload := currentDeployment(ctx, key)
		workload.Spec.Template.Spec.Containers[0].Image = newImage
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, key, &shadow)).To(Succeed())
		Expect(shadow.Spec.Template.Spec.Containers[0].Image).To(
			Equal(oldImage),
			"the shadow template must stay stable so the native controller does not replace Pods",
		)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Spec.Containers[0].Image).To(Equal(newImage))
		Expect(pod.UID).To(Equal(uid))
		Expect(pod.Status.PodIP).To(Equal("10.0.0.42"))

		By("scaling through the standard scale subresource used by HPA")
		workload = currentDeployment(ctx, key)
		scale := &autoscalingv1.Scale{}
		Expect(k8sClient.SubResource("scale").Get(ctx, workload, scale)).To(Succeed())
		scale.Spec.Replicas = 3
		Expect(k8sClient.SubResource("scale").Update(ctx, workload, client.WithSubResourceBody(scale))).To(Succeed())
		Expect(*currentDeployment(ctx, key).Spec.Replicas).To(Equal(int32(3)))
	})
})

func podTemplate(name, image string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabel: name}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: name, Image: image,
		}}},
	}
}

func currentDeployment(ctx context.Context, key types.NamespacedName) *appsv1alpha1.Deployment {
	var workload appsv1alpha1.Deployment
	Expect(k8sClient.Get(ctx, key, &workload)).To(Succeed())
	return &workload
}

func deleteIfPresent(ctx context.Context, object client.Object) {
	err := k8sClient.Delete(ctx, object)
	Expect(client.IgnoreNotFound(err)).To(Succeed())
}

func copyMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	maps.Copy(result, input)
	return result
}
