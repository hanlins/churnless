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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

var _ = Describe("ReplicaSet Controller", func() {
	const (
		name      = "replicaset-test"
		namespace = "default"
		oldImage  = "registry.k8s.io/pause:3.9"
		newImage  = "registry.k8s.io/pause:3.10"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: name, Namespace: namespace}

	AfterEach(func() {
		deletePodsWithLabel(ctx, namespace, name)
		deleteIfPresent(
			ctx,
			&appsv1alpha1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			},
		)
	})

	It("owns Pods directly and applies image changes in place", func() {
		replicas := int32(1)
		workload := &appsv1alpha1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, oldImage),
			}},
		}
		Expect(k8sClient.Create(ctx, workload)).To(Succeed())
		reconciler := &ReplicaSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		By("creating a Pod controlled directly by the Churnless ReplicaSet")
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		pod := onlyPodWithLabel(ctx, namespace, name)
		Expect(metav1.IsControlledBy(pod, workload)).To(BeTrue())
		Expect(pod.Spec.Containers[0].Image).To(Equal(oldImage))
		uid := pod.UID

		By("reporting the initial Pod ready")
		setPodReady(ctx, pod, oldImage, "10.0.0.41")
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("changing the ReplicaSet image")
		Expect(k8sClient.Get(ctx, key, workload)).To(Succeed())
		workload.Spec.Template.Spec.Containers[0].Image = newImage
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Spec.Containers[0].Image).To(Equal(newImage))
		Expect(pod.UID).To(Equal(uid))
		Expect(pod.Status.PodIP).To(Equal("10.0.0.41"))
		Expect(pod.Annotations[revisionAnnotation]).
			To(Equal((mutablePodPolicy{}).revision(&workload.Spec.Template)))

		var native appsv1.ReplicaSet
		Expect(k8sClient.Get(ctx, key, &native)).To(MatchError(ContainSubstring("not found")))
	})

	It("applies Pod-template labels and annotations in place", func() {
		replicas := int32(1)
		workload := &appsv1alpha1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, oldImage),
			}},
		}
		Expect(k8sClient.Create(ctx, workload)).To(Succeed())
		reconciler := &ReplicaSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		pod := onlyPodWithLabel(ctx, namespace, name)
		uid := pod.UID

		Expect(k8sClient.Get(ctx, key, workload)).To(Succeed())
		workload.Spec.Template.Labels["example.com/track"] = updatedTestValue
		workload.Spec.Template.Annotations = map[string]string{
			templateRevisionAnnotation: updatedTestValue,
		}
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.UID).To(Equal(uid))
		Expect(pod.Labels["example.com/track"]).To(Equal(updatedTestValue))
		Expect(pod.Annotations[templateRevisionAnnotation]).To(Equal(updatedTestValue))
	})
})

func deletePodsWithLabel(ctx context.Context, namespace, value string) {
	var pods corev1.PodList
	Expect(k8sClient.List(
		ctx,
		&pods,
		client.InNamespace(namespace),
		client.MatchingLabels{appLabel: value},
	)).To(Succeed())
	for i := range pods.Items {
		deleteIfPresent(ctx, &pods.Items[i])
	}
}

func onlyPodWithLabel(ctx context.Context, namespace, value string) *corev1.Pod {
	var pods corev1.PodList
	Expect(k8sClient.List(
		ctx,
		&pods,
		client.InNamespace(namespace),
		client.MatchingLabels{appLabel: value},
	)).To(Succeed())
	Expect(pods.Items).To(HaveLen(1))
	return &pods.Items[0]
}

func setPodReady(ctx context.Context, pod *corev1.Pod, image, ip string) {
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
	pod.Status.PodIP = ip
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: pod.Spec.Containers[0].Name, Image: image, Ready: true,
	}}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}
