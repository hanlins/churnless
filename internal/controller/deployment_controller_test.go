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
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	deploymentReconciler := &DeploymentReconciler{}
	replicaSetReconciler := &ReplicaSetReconciler{}

	BeforeEach(func() {
		deploymentReconciler = &DeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		replicaSetReconciler = &ReplicaSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
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
		deletePodsWithLabel(ctx, namespace, name)
		var replicaSets appsv1alpha1.ReplicaSetList
		Expect(k8sClient.List(ctx, &replicaSets, client.InNamespace(namespace))).To(Succeed())
		for i := range replicaSets.Items {
			if replicaSets.Items[i].Labels[structuralRevisionLabel] != "" {
				deleteIfPresent(ctx, &replicaSets.Items[i])
			}
		}
		deleteIfPresent(
			ctx,
			&appsv1alpha1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}},
		)
	})

	It("keeps the same ReplicaSet and Pod identity for an image update", func() {
		By("creating a Churnless ReplicaSet and letting it create the Pod")
		reconcileDeployment(ctx, key, deploymentReconciler, 4)
		replicaSet := onlyDeploymentReplicaSet(ctx)
		Expect(metav1.IsControlledBy(replicaSet, currentDeployment(ctx, key))).To(BeTrue())
		Expect(replicaSet.Spec.Template.Spec.Containers[0].Image).To(Equal(oldImage))

		_, err := replicaSetReconciler.Reconcile(
			ctx,
			reconcile.Request{NamespacedName: client.ObjectKeyFromObject(replicaSet)},
		)
		Expect(err).NotTo(HaveOccurred())
		pod := onlyPodWithLabel(ctx, namespace, name)
		Expect(metav1.IsControlledBy(pod, replicaSet)).To(BeTrue())
		setPodReady(ctx, pod, oldImage, "10.0.0.42")
		_, err = replicaSetReconciler.Reconcile(
			ctx,
			reconcile.Request{NamespacedName: client.ObjectKeyFromObject(replicaSet)},
		)
		Expect(err).NotTo(HaveOccurred())
		uid := pod.UID
		replicaSetUID := replicaSet.UID

		By("changing only the Deployment image")
		workload := currentDeployment(ctx, key)
		workload.Spec.Template.Spec.Containers[0].Image = newImage
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		reconcileDeployment(ctx, key, deploymentReconciler, 2)

		replicaSet = onlyDeploymentReplicaSet(ctx)
		Expect(replicaSet.UID).To(Equal(replicaSetUID))
		Expect(replicaSet.Spec.Template.Spec.Containers[0].Image).To(Equal(newImage))
		_, err = replicaSetReconciler.Reconcile(
			ctx,
			reconcile.Request{NamespacedName: client.ObjectKeyFromObject(replicaSet)},
		)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Spec.Containers[0].Image).To(Equal(newImage))
		Expect(pod.UID).To(Equal(uid))
		Expect(pod.Status.PodIP).To(Equal("10.0.0.42"))
		reconcileDeployment(ctx, key, deploymentReconciler, 1)

		var native appsv1.Deployment
		Expect(k8sClient.Get(ctx, key, &native)).To(MatchError(ContainSubstring("not found")))

		By("scaling through the standard scale subresource used by HPA")
		workload = currentDeployment(ctx, key)
		scale := &autoscalingv1.Scale{}
		Expect(k8sClient.SubResource("scale").Get(ctx, workload, scale)).To(Succeed())
		Expect(scale.Status.Selector).To(Equal(appLabel + "=" + name))
		scale.Spec.Replicas = 3
		Expect(k8sClient.SubResource("scale").Update(
			ctx,
			workload,
			client.WithSubResourceBody(scale),
		)).To(Succeed())
		Expect(*currentDeployment(ctx, key).Spec.Replicas).To(Equal(int32(3)))
	})

	It("creates a new ReplicaSet for a structural template change", func() {
		reconcileDeployment(ctx, key, deploymentReconciler, 1)
		first := onlyDeploymentReplicaSet(ctx)

		workload := currentDeployment(ctx, key)
		workload.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{
			Name: "STRUCTURAL_REVISION", Value: updatedTestValue,
		}}
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		reconcileDeployment(ctx, key, deploymentReconciler, 1)

		replicaSets := deploymentReplicaSets(ctx)
		Expect(replicaSets).To(HaveLen(2))
		Expect(replicaSets[0].UID == first.UID || replicaSets[1].UID == first.UID).To(BeTrue())
		Expect(replicaSets[0].Labels[structuralRevisionLabel]).
			NotTo(Equal(replicaSets[1].Labels[structuralRevisionLabel]))
	})

	It("keeps one ReplicaSet for mutable metadata and best-effort CPU resources", func() {
		workload := currentDeployment(ctx, key)
		workload.Annotations = map[string]string{
			inPlaceResourcesAnnotation: inPlaceResourcesBestEffort,
		}
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		reconcileDeployment(ctx, key, deploymentReconciler, 3)
		before := onlyDeploymentReplicaSet(ctx)

		workload = currentDeployment(ctx, key)
		workload.Spec.Template.Labels["example.com/track"] = updatedTestValue
		workload.Spec.Template.Annotations = map[string]string{
			templateRevisionAnnotation: updatedTestValue,
		}
		workload.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("200m"),
		}
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		reconcileDeployment(ctx, key, deploymentReconciler, 3)

		after := onlyDeploymentReplicaSet(ctx)
		Expect(after.UID).To(Equal(before.UID))
		Expect(after.Spec.Template.Labels["example.com/track"]).To(Equal(updatedTestValue))
		Expect(after.Spec.Template.Annotations[templateRevisionAnnotation]).
			To(Equal(updatedTestValue))
		Expect(after.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String()).
			To(Equal("200m"))
		Expect(after.Annotations[inPlaceResourcesAnnotation]).
			To(Equal(inPlaceResourcesBestEffort))
	})

	It("creates a new ReplicaSet for an explicit redeploy token", func() {
		reconcileDeployment(ctx, key, deploymentReconciler, 3)
		before := onlyDeploymentReplicaSet(ctx)

		workload := currentDeployment(ctx, key)
		if workload.Annotations == nil {
			workload.Annotations = map[string]string{}
		}
		workload.Annotations[redeployAnnotation] = redeployTestTimestamp
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		reconcileDeployment(ctx, key, deploymentReconciler, 2)

		replicaSets := deploymentReplicaSets(ctx)
		Expect(replicaSets).To(HaveLen(2))
		Expect(replicaSets[0].UID == before.UID || replicaSets[1].UID == before.UID).To(BeTrue())
	})

	It("does not advance image or structural revisions while paused", func() {
		reconcileDeployment(ctx, key, deploymentReconciler, 3)
		before := onlyDeploymentReplicaSet(ctx)
		beforeUID := before.UID
		beforeRevision := before.Labels[structuralRevisionLabel]

		workload := currentDeployment(ctx, key)
		workload.Spec.Paused = true
		workload.Spec.Template.Spec.Containers[0].Image = newImage
		workload.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{
			Name: "STRUCTURAL_REVISION", Value: "paused",
		}}
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		reconcileDeployment(ctx, key, deploymentReconciler, 3)

		replicaSets := deploymentReplicaSets(ctx)
		Expect(replicaSets).To(HaveLen(1))
		Expect(replicaSets[0].UID).To(Equal(beforeUID))
		Expect(replicaSets[0].Labels[structuralRevisionLabel]).To(Equal(beforeRevision))
		Expect(replicaSets[0].Spec.Template.Spec.Containers[0].Image).To(Equal(oldImage))
		Expect(replicaSets[0].Spec.Template.Spec.Containers[0].Env).To(BeEmpty())
		Expect(replicaSets[0].Annotations[inPlaceParallelismAnnotation]).To(Equal("0"))
	})

	It("does not trust stale ReplicaSet image progress", func() {
		replicaSet := &appsv1alpha1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Generation: 2},
			Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
				Template: podTemplate(name, newImage),
			}},
			Status: appsv1alpha1.ReplicaSetStatus{
				ReplicaSetStatus: appsv1.ReplicaSetStatus{ObservedGeneration: 1},
				InPlace: &appsv1alpha1.InPlaceUpdateStatus{
					Revision:             imageRevisionPointer(newImage),
					UpdatedReplicas:      1,
					ReadyUpdatedReplicas: 1,
				},
			},
		}

		progress := observedInPlaceStatus(replicaSet)
		Expect(progress.Revision).To(Equal((mutablePodPolicy{}).revision(&replicaSet.Spec.Template)))
		Expect(progress.UpdatedReplicas).To(BeZero())
		Expect(progress.ReadyUpdatedReplicas).To(BeZero())

		replicaSet.Status.ObservedGeneration = replicaSet.Generation
		replicaSet.Status.InPlace.Revision = (mutablePodPolicy{}).revision(&replicaSet.Spec.Template)
		Expect(observedInPlaceStatus(replicaSet).UpdatedReplicas).To(Equal(int32(1)))
	})
})

func imageRevisionPointer(image string) string {
	template := podTemplate("revision", image)
	return (mutablePodPolicy{}).revision(&template)
}

func reconcileDeployment(
	ctx context.Context,
	key types.NamespacedName,
	reconciler *DeploymentReconciler,
	count int,
) {
	for range count {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}
}

func onlyDeploymentReplicaSet(ctx context.Context) *appsv1alpha1.ReplicaSet {
	matches := deploymentReplicaSets(ctx)
	Expect(matches).To(HaveLen(1))
	return &matches[0]
}

func deploymentReplicaSets(ctx context.Context) []appsv1alpha1.ReplicaSet {
	var replicaSets appsv1alpha1.ReplicaSetList
	Expect(k8sClient.List(ctx, &replicaSets, client.InNamespace("default"))).To(Succeed())
	matches := make([]appsv1alpha1.ReplicaSet, 0, 1)
	for i := range replicaSets.Items {
		owner := metav1.GetControllerOf(&replicaSets.Items[i])
		if owner != nil && owner.Kind == "Deployment" && owner.Name == "deployment-test" {
			matches = append(matches, replicaSets.Items[i])
		}
	}
	return matches
}

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
