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
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

var _ = Describe("Migration Controller", func() {
	const (
		namespace = "default"
		image     = "registry.k8s.io/pause:3.10"
	)

	ctx := context.Background()
	reconciler := &MigrationReconciler{}

	BeforeEach(func() {
		reconciler = &MigrationReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	AfterEach(func() {
		for _, name := range []string{"takeover-test", "handoff-test"} {
			deletePodsWithLabel(ctx, namespace, name)
			deleteIfPresent(ctx, &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			})
			deleteIfPresent(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			})
			deleteIfPresent(ctx, &appsv1alpha1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			})
		}
		var nativeReplicaSets appsv1.ReplicaSetList
		Expect(k8sClient.List(ctx, &nativeReplicaSets, client.InNamespace(namespace))).To(Succeed())
		for i := range nativeReplicaSets.Items {
			if nativeReplicaSets.Items[i].Annotations[migrationIDAnnotation] != "" {
				deleteIfPresent(ctx, &nativeReplicaSets.Items[i])
			}
		}
		var churnlessReplicaSets appsv1alpha1.ReplicaSetList
		Expect(k8sClient.List(ctx, &churnlessReplicaSets, client.InNamespace(namespace))).To(Succeed())
		for i := range churnlessReplicaSets.Items {
			if churnlessReplicaSets.Items[i].Annotations[migrationIDAnnotation] != "" ||
				metav1.GetControllerOf(&churnlessReplicaSets.Items[i]) != nil {
				deleteIfPresent(ctx, &churnlessReplicaSets.Items[i])
			}
		}
	})

	It("starts takeover only after the native Deployment is complete", func() {
		const name = "takeover-test"
		replicas := int32(1)
		source := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   namespace,
				Labels:      map[string]string{"example.com/workload": "takeover"},
				Annotations: map[string]string{takeoverAnnotation: annotationEnabledValue},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Paused:   true,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, image),
			},
		}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: namespace}
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       deploymentKind,
					Name:       name,
				},
				MinReplicas: &replicas,
				MaxReplicas: 2,
			},
		}
		Expect(k8sClient.Create(ctx, hpa)).To(Succeed())

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
		var target appsv1alpha1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(MatchError(ContainSubstring("not found")))

		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		source.Status = appsv1.DeploymentStatus{
			ObservedGeneration: source.Generation,
			Replicas:           replicas,
			UpdatedReplicas:    replicas,
			ReadyReplicas:      replicas,
			AvailableReplicas:  replicas,
		}
		Expect(k8sClient.Status().Update(ctx, source)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, key, &target)).To(Succeed())
		Expect(target.Labels).To(Equal(source.Labels))
		Expect(target.Annotations).NotTo(HaveKey(takeoverAnnotation))
		Expect(target.Annotations[migrationSourceAnnotation]).To(Equal(nativeDeploymentSource))
		Expect(target.Annotations[migrationIDAnnotation]).To(Equal(string(source.UID)))
		Expect(target.Annotations[migrationPhaseAnnotation]).To(Equal(migrationPhaseWarming))
		Expect(target.Annotations[migrationOriginalPausedAnnotation]).To(Equal(annotationEnabledValue))
		Expect(target.Spec.Paused).To(BeFalse())
		Expect(target.Spec.Template).To(Equal(source.Spec.Template))

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, hpa)).To(Succeed())
		Expect(hpa.Spec.ScaleTargetRef.APIVersion).
			To(Equal(appsv1alpha1.GroupVersion.String()))
	})

	It("creates a native target for a complete Churnless Deployment", func() {
		const name = "handoff-test"
		replicas := int32(1)
		source := &appsv1alpha1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   namespace,
				Labels:      map[string]string{"example.com/workload": "handoff"},
				Annotations: map[string]string{handoffAnnotation: annotationEnabledValue},
			},
			Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Paused:   true,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, image),
			}},
		}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: namespace}
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())

		revision := structuralRevision(source)
		template := source.Spec.Template.DeepCopy()
		template.Labels[structuralRevisionLabel] = revision
		selector := source.Spec.Selector.DeepCopy()
		selector.MatchLabels[structuralRevisionLabel] = revision
		replicaSet := &appsv1alpha1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      replicaSetName(name, revision),
				Namespace: namespace,
				Labels:    map[string]string{structuralRevisionLabel: revision},
			},
			Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
				Selector: selector,
				Template: *template,
			}},
		}
		Expect(controllerutil.SetControllerReference(source, replicaSet, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaSet)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(replicaSet), replicaSet)).To(Succeed())
		replicaSet.Status = appsv1alpha1.ReplicaSetStatus{
			ReplicaSetStatus: appsv1.ReplicaSetStatus{
				ObservedGeneration: replicaSet.Generation,
				Replicas:           replicas,
				ReadyReplicas:      replicas,
				AvailableReplicas:  replicas,
			},
			InPlace: &appsv1alpha1.InPlaceUpdateStatus{
				Revision:             mutablePodPolicyFor(replicaSet.Annotations).revision(&replicaSet.Spec.Template),
				UpdatedReplicas:      replicas,
				ReadyUpdatedReplicas: replicas,
			},
		}
		Expect(k8sClient.Status().Update(ctx, replicaSet)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var target appsv1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(Succeed())
		Expect(target.Labels).To(Equal(source.Labels))
		Expect(target.Annotations).NotTo(HaveKey(handoffAnnotation))
		Expect(target.Annotations[migrationSourceAnnotation]).To(Equal(churnlessDeploymentSource))
		Expect(target.Annotations[migrationIDAnnotation]).To(Equal(string(source.UID)))
		Expect(target.Annotations[migrationOriginalPausedAnnotation]).To(Equal(annotationEnabledValue))
		Expect(target.Spec.Paused).To(BeFalse())
		Expect(target.Spec.Selector).To(Equal(source.Spec.Selector))
		Expect(target.Spec.Template.Labels).To(Equal(source.Spec.Template.Labels))
		Expect(target.Spec.Template.Spec.Containers[0].Image).
			To(Equal(source.Spec.Template.Spec.Containers[0].Image))
	})

	It("marks source Pods for selector-compatible preferential adoption", func() {
		const name = "takeover-test"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   namespace,
				Labels:      map[string]string{appLabel: name},
				Annotations: map[string]string{corev1.PodDeletionCost: "7"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: image}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		changed, err := reconciler.preparePod(
			ctx,
			pod,
			"migration-uid",
			migrationRoleSource,
			sourceDeletionCost,
			map[string]string{structuralRevisionLabel: "revision"},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Labels[structuralRevisionLabel]).To(Equal("revision"))
		Expect(pod.Annotations[migrationIDAnnotation]).To(Equal("migration-uid"))
		Expect(pod.Annotations[migrationRoleAnnotation]).To(Equal(migrationRoleSource))
		Expect(pod.Annotations[corev1.PodDeletionCost]).To(Equal(sourceDeletionCost))
	})
})
