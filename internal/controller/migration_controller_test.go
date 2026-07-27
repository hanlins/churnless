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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

var _ = Describe("Migration Controller", func() {
	const (
		namespace             = "default"
		image                 = "registry.k8s.io/pause:3.10"
		testDeletionFinalizer = "test.churnless.io/block-deletion"
	)

	ctx := context.Background()
	reconciler := &MigrationReconciler{}

	BeforeEach(func() {
		reconciler = &MigrationReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	AfterEach(func() {
		for _, name := range []string{
			"takeover-test",
			"handoff-test",
			"recovery-test",
			"cancel-test",
			"cancel-handoff-test",
			"degrade-test",
			"deleting-source-test",
			"deleting-target-test",
			"late-return-test",
			"adoption-test",
		} {
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
				Name:      name,
				Namespace: namespace,
				Labels:    map[string]string{"example.com/workload": "takeover"},
				Annotations: map[string]string{
					controllerAnnotation: churnlessControllerValue,
				},
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
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
		Expect(target.Annotations[migrationSourceAnnotation]).To(Equal(nativeDeploymentSource))
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModePreserve))
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
				Name:      name,
				Namespace: namespace,
				Labels:    map[string]string{"example.com/workload": "handoff"},
				Annotations: map[string]string{
					controllerAnnotation: nativeControllerValue,
				},
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
		Expect(target.Annotations[controllerAnnotation]).To(Equal(nativeControllerValue))
		Expect(target.Annotations[migrationSourceAnnotation]).To(Equal(churnlessDeploymentSource))
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModePreserve))
		Expect(target.Annotations[migrationIDAnnotation]).To(Equal(string(source.UID)))
		Expect(target.Annotations[migrationOriginalPausedAnnotation]).To(Equal(annotationEnabledValue))
		Expect(target.Spec.Paused).To(BeFalse())
		Expect(target.Spec.Selector).To(Equal(source.Spec.Selector))
		Expect(target.Spec.Template.Labels).To(Equal(source.Spec.Template.Labels))
		Expect(target.Spec.Template.Spec.Containers[0].Image).
			To(Equal(source.Spec.Template.Spec.Containers[0].Image))
	})

	It("starts recovery handoff before an unhealthy Churnless rollout completes", func() {
		const name = "recovery-test"
		replicas := int32(1)
		source := &appsv1alpha1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Annotations: map[string]string{
					controllerAnnotation: nativeControllerValue,
				},
			},
			Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, image),
			}},
		}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: namespace}
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var target appsv1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(nativeControllerValue))
		Expect(target.Annotations[migrationSourceAnnotation]).To(Equal(churnlessDeploymentSource))
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModeRecovery))
		Expect(target.Spec.Template.Labels).To(Equal(source.Spec.Template.Labels))
		Expect(target.Spec.Template.Spec.Containers[0].Image).
			To(Equal(source.Spec.Template.Spec.Containers[0].Image))
	})

	It("does not start takeover from a deleting native Deployment", func() {
		const name = "deleting-source-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := nativeMigrationTestDeployment(
			name,
			namespace,
			image,
			churnlessControllerValue,
		)
		source.Finalizers = []string{testDeletionFinalizer}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		clearFinalizersAfterTest(ctx, source)
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		replicas := desiredReplicas(source.Spec.Replicas)
		source.Status = appsv1.DeploymentStatus{
			ObservedGeneration: source.Generation,
			Replicas:           replicas,
			UpdatedReplicas:    replicas,
			ReadyReplicas:      replicas,
			AvailableReplicas:  replicas,
		}
		Expect(k8sClient.Status().Update(ctx, source)).To(Succeed())
		Expect(k8sClient.Delete(ctx, source)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		Expect(source.DeletionTimestamp.IsZero()).To(BeFalse())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var target appsv1alpha1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(Satisfy(apierrors.IsNotFound))
	})

	It("does not cut over to a deleting migration target", func() {
		const name = "deleting-target-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := nativeMigrationTestDeployment(
			name,
			namespace,
			image,
			churnlessControllerValue,
		)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		targetMetadata := migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			nativeDeploymentSource,
			migrationModePreserve,
		)
		targetMetadata.Finalizers = []string{testDeletionFinalizer}
		target := &appsv1alpha1.Deployment{
			ObjectMeta: targetMetadata,
			Spec:       appsv1alpha1.DeploymentSpec{DeploymentSpec: *source.Spec.DeepCopy()},
		}
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		clearFinalizersAfterTest(ctx, target)
		Expect(k8sClient.Delete(ctx, target)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.DeletionTimestamp.IsZero()).To(BeFalse())

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		Expect(source.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(source.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
	})

	It("cancels an in-progress takeover when native ownership is requested", func() {
		const name = "cancel-test"
		replicas := int32(1)
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Annotations: map[string]string{
					controllerAnnotation: churnlessControllerValue,
				},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, image),
			},
		}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		target := &appsv1alpha1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Annotations: map[string]string{
					controllerAnnotation:      nativeControllerValue,
					migrationIDAnnotation:     string(source.UID),
					migrationModeAnnotation:   migrationModePreserve,
					migrationSourceAnnotation: nativeDeploymentSource,
				},
			},
			Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: *source.Spec.DeepCopy()},
		}
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: appsv1alpha1.GroupVersion.String(),
					Kind:       deploymentKind,
					Name:       name,
				},
				MinReplicas: &replicas,
				MaxReplicas: 2,
			},
		}
		Expect(k8sClient.Create(ctx, hpa)).To(Succeed())

		Eventually(func(g Gomega) {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			g.Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, key, target)
			g.Expect(err == nil || apierrors.IsNotFound(err)).To(BeTrue())
			if err == nil {
				g.Expect(target.DeletionTimestamp.IsZero()).To(BeFalse())
			}
			g.Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
			g.Expect(source.Annotations[controllerAnnotation]).To(Equal(nativeControllerValue))
			g.Expect(k8sClient.Get(ctx, key, hpa)).To(Succeed())
			g.Expect(hpa.Spec.ScaleTargetRef.APIVersion).
				To(Equal(appsv1.SchemeGroupVersion.String()))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
	})

	It("cancels an in-progress handoff when Churnless ownership is requested", func() {
		const name = "cancel-handoff-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(
			name,
			namespace,
			image,
			nativeControllerValue,
		)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		targetMetadata := migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			churnlessDeploymentSource,
			migrationModePreserve,
		)
		targetMetadata.Annotations[controllerAnnotation] = churnlessControllerValue
		target := &appsv1.Deployment{
			ObjectMeta: targetMetadata,
			Spec:       *source.Spec.DeploymentSpec.DeepCopy(),
		}
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		replicas := desiredReplicas(source.Spec.Replicas)
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

		Eventually(func(g Gomega) {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			g.Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, key, target)
			g.Expect(err == nil || apierrors.IsNotFound(err)).To(BeTrue())
			if err == nil {
				g.Expect(target.DeletionTimestamp.IsZero()).To(BeFalse())
			}
			g.Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
			g.Expect(source.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
			g.Expect(k8sClient.Get(ctx, key, hpa)).To(Succeed())
			g.Expect(hpa.Spec.ScaleTargetRef.APIVersion).
				To(Equal(appsv1alpha1.GroupVersion.String()))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
	})

	It("switches a preserve handoff to recovery if the source degrades", func() {
		const name = "degrade-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(
			name,
			namespace,
			image,
			nativeControllerValue,
		)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		target := &appsv1.Deployment{
			ObjectMeta: migrationTargetMetadata(
				source.ObjectMeta,
				source.UID,
				source.Generation,
				source.Spec.Paused,
				churnlessDeploymentSource,
				migrationModePreserve,
			),
			Spec: *source.Spec.DeploymentSpec.DeepCopy(),
		}
		Expect(k8sClient.Create(ctx, target)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModeRecovery))
	})

	It("honors a Churnless return requested after handoff source deletion starts", func() {
		const name = "late-return-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(
			name,
			namespace,
			image,
			nativeControllerValue,
		)
		source.Finalizers = []string{testDeletionFinalizer}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		clearFinalizersAfterTest(ctx, source)
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		target := &appsv1.Deployment{
			ObjectMeta: migrationTargetMetadata(
				source.ObjectMeta,
				source.UID,
				source.Generation,
				source.Spec.Paused,
				churnlessDeploymentSource,
				migrationModePreserve,
			),
			Spec: *source.Spec.DeploymentSpec.DeepCopy(),
		}
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name + "-target",
				Namespace: namespace,
				Annotations: map[string]string{
					migrationIDAnnotation: string(source.UID),
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: source.Spec.Replicas,
				Selector: source.Spec.Selector.DeepCopy(),
				Template: *source.Spec.Template.DeepCopy(),
			},
		}
		Expect(controllerutil.SetControllerReference(target, replicaSet, k8sClient.Scheme())).
			To(Succeed())
		Expect(k8sClient.Create(ctx, replicaSet)).To(Succeed())

		Expect(k8sClient.Delete(ctx, source)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		source.Annotations[controllerAnnotation] = churnlessControllerValue
		Expect(k8sClient.Update(ctx, source)).To(Succeed())

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(reconcile.Result{Requeue: true}))
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))

		source.Finalizers = nil
		Expect(k8sClient.Update(ctx, source)).To(Succeed())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, key, source)
			return apierrors.IsNotFound(err)
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

		result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(reconcile.Result{Requeue: true}))
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
		Expect(target.Annotations).NotTo(HaveKey(migrationSourceAnnotation))
	})

	It("marks source Pods for adoption and restores their template labels", func() {
		const name = "takeover-test"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					appLabel:                name,
					structuralRevisionLabel: "original",
				},
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

		sourceTemplate := podTemplate(name, image)
		sourceTemplate.Labels[structuralRevisionLabel] = "original"
		Expect(reconciler.restoreCancelledMigrationPod(
			ctx,
			pod,
			&sourceTemplate,
			structuralRevisionLabel,
		)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Labels[structuralRevisionLabel]).To(Equal("original"))
		Expect(pod.Annotations).NotTo(HaveKey(migrationIDAnnotation))
		Expect(pod.Annotations).NotTo(HaveKey(migrationRoleAnnotation))
		Expect(pod.Annotations).NotTo(HaveKey(corev1.PodDeletionCost))
	})

	It("explicitly adopts orphaned migration Pods into the target ReplicaSet", func() {
		const name = "adoption-test"
		replicas := int32(1)
		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template: podTemplate(name, image),
			},
		}
		Expect(k8sClient.Create(ctx, replicaSet)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(replicaSet), replicaSet)).To(Succeed())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels:    map[string]string{appLabel: name},
				Annotations: map[string]string{
					migrationIDAnnotation:   "migration-uid",
					migrationRoleAnnotation: migrationRoleSource,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: image}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		changed, err := reconciler.adoptMigrationPods(
			ctx,
			namespace,
			"migration-uid",
			nativePodAdoptionTargets([]*appsv1.ReplicaSet{replicaSet}),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(metav1.IsControlledBy(pod, replicaSet)).To(BeTrue())
	})
})

func clearFinalizersAfterTest(ctx context.Context, object client.Object) {
	DeferCleanup(func() {
		fresh := object.DeepCopyObject().(client.Object)
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), fresh)
		if apierrors.IsNotFound(err) {
			return
		}
		Expect(err).NotTo(HaveOccurred())
		if len(fresh.GetFinalizers()) == 0 {
			return
		}
		fresh.SetFinalizers(nil)
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
	})
}

func nativeMigrationTestDeployment(
	name, namespace, image, controller string,
) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{controllerAnnotation: controller},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
			Template: podTemplate(name, image),
		},
	}
}

func churnlessMigrationTestDeployment(
	name, namespace, image, controller string,
) *appsv1alpha1.Deployment {
	native := nativeMigrationTestDeployment(name, namespace, image, controller)
	return &appsv1alpha1.Deployment{
		ObjectMeta: *native.ObjectMeta.DeepCopy(),
		Spec: appsv1alpha1.DeploymentSpec{
			DeploymentSpec: *native.Spec.DeepCopy(),
		},
	}
}
