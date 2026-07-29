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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const (
	migrationTestNamespace = "default"
	migrationTestImage     = "registry.k8s.io/pause:3.10"
	migrationTestID        = "migration-uid"
	migrationTestContainer = "main"
	invalidMigrationState  = "unknown"
)

var _ = Describe("Deployment migration engine", func() {
	const (
		namespace             = migrationTestNamespace
		image                 = migrationTestImage
		testDeletionFinalizer = "test.churnless.io/block-deletion"
	)

	ctx := context.Background()
	var engine *DeploymentMigrationEngine

	BeforeEach(func() {
		var err error
		engine, err = NewDeploymentMigrationEngine(
			k8sClient,
			k8sClient,
			k8sClient.Scheme(),
		)
		Expect(err).NotTo(HaveOccurred())
	})

	It("requires explicit live API dependencies", func() {
		_, err := NewDeploymentMigrationEngine(
			nil,
			k8sClient,
			k8sClient.Scheme(),
		)
		Expect(err).To(MatchError("migration writer is required"))

		_, err = NewDeploymentMigrationEngine(
			k8sClient,
			nil,
			k8sClient.Scheme(),
		)
		Expect(err).To(MatchError("live migration reader is required"))

		_, err = NewDeploymentMigrationEngine(k8sClient, k8sClient, nil)
		Expect(err).To(MatchError("migration scheme is required"))
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
			"deleting-handoff-source-test",
			"deleting-target-test",
			"late-return-test",
			"adoption-test",
			"driver-request-test",
			"request-delete-race-test",
			"invalid-state-test",
			"target-create-race-test",
			"source-replicaset-race-test",
			"completed-advance-test",
			"superseded-advance-test",
			"missing-deletion-cost-snapshot",
			"invalid-deletion-cost-snapshot",
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
		source := nativeMigrationTestDeployment(name, churnlessControllerValue)
		source.Labels = map[string]string{"example.com/workload": "takeover"}
		source.Spec.Paused = true
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: namespace}
		hpa := migrationTestHPA(name, appsv1.SchemeGroupVersion.String())
		Expect(k8sClient.Create(ctx, hpa)).To(Succeed())

		_, result, err := engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeZero())
		var target appsv1alpha1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(MatchError(ContainSubstring("not found")))

		setNativeMigrationComplete(ctx, source)
		_, _, err = engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, key, &target)).To(Succeed())
		Expect(target.Labels).To(Equal(source.Labels))
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModePreserve))
		Expect(target.Annotations[migrationStateVersionAnnotation]).To(Equal(migrationStateVersion))
		Expect(target.Annotations[migrationIDAnnotation]).To(Equal(string(source.UID)))
		Expect(target.Annotations[migrationOriginalPausedAnnotation]).To(Equal("true"))
		Expect(target.Spec.Paused).To(BeFalse())
		Expect(target.Spec.Template).To(Equal(source.Spec.Template))

		_, _, err = engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, hpa)).To(Succeed())
		Expect(hpa.Spec.ScaleTargetRef.APIVersion).
			To(Equal(appsv1alpha1.GroupVersion.String()))
	})

	It("resumes when another driver creates the takeover target", func() {
		const name = "target-create-race-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := nativeMigrationTestDeployment(name, churnlessControllerValue)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		setNativeMigrationComplete(ctx, source)

		target := takeoverMigrationTarget(source)
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		targetUID := target.UID

		result, err := engine.startTakeover(ctx, source)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeZero())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.UID).To(Equal(targetUID))

		progress, _, err := engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())
		Expect(progress.Mode).To(Equal(migrationModePreserve))
	})

	It("prepares a source ReplicaSet created during cutover validation", func() {
		const name = "source-replicaset-race-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := nativeMigrationTestDeployment(name, churnlessControllerValue)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		setNativeMigrationComplete(ctx, source)
		target := takeoverMigrationTarget(source)
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		createCompleteChurnlessReplicaSet(ctx, target)

		liveClient, err := client.NewWithWatch(
			cfg,
			client.Options{Scheme: k8sClient.Scheme()},
		)
		Expect(err).NotTo(HaveOccurred())
		getCalls := 0
		var lateReplicaSet *appsv1.ReplicaSet
		racingClient := interceptor.NewClient(liveClient, interceptor.Funcs{
			Get: func(
				ctx context.Context,
				base client.WithWatch,
				key client.ObjectKey,
				object client.Object,
				options ...client.GetOption,
			) error {
				if err := base.Get(ctx, key, object, options...); err != nil {
					return err
				}
				getCalls++
				// The fourth Get completes the second, cutover-validation pair read.
				if getCalls != 4 {
					return nil
				}
				replicas := desiredReplicas(source.Spec.Replicas)
				lateReplicaSet = &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name + "-late",
						Namespace: namespace,
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: &replicas,
						Selector: source.Spec.Selector.DeepCopy(),
						Template: *source.Spec.Template.DeepCopy(),
					},
				}
				if err := controllerutil.SetControllerReference(
					source,
					lateReplicaSet,
					base.Scheme(),
				); err != nil {
					return err
				}
				if err := base.Create(ctx, lateReplicaSet); err != nil {
					return err
				}
				return nil
			},
		})
		racingEngine, err := NewDeploymentMigrationEngine(
			racingClient,
			racingClient,
			k8sClient.Scheme(),
		)
		Expect(err).NotTo(HaveOccurred())

		_, _, err = racingEngine.AdvanceMigration(
			ctx,
			key,
			MigrationDestinationChurnless,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		Expect(k8sClient.Get(
			ctx,
			client.ObjectKeyFromObject(lateReplicaSet),
			lateReplicaSet,
		)).To(Succeed())
		Expect(lateReplicaSet.Annotations[migrationIDAnnotation]).
			To(Equal(string(source.UID)))
	})

	DescribeTable(
		"does not mutate terminal progress",
		func(name string, destination MigrationDestination, active, complete, superseded bool) {
			key := types.NamespacedName{Name: name, Namespace: namespace}
			source := nativeMigrationTestDeployment(name, nativeControllerValue)
			Expect(k8sClient.Create(ctx, source)).To(Succeed())
			var observed client.Object = source
			if active {
				observed = takeoverMigrationTarget(source)
				Expect(k8sClient.Create(ctx, observed)).To(Succeed())
			}
			resourceVersion := observed.GetResourceVersion()

			progress, delay, err := engine.AdvanceMigration(ctx, key, destination)
			Expect(err).NotTo(HaveOccurred())
			Expect(progress.Complete).To(Equal(complete))
			Expect(progress.Superseded).To(Equal(superseded))
			Expect(delay).To(BeZero())
			Expect(k8sClient.Get(ctx, key, observed)).To(Succeed())
			Expect(observed.GetResourceVersion()).To(Equal(resourceVersion))
			Expect(observed.GetDeletionTimestamp().IsZero()).To(BeTrue())
		},
		Entry("completed", "completed-advance-test", MigrationDestinationNative, false, true, false),
		Entry(
			"superseded",
			"superseded-advance-test",
			MigrationDestinationChurnless,
			true,
			false,
			true,
		),
	)

	It("creates a native target for a complete Churnless Deployment", func() {
		const name = "handoff-test"
		source := churnlessMigrationTestDeployment(name, nativeControllerValue)
		source.Labels = map[string]string{"example.com/workload": "handoff"}
		source.Spec.Paused = true
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: namespace}
		createCompleteChurnlessReplicaSet(ctx, source)

		_, _, err := engine.AdvanceMigration(ctx, key, MigrationDestinationNative)
		Expect(err).NotTo(HaveOccurred())

		var target appsv1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(Succeed())
		Expect(target.Labels).To(Equal(source.Labels))
		Expect(target.Annotations[controllerAnnotation]).To(Equal(nativeControllerValue))
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModePreserve))
		Expect(target.Annotations[migrationStateVersionAnnotation]).To(Equal(migrationStateVersion))
		Expect(target.Annotations[migrationIDAnnotation]).To(Equal(string(source.UID)))
		Expect(target.Annotations[migrationOriginalPausedAnnotation]).To(Equal("true"))
		Expect(target.Spec.Paused).To(BeFalse())
		Expect(target.Spec.Selector).To(Equal(source.Spec.Selector))
		Expect(target.Spec.Template.Labels).To(Equal(source.Spec.Template.Labels))
		Expect(target.Spec.Template.Spec.Containers[0].Image).
			To(Equal(source.Spec.Template.Spec.Containers[0].Image))
	})

	It("records an imperative handoff request on the Churnless source", func() {
		const name = "driver-request-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(
			name,
			churnlessControllerValue,
		)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())

		Expect(engine.RequestMigration(
			ctx,
			key,
			MigrationDestinationNative,
		)).To(Succeed())

		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		Expect(source.Annotations[controllerAnnotation]).To(Equal(nativeControllerValue))
	})

	It("re-resolves the request target when the source disappears before patch", func() {
		const name = "request-delete-race-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(name, nativeControllerValue)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		target := handoffMigrationTarget(source)
		Expect(k8sClient.Create(ctx, target)).To(Succeed())

		racingClient := &deleteBeforePatchClient{
			Client:       k8sClient,
			deleteObject: source.DeepCopy(),
		}
		racingEngine, err := NewDeploymentMigrationEngine(
			racingClient,
			racingClient,
			k8sClient.Scheme(),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(racingEngine.RequestMigration(
			ctx,
			key,
			MigrationDestinationChurnless,
		)).To(Succeed())
		Expect(racingClient.deleted).To(BeTrue())
		Expect(k8sClient.Get(ctx, key, source)).To(Satisfy(apierrors.IsNotFound))
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
	})

	DescribeTable(
		"fails closed on malformed durable migration state",
		func(mutate func(map[string]string), expectedError string) {
			const name = "invalid-state-test"
			key := types.NamespacedName{Name: name, Namespace: namespace}
			source := nativeMigrationTestDeployment(
				name,
				churnlessControllerValue,
			)
			Expect(k8sClient.Create(ctx, source)).To(Succeed())
			target := takeoverMigrationTarget(source)
			mutate(target.Annotations)
			Expect(k8sClient.Create(ctx, target)).To(Succeed())

			_, _, err := engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
			Expect(err).To(MatchError(ContainSubstring(expectedError)))
			Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
			Expect(source.DeletionTimestamp.IsZero()).To(BeTrue())
		},
		Entry(
			"with an unknown state version",
			func(annotations map[string]string) {
				annotations[migrationStateVersionAnnotation] = invalidMigrationState
			},
			"unsupported migration state version",
		),
		Entry(
			"with an unknown mode",
			func(annotations map[string]string) {
				annotations[migrationModeAnnotation] = invalidMigrationState
			},
			"unsupported migration mode",
		),
		Entry(
			"with an unknown desired controller",
			func(annotations map[string]string) {
				annotations[controllerAnnotation] = invalidMigrationState
			},
			"unsupported desired controller",
		),
		Entry(
			"with recovery mode during takeover",
			func(annotations map[string]string) {
				annotations[migrationModeAnnotation] = migrationModeRecovery
			},
			"recovery mode is not supported for takeover",
		),
	)

	It("starts recovery handoff before an unhealthy Churnless rollout completes", func() {
		const name = "recovery-test"
		source := churnlessMigrationTestDeployment(name, nativeControllerValue)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: namespace}

		_, _, err := engine.AdvanceMigration(ctx, key, MigrationDestinationNative)
		Expect(err).NotTo(HaveOccurred())

		var target appsv1.Deployment
		Expect(k8sClient.Get(ctx, key, &target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(nativeControllerValue))
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModeRecovery))
		Expect(target.Spec.Template.Labels).To(Equal(source.Spec.Template.Labels))
		Expect(target.Spec.Template.Spec.Containers[0].Image).
			To(Equal(source.Spec.Template.Spec.Containers[0].Image))
	})

	DescribeTable(
		"rejects a deleting source",
		func(source, target client.Object, expectedError string) {
			key := client.ObjectKeyFromObject(source)
			source.SetFinalizers([]string{testDeletionFinalizer})
			Expect(k8sClient.Create(ctx, source)).To(Succeed())
			clearFinalizersAfterTest(ctx, source)
			Expect(k8sClient.Delete(ctx, source)).To(Succeed())
			Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
			Expect(source.GetDeletionTimestamp().IsZero()).To(BeFalse())

			destination := MigrationDestinationChurnless
			if _, handoff := source.(*appsv1alpha1.Deployment); handoff {
				destination = MigrationDestinationNative
			}
			_, _, err := engine.AdvanceMigration(ctx, key, destination)
			Expect(err).To(MatchError(ContainSubstring(expectedError)))
			Expect(k8sClient.Get(ctx, key, target)).To(Satisfy(apierrors.IsNotFound))
		},
		Entry(
			"during takeover",
			nativeMigrationTestDeployment("deleting-source-test", churnlessControllerValue),
			&appsv1alpha1.Deployment{ObjectMeta: metav1.ObjectMeta{
				Name: "deleting-source-test", Namespace: namespace,
			}},
			"cannot take over native Deployment default/deleting-source-test because it is deleting",
		),
		Entry(
			"during handoff",
			churnlessMigrationTestDeployment("deleting-handoff-source-test", nativeControllerValue),
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
				Name: "deleting-handoff-source-test", Namespace: namespace,
			}},
			"cannot hand off Churnless Deployment default/deleting-handoff-source-test because it is deleting",
		),
	)

	It("does not cut over to a deleting migration target", func() {
		const name = "deleting-target-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := nativeMigrationTestDeployment(
			name,
			churnlessControllerValue,
		)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		target := takeoverMigrationTarget(source)
		target.Finalizers = []string{testDeletionFinalizer}
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		clearFinalizersAfterTest(ctx, target)
		Expect(k8sClient.Delete(ctx, target)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.DeletionTimestamp.IsZero()).To(BeFalse())

		_, result, err := engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeZero())
		Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
		Expect(source.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(source.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
	})

	DescribeTable(
		"cancels before cutover and restores dependents",
		func(name string, takeover bool) {
			key := types.NamespacedName{Name: name, Namespace: namespace}
			var source, target client.Object
			var restoredController, initialAPI, restoredAPI string
			if takeover {
				native := nativeMigrationTestDeployment(name, nativeControllerValue)
				Expect(k8sClient.Create(ctx, native)).To(Succeed())
				source = native
				target = takeoverMigrationTarget(native)
				restoredController = nativeControllerValue
				initialAPI = appsv1alpha1.GroupVersion.String()
				restoredAPI = appsv1.SchemeGroupVersion.String()
			} else {
				churnless := churnlessMigrationTestDeployment(name, churnlessControllerValue)
				Expect(k8sClient.Create(ctx, churnless)).To(Succeed())
				source = churnless
				target = handoffMigrationTarget(churnless)
				restoredController = churnlessControllerValue
				initialAPI = appsv1.SchemeGroupVersion.String()
				restoredAPI = appsv1alpha1.GroupVersion.String()
			}
			Expect(k8sClient.Create(ctx, target)).To(Succeed())
			hpa := migrationTestHPA(name, initialAPI)
			Expect(k8sClient.Create(ctx, hpa)).To(Succeed())

			Eventually(func(g Gomega) {
				_, _, err := engine.AdvanceMigration(
					ctx, key, MigrationDestination(restoredController),
				)
				g.Expect(err).NotTo(HaveOccurred())
				err = k8sClient.Get(ctx, key, target)
				g.Expect(err == nil || apierrors.IsNotFound(err)).To(BeTrue())
				if err == nil {
					g.Expect(target.GetDeletionTimestamp().IsZero()).To(BeFalse())
				}
				g.Expect(k8sClient.Get(ctx, key, source)).To(Succeed())
				g.Expect(source.GetAnnotations()[controllerAnnotation]).
					To(Equal(restoredController))
				g.Expect(k8sClient.Get(ctx, key, hpa)).To(Succeed())
				g.Expect(hpa.Spec.ScaleTargetRef.APIVersion).To(Equal(restoredAPI))
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		},
		Entry("takeover", "cancel-test", true),
		Entry("handoff", "cancel-handoff-test", false),
	)

	It("switches a preserve handoff to recovery if the source degrades", func() {
		const name = "degrade-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(
			name,
			nativeControllerValue,
		)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		target := handoffMigrationTarget(source)
		Expect(k8sClient.Create(ctx, target)).To(Succeed())

		_, _, err := engine.AdvanceMigration(ctx, key, MigrationDestinationNative)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[migrationModeAnnotation]).To(Equal(migrationModeRecovery))
	})

	It("honors a Churnless return requested after handoff source deletion starts", func() {
		const name = "late-return-test"
		key := types.NamespacedName{Name: name, Namespace: namespace}
		source := churnlessMigrationTestDeployment(
			name,
			nativeControllerValue,
		)
		source.Finalizers = []string{testDeletionFinalizer}
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		clearFinalizersAfterTest(ctx, source)
		target := handoffMigrationTarget(source)
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
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

		_, result, err := engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeZero())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))

		source.Finalizers = nil
		Expect(k8sClient.Update(ctx, source)).To(Succeed())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, key, source)
			return apierrors.IsNotFound(err)
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

		_, result, err = engine.AdvanceMigration(ctx, key, MigrationDestinationChurnless)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeZero())
		Expect(k8sClient.Get(ctx, key, target)).To(Succeed())
		Expect(target.Annotations[controllerAnnotation]).To(Equal(churnlessControllerValue))
		Expect(target.Annotations).NotTo(HaveKey(migrationStateVersionAnnotation))
	})

	It("marks source Pods for adoption and restores their template labels", func() {
		const name = "takeover-test"
		pod := migrationTestPod(name, map[string]string{corev1.PodDeletionCost: "7"})
		pod.Labels[structuralRevisionLabel] = "original"
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		changed, err := engine.preparePod(
			ctx,
			pod,
			migrationTestID,
			migrationRoleSource,
			sourceDeletionCost,
			map[string]string{structuralRevisionLabel: "revision"},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Labels[structuralRevisionLabel]).To(Equal("revision"))
		Expect(pod.Annotations[migrationIDAnnotation]).To(Equal(migrationTestID))
		Expect(pod.Annotations[migrationRoleAnnotation]).To(Equal(migrationRoleSource))
		Expect(pod.Annotations[corev1.PodDeletionCost]).To(Equal(sourceDeletionCost))

		sourceTemplate := podTemplate(name, image)
		sourceTemplate.Labels[structuralRevisionLabel] = "original"
		Expect(engine.restoreCancelledMigrationPod(
			ctx,
			pod,
			&sourceTemplate,
			structuralRevisionLabel,
		)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.Labels[structuralRevisionLabel]).To(Equal("original"))
		Expect(pod.Annotations).NotTo(HaveKey(migrationIDAnnotation))
		Expect(pod.Annotations).NotTo(HaveKey(migrationRoleAnnotation))
		Expect(pod.Annotations[corev1.PodDeletionCost]).To(Equal("7"))
		Expect(pod.Annotations).NotTo(HaveKey(migrationOriginalDeletionCost))
	})

	DescribeTable(
		"restores the exact Pod deletion-cost annotation state",
		func(original map[string]string, present bool, value string) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					corev1.PodDeletionCost: sourceDeletionCost,
					migrationOriginalDeletionCost: migrationDeletionCostSnapshot(
						original,
					),
				},
			}}
			Expect(restoreMigrationDeletionCost(pod)).To(Succeed())

			restored, restoredPresent := pod.Annotations[corev1.PodDeletionCost]
			Expect(restoredPresent).To(Equal(present))
			Expect(restored).To(Equal(value))
			Expect(pod.Annotations).NotTo(HaveKey(migrationOriginalDeletionCost))
		},
		Entry("when absent", nil, false, ""),
		Entry(
			"when present with an empty value",
			map[string]string{corev1.PodDeletionCost: ""},
			true,
			"",
		),
	)

	It("rejects cleanup without a deletion-cost snapshot", func() {
		Expect(restoreMigrationDeletionCost(&corev1.Pod{})).
			To(MatchError("original value is missing"))
	})

	DescribeTable(
		"fails closed when a prepared Pod cannot restore its deletion cost",
		func(name string, annotations map[string]string, message string) {
			want := maps.Clone(annotations)
			pod := migrationTestPod(name, annotations)
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			changed, err := engine.preparePod(
				ctx,
				pod,
				migrationTestID,
				migrationRoleSource,
				sourceDeletionCost,
				map[string]string{structuralRevisionLabel: "revision"},
			)
			Expect(changed).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring(message)))

			var persisted corev1.Pod
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &persisted)).
				To(Succeed())
			Expect(persisted.Labels).NotTo(HaveKey(structuralRevisionLabel))
			Expect(persisted.Annotations).To(Equal(want))
		},
		Entry(
			"when the snapshot is missing",
			"missing-deletion-cost-snapshot",
			map[string]string{
				migrationIDAnnotation:  migrationTestID,
				corev1.PodDeletionCost: sourceDeletionCost,
			},
			"original deletion cost is missing",
		),
		Entry(
			"when the snapshot is invalid",
			"invalid-deletion-cost-snapshot",
			map[string]string{
				migrationIDAnnotation:         migrationTestID,
				migrationOriginalDeletionCost: "invalid",
				corev1.PodDeletionCost:        sourceDeletionCost,
			},
			`snapshot "invalid" is invalid`,
		),
	)

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
		pod := migrationTestPod(name, map[string]string{
			migrationIDAnnotation:   migrationTestID,
			migrationRoleAnnotation: migrationRoleSource,
		})
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		staleTarget := replicaSet.DeepCopy()
		staleTarget.UID = types.UID("stale-target")
		changed, err := engine.adoptMigrationPods(
			ctx,
			namespace,
			migrationTestID,
			nativePodAdoptionTargets([]*appsv1.ReplicaSet{staleTarget}),
		)
		Expect(changed).To(BeFalse())
		Expect(err).To(MatchError(ContainSubstring("changed or is deleting")))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(metav1.GetControllerOf(pod)).To(BeNil())

		changed, err = engine.adoptMigrationPods(
			ctx,
			namespace,
			migrationTestID,
			nativePodAdoptionTargets([]*appsv1.ReplicaSet{replicaSet}),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(metav1.IsControlledBy(pod, replicaSet)).To(BeTrue())
	})
})

type deleteBeforePatchClient struct {
	client.Client
	deleteObject client.Object
	deleted      bool
}

func (c *deleteBeforePatchClient) Patch(
	ctx context.Context,
	object client.Object,
	patch client.Patch,
	options ...client.PatchOption,
) error {
	if !c.deleted {
		c.deleted = true
		if err := c.Delete(ctx, c.deleteObject); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

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

func migrationTestPod(name string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   migrationTestNamespace,
			Labels:      map[string]string{appLabel: name},
			Annotations: maps.Clone(annotations),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: migrationTestContainer, Image: migrationTestImage}},
		},
	}
}

func nativeMigrationTestDeployment(
	name, controller string,
) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   migrationTestNamespace,
			Annotations: map[string]string{controllerAnnotation: controller},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
			Template: podTemplate(name, migrationTestImage),
		},
	}
}

func churnlessMigrationTestDeployment(
	name, controller string,
) *appsv1alpha1.Deployment {
	native := nativeMigrationTestDeployment(name, controller)
	return &appsv1alpha1.Deployment{
		ObjectMeta: *native.ObjectMeta.DeepCopy(),
		Spec: appsv1alpha1.DeploymentSpec{
			DeploymentSpec: *native.Spec.DeepCopy(),
		},
	}
}

func takeoverMigrationTarget(source *appsv1.Deployment) *appsv1alpha1.Deployment {
	return &appsv1alpha1.Deployment{
		ObjectMeta: migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			migrationModePreserve,
		),
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: *source.Spec.DeepCopy()},
	}
}

func handoffMigrationTarget(source *appsv1alpha1.Deployment) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: migrationTargetMetadata(
			source.ObjectMeta,
			source.UID,
			source.Generation,
			source.Spec.Paused,
			migrationModePreserve,
		),
		Spec: *source.Spec.DeploymentSpec.DeepCopy(),
	}
}

func migrationTestHPA(name, apiVersion string) *autoscalingv2.HorizontalPodAutoscaler {
	replicas := int32(1)
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: migrationTestNamespace},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: apiVersion,
				Kind:       deploymentKind,
				Name:       name,
			},
			MinReplicas: &replicas,
			MaxReplicas: 2,
		},
	}
}

func setNativeMigrationComplete(ctx context.Context, source *appsv1.Deployment) {
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source)).To(Succeed())
	replicas := desiredReplicas(source.Spec.Replicas)
	source.Status = appsv1.DeploymentStatus{
		ObservedGeneration: source.Generation,
		Replicas:           replicas,
		UpdatedReplicas:    replicas,
		ReadyReplicas:      replicas,
		AvailableReplicas:  replicas,
	}
	Expect(k8sClient.Status().Update(ctx, source)).To(Succeed())
}

func createCompleteChurnlessReplicaSet(
	ctx context.Context,
	source *appsv1alpha1.Deployment,
) {
	replicas := desiredReplicas(source.Spec.Replicas)
	revision := structuralRevision(source)
	template := source.Spec.Template.DeepCopy()
	template.Labels[structuralRevisionLabel] = revision
	selector := source.Spec.Selector.DeepCopy()
	selector.MatchLabels[structuralRevisionLabel] = revision
	replicaSet := &appsv1alpha1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaSetName(source.Name, revision),
			Namespace: source.Namespace,
			Labels:    map[string]string{structuralRevisionLabel: revision},
		},
		Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: selector,
			Template: *template,
		}},
	}
	Expect(controllerutil.SetControllerReference(source, replicaSet, k8sClient.Scheme())).
		To(Succeed())
	Expect(k8sClient.Create(ctx, replicaSet)).To(Succeed())
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
}
