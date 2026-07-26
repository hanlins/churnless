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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

var _ = Describe("StatefulSet Controller", func() {
	const (
		name      = "statefulset-test"
		namespace = "default"
		oldImage  = "registry.k8s.io/pause:3.9"
		newImage  = "registry.k8s.io/pause:3.10"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: name, Namespace: namespace}

	AfterEach(func() {
		deleteIfPresent(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
		deleteIfPresent(
			ctx,
			&appsv1alpha1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			},
		)
	})

	It("preserves StatefulSet compatibility and masks rolling image changes", func() {
		replicas := int32(2)
		partition := int32(1)
		workload := &appsv1alpha1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1alpha1.StatefulSetSpec{StatefulSetSpec: appsv1.StatefulSetSpec{
				Replicas:    &replicas,
				ServiceName: name,
				Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: name}},
				Template:    podTemplate(name, oldImage),
				UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
					Type: appsv1.RollingUpdateStatefulSetStrategyType,
					RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
						Partition: &partition,
					},
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
					ObjectMeta: metav1.ObjectMeta{Name: "data"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				}},
			}},
		}
		Expect(k8sClient.Create(ctx, workload)).To(Succeed())
		reconciler := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var shadow appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, key, &shadow)).To(Succeed())
		Expect(shadow.Spec.VolumeClaimTemplates).To(HaveLen(1))
		Expect(*shadow.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(partition))

		Expect(k8sClient.Get(ctx, key, workload)).To(Succeed())
		workload.Spec.Template.Spec.Containers[0].Image = newImage
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, &shadow)).To(Succeed())
		Expect(shadow.Spec.Template.Spec.Containers[0].Image).To(Equal(oldImage))
	})
})
