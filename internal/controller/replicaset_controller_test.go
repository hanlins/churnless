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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
		deleteIfPresent(ctx, &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
		deleteIfPresent(
			ctx,
			&appsv1alpha1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			},
		)
	})

	It("creates a native ReplicaSet and masks image-only changes", func() {
		replicas := int32(2)
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

		var shadow appsv1.ReplicaSet
		Expect(k8sClient.Get(ctx, key, &shadow)).To(Succeed())
		Expect(shadow.Spec.Template.Spec.Containers[0].Image).To(Equal(oldImage))

		Expect(k8sClient.Get(ctx, key, workload)).To(Succeed())
		workload.Spec.Template.Spec.Containers[0].Image = newImage
		Expect(k8sClient.Update(ctx, workload)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, &shadow)).To(Succeed())
		Expect(shadow.Spec.Template.Spec.Containers[0].Image).To(Equal(oldImage))
	})
})
