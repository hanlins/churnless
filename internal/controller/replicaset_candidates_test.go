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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

func TestClaimPodsUsesSelectorScopedLiveListAndOwnerIndex(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	workload := &appsv1alpha1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1alpha1.GroupVersion.String(),
			Kind:       replicaSetKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "indexed",
			Namespace: rolloutTestNamespace,
			UID:       types.UID("indexed"),
		},
	}
	controllerReference := *metav1.NewControllerRef(
		workload,
		appsv1alpha1.GroupVersion.WithKind(replicaSetKind),
	)
	matching := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:            "matching",
		Namespace:       workload.Namespace,
		Labels:          map[string]string{appLabel: workload.Name},
		OwnerReferences: []metav1.OwnerReference{controllerReference},
	}}
	noLongerMatching := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:            "no-longer-matching",
		Namespace:       workload.Namespace,
		Labels:          map[string]string{appLabel: "other"},
		OwnerReferences: []metav1.OwnerReference{controllerReference},
	}}
	unrelated := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "unrelated",
		Namespace: workload.Namespace,
		Labels:    map[string]string{appLabel: "other"},
	}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, podControllerUIDIndex, podControllerUID).
		WithObjects(matching, noLongerMatching, unrelated).
		Build()
	liveReader := &recordingReader{Reader: k8sClient}
	reconciler := &ReplicaSetReconciler{
		Client:          k8sClient,
		APIReader:       liveReader,
		Scheme:          scheme,
		podOwnerIndexed: true,
	}
	selector := labels.SelectorFromSet(map[string]string{appLabel: workload.Name})

	_, changed, err := reconciler.claimPods(context.Background(), workload, selector)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("controlled Pod that stopped matching was not released")
	}

	var released corev1.Pod
	if err := k8sClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(noLongerMatching),
		&released,
	); err != nil {
		t.Fatal(err)
	}
	if metav1.GetControllerOf(&released) != nil {
		t.Fatal("released Pod still has a controller owner")
	}
	if len(liveReader.lists) != 1 {
		t.Fatalf("live Pod lists = %d, want 1 selector-scoped list", len(liveReader.lists))
	}
	if got := liveReader.lists[0].LabelSelector.String(); got != selector.String() {
		t.Fatalf("live Pod selector = %q, want %q", got, selector)
	}
}

type recordingReader struct {
	client.Reader
	lists []client.ListOptions
}

func (r *recordingReader) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	options := (&client.ListOptions{}).ApplyOptions(opts)
	r.lists = append(r.lists, *options)
	return r.Reader.List(ctx, list, opts...)
}
