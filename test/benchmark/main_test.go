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

package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestListPodsCountsTerminatingPodsForCleanup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:              "terminating",
		Namespace:         "default",
		Labels:            map[string]string{benchmarkLabel: "workload"},
		DeletionTimestamp: &now,
		Finalizers:        []string{"benchmark.test/hold"},
	}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod).
		Build()

	snapshot, err := listPods(
		context.Background(),
		k8sClient,
		pod.Namespace,
		"workload",
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.total != 1 {
		t.Fatalf("total Pods = %d, want terminating Pod counted", snapshot.total)
	}
	if snapshot.active != 0 {
		t.Fatalf("active Pods = %d, want terminating Pod excluded", snapshot.active)
	}
}
