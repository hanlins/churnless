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
	"errors"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/kubecompat"
)

const (
	rolloutTestNamespace = "default"
	replicaSetKind       = "ReplicaSet"
)

// These cases are derived from Kubernetes v1.36.0:
//
//	pkg/controller/deployment/rolling_test.go
//
// Keep the scenarios recognizable so a Kubernetes dependency bump can diff
// Churnless policy decisions against the upstream controller tests.
func TestRollingUpdatePolicyMatchesUpstreamCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		desired              int32
		maxSurge             intstr.IntOrString
		maxUnavailable       intstr.IntOrString
		oldReplicas          int32
		oldAvailable         int32
		currentReplicas      int32
		currentAvailable     int32
		expectedOldReplicas  int32
		expectedNewReplicas  int32
		expectedPolicyChange bool
	}{
		{
			name:                 "zero resolved fenceposts still make progress",
			desired:              10,
			maxSurge:             intstr.FromInt32(0),
			maxUnavailable:       intstr.FromInt32(0),
			oldReplicas:          10,
			oldAvailable:         10,
			expectedOldReplicas:  9,
			expectedNewReplicas:  0,
			expectedPolicyChange: true,
		},
		{
			name:                 "scale new ReplicaSet only to max surge",
			desired:              10,
			maxSurge:             intstr.FromInt32(2),
			maxUnavailable:       intstr.FromInt32(0),
			oldReplicas:          10,
			oldAvailable:         10,
			expectedOldReplicas:  10,
			expectedNewReplicas:  2,
			expectedPolicyChange: true,
		},
		{
			name:                 "fill unused capacity before using surge",
			desired:              10,
			maxSurge:             intstr.FromInt32(2),
			maxUnavailable:       intstr.FromInt32(0),
			oldReplicas:          5,
			oldAvailable:         5,
			expectedOldReplicas:  5,
			expectedNewReplicas:  7,
			expectedPolicyChange: true,
		},
		{
			name:                 "stop scaling at desired plus surge",
			desired:              10,
			maxSurge:             intstr.FromInt32(2),
			maxUnavailable:       intstr.FromInt32(0),
			oldReplicas:          10,
			oldAvailable:         10,
			currentReplicas:      2,
			expectedOldReplicas:  10,
			expectedNewReplicas:  2,
			expectedPolicyChange: false,
		},
		{
			name:                 "scale down available old replicas to unavailable budget",
			desired:              10,
			maxSurge:             intstr.FromInt32(0),
			maxUnavailable:       intstr.FromInt32(2),
			oldReplicas:          10,
			oldAvailable:         10,
			expectedOldReplicas:  8,
			expectedNewReplicas:  0,
			expectedPolicyChange: true,
		},
		{
			name:                 "remove unhealthy old replicas first",
			desired:              10,
			maxSurge:             intstr.FromInt32(0),
			maxUnavailable:       intstr.FromInt32(2),
			oldReplicas:          10,
			oldAvailable:         8,
			expectedOldReplicas:  8,
			expectedNewReplicas:  0,
			expectedPolicyChange: true,
		},
		{
			name:                 "do not reduce availability for unavailable new replicas",
			desired:              10,
			maxSurge:             intstr.FromInt32(0),
			maxUnavailable:       intstr.FromInt32(2),
			oldReplicas:          8,
			oldAvailable:         8,
			currentReplicas:      2,
			currentAvailable:     0,
			expectedOldReplicas:  8,
			expectedNewReplicas:  2,
			expectedPolicyChange: false,
		},
		{
			name:                 "fill free surge before considering stale availability",
			desired:              4,
			maxSurge:             intstr.FromInt32(1),
			maxUnavailable:       intstr.FromInt32(1),
			oldReplicas:          3,
			oldAvailable:         4,
			currentReplicas:      1,
			currentAvailable:     0,
			expectedOldReplicas:  3,
			expectedNewReplicas:  2,
			expectedPolicyChange: true,
		},
		{
			name:                 "do not reuse stale availability after scale down",
			desired:              4,
			maxSurge:             intstr.FromInt32(1),
			maxUnavailable:       intstr.FromInt32(1),
			oldReplicas:          3,
			oldAvailable:         4,
			currentReplicas:      2,
			currentAvailable:     0,
			expectedOldReplicas:  3,
			expectedNewReplicas:  2,
			expectedPolicyChange: false,
		},
		{
			name:                 "large rollout uses percentage surge without overflow",
			desired:              50_000,
			maxSurge:             intstr.FromString("25%"),
			maxUnavailable:       intstr.FromString("25%"),
			oldReplicas:          50_000,
			oldAvailable:         50_000,
			expectedOldReplicas:  50_000,
			expectedNewReplicas:  12_500,
			expectedPolicyChange: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			scheme := newRolloutTestScheme(t)
			oldReplicaSet := rolloutReplicaSet("old", "old", test.oldReplicas, test.oldAvailable)
			currentReplicaSet := rolloutReplicaSet(
				"current",
				"current",
				test.currentReplicas,
				test.currentAvailable,
			)
			workload := rolloutDeployment(
				test.desired,
				appsv1.RollingUpdateDeploymentStrategyType,
				test.maxSurge,
				test.maxUnavailable,
			)
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(oldReplicaSet.DeepCopy(), currentReplicaSet.DeepCopy()).
				Build()
			reconciler := &DeploymentReconciler{Client: k8sClient, Scheme: scheme}

			changed, err := reconciler.rollout(
				context.Background(),
				workload,
				currentReplicaSet,
				[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
			)
			if err != nil {
				t.Fatal(err)
			}
			if changed != test.expectedPolicyChange {
				t.Fatalf("changed = %t, want %t", changed, test.expectedPolicyChange)
			}

			assertReplicaCount(t, k8sClient, oldReplicaSet, test.expectedOldReplicas)
			assertReplicaCount(t, k8sClient, currentReplicaSet, test.expectedNewReplicas)
		})
	}
}

// This is the controller-level form of the Kubernetes Recreate conformance
// invariant in test/e2e/apps/deployment.go: new replicas must remain at zero
// until every old ReplicaSet has reached zero.
func TestRecreatePolicyDoesNotOverlapRevisions(t *testing.T) {
	t.Parallel()

	scheme := newRolloutTestScheme(t)
	zero := intstr.FromInt32(0)
	workload := rolloutDeployment(
		3,
		appsv1.RecreateDeploymentStrategyType,
		zero,
		zero,
	)
	oldReplicaSet := rolloutReplicaSet("old", "old", 3, 3)
	currentReplicaSet := rolloutReplicaSet("current", "current", 0, 0)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(oldReplicaSet.DeepCopy(), currentReplicaSet.DeepCopy()).
		Build()
	reconciler := &DeploymentReconciler{Client: k8sClient, Scheme: scheme}

	changed, err := reconciler.rollout(
		context.Background(),
		workload,
		currentReplicaSet,
		[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("old ReplicaSet was not scaled down")
	}
	assertReplicaCount(t, k8sClient, oldReplicaSet, 0)
	assertReplicaCount(t, k8sClient, currentReplicaSet, 0)

	changed, err = reconciler.rollout(
		context.Background(),
		workload,
		currentReplicaSet,
		[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
	)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("current ReplicaSet scaled up while old Pods were still observed")
	}
	assertReplicaCount(t, k8sClient, currentReplicaSet, 0)

	oldReplicaSet.Status.Replicas = 0
	terminating := int32(2)
	oldReplicaSet.Status.TerminatingReplicas = &terminating
	changed, err = reconciler.rollout(
		context.Background(),
		workload,
		currentReplicaSet,
		[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
	)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("current ReplicaSet scaled up while old Pods were terminating")
	}
	assertReplicaCount(t, k8sClient, currentReplicaSet, 0)

	oldReplicaSet.Status.TerminatingReplicas = nil
	runningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "old-running",
			Namespace: oldReplicaSet.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
				oldReplicaSet,
				appsv1alpha1.GroupVersion.WithKind(replicaSetKind),
			)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if err := k8sClient.Create(context.Background(), runningPod); err != nil {
		t.Fatal(err)
	}
	changed, err = reconciler.rollout(
		context.Background(),
		workload,
		currentReplicaSet,
		[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
	)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("current ReplicaSet scaled up while a live old Pod was still observable")
	}
	assertReplicaCount(t, k8sClient, currentReplicaSet, 0)

	if err := k8sClient.Delete(context.Background(), runningPod); err != nil {
		t.Fatal(err)
	}
	changed, err = reconciler.rollout(
		context.Background(),
		workload,
		currentReplicaSet,
		[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("current ReplicaSet was not scaled up after old Pods terminated")
	}
	assertReplicaCount(t, k8sClient, currentReplicaSet, 3)
}

func TestDeploymentParallelismDoesNotReuseStaleAvailability(t *testing.T) {
	t.Parallel()

	maxSurge := intstr.FromInt32(1)
	maxUnavailable := intstr.FromInt32(1)
	workload := rolloutDeployment(
		4,
		appsv1.RollingUpdateDeploymentStrategyType,
		maxSurge,
		maxUnavailable,
	)
	oldReplicaSet := rolloutReplicaSet("old", "old", 0, 4)
	currentReplicaSet := rolloutReplicaSet("current", "current", 4, 0)

	parallelism, err := deploymentParallelism(
		workload,
		[]*appsv1alpha1.ReplicaSet{oldReplicaSet, currentReplicaSet},
	)
	if err != nil {
		t.Fatal(err)
	}
	if parallelism != 0 {
		t.Fatalf("parallelism = %d, want 0 while current replicas are unavailable", parallelism)
	}
}

// Kubernetes caps one ReplicaSet sync at 500 creates/deletes and slow-starts
// creation to avoid flooding the API server when quota or admission rejects a
// large workload. These cases exercise that behavior through the Churnless
// reconciler, not only through the compatibility helper.
//
// Upstream sources:
//
//	pkg/controller/replicaset/replica_set.go
//	pkg/controller/replicaset/replica_set_test.go (doTestControllerBurstReplicas)
func TestReplicaSetLargeScaleBurstAndSlowStart(t *testing.T) {
	t.Parallel()

	t.Run("caps a ten-thousand Pod deficit", func(t *testing.T) {
		t.Parallel()

		workload, reconciler, counter := largeReplicaSetReconciler(t, 10_000, nil)
		changed, err := reconciler.reconcileReplicas(context.Background(), workload, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			t.Fatal("large ReplicaSet deficit did not report a change")
		}
		if got := counter.creates.Load(); got != kubecompat.BurstReplicas {
			t.Fatalf("Pod creates = %d, want burst limit %d", got, kubecompat.BurstReplicas)
		}
	})

	t.Run("stops after the first failed slow-start batch", func(t *testing.T) {
		t.Parallel()

		quotaError := errors.New("quota exhausted")
		workload, reconciler, counter := largeReplicaSetReconciler(t, 10_000, quotaError)
		changed, err := reconciler.reconcileReplicas(context.Background(), workload, nil)
		if !errors.Is(err, quotaError) {
			t.Fatalf("error = %v, want %v", err, quotaError)
		}
		if changed {
			t.Fatal("failed first batch incorrectly reported successful creation")
		}
		if got := counter.creates.Load(); got != kubecompat.SlowStartInitialBatchSize {
			t.Fatalf(
				"Pod create attempts = %d, want initial batch %d",
				got,
				kubecompat.SlowStartInitialBatchSize,
			)
		}
	})
}

type countingClient struct {
	client.Client
	creates   atomic.Int32
	createErr error
}

func (c *countingClient) Create(
	_ context.Context,
	_ client.Object,
	_ ...client.CreateOption,
) error {
	c.creates.Add(1)
	return c.createErr
}

func largeReplicaSetReconciler(
	t *testing.T,
	replicas int32,
	createErr error,
) (*appsv1alpha1.ReplicaSet, *ReplicaSetReconciler, *countingClient) {
	t.Helper()

	scheme := newRolloutTestScheme(t)
	workload := &appsv1alpha1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1alpha1.GroupVersion.String(),
			Kind:       replicaSetKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "large",
			Namespace: rolloutTestNamespace,
			UID:       types.UID("large"),
		},
		Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{appLabel: "large"},
			},
			Template: podTemplate("large", "registry.k8s.io/pause:3.10"),
		}},
	}
	counter := &countingClient{
		Client:    fake.NewClientBuilder().WithScheme(scheme).Build(),
		createErr: createErr,
	}
	return workload, &ReplicaSetReconciler{Client: counter, Scheme: scheme}, counter
}

func rolloutDeployment(
	replicas int32,
	strategy appsv1.DeploymentStrategyType,
	maxSurge, maxUnavailable intstr.IntOrString,
) *appsv1alpha1.Deployment {
	return &appsv1alpha1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: rolloutTestNamespace},
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{},
			Strategy: appsv1.DeploymentStrategy{
				Type: strategy,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavailable,
				},
			},
		}},
	}
}

func rolloutReplicaSet(
	name string,
	uid types.UID,
	replicas, available int32,
) *appsv1alpha1.ReplicaSet {
	return &appsv1alpha1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1alpha1.GroupVersion.String(),
			Kind:       replicaSetKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rolloutTestNamespace,
			UID:       uid,
		},
		Spec: appsv1alpha1.ReplicaSetSpec{ReplicaSetSpec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
		}},
		Status: appsv1alpha1.ReplicaSetStatus{
			ReplicaSetStatus: appsv1.ReplicaSetStatus{
				Replicas:          replicas,
				AvailableReplicas: available,
			},
		},
	}
}

func newRolloutTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func assertReplicaCount(
	t *testing.T,
	k8sClient client.Client,
	replicaSet *appsv1alpha1.ReplicaSet,
	expected int32,
) {
	t.Helper()

	var current appsv1alpha1.ReplicaSet
	if err := k8sClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(replicaSet),
		&current,
	); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Replicas == nil || *current.Spec.Replicas != expected {
		t.Fatalf(
			"ReplicaSet %s replicas = %v, want %d",
			replicaSet.Name,
			current.Spec.Replicas,
			expected,
		)
	}
}
