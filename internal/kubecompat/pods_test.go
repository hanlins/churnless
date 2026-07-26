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

package kubecompat

import (
	"errors"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestComparePodsForDeletion(t *testing.T) {
	t.Parallel()
	now := metav1.Now()
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	left := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "lower-cost",
			Annotations: map[string]string{corev1.PodDeletionCost: "-10"},
		},
		Spec:   corev1.PodSpec{NodeName: "node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{ready}},
	}
	right := left.DeepCopy()
	right.Name = "higher-cost"
	right.Annotations = map[string]string{corev1.PodDeletionCost: "10"}
	if got := ComparePodsForDeletion(&left, right, 1, 1, now); got >= 0 {
		t.Fatalf("lower deletion-cost comparison = %d, want negative", got)
	}

	left.Spec.NodeName = ""
	left.Annotations = nil
	right.Annotations = nil
	if got := ComparePodsForDeletion(&left, right, 1, 1, now); got >= 0 {
		t.Fatalf("unscheduled comparison = %d, want negative", got)
	}
}

func TestSlowStartBatch(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	successes, err := SlowStartBatch(7, 1, func() error {
		calls.Add(1)
		return nil
	})
	if err != nil || successes != 7 || calls.Load() != 7 {
		t.Fatalf("successes/calls/error = %d/%d/%v, want 7/7/nil", successes, calls.Load(), err)
	}

	calls.Store(0)
	successes, err = SlowStartBatch(7, 1, func() error {
		calls.Add(1)
		return errors.New("quota")
	})
	if err == nil || successes != 0 || calls.Load() != 1 {
		t.Fatalf("successes/calls/error = %d/%d/%v, want 0/1/error", successes, calls.Load(), err)
	}
}
