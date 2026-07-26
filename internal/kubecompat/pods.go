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
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	upstreamcontroller "k8s.io/kubernetes/pkg/controller"
)

const (
	// BurstReplicas matches the native ReplicaSet controller's create/delete
	// limit. Kubernetes does not export that controller-local constant.
	BurstReplicas = 500

	// SlowStartInitialBatchSize comes directly from the shared upstream
	// controller package.
	SlowStartInitialBatchSize = upstreamcontroller.SlowStartInitialBatchSize
)

// SlowStartBatch mirrors the native ReplicaSet controller's unexported batch
// helper. All exported Pod and ordering behavior below delegates upstream.
func SlowStartBatch(count, initialBatchSize int, fn func() error) (int, error) {
	remaining := count
	successes := 0
	for batchSize := min(remaining, initialBatchSize); batchSize > 0; batchSize = min(2*batchSize, remaining) {
		results := make(chan error, batchSize)
		var waitGroup sync.WaitGroup
		waitGroup.Add(batchSize)
		for range batchSize {
			go func() {
				defer waitGroup.Done()
				results <- fn()
			}()
		}
		waitGroup.Wait()
		close(results)
		var batchError error
		for err := range results {
			if err == nil {
				successes++
			} else if batchError == nil {
				batchError = err
			}
		}
		if batchError != nil {
			return successes, batchError
		}
		remaining -= batchSize
	}
	return successes, nil
}

// IsPodReady delegates to Kubernetes's Pod readiness helper.
func IsPodReady(pod *corev1.Pod) bool {
	return podutil.IsPodReady(pod)
}

// IsPodAvailable delegates to Kubernetes's minReadySeconds helper.
func IsPodAvailable(pod *corev1.Pod, minReadySeconds int32, now metav1.Time) bool {
	return podutil.IsPodAvailable(pod, minReadySeconds, now)
}

// ComparePodsForDeletion delegates to the native controller's
// ActivePodsWithRanks ordering. A negative result means left is deleted first.
func ComparePodsForDeletion(
	left, right *corev1.Pod,
	leftRank, rightRank int,
	now metav1.Time,
) int {
	order := upstreamcontroller.ActivePodsWithRanks{
		Pods: []*corev1.Pod{left, right},
		Rank: []int{leftRank, rightRank},
		Now:  now,
	}
	if order.Less(0, 1) {
		return -1
	}
	if order.Less(1, 0) {
		return 1
	}
	return 0
}
