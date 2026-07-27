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

// Command benchmark compares image-only rollouts for native and Churnless
// Deployments on the same Kubernetes cluster. It is an end-to-end benchmark:
// controller reconciliation, API writes, kubelet restarts, readiness, and
// garbage collection are all included.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/kubecompat"
)

const benchmarkLabel = "churnless.io/rollout-benchmark"

type options struct {
	context    string
	replicas   int
	iterations int
	oldImage   string
	newImage   string
	timeout    time.Duration
}

type workloadKind string

const (
	nativeKind    workloadKind = "native"
	churnlessKind workloadKind = "churnless"
)

type podSnapshot struct {
	total      int
	active     int
	ready      int
	desired    int
	identities map[string]string
}

type rolloutSample struct {
	duration    time.Duration
	retained    int
	maxActive   int
	minReady    int
	replicaSets int
}

func main() {
	opts := parseFlags()
	if err := run(opts); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "rollout benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.context, "context", "", "kubeconfig context (required)")
	flag.IntVar(&opts.replicas, "replicas", 100, "replicas in each workload")
	flag.IntVar(&opts.iterations, "iterations", 3, "image rollouts per workload")
	flag.StringVar(
		&opts.oldImage,
		"old-image",
		"registry.k8s.io/pause:3.9",
		"first preloaded workload image",
	)
	flag.StringVar(
		&opts.newImage,
		"new-image",
		"registry.k8s.io/pause:3.10",
		"second preloaded workload image",
	)
	flag.DurationVar(&opts.timeout, "timeout", 10*time.Minute, "timeout per readiness or rollout wait")
	flag.Parse()
	return opts
}

func run(opts options) error {
	switch {
	case opts.context == "":
		return errors.New("-context is required")
	case opts.replicas < 1:
		return errors.New("-replicas must be positive")
	case opts.iterations < 1:
		return errors.New("-iterations must be positive")
	case opts.oldImage == opts.newImage:
		return errors.New("-old-image and -new-image must differ")
	}

	restConfig, err := kubeConfig(opts.context)
	if err != nil {
		return err
	}
	restConfig.QPS = 100
	restConfig.Burst = 200
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"apps/v1":               appsv1.AddToScheme,
		"core/v1":               corev1.AddToScheme,
		"churnless.io/v1alpha1": appsv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return fmt.Errorf("add %s to scheme: %w", name, err)
		}
	}
	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}

	namespace := fmt.Sprintf("churnless-bench-%d", time.Now().Unix())
	ctx := context.Background()
	if err := k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}); err != nil {
		return fmt.Errorf("create benchmark Namespace: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := k8sClient.Delete(
			cleanupCtx,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}},
		); client.IgnoreNotFound(err) != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: delete benchmark Namespace: %v\n", err)
		}
	}()

	replicas := int32(opts.replicas)
	surgeValue := intstr.FromString("25%")
	unavailableValue := intstr.FromString("25%")
	maxSurge, maxUnavailable, err := kubecompat.ResolveFenceposts(
		&surgeValue,
		&unavailableValue,
		replicas,
	)
	if err != nil {
		return fmt.Errorf("resolve benchmark fenceposts: %w", err)
	}

	fmt.Printf(
		"Benchmarking native and Churnless rollouts with %d Pods in %s...\n",
		opts.replicas,
		namespace,
	)

	results := map[workloadKind][]rolloutSample{
		nativeKind:    make([]rolloutSample, 0, opts.iterations),
		churnlessKind: make([]rolloutSample, 0, opts.iterations),
	}
	fromImage, toImage := opts.oldImage, opts.newImage
	for iteration := 1; iteration <= opts.iterations; iteration++ {
		order := []workloadKind{churnlessKind, nativeKind}
		if iteration%2 == 0 {
			slices.Reverse(order)
		}
		for _, kind := range order {
			name := fmt.Sprintf("%s-%d", kind, iteration)
			if err := createWorkload(
				ctx,
				k8sClient,
				namespace,
				kind,
				name,
				replicas,
				fromImage,
				surgeValue,
				unavailableValue,
			); err != nil {
				return fmt.Errorf("create %s iteration %d: %w", kind, iteration, err)
			}
			sample, err := benchmarkRollout(
				ctx,
				k8sClient,
				namespace,
				kind,
				name,
				replicas,
				fromImage,
				toImage,
				replicas+maxSurge,
				replicas-maxUnavailable,
				opts.timeout,
			)
			if err != nil {
				return fmt.Errorf("%s iteration %d: %w", kind, iteration, err)
			}
			sample.replicaSets, err = countReplicaSets(
				ctx,
				k8sClient,
				namespace,
				kind,
				name,
			)
			if err != nil {
				return fmt.Errorf("count %s ReplicaSets: %w", kind, err)
			}
			results[kind] = append(results[kind], sample)
			fmt.Printf(
				"iteration=%d workload=%s duration=%s retained=%d/%d maxActive=%d minReady=%d replicaSets=%d\n",
				iteration,
				kind,
				sample.duration.Round(time.Millisecond),
				sample.retained,
				replicas,
				sample.maxActive,
				sample.minReady,
				sample.replicaSets,
			)
			if err := deleteWorkload(
				ctx,
				k8sClient,
				namespace,
				kind,
				name,
				opts.timeout,
			); err != nil {
				return fmt.Errorf("delete %s iteration %d: %w", kind, iteration, err)
			}
		}
		fromImage, toImage = toImage, fromImage
	}

	printSummary(
		opts,
		results[nativeKind],
		results[churnlessKind],
	)
	return nil
}

func benchmarkRollout(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	name string,
	replicas int32,
	fromImage, toImage string,
	maxActive, minReady int32,
	timeout time.Duration,
) (rolloutSample, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	before, err := waitForRollout(
		waitCtx,
		k8sClient,
		namespace,
		kind,
		name,
		replicas,
		fromImage,
		maxActive,
		0,
	)
	cancel()
	if err != nil {
		return rolloutSample{}, fmt.Errorf("wait for initial image: %w", err)
	}

	start := time.Now()
	if err := updateImage(ctx, k8sClient, namespace, kind, name, toImage); err != nil {
		return rolloutSample{}, err
	}
	waitCtx, cancel = context.WithTimeout(ctx, timeout)
	after, err := waitForRollout(
		waitCtx,
		k8sClient,
		namespace,
		kind,
		name,
		replicas,
		toImage,
		maxActive,
		minReady,
	)
	cancel()
	if err != nil {
		return rolloutSample{}, err
	}
	duration := time.Since(start)
	retained := retainedPods(before.identities, after.identities)
	switch kind {
	case churnlessKind:
		if retained != int(replicas) {
			return rolloutSample{}, fmt.Errorf(
				"retained %d/%d Pod identities, want all identities retained",
				retained,
				replicas,
			)
		}
	case nativeKind:
		if retained != 0 {
			return rolloutSample{}, fmt.Errorf(
				"retained %d/%d Pod identities, want native image rollout replacements",
				retained,
				replicas,
			)
		}
	}
	return rolloutSample{
		duration:  duration,
		retained:  retained,
		maxActive: after.active,
		minReady:  after.ready,
	}, nil
}

func waitForRollout(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	name string,
	replicas int32,
	image string,
	maxActive, minReady int32,
) (podSnapshot, error) {
	observedMaxActive := 0
	observedMinReady := int(replicas)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		snapshot, err := listPods(ctx, k8sClient, namespace, name, image)
		if err != nil {
			return podSnapshot{}, err
		}
		observedMaxActive = max(observedMaxActive, snapshot.active)
		observedMinReady = min(observedMinReady, snapshot.ready)
		if int32(snapshot.active) > maxActive {
			return podSnapshot{}, fmt.Errorf(
				"active Pods = %d, exceeds replicas + maxSurge = %d",
				snapshot.active,
				maxActive,
			)
		}
		if int32(snapshot.ready) < minReady {
			return podSnapshot{}, fmt.Errorf(
				"ready Pods = %d, below replicas - maxUnavailable = %d",
				snapshot.ready,
				minReady,
			)
		}
		complete, err := workloadComplete(
			ctx,
			k8sClient,
			namespace,
			kind,
			name,
			replicas,
		)
		if err != nil {
			return podSnapshot{}, err
		}
		if complete &&
			snapshot.active == int(replicas) &&
			snapshot.ready == int(replicas) &&
			snapshot.desired == int(replicas) {
			snapshot.active = observedMaxActive
			snapshot.ready = observedMinReady
			return snapshot, nil
		}

		select {
		case <-ctx.Done():
			return podSnapshot{}, fmt.Errorf(
				"wait for %s image %s: %w",
				kind,
				image,
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func listPods(
	ctx context.Context,
	k8sClient client.Client,
	namespace, workload, image string,
) (podSnapshot, error) {
	var pods corev1.PodList
	if err := k8sClient.List(
		ctx,
		&pods,
		client.InNamespace(namespace),
		client.MatchingLabels{benchmarkLabel: workload},
	); err != nil {
		return podSnapshot{}, fmt.Errorf("list Pods: %w", err)
	}
	snapshot := podSnapshot{identities: make(map[string]string, len(pods.Items))}
	for i := range pods.Items {
		pod := &pods.Items[i]
		snapshot.total++
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		snapshot.active++
		if isPodReady(pod) {
			snapshot.ready++
		}
		if len(pod.Spec.Containers) > 0 && pod.Spec.Containers[0].Image == image {
			snapshot.desired++
		}
		snapshot.identities[pod.Name] = string(pod.UID)
	}
	return snapshot, nil
}

func workloadComplete(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	name string,
	replicas int32,
) (bool, error) {
	key := client.ObjectKey{Namespace: namespace, Name: name}
	switch kind {
	case nativeKind:
		var workload appsv1.Deployment
		if err := k8sClient.Get(ctx, key, &workload); err != nil {
			return false, err
		}
		return workload.Status.ObservedGeneration >= workload.Generation &&
			workload.Status.UpdatedReplicas == replicas &&
			workload.Status.AvailableReplicas == replicas, nil
	case churnlessKind:
		var workload appsv1alpha1.Deployment
		if err := k8sClient.Get(ctx, key, &workload); err != nil {
			return false, err
		}
		return workload.Status.ObservedGeneration >= workload.Generation &&
			workload.Status.UpdatedReplicas == replicas &&
			workload.Status.AvailableReplicas == replicas &&
			workload.Status.InPlace != nil &&
			workload.Status.InPlace.ReadyUpdatedReplicas == replicas, nil
	default:
		return false, fmt.Errorf("unknown workload kind %q", kind)
	}
}

func updateImage(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	name, image string,
) error {
	key := client.ObjectKey{Namespace: namespace, Name: name}
	switch kind {
	case nativeKind:
		var workload appsv1.Deployment
		if err := k8sClient.Get(ctx, key, &workload); err != nil {
			return err
		}
		workload.Spec.Template.Spec.Containers[0].Image = image
		return k8sClient.Update(ctx, &workload)
	case churnlessKind:
		var workload appsv1alpha1.Deployment
		if err := k8sClient.Get(ctx, key, &workload); err != nil {
			return err
		}
		workload.Spec.Template.Spec.Containers[0].Image = image
		return k8sClient.Update(ctx, &workload)
	default:
		return fmt.Errorf("unknown workload kind %q", kind)
	}
}

func createNativeDeployment(
	ctx context.Context,
	k8sClient client.Client,
	namespace, name string,
	replicas int32,
	image string,
	maxSurge, maxUnavailable intstr.IntOrString,
) error {
	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: benchmarkDeploymentSpec(
			name,
			replicas,
			image,
			maxSurge,
			maxUnavailable,
		),
	}
	if err := k8sClient.Create(ctx, workload); err != nil {
		return fmt.Errorf("create native Deployment: %w", err)
	}
	return nil
}

func createWorkload(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	name string,
	replicas int32,
	image string,
	maxSurge, maxUnavailable intstr.IntOrString,
) error {
	switch kind {
	case nativeKind:
		return createNativeDeployment(
			ctx,
			k8sClient,
			namespace,
			name,
			replicas,
			image,
			maxSurge,
			maxUnavailable,
		)
	case churnlessKind:
		return createChurnlessDeployment(
			ctx,
			k8sClient,
			namespace,
			name,
			replicas,
			image,
			maxSurge,
			maxUnavailable,
		)
	default:
		return fmt.Errorf("unknown workload kind %q", kind)
	}
}

func createChurnlessDeployment(
	ctx context.Context,
	k8sClient client.Client,
	namespace, name string,
	replicas int32,
	image string,
	maxSurge, maxUnavailable intstr.IntOrString,
) error {
	workload := &appsv1alpha1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: benchmarkDeploymentSpec(
			name,
			replicas,
			image,
			maxSurge,
			maxUnavailable,
		)},
	}
	if err := k8sClient.Create(ctx, workload); err != nil {
		return fmt.Errorf("create Churnless Deployment: %w", err)
	}
	return nil
}

func benchmarkDeploymentSpec(
	name string,
	replicas int32,
	image string,
	maxSurge, maxUnavailable intstr.IntOrString,
) appsv1.DeploymentSpec {
	return appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{benchmarkLabel: name},
		},
		Strategy: appsv1.DeploymentStrategy{
			Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxSurge:       &maxSurge,
				MaxUnavailable: &maxUnavailable,
			},
		},
		Template: benchmarkTemplate(name, image),
	}
}

func benchmarkTemplate(name, image string) corev1.PodTemplateSpec {
	zero := int64(0)
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{benchmarkLabel: name},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &zero,
			Containers: []corev1.Container{{
				Name:            "workload",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
			}},
		},
	}
}

func deleteWorkload(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	name string,
	timeout time.Duration,
) error {
	key := client.ObjectKey{Namespace: namespace, Name: name}
	switch kind {
	case nativeKind:
		if err := k8sClient.Delete(
			ctx,
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
				Name: key.Name, Namespace: key.Namespace,
			}},
		); client.IgnoreNotFound(err) != nil {
			return err
		}
	case churnlessKind:
		if err := k8sClient.Delete(
			ctx,
			&appsv1alpha1.Deployment{ObjectMeta: metav1.ObjectMeta{
				Name: key.Name, Namespace: key.Namespace,
			}},
		); client.IgnoreNotFound(err) != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown workload kind %q", kind)
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := listPods(waitCtx, k8sClient, namespace, name, "")
		if err != nil {
			return err
		}
		if snapshot.total == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for workload Pods to be deleted: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func countReplicaSets(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	kind workloadKind,
	workload string,
) (int, error) {
	selector := labels.SelectorFromSet(map[string]string{benchmarkLabel: workload})
	switch kind {
	case nativeKind:
		var list appsv1.ReplicaSetList
		if err := k8sClient.List(
			ctx,
			&list,
			client.InNamespace(namespace),
			client.MatchingLabelsSelector{Selector: selector},
		); err != nil {
			return 0, err
		}
		return len(list.Items), nil
	case churnlessKind:
		var list appsv1alpha1.ReplicaSetList
		if err := k8sClient.List(ctx, &list, client.InNamespace(namespace)); err != nil {
			return 0, err
		}
		count := 0
		for i := range list.Items {
			if selector.Matches(labels.Set(list.Items[i].Spec.Template.Labels)) {
				count++
			}
		}
		return count, nil
	default:
		return 0, fmt.Errorf("unknown workload kind %q", kind)
	}
}

func printSummary(
	opts options,
	native, churnless []rolloutSample,
) {
	nativeMedian := medianDuration(native)
	churnlessMedian := medianDuration(churnless)
	ratio := float64(nativeMedian) / float64(churnlessMedian)

	fmt.Println()
	fmt.Println("Rollout benchmark summary")
	fmt.Printf(
		"replicas=%d iterations=%d policy=maxSurge:25%%,maxUnavailable:25%%\n",
		opts.replicas,
		opts.iterations,
	)
	fmt.Printf(
		"native    median=%s range=%s..%s retained=0/%d replicaSets=%d\n",
		nativeMedian.Round(time.Millisecond),
		minDuration(native).Round(time.Millisecond),
		maxDuration(native).Round(time.Millisecond),
		opts.replicas,
		medianReplicaSets(native),
	)
	fmt.Printf(
		"churnless median=%s range=%s..%s retained=%d/%d replicaSets=%d\n",
		churnlessMedian.Round(time.Millisecond),
		minDuration(churnless).Round(time.Millisecond),
		maxDuration(churnless).Round(time.Millisecond),
		opts.replicas,
		opts.replicas,
		medianReplicaSets(churnless),
	)
	fmt.Printf("native/churnless median duration ratio=%.2fx\n", ratio)
	fmt.Println("Note: this single-node Kind result includes controller, API, kubelet, and readiness latency.")
}

func retainedPods(before, after map[string]string) int {
	retained := 0
	for name, uid := range before {
		if after[name] == uid {
			retained++
		}
	}
	return retained
}

func isPodReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady &&
			pod.Status.Conditions[i].Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func medianDuration(samples []rolloutSample) time.Duration {
	values := durations(samples)
	return values[len(values)/2]
}

func minDuration(samples []rolloutSample) time.Duration {
	return durations(samples)[0]
}

func maxDuration(samples []rolloutSample) time.Duration {
	values := durations(samples)
	return values[len(values)-1]
}

func durations(samples []rolloutSample) []time.Duration {
	values := make([]time.Duration, len(samples))
	for i := range samples {
		values[i] = samples[i].duration
	}
	slices.Sort(values)
	return values
}

func medianReplicaSets(samples []rolloutSample) int {
	values := make([]int, len(samples))
	for i := range samples {
		values[i] = samples[i].replicaSets
	}
	slices.Sort(values)
	return values[len(values)/2]
}

func kubeConfig(contextName string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		overrides,
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load context %q: %w", contextName, err)
	}
	return config, nil
}
