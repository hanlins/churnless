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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const (
	updatedTestValue           = "two"
	templateRevisionAnnotation = "example.com/revision"
	redeployTestTimestamp      = "2026-07-26T12:00:00Z"
	oldTemplateMetadataKey     = "example.com/old"
	injectedMetadataKey        = "injector.example/x"
	injectedMetadataValue      = "keep"
	testWorkloadName           = "revision"
	testResizePodName          = "resize"
)

func TestStructuralRevisionClassifiesMutableAndRedeployFields(t *testing.T) {
	t.Parallel()

	base := revisionTestDeployment()
	base.Annotations[inPlaceResourcesAnnotation] = inPlaceResourcesBestEffort
	revision := structuralRevision(base)

	metadata := base.DeepCopy()
	metadata.Spec.Template.Labels["example.com/track"] = updatedTestValue
	metadata.Spec.Template.Annotations = map[string]string{
		templateRevisionAnnotation: updatedTestValue,
	}
	if got := structuralRevision(metadata); got != revision {
		t.Fatalf("template metadata revision = %q, want %q", got, revision)
	}

	resources := base.DeepCopy()
	resources.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] =
		resource.MustParse("200m")
	if got := structuralRevision(resources); got != revision {
		t.Fatalf("best-effort CPU revision = %q, want %q", got, revision)
	}

	immutable := base.DeepCopy()
	immutable.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{
		Name: "IMMUTABLE", Value: updatedTestValue,
	}}
	if got := structuralRevision(immutable); got == revision {
		t.Fatal("immutable container field did not create a structural revision")
	}

	redeploy := base.DeepCopy()
	redeploy.Annotations[redeployAnnotation] = redeployTestTimestamp
	if got := structuralRevision(redeploy); got == revision {
		t.Fatal("explicit redeploy token did not create a structural revision")
	}

	kubectlRestart := base.DeepCopy()
	kubectlRestart.Spec.Template.Annotations = map[string]string{
		kubectlRestartedAtAnnotation: redeployTestTimestamp,
	}
	if got := structuralRevision(kubectlRestart); got == revision {
		t.Fatal("Pod-template restart token did not create a structural revision")
	}

	kubectlAnnotate := base.DeepCopy()
	kubectlAnnotate.Annotations[kubectlRestartedAtAnnotation] = redeployTestTimestamp
	if got := structuralRevision(kubectlAnnotate); got == revision {
		t.Fatal("workload restart annotation did not create a structural revision")
	}

	withoutBestEffort := base.DeepCopy()
	delete(withoutBestEffort.Annotations, inPlaceResourcesAnnotation)
	withoutBestEffortRevision := structuralRevision(withoutBestEffort)
	withoutBestEffort.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] =
		resource.MustParse("200m")
	if got := structuralRevision(withoutBestEffort); got == withoutBestEffortRevision {
		t.Fatal("default resource change unexpectedly stayed in place")
	}
}

func TestSetPodMetadataTracksTemplateKeysWithoutRemovingInjectedMetadata(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{
			appLabel:               "metadata",
			oldTemplateMetadataKey: "old",
			injectedMetadataKey:    injectedMetadataValue,
		},
		Annotations: map[string]string{
			oldTemplateMetadataKey: "old",
			injectedMetadataKey:    injectedMetadataValue,
			managedLabelKeysAnnotation: encodeManagedKeys(map[string]struct{}{
				appLabel: {}, oldTemplateMetadataKey: {},
			}),
			managedAnnotationKeysAnnotation: encodeManagedKeys(map[string]struct{}{
				oldTemplateMetadataKey: {},
			}),
		},
	}}
	template := &corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{
			appLabel:                "metadata",
			"example.com/new":       "new",
			structuralRevisionLabel: testWorkloadName,
		},
		Annotations: map[string]string{"example.com/new": "new"},
	}}

	setPodMetadata(pod, template)

	if !podMetadataMatches(pod, template) {
		t.Fatal("Pod metadata does not match its template after synchronization")
	}
	if _, ok := pod.Labels[oldTemplateMetadataKey]; ok {
		t.Fatal("removed template label remained on Pod")
	}
	if _, ok := pod.Annotations[oldTemplateMetadataKey]; ok {
		t.Fatal("removed template annotation remained on Pod")
	}
	if pod.Labels[injectedMetadataKey] != injectedMetadataValue ||
		pod.Annotations[injectedMetadataKey] != injectedMetadataValue {
		t.Fatal("metadata injected outside the template was removed")
	}
}

func TestBestEffortResourceResizeRetainsPodIdentity(t *testing.T) {
	t.Parallel()

	scheme := workloadTestScheme(t)
	pod, template := resourceResizeFixture()
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
	k8sClient := interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(
			ctx context.Context,
			k8sClient client.Client,
			subresource string,
			obj client.Object,
			_ ...client.SubResourceUpdateOption,
		) error {
			if subresource != podResizeSubresource {
				return nil
			}
			return k8sClient.Update(ctx, obj)
		},
	})

	if err := updatePods(
		context.Background(),
		k8sClient,
		[]corev1.Pod{*pod.DeepCopy()},
		template,
		allPods,
		1,
		mutablePodPolicy{resizeResources: true},
	); err != nil {
		t.Fatal(err)
	}

	var updated corev1.Pod
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.UID != pod.UID {
		t.Fatalf("Pod UID = %q, want retained UID %q", updated.UID, pod.UID)
	}
	if got := updated.Spec.Containers[0].Resources.Requests.Cpu().String(); got != "200m" {
		t.Fatalf("CPU request = %q, want 200m", got)
	}
}

func TestBestEffortResourceResizeFallsBackToReplacement(t *testing.T) {
	t.Parallel()

	scheme := workloadTestScheme(t)
	pod, template := resourceResizeFixture()
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
	k8sClient := interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(
			_ context.Context,
			_ client.Client,
			subresource string,
			obj client.Object,
			_ ...client.SubResourceUpdateOption,
		) error {
			if subresource == podResizeSubresource {
				return apierrors.NewForbidden(
					schema.GroupResource{Resource: "pods/resize"},
					obj.GetName(),
					errors.New("resize is unsupported"),
				)
			}
			return nil
		},
	})

	if err := updatePods(
		context.Background(),
		k8sClient,
		[]corev1.Pod{*pod.DeepCopy()},
		template,
		allPods,
		1,
		mutablePodPolicy{resizeResources: true},
	); err != nil {
		t.Fatal(err)
	}

	var replaced corev1.Pod
	err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), &replaced)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("fallback Pod lookup error = %v, want NotFound", err)
	}
}

func TestBestEffortResourceResizeReplacesPodAfterInfeasibleStatus(t *testing.T) {
	t.Parallel()

	scheme := workloadTestScheme(t)
	pod, template := resourceResizeFixture()
	pod.Spec.Containers[0].Resources = *template.Spec.Containers[0].Resources.DeepCopy()
	pod.Status.Resize = corev1.PodResizeStatusInfeasible
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod.DeepCopy()).
		Build()

	if err := updatePods(
		context.Background(),
		k8sClient,
		[]corev1.Pod{*pod.DeepCopy()},
		template,
		allPods,
		1,
		mutablePodPolicy{resizeResources: true},
	); err != nil {
		t.Fatal(err)
	}

	var replaced corev1.Pod
	err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &replaced)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("infeasible Pod lookup error = %v, want NotFound", err)
	}
}

func revisionTestDeployment() *appsv1alpha1.Deployment {
	replicas := int32(1)
	return &appsv1alpha1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testWorkloadName,
			Namespace:   rolloutTestNamespace,
			Annotations: map[string]string{},
		},
		Spec: appsv1alpha1.DeploymentSpec{DeploymentSpec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{appLabel: testWorkloadName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{appLabel: testWorkloadName},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "workload",
					Image: "registry.k8s.io/pause:3.9",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				}}},
			},
		}},
	}
}

func resourceResizeFixture() (*corev1.Pod, *corev1.PodTemplateSpec) {
	template := revisionTestDeployment().Spec.Template.DeepCopy()
	template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] =
		resource.MustParse("200m")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testResizePodName,
			Namespace: rolloutTestNamespace,
			UID:       "resize-uid",
			Labels:    map[string]string{appLabel: testWorkloadName},
		},
		Spec: *revisionTestDeployment().Spec.Template.Spec.DeepCopy(),
	}
	setPodMetadata(pod, template)
	return pod, template
}

func workloadTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
