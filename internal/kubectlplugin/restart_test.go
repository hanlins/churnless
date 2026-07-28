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

package kubectlplugin

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/workloadmeta"
)

const (
	restartTestNamespace      = "team"
	restartTestWorkload       = "web"
	restartTestNativeValue    = "native"
	restartTestChurnlessValue = "churnless"
	restartTestTimestamp      = "2026-07-28T19:34:56.123456789Z"
	restartTestYes            = "yes"
)

var restartTestTime = time.Date(
	2026,
	time.July,
	28,
	12,
	34,
	56,
	123456789,
	time.FixedZone("PDT", -7*60*60),
)

func TestParseRestartResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resource   string
		apiVersion restartAPIVersion
	}{
		{"deployment/web", restartAPIVersionAny},
		{"deployments/web", restartAPIVersionAny},
		{"deploy/web", restartAPIVersionNative},
		{"deployment.apps/web", restartAPIVersionNative},
		{"deployments.apps/web", restartAPIVersionNative},
		{"cdeploy/web", restartAPIVersionChurnless},
		{"deployment.churnless.io/web", restartAPIVersionChurnless},
		{"deployments.churnless.io/web", restartAPIVersionChurnless},
	}
	for _, test := range tests {
		t.Run(test.resource, func(t *testing.T) {
			t.Parallel()

			parsed, err := parseRestartResource(test.resource)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.name != restartTestWorkload || parsed.apiVersion != test.apiVersion {
				t.Fatalf("parsed resource = %#v", parsed)
			}
		})
	}

	for _, resource := range []string{
		restartTestWorkload,
		"pod/" + restartTestWorkload,
		"deployment/",
		"deployment/a/b",
	} {
		if _, err := parseRestartResource(resource); err == nil {
			t.Fatalf("parseRestartResource(%q) succeeded", resource)
		}
	}
}

func TestDeploymentRestarterRestartsNativeDeployment(t *testing.T) {
	t.Parallel()

	deployment := nativeRestartTestDeployment(false)
	deployment.Spec.Template.Annotations = map[string]string{
		"example.com/keep": restartTestNativeValue,
	}
	k8sClient := restartTestClient(t, deployment)
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	canonical, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment/"+restartTestWorkload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != "deployment.apps/"+restartTestWorkload {
		t.Fatalf("canonical resource = %q", canonical)
	}

	var current appsv1.Deployment
	key := types.NamespacedName{
		Namespace: restartTestNamespace,
		Name:      restartTestWorkload,
	}
	if err := k8sClient.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if got := current.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; got !=
		restartTestTimestamp {
		t.Fatalf("restart timestamp = %q", got)
	}
	if got := current.Spec.Template.Annotations["example.com/keep"]; got != restartTestNativeValue {
		t.Fatalf("preserved annotation = %q", got)
	}
}

func TestDeploymentRestarterResolvesNativeWhenChurnlessAPIIsNotServed(t *testing.T) {
	t.Parallel()

	deployment := nativeRestartTestDeployment(false)
	baseClient := restartTestClient(t, deployment)
	k8sClient := &noChurnlessKindClient{Client: baseClient}
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	canonical, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment/"+restartTestWorkload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != "deployment.apps/"+restartTestWorkload {
		t.Fatalf("canonical resource = %q", canonical)
	}

	var current appsv1.Deployment
	if err := baseClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(deployment),
		&current,
	); err != nil {
		t.Fatal(err)
	}
	if got := current.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; got !=
		restartTestTimestamp {
		t.Fatalf("restart timestamp = %q", got)
	}
}

func TestDeploymentRestarterRestartsExplicitChurnlessDeployment(t *testing.T) {
	t.Parallel()

	native := nativeRestartTestDeployment(false)
	churnless := churnlessRestartTestDeployment(false)
	churnless.Spec.Template.Annotations = map[string]string{
		"example.com/keep": restartTestChurnlessValue,
	}
	k8sClient := restartTestClient(t, native, churnless)
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	canonical, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"cdeploy/"+restartTestWorkload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != "deployment.churnless.io/"+restartTestWorkload {
		t.Fatalf("canonical resource = %q", canonical)
	}

	key := types.NamespacedName{
		Namespace: restartTestNamespace,
		Name:      restartTestWorkload,
	}
	var currentChurnless appsv1alpha1.Deployment
	if err := k8sClient.Get(context.Background(), key, &currentChurnless); err != nil {
		t.Fatal(err)
	}
	if got := currentChurnless.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; got !=
		restartTestTimestamp {
		t.Fatalf("restart timestamp = %q", got)
	}
	if got := currentChurnless.Spec.Template.Annotations["example.com/keep"]; got !=
		restartTestChurnlessValue {
		t.Fatalf("preserved annotation = %q", got)
	}

	var currentNative appsv1.Deployment
	if err := k8sClient.Get(context.Background(), key, &currentNative); err != nil {
		t.Fatal(err)
	}
	if _, found := currentNative.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; found {
		t.Fatal("explicit Churnless restart modified native Deployment")
	}
}

func TestDeploymentRestarterResolvesChurnlessDeploymentWithNilAnnotations(t *testing.T) {
	t.Parallel()

	churnless := churnlessRestartTestDeployment(false)
	k8sClient := restartTestClient(t, churnless)
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	canonical, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment/"+restartTestWorkload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != "deployment.churnless.io/"+restartTestWorkload {
		t.Fatalf("canonical resource = %q", canonical)
	}
	var current appsv1alpha1.Deployment
	if err := k8sClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(churnless),
		&current,
	); err != nil {
		t.Fatal(err)
	}
	if got := current.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; got !=
		restartTestTimestamp {
		t.Fatalf("restart timestamp = %q", got)
	}
}

func TestDeploymentRestarterRejectsGenericAmbiguity(t *testing.T) {
	t.Parallel()

	native := nativeRestartTestDeployment(false)
	churnless := churnlessRestartTestDeployment(false)
	k8sClient := restartTestClient(t, native, churnless)
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	_, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment/"+restartTestWorkload,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "is ambiguous") ||
		!strings.Contains(err.Error(), "deployment.apps/"+restartTestWorkload) ||
		!strings.Contains(err.Error(), "deployment.churnless.io/"+restartTestWorkload) {
		t.Fatalf("error = %v", err)
	}
	assertRestartAnnotationAbsent(t, k8sClient, native)
	assertRestartAnnotationAbsent(t, k8sClient, churnless)
}

func TestDeploymentRestarterReportsMissingGenericDeployment(t *testing.T) {
	t.Parallel()

	restarter := newDeploymentRestarter(restartTestClient(t), func() time.Time {
		return restartTestTime
	})
	_, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment/"+restartTestWorkload,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "was not found") ||
		!strings.Contains(err.Error(), "deployment.apps/"+restartTestWorkload) ||
		!strings.Contains(err.Error(), "deployment.churnless.io/"+restartTestWorkload) {
		t.Fatalf("error = %v", err)
	}
}

func TestDeploymentRestarterFailsClosedWhenGenericLookupIsForbidden(t *testing.T) {
	t.Parallel()

	churnless := churnlessRestartTestDeployment(false)
	baseClient := restartTestClient(t, churnless)
	k8sClient := &forbiddenNativeGetClient{Client: baseClient}
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	_, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment/"+restartTestWorkload,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "resolve native Deployment") ||
		!apierrors.IsForbidden(err) {
		t.Fatalf("error = %v", err)
	}
	assertRestartAnnotationAbsent(t, baseClient, churnless)
}

func TestDeploymentRestarterRejectsPausedDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		object   client.Object
		resource string
	}{
		{
			name:     restartTestNativeValue,
			object:   nativeRestartTestDeployment(true),
			resource: "deployment.apps/" + restartTestWorkload,
		},
		{
			name:     "Churnless",
			object:   churnlessRestartTestDeployment(true),
			resource: "deployment.churnless.io/" + restartTestWorkload,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			k8sClient := restartTestClient(t, test.object)
			restarter := newDeploymentRestarter(
				k8sClient,
				func() time.Time { return restartTestTime },
			)
			_, err := restarter.Restart(
				context.Background(),
				restartTestNamespace,
				test.resource,
			)
			if err == nil || !strings.Contains(err.Error(), "is paused") {
				t.Fatalf("error = %v", err)
			}
			assertRestartAnnotationAbsent(t, k8sClient, test.object)
		})
	}
}

func TestDeploymentRestarterRetriesConflictAndPreservesConcurrentChanges(t *testing.T) {
	t.Parallel()

	deployment := nativeRestartTestDeployment(false)
	deployment.UID = types.UID("native-uid")
	deployment.Spec.Template.Annotations = map[string]string{
		"example.com/original": restartTestYes,
	}
	baseClient := restartTestClient(t, deployment)
	k8sClient := &conflictOnceClient{
		Client: baseClient,
		key: types.NamespacedName{
			Namespace: restartTestNamespace,
			Name:      restartTestWorkload,
		},
	}
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	canonical, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deploy/"+restartTestWorkload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != "deployment.apps/"+restartTestWorkload {
		t.Fatalf("canonical resource = %q", canonical)
	}
	if k8sClient.patchCalls != 2 {
		t.Fatalf("patch calls = %d, want 2", k8sClient.patchCalls)
	}

	var current appsv1.Deployment
	if err := baseClient.Get(context.Background(), k8sClient.key, &current); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"example.com/original":                    restartTestYes,
		"example.com/concurrent":                  restartTestYes,
		workloadmeta.KubectlRestartedAtAnnotation: restartTestTimestamp,
	} {
		if got := current.Spec.Template.Annotations[key]; got != want {
			t.Fatalf("annotation %q = %q, want %q", key, got, want)
		}
	}
}

func TestDeploymentRestarterExplainsAmbiguousInterruption(t *testing.T) {
	t.Parallel()

	deployment := nativeRestartTestDeployment(false)
	baseClient := restartTestClient(t, deployment)
	k8sClient := &failingPatchClient{
		Client: baseClient,
		err:    context.DeadlineExceeded,
	}
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	_, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		"deployment.apps/"+restartTestWorkload,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "cluster state may have advanced") ||
		!strings.Contains(err.Error(), workloadmeta.KubectlRestartedAtAnnotation) ||
		!strings.Contains(err.Error(), "before retrying") {
		t.Fatalf("error = %v", err)
	}
	if k8sClient.patchCalls != 1 {
		t.Fatalf("patch calls = %d, want 1", k8sClient.patchCalls)
	}
	assertRestartAnnotationAbsent(t, baseClient, deployment)
}

type conflictOnceClient struct {
	client.Client
	key        types.NamespacedName
	patchCalls int
}

func (c *conflictOnceClient) Patch(
	ctx context.Context,
	object client.Object,
	patch client.Patch,
	options ...client.PatchOption,
) error {
	c.patchCalls++
	if c.patchCalls == 1 {
		var current appsv1.Deployment
		if err := c.Get(ctx, c.key, &current); err != nil {
			return err
		}
		if current.Spec.Template.Annotations == nil {
			current.Spec.Template.Annotations = map[string]string{}
		}
		current.Spec.Template.Annotations["example.com/concurrent"] = restartTestYes
		if err := c.Update(ctx, &current); err != nil {
			return err
		}
		return apierrors.NewConflict(
			appsv1.Resource("deployments"),
			object.GetName(),
			fmt.Errorf("concurrent update"),
		)
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

type failingPatchClient struct {
	client.Client
	err        error
	patchCalls int
}

func (c *failingPatchClient) Patch(
	context.Context,
	client.Object,
	client.Patch,
	...client.PatchOption,
) error {
	c.patchCalls++
	return c.err
}

type forbiddenNativeGetClient struct {
	client.Client
}

func (c *forbiddenNativeGetClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, native := object.(*appsv1.Deployment); native {
		return apierrors.NewForbidden(
			appsv1.Resource("deployments"),
			key.Name,
			fmt.Errorf("forbidden"),
		)
	}
	return c.Client.Get(ctx, key, object, options...)
}

type noChurnlessKindClient struct {
	client.Client
}

func (c *noChurnlessKindClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, churnless := object.(*appsv1alpha1.Deployment); churnless {
		return &meta.NoKindMatchError{
			GroupKind: schema.GroupKind{
				Group: appsv1alpha1.GroupVersion.Group,
				Kind:  "Deployment",
			},
			SearchedVersions: []string{appsv1alpha1.GroupVersion.Version},
		}
	}
	return c.Client.Get(ctx, key, object, options...)
}

func restartTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

func nativeRestartTestDeployment(paused bool) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restartTestWorkload,
			Namespace: restartTestNamespace,
		},
		Spec: appsv1.DeploymentSpec{Paused: paused},
	}
}

func churnlessRestartTestDeployment(paused bool) *appsv1alpha1.Deployment {
	return &appsv1alpha1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restartTestWorkload,
			Namespace: restartTestNamespace,
		},
		Spec: appsv1alpha1.DeploymentSpec{
			DeploymentSpec: appsv1.DeploymentSpec{Paused: paused},
		},
	}
}

func assertRestartAnnotationAbsent(
	t *testing.T,
	k8sClient client.Client,
	object client.Object,
) {
	t.Helper()

	current := object.DeepCopyObject().(client.Object)
	if err := k8sClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(object),
		current,
	); err != nil {
		t.Fatal(err)
	}
	switch deployment := current.(type) {
	case *appsv1.Deployment:
		if _, found := deployment.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; found {
			t.Fatal("native Deployment has restart annotation")
		}
	case *appsv1alpha1.Deployment:
		if _, found := deployment.Spec.Template.Annotations[workloadmeta.KubectlRestartedAtAnnotation]; found {
			t.Fatal("Churnless Deployment has restart annotation")
		}
	default:
		t.Fatalf("unsupported Deployment type %T", current)
	}
}
