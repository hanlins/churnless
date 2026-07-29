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
	restartTestNamespace = "team"
	restartTestWorkload  = "web"
	restartTestTimestamp = "2026-07-28T19:34:56.123456789Z"
	restartTestYes       = "yes"
)

var restartTestTime = time.Date(
	2026, time.July, 28, 12, 34, 56, 123456789, time.FixedZone("PDT", -7*60*60),
)

func TestDeploymentRestarterResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		objects      []client.Object
		wrap         func(client.Client) client.Client
		api          deploymentAPI
		canonical    string
		changedIndex int
		errorParts   []string
	}{
		{
			name:      "native generic",
			objects:   []client.Object{nativeRestartDeployment(nil)},
			api:       deploymentAPIAny,
			canonical: "deployment.apps/" + restartTestWorkload,
		},
		{
			name:      "Churnless generic",
			objects:   []client.Object{churnlessRestartDeployment(false, nil)},
			api:       deploymentAPIAny,
			canonical: "deployment.churnless.io/" + restartTestWorkload,
		},
		{
			name:    "native without Churnless API",
			objects: []client.Object{nativeRestartDeployment(nil)},
			wrap: func(c client.Client) client.Client {
				return &churnlessNoMatchClient{Client: c}
			},
			api:       deploymentAPIAny,
			canonical: "deployment.apps/" + restartTestWorkload,
		},
		{
			name: "explicit native disambiguates",
			objects: []client.Object{
				nativeRestartDeployment(nil),
				churnlessRestartDeployment(false, nil),
			},
			api:       deploymentAPINative,
			canonical: "deployment.apps/" + restartTestWorkload,
		},
		{
			name: "explicit Churnless disambiguates",
			objects: []client.Object{
				nativeRestartDeployment(nil),
				churnlessRestartDeployment(false, nil),
			},
			api:          deploymentAPIChurnless,
			canonical:    "deployment.churnless.io/" + restartTestWorkload,
			changedIndex: 1,
		},
		{
			name: "generic ambiguity",
			objects: []client.Object{
				nativeRestartDeployment(nil),
				churnlessRestartDeployment(false, nil),
			},
			api: deploymentAPIAny,
			errorParts: []string{
				"is ambiguous",
				"deployment.apps/" + restartTestWorkload,
				"deployment.churnless.io/" + restartTestWorkload,
			},
		},
		{
			name: "generic missing",
			api:  deploymentAPIAny,
			errorParts: []string{
				"was not found",
				"deployment.apps/" + restartTestWorkload,
				"deployment.churnless.io/" + restartTestWorkload,
			},
		},
		{
			name:       "paused Churnless",
			objects:    []client.Object{churnlessRestartDeployment(true, nil)},
			api:        deploymentAPIChurnless,
			errorParts: []string{"is paused"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseClient := restartClient(t, test.objects...)
			k8sClient := baseClient
			if test.wrap != nil {
				k8sClient = test.wrap(baseClient)
			}
			restarter := newDeploymentRestarter(k8sClient, func() time.Time {
				return restartTestTime
			})
			canonical, err := restarter.Restart(
				context.Background(),
				restartTestNamespace,
				deploymentReference{name: restartTestWorkload, api: test.api},
			)

			if test.canonical != "" {
				if err != nil {
					t.Fatal(err)
				}
				if canonical != test.canonical {
					t.Fatalf("canonical resource = %q, want %q", canonical, test.canonical)
				}
			} else {
				if err == nil {
					t.Fatal("restart succeeded")
				}
				for _, want := range test.errorParts {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("error = %v, want substring %q", err, want)
					}
				}
			}

			for i, object := range test.objects {
				got, found := restartAnnotations(t, baseClient, object)[workloadmeta.KubectlRestartedAtAnnotation]
				if test.canonical != "" && i == test.changedIndex {
					if got != restartTestTimestamp {
						t.Fatalf("%T restart timestamp = %q", object, got)
					}
				} else if found {
					t.Fatalf("%T unexpectedly restarted at %q", object, got)
				}
			}
		})
	}
}

func TestDeploymentRestarterReportsInterruptedPatch(t *testing.T) {
	t.Parallel()

	deployment := nativeRestartDeployment(nil)
	baseClient := restartClient(t, deployment)
	k8sClient := &patchErrorClient{
		Client: baseClient,
		err:    context.DeadlineExceeded,
	}
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	_, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		deploymentReference{name: restartTestWorkload, api: deploymentAPINative},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "cluster state may have advanced") ||
		!strings.Contains(err.Error(), workloadmeta.KubectlRestartedAtAnnotation) ||
		!strings.Contains(err.Error(), "before retrying") {
		t.Fatalf("error = %v", err)
	}
	if _, found := restartAnnotations(t, baseClient, deployment)[workloadmeta.KubectlRestartedAtAnnotation]; found {
		t.Fatal("failed patch changed the restart timestamp")
	}
}

func TestDeploymentRestarterRetriesConflict(t *testing.T) {
	t.Parallel()

	deployment := nativeRestartDeployment(map[string]string{
		"example.com/original": restartTestYes,
	})
	deployment.UID = types.UID("native-uid")
	baseClient := restartClient(t, deployment)
	k8sClient := &conflictOnceClient{Client: baseClient}
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		return restartTestTime
	})

	_, err := restarter.Restart(
		context.Background(),
		restartTestNamespace,
		deploymentReference{name: restartTestWorkload, api: deploymentAPINative},
	)
	if err != nil {
		t.Fatal(err)
	}
	if k8sClient.patchCalls != 2 {
		t.Fatalf("patch calls = %d, want 2", k8sClient.patchCalls)
	}
	annotations := restartAnnotations(t, baseClient, deployment)
	for key, want := range map[string]string{
		"example.com/original":                    restartTestYes,
		"example.com/concurrent":                  restartTestYes,
		workloadmeta.KubectlRestartedAtAnnotation: restartTestTimestamp,
	} {
		if got := annotations[key]; got != want {
			t.Fatalf("annotation %q = %q, want %q", key, got, want)
		}
	}
}

func TestDeploymentRestarterChangesTimestamp(t *testing.T) {
	t.Parallel()

	deployment := churnlessRestartDeployment(false, map[string]string{
		workloadmeta.KubectlRestartedAtAnnotation: "old",
	})
	k8sClient := restartClient(t, deployment)
	times := []time.Time{restartTestTime, restartTestTime.Add(time.Second)}
	call := 0
	restarter := newDeploymentRestarter(k8sClient, func() time.Time {
		current := times[call]
		call++
		return current
	})

	previous := "old"
	for _, current := range times {
		_, err := restarter.Restart(
			context.Background(),
			restartTestNamespace,
			deploymentReference{name: restartTestWorkload, api: deploymentAPIChurnless},
		)
		if err != nil {
			t.Fatal(err)
		}
		got := restartAnnotations(t, k8sClient, deployment)[workloadmeta.KubectlRestartedAtAnnotation]
		if want := current.UTC().Format(time.RFC3339Nano); got != want {
			t.Fatalf("restart timestamp = %q, want %q", got, want)
		}
		if got == previous {
			t.Fatalf("restart timestamp did not change from %q", previous)
		}
		previous = got
	}
}

type patchErrorClient struct {
	client.Client
	err error
}

func (c *patchErrorClient) Patch(
	context.Context, client.Object, client.Patch, ...client.PatchOption,
) error {
	return c.err
}

type churnlessNoMatchClient struct {
	client.Client
}

func (c *churnlessNoMatchClient) Get(
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

type conflictOnceClient struct {
	client.Client
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
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), &current); err != nil {
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

func restartClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func nativeRestartDeployment(annotations map[string]string) *appsv1.Deployment {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: restartTestWorkload, Namespace: restartTestNamespace},
	}
	deployment.Spec.Template.Annotations = annotations
	return deployment
}

func churnlessRestartDeployment(
	paused bool, annotations map[string]string,
) *appsv1alpha1.Deployment {
	deployment := &appsv1alpha1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: restartTestWorkload, Namespace: restartTestNamespace},
		Spec: appsv1alpha1.DeploymentSpec{
			DeploymentSpec: appsv1.DeploymentSpec{Paused: paused},
		},
	}
	deployment.Spec.Template.Annotations = annotations
	return deployment
}

func restartAnnotations(
	t *testing.T,
	k8sClient client.Client,
	object client.Object,
) map[string]string {
	t.Helper()

	current := object.DeepCopyObject().(client.Object)
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(object), current); err != nil {
		t.Fatal(err)
	}
	switch deployment := current.(type) {
	case *appsv1.Deployment:
		return deployment.Spec.Template.Annotations
	case *appsv1alpha1.Deployment:
		return deployment.Spec.Template.Annotations
	default:
		t.Fatalf("unsupported Deployment type %T", current)
		return nil
	}
}
