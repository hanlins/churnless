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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	testAppLabel = "app"
	testWebValue = "web"
	testOther    = "other"
)

func TestDefaultDeployment(t *testing.T) {
	t.Parallel()
	spec := appsv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{testAppLabel: testWebValue}},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{testAppLabel: testWebValue}},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{{
					Name: "credentials",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: "credentials"},
					},
				}},
				Containers: []corev1.Container{{
					Name:  "web",
					Image: "nginx:1.28-alpine",
					Env: []corev1.EnvVar{{
						Name: "POD_NAME",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
						},
					}},
				}},
			},
		},
	}

	DefaultDeployment(&spec)

	if *spec.Replicas != 1 {
		t.Fatalf("replicas = %d, want 1", *spec.Replicas)
	}
	if spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType {
		t.Fatalf("strategy = %q, want RollingUpdate", spec.Strategy.Type)
	}
	if got := spec.Strategy.RollingUpdate.MaxSurge.String(); got != "25%" {
		t.Fatalf("maxSurge = %q, want 25%%", got)
	}
	if got := spec.Strategy.RollingUpdate.MaxUnavailable.String(); got != "25%" {
		t.Fatalf("maxUnavailable = %q, want 25%%", got)
	}
	if *spec.RevisionHistoryLimit != 10 || *spec.ProgressDeadlineSeconds != 600 {
		t.Fatalf(
			"history/deadline = %d/%d, want 10/600",
			*spec.RevisionHistoryLimit,
			*spec.ProgressDeadlineSeconds,
		)
	}
	podSpec := spec.Template.Spec
	if podSpec.RestartPolicy != corev1.RestartPolicyAlways ||
		podSpec.DNSPolicy != corev1.DNSClusterFirst ||
		podSpec.SchedulerName != corev1.DefaultSchedulerName {
		t.Fatalf("Pod defaults not applied: %#v", podSpec)
	}
	if podSpec.Containers[0].ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("imagePullPolicy = %q, want IfNotPresent", podSpec.Containers[0].ImagePullPolicy)
	}
	if got := podSpec.Volumes[0].Secret.DefaultMode; got == nil || *got != 0o644 {
		t.Fatalf("secret defaultMode = %v, want 0644", got)
	}
	if got := podSpec.Containers[0].Env[0].ValueFrom.FieldRef.APIVersion; got != "v1" {
		t.Fatalf("fieldRef apiVersion = %q, want v1", got)
	}
}

func TestValidateDeployment(t *testing.T) {
	t.Parallel()
	spec := validDeploymentSpec()
	DefaultDeployment(&spec)
	if errs := ValidateDeployment(&spec, nil); len(errs) != 0 {
		t.Fatalf("valid spec rejected: %v", errs)
	}

	spec.Template.Labels[testAppLabel] = testOther
	if errs := ValidateDeployment(&spec, nil); len(errs) == 0 {
		t.Fatal("selector/template mismatch was accepted")
	}

	spec = validDeploymentSpec()
	DefaultDeployment(&spec)
	zero := intstr.FromInt32(0)
	spec.Strategy.RollingUpdate.MaxSurge = &zero
	spec.Strategy.RollingUpdate.MaxUnavailable = &zero
	if errs := ValidateDeployment(&spec, nil); len(errs) == 0 {
		t.Fatal("zero maxSurge and maxUnavailable were accepted")
	}

	old := validDeploymentSpec()
	DefaultDeployment(&old)
	spec = *old.DeepCopy()
	spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{testAppLabel: testOther}}
	spec.Template.Labels = map[string]string{testAppLabel: testOther}
	if errs := ValidateDeployment(&spec, &old); len(errs) == 0 {
		t.Fatal("selector mutation was accepted")
	}

	spec = validDeploymentSpec()
	DefaultDeployment(&spec)
	spec.Template.Spec.Containers[0].Name = "INVALID_NAME"
	if errs := ValidateDeployment(&spec, nil); len(errs) == 0 {
		t.Fatal("invalid nested Pod template was accepted")
	}
}

func TestReplicaSetUsesUpstreamAdmission(t *testing.T) {
	t.Parallel()
	deployment := validDeploymentSpec()
	spec := appsv1.ReplicaSetSpec{
		Selector: deployment.Selector,
		Template: deployment.Template,
	}

	DefaultReplicaSet(&spec)
	if spec.Replicas == nil || *spec.Replicas != 1 {
		t.Fatalf("replicas = %v, want 1", spec.Replicas)
	}
	if errs := ValidateReplicaSet(&spec, nil); len(errs) != 0 {
		t.Fatalf("valid spec rejected: %v", errs)
	}

	spec.Template.Spec.Containers[0].Name = "INVALID_NAME"
	if errs := ValidateReplicaSet(&spec, nil); len(errs) == 0 {
		t.Fatal("invalid nested Pod template was accepted")
	}
}

func TestResolveFenceposts(t *testing.T) {
	t.Parallel()
	surge := intstr.FromString("0%")
	unavailable := intstr.FromString("1%")
	gotSurge, gotUnavailable, err := ResolveFenceposts(&surge, &unavailable, 1)
	if err != nil {
		t.Fatal(err)
	}
	if gotSurge != 0 || gotUnavailable != 1 {
		t.Fatalf("fenceposts = %d/%d, want 0/1", gotSurge, gotUnavailable)
	}
}

func TestPodAvailability(t *testing.T) {
	t.Parallel()
	now := metav1.NewTime(time.Unix(100, 0))
	pod := corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}}}
	if IsPodAvailable(&pod, 10, now) {
		t.Fatal("zero Ready transition time counted as available")
	}
	pod.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-10 * time.Second))
	if !IsPodAvailable(&pod, 10, now) {
		t.Fatal("Pod was not available at the exact minReadySeconds boundary")
	}
}

func validDeploymentSpec() appsv1.DeploymentSpec {
	replicas := int32(1)
	return appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{testAppLabel: testWebValue}},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{testAppLabel: testWebValue}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:  "web",
				Image: "nginx:1.28-alpine",
			}}},
		},
	}
}
