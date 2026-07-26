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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
)

const (
	ownerKindAnnotation = "apps.churnless.io/owner-kind"
	ownerNameAnnotation = "apps.churnless.io/owner-name"
	ownerUIDLabel       = "apps.churnless.io/owner-uid"
	revisionAnnotation  = "apps.churnless.io/image-revision"
)

type podProgress struct {
	Revision string
	Updated  int32
	Ready    int32
	Total    int32
}

func desiredReplicas(replicas *int32) int32 {
	if replicas == nil {
		return 1
	}
	return *replicas
}

func selectorString(selector *metav1.LabelSelector) (string, error) {
	parsed, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

func imageRevision(template *corev1.PodTemplateSpec) string {
	type image struct {
		Name  string `json:"name"`
		Image string `json:"image"`
		Init  bool   `json:"init,omitempty"`
	}
	images := make([]image, 0, len(template.Spec.InitContainers)+len(template.Spec.Containers))
	for _, container := range template.Spec.InitContainers {
		images = append(images, image{Name: container.Name, Image: container.Image, Init: true})
	}
	for _, container := range template.Spec.Containers {
		images = append(images, image{Name: container.Name, Image: container.Image})
	}
	data, _ := json.Marshal(images)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:8])
}

func injectOwnership(
	template *corev1.PodTemplateSpec,
	owner client.Object,
	kind string,
) {
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	template.Labels[ownerUIDLabel] = string(owner.GetUID())
	template.Annotations[ownerKindAnnotation] = kind
	template.Annotations[ownerNameAnnotation] = owner.GetName()
	template.Annotations[revisionAnnotation] = imageRevision(template)
}

func cleanTemplate(template *corev1.PodTemplateSpec) *corev1.PodTemplateSpec {
	clean := template.DeepCopy()
	delete(clean.Labels, ownerUIDLabel)
	if len(clean.Labels) == 0 {
		clean.Labels = nil
	}
	delete(clean.Annotations, ownerKindAnnotation)
	delete(clean.Annotations, ownerNameAnnotation)
	delete(clean.Annotations, revisionAnnotation)
	if len(clean.Annotations) == 0 {
		clean.Annotations = nil
	}
	return clean
}

func clearImages(template *corev1.PodTemplateSpec) {
	for i := range template.Spec.InitContainers {
		template.Spec.InitContainers[i].Image = ""
	}
	for i := range template.Spec.Containers {
		template.Spec.Containers[i].Image = ""
	}
}

func templatesEqualExceptImages(current, desired *corev1.PodTemplateSpec) bool {
	left := cleanTemplate(current)
	right := cleanTemplate(desired)
	clearImages(left)
	clearImages(right)
	return apiequality.Semantic.DeepEqual(left, right)
}

func imagesEqual(current, desired *corev1.PodTemplateSpec) bool {
	return containerImagesEqual(current.Spec.InitContainers, desired.Spec.InitContainers) &&
		containerImagesEqual(current.Spec.Containers, desired.Spec.Containers)
}

func containerImagesEqual(current, desired []corev1.Container) bool {
	if len(current) != len(desired) {
		return false
	}
	for i := range desired {
		if current[i].Name != desired[i].Name || current[i].Image != desired[i].Image {
			return false
		}
	}
	return true
}

func prepareShadowTemplate(
	current, desired *corev1.PodTemplateSpec,
	owner client.Object,
	kind string,
	preserveImages bool,
) *corev1.PodTemplateSpec {
	if preserveImages && templatesEqualExceptImages(current, desired) {
		return current.DeepCopy()
	}
	result := desired.DeepCopy()
	injectOwnership(result, owner, kind)
	return result
}

func listOwnedPods(
	ctx context.Context,
	c client.Client,
	owner client.Object,
) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := c.List(
		ctx,
		&list,
		client.InNamespace(owner.GetNamespace()),
		client.MatchingLabels{ownerUIDLabel: string(owner.GetUID())},
	); err != nil {
		return nil, err
	}
	pods := make([]corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].DeletionTimestamp.IsZero() {
			pods = append(pods, list.Items[i])
		}
	}
	return pods, nil
}

func calculateProgress(
	pods []corev1.Pod,
	template *corev1.PodTemplateSpec,
	eligible func(corev1.Pod) bool,
) podProgress {
	progress := podProgress{Revision: imageRevision(template)}
	for i := range pods {
		if !eligible(pods[i]) {
			continue
		}
		progress.Total++
		if !podSpecHasImages(&pods[i], template) {
			continue
		}
		progress.Updated++
		if podReadyAndObserved(&pods[i], template) {
			progress.Ready++
		}
	}
	return progress
}

func updatePods(
	ctx context.Context,
	c client.Client,
	pods []corev1.Pod,
	template *corev1.PodTemplateSpec,
	eligible func(corev1.Pod) bool,
	parallelism int,
	less func(corev1.Pod, corev1.Pod) bool,
) error {
	if parallelism < 1 {
		return nil
	}
	inFlight := 0
	for i := range pods {
		if eligible(pods[i]) &&
			podSpecHasImages(&pods[i], template) &&
			!podReadyAndObserved(&pods[i], template) {
			inFlight++
		}
	}

	if less == nil {
		less = func(left, right corev1.Pod) bool {
			return left.CreationTimestamp.Before(&right.CreationTimestamp)
		}
	}
	slices.SortStableFunc(pods, func(left, right corev1.Pod) int {
		switch {
		case less(left, right):
			return -1
		case less(right, left):
			return 1
		default:
			return 0
		}
	})
	for i := range pods {
		if inFlight >= parallelism {
			break
		}
		if !eligible(pods[i]) || podSpecHasImages(&pods[i], template) {
			continue
		}
		before := pods[i].DeepCopy()
		if err := setPodImages(&pods[i], template); err != nil {
			return err
		}
		if pods[i].Annotations == nil {
			pods[i].Annotations = map[string]string{}
		}
		pods[i].Annotations[revisionAnnotation] = imageRevision(template)
		if err := c.Patch(ctx, &pods[i], client.MergeFrom(before)); err != nil {
			return fmt.Errorf("patch Pod %s/%s: %w", pods[i].Namespace, pods[i].Name, err)
		}
		inFlight++
	}
	return nil
}

func setPodImages(pod *corev1.Pod, template *corev1.PodTemplateSpec) error {
	if err := setContainerImages(pod.Spec.InitContainers, template.Spec.InitContainers); err != nil {
		return fmt.Errorf("init containers: %w", err)
	}
	if err := setContainerImages(pod.Spec.Containers, template.Spec.Containers); err != nil {
		return fmt.Errorf("containers: %w", err)
	}
	return nil
}

func setContainerImages(current, desired []corev1.Container) error {
	byName := make(map[string]int, len(current))
	for i := range current {
		byName[current[i].Name] = i
	}
	for i := range desired {
		index, ok := byName[desired[i].Name]
		if !ok {
			return fmt.Errorf("container %q is missing from Pod", desired[i].Name)
		}
		current[index].Image = desired[i].Image
	}
	return nil
}

func podSpecHasImages(pod *corev1.Pod, template *corev1.PodTemplateSpec) bool {
	return podContainersHaveImages(pod.Spec.InitContainers, template.Spec.InitContainers) &&
		podContainersHaveImages(pod.Spec.Containers, template.Spec.Containers)
}

func podContainersHaveImages(current, desired []corev1.Container) bool {
	images := make(map[string]string, len(current))
	for i := range current {
		images[current[i].Name] = current[i].Image
	}
	for i := range desired {
		if images[desired[i].Name] != desired[i].Image {
			return false
		}
	}
	return true
}

func podReadyAndObserved(pod *corev1.Pod, template *corev1.PodTemplateSpec) bool {
	if !isPodReady(pod) {
		return false
	}
	return statusesHaveImages(pod.Status.InitContainerStatuses, template.Spec.InitContainers) &&
		statusesHaveImages(pod.Status.ContainerStatuses, template.Spec.Containers)
}

func isPodReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		condition := pod.Status.Conditions[i]
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func statusesHaveImages(statuses []corev1.ContainerStatus, desired []corev1.Container) bool {
	byName := make(map[string]corev1.ContainerStatus, len(statuses))
	for i := range statuses {
		byName[statuses[i].Name] = statuses[i]
	}
	for i := range desired {
		status, ok := byName[desired[i].Name]
		if !ok || normalizeImage(status.Image) != normalizeImage(desired[i].Image) {
			return false
		}
	}
	return true
}

func normalizeImage(image string) string {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return image
	}
	return reference.FamiliarString(named)
}

func allPods(_ corev1.Pod) bool {
	return true
}

func statefulSetEligible(partition int32) func(corev1.Pod) bool {
	return func(pod corev1.Pod) bool {
		ordinal, err := podOrdinal(pod.Name)
		return err == nil && int32(ordinal) >= partition
	}
}

func statefulSetOrder(left, right corev1.Pod) bool {
	leftOrdinal, leftErr := podOrdinal(left.Name)
	rightOrdinal, rightErr := podOrdinal(right.Name)
	if leftErr != nil || rightErr != nil {
		return left.Name > right.Name
	}
	return leftOrdinal > rightOrdinal
}

func podOrdinal(name string) (int64, error) {
	index := strings.LastIndexByte(name, '-')
	if index < 0 {
		return 0, fmt.Errorf("pod name %q has no ordinal", name)
	}
	return strconv.ParseInt(name[index+1:], 10, 32)
}

func inPlaceStatus(progress podProgress) *appsv1alpha1.InPlaceUpdateStatus {
	return &appsv1alpha1.InPlaceUpdateStatus{
		Revision:             progress.Revision,
		UpdatedReplicas:      progress.Updated,
		ReadyUpdatedReplicas: progress.Ready,
	}
}

func podWatch(kind string) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
		if object.GetAnnotations()[ownerKindAnnotation] != kind {
			return nil
		}
		name := object.GetAnnotations()[ownerNameAnnotation]
		if name == "" {
			return nil
		}
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: name},
		}}
	})
}

func requeueWhileUpdating(progress podProgress) ctrl.Result {
	if progress.Updated < progress.Total || progress.Ready < progress.Total {
		return ctrl.Result{RequeueAfter: 2 * time.Second}
	}
	return ctrl.Result{}
}

func ignoreNotFound(err error) (ctrl.Result, error) {
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, err
}
