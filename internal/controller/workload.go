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
	"maps"
	"slices"
	"time"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/kubecompat"
	"github.com/hanlins/churnless/internal/workloadmeta"
)

const (
	revisionAnnotation              = "churnless.io/in-place-revision"
	managedLabelKeysAnnotation      = "churnless.io/managed-template-label-keys"
	managedAnnotationKeysAnnotation = "churnless.io/managed-template-annotation-keys"
	inPlaceResourcesAnnotation      = "churnless.io/in-place-resources"
	inPlaceResourcesBestEffort      = "best-effort"
	redeployAnnotation              = "churnless.io/redeploy-at"
	kubectlRestartedAtAnnotation    = workloadmeta.KubectlRestartedAtAnnotation
	podResizeSubresource            = "resize"
)

type podProgress struct {
	Revision string
	Updated  int32
	Ready    int32
	Total    int32
}

type mutablePodPolicy struct {
	resizeResources bool
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

func mutablePodPolicyFor(annotations map[string]string) mutablePodPolicy {
	return mutablePodPolicy{
		resizeResources: annotations[inPlaceResourcesAnnotation] == inPlaceResourcesBestEffort,
	}
}

func (p mutablePodPolicy) resourcePolicy() string {
	if p.resizeResources {
		return inPlaceResourcesBestEffort
	}
	return ""
}

func (p mutablePodPolicy) applyResourcePolicy(annotations map[string]string) {
	if p.resizeResources {
		annotations[inPlaceResourcesAnnotation] = inPlaceResourcesBestEffort
	} else {
		delete(annotations, inPlaceResourcesAnnotation)
	}
}

func (p mutablePodPolicy) clearFromStructural(template *corev1.PodTemplateSpec) {
	template.Labels = nil
	template.Annotations = nil
	clearImages(template)
	if p.resizeResources {
		clearResizableResources(template)
	}
}

func (p mutablePodPolicy) revision(template *corev1.PodTemplateSpec) string {
	type revisionContainer struct {
		Name      string                       `json:"name"`
		Image     string                       `json:"image"`
		Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
		Init      bool                         `json:"init,omitempty"`
	}
	containers := make(
		[]revisionContainer,
		0,
		len(template.Spec.InitContainers)+len(template.Spec.Containers),
	)
	for _, current := range template.Spec.InitContainers {
		containers = append(containers, revisionContainer{
			Name: current.Name, Image: current.Image, Init: true,
		})
	}
	for _, current := range template.Spec.Containers {
		item := revisionContainer{
			Name:  current.Name,
			Image: current.Image,
		}
		if p.resizeResources {
			resources := resizableResources(current.Resources)
			item.Resources = &resources
		}
		containers = append(containers, item)
	}
	clean := cleanTemplate(template)
	data, _ := json.Marshal(struct {
		Labels      map[string]string   `json:"labels,omitempty"`
		Annotations map[string]string   `json:"annotations,omitempty"`
		Containers  []revisionContainer `json:"containers"`
	}{
		Labels:      clean.Labels,
		Annotations: clean.Annotations,
		Containers:  containers,
	})
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:8])
}

func cleanTemplate(template *corev1.PodTemplateSpec) *corev1.PodTemplateSpec {
	clean := template.DeepCopy()
	delete(clean.Labels, structuralRevisionLabel)
	if len(clean.Labels) == 0 {
		clean.Labels = nil
	}
	delete(clean.Annotations, revisionAnnotation)
	delete(clean.Annotations, managedLabelKeysAnnotation)
	delete(clean.Annotations, managedAnnotationKeysAnnotation)
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

func clearResizableResources(template *corev1.PodTemplateSpec) {
	for i := range template.Spec.Containers {
		resources := &template.Spec.Containers[i].Resources
		delete(resources.Requests, corev1.ResourceCPU)
		delete(resources.Requests, corev1.ResourceMemory)
		delete(resources.Limits, corev1.ResourceCPU)
		delete(resources.Limits, corev1.ResourceMemory)
		if len(resources.Requests) == 0 {
			resources.Requests = nil
		}
		if len(resources.Limits) == 0 {
			resources.Limits = nil
		}
	}
}

func calculateProgress(
	pods []corev1.Pod,
	template *corev1.PodTemplateSpec,
	eligible func(corev1.Pod) bool,
	policy mutablePodPolicy,
) podProgress {
	progress := podProgress{Revision: policy.revision(template)}
	for i := range pods {
		if !eligible(pods[i]) {
			continue
		}
		progress.Total++
		if !policy.specMatches(&pods[i], template) {
			continue
		}
		progress.Updated++
		if policy.readyAndObserved(&pods[i], template) {
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
	policy mutablePodPolicy,
) error {
	if parallelism < 1 {
		return nil
	}
	inFlight := 0
	for i := range pods {
		if eligible(pods[i]) &&
			!policy.resizeInfeasible(&pods[i]) &&
			policy.specMatches(&pods[i], template) &&
			!policy.readyAndObserved(&pods[i], template) {
			inFlight++
		}
	}

	slices.SortStableFunc(pods, func(left, right corev1.Pod) int {
		switch {
		case left.CreationTimestamp.Before(&right.CreationTimestamp):
			return -1
		case right.CreationTimestamp.Before(&left.CreationTimestamp):
			return 1
		default:
			return 0
		}
	})
	for i := range pods {
		if inFlight >= parallelism {
			break
		}
		if !eligible(pods[i]) {
			continue
		}
		if policy.resizeInfeasible(&pods[i]) {
			if err := c.Delete(ctx, &pods[i]); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf(
					"replace Pod %s/%s after infeasible resize: %w",
					pods[i].Namespace,
					pods[i].Name,
					err,
				)
			}
			inFlight++
			continue
		}
		if policy.specMatches(&pods[i], template) {
			continue
		}

		before := pods[i].DeepCopy()
		if err := setPodImages(&pods[i], template); err != nil {
			return err
		}
		setPodMetadata(&pods[i], template)
		pods[i].Annotations[revisionAnnotation] = policy.revision(template)
		if err := c.Patch(ctx, &pods[i], client.MergeFrom(before)); err != nil {
			return fmt.Errorf("patch Pod %s/%s: %w", pods[i].Namespace, pods[i].Name, err)
		}

		if policy.resizeResources && !podHasResizableResources(&pods[i], template) {
			if err := setPodResizableResources(&pods[i], template); err != nil {
				return err
			}
			if err := c.SubResource(podResizeSubresource).Update(ctx, &pods[i]); err != nil {
				if !fallbackFromResizeError(err) {
					return fmt.Errorf(
						"resize Pod %s/%s: %w",
						pods[i].Namespace,
						pods[i].Name,
						err,
					)
				}
				if deleteErr := c.Delete(ctx, &pods[i]); client.IgnoreNotFound(deleteErr) != nil {
					return fmt.Errorf(
						"replace Pod %s/%s after resize rejection: %w",
						pods[i].Namespace,
						pods[i].Name,
						deleteErr,
					)
				}
			}
		}
		inFlight++
	}
	return nil
}

func (p mutablePodPolicy) resizeInfeasible(pod *corev1.Pod) bool {
	return p.resizeResources && pod.Status.Resize == corev1.PodResizeStatusInfeasible
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

func setPodMetadata(pod *corev1.Pod, template *corev1.PodTemplateSpec) {
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	desiredManagedLabels := managedTemplateLabelKeys(template.Labels)
	for _, key := range decodeManagedKeys(pod.Annotations[managedLabelKeysAnnotation]) {
		if _, ok := desiredManagedLabels[key]; !ok {
			delete(pod.Labels, key)
		}
	}
	maps.Copy(pod.Labels, template.Labels)

	desiredManagedAnnotations := managedTemplateAnnotationKeys(template.Annotations)
	for _, key := range decodeManagedKeys(pod.Annotations[managedAnnotationKeysAnnotation]) {
		if _, ok := desiredManagedAnnotations[key]; !ok {
			delete(pod.Annotations, key)
		}
	}
	maps.Copy(pod.Annotations, template.Annotations)

	pod.Annotations[managedLabelKeysAnnotation] = encodeManagedKeys(desiredManagedLabels)
	pod.Annotations[managedAnnotationKeysAnnotation] = encodeManagedKeys(desiredManagedAnnotations)
}

func podMetadataMatches(pod *corev1.Pod, template *corev1.PodTemplateSpec) bool {
	for key, value := range template.Labels {
		if pod.Labels[key] != value {
			return false
		}
	}
	for key, value := range template.Annotations {
		if pod.Annotations[key] != value {
			return false
		}
	}

	desiredManagedLabels := managedTemplateLabelKeys(template.Labels)
	for _, key := range decodeManagedKeys(pod.Annotations[managedLabelKeysAnnotation]) {
		if _, ok := desiredManagedLabels[key]; !ok {
			return false
		}
	}
	desiredManagedAnnotations := managedTemplateAnnotationKeys(template.Annotations)
	for _, key := range decodeManagedKeys(pod.Annotations[managedAnnotationKeysAnnotation]) {
		if _, ok := desiredManagedAnnotations[key]; !ok {
			return false
		}
	}
	return pod.Annotations[managedLabelKeysAnnotation] == encodeManagedKeys(desiredManagedLabels) &&
		pod.Annotations[managedAnnotationKeysAnnotation] == encodeManagedKeys(desiredManagedAnnotations)
}

func managedTemplateLabelKeys(values map[string]string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for key := range values {
		if key != structuralRevisionLabel {
			result[key] = struct{}{}
		}
	}
	return result
}

func managedTemplateAnnotationKeys(values map[string]string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for key := range values {
		switch key {
		case revisionAnnotation, managedLabelKeysAnnotation, managedAnnotationKeysAnnotation:
			continue
		default:
			result[key] = struct{}{}
		}
	}
	return result
}

func encodeManagedKeys(values map[string]struct{}) string {
	keys := slices.Sorted(maps.Keys(values))
	data, _ := json.Marshal(keys)
	return string(data)
}

func decodeManagedKeys(value string) []string {
	var keys []string
	if err := json.Unmarshal([]byte(value), &keys); err != nil {
		return nil
	}
	return keys
}

func (p mutablePodPolicy) specMatches(
	pod *corev1.Pod,
	template *corev1.PodTemplateSpec,
) bool {
	return podMetadataMatches(pod, template) &&
		podSpecHasImages(pod, template) &&
		(!p.resizeResources || podHasResizableResources(pod, template))
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

func resizableResources(resources corev1.ResourceRequirements) corev1.ResourceRequirements {
	result := corev1.ResourceRequirements{}
	for name, quantity := range resources.Requests {
		if name == corev1.ResourceCPU || name == corev1.ResourceMemory {
			if result.Requests == nil {
				result.Requests = corev1.ResourceList{}
			}
			result.Requests[name] = quantity.DeepCopy()
		}
	}
	for name, quantity := range resources.Limits {
		if name == corev1.ResourceCPU || name == corev1.ResourceMemory {
			if result.Limits == nil {
				result.Limits = corev1.ResourceList{}
			}
			result.Limits[name] = quantity.DeepCopy()
		}
	}
	return result
}

func podHasResizableResources(
	pod *corev1.Pod,
	template *corev1.PodTemplateSpec,
) bool {
	currentByName := make(map[string]corev1.ResourceRequirements, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		currentByName[pod.Spec.Containers[i].Name] = resizableResources(
			pod.Spec.Containers[i].Resources,
		)
	}
	for i := range template.Spec.Containers {
		current, ok := currentByName[template.Spec.Containers[i].Name]
		if !ok || !apiequality.Semantic.DeepEqual(
			current,
			resizableResources(template.Spec.Containers[i].Resources),
		) {
			return false
		}
	}
	return true
}

func setPodResizableResources(
	pod *corev1.Pod,
	template *corev1.PodTemplateSpec,
) error {
	currentByName := make(map[string]int, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		currentByName[pod.Spec.Containers[i].Name] = i
	}
	for i := range template.Spec.Containers {
		index, ok := currentByName[template.Spec.Containers[i].Name]
		if !ok {
			return fmt.Errorf(
				"container %q is missing from Pod during resize",
				template.Spec.Containers[i].Name,
			)
		}
		current := &pod.Spec.Containers[index].Resources
		delete(current.Requests, corev1.ResourceCPU)
		delete(current.Requests, corev1.ResourceMemory)
		delete(current.Limits, corev1.ResourceCPU)
		delete(current.Limits, corev1.ResourceMemory)
		desired := resizableResources(template.Spec.Containers[i].Resources)
		if current.Requests == nil && len(desired.Requests) > 0 {
			current.Requests = corev1.ResourceList{}
		}
		if current.Limits == nil && len(desired.Limits) > 0 {
			current.Limits = corev1.ResourceList{}
		}
		for name, quantity := range desired.Requests {
			current.Requests[name] = quantity.DeepCopy()
		}
		for name, quantity := range desired.Limits {
			current.Limits[name] = quantity.DeepCopy()
		}
		if len(current.Requests) == 0 {
			current.Requests = nil
		}
		if len(current.Limits) == 0 {
			current.Limits = nil
		}
	}
	return nil
}

func fallbackFromResizeError(err error) bool {
	switch apierrors.ReasonForError(err) {
	case metav1.StatusReasonBadRequest,
		metav1.StatusReasonForbidden,
		metav1.StatusReasonInvalid,
		metav1.StatusReasonMethodNotAllowed,
		metav1.StatusReasonNotFound:
		return true
	default:
		return false
	}
}

func (p mutablePodPolicy) readyAndObserved(
	pod *corev1.Pod,
	template *corev1.PodTemplateSpec,
) bool {
	if !p.specMatches(pod, template) {
		return false
	}
	if p.resizeResources && pod.Status.Resize != "" {
		return false
	}
	if !kubecompat.IsPodReady(pod) {
		return false
	}
	return statusesHaveImages(pod.Status.InitContainerStatuses, template.Spec.InitContainers) &&
		statusesHaveImages(pod.Status.ContainerStatuses, template.Spec.Containers)
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

func inPlaceStatus(progress podProgress) *appsv1alpha1.InPlaceUpdateStatus {
	return &appsv1alpha1.InPlaceUpdateStatus{
		Revision:             progress.Revision,
		UpdatedReplicas:      progress.Updated,
		ReadyUpdatedReplicas: progress.Ready,
	}
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
