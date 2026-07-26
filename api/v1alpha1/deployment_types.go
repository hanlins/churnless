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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InPlaceUpdateStatus describes the image revision applied directly to Pods.
type InPlaceUpdateStatus struct {
	// revision is a deterministic hash of the desired container images.
	// +optional
	Revision string `json:"revision,omitempty"`

	// updatedReplicas is the number of Pods whose specs contain the desired images.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// readyUpdatedReplicas is the number of updated Pods that are ready and
	// observed by the kubelet with the desired images.
	// +optional
	ReadyUpdatedReplicas int32 `json:"readyUpdatedReplicas,omitempty"`
}

// DeploymentSpec is wire-compatible with apps/v1 DeploymentSpec.
type DeploymentSpec struct {
	appsv1.DeploymentSpec `json:",inline"`
}

// DeploymentStatus includes every apps/v1 DeploymentStatus field.
type DeploymentStatus struct {
	appsv1.DeploymentStatus `json:",inline"`

	// selector is the label selector used by the scale subresource and HPA.
	// +optional
	Selector string `json:"selector,omitempty"`

	// inPlace describes progress of the current image update.
	// +optional
	InPlace *InPlaceUpdateStatus `json:"inPlace,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Up-to-date",type=integer,JSONPath=".status.updatedReplicas"
// +kubebuilder:printcolumn:name="Available",type=integer,JSONPath=".status.availableReplicas"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// Deployment is the Schema for the deployments API.
type Deployment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Deployment.
	// +required
	Spec DeploymentSpec `json:"spec"`

	// status defines the observed state of Deployment.
	// +optional
	Status DeploymentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DeploymentList contains a list of Deployment.
type DeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Deployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Deployment{}, &DeploymentList{})
		return nil
	})
}
