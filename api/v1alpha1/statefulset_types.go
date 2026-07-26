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

// StatefulSetSpec is wire-compatible with apps/v1 StatefulSetSpec.
type StatefulSetSpec struct {
	appsv1.StatefulSetSpec `json:",inline"`
}

// StatefulSetStatus includes every apps/v1 StatefulSetStatus field.
type StatefulSetStatus struct {
	appsv1.StatefulSetStatus `json:",inline"`

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
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=".status.currentReplicas"
// +kubebuilder:printcolumn:name="Updated",type=integer,JSONPath=".status.updatedReplicas"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// StatefulSet is the Schema for the statefulsets API.
type StatefulSet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of StatefulSet.
	// +required
	Spec StatefulSetSpec `json:"spec"`

	// status defines the observed state of StatefulSet.
	// +optional
	Status StatefulSetStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// StatefulSetList contains a list of StatefulSet.
type StatefulSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []StatefulSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &StatefulSet{}, &StatefulSetList{})
		return nil
	})
}
