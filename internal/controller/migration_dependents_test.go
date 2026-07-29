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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var _ = Describe("Migration dependents", func() {
	const (
		workload          = "workload"
		differentWorkload = "other"
	)

	DescribeTable(
		"retargets supported workload references",
		func(reference migrationDependentReference, target map[string]any) {
			dependent := &unstructured.Unstructured{Object: map[string]any{
				referenceSpecField: map[string]any{
					reference.referencePath[1]: target,
				},
			}}

			changed, err := retargetMigrationDependent(
				dependent,
				reference,
				workload,
				nativeDeploymentAPIVersion,
				churnlessDeploymentAPIVersion,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeTrue())
			apiVersion, found, err := unstructured.NestedString(
				dependent.Object,
				append(reference.referencePath, referenceAPIVersionField)...,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(apiVersion).To(Equal(churnlessDeploymentAPIVersion))
			kind, found, err := unstructured.NestedString(
				dependent.Object,
				append(reference.referencePath, referenceKindField)...,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(kind).To(Equal(deploymentKind))
		},
		Entry(
			"for an HPA",
			migrationDependentReferences[2],
			map[string]any{
				referenceAPIVersionField: nativeDeploymentAPIVersion,
				referenceKindField:       deploymentKind,
				referenceNameField:       workload,
			},
		),
		Entry(
			"for a VPA",
			migrationDependentReferences[1],
			map[string]any{
				referenceAPIVersionField: nativeDeploymentAPIVersion,
				referenceKindField:       deploymentKind,
				referenceNameField:       workload,
			},
		),
		Entry(
			"for a KEDA ScaledObject with defaulted target type",
			migrationDependentReferences[0],
			map[string]any{referenceNameField: workload},
		),
	)

	It("leaves references to another workload unchanged", func() {
		reference := migrationDependentReferences[0]
		dependent := &unstructured.Unstructured{Object: map[string]any{
			referenceSpecField: map[string]any{
				scaleTargetRefField: map[string]any{
					referenceNameField: differentWorkload,
				},
			},
		}}

		changed, err := retargetMigrationDependent(
			dependent,
			reference,
			workload,
			nativeDeploymentAPIVersion,
			churnlessDeploymentAPIVersion,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())
		Expect(dependent.Object[referenceSpecField].(map[string]any)[scaleTargetRefField]).
			To(Equal(map[string]any{referenceNameField: differentWorkload}))
	})
})
