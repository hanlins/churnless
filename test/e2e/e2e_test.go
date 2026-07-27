//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/test/utils"
)

// namespace where the project is deployed in
const namespace = "churnless-system"

// serviceAccountName created for the project
const serviceAccountName = "churnless-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "churnless-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "churnless-metrics-binding"

const kubectlRestartAnnotationKey = "kubectl.kubernetes.io/restartedAt"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should expose distinct resource names, short names, and category", func() {
			By("checking the Churnless discovery document")
			cmd := exec.Command("kubectl", "get", "--raw", "/apis/churnless.io/v1alpha1")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			var discovery struct {
				GroupVersion string `json:"groupVersion"`
				Resources    []struct {
					Name       string   `json:"name"`
					Kind       string   `json:"kind"`
					ShortNames []string `json:"shortNames"`
					Categories []string `json:"categories"`
				} `json:"resources"`
			}
			Expect(json.Unmarshal([]byte(output), &discovery)).To(Succeed())
			Expect(discovery.GroupVersion).To(Equal("churnless.io/v1alpha1"))

			resources := make(map[string]struct {
				Kind       string
				ShortNames []string
				Categories []string
			})
			for _, resource := range discovery.Resources {
				resources[resource.Name] = struct {
					Kind       string
					ShortNames []string
					Categories []string
				}{
					Kind:       resource.Kind,
					ShortNames: resource.ShortNames,
					Categories: resource.Categories,
				}
			}
			Expect(resources).To(HaveKeyWithValue(
				"deployments",
				SatisfyAll(
					HaveField("Kind", "Deployment"),
					HaveField("ShortNames", ContainElement("cdeploy")),
					HaveField("Categories", ContainElement("churnless")),
				),
			))
			Expect(resources).To(HaveKeyWithValue(
				"replicasets",
				SatisfyAll(
					HaveField("Kind", "ReplicaSet"),
					HaveField("ShortNames", ContainElement("chrs")),
					HaveField("Categories", ContainElement("churnless")),
				),
			))

			By("resolving the Churnless short names and category")
			for _, resource := range []string{"cdeploy", "chrs", "churnless"} {
				cmd = exec.Command("kubectl", "get", resource, "--all-namespaces", "-o", "name")
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to resolve %s", resource)
			}

			By("ensuring the native short names still resolve to apps/v1 resources")
			cmd = exec.Command("kubectl", "get", "deploy", "-n", namespace, "-o", "name")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(ContainSubstring("deployment.apps/churnless-controller-manager"))

			cmd = exec.Command("kubectl", "get", "rs", "-n", namespace, "-o", "name")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(ContainSubstring("replicaset.apps/churnless-controller-manager-"))
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			cmd := exec.Command(
				"kubectl",
				"delete",
				"clusterrolebinding",
				metricsRoleBindingName,
				"--ignore-not-found",
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				cmd := exec.Command(
					"kubectl",
					"delete",
					"clusterrolebinding",
					metricsRoleBindingName,
					"--ignore-not-found",
				)
				_, _ = utils.Run(cmd)
			})

			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd = exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=churnless-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			By("waiting for the webhook service endpoints to be ready")
			verifyWebhookEndpointsReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpointslices.discovery.k8s.io", "-n", namespace,
					"-l", "kubernetes.io/service-name=churnless-webhook-service",
					"-o", "jsonpath={range .items[*]}{range .endpoints[*]}{.addresses[*]}{end}{end}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Webhook endpoints should exist")
				g.Expect(output).ShouldNot(BeEmpty(), "Webhook endpoints not yet ready")
			}
			Eventually(verifyWebhookEndpointsReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying the mutating webhook server is ready")
			verifyMutatingWebhookReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "mutatingwebhookconfigurations.admissionregistration.k8s.io",
					"churnless-mutating-webhook-configuration",
					"-o", "jsonpath={.webhooks[0].clientConfig.caBundle}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "MutatingWebhookConfiguration should exist")
				g.Expect(output).ShouldNot(BeEmpty(), "Mutating webhook CA bundle not yet injected")
			}
			Eventually(verifyMutatingWebhookReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying the validating webhook server is ready")
			verifyValidatingWebhookReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "validatingwebhookconfigurations.admissionregistration.k8s.io",
					"churnless-validating-webhook-configuration",
					"-o", "jsonpath={.webhooks[0].clientConfig.caBundle}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "ValidatingWebhookConfiguration should exist")
				g.Expect(output).ShouldNot(BeEmpty(), "Validating webhook CA bundle not yet injected")
			}
			Eventually(verifyValidatingWebhookReady, 3*time.Minute, time.Second).Should(Succeed())

			By("waiting additional time for webhook server to stabilize")
			time.Sleep(5 * time.Second)

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -sS -k -o /dev/null -w '%%{http_code}' -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("200"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		It("should update Deployment Pods in place and expose the scale subresource", func() {
			const (
				workload = "deployment-sample"
				oldImage = "nginx:1.27-alpine"
				newImage = "nginx:1.28-alpine"
			)
			DeferCleanup(func() {
				cmd := exec.Command(
					"kubectl",
					"delete",
					"deployment.churnless.io",
					workload,
					"--ignore-not-found",
				)
				_, _ = utils.Run(cmd)
			})

			By("creating a custom Deployment")
			cmd := exec.Command(
				"kubectl",
				"apply",
				"-f",
				"config/samples/churnless_v1alpha1_deployment.yaml",
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			before := eventuallyDeploymentPods(workload, 2, oldImage)
			replicaSetBefore := eventuallyDeploymentReplicaSet(workload, oldImage)

			By("resolving Churnless resources through their ergonomic names")
			cmd = exec.Command("kubectl", "get", "cdeploy/"+workload, "-o", "name")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("deployment.churnless.io/" + workload))

			cmd = exec.Command(
				"kubectl",
				"get",
				"chrs/"+replicaSetBefore.Name,
				"-o",
				"name",
			)
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("replicaset.churnless.io/" + replicaSetBefore.Name))

			cmd = exec.Command(
				"kubectl",
				"get",
				"churnless",
				"-o",
				"name",
			)
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(utils.GetNonEmptyLines(output)).To(ConsistOf(
				"deployment.churnless.io/"+workload,
				"replicaset.churnless.io/"+replicaSetBefore.Name,
			))

			By("changing only the image")
			cmd = exec.Command(
				"kubectl",
				"patch",
				"deployment.churnless.io",
				workload,
				"--type=merge",
				"-p",
				fmt.Sprintf(`{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"%s","ports":[{"containerPort":80}],"resources":{"requests":{"cpu":"100m"}}}]}}}}`, newImage),
			)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			after := eventuallyDeploymentPods(workload, 2, newImage)
			replicaSetAfter := eventuallyDeploymentReplicaSet(workload, newImage)

			for name, previous := range before {
				current, ok := after[name]
				Expect(ok).To(BeTrue(), "Pod %s was replaced", name)
				Expect(current.UID).To(Equal(previous.UID), "Pod %s UID changed", name)
				Expect(current.IP).To(Equal(previous.IP), "Pod %s IP changed", name)
			}
			Expect(replicaSetAfter.Name).To(Equal(replicaSetBefore.Name))
			Expect(replicaSetAfter.UID).To(Equal(replicaSetBefore.UID))
			cmd = exec.Command(
				"kubectl",
				"get",
				"deployment.apps",
				workload,
			)
			_, err = utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "a native shadow Deployment must not exist")
			cmd = exec.Command(
				"kubectl",
				"get",
				"replicasets.apps",
				"-l",
				"app="+workload,
				"-o",
				"jsonpath={.items[*].metadata.name}",
			)
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(BeEmpty(), "a native shadow ReplicaSet must not exist")

			By("using the standard scale subresource that HPA uses")
			cmd = exec.Command(
				"kubectl",
				"scale",
				"deployment.churnless.io/"+workload,
				"--replicas=3",
			)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			scaled := eventuallyDeploymentPods(workload, 3, newImage)
			for name, previous := range before {
				Expect(scaled[name].UID).To(Equal(previous.UID))
				Expect(scaled[name].IP).To(Equal(previous.IP))
			}

			By("letting an HPA scale the custom Deployment through /scale")
			hpa := fmt.Sprintf(`apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: %s
spec:
  scaleTargetRef:
    apiVersion: churnless.io/v1alpha1
    kind: Deployment
    name: %s
  minReplicas: 1
  maxReplicas: 3
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 0
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 80
`, workload, workload)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(hpa)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "hpa", workload, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			Eventually(func(g Gomega) {
				cmd := exec.Command(
					"kubectl",
					"get",
					"deployment.churnless.io",
					workload,
					"-o",
					"jsonpath={.spec.replicas}",
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("1"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())
			eventuallyDeploymentPods(workload, 1, newImage)
		})

		It("should take over a native Deployment and hand it back without replacing Pods", func() {
			const (
				workload = "migration-switch"
				image    = "nginx:1.28-alpine"
				replicas = 2
			)
			DeferCleanup(func() {
				for _, resource := range []string{
					"horizontalpodautoscaler.autoscaling/" + workload,
					"deployment.apps/" + workload,
					"deployment.churnless.io/" + workload,
				} {
					cmd := exec.Command("kubectl", "delete", resource, "--ignore-not-found")
					_, _ = utils.Run(cmd)
				}
			})

			By("creating a complete native Deployment")
			Expect(applyMigrationDeployment(workload, replicas, image)).To(Succeed())
			nativePods := eventuallyOwnedDeploymentPods(
				workload,
				replicas,
				image,
				appsv1.SchemeGroupVersion.String(),
			)

			By("creating an HPA that targets the native Deployment")
			Expect(applyMigrationHPA(workload, replicas)).To(Succeed())
			eventuallyHPATarget(workload, appsv1.SchemeGroupVersion.String())

			By("requesting Churnless takeover with an annotation")
			cmd := exec.Command(
				"kubectl",
				"annotate",
				"deployment.apps/"+workload,
				"churnless.io/takeover=true",
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			takenOverPods := eventuallyOwnedDeploymentPods(
				workload,
				replicas,
				image,
				appsv1alpha1.GroupVersion.String(),
			)
			expectPodIdentityRetained(nativePods, takenOverPods)
			eventuallyHPATarget(workload, appsv1alpha1.GroupVersion.String())
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment.apps/"+workload)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred())
			}).Should(Succeed())

			By("requesting emergency handoff to the native controller")
			cmd = exec.Command(
				"kubectl",
				"annotate",
				"deployment.churnless.io/"+workload,
				"churnless.io/handoff=true",
			)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			handedOffPods := eventuallyOwnedDeploymentPods(
				workload,
				replicas,
				image,
				appsv1.SchemeGroupVersion.String(),
			)
			expectPodIdentityRetained(takenOverPods, handedOffPods)
			eventuallyHPATarget(workload, appsv1.SchemeGroupVersion.String())
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment.churnless.io/"+workload)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred())
			}).Should(Succeed())
		})

		It("should enforce RollingUpdate availability and surge limits", func() {
			const (
				workload = "rolling-policy"
				oldImage = "nginx:1.27-alpine"
				newImage = "nginx:1.28-alpine"
				replicas = 4
			)
			defer deleteChurnlessDeployment(workload)

			By("creating a workload with absolute RollingUpdate fenceposts")
			Expect(applyPolicyDeployment(
				workload,
				replicas,
				appsv1.RollingUpdateDeploymentStrategyType,
				oldImage,
			)).To(Succeed())
			before := eventuallyDeploymentPods(workload, replicas, oldImage)
			eventuallyChurnlessDeploymentComplete(workload, replicas)

			By("starting a structural rollout")
			Expect(patchStructuralRevision(workload, newImage)).To(Succeed())

			// This mirrors the availability and surge invariants exercised by
			// Kubernetes v1.36's Deployment RollingUpdate controller tests.
			after := watchRollingUpdatePolicy(
				workload,
				newImage,
				replicas,
				replicas+1,
				replicas-1,
			)
			Expect(retainedIdentities(before, after)).To(BeZero(),
				"a structural rollout unexpectedly retained Pod identity")
		})

		It("should not overlap old and new Pods during a Recreate rollout", func() {
			const (
				workload = "recreate-policy"
				oldImage = "nginx:1.27-alpine"
				newImage = "nginx:1.28-alpine"
				replicas = 2
			)
			defer deleteChurnlessDeployment(workload)

			By("creating a Recreate workload with a visible termination window")
			Expect(applyPolicyDeployment(
				workload,
				replicas,
				appsv1.RecreateDeploymentStrategyType,
				oldImage,
			)).To(Succeed())
			eventuallyDeploymentPods(workload, replicas, oldImage)
			eventuallyChurnlessDeploymentComplete(workload, replicas)

			By("starting a structural Recreate rollout")
			Expect(patchStructuralRevision(workload, newImage)).To(Succeed())

			// Kubernetes's Recreate conformance test watches this exact
			// invariant: no new Pod may run while an old Pod is still active.
			watchRecreatePolicy(workload, oldImage, newImage, replicas)
		})

		It("should update mutable fields in place and rebuild on fallback, restart, and immutable changes", func() {
			const (
				workload = "mutable-fields"
				image    = "nginx:1.28-alpine"
			)
			defer deleteChurnlessDeployment(workload)

			By("creating an opt-in best-effort resource workload")
			Expect(applyMutableFieldsDeployment(workload, image)).To(Succeed())
			expectedPod := mutablePodExpectation{
				workload:      workload,
				image:         image,
				cpuRequest:    "100m",
				memoryRequest: "32Mi",
			}
			initial := eventuallyMutablePod(expectedPod)
			initialReplicaSet := eventuallyDeploymentReplicaSet(workload, image)

			By("changing Pod-template labels and annotations")
			Expect(patchMutableMetadata(workload)).To(Succeed())
			expectedPod.labelValue = "two"
			expectedPod.annotationValue = "two"
			metadataUpdated := eventuallyMutablePod(expectedPod)
			Expect(metadataUpdated.UID).To(Equal(initial.UID))
			Expect(metadataUpdated.Status.PodIP).To(Equal(initial.Status.PodIP))
			Expect(eventuallyDeploymentReplicaSet(workload, image).UID).
				To(Equal(initialReplicaSet.UID))

			By("resizing CPU in place while preserving the Pod QoS class")
			Expect(patchMutableResources(
				workload,
				image,
				"200m",
				"",
				"32Mi",
				"",
			)).To(Succeed())
			expectedPod.cpuRequest = "200m"
			resized := eventuallyMutablePod(expectedPod)
			Expect(resized.UID).To(Equal(initial.UID))
			Expect(resized.Status.PodIP).To(Equal(initial.Status.PodIP))

			By("falling back to replacement when resize would change Pod QoS")
			Expect(patchMutableResources(
				workload,
				image,
				"200m",
				"200m",
				"64Mi",
				"64Mi",
			)).To(Succeed())
			expectedPod.cpuLimit = "200m"
			expectedPod.memoryRequest = "64Mi"
			expectedPod.memoryLimit = "64Mi"
			fallback := eventuallyMutablePod(expectedPod)
			Expect(fallback.UID).NotTo(Equal(resized.UID))
			Expect(eventuallyDeploymentReplicaSet(workload, image).UID).
				To(Equal(initialReplicaSet.UID),
					"best-effort fallback unexpectedly created another ReplicaSet")

			By("triggering redeploy with the standard kubectl restart annotation")
			Expect(kubectlRestartAnnotation(workload)).To(Succeed())
			eventuallyOwnedReplicaSetCount(workload, 2)
			expectedPod.excludedUID = fallback.UID
			restarted := eventuallyMutablePod(expectedPod)
			Expect(restarted.UID).NotTo(Equal(fallback.UID))

			By("changing an immutable container field")
			Expect(patchImmutableField(workload, image)).To(Succeed())
			eventuallyOwnedReplicaSetCount(workload, 3)
			expectedPod.excludedUID = restarted.UID
			immutable := eventuallyMutablePod(expectedPod)
			Expect(immutable.UID).NotTo(Equal(restarted.UID))
		})

		It("should match native admission defaults and critical validation", func() {
			native, err := serverDryRunDeployment("apps/v1", "admission-defaults", true)
			Expect(err).NotTo(HaveOccurred())
			churnless, err := serverDryRunDeployment(
				"churnless.io/v1alpha1",
				"admission-defaults",
				true,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(churnless).To(Equal(native))

			_, nativeErr := serverDryRunDeployment("apps/v1", "admission-invalid", false)
			_, churnlessErr := serverDryRunDeployment(
				"churnless.io/v1alpha1",
				"admission-invalid",
				false,
			)
			Expect(nativeErr).To(HaveOccurred())
			Expect(churnlessErr).To(HaveOccurred())

			nativeReplicaSet, err := serverDryRunReplicaSet(
				"apps/v1",
				"replicaset-admission-defaults",
			)
			Expect(err).NotTo(HaveOccurred())
			churnlessReplicaSet, err := serverDryRunReplicaSet(
				"churnless.io/v1alpha1",
				"replicaset-admission-defaults",
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(churnlessReplicaSet).To(Equal(nativeReplicaSet))
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks
	})
})

func serverDryRunDeployment(
	apiVersion, name string,
	selectorMatches bool,
) (appsv1.DeploymentSpec, error) {
	templateLabel := name
	if !selectorMatches {
		templateLabel = "other"
	}
	manifest := fmt.Sprintf(`apiVersion: %s
kind: Deployment
metadata:
  name: %s
spec:
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      volumes:
      - name: credentials
        secret:
          secretName: credentials
      containers:
      - name: web
        image: nginx:1.28-alpine
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
`, apiVersion, name, name, templateLabel)
	cmd := exec.Command("kubectl", "create", "--dry-run=server", "-f", "-", "-o", "json")
	cmd.Stdin = strings.NewReader(manifest)
	output, err := utils.Run(cmd)
	if err != nil {
		return appsv1.DeploymentSpec{}, err
	}
	var workload struct {
		Spec appsv1.DeploymentSpec `json:"spec"`
	}
	if err := json.Unmarshal([]byte(output), &workload); err != nil {
		return appsv1.DeploymentSpec{}, err
	}
	return workload.Spec, nil
}

func serverDryRunReplicaSet(apiVersion, name string) (appsv1.ReplicaSetSpec, error) {
	manifest := fmt.Sprintf(`apiVersion: %s
kind: ReplicaSet
metadata:
  name: %s
spec:
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      volumes:
      - name: credentials
        secret:
          secretName: credentials
      containers:
      - name: web
        image: nginx:1.28-alpine
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
`, apiVersion, name, name, name)
	cmd := exec.Command("kubectl", "create", "--dry-run=server", "-f", "-", "-o", "json")
	cmd.Stdin = strings.NewReader(manifest)
	output, err := utils.Run(cmd)
	if err != nil {
		return appsv1.ReplicaSetSpec{}, err
	}
	var workload struct {
		Spec appsv1.ReplicaSetSpec `json:"spec"`
	}
	if err := json.Unmarshal([]byte(output), &workload); err != nil {
		return appsv1.ReplicaSetSpec{}, err
	}
	return workload.Spec, nil
}

type podIdentity struct {
	UID string
	IP  string
}

type policyPodSnapshot struct {
	Active         int
	Ready          int
	ByImage        map[string]int
	PresentByImage map[string]int
	RunningByImage map[string]int
	Identities     map[string]podIdentity
}

type replicaSetIdentity struct {
	Name string
	UID  string
}

func eventuallyDeploymentReplicaSet(workload, image string) replicaSetIdentity {
	var result replicaSetIdentity
	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"replicasets.churnless.io",
			"-o",
			"json",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		var list struct {
			Items []struct {
				Metadata struct {
					Name            string `json:"name"`
					UID             string `json:"uid"`
					OwnerReferences []struct {
						Kind string `json:"kind"`
						Name string `json:"name"`
					} `json:"ownerReferences"`
				} `json:"metadata"`
				Spec struct {
					Template struct {
						Spec struct {
							Containers []struct {
								Image string `json:"image"`
							} `json:"containers"`
						} `json:"spec"`
					} `json:"template"`
				} `json:"spec"`
			} `json:"items"`
		}
		g.Expect(json.Unmarshal([]byte(output), &list)).To(Succeed())
		matches := make([]replicaSetIdentity, 0, 1)
		for _, replicaSet := range list.Items {
			for _, owner := range replicaSet.Metadata.OwnerReferences {
				if owner.Kind == "Deployment" && owner.Name == workload {
					g.Expect(replicaSet.Spec.Template.Spec.Containers).NotTo(BeEmpty())
					g.Expect(replicaSet.Spec.Template.Spec.Containers[0].Image).To(Equal(image))
					matches = append(matches, replicaSetIdentity{
						Name: replicaSet.Metadata.Name,
						UID:  replicaSet.Metadata.UID,
					})
				}
			}
		}
		g.Expect(matches).To(HaveLen(1))
		result = matches[0]
	}, 5*time.Minute, 2*time.Second).Should(Succeed())
	return result
}

func eventuallyDeploymentPods(workload string, count int, image string) map[string]podIdentity {
	return eventuallyOwnedDeploymentPods(
		workload,
		count,
		image,
		appsv1alpha1.GroupVersion.String(),
	)
}

func eventuallyOwnedDeploymentPods(
	workload string,
	count int,
	image, ownerAPIVersion string,
) map[string]podIdentity {
	var result map[string]podIdentity
	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"pods",
			"-l",
			"app="+workload,
			"-o",
			"json",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		var list struct {
			Items []struct {
				Metadata struct {
					Name              string  `json:"name"`
					UID               string  `json:"uid"`
					DeletionTimestamp *string `json:"deletionTimestamp"`
					OwnerReferences   []struct {
						APIVersion string `json:"apiVersion"`
						Kind       string `json:"kind"`
						Controller bool   `json:"controller"`
					} `json:"ownerReferences"`
				} `json:"metadata"`
				Spec struct {
					Containers []struct {
						Image string `json:"image"`
					} `json:"containers"`
				} `json:"spec"`
				Status struct {
					PodIP      string `json:"podIP"`
					Conditions []struct {
						Type   string `json:"type"`
						Status string `json:"status"`
					} `json:"conditions"`
				} `json:"status"`
			} `json:"items"`
		}
		g.Expect(json.Unmarshal([]byte(output), &list)).To(Succeed())
		current := make(map[string]podIdentity, len(list.Items))
		for _, pod := range list.Items {
			if pod.Metadata.DeletionTimestamp != nil {
				continue
			}
			g.Expect(pod.Spec.Containers).NotTo(BeEmpty())
			g.Expect(pod.Spec.Containers[0].Image).To(Equal(image))
			g.Expect(pod.Status.PodIP).NotTo(BeEmpty())
			g.Expect(pod.Metadata.OwnerReferences).To(ContainElement(SatisfyAll(
				HaveField("APIVersion", ownerAPIVersion),
				HaveField("Kind", "ReplicaSet"),
				HaveField("Controller", true),
			)))
			ready := false
			for _, condition := range pod.Status.Conditions {
				ready = ready || condition.Type == "Ready" && condition.Status == "True"
			}
			g.Expect(ready).To(BeTrue())
			current[pod.Metadata.Name] = podIdentity{UID: pod.Metadata.UID, IP: pod.Status.PodIP}
		}
		g.Expect(current).To(HaveLen(count))
		result = current
	}, 5*time.Minute, 2*time.Second).Should(Succeed())
	return result
}

func expectPodIdentityRetained(
	before, after map[string]podIdentity,
) {
	Expect(after).To(HaveLen(len(before)))
	for name, previous := range before {
		current, ok := after[name]
		Expect(ok).To(BeTrue(), "Pod %s was replaced", name)
		Expect(current.UID).To(Equal(previous.UID), "Pod %s UID changed", name)
		Expect(current.IP).To(Equal(previous.IP), "Pod %s IP changed", name)
	}
}

func eventuallyChurnlessDeploymentComplete(workload string, replicas int32) {
	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"deployment.churnless.io",
			workload,
			"-o",
			"json",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		var deployment appsv1alpha1.Deployment
		g.Expect(json.Unmarshal([]byte(output), &deployment)).To(Succeed())
		g.Expect(deployment.Status.ObservedGeneration).To(
			BeNumerically(">=", deployment.Generation),
		)
		g.Expect(deployment.Status.Replicas).To(Equal(replicas))
		g.Expect(deployment.Status.UpdatedReplicas).To(Equal(replicas))
		g.Expect(deployment.Status.AvailableReplicas).To(Equal(replicas))
		g.Expect(deployment.Status.InPlace).NotTo(BeNil())
		g.Expect(deployment.Status.InPlace.ReadyUpdatedReplicas).To(Equal(replicas))
	}, 5*time.Minute, 200*time.Millisecond).Should(Succeed())
}

func applyMigrationDeployment(workload string, replicas int, image string) error {
	manifest := fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
spec:
  replicas: %d
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: nginx
        image: %s
        resources:
          requests:
            cpu: 25m
`, workload, replicas, workload, workload, image)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

func applyMigrationHPA(workload string, replicas int) error {
	manifest := fmt.Sprintf(`apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: %s
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: %s
  minReplicas: %d
  maxReplicas: %d
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 80
`, workload, workload, replicas, replicas)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

func eventuallyHPATarget(workload, apiVersion string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"horizontalpodautoscaler.autoscaling/"+workload,
			"-o",
			"jsonpath={.spec.scaleTargetRef.apiVersion}",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal(apiVersion))
	}, 5*time.Minute, 200*time.Millisecond).Should(Succeed())
}

func applyPolicyDeployment(
	workload string,
	replicas int,
	strategy appsv1.DeploymentStrategyType,
	image string,
) error {
	strategyYAML := fmt.Sprintf("  strategy:\n    type: %s\n", strategy)
	if strategy == appsv1.RollingUpdateDeploymentStrategyType {
		strategyYAML += "    rollingUpdate:\n      maxSurge: 1\n      maxUnavailable: 1\n"
	}
	manifest := fmt.Sprintf(`apiVersion: churnless.io/v1alpha1
kind: Deployment
metadata:
  name: %s
spec:
  replicas: %d
%s  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      terminationGracePeriodSeconds: 5
      containers:
      - name: nginx
        image: %s
`, workload, replicas, strategyYAML, workload, workload, image)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

func patchStructuralRevision(workload, image string) error {
	patch := fmt.Sprintf(
		`{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"%s","env":[{"name":"CHURNLESS_E2E_IMMUTABLE","value":"two"}]}]}}}}`,
		image,
	)
	cmd := exec.Command(
		"kubectl",
		"patch",
		"deployment.churnless.io",
		workload,
		"--type=merge",
		"-p",
		patch,
	)
	_, err := utils.Run(cmd)
	return err
}

func applyMutableFieldsDeployment(workload, image string) error {
	manifest := fmt.Sprintf(`apiVersion: churnless.io/v1alpha1
kind: Deployment
metadata:
  name: %s
  annotations:
    churnless.io/in-place-resources: best-effort
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: nginx
        image: %s
        resources:
          requests:
            cpu: 100m
            memory: 32Mi
`, workload, workload, workload, image)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

func patchMutableMetadata(workload string) error {
	cmd := exec.Command(
		"kubectl",
		"patch",
		"deployment.churnless.io",
		workload,
		"--type=merge",
		"-p",
		`{"spec":{"template":{"metadata":{"labels":{"example.com/track":"two"},"annotations":{"example.com/revision":"two"}}}}}`,
	)
	_, err := utils.Run(cmd)
	return err
}

func patchMutableResources(
	workload, image, cpuRequest, cpuLimit, memoryRequest, memoryLimit string,
) error {
	requests := fmt.Sprintf(`"cpu":"%s","memory":"%s"`, cpuRequest, memoryRequest)
	limits := ""
	if cpuLimit != "" || memoryLimit != "" {
		limits = fmt.Sprintf(
			`,"limits":{"cpu":"%s","memory":"%s"}`,
			cpuLimit,
			memoryLimit,
		)
	}
	patch := fmt.Sprintf(
		`{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"%s","resources":{"requests":{%s}%s}}]}}}}`,
		image,
		requests,
		limits,
	)
	cmd := exec.Command(
		"kubectl",
		"patch",
		"deployment.churnless.io",
		workload,
		"--type=merge",
		"-p",
		patch,
	)
	_, err := utils.Run(cmd)
	return err
}

func kubectlRestartAnnotation(workload string) error {
	cmd := exec.Command(
		"kubectl",
		"annotate",
		"deployment.churnless.io/"+workload,
		kubectlRestartAnnotationKey+"="+time.Now().UTC().Format(time.RFC3339Nano),
		"--overwrite",
	)
	_, err := utils.Run(cmd)
	return err
}

func patchImmutableField(workload, image string) error {
	patch := fmt.Sprintf(
		`{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"%s","resources":{"requests":{"cpu":"200m","memory":"64Mi"},"limits":{"cpu":"200m","memory":"64Mi"}},"env":[{"name":"CHURNLESS_E2E_IMMUTABLE","value":"two"}]}]}}}}`,
		image,
	)
	cmd := exec.Command(
		"kubectl",
		"patch",
		"deployment.churnless.io",
		workload,
		"--type=merge",
		"-p",
		patch,
	)
	_, err := utils.Run(cmd)
	return err
}

type mutablePodExpectation struct {
	workload        string
	image           string
	labelValue      string
	annotationValue string
	cpuRequest      string
	cpuLimit        string
	memoryRequest   string
	memoryLimit     string
	excludedUID     types.UID
}

func eventuallyMutablePod(expected mutablePodExpectation) corev1.Pod {
	var result corev1.Pod
	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"pods",
			"-l",
			"app="+expected.workload,
			"-o",
			"json",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		var list corev1.PodList
		g.Expect(json.Unmarshal([]byte(output), &list)).To(Succeed())
		current := make([]corev1.Pod, 0, 1)
		for i := range list.Items {
			if list.Items[i].DeletionTimestamp.IsZero() {
				current = append(current, list.Items[i])
			}
		}
		g.Expect(current).To(HaveLen(1))
		pod := &current[0]
		if expected.excludedUID != "" {
			g.Expect(pod.UID).NotTo(Equal(expected.excludedUID))
		}
		g.Expect(pod.Spec.Containers).NotTo(BeEmpty())
		g.Expect(pod.Spec.Containers[0].Image).To(Equal(expected.image))
		g.Expect(pod.Status.PodIP).NotTo(BeEmpty())
		g.Expect(podReady(pod)).To(BeTrue())
		g.Expect(pod.Status.Resize).To(BeEmpty())
		if expected.labelValue != "" {
			g.Expect(pod.Labels["example.com/track"]).To(Equal(expected.labelValue))
		}
		if expected.annotationValue != "" {
			g.Expect(pod.Annotations["example.com/revision"]).To(Equal(expected.annotationValue))
		}
		expectPodResource(g, pod, corev1.ResourceCPU, expected.cpuRequest, expected.cpuLimit)
		expectPodResource(
			g,
			pod,
			corev1.ResourceMemory,
			expected.memoryRequest,
			expected.memoryLimit,
		)
		result = *pod.DeepCopy()
	}, 5*time.Minute, 200*time.Millisecond).Should(Succeed())
	return result
}

func expectPodResource(
	g Gomega,
	pod *corev1.Pod,
	name corev1.ResourceName,
	request,
	limit string,
) {
	resources := pod.Spec.Containers[0].Resources
	requestQuantity, hasRequest := resources.Requests[name]
	g.Expect(hasRequest).To(Equal(request != ""))
	if request != "" {
		g.Expect(requestQuantity.String()).To(Equal(request))
	}
	limitQuantity, hasLimit := resources.Limits[name]
	g.Expect(hasLimit).To(Equal(limit != ""))
	if limit != "" {
		g.Expect(limitQuantity.String()).To(Equal(limit))
	}
}

func eventuallyOwnedReplicaSetCount(workload string, count int) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "replicasets.churnless.io", "-o", "json")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		var list appsv1alpha1.ReplicaSetList
		g.Expect(json.Unmarshal([]byte(output), &list)).To(Succeed())
		matches := 0
		for i := range list.Items {
			for _, owner := range list.Items[i].OwnerReferences {
				if owner.Controller != nil && *owner.Controller &&
					owner.Kind == "Deployment" &&
					owner.Name == workload {
					matches++
				}
			}
		}
		g.Expect(matches).To(Equal(count))
	}, 5*time.Minute, 200*time.Millisecond).Should(Succeed())
}

func watchRollingUpdatePolicy(
	workload, desiredImage string,
	replicas, maxActive, minReady int,
) map[string]podIdentity {
	deadline := time.Now().Add(5 * time.Minute)
	var last policyPodSnapshot
	for time.Now().Before(deadline) {
		snapshot, err := currentPolicyPods(workload)
		Expect(err).NotTo(HaveOccurred())
		last = snapshot
		Expect(snapshot.Active).To(BeNumerically("<=", maxActive),
			"RollingUpdate exceeded maxSurge")
		Expect(snapshot.Ready).To(BeNumerically(">=", minReady),
			"RollingUpdate exceeded maxUnavailable")
		if snapshot.Active == replicas &&
			snapshot.Ready == replicas &&
			snapshot.ByImage[desiredImage] == replicas {
			return snapshot.Identities
		}
		time.Sleep(200 * time.Millisecond)
	}
	Fail(fmt.Sprintf(
		"RollingUpdate did not complete: active=%d ready=%d images=%v",
		last.Active,
		last.Ready,
		last.ByImage,
	))
	return nil
}

func watchRecreatePolicy(
	workload, oldImage, desiredImage string,
	replicas int,
) {
	deadline := time.Now().Add(5 * time.Minute)
	var last policyPodSnapshot
	for time.Now().Before(deadline) {
		snapshot, err := currentPolicyPods(workload)
		Expect(err).NotTo(HaveOccurred())
		last = snapshot
		Expect(
			snapshot.RunningByImage[oldImage] > 0 &&
				snapshot.RunningByImage[desiredImage] > 0,
		).To(BeFalse(), "Recreate ran old and new Pods at the same time")
		if snapshot.Active == replicas &&
			snapshot.Ready == replicas &&
			snapshot.ByImage[desiredImage] == replicas &&
			snapshot.RunningByImage[oldImage] == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	Fail(fmt.Sprintf(
		"Recreate rollout did not complete: active=%d ready=%d images=%v",
		last.Active,
		last.Ready,
		last.RunningByImage,
	))
}

func currentPolicyPods(workload string) (policyPodSnapshot, error) {
	cmd := exec.Command("kubectl", "get", "pods", "-l", "app="+workload, "-o", "json")
	output, err := utils.Run(cmd)
	if err != nil {
		return policyPodSnapshot{}, err
	}
	var list corev1.PodList
	if err := json.Unmarshal([]byte(output), &list); err != nil {
		return policyPodSnapshot{}, err
	}
	snapshot := policyPodSnapshot{
		ByImage:        make(map[string]int),
		PresentByImage: make(map[string]int),
		RunningByImage: make(map[string]int),
		Identities:     make(map[string]podIdentity),
	}
	for i := range list.Items {
		pod := &list.Items[i]
		if len(pod.Spec.Containers) == 0 {
			return policyPodSnapshot{}, fmt.Errorf("Pod %s has no containers", pod.Name)
		}
		snapshot.PresentByImage[pod.Spec.Containers[0].Image]++
		if pod.Status.Phase != corev1.PodFailed &&
			pod.Status.Phase != corev1.PodSucceeded {
			snapshot.RunningByImage[pod.Spec.Containers[0].Image]++
		}
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		snapshot.Active++
		snapshot.ByImage[pod.Spec.Containers[0].Image]++
		if podReady(pod) {
			snapshot.Ready++
		}
		snapshot.Identities[pod.Name] = podIdentity{
			UID: string(pod.UID),
			IP:  pod.Status.PodIP,
		}
	}
	return snapshot, nil
}

func podReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady &&
			pod.Status.Conditions[i].Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func retainedIdentities(
	before, after map[string]podIdentity,
) int {
	retained := 0
	for name, previous := range before {
		if current, ok := after[name]; ok && current.UID == previous.UID {
			retained++
		}
	}
	return retained
}

func deleteChurnlessDeployment(workload string) {
	cmd := exec.Command(
		"kubectl",
		"delete",
		"deployment.churnless.io",
		workload,
		"--ignore-not-found",
	)
	_, _ = utils.Run(cmd)
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
