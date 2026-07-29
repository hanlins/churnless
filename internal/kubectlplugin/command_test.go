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
	"bytes"
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"

	"github.com/hanlins/churnless/internal/controller"
)

const commandTestNamespace, commandTestWorkload = "team", "web"

type fakeTransferer struct {
	key         types.NamespacedName
	destination controller.MigrationDestination
	observe     func(controller.MigrationProgress)
}

func (f *fakeTransferer) Transfer(
	_ context.Context, key types.NamespacedName, destination controller.MigrationDestination,
) error {
	f.key = key
	f.destination = destination
	f.observe(controller.MigrationProgress{
		Destination: destination,
		Complete:    true,
		Message:     "handoff complete; native Kubernetes is authoritative",
	})
	return nil
}

func TestHandoffCommandUsesNamespaceAndDriver(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	streams := genericclioptions.IOStreams{Out: &output, ErrOut: &output}
	configFlags := genericclioptions.NewConfigFlags(true)
	server := "https://127.0.0.1"
	insecure := true
	configFlags.APIServer = &server
	configFlags.Insecure = &insecure
	runner := &fakeTransferer{}
	command := newCommand(
		streams, configFlags,
		func(_ *rest.Config, observe func(controller.MigrationProgress)) (transferred, error) {
			runner.observe = observe
			return runner, nil
		},
	)
	command.SetArgs([]string{
		handoffCommand,
		churnlessDeploymentResource + "/" + commandTestWorkload,
		"--namespace",
		commandTestNamespace,
		"--timeout=0",
	})

	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runner.key != (types.NamespacedName{
		Namespace: commandTestNamespace,
		Name:      commandTestWorkload,
	}) {
		t.Fatalf("key = %v", runner.key)
	}
	if runner.destination != controller.MigrationDestinationNative {
		t.Fatalf("destination = %q", runner.destination)
	}
	if got, want := output.String(),
		"deployment/web: handoff complete; native Kubernetes is authoritative\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestTransferDeploymentName(t *testing.T) {
	t.Parallel()

	nativeAliases := []string{
		deploymentResource, "deployments", "deploy",
		nativeDeploymentResource, "deployments.apps",
	}
	churnlessAliases := []string{
		"cdeploy", churnlessDeploymentResource, "deployments.churnless.io",
	}
	tests := []struct {
		name         string
		destination  controller.MigrationDestination
		accepted     []string
		rejected     []string
		expectedHint string
	}{
		{
			name:         takeoverCommand,
			destination:  controller.MigrationDestinationChurnless,
			accepted:     nativeAliases,
			rejected:     churnlessAliases,
			expectedHint: "native Deployment source (" + nativeDeploymentResource + "/NAME)",
		},
		{
			name:         handoffCommand,
			destination:  controller.MigrationDestinationNative,
			accepted:     churnlessAliases,
			rejected:     nativeAliases,
			expectedHint: "Churnless Deployment source (" + churnlessDeploymentResource + "/NAME)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, kind := range tt.accepted {
				resource := kind + "/" + commandTestWorkload
				name, err := transferDeploymentName(resource, tt.destination)
				if err != nil {
					t.Fatalf("transferDeploymentName(%q): %v", resource, err)
				}
				if name != commandTestWorkload {
					t.Fatalf("transferDeploymentName(%q) = %q", resource, name)
				}
			}
			for _, kind := range tt.rejected {
				resource := kind + "/" + commandTestWorkload
				_, err := transferDeploymentName(resource, tt.destination)
				if err == nil {
					t.Fatalf("transferDeploymentName(%q) succeeded", resource)
				}
				if !strings.Contains(err.Error(), tt.expectedHint) {
					t.Fatalf("transferDeploymentName(%q) error = %q", resource, err)
				}
			}
		})
	}
}

func TestTransferCommandUsageNamesSourceAPI(t *testing.T) {
	t.Parallel()

	command := newCommand(
		genericclioptions.IOStreams{},
		genericclioptions.NewConfigFlags(true),
		nil,
	)
	tests := map[string]struct {
		use     string
		example string
	}{
		takeoverCommand: {
			use:     takeoverCommand + " " + nativeDeploymentResource + "/NAME",
			example: "kubectl churnless " + takeoverCommand + " " + nativeDeploymentResource + "/web",
		},
		handoffCommand: {
			use:     handoffCommand + " " + churnlessDeploymentResource + "/NAME",
			example: "kubectl churnless " + handoffCommand + " " + churnlessDeploymentResource + "/web",
		},
	}
	for name, want := range tests {
		subcommand, _, err := command.Find([]string{name})
		if err != nil {
			t.Fatal(err)
		}
		if subcommand.Use != want.use {
			t.Errorf("%s Use = %q, want %q", name, subcommand.Use, want.use)
		}
		if !strings.Contains(subcommand.Example, want.example) {
			t.Errorf("%s Example = %q, want %q", name, subcommand.Example, want.example)
		}
	}
}

func TestParseDeploymentReferenceRejectsInvalidResources(t *testing.T) {
	t.Parallel()

	for _, resource := range []string{
		commandTestWorkload,
		"pod/" + commandTestWorkload,
		deploymentResource + "/",
		deploymentResource + "/a/b",
	} {
		if _, err := parseDeploymentReference(resource); err == nil {
			t.Fatalf("parseDeploymentReference(%q) succeeded", resource)
		}
	}
}
