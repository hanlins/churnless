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
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"

	"github.com/hanlins/churnless/internal/controller"
)

const commandTestWorkload = "web"

type fakeTransferer struct {
	key         types.NamespacedName
	destination controller.MigrationDestination
	observe     func(controller.MigrationProgress)
}

func (f *fakeTransferer) Transfer(
	_ context.Context,
	key types.NamespacedName,
	destination controller.MigrationDestination,
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

type fakeRestarter struct {
	namespace string
	resource  string
}

func (f *fakeRestarter) Restart(
	_ context.Context,
	namespace, resource string,
) (string, error) {
	f.namespace = namespace
	f.resource = resource
	return "deployment.churnless.io/" + commandTestWorkload, nil
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
		streams,
		configFlags,
		func(
			_ *rest.Config,
			observe func(controller.MigrationProgress),
		) (transferred, error) {
			runner.observe = observe
			return runner, nil
		},
		func(_ *rest.Config) (restarted, error) {
			t.Fatal("restart factory was called for handoff")
			return nil, nil
		},
	)
	command.SetArgs([]string{
		"handoff",
		"deployment/" + commandTestWorkload,
		"--namespace",
		restartTestNamespace,
		"--timeout=0",
	})

	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runner.key != (types.NamespacedName{
		Namespace: restartTestNamespace,
		Name:      commandTestWorkload,
	}) {
		t.Fatalf("key = %v", runner.key)
	}
	if runner.destination != controller.MigrationDestinationNative {
		t.Fatalf("destination = %q", runner.destination)
	}
	if got := output.String(); got !=
		"deployment/web: handoff complete; native Kubernetes is authoritative\n" {
		t.Fatalf("output = %q", got)
	}
}

func TestRestartCommandUsesNamespaceAndPrintsResolvedResource(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	streams := genericclioptions.IOStreams{Out: &output, ErrOut: &output}
	configFlags := genericclioptions.NewConfigFlags(true)
	server := "https://127.0.0.1"
	insecure := true
	configFlags.APIServer = &server
	configFlags.Insecure = &insecure
	restarter := &fakeRestarter{}
	command := newCommand(
		streams,
		configFlags,
		func(
			_ *rest.Config,
			_ func(controller.MigrationProgress),
		) (transferred, error) {
			t.Fatal("transfer factory was called for restart")
			return nil, nil
		},
		func(_ *rest.Config) (restarted, error) {
			return restarter, nil
		},
	)
	command.SetArgs([]string{
		"rollout",
		"restart",
		"deployment/" + commandTestWorkload,
		"--namespace",
		restartTestNamespace,
	})

	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarter.namespace != restartTestNamespace {
		t.Fatalf("namespace = %q", restarter.namespace)
	}
	if restarter.resource != "deployment/"+commandTestWorkload {
		t.Fatalf("resource = %q", restarter.resource)
	}
	if got := output.String(); got !=
		"deployment.churnless.io/web restarted\n" {
		t.Fatalf("output = %q", got)
	}
}

func TestDeploymentName(t *testing.T) {
	t.Parallel()

	for _, resource := range []string{
		"deployment/" + commandTestWorkload,
		"deploy/" + commandTestWorkload,
		"deployment.apps/" + commandTestWorkload,
		"deployment.churnless.io/" + commandTestWorkload,
		"cdeploy/" + commandTestWorkload,
	} {
		name, err := deploymentName(resource)
		if err != nil {
			t.Fatalf("deploymentName(%q): %v", resource, err)
		}
		if name != commandTestWorkload {
			t.Fatalf("deploymentName(%q) = %q", resource, name)
		}
	}
	for _, resource := range []string{
		commandTestWorkload,
		"pod/" + commandTestWorkload,
		"deployment/",
		"deployment/a/b",
	} {
		if _, err := deploymentName(resource); err == nil {
			t.Fatalf("deploymentName(%q) succeeded", resource)
		}
	}
}
