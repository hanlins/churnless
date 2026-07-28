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
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"

	"github.com/hanlins/churnless/internal/controller"
)

const commandTestNamespace, commandTestWorkload = "team", "web"

var errCommandTestRestart = errors.New("restart failed")

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

type fakeRestarter struct {
	namespace string
	reference deploymentReference
	err       error
}

func (f *fakeRestarter) Restart(
	_ context.Context, namespace string, reference deploymentReference,
) (string, error) {
	f.namespace, f.reference = namespace, reference
	if f.err != nil {
		return "", f.err
	}
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
		streams, configFlags,
		func(_ *rest.Config, observe func(controller.MigrationProgress)) (transferred, error) {
			runner.observe = observe
			return runner, nil
		},
		nil,
	)
	command.SetArgs([]string{
		"handoff",
		"deployment/" + commandTestWorkload,
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

func TestRestartCommandHelp(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	command := newCommand(
		genericclioptions.IOStreams{Out: &output, ErrOut: &output},
		genericclioptions.NewConfigFlags(true),
		nil,
		nil,
	)
	command.SetArgs([]string{rolloutCommand, "restart", "--help"})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"restart deployment/NAME",
		"Force a new native Kubernetes or Churnless rollout",
		"kubectl churnless rollout restart deployment/web",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("help missing %q:\n%s", want, output.String())
		}
	}
}

func TestRestartCommandReportsOnlySuccess(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		err        error
		wantOutput string
	}{
		{name: "success", wantOutput: "deployment.churnless.io/web restarted\n"},
		{name: "failure", err: errCommandTestRestart},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var output bytes.Buffer
			configFlags := genericclioptions.NewConfigFlags(true)
			server, insecure := "https://127.0.0.1", true
			configFlags.APIServer, configFlags.Insecure = &server, &insecure
			restarter := &fakeRestarter{err: test.err}
			command := newCommand(
				genericclioptions.IOStreams{Out: &output, ErrOut: &output},
				configFlags,
				nil,
				func(*rest.Config) (restarted, error) { return restarter, nil },
			)
			command.SetArgs([]string{
				rolloutCommand, "restart", "deployment/" + commandTestWorkload,
				"--namespace", commandTestNamespace,
			})

			err := command.ExecuteContext(context.Background())
			if !errors.Is(err, test.err) {
				t.Fatalf("error = %v, want %v", err, test.err)
			}
			if restarter.namespace != commandTestNamespace ||
				restarter.reference != (deploymentReference{
					name: commandTestWorkload,
					api:  deploymentAPIAny,
				}) {
				t.Fatalf("restart target = %q %#v", restarter.namespace, restarter.reference)
			}
			if got := output.String(); got != test.wantOutput {
				t.Fatalf("output = %q, want %q", got, test.wantOutput)
			}
		})
	}
}

func TestDeploymentName(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{
		"deployment", "deployments", "deploy",
		"deployment.apps", "deployments.apps",
		"deployment.churnless.io", "deployments.churnless.io", "cdeploy",
	} {
		resource := kind + "/" + commandTestWorkload
		name, err := deploymentName(resource)
		if err != nil {
			t.Fatalf("deploymentName(%q): %v", resource, err)
		}
		if name != commandTestWorkload {
			t.Fatalf("deploymentName(%q) = %q", resource, name)
		}
	}
	for _, resource := range []string{
		commandTestWorkload, "pod/" + commandTestWorkload, "deployment/", "deployment/a/b",
	} {
		if _, err := deploymentName(resource); err == nil {
			t.Fatalf("deploymentName(%q) succeeded", resource)
		}
	}
}

func TestParseDeploymentReference(t *testing.T) {
	t.Parallel()

	for resource, wantAPI := range map[string]deploymentAPI{
		"deployment/web":              deploymentAPIAny,
		"deployment.apps/web":         deploymentAPINative,
		"deployment.churnless.io/web": deploymentAPIChurnless,
	} {
		got, err := parseDeploymentReference(resource)
		if err != nil {
			t.Fatalf("parseDeploymentReference(%q): %v", resource, err)
		}
		if got != (deploymentReference{name: commandTestWorkload, api: wantAPI}) {
			t.Fatalf("parseDeploymentReference(%q) = %#v", resource, got)
		}
	}
}
