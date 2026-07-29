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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1alpha1 "github.com/hanlins/churnless/api/v1alpha1"
	"github.com/hanlins/churnless/internal/controller"
)

const (
	defaultTransferTimeout      = 5 * time.Minute
	takeoverCommand             = "takeover"
	handoffCommand              = "handoff"
	deploymentResource          = "deployment"
	nativeDeploymentResource    = "deployment.apps"
	churnlessDeploymentResource = "deployment.churnless.io"
)

type deploymentAPI int

const (
	deploymentAPIAny deploymentAPI = iota
	deploymentAPINative
	deploymentAPIChurnless
)

type deploymentReference struct {
	name string
	api  deploymentAPI
}

type transferred interface {
	Transfer(context.Context, types.NamespacedName, controller.MigrationDestination) error
}

type transfererFactory func(*rest.Config, func(controller.MigrationProgress)) (transferred, error)

// NewCommand creates the kubectl-churnless command tree.
func NewCommand(streams genericclioptions.IOStreams) *cobra.Command {
	return newCommand(streams, genericclioptions.NewConfigFlags(true), newTransferer)
}

func newCommand(
	streams genericclioptions.IOStreams,
	configFlags *genericclioptions.ConfigFlags,
	transferFactory transfererFactory,
) *cobra.Command {
	command := &cobra.Command{
		Use:           "churnless",
		Short:         "Transfer Deployments between native Kubernetes and Churnless",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	configFlags.AddFlags(command.PersistentFlags())
	command.AddCommand(
		newTransferCommand(
			streams, configFlags, transferFactory, takeoverCommand,
			"Transfer a native Deployment to Churnless",
			controller.MigrationDestinationChurnless,
		),
		newTransferCommand(
			streams, configFlags, transferFactory, handoffCommand,
			"Transfer a Churnless Deployment to native Kubernetes",
			controller.MigrationDestinationNative,
		),
	)
	return command
}

func newTransferCommand(
	streams genericclioptions.IOStreams,
	configFlags *genericclioptions.ConfigFlags,
	factory transfererFactory,
	name, short string,
	destination controller.MigrationDestination,
) *cobra.Command {
	timeout := defaultTransferTimeout
	source := nativeDeploymentResource
	if destination == controller.MigrationDestinationNative {
		source = churnlessDeploymentResource
	}
	command := &cobra.Command{
		Use:     name + " " + source + "/NAME",
		Short:   short,
		Example: "  kubectl churnless " + name + " " + source + "/web",
		Args:    cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			workload, err := transferDeploymentName(args[0], destination)
			if err != nil {
				return err
			}
			namespace, config, err := commandTarget(configFlags)
			if err != nil {
				return err
			}
			resource := deploymentResource + "/" + workload
			driver, err := factory(config, func(progress controller.MigrationProgress) {
				_, _ = fmt.Fprintf(streams.Out, "%s: %s\n", resource, progress.Message)
			})
			if err != nil {
				return fmt.Errorf("create migration client: %w", err)
			}

			ctx := command.Context()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			if err := driver.Transfer(
				ctx, types.NamespacedName{Namespace: namespace, Name: workload}, destination,
			); err != nil {
				return fmt.Errorf("%s %s: %w", name, resource, err)
			}
			return nil
		},
	}
	command.Flags().DurationVar(&timeout, "timeout", defaultTransferTimeout,
		"Maximum time to drive the transfer; rerun the same command to resume",
	)
	return command
}

func commandTarget(configFlags *genericclioptions.ConfigFlags) (string, *rest.Config, error) {
	namespace, _, err := configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return "", nil, fmt.Errorf("resolve namespace: %w", err)
	}
	config, err := configFlags.ToRESTConfig()
	if err != nil {
		return "", nil, fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	config.UserAgent = "kubectl-churnless"
	return namespace, config, nil
}

func transferDeploymentName(
	resource string,
	destination controller.MigrationDestination,
) (string, error) {
	reference, err := parseDeploymentReference(resource)
	if err != nil {
		return "", err
	}

	expectedSource := "native Deployment source (" + nativeDeploymentResource + "/NAME)"
	valid := reference.api == deploymentAPINative || reference.api == deploymentAPIAny
	if destination == controller.MigrationDestinationNative {
		expectedSource = "Churnless Deployment source (" + churnlessDeploymentResource + "/NAME)"
		valid = reference.api == deploymentAPIChurnless
	}
	if !valid {
		return "", fmt.Errorf("expected a %s, got %q", expectedSource, resource)
	}
	return reference.name, nil
}

func parseDeploymentReference(resource string) (deploymentReference, error) {
	kind, name, found := strings.Cut(resource, "/")
	if !found || name == "" || strings.Contains(name, "/") {
		return deploymentReference{}, fmt.Errorf(
			"expected %s/NAME, got %q",
			deploymentResource,
			resource,
		)
	}

	var api deploymentAPI
	switch strings.ToLower(kind) {
	case deploymentResource, "deployments":
		api = deploymentAPIAny
	case "deploy", nativeDeploymentResource, "deployments.apps":
		api = deploymentAPINative
	case "cdeploy", churnlessDeploymentResource, "deployments.churnless.io":
		api = deploymentAPIChurnless
	default:
		return deploymentReference{}, fmt.Errorf(
			"only Deployment resources are supported, got %q",
			kind,
		)
	}
	return deploymentReference{name: name, api: api}, nil
}

func newTransferer(
	config *rest.Config,
	observe func(controller.MigrationProgress),
) (transferred, error) {
	k8sClient, scheme, err := newKubernetesClient(config)
	if err != nil {
		return nil, err
	}
	engine, err := controller.NewDeploymentMigrationEngine(
		k8sClient,
		k8sClient,
		scheme,
	)
	if err != nil {
		return nil, err
	}
	return &controller.MigrationDriver{
		Engine:  engine,
		Observe: observe,
	}, nil
}

func newKubernetesClient(config *rest.Config) (client.Client, *runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("register Kubernetes API types: %w", err)
	}
	if err := appsv1alpha1.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("register Churnless API types: %w", err)
	}
	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, err
	}
	return k8sClient, scheme, nil
}
