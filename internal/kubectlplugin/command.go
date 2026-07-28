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

const defaultTransferTimeout = 5 * time.Minute

type transferred interface {
	Transfer(
		context.Context,
		types.NamespacedName,
		controller.MigrationDestination,
	) error
}

type transfererFactory func(
	*rest.Config,
	func(controller.MigrationProgress),
) (transferred, error)

// NewCommand creates the kubectl-churnless command tree.
func NewCommand(streams genericclioptions.IOStreams) *cobra.Command {
	return newCommand(
		streams,
		genericclioptions.NewConfigFlags(true),
		newTransferer,
	)
}

func newCommand(
	streams genericclioptions.IOStreams,
	configFlags *genericclioptions.ConfigFlags,
	factory transfererFactory,
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
			streams,
			configFlags,
			factory,
			"takeover",
			"Transfer a native Deployment to Churnless",
			controller.MigrationDestinationChurnless,
		),
		newTransferCommand(
			streams,
			configFlags,
			factory,
			"handoff",
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
	command := &cobra.Command{
		Use:     name + " deployment/NAME",
		Short:   short,
		Example: "  kubectl churnless " + name + " deployment/web",
		Args:    cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			workload, err := deploymentName(args[0])
			if err != nil {
				return err
			}
			namespace, _, err := configFlags.ToRawKubeConfigLoader().Namespace()
			if err != nil {
				return fmt.Errorf("resolve namespace: %w", err)
			}
			config, err := configFlags.ToRESTConfig()
			if err != nil {
				return fmt.Errorf("load Kubernetes configuration: %w", err)
			}
			config.UserAgent = "kubectl-churnless"
			resource := "deployment/" + workload
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
				ctx,
				types.NamespacedName{Namespace: namespace, Name: workload},
				destination,
			); err != nil {
				return fmt.Errorf("%s %s: %w", name, resource, err)
			}
			return nil
		},
	}
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		defaultTransferTimeout,
		"Maximum time to drive the transfer; rerun the same command to resume",
	)
	return command
}

func deploymentName(resource string) (string, error) {
	kind, name, found := strings.Cut(resource, "/")
	if !found || name == "" || strings.Contains(name, "/") {
		return "", fmt.Errorf(
			"expected deployment/NAME, got %q",
			resource,
		)
	}
	switch kind {
	case "deployment", "deploy", "deployment.apps", "deployment.churnless.io", "cdeploy":
		return name, nil
	default:
		return "", fmt.Errorf(
			"only Deployment migration is supported, got %q",
			kind,
		)
	}
}

func newTransferer(
	config *rest.Config,
	observe func(controller.MigrationProgress),
) (transferred, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("register Kubernetes API types: %w", err)
	}
	if err := appsv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("register Churnless API types: %w", err)
	}
	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	return &controller.MigrationDriver{
		Engine: &controller.MigrationReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    scheme,
		},
		Observe: observe,
	}, nil
}
