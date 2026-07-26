# Churnless

Churnless provides Kubernetes-compatible `Deployment` and `ReplicaSet` APIs
that can update container images without replacing Pods.

During an image-only rollout, Churnless restarts containers inside the
existing Pods. Pod names, UIDs, and IPs remain stable as long as the Pods
themselves are not deleted or rescheduled.

> [!IMPORTANT]
> Churnless is experimental and its API is `v1alpha1`. Test it carefully
> before using it for production workloads.

## Why Churnless?

A native Kubernetes Deployment creates a new ReplicaSet whenever its Pod
template changes. Updating an image therefore replaces Pods, which can cause
Pod IP churn even when the application only needs a new container process.

Churnless changes the revision boundary:

- Image changes are revisions within the current Churnless ReplicaSet.
- Other Pod-template changes create a new Churnless ReplicaSet and use a
  replacement rollout.

This preserves the normal Deployment-to-ReplicaSet responsibility split while
allowing the ReplicaSet controller to reconcile mutable Pod image fields
directly.

```text
Deployment.apps.churnless.io
└── ReplicaSet.apps.churnless.io
    └── Pod
```

There are no native Deployment or ReplicaSet shadow objects.

## Features

- Familiar `Deployment` and `ReplicaSet` kinds under
  `apps.churnless.io/v1alpha1`.
- Native `apps/v1` spec and status types embedded directly in the Go API.
- In-place updates for regular-container and init-container images.
- Stable Pod name, UID, and IP during successful image-only rollouts.
- Structural Deployment revisions backed by separate Churnless ReplicaSets.
- RollingUpdate and Recreate orchestration for structural changes.
- Selector-based Pod adoption and controller owner references.
- Replica scaling, self-healing, readiness, availability, and rollout status.
- Standard `/scale` subresources for `kubectl scale` and HPA.
- Kubebuilder-native manifests, RBAC, tests, and local development workflow.

## Quick start with kind

Prerequisites:

- Go
- Docker
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [kind](https://kind.sigs.k8s.io/)

Create a local cluster, build and load the controller, install the CRDs, and
deploy a two-replica sample:

```sh
make kind-up
```

Inspect the custom Deployment, its Churnless ReplicaSet, and the Pods:

```sh
make kind-status
```

Record the initial Pod identities:

```sh
kubectl get pods -l app=deployment-sample \
  -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,IP:.status.podIP,IMAGE:.spec.containers[0].image'
```

Apply an image-only revision:

```sh
kubectl patch deployment.apps.churnless.io deployment-sample \
  --type=merge \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"nginx:1.28-alpine","ports":[{"containerPort":80}]}]}}}}'
```

Run the identity command again after the Pods are ready. The image and
container restart count change, while each Pod's name, UID, and IP remain the
same.

Delete the playground when finished:

```sh
make kind-down
```

The cluster name and controller image can be overridden:

```sh
KIND_CLUSTER=my-churnless \
KIND_IMAGE=example.com/churnless:dev \
make kind-up
```

## API compatibility

The custom resources intentionally use the native kind names with a different
group and version:

| Kind | API version | Embedded Kubernetes types |
| --- | --- | --- |
| `Deployment` | `apps.churnless.io/v1alpha1` | `apps/v1.DeploymentSpec`, `apps/v1.DeploymentStatus` |
| `ReplicaSet` | `apps.churnless.io/v1alpha1` | `apps/v1.ReplicaSetSpec`, `apps/v1.ReplicaSetStatus` |

For supported behavior, a native manifest can be migrated by changing only
its `apiVersion`:

```yaml
apiVersion: apps.churnless.io/v1alpha1
kind: Deployment
```

Use fully qualified resource names when both native and Churnless APIs are
installed:

```sh
kubectl get deployments.apps.churnless.io
kubectl get replicasets.apps.churnless.io
```

The current compatibility boundary is deliberate:

- The native field layout is preserved, but a CRD does not automatically
  inherit every built-in admission default or validation rule.
- Stock `kubectl rollout` does not register the custom GVK. Inspect
  `status.inPlace` or use normal `kubectl get` and `kubectl wait` workflows.
- Image-only revisions reuse one ReplicaSet and therefore do not appear as
  separate native-style ReplicaSet history entries.
- Progress-deadline enforcement, revision-history cleanup, and hash-collision
  handling are not complete yet.

## HPA and scaling

Both CRDs expose the standard `/scale` subresource:

```sh
kubectl scale deployment.apps.churnless.io/deployment-sample --replicas=3
```

An HPA can target a Churnless Deployment directly:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: web
spec:
  scaleTargetRef:
    apiVersion: apps.churnless.io/v1alpha1
    kind: Deployment
    name: deployment-sample
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

## Install on an existing cluster

Build and publish a controller image, then install it with Kustomize:

```sh
make docker-build docker-push IMG=ghcr.io/your-org/churnless:tag
make deploy IMG=ghcr.io/your-org/churnless:tag
```

Apply one of the sample workloads:

```sh
kubectl apply -f config/samples/apps_v1alpha1_deployment.yaml
```

Remove the controller and CRDs:

```sh
make undeploy
make uninstall
```

## Guarantees and limitations

- In-place image updates preserve Pod identity only while the Pod continues to
  exist on the same node.
- Eviction, node failure, manual deletion, scheduling changes, and structural
  Pod-template updates can still replace a Pod and change its IP.
- Churnless updates only Kubernetes-mutable image fields in place. Other
  template changes use a structural rollout.
- With a zero effective `maxUnavailable`, Churnless restarts one Pod at a time
  so an image rollout can make progress without replacing the original Pods.
- A replacement Pod created during scaling or self-healing starts from the
  latest ReplicaSet template.

## Development

The project was bootstrapped with Kubebuilder and follows its standard layout.

Run the local checks:

```sh
make manifests generate
make test
make lint
```

Run the isolated kind end-to-end suite:

```sh
CERT_MANAGER_INSTALL_SKIP=true make test-e2e
```

The e2e suite verifies:

- `Deployment.apps.churnless.io → ReplicaSet.apps.churnless.io → Pod`
  controller ownership.
- Absence of native Deployment and ReplicaSet shadows.
- Stable Churnless ReplicaSet identity during an image revision.
- Stable Pod names, UIDs, and IPs during the rollout.
- New Pods created through `/scale` use the latest image.

## Contributing

Issues and pull requests are welcome. Please include tests for behavior
changes and run `make test`, `make lint`, and the kind e2e suite before
submitting a controller change.

## License

Licensed under the [Apache License 2.0](LICENSE).
