# Churnless

Keep Pod identity stable during container image rollouts.

Churnless provides native-shaped Kubernetes `Deployment` and `ReplicaSet`
APIs that update container images without replacing healthy Pods. During a
successful image-only rollout, Pod names, UIDs, and IPs stay the same.

> [!IMPORTANT]
> Churnless is experimental and its API is `v1alpha1`. Test it carefully
> before using it for production workloads.

## Why

A native Kubernetes Deployment creates a new ReplicaSet whenever its Pod
template changes. An image rollout therefore creates replacement Pods and may
churn their IPs. Churnless treats images as mutable revisions inside a
ReplicaSet and patches the existing Pods instead.

```text
Deployment.churnless.io
└── ReplicaSet.churnless.io
    └── Pod
```

Churnless uses no native Deployment or ReplicaSet shadow objects.

Highlights:

- `Deployment` and `ReplicaSet` under `churnless.io/v1alpha1`
- Native `apps/v1` specs and statuses embedded in the Go API
- In-place regular-container and init-container image updates
- RollingUpdate and Recreate for structural template changes
- Scaling, self-healing, Pod adoption, availability, and rollout status
- Standard `/scale` support for `kubectl scale` and HPA
- Upstream Kubernetes v1.36 workload defaulting and validation
- Kubebuilder-native manifests and an isolated kind test workflow

## Try it with kind

Prerequisites:

- Go
- Docker
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [kind](https://kind.sigs.k8s.io/)

Create a local cluster, build and load the controller, install the CRDs, and
deploy a two-replica sample. The setup also installs pinned cert-manager and
Metrics Server releases for webhook and HPA testing:

```sh
make kind-up
```

Inspect the custom Deployment, its Churnless ReplicaSet, and the Pods:

```sh
make kind-status
```

Patch the sample image using the Churnless Deployment short name:

```sh
kubectl patch cdeploy deployment-sample \
  --type=merge \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"nginx:1.28-alpine","ports":[{"containerPort":80}]}]}}}}'
```

Compare Pod identity before and after the rollout:

```sh
kubectl get pods -l app=deployment-sample \
  -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,IP:.status.podIP,IMAGE:.spec.containers[0].image'
```

Clean up:

```sh
make kind-down
```

## Use the API

For supported behavior, migrate a native manifest by changing its API version:

```yaml
apiVersion: churnless.io/v1alpha1
kind: Deployment
```

Use the distinct Churnless short names for interactive commands:

```sh
kubectl get cdeploy
kubectl get chrs
kubectl get churnless
kubectl scale cdeploy/deployment-sample --replicas=3
```

For scripts, use fully qualified resource names such as
`deployments.churnless.io` and `replicasets.churnless.io`.

HPA can target the Churnless GVK directly through its standard `/scale`
subresource.

## Design

See [DESIGN.md](DESIGN.md) for the compatibility contract, controller
responsibilities, revision model, invariants, known gaps, and required
acceptance tests.

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
make test-e2e
```

## Contributing

Issues and pull requests are welcome. Please include tests for behavior
changes and keep [the design contract](DESIGN.md) current.

## License

Licensed under the [Apache License 2.0](LICENSE).
