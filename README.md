# Churnless

Churnless provides Kubernetes-compatible workload APIs that update container
images in place. During an image-only rollout, the kubelet restarts containers
inside the existing Pods, preserving each Pod's name, UID, and IP.

The API group/version is `apps.churnless.io/v1alpha1`. Its Kinds intentionally
match the native workload names; use fully qualified resource names with
`kubectl` to disambiguate them from `apps/v1`:

| Kind | Native spec/status compatibility | Native shadow |
| --- | --- | --- |
| `Deployment` | `apps/v1.DeploymentSpec` and `DeploymentStatus` | `Deployment` |
| `StatefulSet` | `apps/v1.StatefulSetSpec` and `StatefulSetStatus` | `StatefulSet` |
| `ReplicaSet` | `apps/v1.ReplicaSetSpec` and `ReplicaSetStatus` | `ReplicaSet` |

The upstream types are embedded with `json:",inline"` in Go. This keeps their
wire format and generated OpenAPI schema aligned with Kubernetes, including
Pod templates, rollout strategies, affinity, probes, security contexts,
StatefulSet PVC templates, and future fields picked up when the Kubernetes
dependencies are upgraded.

## How it works

Each custom workload owns a same-name native workload. The native controller
continues to provide ordinary behavior such as replica management, scheduling,
self-healing, Deployment revision history, StatefulSet identity/PVC handling,
and status conditions.

Before syncing an update, Churnless performs a dry-run update of the native
workload. This applies the API server's native defaulting and validation:

- If only `spec.template.spec.containers[*].image` or
  `initContainers[*].image` changed, the native template stays stable and
  Churnless patches the existing Pods in availability-aware batches.
- If any other template field changed, the complete desired template is
  passed to the native workload and Kubernetes performs its normal replacement
  rollout.
- `StatefulSet` honors `RollingUpdate.partition` and descending ordinal
  order. With `OnDelete`, Churnless updates the native template and leaves
  existing Pods alone, matching native semantics.
- A paused `Deployment` does not apply its pending image revision until
  it is resumed.

Churnless watches both the shadow workload and its Pods. A newly created Pod
after scaling or self-healing is brought to the latest desired image even when
the shadow template intentionally remains on an older image.

## Quick start

Prerequisites are Go, Docker, kubectl, and access to a Kubernetes cluster.
The repository was bootstrapped with native Kubebuilder v4.

```sh
make install
make run
```

In another terminal:

```sh
kubectl apply -f config/samples/apps_v1alpha1_deployment.yaml
kubectl get deployments.apps.churnless.io,pods
```

Apply an image-only revision:

```sh
kubectl patch deployment.apps.churnless.io deployment-sample \
  --type=merge \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"nginx:1.28-alpine","ports":[{"containerPort":80}]}]}}}}'
```

The in-place progress is available under `status.inPlace`; native-compatible
status fields remain at their usual paths.

## HPA and scaling

Every CRD exposes the standard `/scale` subresource:

```sh
kubectl scale deployment.apps.churnless.io/deployment-sample \
  --replicas=3
```

HPA can target the custom GVK directly:

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

`spec.replicas`, `status.replicas`, and `status.selector` are mapped to
`autoscaling/v1.Scale`, so HPA sees the same paths it expects from native
workloads.

## Validation

```sh
make test
CERT_MANAGER_INSTALL_SKIP=true make test-e2e
```

The envtest suite covers native shadow creation, complete embedded fields,
image masking, Pod identity/IP preservation, StatefulSet partition data, and
the scale subresource. The kind e2e test builds and deploys the manager, rolls a
two-replica `Deployment`, asserts stable Pod UID/IP values, then scales
it through `/scale` and verifies new Pods converge to the latest image.

## Guarantees and limits

- Pod IP preservation applies to image-only changes while the Pod remains on
  its node. Eviction, node failure, deletion, or another native replacement
  condition can still create a new Pod and IP.
- Kubernetes permits ordinary Pod updates only for a small set of fields,
  including container and init-container images. Other template changes use
  the native replacement path.
- An in-place rollout cannot use Deployment `maxSurge` without creating extra
  Pods. Churnless honors the `maxUnavailable` budget; if it rounds to zero,
  one Pod must restart at a time for the rollout to make progress.
- The same-name native workload is an implementation detail and must not
  already be owned by another object.
- Native `kubectl rollout history/undo` does not understand the custom GVK,
  and image-only revisions intentionally do not create Deployment
  ReplicaSets. Roll back by setting the previous image on the custom workload.
- The API is currently `v1alpha1`; use versioned manifests and test upgrades
  before production adoption.
