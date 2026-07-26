# Churnless Design

This document is the design contract for Churnless. It is written for both
human maintainers and coding agents. Read it before changing the API,
controllers, ownership model, or rollout behavior.

The [README](README.md) is the concise, user-facing introduction. This document
owns the architectural detail. When implementation and design diverge, update
the implementation or explicitly record the gap here; do not let two competing
designs emerge.

## Goals

Churnless extends the Kubernetes workload model with one deliberate change:
container image updates should restart containers in existing Pods instead of
replacing those Pods whenever Kubernetes permits it.

The project aims to provide:

- Native-shaped `Deployment` and `ReplicaSet` resources under a distinct API
  group.
- A migration path where a supported native manifest works after changing only
  `apiVersion`.
- Native workload behavior for scaling, self-healing, rollout strategies,
  readiness, availability, status, and controller ownership.
- Standard `/scale` subresources so `kubectl scale` and HPA work normally.
- Stable Pod name, UID, and IP during a successful image-only rollout.
- Clear controller responsibility boundaries and no competing shadow
  controllers.

Pod identity preservation is best effort, not an availability guarantee. A Pod
can still be replaced because of eviction, node failure, deletion, scheduling,
scaling, or a structural template change.

## Non-goals

- Mutating arbitrary Pod fields in place. Churnless only treats regular and
  init-container images as in-place fields.
- Preserving Pod identity when Kubernetes must create a different Pod.
- Delegating desired-state ownership to native Deployment or ReplicaSet shadow
  objects.
- Claiming compatibility for behavior that is neither implemented nor tested.
- Providing a StatefulSet API before its identity and rollout semantics have a
  complete design and implementation.

## System model

```text
Deployment.churnless.io
└── ReplicaSet.churnless.io
    └── Pod
```

There are no native `Deployment.apps` or `ReplicaSet.apps` shadows. Every
object has exactly one authoritative controller:

| Resource | Responsibility |
| --- | --- |
| Churnless Deployment | Chooses structural revisions, creates and scales Churnless ReplicaSets, applies rollout strategy, and aggregates status. |
| Churnless ReplicaSet | Selects and adopts Pods, maintains replica count, creates and deletes Pods, updates Pod images in place, and reports status. |
| Admission webhooks | Apply and validate the native workload semantics that CRD schemas do not inherit from built-in API storage. |
| kubelet | Observes the patched Pod image fields and restarts the affected containers. |

Desired state flows only downward through this chain. Status flows upward. A
controller must not directly manage resources owned by the layer below its
child controller.

## API contract

The public API group is `churnless.io/v1alpha1`:

| Kind | Resource | Short name | Spec | Status |
| --- | --- | --- | --- | --- |
| `Deployment` | `deployments.churnless.io` | `cdeploy` | Inline `apps/v1.DeploymentSpec` | Inline `apps/v1.DeploymentStatus` plus Churnless progress |
| `ReplicaSet` | `replicasets.churnless.io` | `chrs` | Inline `apps/v1.ReplicaSetSpec` | Inline `apps/v1.ReplicaSetStatus` plus Churnless progress |

Both resources belong to the `churnless` kubectl category, so
`kubectl get churnless` lists them together. Short names are intended for
interactive use; automation should use fully qualified resource names. Native
`deploy` and `rs` shortcuts remain reserved for native `apps/v1` resources.

Inlining preserves the native JSON field layout and Go field vocabulary. The
Churnless status extension is:

```yaml
status:
  selector: app=web
  inPlace:
    revision: <image-revision>
    updatedReplicas: 2
    readyUpdatedReplicas: 2
```

`status.selector` backs the standard `/scale` subresource. HPA targets the
Churnless GVK directly and writes `spec.replicas` through that subresource.
Unlike the built-in Deployment REST storage, generic CRD scale storage cannot
derive a selector string from the structured `spec.selector`; the explicit
status field is therefore required by the CRD's `labelSelectorPath`.

`ReplicaSet.status.inPlace` is the child controller's image-specific progress
contract. The Deployment consumes it only when its `revision` matches the
ReplicaSet template and its `observedGeneration` is current. The Deployment
status copies that fresh progress for user-facing observability; it is not
desired state and is never used in place of the ReplicaSet spec.

API shape and behavior are separate compatibility requirements:

1. Native fields keep their native JSON representation.
2. Supported fields keep their native meaning unless this document defines a
   deliberate divergence.
3. A field must not be presented as compatible solely because the CRD schema
   accepts it.
4. New compatibility work requires tests derived from the corresponding native
   behavior.
5. Deliberate divergences must be visible in status or documentation.

## Upstream compatibility layer

Kubernetes publishes `k8s.io/api` and `k8s.io/apimachinery` as staging modules,
but its complete apps defaulting, apps validation, and controller helpers live
in the non-staging `k8s.io/kubernetes` module. Churnless deliberately imports
those upstream packages behind `internal/kubecompat`, pins every Kubernetes
module to the same `v1.36` release line, and exposes only the small interface
needed by its webhooks and controllers.

The compatibility layer delegates directly to upstream Kubernetes for:

- Covering Deployment and ReplicaSet defaults, including nested Pod templates.
- Deployment, ReplicaSet, selector, strategy, and full Pod-template validation.
- RollingUpdate fencepost calculation.
- Pod readiness and availability calculation.
- Native scale-down preference.

Only behavior that Kubernetes keeps controller-local and unexported, currently
ReplicaSet slow-start batching and its 500-Pod burst limit, is mirrored locally
with an upstream source reference and focused tests. Importing the non-staging
module is an intentional dependency tradeoff: it is less stable as a Go API,
but avoids maintaining a partial fork of workload admission behavior.

Server-side dry-run tests compare selected Churnless admission behavior with
the native GVK. The native API server is a test oracle, not a runtime
dependency and not a shadow controller.

## Revision model

Churnless separates a Pod template into two kinds of revision.

### Image revision

An image revision contains the names and images of regular and init containers.
Changing only those images:

1. Keeps the current Churnless ReplicaSet.
2. Copies the new images into that ReplicaSet template.
3. Patches the image fields of its existing Pods.
4. Waits until the kubelet reports the desired images and the Pods are ready.
5. Reports progress through `status.inPlace`.

Pod names do not encode the image revision. The ReplicaSet and Pod objects keep
their names and UIDs, and their IPs remain stable while the Pods remain on the
same nodes.

### Structural revision

A structural revision is a hash of the Pod template after internal Churnless
metadata and all container image values are removed. Any change to that
remaining template creates or selects a different Churnless ReplicaSet.

The Deployment then uses its native-shaped strategy:

- `RollingUpdate` scales new and old ReplicaSets within `maxSurge` and
  `maxUnavailable`.
- `Recreate` scales old ReplicaSets to zero before scaling the new ReplicaSet.
- `paused: true` stops rollout progression.

This boundary is fundamental: image changes are mutable revisions inside a
ReplicaSet; all other Pod-template changes are immutable ReplicaSet revisions.

## In-place rollout behavior

The Deployment calculates image-update parallelism from availability and
`maxUnavailable`, then passes that budget to the current ReplicaSet. If
`maxUnavailable` is effectively zero and all Pods are available, one Pod is
still updated at a time so the rollout can progress.

The ReplicaSet:

1. Counts Pods already updated and Pods still restarting.
2. Selects no more than the allowed number of additional Pods.
3. Patches images by container name.
4. Annotates each Pod with `churnless.io/image-revision`.
5. Considers a Pod complete only when its spec, kubelet-observed image status,
   and readiness all match the desired revision.

ReplicaSet progress is generation-fenced. A Deployment treats progress as zero
until the ReplicaSet has observed its current spec generation and reported the
matching image revision. This prevents an image patch from temporarily
presenting stale completion status.

Scaling and self-healing always create Pods from the latest ReplicaSet
template. Those operations may create a new Pod and therefore do not promise
identity preservation.

## Reconciliation invariants

Every controller change must preserve these invariants:

- A Churnless Deployment owns only Churnless ReplicaSets.
- A Churnless ReplicaSet is the sole controller owner of its Pods.
- Native Deployment and ReplicaSet shadows are never created.
- Image-only changes keep the structural ReplicaSet identity.
- Structural template changes use a distinct ReplicaSet identity.
- A replacement Pod starts with the latest desired image revision.
- Reconciliation is idempotent and safe after partial progress or restart.
- Pod adoption re-reads the ReplicaSet from the API server and verifies its UID
  and deletion state before taking ownership.
- Replica-count decisions use uncached reads so a fast requeue cannot create
  another batch from stale informer state.
- Selectors determine Pod membership; unowned matching Pods may be adopted and
  controlled Pods that stop matching are released.
- Status reflects observed objects and never substitutes for desired state.

Internal labels and annotations under `churnless.io/` are controller
implementation details. They must not become the only source of ownership;
Kubernetes controller owner references remain authoritative.

## Compatibility boundary

The target is behavioral parity for supported native Deployment and ReplicaSet
manifests, with in-place image rollout as the intentional difference. The
current `v1alpha1` implementation still has known gaps:

- Webhook behavior uses the feature-gate defaults compiled into the pinned
  Kubernetes library; it cannot discover different feature-gate settings or
  unrelated admission plugins configured on the hosting API server.
- Stock `kubectl rollout` does not discover the custom GVK.
- Image revisions reuse one ReplicaSet, so they are not separate ReplicaSet
  history entries.
- Progress-deadline enforcement, revision-history cleanup, hash-collision
  handling, and the full set of native failure conditions are incomplete.

These are implementation gaps, not alternate architecture. Close them within
the ownership and revision model above. If a native semantic cannot fit that
model, document the conflict and make an explicit design decision before
changing code.

## Validation contract

Controller changes should be covered at the lowest useful level and must prove
cross-controller behavior in an isolated kind cluster.

The acceptance suite must continue to verify:

- `Deployment.churnless.io → ReplicaSet.churnless.io → Pod`
  controller ownership.
- Absence of native shadow workloads.
- Stable ReplicaSet name and UID for image-only revisions.
- Stable Pod name, UID, and IP during a successful image-only rollout.
- Structural changes create a different ReplicaSet.
- `/scale` changes replica count and newly created Pods use the latest image.
- `/scale` exposes the selector string and a real HPA can change replicas.
- Deployment and ReplicaSet defaults, plus critical invalid-selector behavior,
  match their native GVKs under server-side dry-run.

Run:

```sh
make test
make lint
make test-e2e
```

## Rules for evolving the design

- Update this document in the same change as an architectural or compatibility
  decision.
- Keep the README short and update it only when user-visible behavior changes.
- Keep native types inline unless a documented compatibility need requires a
  different wire representation.
- Prefer extending the existing Deployment/ReplicaSet responsibility split over
  adding a controller that competes for the same object.
- Do not add a workload kind until its ownership, revision boundary, status,
  scaling, and identity guarantees are designed and tested end to end.
