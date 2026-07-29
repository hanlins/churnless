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
mutable Pod-template updates should be applied to existing Pods instead of
replacing those Pods whenever Kubernetes permits it.

The project aims to provide:

- Native-shaped `Deployment` and `ReplicaSet` resources under a distinct API
  group.
- A migration path where a supported native manifest works after changing only
  `apiVersion`.
- Native workload behavior for scaling, self-healing, rollout strategies,
  readiness, availability, status, and controller ownership.
- Standard `/scale` subresources so `kubectl scale` and HPA work normally.
- Stable Pod name, UID, and IP during successful metadata, image, and supported
  resource updates.
- Clear controller responsibility boundaries and no competing shadow
  controllers.

Pod identity preservation is best effort, not an availability guarantee. A Pod
can still be replaced because of eviction, node failure, deletion, scheduling,
scaling, or a structural template change.

## Non-goals

- Mutating arbitrary Pod fields in place. Churnless treats Pod-template labels
  and annotations plus regular and init-container images as in-place fields.
  Regular-container CPU and memory are opt-in, best-effort fields.
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
| Churnless ReplicaSet | Selects and adopts Pods, maintains replica count, creates and deletes Pods, applies supported Pod metadata, image, and resource changes in place, and reports status. |
| Migration engine | Transfers a stable Deployment and its live Pods between native Kubernetes and Churnless ownership chains when driven by the kubectl plugin. |
| Admission webhooks | Apply and validate the native workload semantics that CRD schemas do not inherit from built-in API storage. |
| kubelet | Observes patched images and resource resize requests, then restarts or resizes affected containers as required. |

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
    revision: <in-place-revision>
    updatedReplicas: 2
    readyUpdatedReplicas: 2
```

`status.selector` backs the standard `/scale` subresource. HPA targets the
Churnless GVK directly and writes `spec.replicas` through that subresource.
Unlike the built-in Deployment REST storage, generic CRD scale storage cannot
derive a selector string from the structured `spec.selector`; the explicit
status field is therefore required by the CRD's `labelSelectorPath`.

`ReplicaSet.status.inPlace` is the child controller's mutable-field progress
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

The Deployment and ReplicaSet webhooks run for creates and spec-changing
updates. Metadata-only updates are excluded with admission `matchConditions`
because the current defaulting and validation contracts operate only on
`spec`. This keeps desired-state changes fail-closed while allowing Kubernetes
metadata, including migration annotations and owner bookkeeping, to remain
writable if the Churnless admission server is unavailable. Adding any future
webhook behavior for metadata requires revisiting this condition.

## Revision model

Churnless separates a Pod template into two kinds of revision.

### In-place revision

An in-place revision contains:

- Pod-template labels and annotations.
- The names and images of regular and init containers.
- Regular-container CPU and memory requests/limits when the workload opts in
  with `churnless.io/in-place-resources: best-effort`.

Changing only those fields:

1. Keeps the current Churnless ReplicaSet.
2. Copies the new mutable values into that ReplicaSet template.
3. Patches supported metadata and image fields on its existing Pods.
4. Uses the Pod `resize` subresource for opted-in CPU and memory changes.
5. Waits until the desired Pod spec and runtime-observed state converge.
6. Reports progress through `status.inPlace`.

Pod names do not encode the in-place revision. The ReplicaSet and Pod objects
keep their names and UIDs, and their IPs remain stable while the Pods remain on
the same nodes.

Metadata synchronization tracks only keys originating in the Pod template.
Removing a previously managed key removes it from the Pod, while labels and
annotations injected by admission or other controllers are preserved.

Resource updates are deliberately best effort. Only regular-container CPU and
memory values are candidates, and the opt-in should normally be set when the
workload is created. If the API server, node, QoS rules, or requested values do
not permit an in-place resize, Churnless deletes and recreates that Pod from the
same ReplicaSet under the existing rollout availability budget. Init-container,
extended-resource, resize-policy, and Pod-level resource changes remain
structural.

### Structural revision

A structural revision is a hash of the Pod template after internal Churnless
metadata and all supported in-place values are removed. Any change to an
immutable field in the remaining template creates or selects a different
Churnless ReplicaSet.

The Deployment then uses its native-shaped strategy:

- `RollingUpdate` scales new and old ReplicaSets within `maxSurge` and
  `maxUnavailable`.
- `Recreate` scales old ReplicaSets to zero and waits for their active and
  terminating Pods to disappear before scaling the new ReplicaSet.
- `paused: true` stops rollout progression.

This boundary is fundamental: only the explicitly supported fields are mutable
inside a ReplicaSet; all other Pod-template changes are immutable ReplicaSet
revisions.

### Explicit redeploy

A redeploy deliberately salts the structural revision even when the Pod
template is otherwise unchanged. Churnless recognizes Kubernetes' standard
restart annotation as a workload-level redeploy token:

```sh
kubectl annotate deployment.churnless.io/web \
  kubectl.kubernetes.io/restartedAt="$(date -u +%Y-%m-%dT%H:%M:%SZ)" --overwrite
```

Native `kubectl rollout restart` writes the same key into a built-in
Deployment's Pod template, and Churnless recognizes that placement too.
However, the kubectl subcommand's compiled-in scheme rejects custom Deployment
GVKs before sending a request, so the literal
`kubectl rollout restart deployment.churnless.io/web` command cannot operate on
the CRD. The generic `kubectl annotate` command above does not have that client
limitation.

Existing automation can alternatively set the Churnless-specific alias:

```sh
kubectl annotate deployment.churnless.io/web \
  churnless.io/redeploy-at="$(date -u +%Y-%m-%dT%H:%M:%SZ)" --overwrite
```

Either annotation creates a new Churnless ReplicaSet and new Pods according to
the configured rollout strategy.

## In-place rollout behavior

The Deployment calculates in-place-update parallelism from availability and
`maxUnavailable`, then passes that budget to the current ReplicaSet. If
`maxUnavailable` is effectively zero and all Pods are available, one Pod is
still updated at a time so the rollout can progress.

The ReplicaSet:

1. Counts Pods already updated and Pods still restarting.
2. Selects no more than the allowed number of additional Pods.
3. Patches supported metadata and images, and requests opted-in resource
   resize by container name.
4. Annotates each Pod with `churnless.io/in-place-revision`.
5. Considers a Pod complete only when its mutable spec, kubelet-observed state,
   and readiness all match the desired revision.

ReplicaSet progress is generation-fenced. A Deployment treats progress as zero
until the ReplicaSet has observed its current spec generation and reported the
matching in-place revision. This prevents a Pod patch from temporarily
presenting stale completion status.

Scaling and self-healing always create Pods from the latest ReplicaSet
template. Those operations may create a new Pod and therefore do not promise
identity preservation.

## Takeover and handoff

The public transfer surface is the kubectl plugin:

```sh
kubectl churnless takeover deployment.apps/web
kubectl churnless handoff deployment.churnless.io/web
```

The argument names the source API; `deploy/web` and `cdeploy/web` are the
corresponding short forms.

Each command records durable desired-controller state, repeatedly steps the
reusable engine through an uncached API client, reports progress, and waits
for completion. Migration state lives in Kubernetes rather
than a local checkpoint, so interruption or timeout is safe: rerunning the same
command resumes the operation. Optimistic patches and UID/resource-version
delete preconditions make retries safe.

There is intentionally no continuously running migration controller in the
baseline architecture. Raw transfer annotations are internal checkpoints, not
independent triggers. This makes emergency handoff depend only on the plugin
binary, API server, and native controllers. The engine keeps a
controller-neutral `Request`/`Advance` boundary so a future optional
controller can be a thin scheduling adapter without duplicating cutover logic.

If another plugin invocation requests the opposite controller, an older
synchronous command reports that it was superseded and stops driving instead
of overwriting the newer intent. A same-name target Deployment must not already
exist outside valid transfer state, and migration does not start from a source
that is already deleting.

Takeover requires a complete, stable native rollout because its normal purpose
is an identity-preserving move from a known-good baseline.

Handoff is always allowed to start. The engine creates the target under the
other GVK with the source spec, labels, and non-migration annotations. It
records the source UID, generation, original paused state, and `preserve` or
`recovery` mode on the target; the target GVK identifies the direction. A
source spec change during migration blocks cutover so two
different desired states cannot be silently combined.

Cancellation is symmetric while the source still exists and is not deleting:
running the opposite plugin command restores dependent references and source
migration metadata before removing the target, keeping the source controller
authoritative.

Before cutover, the target creates its ReplicaSet and temporary Pods. The
engine then:

1. Retargets supported GVK-specific dependents to the target Deployment.
2. Adds the target ReplicaSet's required labels to the live source Pods.
3. Gives source Pods a higher deletion preference than temporary target Pods.
4. Orphan-deletes the source Deployment and its ReplicaSets.
5. Explicitly adopts the now-unowned source Pods into the target ReplicaSet.
6. Waits for the target rollout to settle, removes temporary migration
   metadata, and restores the source's original paused state on the target.

This ordering keeps the source authoritative until the target ownership chain
is ready and normally retains each ready source Pod's name, UID, and IP.
Retention remains best effort: eviction, deletion, node failure, or another
controller acting during cutover can still replace a Pod. Temporary target
Pods also mean Pod objects and resource requests can briefly exceed the
Deployment replica count, although pending or less-ready temporary Pods are
preferred for deletion after adoption.

If the desired controller changes after source deletion has already started,
the current transfer can no longer be cancelled safely. The engine copies
the new request to the surviving target, finishes the current ownership
bookkeeping, and immediately starts the reverse transfer. In particular, a
native fallback requested during takeover does not wait for a newly unhealthy
Churnless rollout before starting recovery.

If handoff starts while the Churnless source is incomplete, the recorded mode
is `recovery`. The native Deployment and current native ReplicaSet are allowed
to create their desired Pod count, but source Pods are not relabeled or
adopted. After dependents point to the native GVK, the engine
foreground-deletes the Churnless hierarchy and lets garbage collection remove
its Pods before declaring migration complete. Recovery does not wait for
native Pods to become Ready: the goal is to restore native desired-state
ownership even when the copied workload spec is itself unhealthy. Pod identity
is not preserved in this mode. A preserve-mode handoff also switches to
recovery if the Churnless source becomes incomplete before cutover, so fallback
cannot remain stuck waiting on the controller it is intended to replace. The
plugin reports progress from persisted state and does not require Event-create
permission.

Emergency handoff works while the entire Churnless manager is down:
metadata-only updates bypass its webhooks and native controllers warm the
destination. The preserve and recovery rules above still apply. Takeover
requires healthy Churnless admission, Deployment, and ReplicaSet controllers.

Migration currently covers Deployments, not standalone ReplicaSets. The
engine rewrites these namespaced workload references when they point to
the same-name source Deployment:

- `autoscaling/v2` HorizontalPodAutoscaler `spec.scaleTargetRef`
- `autoscaling.k8s.io/v1` VerticalPodAutoscaler `spec.targetRef`
- `keda.sh/v1alpha1` ScaledObject `spec.scaleTargetRef`

KEDA's omitted `apiVersion` and `kind` defaults are interpreted as
`apps/v1 Deployment`. Each patch is idempotent and matches the source
`apiVersion`, kind, and name before changing the `apiVersion`. KEDA is handled
before HPA so its generated HPA follows the same target. A missing optional VPA
or KEDA API is ignored, while an installed API that cannot be listed or patched
blocks cutover instead of leaving a stale target.

Services, selector-based PodDisruptionBudgets, and other selector-based
resources continue to select Pods by label throughout the transfer and do not
need a GVK rewrite. Arbitrary custom-resource reference fields cannot be
discovered safely; policy or automation outside the supported references above
must be reviewed separately.

## Reconciliation invariants

Every controller change must preserve these invariants:

- A Churnless Deployment owns only Churnless ReplicaSets.
- A Churnless ReplicaSet is the sole controller owner of its Pods.
- Native Deployment and ReplicaSet shadows are never created.
- Supported in-place changes keep the structural ReplicaSet identity.
- Structural template changes use a distinct ReplicaSet identity.
- Explicit redeploy tokens always use a distinct ReplicaSet identity.
- A replacement Pod starts with the latest desired in-place revision.
- Reconciliation is idempotent and safe after partial progress or restart.
- Deleting Churnless Deployments and ReplicaSets never create or update
  dependents, allowing foreground recovery handoff to terminate the hierarchy.
- Pod adoption re-reads the ReplicaSet from the API server and verifies its UID
  and deletion state before taking ownership.
- Identity-preserving migration never orphan-deletes the source hierarchy until the
  supported dependent references point to the target GVK, the target ReplicaSet
  exists, and every observed source Pod matches its selector.
- Recovery handoff never foreground-deletes the Churnless hierarchy until
  supported dependents point to the native GVK and the native ReplicaSet has
  created its desired Pod count.
- Migration state is recoverable from the target Deployment and source
  ReplicaSet annotations after plugin interruption.
- Durable migration state has an explicit version and fails closed on an
  unknown version, mode, or desired-controller value before ownership is
  orphaned. The engine live-reads both Deployments again immediately before
  source deletion, whose UID and resource-version preconditions close a racing
  reversal.
- Per-Pod deletion cost is recorded before migration preference is applied and
  restored exactly after completion or cancellation.
- The kubectl driver owns scheduling while the reusable migration engine owns
  all transfer semantics; a future controller must adapt that engine rather
  than implement a second cutover path.
- The baseline manager does not watch transfer annotations or receive
  transfer-only Deployment, autoscaler, KEDA, VPA, or Event permissions.
- Replica-count decisions use uncached reads so a fast requeue cannot create
  another batch from stale informer state.
- ReplicaSet Pod discovery combines a live selector-scoped list with a cached
  controller-UID index. The index finds controlled Pods that stopped matching;
  those cached-only candidates are re-read live before release. This avoids a
  namespace-wide live Pod list without weakening replica-count or adoption
  safety.
- Selectors determine Pod membership; unowned matching Pods may be adopted and
  controlled Pods that stop matching are released.
- Status reflects observed objects and never substitutes for desired state.

Internal labels and annotations under `churnless.io/` must not become the only
source of ownership; Kubernetes controller owner references remain
authoritative. The documented `in-place-resources` and `redeploy-at`
annotations are public controls. Transfer annotations, including
`churnless.io/controller`, are plugin-managed implementation details.

## Compatibility boundary

The target is behavioral parity for supported native Deployment and ReplicaSet
manifests, with in-place image rollout as the intentional difference. The
current `v1alpha1` implementation still has known gaps:

- Webhook behavior uses the feature-gate defaults compiled into the pinned
  Kubernetes library; it cannot discover different feature-gate settings or
  unrelated admission plugins configured on the hosting API server.
- Built-in `kubectl rollout` subcommands use a compiled-in typed scheme and
  cannot operate directly on the Churnless CRD. Use the documented standard
  restart annotation with generic `kubectl annotate`.
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
- Stable ReplicaSet name and UID for supported in-place revisions.
- Stable Pod name, UID, and IP during successful metadata and image updates.
- Best-effort CPU/memory changes either retain Pod identity through `resize` or
  converge by controlled replacement when resize is rejected.
- Structural changes create a different ReplicaSet.
- `kubectl.kubernetes.io/restartedAt` and `churnless.io/redeploy-at` create a
  different ReplicaSet and new Pods.
- RollingUpdate stays within its availability and surge fenceposts, and
  Recreate never runs old and new revisions at the same time.
- `/scale` changes replica count and newly created Pods use the latest image.
- `/scale` exposes the selector string and a real HPA can change replicas.
- Plugin takeover and manager-down healthy handoff preserve Pod name, UID, IP,
  and the exact Deployment/ReplicaSet ownership chain while a real HPA follows
  both directions; metadata updates remain available and spec changes remain
  fail-closed during the outage.
- With the manager still scaled to zero, the plugin can also return an
  incomplete Churnless rollout to native ownership without waiting for
  Churnless completion; this recovery path intentionally replaces Pods.
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
