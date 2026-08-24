# exoscale-sks-node-operator

Keeps Exoscale SKS worker nodes on the current Kubernetes patch version, inside a
maintenance window.

Exoscale SKS upgrades the **control plane** only. Worker nodes keep the kubelet they
booted with, indefinitely: SKS-managed nodepools have no rotation mechanism at all, and
Karpenter pools are commonly provisioned with `expireAfter: Never`. Rotating them is
otherwise a manual runbook step — evict the workers and wait for replacements to come
back on the newer version. This operator does that on a schedule.

Scope is deliberately narrow: **Exoscale only**, one cluster (the one it points at),
**node replacement only**. It does not upgrade control planes and does not remediate
unhealthy nodes.

## How a node is chosen

The operator resolves a **target version** and replaces only nodes strictly below it:

```
target = max( control-plane version,                 # SKSCluster.Version
              newest version Exoscale offers,        # ListSKSClusterVersions
              highest kubelet among Ready nodes )    # observed
        — restricted to the control plane's own major.minor
```

Two properties of this are worth stating outright, because the obvious alternatives are
both wrong:

- **A node ahead of the control plane is not outdated.** SKS boots new nodes on the
  newest patch of the minor, so a fresh node legitimately runs *ahead* of the control
  plane — v1.36.3 nodes against a v1.36.2 control plane is the healthy steady state.
  Treating that as drift would replace the node, get another node one patch ahead, and
  loop forever.

- **Consulting the versions Exoscale offers is what makes progress possible.** Deriving
  the target from observed kubelets alone means a cluster where *every* node is outdated
  resolves `target == control plane`, nothing qualifies, and the fleet silently never
  updates.

Restricting to the control plane's minor keeps a stray node from dragging the fleet
across a minor boundary, where kubelet would lead the API server past the supported skew.

**Probe and halt.** A version being offered as a control-plane version is not proof a
node image exists at that patch. Under `Surge` the new node is checked before the old one
is touched: if it comes up below target, the surge is undone, the outdated node is left
alone, and the policy goes `Degraded/TargetUnreachable` and stops. The cost of an
unobtainable target is one node created and destroyed, not the whole fleet rotated.

`versionPolicy.source` selects the strategy: `Auto` (above), `ControlPlane`
(conservative — only nodes below the control plane), or `Pinned`.

## How a node is replaced

Nodes are classified from their own labels — no API walk:

| | discriminator | replacement strategy |
|---|---|---|
| **SKS-managed** | `node.exoscale.net/nodepool-id` | surge the pool, drain, then evict the instance |
| **Karpenter-managed** | `karpenter.sh/nodepool` + a `NodeClaim` owner reference | drain, then delete the `NodeClaim` |

The SKS flow is `Pending → ScalingUp → WaitingForNode → Cordoning → Draining → Evicting
→ Completed`. The pool is always grown by one first, because
**`EvictSKSNodepoolMembers` shrinks the pool by one** — scaling up first and evicting
after leaves the pool at the size it started at and never dips capacity.

Karpenter's flow is `Pending → Cordoning → Draining → Deleting → Completed`. Karpenter
reprovisions on pending pods, so there is no surge.

Each rotation gets its own `NodeReplacement` object holding the phase, because the flow
spans minutes of asynchronous cloud operations and must survive a restart — including the
operator draining the node it is itself running on. Inspect them with:

```sh
kubectl get nodereplacements
```

Draining delegates to `k8s.io/kubectl/pkg/drain`, so PodDisruptionBudgets are honoured
and DaemonSet, mirror and static pods are filtered rather than blocking forever.

## Things it is careful about

- **The cluster-autoscaler also owns SKS pool sizes.** `ScaleSKSNodepool` takes an
  **absolute** size, so the current size is re-read immediately before scaling and again
  before evicting. A pool already at the autoscaler's own maximum will be surged past it
  anyway and the autoscaler will trim it back on its own terms.
- **Nodepools mid-operation are left alone.** Nothing is scaled or evicted unless the pool
  reports `running`.
- **Failures roll back, and never guess.** A failed replacement uncordons what it cordoned,
  and removes the node it surged in *only* if it can positively identify it — draining it
  first. Otherwise the pool is left one node larger with a `SurgeNotReclaimed` warning, for
  the autoscaler to reclaim gracefully: scaling down blindly lets Exoscale choose the
  victim, and it can pick a node still carrying undrained workload.
- **Waiting is bounded by time, never by an attempt count.** Status writes re-trigger the
  controller through its own watch, so a counter burns through in seconds rather than
  minutes.
- **In-flight work always finishes.** Replacements only *start* inside a maintenance
  window; one already running continues past the end of it, because a half-drained node or
  a surged pool is worse than overrunning.
- **The operator replaces its own node last**, so it keeps handing out work for as long as
  possible. Set `NODE_NAME` (downward API) or `--self-node-name`.
- **Karpenter overlap.** Exoscale runs Karpenter in the managed control plane and its
  provider already detects image drift, gated by the NodePool's own disruption budgets.
  Deleting a `NodeClaim` is an *involuntary* disruption and **bypasses those budgets**, so
  this policy's window and concurrency limits are the only gate on that path. Nodes
  Karpenter is already retiring (`karpenter.sh/disrupted`, `do-not-disrupt`, or
  terminating) are skipped. Set `strategies.karpenterManaged.enabled: false` to leave
  Karpenter pools entirely to Karpenter.

Opt a single node out at any time with `sks.whizus.com/skip: "true"`.

## Configuration

A full, commented example is in [`config/samples`](config/samples/sks_v1alpha1_nodeupgradepolicy.yaml):

```yaml
apiVersion: sks.whizus.com/v1alpha1
kind: NodeUpgradePolicy
metadata:
  name: default
spec:
  dryRun: false
  maintenanceWindows:
    - schedule: "0 2 * * *"   # cron marks the window START
      duration: 1h
      timeZone: Europe/Vienna
  rollout:
    maxUnavailable: 1
  drain:
    enabled: true
  strategies:
    sksManaged:
      enabled: true
```

With `dryRun: true` the operator evaluates everything and reports it through status and
events without cordoning, draining, scaling or evicting. `paused: true` stops new
replacements while letting in-flight ones finish.

## Credentials

`EXOSCALE_API_KEY`, `EXOSCALE_API_SECRET` and `EXOSCALE_ZONE`. The API key needs only:

```
list-sks-clusters      get-sks-cluster    list-sks-cluster-versions
get-sks-nodepool       scale-sks-nodepool evict-sks-nodepool-members
get-instance           get-operation
```

No `upgrade-sks-cluster`, no delete. In-cluster they come from a Secret named
`exoscale-sks-node-operator-credentials` via `envFrom`.

## Running it

Out of cluster, against whatever `KUBECONFIG` points at:

```sh
export KUBECONFIG=...            # or pass --kubeconfig
export EXOSCALE_API_KEY=...  EXOSCALE_API_SECRET=...  EXOSCALE_ZONE=de-muc-1
make install-crds
make run-local
```

In cluster:

```sh
make docker-build docker-push IMG=ghcr.io/whizus/exoscale-sks-node-operator:dev
make deploy IMG=ghcr.io/whizus/exoscale-sks-node-operator:dev
```

`make build-installer` emits a single `dist/install.yaml` for GitOps.

## Development

```sh
make test      # unit tests + envtest
make lint      # golangci-lint
make build     # binary
```

The Exoscale API is behind a small interface (`internal/exoscale`) with a fake, so the
controllers are tested without network access. `test/crds` holds a minimal `NodeClaim`
CRD so the Karpenter path is covered without vendoring Karpenter's API module.

## Metrics

`sks_node_operator_` — `outdated_nodes`, `managed_nodes`, `target_version_info`,
`in_maintenance_window`, `active_replacements`, `replacements_total`,
`replacement_duration_seconds`, `nodepool_state`, `nodepool_size`.

The last two exist so a nodepool stuck outside `running`, or one whose size drops toward
zero, is alertable.
