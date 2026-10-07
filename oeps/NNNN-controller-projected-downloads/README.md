# Controller-projected model download scope

An external controller can narrow model storage eligibility to selected node
pools while preserving the original eligibility of ordinary nodes. The agent
does not watch endpoints, resolve customer identity, or change queue priority.
This proposal documents the implementation ported from the downstream image
overlays; it does not introduce a second download-selection algorithm.

## Agent contract

Both arguments are empty by default, preserving existing behavior:

```text
--download-scope-node-label=example.com/scoped-node
--download-scope-pool-label=example.com/pool
```

The existing Helm `modelAgent.env` facility can set the equivalent
`DOWNLOAD_SCOPE_NODE_LABEL` and `DOWNLOAD_SCOPE_POOL_LABEL` variables. Raw
DaemonSets can pass the arguments directly. Configure every deployed GPU/CPU
agent consistently. The deployment must select an image built from this source;
existing chart image defaults are not evidence of feature support.

The controller preserves `spec.storage.nodeSelector` and intersects each original
required affinity term with ordinary nodes or its selected pool set. It atomically
publishes that affinity and the `models.ome.io/download-scope` JSON annotation:

- `version`: `v1`;
- `uid`: the current model UID;
- `applied`: the exact published NodeAffinity;
- `pools`: allowed values of the configured pool label;
- controller-owned baseline fields may support reversible restoration.

For classified nodes, new downloads require both existing storage eligibility and
consistent scope evidence. Missing identity or incomplete evidence blocks new
downloads without treating uncertainty as permission to delete a cache. A known
eligibility loss can remove local state; queued work is revalidated against the
current model UID and eligibility. Node identity changes replay eligibility.

Nodes with neither label retain ordinary eligibility. Consequently, scoped nodes
must register with their classification before agents observe them. Removing both
labels is not a safe quarantine mechanism. This contract selects downloads; it is
not a workload-admission or tenant-authorization boundary.

## Ownership and migration

Pool selection, reference retention, optimistic updates, and restoration remain
the external controller's responsibility. The agent adds no endpoint clients or
endpoint readiness dependency. Existing deletion, reuse and cancellation paths
remain in place.

The manager retains compatibility fields and retirement-only handling for old
endpoint-demand observations. Ordinary models without such observations are a
no-op. Retirement requires a compatible, namespace-scoped consumer inventory
covering GPU/CPU DaemonSets and old/surge Pods; it uses uncached reads and does not
require all agents to be Ready. It produces no new endpoint demand.

Disable the agent guard through a rollout, then let the owning controller restore
the original affinities before removing its lifecycle permissions. Clearing agent
arguments alone does not undo persisted affinity restrictions.

## Validation

`pkg/modelagent/download_scope_test.go` covers ordinary and scoped eligibility,
incomplete evidence, node promotion, same-name recreation, eligibility-loss cleanup,
queued-task revalidation and concurrent refresh. `pkg/modeldownloadpolicy` and
BaseModel controller tests cover consumer inventory and safe retirement. Manager
readiness tests preserve the downstream requirement to sync controller sources
before advertising readiness, independently of liveness.

The accompanying source migration keeps runtime behavior equivalent to the old
overlays after upstream changes are accounted for. Generated APIs and manifests
must be regenerated, and image consumers must validate their schemas/RBAC against
the exact source commit before updating their image pin.
