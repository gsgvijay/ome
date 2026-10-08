# Controller-scoped eager model downloads

## Goal

Allow a trusted controller to restrict eager model preparation on a classified
subset of nodes, without modifying ordinary-node eligibility or scheduling.
The agent does not watch endpoints, infer customers, or determine demand.
Existing model creation/update events still initiate preparation.

## Contract

The optional model-agent flags (all must be supplied together) are:

- `--download-scope-node-label`
- `--download-scope-pool-label`
- `--download-scope-verified-annotation`
- `--download-scope-quarantine-taint`

All default to empty (disabled). With them enabled, any configured node marker
classifies a node as scoped. Such nodes require the classification value
`true`, nonempty pool membership, verification value `true`, no quarantine
taint, and a current projection before starting downloads. Classification must
be present at initial node registration; otherwise the node is indistinguishable
from an ordinary node. These are trusted controller/installer-owned fields.

A trusted model controller writes `models.ome.io/eager-download-scope`:

```json
{"version":"v1","uid":"<model UID>","sourceLabels":{"owner":"customer"},
 "allowed":true,"selector":{"matchLabels":{"download-owner":"customer"}}}
```

The agent binds the projection to the exact UID and current source-label values.
An empty selector permits every verified scoped node; `allowed:false` excludes
the model. This selector is ANDed with the existing storage selector/affinity.
It never expands storage eligibility. Unknown/missing/stale evidence blocks new
downloads but is not evidence to delete an existing cache. Confirmed exclusion
uses the existing node-ineligibility deletion path. Model deletion still uses
the normal deletion path, with current-UID revalidation on scoped nodes.
These annotations are an eligibility contract, not an authorization mechanism
for untrusted Kubernetes writers or a replacement for serving admission.

## Lifecycle and scalability

The agent uses its existing two model informers. It reads only its own Node
every 30 seconds while enabled; it adds no cluster-wide node or endpoint watch.
Node verification/label changes replay cached models under the same lock as
model updates, avoiding lost preparation events. No replay is performed for
unchanged selection inputs. Node-read/list errors retry on the next cycle.
Queued work is revalidated before execution; stale deletes cannot delete a
same-name replacement or a model that became eligible again.

Ordinary nodes bypass the new contract. Annotation-only projection updates do
not restart their downloads. No priority queue, endpoint demand, new readiness
state, CRD field, API change, or manager/RBAC change is introduced.

Model readiness does not imply every eligible node already has the weights.
An endpoint may still wait for a cold download after a node joins or a model
changes; the existing readiness and serving workflow remains authoritative.
No preparation deadline is converted into successful serving readiness.

## Rollout and rollback

Publish projections first (inert on old agents), establish bootstrap node
classification and verified ownership, then roll out compatible agents with
the four flags. CPU and GPU agents must use the same contract. Audit existing
classified nodes before enabling the guard. Remove the four flags via an agent
rollout before stopping the publisher; annotations may remain inert. No storage
affinity restoration or endpoint migration is necessary.

## Validation

`go test ./pkg/modelagent ./cmd/model-agent` covers scope configuration,
ordinary-node behavior, storage intersection, shared models, owner isolation,
UID replacement, stale ownership, verification replay, cache preservation,
and confirmed exclusion following uncertain evidence. These tests are local
logic tests; a rollout canary must additionally verify bootstrap labels,
projection convergence, original import readiness, and cold endpoint behavior.
