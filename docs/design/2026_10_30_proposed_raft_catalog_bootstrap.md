# Raft catalog bootstrap

Status: Proposed
Author: bootjp
Date: 2026-10-30

## Context

The serializable isolation audit requires apply verdicts to depend only on
the replicated prefix. PR #1355 still compares against LastCommitTS, but
startup currently calls EnsureCatalogSnapshot before starting the gRPC
transport. An empty catalog calls CatalogStore.Save directly on the runtime
store at LastCommitTS + 1. Raft replay can run concurrently with this local
write. The two-Pebble/FSM regression at cc2a3269 proves divergent RAW and
PREPARE verdicts, including a batch that advances both applied indices while
leaving different user data.

A production caller audit found this bootstrap to be the advancing direct
writer. Route splits, job transitions, promotion completion and migration
RPCs already dispatch through a coordinator/proposer. The S3 blob cache uses
ApplyMutationsPreservingLastCommitTS and is not replicated progress.

This design removes new local catalog bootstrap writes. It does not certify
or repair stores that already diverged, and does not close all A0/A7 gates.
The isolation audit remains partial, and #1355 remains blocked until its
replica-determinism and legacy-state acceptance obligations are satisfied.

## Decisions

### Ordered startup

1. Open stores and construct runtimes, initial configured route cache,
   coordinator, catalog handle and Distribution server without saving catalog
   data. Retain synchronous registration/epoch validation before any server
   serves. Arm the existing registration watchers in their original order.
2. Reuse startupPublicKVGate and the existing startup-gated coordinator to
   start Raft transport with public mutations/reads still unavailable. Do not
   wait for catalog initialization before the listeners needed for quorum.
   Do not add a separate public listener or bypass the authentication/TLS and
   encryption-aware proposer wiring.
3. Retain join readiness and startup rotation ordering. Once rotation allows
   mutations, start leader HLC lease renewal before requesting the bootstrap
   commit timestamp. Bootstrap is local leader work, not an inbound public
   request; it uses the raw configured coordinator, never an exemption that
   an arbitrary forwarded request can claim.
4. Initialize/wait for the durable catalog through Raft, then apply that
   snapshot to the local engine and seed key-visualization route IDs. Only
   then start catalog-dependent watchers, split/migration background work,
   public protocol serving and FUSE, and mark the public gate ready.
5. Context cancellation or a terminal bootstrap failure drains only startup's
   owned goroutines/listeners before stores close. Never mark ready after a
   failed or indeterminate bootstrap.

The gate must continue covering RawKV/TransactionalKV, Internal.Forward,
admin forwarding, distribution/migration mutations and S3 chunk pushes.
Raft transport, authenticated capability inspection and the existing local
encryption/registration proposal path must still make progress. Preserve
existing rotation tests and add catalog-specific admission tests; do not
infer safety merely from a gate being present.

### Replicated catalog initialization

- Add a non-mutating, snapshot-scoped catalog bootstrap plan. It accepts
  only version zero with no durable routes, normalizes the configured seed
  routes, assigns nonzero route IDs, and includes version/next-route-id and
  catalog delta metadata with existing codecs. It must not call direct Save.
- The verified catalog-group leader obtains a leader-issued read timestamp
  and commit timestamp through the existing configured timestamp pipeline.
  It rechecks empty catalog state at that exact read timestamp and dispatches
  the complete plan through the encryption-aware coordinator/proposer as an
  existing single-group transaction. Catalog version/next-id absence and all
  plan dependencies are validated at apply. No new timestamp opcode, snapshot
  format or independent wall-clock allocation is introduced.
- Followers do not generate timestamps, persist seed routes, or forward
  arbitrary client traffic through a startup exception. They wait until the
  replicated catalog is locally applied, load its authoritative snapshot,
  and apply it to the route cache. The winning durable seed is authoritative;
  a follower must not overwrite it from its local engine statistics.
- A lost reply, leader change or write conflict triggers state reconciliation,
  not an assumption that a fresh write is safe. Reload the durable version;
  if populated, use it. Otherwise repeat leader verification and rebuild the
  plan from a fresh snapshot. Bounded per-attempt contexts and cancellation
  keep startup interruptible; do not return a false success on retry exhaustion.
- Populated catalogs are loaded without mutation, preserving their version,
  IDs, route histories and migration ownership data. Reject malformed catalog
  state rather than repairing it implicitly. An uninitialized catalog is not
  permission to erase user data, Raft metadata or membership.
- Production server startup must not retain a fallback to direct Save.
  Direct CatalogStore library helpers can remain for isolated tests/tools,
  but their callers must be audited and the production path pinned by tests.

### Existing stores and scope

Loading a populated catalog does not prove its historical creation was
replicated or that all replicas have equal data/watermarks. This PR cannot
retroactively establish that proof. Preserve data and document recovery as a
separate explicit quiesce/reconcile/restore operation, not an automatic wipe,
local-max initialization, volatile watermark, or unsupported safety claim.
Before enabling the new RAW/PREPARE fence on an existing group, the parent
audit must establish an agreed replicated baseline and handle old prepared
transactions and mixed-version members. Those remain merge gates for #1355.

## Acceptance

- Runtime-red production-path regression: setup of the catalog handle must
  not issue any direct advancing store mutation. Retain the original
  local-bootstrap divergence reproduction as diagnostic evidence, not a
  passing proof after merely changing its assertions.
- Three-node cold startup must form quorum and bootstrap without deadlock;
  inspect actual catalog mutations in committed entries, durable versions,
  route IDs and local applied state, not just successful TCP connections.
- Race two leader attempts and lose a success reply. Exactly one version-1
  bootstrap wins; retries load it without another catalog version advance.
- Delay a follower's catalog apply and prove every public write/read gate
  stays closed until its local catalog is usable. Transport still progresses.
- Verify restart/join/populated catalog, shutdown/cancellation, leadership
  loss, malformed catalog, multi-group catalog routing, encryption off/on,
  writer-registration gates and rotate-on-startup.
- Audit every changed startup helper and direct mutation caller. Run full
  root/KV/distribution/store race suites, unfiltered lint, applicable Jepsen,
  and final-push code/security review and CI before merge.
- Record data loss, distributed concurrency, performance, consistency and
  coverage as five independent review passes. Do not check the parent
  isolation or all-design-doc goal complete from this prerequisite alone.
