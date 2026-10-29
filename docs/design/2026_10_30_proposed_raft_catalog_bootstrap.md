# Raft catalog bootstrap

Status: Proposed; revised after startup and protocol review, not accepted yet
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
   mutations, start leader HLC lease renewal on every node independently of
   catalog completion, and enter the control-ready stage below before
   requesting bootstrap timestamps. Bootstrap is local leader work, not an inbound public
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
Raft transport, capability inspection and the explicitly authorized remote
encryption/registration control path below must still make progress. Preserve
existing rotation tests and add catalog-specific admission tests; do not
infer safety merely from a gate being present.

### Control admission before data readiness

Use three monotone startup states: registration-ready after synchronous
epoch checks and watcher arming; control-ready after join readiness and
rotation; data-ready only after local catalog application and route-ID seeding.
Registration-ready permits node writer registration, because rotation can
depend on a follower registering on the default-group leader. Control-ready
permits the following dependencies while ordinary public data remains closed:

- Distribution.GetTimestamp, retaining existing rollout-mode activation
  authorization and request bounds;
- RawKV.RawLatestCommitTS for authoritative leader-fenced group floors only,
  never RawGet, scans, or ordinary RawKV operations;
- Internal.GetPrepareEvidenceState and ForwardAdminProposal only for fully
  decoded existing prepare-evidence activation/seal/fence/release payloads;
  unknown payloads, trailing bytes and other admin proposals remain denied;
- existing read-only capability/state inspection needed by these paths.

Do not assume these methods are already authenticated: current GetTimestamp
and writer registration have no AdminTokenAuth protection, and server TLS
alone does not authenticate a client. Introduce a dedicated
`--startupControlTokenFile`, with a separate startup-control metadata header.
It must not infer authority from or reuse admin, migration or blob-peer
credentials. Preserve any existing underlying admin/method authorization;
the new check only authorizes the narrow startup-stage exception.

Token-bearing network calls require verified TLS and transport-security
enforcement by their per-RPC credentials. Explicit `--adminInsecureNoAuth`
is the only no-auth exception, recorded in acceptance evidence; never infer
it from loopback, absent credentials or a caller flag. Without a control
token or that explicit exception, remote control requests remain unavailable.
Local-only legacy bootstrap can still progress without exposing remote
control RPCs. Configurations requiring remote Phase-D/registration progress
must fail with an actionable configuration error rather than wait forever.

Thread the per-node credentials explicitly through timestamp routing,
group-floor reads, prepare-evidence controls and registration clients. Do
not add process-global mutable credentials, replace another credential's
header, accept caller-supplied bypass booleans, log secrets, or send startup
credentials on unrelated RPCs. Membership, registration epoch, TSO rollout
and payload-specific authorization still apply. Test missing/wrong credentials,
wrong stage, wrong method/payload, TLS failure and mixed caller authority.

On shutdown, unregister/stop new HLC and rotation callbacks, cancel attempts,
and join their owned child proposal goroutines before draining listeners and
closing stores. `eg.Wait()` or context cancellation alone is not proof that
the existing detached proposal/rotation work has returned. Add lifecycle
receipts/joins and tests that hold a proposal during startup cancellation.

### Replicated catalog initialization

- Add a non-mutating, snapshot-scoped catalog bootstrap plan. It accepts
  only version zero with no durable routes, normalizes the configured seed
  routes, assigns nonzero route IDs, and includes version/next-route-id and
  catalog delta metadata with existing codecs. It must not call direct Save.
- The verified catalog-group leader obtains a leader-issued read timestamp
  and commit timestamp through the existing configured timestamp pipeline.
  First allocate a positive timestamp through that pipeline and commit/apply
  an existing read-watermark-advance entry on the catalog group through its
  encryption-aware proposer. Use the resulting applied group watermark,
  not merely the allocated future timestamp, as the exact read snapshot.
  This also avoids treating empty-store StartTS=0 as an auto-allocation request.
  Main's existing one-phase commit fence rejects any older outstanding
  bootstrap proposal at or below that applied watermark. Thus absence-CAS
  does not incorrectly accept a write that lands below a future StartTS.
  Retain the exact read token/voucher and pin through plan creation/dispatch.
  It rechecks empty catalog state at that exact read timestamp and dispatches
  the complete plan through the encryption-aware coordinator/proposer as an
  existing single-group transaction. Catalog version/next-id absence and all
  plan dependencies are validated at apply. No new timestamp opcode, snapshot
  format or independent wall-clock allocation is introduced.
- Before using that advance, negotiate bootstrap protocol version 1 from
  every current catalog-group member, including learners, using a new
  `bootstrap_protocol_version` field on CatalogCapabilitiesResponse (field 5).
  Zero/missing/unsupported capability is not safe support. Recheck membership
  epoch before proposing. This capability promises the existing watermark
  opcode, one-phase fence and new bootstrap behavior; no release/SHA guessing.
  Add generated-code compatibility and mixed-version rejection tests.
- Verify every mutation and read dependency resolves to the catalog group;
  a leader check on only the version key or default group is insufficient.
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
  Include the parked old proposal / future read timestamp counterexample,
  empty-store zero watermark and a separate Phase-D timestamp-group leader.
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
