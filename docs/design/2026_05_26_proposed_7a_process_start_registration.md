# Stage 7a — process-start writer registration + coordinator first-write gate

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-26 |
| Parent design | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 writer registry, §5.2 process-start path) |
| Builds on | 6A (`ApplyRegistration` §4.1 case 1–4 dispatch), 5B (`RegisterEncryptionWriter` RPC + leader Proposer wiring), 6C-3 (`ErrLocalEpochRollback`), 6D-6c-2 (`BumpLocalEpoch`, `DeterministicNonceFactory`, keystore hydration) |
| Slice of | Stage 7 (writer registry + deterministic nonce). 7b (post-rotation re-registration) and 7c (ConfChange-time registration) follow. |

## 0. Why this slice exists

6D-6c-2 wired the production storage write path: on a fresh process
load with an active storage DEK, `BumpLocalEpoch` advances and fsyncs
the §4.1 `local_epoch`, and the `DeterministicNonceFactory` is pinned
to that bumped epoch. But the **writer registry** — the Raft-replicated
record that makes the 16-bit `node_id` uniqueness invariant
unconditional (§4.1) — is not updated on restart. The deferred gap,
called out in the 6D-6c-2 partial doc and the Stage-6D parent doc:

> the multi-node-churn registration-before-first-write nonce gate is a
> Stage 7 item.

7a closes the **process-start** half of that gap (§4.1 "Process start
under an existing DEK"): a node that restarts under an already-active
storage envelope must register its bumped `local_epoch` in the writer
registry **before it originates any encrypted write**, and must skip
the registration when its registry slot already records the current
epoch (the bootstrap cohort / no-op-restart case). 7b and 7c cover the
post-rotation and ConfChange-time paths.

## 1. The §4.1 / §5.2 contract (process-start path)

Restating the authoritative parent-doc contract that 7a implements:

1. The trigger fires **only** on a fresh process load whose
   `BumpLocalEpoch` persisted a value **strictly greater** than the
   registry's current `last_seen_local_epoch` for this
   `(full_node_id, active_storage_dek_id)`.
2. In that case the node proposes
   `RegisterEncryptionWriter(dek_id, full_node_id, local_epoch)`
   through the default group's leader **before the coordinator accepts
   any client write that would land as an encrypted entry**.
3. It does **NOT** fire on the bootstrap cohort's first observation of
   the `enable-storage-envelope` cutover — those nodes were already
   registered by the §5.6 bootstrap batch at the same `local_epoch`, so
   a duplicate registration with an unchanged epoch would be rejected
   by §4.1 case 3 (`ErrLocalEpochRollback`) and brick a healthy
   rollout. The implementation checks the registry on coordinator
   startup and **skips the propose** if its slot already contains the
   current `local_epoch` (§4.1 case 2-idempotent territory).
4. A node booting in **Phase 0** (sidecar present, but
   `StorageEnvelopeActive == false`) defers registration until both the
   bootstrap and the cutover entries commit — the registry keys do not
   yet exist cluster-wide. FSM apply may still run on leader-proposed
   entries during this window (decrypted with the sidecar DEKs the node
   already holds); only **self-originated** encrypted writes are gated.

## 2. Why registration must precede self-originated encrypted writes

FSM apply runs `kvFSM.Apply → store.ApplyMutationsRaft → encryptForKey
→ nonceFactory.Next()` on **every** node, so each node encrypts the
committed value with **its own** `node_id ‖ local_epoch ‖ write_count`
nonce. The registry is the cluster-wide ledger of which `node_id` has
written under which DEK at which epoch; the §4.1 `ErrLocalEpochRollback`
/ `ErrNodeIDCollision` guards rely on it. Registering the bumped epoch
before this node originates writes under the active DEK keeps the
ledger ahead of the nonces this load will emit, so a future
stale-disk restore (epoch ≤ registry) is detected rather than silently
recycling nonces.

## 3. Proposed implementation

### 3.1 Registration decision (startup, post-`buildShardGroups`)

The bumped epoch comes from 6D-6c-2's `prepareStorageNonceEpoch` (run
inside `buildEncryptionWriteWiring`, before `buildShardGroups`). The
**registration-intent decision**, however, **cannot** live there:

> **Timing constraint (review finding #1).** The intent decision reads
> the writer registry, which is backed by the default group's Pebble
> `MVCCStore`. That store is not open until `buildShardGroups` returns.
> So the intent decision is a **post-`buildShardGroups` step**, wired
> into the same `run()` phase as `chainEncryptionStartupGuard` (which
> already runs after the engines/stores open), **not** into
> `buildEncryptionWriteWiring`. The epoch bump (write-path nonce
> safety) stays pre-`buildShardGroups`; the registry intent (control
> plane) runs after.

**Ordering assumption (review #1).** This intent logic runs *after*
the §9.1 `CheckLocalEpochRollback` startup guard has passed (it runs in
the same post-engine phase and halts the process on
`ErrLocalEpochRollback`). Consequently `epoch < registry.last_seen`
(rollback) and the no-row-post-cutover case are **unreachable** here —
the guard already rejected them. A **missing** registry row is treated
as `last_seen = 0`, which falls into the `< epoch` "propose" branch
(the legitimate freshly-joined writer whose row the guard tolerated
pre-cutover, or the first registration under a new DEK). The intent
therefore only ever sees `==` (skip) or `<` (propose).

Intent:

```text
if !StorageEnvelopeActive            -> defer (Phase 0; §1.4)
if registry.last_seen(node,DEK) == epoch -> skip   (already registered; §1.3)
if registry.last_seen(node,DEK) <  epoch -> propose RegisterEncryptionWriter
                                            (the §1.1/1.2 trigger;
                                             missing row == last_seen 0)
```

The registry read is local (Pebble, no Raft round-trip — the registry
is replicated state already present on this node post-bootstrap),
reusing the same `RegistryKey(dek_id, NodeID16(full_node_id))` lookup
`ApplyRegistration` / `GuardLocalEpochRollback` use.

**Registry handle access (review finding #2 — as built).** The intent
step reads the registry through `store.WriterRegistryFor(
shardGroups[cfg.defaultGroup].Store)`. The proposal-round plan was to
thread the per-shard handle out of `buildShardGroups`, but
`WriterRegistryFor` returns a **stateless** `*pebbleWriterRegistry`
wrapper over the shared `*pebbleStore` — a second wrapper for a
read-only `GetRegistryRow` is byte-for-byte equivalent to the
applier's handle and holds no state of its own. So the simpler choice
(a fresh read-only wrapper over the already-open default-group store)
avoids widening `buildShardGroups`' already-long return signature with
no correctness cost: the durable registry rows still have a single
owner (the FSM apply path), and the intent step only reads.

### 3.2 Coordinator first-write gate

The coordinator gains a **registration barrier**: a one-shot readiness
signal that blocks the first client write which would land encrypted
until the pending `RegisterEncryptionWriter` proposal has committed.

**Barrier representation — `chan struct{}`, three states (review #5).**
The barrier field on `ShardedCoordinator` is a `chan struct{}` with an
explicit three-state semantic:

| State | Meaning | Write behavior |
|---|---|---|
| `nil` | no pending registration (Phase 0, skip case, or encryption off) | ungated |
| open (non-nil, not closed) | registration proposed, not yet committed | mutating encrypted writes block on it |
| closed | registration committed | ungated (fast path: non-blocking `select` branch) |

The channel MUST be created (open) **before** the barrier reference is
visible to any write-dispatch goroutine — i.e., armed in the
post-`buildShardGroups` phase (§3.1) before gRPC starts serving. A
`nil` channel blocks forever on receive (would deadlock rather than
ctx-cancel), so `nil` is reserved exclusively for the never-armed
ungated case and the dispatch path checks `barrier == nil` first.

**Gate injection — `ShardedCoordinator.Dispatch` (review #4).** The
reads (`LinearizableRead`, `LeaseRead`, `VerifyLeader`) are separate
`Coordinator` methods that never reach `Dispatch`, so they are
structurally ungated. Inside `Dispatch`, the gate applies only to
mutating encrypted writes. Go has no `closed()` builtin, so the
open-vs-closed distinction is a non-blocking `select` after the `nil`
check (review #2):

```go
if barrier != nil {
    select {
    case <-barrier:
        // closed → registration committed → pass through (fast path)
    default:
        // open → registration pending; gate only mutating encrypted writes
        if cache.StorageEnvelopeActive() && cache.activeKeyID != 0 && hasMutatingElems(reqs) {
            select {
            case <-barrier:        // registration committed while we waited
            case <-ctx.Done():
                return ctx.Err()   // fail-closed: never emit an unregistered nonce
            }
        }
    }
}
```

`hasMutatingElems` is required (not just `IsTxn`): a read-only
transaction carrying only `ReadKeys` (no PUT/DEL elements) must stay
ungated even though it flows through `dispatchTxn`. Fail-closed: a
write that cannot register within its `ctx` returns the ctx error
rather than emitting an unregistered nonce.

**Multi-shard note (review #6).** The barrier is node-global; the
registration targets the default group. So in a multi-shard
deployment a write to group 2 blocks until the group-1 registration
Raft round-trip commits. This is correct — nonce uniqueness is
DEK-wide, not shard-scoped — and only the first encrypted write pays
the wait.

**Arming + async commit (review #3).** §3.1's "propose" branch arms
the barrier (creates the open channel) and starts a goroutine that
proposes `RegisterEncryptionWriter` and waits for its apply (the same
applied-index wait the admin RPC uses), then `close()`s the barrier.
That goroutine MUST `select` on **both** the apply signal **and** the
process run-context: on run-ctx cancellation (clean shutdown before
commit) it returns **without** closing the barrier, leaving any
in-flight encrypted writes blocked until they hit their own ctx
deadline / the gRPC server drains — never closing the barrier on an
uncommitted registration. No goroutine leak: it always exits on one of
the two signals.

**Transient propose failures (review #3, follow-on).** A propose that
*starts* but fails mid-flight — leader change, proposal timeout,
`ErrProposalDropped` — is retried inside the goroutine with bounded
backoff, against the same run-context (so retries stop on shutdown).
The barrier stays open across retries, so encrypted writes keep
waiting (fail-closed) rather than proceeding unregistered. The retry
loop only exits by (a) a committed registration → `close()` the
barrier, or (b) run-ctx cancellation → return without closing. There
is no terminal "give up and close" path: closing the barrier without a
committed registration would let this node emit unregistered nonces,
which is the exact hazard 7a exists to prevent.

**Snapshot/compaction interaction (review #8).** If the registration
entry is compacted into a snapshot before the goroutine observes its
index, the engine's `AppliedIndex()` is already ≥ the snapshot index
and the snapshot application updated the registry row, so the
apply-wait completes correctly — same reasoning as the 6C-2b scanner.

### 3.3 Leader routing for the propose

`RegisterEncryptionWriter` proposes a §11.3 0x03 entry through the
default group's leader (the existing 5B Proposer path). On a follower,
the startup propose forwards to the current leader (same path the admin
RPC uses). If no leader is available yet at startup, the barrier stays
**open** (pending — the channel is non-nil and not yet closed, so
mutating encrypted writes block per the §3.2 table; "closed" is
reserved for the committed/ungated state) and the first encrypted
write blocks until leadership settles — bounded by the client's `ctx`
deadline (fail-closed: a write that cannot register does not get to
emit an unregistered nonce).

## 4. Scope

### In scope (7a)
- Startup registration-intent decision (defer / skip / propose).
- Coordinator first-write barrier gating self-originated encrypted
  writes on registration commit.
- Follower→leader forwarding of the startup registration propose.
- Unit tests: the three intent branches; barrier blocks-then-releases;
  reads + non-encrypted writes never gated; Phase-0 defer; ctx-cancel
  while blocked.

### Out of scope
- **7a-2 (complete write-path coverage)** — the coordinator-layer
  barrier in this slice gates the client-write path (`Dispatch`) and
  the internal lock-resolution path (`leaseRefreshingTxn.Commit`), but
  **not** internal paths that write to the store directly without going
  through the coordinator — notably `distribution.CatalogStore.Save` →
  `store.ApplyMutations` (codex P1 on PR #839). Coordinator-layer
  gating is structurally incapable of covering those; the single
  chokepoint for "registered before any encrypted write" is the
  nonce-emission point, `store.encryptForKey`. 7a-2 will fail-close
  there (a non-blocking registered-flag read that refuses to emit an
  encrypted nonce before registration), giving complete coverage of
  every write path. **Interim bounded-safety** until 7a-2: the §5.1
  `local_epoch` bump already prevents concrete nonce *reuse* (every
  nonce this load emits carries the freshly-bumped epoch — unique vs.
  all prior loads, monotonic within), and the only scenario where the
  registry ledger is load-bearing for nonce uniqueness — a cross-node
  16-bit `node_id` collision — is caught by the `ErrNodeIDCollision`
  startup membership pre-check. So the residual exploitable window
  (catalog/direct-store write × node_id collision past the startup
  guard × sub-second pre-registration window) is negligible, and 7a-2
  closes it entirely.
- **7b** — post-rotation re-registration (register against the new DEK
  on a `rotate-dek` apply, same barrier mechanism).
- **7c** — ConfChange-time registration (leader pairs a
  `RegisterEncryptionWriter` with a learner/voter add before the
  conf-change commits; `ErrPeerNotEncryptionRegistered`).
- Snapshot of registry rows across compaction (covered by the §5.5
  resync path already shipped).

## 5. Self-review checklist (for the implementation PR)

- **Data loss / consistency** — registration is additive (a 0x03 entry
  + a registry-row insert/advance); no user data path changes. The
  gate only *delays* encrypted writes, never drops them.
- **Concurrency** — the barrier is read on every encrypted write; once
  closed the fast path is a non-blocking `select` branch on the closed
  channel (O(1), no extra synchronization), and ctx-aware while
  pending. No deadlock if leadership never settles — ctx bound. The
  `nil` (never-armed) case is checked first so it is never received on.
- **Performance** — fast path after first write is a non-blocking
  `select` on the closed channel (O(1)); the registry read + propose
  happen once per process load.
- **Data consistency** — registry monotonicity (§4.1 case 2/3)
  preserved; skip-at-equal-epoch avoids the `ErrLocalEpochRollback`
  self-brick.
- **Test coverage** — intent branches, barrier blocking/release,
  ungated reads, Phase-0 defer, ctx-cancel.

## 6. Resolved design decisions (PR #835 round 1)

All three original open questions were settled by the round-1 review:

1. **Gate injection point** → `ShardedCoordinator.Dispatch`, the single
   adapter entry point. Reads (`LinearizableRead` / `LeaseRead` /
   `VerifyLeader`) are separate methods, structurally ungated. Gate
   condition gated on `hasMutatingElems(reqs)` (PUT/DEL/DEL_PREFIX), so
   read-only transactions stay ungated (§3.2).
2. **Barrier representation** → `chan struct{}` with the explicit
   nil / open / closed three-state semantic (§3.2). Idiomatic,
   ctx-aware, no extra synchronization primitives.
3. **Sync vs async propose** → asynchronous: arm the barrier and let
   only the first encrypted write wait, so reads serve immediately. The
   propose goroutine selects on both the apply signal and the process
   run-context to avoid a leak / a barrier-close on an uncommitted
   registration (§3.2, review #3).

Both items were resolved in the implementation PR (#839):
`hasMutatingElems` is implemented over `OperationGroup.Elems` in
`kv/sharded_coordinator.go`; the `WriterRegistryStore` handle is a
stateless `store.WriterRegistryFor(...)` wrapper (§3.1) rather than a
threaded-out return value from `buildShardGroups`.
