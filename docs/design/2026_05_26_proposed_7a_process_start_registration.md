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

### 3.1 Registration decision (startup, post-bump)

Extend the 6D-6c-2 startup wiring (`prepareStorageNonceEpoch` returns
the bumped epoch). After the bump, compute a **registration intent**:

```
if !StorageEnvelopeActive            -> defer (Phase 0; §1.4)
if registry.last_seen(node,DEK) == epoch -> skip   (already registered; §1.3)
if registry.last_seen(node,DEK) <  epoch -> propose RegisterEncryptionWriter
                                            (the §1.1/1.2 trigger)
```

The registry read is local (Pebble, no Raft round-trip — the registry
is replicated state already present on this node post-bootstrap),
reusing the same `RegistryKey(dek_id, NodeID16(full_node_id))` lookup
`ApplyRegistration` / `GuardLocalEpochRollback` use.

### 3.2 Coordinator first-write gate

The coordinator gains a **registration barrier**: a one-shot readiness
signal that blocks the first client write which would land encrypted
(gate active + active DEK) until the pending `RegisterEncryptionWriter`
proposal has committed (its `applied_index` observed). Proposed shape:

- A `registrationBarrier` on `ShardedCoordinator`: `nil` when no
  registration is pending (Phase 0, skip case, or encryption-off), else
  a channel/`atomic` closed when registration commits.
- On the write dispatch path, when the write targets the storage
  envelope (gate on + DEK active) and the barrier is unclosed, block on
  it (honoring `ctx`) before proposing. Reads and non-encrypted writes
  are never gated.
- The barrier is armed at startup by §3.1's "propose" branch and closed
  by the `RegisterEncryptionWriter` apply (observed via the same
  applied-index wait the admin RPC already uses).

**Open question for review:** exact injection point — gate inside
`ShardedCoordinator.Dispatch` before the propose, vs. a smaller
wrapper. Leaning toward `Dispatch` since it is the single entry point
all adapters funnel through, but the txn vs. raw-put split needs a
careful read so reads stay ungated.

### 3.3 Leader routing for the propose

`RegisterEncryptionWriter` proposes a §11.3 0x03 entry through the
default group's leader (the existing 5B Proposer path). On a follower,
the startup propose forwards to the current leader (same path the admin
RPC uses). If no leader is available yet at startup, the barrier stays
closed-pending and the first encrypted write blocks until leadership
settles — bounded by the client's `ctx` deadline (fail-closed: a write
that cannot register does not get to emit an unregistered nonce).

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
- **Concurrency** — the barrier is read on every encrypted write; must
  be a lock-free fast path once closed (atomic load), and ctx-aware
  while pending. No deadlock if leadership never settles — ctx bound.
- **Performance** — fast path after first write is a single atomic
  load; the registry read + propose happen once per process load.
- **Data consistency** — registry monotonicity (§4.1 case 2/3)
  preserved; skip-at-equal-epoch avoids the `ErrLocalEpochRollback`
  self-brick.
- **Test coverage** — intent branches, barrier blocking/release,
  ungated reads, Phase-0 defer, ctx-cancel.

## 6. Open questions

1. Coordinator gate injection point (§3.2) — `Dispatch` vs. a narrower
   seam; confirm reads/txn-reads stay ungated.
2. Barrier representation — `chan struct{}` closed on commit vs.
   `atomic.Bool` + condition var. Leaning `chan struct{}` for ctx-aware
   `select`.
3. Should the startup propose be synchronous (block startup until it
   commits) or asynchronous (arm the barrier, let the first encrypted
   write wait)? Leaning asynchronous so a node with no pending writes
   starts serving reads immediately, and only encrypted writes wait.
