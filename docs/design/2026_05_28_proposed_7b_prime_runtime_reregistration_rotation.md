# Stage 7b' — runtime writer re-registration (rotation case)

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-28 |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 writer registry, §5.2 RotateDEK apply), [`2026_05_28_implemented_7b_runtime_reregistration.md`](2026_05_28_implemented_7b_runtime_reregistration.md) (cutover case + §6 deferred-rotation analysis) |
| Builds on | 7a (process-start propose path), 7a-2 (storage-layer `Registered()` gate), 7b (runtime watcher for the cutover case) |

## 0. Why this slice exists

7b shipped the runtime registration watcher for the **cutover** case
(Phase-0 boot or pre-bootstrap boot → runtime `EnableStorageEnvelope`).
The watcher explicitly **defers** the rotation case via a log-once skip
in [`runtimeRegistrationInScope`](../../main_encryption_registration.go)
when `bootDEKID != 0 && activeDEK != bootDEKID`. This slice closes that
gap.

The rotation case is a non-proposer node observing a committed
`RotateDEK` and needing to register `(newDEK, node, ?)` so its
post-rotation writes can pass the §4.1 monotonicity check on next
restart. Without re-registration the node's storage gate stays
fail-closed under `newDEK` indefinitely, and a routine rotation bricks
every non-proposer for the rest of the process load.

## 1. The problem (recap from 7b §6)

After `RotateDEK` applies, every node observes:
- `sidecar.Active.Storage = newDEK`
- `sidecar.Keys[newDEK].LocalEpoch = 0` (proposer's perspective at apply)
- The writer registry has a row for `(newDEK, proposer.node_id, 0)`
  inserted by `applyRotateDEK` via `ApplyRegistration(ProposerRegistration)`.

A non-proposer's nonce factory is still emitting under whichever DEK
was active at the **non-proposer's** process-start, pinned to that
load's `w.epoch > 0`. Two failure modes if the watcher naively
registers `(newDEK, node, w.epoch)`:

- **Brick.** Next restart calls `BumpLocalEpoch(newDEK)` which bumps
  the **sidecar's** value `0 → 1`; the §9.1 startup guard sees
  `w.epoch_new (1) < lastSeen (w.epoch_old)` and refuses to boot.
- **Nonce reuse if the guard were bypassed.** Subsequent restarts
  walk `Keys[newDEK].LocalEpoch` up `1, 2, 3, ...` and would
  eventually re-emit `(node, w.epoch_old, write_count)` nonces under
  `newDEK`, colliding with what the rotating load already wrote.

The brick scenario is loud (boot fails); the nonce-reuse scenario is
silent and catastrophic.

## 2. Architecture choice — three viable options

7b §6 enumerated three viable fixes. Restated with this slice's
recommendation:

(a) **Per-DEK nonce factory.** Replace the single-pointer
   `pebbleStore.nonceFactory` with a per-DEK map keyed by
   `activeStorageDEKID`. On rotation, the apply path installs a fresh
   nonce factory for the new DEK at `local_epoch = 0`. The runtime
   watcher then registers `(newDEK, node, 0)` consistently with the
   sidecar.
   - **Pro:** Cleanest invariant — each DEK has its own (epoch,
     write_count) namespace; no cross-DEK epoch coupling at all.
   - **Con:** Largest blast radius — touches `pebbleStore`, the
     `WithEncryption` option, the nonce-factory contract, the storage
     gate's lookup, AND the watcher. Higher migration risk.

(b) **Per-node sidecar coordination at apply time.** `applyRotateDEK`
   on each node writes `sidecar.Keys[newDEK].LocalEpoch =
   thisLoad.w.epoch` (the local node's pinned value, NOT the
   proposer's). 7b's runtime watcher (this slice extends its
   scope-check) then registers `(newDEK, node, w.epoch)` consistently
   with the sidecar.
   - **Pro:** Smallest surface area — only `applyRotateDEK`'s sidecar
     mutation and the watcher's scope-check change. Each node's
     sidecar accurately records its own highest-emitted `local_epoch`
     under `newDEK` because **only that node's writes touch its
     sidecar's `Keys[newDEK]` entry**.
   - **Con:** Requires plumbing `w.epoch` into the `Applier` (or a
     sidecar-mutator helper called from the apply path). The
     `Applier` is FSM-internal and was deliberately decoupled from
     write-path state; adding `w.epoch` blurs that boundary.

(c) **Proposer-pinned rotation epoch.** The rotation entry carries an
   explicit `local_epoch` field; `applyRotateDEK` writes that value
   into every node's `sidecar.Keys[newDEK].LocalEpoch`
   deterministically. The sidecar value is cluster-uniform.
   - **Pro:** Cluster-uniform sidecar simplifies forensic recovery
     (every node's record for `newDEK` is the same).
   - **Con:** The proposer's `w.epoch` is not guaranteed to be ≥
     every other node's `w.epoch` at apply time. If proposer.w.epoch
     < non_proposer.w.epoch, applying the proposer's value would
     trigger the same brick / nonce-reuse failure modes that
     motivated this slice. Detecting and gating on per-node bound
     requires per-node Raft round-trips before the rotation can
     commit — a strictly larger coordination problem than (b).

**Recommendation: option (b).** The "each node's sidecar records its
own emissions" invariant is the natural one for a per-node nonce
factory; recording someone else's epoch (the proposer's, or an
arbitrary cluster value) violates the sidecar's purpose of bounding
**this node's** crash-recovery window. Option (a) is architecturally
cleaner but ships materially more code; we will revisit it if option
(b) accumulates too many footnotes during rotation scenarios.

## 3. Design (option b)

### 3.1 `applyRotateDEK` per-node sidecar mutation

`applyRotateDEK` currently writes the new key with `LocalEpoch: 0`
via `writeRotationSidecar`. The change:

- Inject a `localEpochProvider func() uint16` into the `Applier`
  via a new `WithLocalEpoch` functional option (symmetric with
  `WithStateCache`, `WithSidecarPath`, etc.).
- The provider closes over the same `encryptionWriteWiring.epoch`
  the nonce factory is pinned to. `main_encryption_write_wiring.go`'s
  `buildEncryptionWriteWiring` already computes this value and
  threads it through; this slice exposes it to the `Applier`
  construction site in `main.go`.
- `writeRotationSidecar` calls `a.localEpoch()` (or `0` if no
  provider is configured — preserving the FSM-internal test
  harnesses that construct an `Applier` without write-path state)
  and writes that value into `sc.Keys[newDEK].LocalEpoch`.

#### 3.1.1 Why the provider, not a captured value

The `Applier` outlives the wiring (snapshots are restored under a
fresh `Applier`). A captured `uint16` would freeze the epoch at
construction time; a func provider re-reads on every apply so a
late `BumpLocalEpoch` (between `Applier` construction and the
rotation apply) is reflected.

In practice the apply runs in the FSM goroutine while
`BumpLocalEpoch` runs at process start — they should not interleave.
The provider's late-binding is defense-in-depth against a future
runtime-bump path (rotation-with-epoch-renew, not currently in scope).

#### 3.1.2 Backward compatibility — pre-bootstrap and unwired tests

`localEpoch()` returns 0 for two cases:
1. Pre-bootstrap process loads (no sidecar, `w.epoch == 0`).
2. Test harnesses that instantiate an `Applier` without the
   `WithLocalEpoch` option.

The applier MUST gracefully fall back to `0` in both cases —
existing applier tests construct an `Applier` for FSM-internal
behaviour (bootstrap, rotation, registration) and have no notion of
a nonce-factory pinned epoch.

### 3.2 7b watcher scope-check extension

[`runtimeRegistrationInScope`](../../main_encryption_registration.go)
currently treats `bootDEKID != 0 && activeDEK != bootDEKID` as the
deferred case and logs-once-then-skips. After 7b':

- The deferred-skip branch is REPLACED by the in-scope branch — the
  watcher proposes `(activeDEK, node, w.epoch)` consistently with the
  sidecar's `Keys[activeDEK].LocalEpoch` value that `applyRotateDEK`
  wrote.
- The `lastLoggedSkipDEK` counter is replaced by a `lastRegisteredDEK`
  counter that records the most recently registered DEK; if the watcher
  observes a fresh rotation (active DEK changes again while the load is
  running), it re-proposes for the new DEK.
- The scope-check now reads:
  - In scope: `bootDEKID == 0` (pre-bootstrap), OR `activeDEK ==
    bootDEKID` (cutover), OR `activeDEK != bootDEKID && lastRegisteredDEK
    != activeDEK` (rotation, not yet registered for this DEK).
  - Already done: `cache.Registered(activeDEK)` (the gate already says
    yes — the existing first check).

### 3.3 Apply-side ordering invariant

`applyRotateDEK`'s ordering MUST be:

1. KEK-unwrap new DEK.
2. Keystore set (`a.keystore.Set(p.DEKID, dek)`).
3. **Sidecar write — with `localEpoch()` for `Keys[newDEK].LocalEpoch`.**
4. **Proposer registration** (`ApplyRegistration(p.ProposerRegistration)`)
   — proposer's value is `(newDEK, proposer.node_id, proposer.w.epoch)`,
   set by the proposer at propose time. Verbatim into the writer
   registry.

Step 3 changes from `LocalEpoch: 0` to `LocalEpoch: a.localEpoch()`.
Step 4 is unchanged — the proposer's row already targets the proposer's
own `w.epoch`, which is what option (b) wants for the proposer.

Non-proposer nodes do NOT auto-register here — they remain at the
old DEK's registered state. The 7b watcher's next tick observes
`activeDEK != bootDEKID && lastRegisteredDEK != activeDEK` and
proposes their `(newDEK, node, w.epoch)` row.

### 3.4 The §9.1 startup guard is preserved

After 7b' lands, on next restart the non-proposer's
`BumpLocalEpoch(newDEK)` reads `sc.Keys[newDEK].LocalEpoch =
prior.w.epoch` (not 0) and bumps `prior.w.epoch → prior.w.epoch + 1`.
The §9.1 guard checks `prior.w.epoch + 1 < lastSeen = prior.w.epoch`
which is false (it's the natural monotone advance). Boot succeeds.

The pre-existing brick scenario from 7b §6 is closed because every
node's sidecar now records its own emissions accurately.

## 4. Out of scope (deferred slices)

- **Rotation-with-epoch-renew.** A future feature could carry a
  "renew the per-node `LocalEpoch` to a fresh small value" semantic
  with the rotation entry, in conjunction with a per-DEK nonce
  factory (option a). 7b' does not need this — option (b) requires
  no nonce factory change.
- **Rotation-with-rewrite.** Rewriting all live envelopes under the
  new DEK on rotation (§5.4 retire/rewrite) lives in Stage 9. 7b'
  only handles registration; rewrite is independent.
- **Cross-shard rotation atomicity.** RotateDEK is a single-group
  proposal today (the encryption catalog lives in the default
  group). Multi-group rotation atomicity is a much larger design
  problem; out of scope here.

## 5. Verification action items (for the implementation PR)

1. New `applier_test.go` cases:
   - `TestApplier_ApplyRotateDEK_LocalEpochProviderReadAtApply`:
     given a `WithLocalEpoch` provider returning 7, the rotation
     entry under a fresh DEK writes `Keys[newDEK].LocalEpoch = 7`
     to the sidecar.
   - `TestApplier_ApplyRotateDEK_NoLocalEpochProviderFallsBack`:
     constructing the `Applier` without the option preserves
     today's `LocalEpoch: 0` behaviour (test-harness
     backward-compatibility).
   - `TestApplier_ApplyRotateDEK_ProviderInvokedPerApply`: the
     provider closure is invoked on each apply, not cached at
     construction. (A counter-based test fixture suffices.)

2. New `main_encryption_registration_test.go` cases extending the
   existing watcher harness:
   - `TestRuntimeRegistrationTick_RotationInScopeProposes`:
     `bootDEKID == X != 0`, `activeDEK == Y != X`,
     `lastRegisteredDEK != Y`, `w.epoch == 5` → tick proposes
     `(Y, node, 5)`. Pre-populates a registry row at `(Y, node, 5)`
     so `verifyRegistered` short-circuits and `MarkRegistered(Y)`
     fires.
   - `TestRuntimeRegistrationTick_RotationAlreadyRegisteredIsNoOp`:
     after the first proposing tick, a second tick at the same
     `activeDEK` is a no-op (the §4.1 case-2 idempotent guard).
   - `TestRuntimeRegistrationTick_RotationToYetAnotherDEKReProposes`:
     after registering for DEK Y, a subsequent rotation to DEK Z
     (`activeDEK == Z`, `lastRegisteredDEK == Y`) triggers a fresh
     propose. Pins that `lastRegisteredDEK` is updated to Z.

3. Replace `TestRuntimeRegistrationTick_DeferredRotationLogsOnceAndSkips`
   (which pins the 7b deferral) with
   `TestRuntimeRegistrationTick_RotationInScopeProposes` (the 7b'
   behaviour) and document the lifecycle transition in the commit
   message: the deferral was the 7b posture; 7b' is the resolution.

4. Self-review (5-lens) for the implementation PR — same template
   as 7b. Particular attention to:
   - **Data consistency:** Each node's `Keys[newDEK].LocalEpoch`
     matches its own nonce factory's pinned epoch — no cross-node
     epoch contamination.
   - **Concurrency:** The `localEpoch()` provider is invoked from
     the FSM goroutine (apply path) only; no shared-state race
     with the watcher's read of `w.epoch` (which is the same
     captured-at-construction value the provider closes over).

## 6. Rollout / migration

7b' is purely additive — no wire-format change, no FSM apply
semantics change visible to peers, no Raft entry layout change. The
non-proposer's runtime registration RPC payload is identical to
7b's. The only on-disk change is `Keys[newDEK].LocalEpoch` populated
with the local node's epoch rather than 0; this is invisible to
peers (sidecars are per-node) and to ongoing reads (the value is
only consulted by `BumpLocalEpoch` on next process start).

Mixed-version cluster behaviour:
- A 7b-only node observing a rotation continues to log-once-skip and
  remains fail-closed under the new DEK for its lifetime. No data
  loss, no nonce reuse — just degraded availability until the node
  is restarted on the 7b' binary.
- A 7b' node observing a rotation registers and proceeds.

Roll-forward path: deploy 7b' to every node in the cluster, then a
RotateDEK is naturally absorbed by the 7b' watcher on every node.

Rollback path: rolling back to 7b after a 7b' RotateDEK has applied
is safe — the sidecar's `Keys[newDEK].LocalEpoch = prior.w.epoch`
value satisfies the §9.1 startup guard's `w.epoch_new >= lastSeen`
check on next boot of the 7b binary. The 7b binary then runs its
log-once-skip deferral for the rotation case, identical to the
pre-7b' posture.
