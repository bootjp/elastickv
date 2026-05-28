# Stage 7b — runtime writer re-registration (cutover case only; rotation deferred)

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-28 |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 writer registry), [`2026_05_26_proposed_7a_process_start_registration.md`](2026_05_26_proposed_7a_process_start_registration.md) (process-start), [`2026_05_26_proposed_7a2_storage_layer_registration_enforcement.md`](2026_05_26_proposed_7a2_storage_layer_registration_enforcement.md) (storage gate) |
| Builds on | 7a (process-start propose path), 7a-2 (storage-layer `Registered()` gate) |

## 0. Why this slice exists

7a registers a process at process start. 7a-2 wires the storage-layer
direct-path gate to `Registered()` — per-DEK and fail-closed. Two
runtime scenarios fall outside 7a's process-start arm and surfaced as
codex **P1** findings on PR #847:

1. **Phase-0 boot → runtime `EnableStorageEnvelope` (in scope for 7b).**
   A node that booted in Phase 0 (envelope inactive) skips
   `buildProcessStartRegistrationGate`'s propose path. When
   `EnableStorageEnvelope` later applies at runtime, the storage gate
   transitions from "not consulted" (envelope inactive) to "fail-closed
   for an unregistered node" — direct encrypted writes are refused
   indefinitely. **The active DEK does NOT change**, only the envelope
   activates. `prepareStorageNonceEpoch` already called
   `BumpLocalEpoch` for the active DEK at startup, so the sidecar's
   `Keys[activeDEK].LocalEpoch` equals this load's pinned `w.epoch` —
   registering at `w.epoch` keeps the sidecar and the writer-registry
   consistent across restart.

2. **Non-proposer node after runtime `RotateDEK` (NOT in scope; deferred to 7b').**
   `applyRotateDEK` inserts only the *proposer's* registration row for
   the new DEK; every other node has `activeStorageDEKID` re-pointed to
   the new DEK and `Registered()` returns false for it. Registering the
   new DEK at the current load's pinned `w.epoch` would not work
   safely — the sidecar's `Keys[newDEK].LocalEpoch` starts at 0 and is
   bumped only on the next restart, so the registry's
   `LastSeenLocalEpoch` for `(newDEK, node)` would diverge from the
   sidecar and either brick the node on next restart (`w.epoch_new <
   lastSeen`) or, if the §9.1 guard were bypassed, open a real nonce
   reuse window. The fix requires either rebuilding the nonce factory
   per-DEK on rotation or per-node sidecar coordination at apply time
   — both materially larger than 7b's intended watcher slice. See §6.
   Both codex (P1 on PR #848) and gemini (medium #2 on PR #848)
   independently flagged this; 7b explicitly defers case 2.

For case 1 only, 7b adds a runtime trigger: when the storage gate
*would* fail closed because the envelope just became live and the node
never registered (active DEK unchanged), propose the node's own
registration row and seed `Registered()` once the entry commits —
exactly what 7a does at process start, but driven by the cache
transition rather than a startup decision. It is **not** a fail-OPEN
fallback (design §2.3 of 7a-2 forbids those): the storage gate remains
fail-closed for unregistered nodes; 7b just clears the gate by
*actually registering*.

(7c — ConfChange-time registration for a freshly-joining node — is a
distinct mechanism, membership-driven rather than cache-driven, and is
NOT in 7b's scope.)

## 1. Where the trigger fires

The defining condition for "this node must (re)register now" is:

```text
cache.StorageEnvelopeActive() && id, ok := cache.ActiveStorageKeyID(); ok && !cache.Registered()
   ⇔ the storage gate would fail-closed for an encrypted direct write today
```

This is precisely the state where `encryptForKey` on the direct path
would return `ErrWriterNotRegistered`. The 7b watcher acts on this
condition **only when the active DEK ID matches the boot-time active
DEK** (`activeStorageDEKID` unchanged since process start) — i.e. the
cutover case where `StorageEnvelopeActive` flipped but the DEK is the
same one `prepareStorageNonceEpoch` bumped at startup. When the
condition holds with a *different* active DEK id, that is the deferred
rotation case (§6); the watcher logs and skips it.

After `EnableStorageEnvelope` apply: `StorageEnvelopeActive` flips
true; if this node never registered, `Registered()` is false; the boot
DEK is unchanged. The trigger fires.

The trigger is read off the shared `*encryption.StateCache`, which
`applyEnableStorageEnvelope` and `applyRotateDEK` already update via
`RefreshFromSidecar(sc)` after each `WriteSidecar`. 7b does not
introduce a new wire-format event; it observes the existing cache
transitions.

## 2. Proposed design

### 2.1 A runtime registration watcher

`runRuntimeRegistrationWatcher(ctx, coordinate, defaultGroup, w, raftID)`
is a single goroutine started from `setupDistributionAndRegistration`
(or a sibling helper) AFTER `installProcessStartRegistrationGate`. It
runs for the lifetime of `runCtx` and:

1. **Observes** the trigger condition above by polling the
   `StateCache` predicates at a fixed interval
   (`runtimeRegistrationPollInterval`, default 1 s). Polling is simpler
   than an event channel and the interval is bounded by `runCtx`, so
   there is no risk of an unbounded wakeup storm during normal operation
   (the condition flips at most once per cutover/rotation).

2. **Dedupes / scope-checks**: skip the body when (a) the cache reports
   `Registered() == true`, (b) `StorageEnvelopeActive() == false`, or
   (c) the active DEK id has changed from the boot-time DEK (the
   deferred rotation case — log once and skip). Only the cutover-case
   condition triggers the propose.

3. **Proposes** synchronously a registration entry for
   `(bootDEK, fullNodeID, w.epoch)` using the *same*
   `runWriterRegistration` helper 7a already uses. The `local_epoch` is
   `w.epoch` — this load's pinned epoch from
   `buildEncryptionWriteWiring`, post-`BumpLocalEpoch` for the boot
   DEK — which is what the nonce factory actually emits AND equals
   `sidecar.Keys[bootDEK].LocalEpoch`, so the writer-registry row stays
   coherent with the sidecar across restart (§2.2).

4. **Marks** on success via the existing `releaseBarrier(barrier,
   cache, bootDEK)` helper (with a fresh per-attempt barrier whose
   only consumer is the watcher). Once `MarkRegistered(bootDEK)`
   stores, `cache.Registered()` returns true and the gate clears.

5. **Backs off** between attempts using the existing
   `registrationRetryInitial / registrationRetryMax /
   registrationBackoffFactor`. Failures are bounded by `runCtx`; on
   shutdown the goroutine returns without leaving stale state.

**Concurrency model (addresses gemini medium #1 on PR #848).** The
watcher is a single goroutine and the propose is **synchronous in that
goroutine** — no fan-out of background registration workers. This
means:

- At most one in-flight propose at any time → no goroutine
  accumulation across poll ticks, no duplicate Raft proposals from
  the watcher itself.
- The next poll tick can only fire AFTER the current `runWriterRegistration`
  returns (success, verify-committed, or `runCtx` cancellation). The
  per-attempt `registrationAttemptTimeout` (5 s) bounds how long any
  single sub-attempt can block before yielding back to the retry loop.
- The watcher does **not** need to support DEK changing during an
  in-flight propose, because rotation is out of scope (§6) and the
  cutover case does not change the active DEK. If a rotation
  *nevertheless* happens mid-propose, the worst case is the watcher
  completes registration for the (now stale) boot DEK; the loop body
  on the next tick observes the rotated active DEK ≠ boot DEK and
  takes the deferred-skip branch (log once). No fail-OPEN, no
  goroutine leak.

The watcher does **not** touch the `RegistrationGate` the coordinator
holds — 7a's coordinator-layer barrier is a per-process-load
construct, not a per-DEK one. The cutover case does not introduce a
new coordinator barrier; it only needs the storage-layer `Registered()`
predicate to flip true again. The coordinator barrier from process
start has long since closed in normal operation.

### 2.2 `local_epoch` consistency (cutover case)

§4.1 nonce uniqueness is per-DEK over `(node_id, local_epoch,
write_count)`. The nonce factory is built once at process start with
`(NodeID16(raftID), w.epoch)`. For the cutover case the active DEK is
the same one `prepareStorageNonceEpoch` already called `BumpLocalEpoch`
on at startup, so:

- `sidecar.Keys[activeDEK].LocalEpoch == w.epoch` (post-bump value).
- The nonce factory emits `(node, w.epoch, write_count)` under
  `activeDEK`.
- 7b registers `(activeDEK, node, w.epoch)`, advancing the writer
  registry's `LastSeenLocalEpoch` to `w.epoch`.

On the next restart, `BumpLocalEpoch` reads
`sidecar.Keys[activeDEK].LocalEpoch == w.epoch` and bumps it to
`w.epoch + 1`. The §9.1 startup guard sees
`w.epoch_new (w.epoch + 1) > lastSeen (w.epoch)` and takes the propose
path, registering at `w.epoch + 1`. No brick, no nonce reuse — the
sidecar and the writer registry stay coherent across restart.

**Rotation case is materially different.** A runtime `RotateDEK`
installs a new active DEK whose `sidecar.Keys[newDEK].LocalEpoch == 0`,
while the live nonce factory still emits at `w.epoch > 0` under any
DEK currently active. Registering the new DEK at `w.epoch` would make
the registry advance past the sidecar; the next restart's
`BumpLocalEpoch(newDEK)` would only bump `0 → 1`, the §9.1 guard would
refuse boot, and bypassing the guard would risk real nonce reuse once
the per-process bumped epoch under `newDEK` eventually overlapped a
previously-emitted `(node, w.epoch, *)`. See §6 for the deferred fix.

### 2.3 Why polling, not a channel

`StateCache` is a `sync/atomic` predicate today; it deliberately has no
subscription API to keep the hot Put path lock-free. Adding a channel
would mean threading a notifier into `RefreshFromSidecar` (called from
every FSM apply that mutates encryption state), and choosing between
buffered/unbuffered semantics that can drop or block. The polling loop
is simpler:

- One goroutine, one timer.
- O(constant) work per tick (two atomic loads).
- Bounded by `runCtx` so shutdown is clean.
- Visible from logs (each propose attempt logs the trigger).

The 1 s interval is short enough that the "gate trapped after cutover"
window is sub-second in practice, and long enough that an idle node
spends negligible CPU on it. If a future workload makes polling
expensive the predicate could grow a `sync.Cond` or a notification
channel as a follow-on; for 7b the simplest correct design wins.

### 2.4 Composing with 7a process-start

The 7a process-start path remains the primary registration trigger and
covers every restart. 7b is purely additive: after process-start
finishes (the propose-branch goroutine returns or the skip branch was
taken), the watcher begins observing. The two never race because:

- Process-start runs synchronously inside `installProcessStartRegistrationGate`
  for its decision (read `lastSeen`, branch). Its registration goroutine
  closes the barrier and seeds `MarkRegistered` on its own DEK.
- Once that finishes, the watcher's condition (`!cache.Registered()`)
  is false for the boot DEK and the loop body is a no-op until a
  runtime cutover flips `StorageEnvelopeActive`.

If a runtime cutover happens *before* process-start finishes (extremely
unlikely; admin RPC needs the cluster serving), the worst case is the
watcher and the process-start goroutine both propose registrations for
the same `(bootDEK, node, w.epoch)` triple. The §4.1 case 2-idempotent
path handles a duplicate apply as a no-op, so this is safe.

### 2.5 Startup ordering

Same as 7a-2 §2.3: install the watcher AFTER
`installProcessStartRegistrationGate` so the process-start goroutine
exists and the barrier semantics are fixed before any runtime trigger
can fire. The watcher reads only the shared cache + the default-group
engine handle; it has no dependency on the route catalog (the
`OpRegistration` apply writes a writer-registry row only — §5 of 7a-2
already confirmed).

## 3. Scope

### In scope (7b)
- `runRuntimeRegistrationWatcher` goroutine in `main_encryption_registration.go`,
  pinned to the boot-time active DEK (cutover case only).
- A new constant `runtimeRegistrationPollInterval` (default 1 s).
- main.go wiring: start the watcher from `setupDistributionAndRegistration`
  AFTER `installProcessStartRegistrationGate`, under the existing
  errgroup with `runCtx`.
- Reuse of the existing `runWriterRegistration` + `releaseBarrier`
  helpers (no new propose pathway).
- Tests: a fake cache that flips `StorageEnvelopeActive` false→true on
  the boot DEK to assert the watcher proposes for the boot DEK and
  marks `Registered()` true; a transition where `activeStorageDEKID`
  changes to a non-boot DEK to assert the watcher takes the
  deferred-skip branch (logs and does not propose).

### Out of scope (deferred slices)
- **Rotation case (7b'):** non-proposer registration after runtime
  `RotateDEK` — needs per-DEK `local_epoch` coordination (sidecar
  consistency on restart). See §6.
- 7c (ConfChange-time registration for joining nodes) — a separate
  membership-driven mechanism, not cache-driven.
- Replacing the polling loop with a notification channel — possible
  follow-on once a workload need is identified.

## 4. Self-review checklist (for the implementation PR)
- **Data loss** — the watcher only proposes registration entries; it
  never modifies storage or the cutover/rotation sidecar state. A
  failed propose returns to the retry loop or to `runCtx` cancellation;
  no committed write is dropped.
- **Concurrency** — the watcher reads `StateCache` via the existing
  atomic predicates; it shares no mutable state with the FSM apply
  goroutine. The §4.1 case 2-idempotent apply path makes a duplicate
  propose (with the process-start goroutine) safe.
- **Performance** — 1 s polling × two atomic loads = nil overhead. The
  hot Put path is untouched.
- **Data consistency** — registration `local_epoch` equals `w.epoch`
  (the nonce factory's pinned value AND the sidecar's
  `Keys[bootDEK].LocalEpoch` after startup `BumpLocalEpoch`), keeping
  the writer-registry, the sidecar, and the on-disk nonces all aligned
  across restart. The rotation deferral (§6) keeps this invariant safe
  by refusing to propose for a non-boot DEK.
- **Test coverage** — fake-cache `StorageEnvelopeActive` false→true
  transition exercises the cutover propose; an `activeStorageDEKID`
  change to a non-boot DEK exercises the deferred-skip branch (logs,
  does not propose); shutdown cancels cleanly; concurrent process-start
  goroutine + watcher propose is no-op via case 2-idempotent.

## 5. Verification action items (for the implementation PR)
1. Confirm `runWriterRegistration` and `releaseBarrier` can be reused
   from the runtime watcher without leaking the conn cache (the
   goroutine-scoped `connCache.Close()` defer in 7a stays — each watcher
   propose attempt should similarly scope its own cache, or the
   long-running watcher should keep a single cache for its lifetime and
   close on `runCtx`).
2. *(rotation-case verification; relevant to 7b' not 7b)*
   Confirm `applyRotateDEK` updates the shared `StateCache` via
   `RefreshFromSidecar` AFTER the new DEK is durably committed
   (otherwise the watcher could observe `activeStorageDEKID = new` and
   propose before any node has the keystore entry to decrypt the
   resulting envelope). Existing code in `writeRotationSidecar` already
   refreshes the cache after `WriteSidecar` succeeds; just re-verify
   the ordering when implementing the watcher.
3. Decide the propose `local_epoch` policy explicitly in code: this
   document specifies it MUST equal `w.epoch` (the post-`BumpLocalEpoch`
   value for the boot DEK, recorded in the sidecar at startup). Add a
   unit test pinning that the watcher proposes with `w.epoch`, not
   the freshly-rotated DEK's sidecar value.
4. Add a unit test pinning the deferred-skip branch: when
   `activeStorageDEKID` has changed from the boot DEK, the watcher
   logs once and does NOT propose. This makes the rotation deferral
   explicit at the code level.

## 6. Deferred — rotation case (7b')

Non-proposer registration after runtime `RotateDEK` is materially
harder than the cutover case because the new active DEK arrives with
`sidecar.Keys[newDEK].LocalEpoch == 0`, while the nonce factory
already emits at this load's pinned `w.epoch > 0` under whichever DEK
becomes active. Registering the new DEK at `w.epoch` would advance the
writer registry's `LastSeenLocalEpoch` past the sidecar's record,
producing two failure modes on next restart (codex P1 + gemini medium
#2 on PR #848):

- **Brick.** `BumpLocalEpoch(newDEK)` bumps `0 → 1`; the §9.1 startup
  guard sees `w.epoch_new (1) < lastSeen (w.epoch_old)` and refuses to
  boot.
- **Nonce reuse if the guard were bypassed.** Subsequent restarts walk
  `Keys[newDEK].LocalEpoch` up `1, 2, 3, ...` and would eventually
  re-emit `(node, w.epoch_old, write_count)` nonces under `newDEK`,
  colliding with what the rotating load already wrote.

Three viable fixes (each requires its own design slice):

(a) **Per-DEK nonce factory.** Replace the single-pointer
   `pebbleStore.nonceFactory` with a per-DEK map keyed by
   `activeStorageDEKID`. On rotation, the apply path installs a fresh
   nonce factory for the new DEK at `local_epoch = 0` (or the bumped
   per-DEK value). The runtime watcher then registers `(newDEK, node,
   0)` consistently with the sidecar. Architecturally largest.

(b) **Per-node sidecar coordination at apply time.** `applyRotateDEK`
   on each node writes `sidecar.Keys[newDEK].LocalEpoch =
   thisLoad.w.epoch` (this node's pinned value, NOT the proposer's),
   then the watcher registers at the same `w.epoch`. Each node's
   sidecar accurately records its own highest-emitted `local_epoch`
   under `newDEK`. Requires plumbing `w.epoch` into the `Applier` (or
   a sidecar-mutator helper exposed to the watcher).

(c) **Proposer-pinned rotation epoch.** The rotation entry carries an
   explicit `local_epoch` field; `applyRotateDEK` writes that value
   into every node's `sidecar.Keys[newDEK].LocalEpoch` deterministically.
   This makes the sidecar value cluster-uniform but requires that the
   proposer's `w.epoch` ≥ every other node's `w.epoch` at apply time,
   which is not automatic. Probably the worst of the three.

Option (b) is the most likely path forward and is what 7b' will
propose. None of these belong in 7b.
