# Stage 7b — runtime writer re-registration (cutover + rotation cases)

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-28 |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 writer registry), [`2026_05_26_proposed_7a_process_start_registration.md`](2026_05_26_proposed_7a_process_start_registration.md) (process-start), [`2026_05_26_proposed_7a2_storage_layer_registration_enforcement.md`](2026_05_26_proposed_7a2_storage_layer_registration_enforcement.md) (storage gate) |
| Builds on | 7a (process-start propose path), 7a-2 (storage-layer `Registered()` gate) |

## 0. Why this slice exists

7a registers a process at process start. 7a-2 wires the storage-layer
direct-path gate to `Registered()` — per-DEK and fail-closed. Two
runtime scenarios fall outside 7a's process-start arm and are explicitly
deferred as the "runtime-cutover registration follow-on" in 7a's
docstring. Both surfaced as codex **P1** findings on PR #847:

1. **Phase-0 boot → runtime `EnableStorageEnvelope`.** A node that booted
   in Phase 0 (envelope inactive) skips `buildProcessStartRegistrationGate`'s
   propose path. When `EnableStorageEnvelope` later applies at runtime, the
   storage gate transitions from "not consulted" (envelope inactive) to
   "fail-closed for an unregistered node" — direct encrypted writes are
   refused indefinitely.

2. **Non-proposer node after runtime `RotateDEK`.** `applyRotateDEK`
   inserts only the *proposer's* registration row for the new DEK
   (`p.ProposerRegistration`). Every other node has `activeStorageDEKID`
   re-pointed to the new DEK by `RefreshFromSidecar`, so its `Registered()`
   transitions to false (the new id ≠ `registeredStorageDEKID`). Without
   a runtime re-arm those nodes fail closed on the next direct encrypted
   write until restart.

Both share one mechanism: when the storage gate *would* fail closed
because the active DEK changed (or just became live), the affected node
must propose its own registration row for the new DEK and seed
`Registered()` once the entry commits — exactly what 7a does at process
start, but driven by a cache transition rather than a startup decision.

7b adds that runtime trigger. It is **not** a fail-OPEN fallback (design
§2.3 of 7a-2 forbids those): the storage gate remains fail-closed for
unregistered nodes; 7b just clears the gate by *actually registering*.

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
would return `ErrWriterNotRegistered`. The two runtime scenarios above
both produce it:

- After `EnableStorageEnvelope` apply: `StorageEnvelopeActive` flips
  true; if this node never registered, `Registered()` is false.
- After `RotateDEK` apply on a non-proposer: `activeStorageDEKID`
  changes to the new id; `registeredStorageDEKID` still holds the old
  id, so `Registered()` returns false.

The trigger is read off the shared `*encryption.StateCache`, which both
apply paths already update (`a.stateCache.RefreshFromSidecar(sc)` after
each `WriteSidecar`). 7b does not introduce a new wire-format event;
it observes the existing cache transitions.

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

2. **Dedupes** against the already-marked DEK: if
   `cache.Registered()` is true the loop body is a no-op for this tick.
   When the active DEK transitions to a new id and the registration for
   it has not yet committed, the body proceeds.

3. **Proposes** a registration entry for `(activeDEK, fullNodeID,
   w.epoch)` using the *same* `runWriterRegistration` helper 7a already
   uses. The `local_epoch` is `w.epoch` — this load's pinned epoch from
   `buildEncryptionWriteWiring` — which is what the nonce factory
   actually emits, so the writer-registry row that lands is consistent
   with on-disk nonces.

4. **Marks** on success via the existing `releaseBarrier(barrier,
   cache, activeDEK)` helper (with a fresh per-attempt barrier whose
   only consumer is the watcher). Once `MarkRegistered(activeDEK)`
   stores, `cache.Registered()` returns true and the gate clears.

5. **Backs off** between attempts using the existing
   `registrationRetryInitial / registrationRetryMax /
   registrationBackoffFactor`. Failures are bounded by `runCtx`; on
   shutdown the goroutine returns without leaving stale state.

The watcher does **not** touch the `RegistrationGate` the coordinator
holds — 7a's coordinator-layer barrier is a per-process-load
construct, not a per-DEK one. Runtime rotation does not introduce a new
coordinator barrier; it only needs the storage-layer `Registered()`
predicate to flip true again. The coordinator barrier from process
start has long since closed in normal operation.

### 2.2 Per-DEK `local_epoch` consistency

§4.1 nonce uniqueness is per-DEK over `(node_id, local_epoch,
write_count)`. The nonce factory is built once at process start with
`(NodeID16(raftID), w.epoch)` — this pair is reused under whichever
DEK is active when each Put fires. So when 7b proposes a registration
for a freshly rotated DEK, the registration's `local_epoch` MUST equal
`w.epoch` (the nonce factory's pinned value), not the sidecar's
`Keys[newDEKID].LocalEpoch` (which is 0 for a freshly-rotated DEK,
representing the cluster-wide first use of that key).

That is, the registration row records the node's pinned epoch under the
new DEK, mirroring the 7a propose path semantics: a registration for
`(D, node, e)` is the node's commitment that it will emit nonces
`(node, e, write_count)` under DEK `D`. The applier's §4.1 case 1 path
inserts FirstSeen = LastSeen = `e` exactly as designed.

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
  is false for the current DEK and the loop body is a no-op until a
  runtime cutover or rotation flips a predicate.

If a runtime cutover happens *before* process-start finishes (extremely
unlikely; admin RPC needs the cluster serving), the worst case is the
watcher and the process-start goroutine both propose registrations for
the same `(DEK, node, epoch)` triple. The §4.1 case 2-idempotent path
handles a duplicate apply as a no-op, so this is safe.

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
- `runRuntimeRegistrationWatcher` goroutine in `main_encryption_registration.go`.
- A new constant `runtimeRegistrationPollInterval` (default 1 s).
- main.go wiring: start the watcher from `setupDistributionAndRegistration`
  AFTER `installProcessStartRegistrationGate`, under the existing
  errgroup with `runCtx`.
- Reuse of the existing `runWriterRegistration` + `releaseBarrier`
  helpers (no new propose pathway).
- Tests: a fake cache that transitions `activeStorageDEKID` mid-watch
  to assert the watcher proposes for the new DEK and marks
  `Registered()` true; a transition where `StorageEnvelopeActive` flips
  false→true (the Phase-0 cutover case) to assert the same.

### Out of scope
- 7c (ConfChange-time registration for joining nodes) — a separate
  membership-driven mechanism, not cache-driven.
- Replacing the polling loop with a notification channel — possible
  follow-on once a workload need is identified.
- Rebuilding the nonce factory on rotation — the design above
  deliberately reuses the pinned `(node, epoch)` pair across DEKs,
  which is §4.1-safe because nonce uniqueness is per-DEK.

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
- **Data consistency** — registration `local_epoch` equals
  `w.epoch` (the nonce factory's pinned value), keeping the
  writer-registry row aligned with the on-disk nonces. The cutover and
  rotation applies remain unmodified.
- **Test coverage** — fake-cache transitions for cutover and rotation;
  shutdown cancels cleanly; concurrent process-start goroutine + watcher
  propose is no-op via case 2-idempotent.

## 5. Verification action items (for the implementation PR)
1. Confirm `runWriterRegistration` and `releaseBarrier` can be reused
   from the runtime watcher without leaking the conn cache (the
   goroutine-scoped `connCache.Close()` defer in 7a stays — each watcher
   propose attempt should similarly scope its own cache, or the
   long-running watcher should keep a single cache for its lifetime and
   close on `runCtx`).
2. Confirm `applyRotateDEK` updates the shared `StateCache` via
   `RefreshFromSidecar` AFTER the new DEK is durably committed
   (otherwise the watcher could observe `activeStorageDEKID = new` and
   propose before any node has the keystore entry to decrypt the
   resulting envelope). Existing code in `writeRotationSidecar` already
   refreshes the cache after `WriteSidecar` succeeds; just re-verify
   the ordering when implementing the watcher.
3. Decide the propose `local_epoch` policy explicitly in code: this
   document specifies it MUST equal `w.epoch`. Add a unit test pinning
   that the watcher proposes with `w.epoch`, not `sidecar.Keys[newDEK].LocalEpoch`.
