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
     every other node's `w.epoch` at apply time. A natural defence
     ("take the cluster-wide max") is a **strictly larger
     coordination problem than option (b)**: it requires a
     pre-rotation quorum read to collect every node's `w.epoch`,
     take the max, embed it in the rotation entry, then commit —
     i.e., a separate consensus sub-problem before the rotation
     can even be proposed. Without that pre-coordination, if
     `proposer.w.epoch < non_proposer.w.epoch`, applying the
     proposer's value would trigger the same brick / nonce-reuse
     failure modes that motivated this slice.

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

- Inject a `localEpoch uint16` value into the `Applier` via a new
  `WithLocalEpoch(uint16)` functional option (symmetric with
  `WithStateCache`, `WithSidecarPath`, etc.).
- The value is `encryptionWriteWiring.epoch` — the same pinned
  `w.epoch` the nonce factory is constructed with at process start.
  `main_encryption_write_wiring.go`'s `buildEncryptionWriteWiring`
  already computes this value; this slice exposes it to the
  `Applier` construction site in `main.go`.
- `writeRotationSidecar` reads `a.localEpoch` (defaulting to `0`
  when the option was not supplied — preserving the FSM-internal
  test harnesses that construct an `Applier` without write-path
  state) and writes that value into `sc.Keys[newDEK].LocalEpoch`
  **only when `p.Purpose == PurposeStorage`**.

#### 3.1.1 Why the value is storage-only — Raft DEK rotations must keep `LocalEpoch: 0`

`applyRotateDEK` handles both `PurposeStorage` and `PurposeRaft`
rotations today (`writeRotationSidecar` dispatches on `p.Purpose`
when assigning `sc.Active.{Storage,Raft}`). The
`encryptionWriteWiring.epoch` value piped in by `WithLocalEpoch` is
the **storage** write-path's pinned `w.epoch` — the value the
deterministic nonce factory under the storage envelope is using.

A raft DEK rotation has its own (future) per-purpose epoch
counter; cross-applying the storage nonce factory's epoch to a
raft DEK's `LocalEpoch` would corrupt the raft DEK's counter
before the raft envelope path consumes it (codex P2 on PR #855).
Until raft envelope support lands with its own per-purpose
plumbing, raft DEK rotations MUST continue to write `LocalEpoch:
0` exactly as today — `writeRotationSidecar`'s switch on
`p.Purpose` gates the new behaviour to storage:

```go
switch p.Purpose {
case fsmwire.PurposeStorage:
    sc.Active.Storage = p.DEKID
    keyLocalEpoch = a.localEpoch
case fsmwire.PurposeRaft:
    sc.Active.Raft = p.DEKID
    keyLocalEpoch = 0  // current behaviour preserved
}
```

A static `uint16` rather than a `func() uint16` provider is the
right shape today. `BumpLocalEpoch` only runs at process start
(§9.1) — there is no runtime epoch-bump path, so a late-bound
provider would close over the same constant. If a future slice
introduces runtime bumps (e.g. rotation-with-epoch-renew, see §4),
the option will be upgraded to read from an `atomic.Uint32` or
shared state at that point. Adding the provider indirection now,
purely for a hypothetical future caller, violates the codebase's
"don't design for hypothetical future requirements" convention.

#### 3.1.2 Backward compatibility — pre-bootstrap and unwired tests

`a.localEpoch` is `0` (the zero value) for two cases:
1. Pre-bootstrap process loads (no sidecar, `w.epoch == 0`).
2. Test harnesses that instantiate an `Applier` without the
   `WithLocalEpoch` option.

The applier MUST gracefully accept `0` in both cases — existing
applier tests construct an `Applier` for FSM-internal behaviour
(bootstrap, rotation, registration) and have no notion of a
nonce-factory pinned epoch. The pre-bootstrap case is naturally
indistinguishable from the unwired-test case — both write
`LocalEpoch: 0`, which is the value the cluster already used
before this slice for every rotation.

**Ordering invariant: a `RotateDEK` entry is only committed
AFTER `EnableStorageEnvelope` has applied** (claude review on PR
#855). The order is enforced by the propose-side mutator: the
admin-RPC handler refuses to propose `RotateDEK` until
`StorageEnvelopeActive == true`. So a pre-bootstrap node
replaying the log sees `EnableStorageEnvelope` first and only
then sees `RotateDEK`. Two sub-cases under which
`applyRotateDEK` then runs:

- **Same-load Phase-0 boot.** Both entries apply during a single
  process load with `w.epoch == 0` throughout (no restart
  between them, so no `BumpLocalEpoch` advance). Writing
  `LocalEpoch: 0` to `Keys[newDEK]` is **correct** here: no
  writes have been emitted under the new DEK with a non-zero
  epoch on this node, so the sidecar accurately records its
  highest-emitted local_epoch of `0`. The watcher will not
  propose for this load (the 7a process-start path will register
  on next restart with `BumpLocalEpoch`-advanced `w.epoch`).

- **Restart between entries.** A node that crashed between the
  two applies, or that started fresh after both committed,
  reaches `applyRotateDEK` post-restart with `w.epoch > 0`.
  This is the case option (b) is designed for.

The invariant is "writing `LocalEpoch: w.epoch` is correct
regardless of whether `w.epoch` is 0 or positive" — the static
`a.localEpoch` field provides the right value in both sub-cases.

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

**Check ordering (load-bearing).** `runtimeRegistrationInScope`
MUST short-circuit in this order (claude review on PR #855):
1. **Already-registered short-circuit:** `cache.Registered(activeDEK)
   → true` → return `_, false` (no-op).
2. **Scope conditions:** evaluate the in-scope branches below.
3. **Propose** (from `runtimeRegistrationTick`, not this function).

If the `lastRegisteredDEK != activeDEK` test were checked before
`cache.Registered()`, a `B → C → B` oscillation would trigger a
spurious re-propose for `B` after the rotation back from `C`
(`lastRegisteredDEK == C ≠ B`, but `cache.Registered(B) == true`).
This is safe via §4.1 case-2 idempotent but wasteful; ordering
(1) eliminates it.

The scope-check then reads, in order:
- In scope: `bootDEKID == 0` (pre-bootstrap), OR `activeDEK ==
  bootDEKID` (cutover), OR `activeDEK != bootDEKID &&
  lastRegisteredDEK != activeDEK` (rotation, not yet registered
  for this DEK).
- Out of scope: any other combination.

#### 3.2.1 `lastRegisteredDEK` reset on restart

`lastRegisteredDEK` is per-process-load state (not durable). On
restart it resets to `0`. If a rotation happened while the node was
down (`bootDEKID == A`, `activeDEK == B`), the first tick observes
`lastRegisteredDEK (0) != activeDEK (B)` and proposes — the
intended behaviour. The §4.1 case-2 idempotent acceptance covers a
race where a prior load of the same node already registered the row.

#### 3.2.2 Stale-cache transient

If the in-memory `StateCache`'s `Registered` set hasn't yet
reflected a prior registration when a rotation oscillates (`B → C
→ B` before the cache catches up to `B`'s registration), the
watcher observes `lastRegisteredDEK == C ≠ B` and
`cache.Registered(B) == false` → spurious re-propose for `B`.
This is safe via §4.1 case-2 idempotent acceptance and self-heals
on the next tick once the cache refreshes; it is documented here
so the implementation PR's tests do not flag it as a bug.

### 3.3 Apply-side ordering invariant

`applyRotateDEK`'s ordering is **unchanged** from current
[`internal/encryption/applier.go::applyRotateDEK`](../../internal/encryption/applier.go)
(only the value written in step 3 changes):

1. KEK-unwrap new DEK.
2. Keystore set (`a.keystore.Set(p.DEKID, dek)`).
3. **Sidecar write — with `a.localEpoch` for `Keys[newDEK].LocalEpoch`
   when `p.Purpose == PurposeStorage`, `0` otherwise** (see §3.1.1).
4. **Proposer registration** (`ApplyRegistration(p.ProposerRegistration)`)
   — proposer's value is `(newDEK, proposer.node_id, proposer.w.epoch)`,
   set by the proposer at propose time. Verbatim into the writer
   registry.

Step 3 changes from `LocalEpoch: 0` to `LocalEpoch: a.localEpoch` (for
storage). Step 4 is unchanged.

**Sidecar-before-registration is the existing `applyRotateDEK`
ordering** (coderabbitai major review on PR #855 raised concern
this conflicts with a "registration-before-sidecar" invariant —
but that invariant lives in `applyEnableStorageEnvelope`, NOT
`applyRotateDEK`):

- `applyEnableStorageEnvelope` runs `ApplyRegistration` *before*
  `WriteSidecar` because the sidecar's
  `StorageEnvelopeActive=true` flip is the durable
  idempotency-marker. If a crash strands the cutover with the
  marker flipped but no proposer-registration row, the §5.2
  startup guard refuses boot AND FSM replay short-circuits on
  the already-active no-op branch — permanently losing the
  registration. So that path reverses ordering. This is
  explicitly documented in
  [applier.go's `applyEnableStorageEnvelope` fresh-success
  branch](../../internal/encryption/applier.go).

- `applyRotateDEK` uses sidecar-before-registration because the
  rotation has no equivalent idempotency-marker: a crash between
  the sidecar write and the registration insert means
  `sc.Active.Storage == newDEK` but no proposer registry row.
  Recovery: on next process start, **7a's startup registration
  path** observes the new active DEK and proposes
  `(newDEK, proposer.node, proposer.w.epoch)` itself —
  self-healing without requiring FSM replay. Non-proposers
  similarly self-heal via the 7b watcher (this slice). No durable
  fail-open / fail-closed skew is possible.

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
   - `TestApplier_ApplyRotateDEK_LocalEpochAppliedPerApply`:
     verify the static `a.localEpoch` value is correctly read
     on each FSM apply call, not inadvertently reset or derived
     from mutable state. A multi-apply test fixture (two
     rotations in sequence) suffices to pin that the value
     remains stable across applies.

2. New `main_encryption_registration_test.go` cases extending the
   existing watcher harness. Split into **propose-call** vs
   **short-circuit** so each test exercises one path
   independently (coderabbitai minor on PR #855):

   - `TestRuntimeRegistrationTick_RotationInScopeInvokesPropose`:
     `bootDEKID == X != 0`, `activeDEK == Y != X`,
     `lastRegisteredDEK != Y`, `w.epoch == 5`. NO pre-populated
     registry row. Asserts the tick invokes the propose path (use
     a fake `Coordinate` whose `Propose` method records the
     entry) with payload `(Y, node, 5)`.
   - `TestRuntimeRegistrationTick_RotationVerifyShortCircuitsToMark`:
     same wiring as above, but **pre-populate** a registry row at
     `(Y, node, 5)`. Asserts `verifyRegistered` short-circuits,
     `MarkRegistered(Y)` fires, and the fake `Coordinate.Propose`
     is NOT called.
   - `TestRuntimeRegistrationTick_RotationAlreadyRegisteredIsNoOp`:
     after a first-tick MarkRegistered, a second tick at the same
     `activeDEK` is a no-op. Asserts `cache.Registered(Y)` short-
     circuits `runtimeRegistrationInScope` and `lastRegisteredDEK`
     is NOT updated (claude review nit on PR #855 — confirm it
     stays at the original value, not reset to 0 or rewritten).
   - `TestRuntimeRegistrationTick_RotationToYetAnotherDEKReProposes`:
     after registering for DEK Y, a subsequent rotation to DEK Z
     (`activeDEK == Z`, `lastRegisteredDEK == Y`) triggers a fresh
     propose. Pins that `lastRegisteredDEK` updates to Z.
   - `TestRuntimeRegistrationTick_RotationOscillationBtoCtoB`:
     after registering for B then C, rotation back to B with
     `cache.Registered(B) == true` short-circuits at check
     ordering step (1) — no spurious re-propose. Pins the
     §3.2 check-ordering invariant.

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

### 6.1 Rolling-upgrade mitigation strategies

The mixed-version availability degradation above is a real
operational risk: a `RotateDEK` issued during a rolling upgrade
(some nodes still 7b) leaves every 7b node fail-closed under the
new DEK until the operator restarts it on 7b'. Three layered
mitigations close this gap, in order of operator friction:

1. **Operational guideline (lowest friction).** The Stage-7
   operations runbook (parent design §10) will explicitly call out
   that `RotateDEK` MUST NOT be triggered during a rolling upgrade
   that crosses the 7b → 7b' boundary. The 7b' release notes name
   this constraint; an alerting check on `elastickv_encryption_*`
   metrics can warn if the cluster reports mixed versions and a
   `RotateDEK` is observed in the audit log.

2. **Admin RPC capability gate (middle friction).** The
   `EncryptionAdmin.RotateDEK` RPC handler (in the propose-side
   mutator) gates on a cluster-wide capability probe — gossiped or
   queried via `Distribution.ListNodes` — refusing to propose if
   any peer advertises a version older than 7b'. This is
   server-side and cannot be bypassed by a misconfigured client.
   Failure surface: the RPC returns
   `FailedPrecondition: cluster contains pre-7b' nodes; complete
   the rolling upgrade before rotating`. The probe is best-effort
   (a freshly-joined node could race the check), but the §4.1
   case-2 idempotent posture means a false-positive admit only
   degrades availability of the pre-7b' node — it never corrupts.

3. **Cluster-version Raft entry (highest friction; deferred).** A
   future slice could land a `ClusterVersion` Raft entry that
   every node bumps on startup; `RotateDEK` apply would refuse to
   commit if the entry reports a pre-7b' member. This is the
   strongest form but adds a new wire entry and a coordination
   protocol — overkill for a single-feature gate.

7b' will ship (1) and (2). (3) is explicitly deferred — it has
broader applicability than just this slice (snapshot v2 in Stage 8
will face the same problem) and deserves its own design.

Roll-forward path: deploy 7b' to every node in the cluster, verify
the capability probe reports all-7b', then issue `RotateDEK`. The
7b' watcher on each node naturally absorbs the rotation.

Rollback path: rolling back to 7b after a 7b' RotateDEK has
applied is safe (claude review on PR #855 corrected the
mechanism). The boot sequence under the 7b binary:

1. The sidecar's `Keys[newDEK].LocalEpoch = prior.w.epoch` value
   satisfies the §9.1 startup guard. `BumpLocalEpoch(newDEK)`
   advances `prior.w.epoch → prior.w.epoch + 1`; the guard
   checks `prior.w.epoch + 1 >= lastSeen (= prior.w.epoch)` —
   true. Boot succeeds.
2. `bootDEKID = activeDEK = newDEK` (no rotation from this
   boot's perspective — the rotation already happened in the
   prior 7b' load and is durably committed). The 7a
   process-start registration path runs and proposes
   `(newDEK, node, prior.w.epoch + 1)`. §4.1 case-2 monotonic
   acceptance lands the row.
3. The 7b watcher's first tick observes `cache.Registered(newDEK)
   == true` (the 7a startup path just MarkRegistered) and
   returns a no-op for the rest of the load.

The 7b binary's deferred-rotation log-once-skip branch **does NOT
fire** on this rollback — that branch only activates when
`bootDEKID != activeDEK`, but after rollback the two are equal
(it is the cutover/cold-start branch that runs, exactly as on a
fresh 7b binary boot into a cluster that has the new DEK
active). The writer registry's `(newDEK, node, prior.w.epoch)`
row written by 7b' remains in place — case-2 monotonic
acceptance accepts the 7a-startup `(newDEK, node, prior.w.epoch
+ 1)` advance without interference.
