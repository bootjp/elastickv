# Stage 6E — `enable-raft-envelope` admin RPC + Phase-2 raft cutover

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-31 |
| Parent design | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.2 raft envelope, §6.3 engine apply-hook, §6.6 admin RPC, §7.1 Phase-2 cutover) |
| Builds on | Stage 6A–6D (capability gate, storage envelope cutover, sidecar field plumbing), Stage 7 (writer registry), Stage 8a (snapshot header v2 cutover carriage) |
| Sibling slice | Stage 8b — WAL coverage (not blocked on 6E) |

## 0. Why this slice exists

Today every leader proposes Raft entries as plaintext: the
`internal/raftengine/etcd/engine.go::applyNormalEntry` path
decodes the proposal envelope and hands the FSM payload straight
to `kv/fsm.go::Apply`. The §4.2 raft envelope is fully specified
(DEK shape, KEK wrap, in-flight rotation), but the on-the-wire
activation — flipping `wrap on Propose` on the leader and the
matching `unwrap on Apply` on every replica — has not landed.
Without 6E, the cluster cannot reach Phase 2 even though every
prerequisite (6D Phase-1 storage envelope, 7 writer registry, 8a
snapshot cutover carriage) has shipped.

This slice lands the actual Phase-2 raft cutover end-to-end:
admin RPC, sidecar cutover-index recording, engine unwrap-on-
apply, coordinator wrap-on-propose, and the §7.1 proposal-
quiescence barrier that prevents the unwrap path from seeing a
plaintext entry at `index > cutover`.

## 1. Out of scope

- WAL-file at-rest encryption (Stage 8b — sibling slice).
- KEK rotation / DEK retire / rewrite (Stage 9).
- Jepsen workloads exercising the cutover under partition (Stage 9).
- KMS-backed wrappers (Stage 9; the existing in-process keystore
  is the only DEK source 6E sees).

## 2. Architecture

### 2.1 The three load-bearing pieces

1. **`EnableRaftEnvelope` admin RPC** (`proto.EncryptionAdmin`)
   — operator-facing entry point. Runs on the leader of the
   default Raft group; performs the §7.1 capability re-check,
   then drives the quiescence-barrier state machine.
2. **Engine apply-hook unwrap** —
   `internal/raftengine/etcd/engine.go::applyNormalEntry` calls
   `raftDEK.Unwrap(payload)` when `entry.Index > sidecar.RaftEnvelopeCutoverIndex`.
   The §6.3 hook is the dispatch boundary; every replica
   independently consults its **local** sidecar to decide
   wrap vs plaintext per entry.
3. **Coordinator wrap-on-propose switch** —
   `kv/coordinator.go` / `kv/sharded_coordinator.go` consult an
   in-process `wrapOnPropose` flag (set by the cutover barrier,
   reset never within a process lifetime) before calling
   `engine.Propose`. Phase-2 leaders wrap; Phase-0/1 leaders
   propose plaintext exactly as today.

### 2.2 The §7.1 6-step quiescence barrier

The barrier is the only place in the design that intentionally
blocks the proposal intake path. Its sole purpose: prevent a
plaintext proposal accepted between "cutover entry proposed" and
"`wrapOnPropose` flipped" from being appended at an index above
the cutover, which would then fail GCM verification on every
follower's apply path. Expected duration is one Raft commit
RTT (single-digit ms in a healthy cluster).

```text
leader_enable_raft_envelope():
  1. block new USER proposal intake at engine.Propose
     - return ErrEnvelopeCutoverInProgress to client coordinators
     - the block is keyed on a per-call "source" tag the
       coordinator already passes; source = "encryption_admin"
       bypasses the gate so step 3 can propose the cutover entry
  2. wait for the in-flight proposal queue to drain
     (all previously-accepted proposals committed and applied)
  3. encryption-admin path proposes the enable-raft-envelope
     entry (raftEncodeEncryptionRotation = 0x05, NOT raft-DEK-
     wrapped — the entry sits at index == cutover_index, and
     the engine hook fires only on STRICT > comparison)
  4. wait for that entry to commit AND for the local FSM apply
     to set raft_envelope_cutover_index in the sidecar
  5. flip the leader's "wrap on Propose" switch to true
  6. unblock USER proposal intake
```

The narrow user-vs-encryption-admin gate (rather than a global
Propose mutex) is the load-bearing detail: a global gate would
deadlock the cluster on its own cutover proposal. The exemption
also covers any other internal proposal that must be issued
mid-cutover — e.g., a `RegisterEncryptionWriter` triggered by
ConfChangeAddLearner during the barrier (§4.1 fourth path) uses
`source = "encryption_admin"` and bypasses the gate.

### 2.3 Strict-greater-than index dispatch

The dispatch is `entry.Index > cutover_index`, **strict**.
The cutover entry itself sits at `index == cutover_index` and
is NOT wrapped: the engine hook returns `false`, the entry
flows straight to FSM apply, and the apply sets the sidecar's
`RaftEnvelopeCutoverIndex` to its own index. A non-strict
`>=` comparison would attempt to unwrap the cutover entry,
fail GCM, and `ErrRaftUnwrapFailed` halts apply on every
replica — leaving the cluster stuck in Phase 1 forever.

### 2.4 The §6.3 hook lives in the engine, not the FSM

The Raft entry's `Index` is an engine concept; the FSM has no
notion of it. The unwrap therefore lives in
`internal/raftengine/etcd/engine.go::applyNormalEntry`, **before**
the FSM apply call. Concretely:

```go
// internal/raftengine/etcd/engine.go::applyNormalEntry
func (e *engine) applyNormalEntry(entry raftpb.Entry) error {
    id, maybeEncPayload, ok := decodeProposalEnvelope(entry.Data)
    if !ok {
        return nil // pre-envelope entry, leave intact
    }
    payload := maybeEncPayload
    // §6.3 hook: unwrap only at index strictly greater than
    // the locally-recorded cutover. The local sidecar is the
    // source of truth — every replica decides independently
    // and deterministically because the sidecar value is
    // itself replicated via the cutover entry's FSM apply.
    if entry.Index > e.encryption.RaftEnvelopeCutoverIndex() {
        var err error
        payload, err = e.encryption.RaftDEK().Unwrap(maybeEncPayload)
        if err != nil {
            // GCM tag mismatch = sidecar/keystore divergence
            // or on-disk corruption. Halt apply WITHOUT
            // setApplied so the next restart replays this
            // entry under a corrected keystore. Silent skip
            // would diverge the FSM.
            return errors.Wrap(err, "raft envelope: unwrap")
            // engine treats this as ErrRaftUnwrapFailed → fatal
        }
    }
    response := e.fsm.Apply(payload)
    e.resolveProposal(entry.Index, entry.Data, response)
    return nil
}
```

**CRITICAL**: `decodeProposalEnvelope` must run FIRST so the
proposal-ID handoff to `resolveProposal` stays intact. The raft
envelope wraps the FSM payload *inside* the proposal envelope;
wrapping `entry.Data` itself would clobber the proposal-envelope
version byte and break every coordinator write (timeout forever).

## 3. Milestone breakdown

The full Stage 6E is too large for a single PR. Split into
three milestones, **shipped in order** because the safety
properties chain:

### 3.1 Milestone 6E-1 — Admin RPC + sidecar plumbing (no behavior change)

**Scope**:
- New proto `EnableRaftEnvelope(Request)` returns `Response`.
- Server-side handler that proposes the cutover entry
  (`raftEncodeEncryptionRotation = 0x05`, payload identifies
  it as the raft-envelope cutover variant rather than the
  storage-envelope flag).
- FSM apply records the cutover entry's index in
  `sidecar.RaftEnvelopeCutoverIndex` via the existing
  `EncryptionApplier.ApplyRotation` seam.
- CLI subcommand `elastickv-admin encryption enable-raft-envelope`
  (mirrors the existing `enable-storage-envelope` shape).
- Capability re-check fanout (reuses the existing
  `GetCapability` fan-out from 6D, including
  `ConfState.Voters ∪ ConfState.Learners` per §7.1 step 3).

**What 6E-1 does NOT do**:
- Coordinator does not wrap on propose (no `wrapOnPropose` flag yet).
- Engine does not unwrap on apply (no §6.3 hook yet).
- Quiescence barrier not yet in place.

**Why this is safe to ship alone**: with no coordinator wrap
and no engine unwrap, the cutover index advances on the
sidecar but every Raft entry — including those at
`index > cutover` — is still plaintext on the wire. Every
replica sees plaintext, every FSM applies plaintext, every
read returns the right answer. The sidecar field is
load-bearing for 6E-2 / 6E-3 / 8a (already shipped — 8a
emits v2 snapshots carrying this index from now on) but
inert for the apply path until 6E-2 lands.

### 3.2 Milestone 6E-2 — Engine unwrap + coordinator wrap + quiescence barrier (the atomic flip)

**Scope** — all three pieces ship together because the
safety properties are interlocked:
- Engine `applyNormalEntry` adds the §6.3 hook (strict-`>`
  index comparison, `raftDEK.Unwrap` on hit, `ErrRaftUnwrapFailed`
  HaltApply path).
- Coordinator (`kv/coordinator.go` / `kv/sharded_coordinator.go`)
  adds a `wrapOnPropose atomic.Bool` checked before every
  `engine.Propose`; when true, the payload is run through
  `raftDEK.Wrap` first.
- `EnableRaftEnvelope` handler drives the §7.1 6-step
  barrier (intake gate via source-tag, drain, propose,
  await commit-and-apply, flip wrap-flag, unblock).
- New typed errors:
  - `ErrEnvelopeCutoverInProgress` — gate-returned error
    surfaced to coordinator clients during step 1.
  - `ErrRaftUnwrapFailed` — engine-fatal on GCM mismatch.
- New per-call `source` tag on the proposal path; encryption-
  admin path passes `source = "encryption_admin"` to bypass
  the intake gate. ConfChange-time `RegisterEncryptionWriter`
  proposals (Stage 7c §3.1) also pass the same source.

**Why 6E-2 cannot be split further**:
- Engine unwrap BEFORE coordinator wrap → coordinator sends
  plaintext, engine tries to unwrap, GCM fails, halt-apply
  on every replica.
- Coordinator wrap BEFORE engine unwrap → coordinator sends
  wrapped, engine passes wrapped to FSM, FSM cannot decode
  the protobuf, every apply errors.
- Either ordering produces a cluster-wide outage at the
  moment cutover applies. The pieces are atomic at the
  PR-shipping boundary, not just at the runtime cutover
  boundary.

**Quiescence barrier as the boundary**: even with both
pieces in the same PR, the runtime cutover window between
"propose cutover entry" and "flip wrap flag" can still leak
plaintext at `index > cutover`. The barrier closes that
window by blocking new intake, draining, then applying the
flip in a controlled sequence on a single goroutine.

### 3.3 Milestone 6E-3 — 6C-4 Phase-2-specific fail-closed guards

**Scope** (these are guards that only meaningfully fire once
6E-2 has shipped, so they bundle here):
- `ErrEnvelopeCutoverDivergence` — startup-time check that the
  sidecar's `RaftEnvelopeCutoverIndex` agrees with the snapshot
  header's recorded cutover (added by 8a). Divergence means
  the sidecar was hand-edited or a snapshot was restored under
  a different keystore; refuse to boot.
- `ErrEncryptionNotBootstrapped` — `EnableRaftEnvelope` returns
  this if `active.raft == 0` (no DEK pair yet). Pairs with
  the §7.1 step-4 sequencing rule.
- `ErrLocalEpochOutOfRange` on the wire side of `GetCapability`
  and `GetSidecarState` — pairs with Stage 7's writer-registry
  out-of-range check (catches a binary that has rotated past
  the cluster's expected epoch window).

**Why guards ship after 6E-2**: 6C-4 only matters once a node
can actually be in Phase 2. Without 6E-2, the guards never
have anything to gate on — they would be dead code.

## 4. Sidecar interaction

The sidecar's `RaftEnvelopeCutoverIndex` field already exists
(populated to 0 since Stage 5; consumed by Stage 8a's v2
snapshot writer; read by the engine apply-hook in 6E-2).
6E-1 wires the *write side*: the FSM's `ApplyRotation`
handler, on receiving the raft-envelope-cutover variant of
the 0x05 entry, sets the sidecar field to the entry's
applied index via the existing crash-durable
`WriteSidecar` fsync seam.

**Stage 6E does not change the sidecar wire format.** The
field has been on disk since 6D shipped; only its
load-bearingness changes per milestone.

## 5. Why the cutover entry is unwrapped at index == cutover

§7.1 step 4 says: "wait for that entry to commit AND for the
local FSM apply to set `raft_envelope_cutover_index`". The
entry sits at index N; the apply on every replica sets the
local sidecar's cutover to N. The engine hook's `entry.Index >
cutover_index` is false at exactly `entry.Index == N` because
the cutover was just set to N. So the cutover entry itself
flows through unwrap-free — it is the bootstrap moment, and
unwrapping it would require already-having-set the cutover
(chicken/egg). Strict-`>` is the right comparison.

For all entries at `index > N` (proposed after step 5 flips
the wrap-on-propose flag), the comparison is true, the
engine unwraps, and the FSM sees a clean payload. The
boundary is exact; no entry is ever wrapped-then-unwrapped
or unwrapped-then-not-wrapped.

## 6. Verification action items (for the implementation PRs)

### 6.1 Milestone 6E-1 tests

- `TestEnableRaftEnvelope_HappyPath_RecordsSidecarCutover` —
  invoke admin RPC, observe FSM apply at index N, sidecar
  reads back `RaftEnvelopeCutoverIndex = N`.
- `TestEnableRaftEnvelope_CapabilityGateRejectsStaleNode` —
  one learner returns `encryption_capable = false`; admin
  RPC returns `ErrCapabilityCheckFailed` with the node ID;
  sidecar's cutover stays 0.
- `TestEnableRaftEnvelope_NotBootstrapped` — `active.raft == 0`;
  RPC returns `ErrEncryptionNotBootstrapped`; sidecar
  unchanged.
- `TestEnableRaftEnvelope_FollowerForwardsToLeader` — call
  the RPC on a follower; the follower must forward (no
  silent acceptance).

### 6.2 Milestone 6E-2 tests

- `TestApplyNormalEntry_UnwrapAtIndexGreaterThanCutover` —
  craft an entry at index `N+1` whose payload is `raftDEK.Wrap(p)`;
  apply with local sidecar at cutover = N; expect FSM sees `p`.
- `TestApplyNormalEntry_NoUnwrapAtIndexEqualToCutover` — pin
  §5's strict-`>` rule. Entry at index N (the cutover
  entry itself) with cleartext payload; apply with local
  sidecar updated to N by the same apply; expect no
  `Unwrap` call and FSM sees the cleartext.
- `TestApplyNormalEntry_GCMFailureHaltsApply` — entry at
  index `N+1` with corrupted ciphertext; expect
  `ErrRaftUnwrapFailed` returned without `setApplied`,
  pinning the no-silent-skip rule.
- `TestQuiescenceBarrier_BlocksUserProposalsDuringCutover`
  — admin invokes barrier; concurrent user
  `engine.Propose` returns `ErrEnvelopeCutoverInProgress`
  until step 6.
- `TestQuiescenceBarrier_SourceTagExemptsEncryptionAdmin`
  — admin proposal (source = "encryption_admin") goes
  through during the barrier; pin against the
  "deadlock on own cutover proposal" failure mode.
- `TestQuiescenceBarrier_DrainsInFlightProposals` — N
  user proposals accepted at step 0; step 2 must wait for
  all N to commit-and-apply before step 3 fires.
- `TestCoordinatorWrap_OnlyAfterFlagFlip` — with
  `wrapOnPropose = false`, proposals are plaintext; flip to
  true; subsequent proposals are wrapped. Pin against any
  in-process race that could flip the flag mid-proposal.
- `TestPhase2EndToEnd_ProposeWrapApplyUnwrap` — full
  round-trip: coordinator wraps, engine unwraps, FSM
  applies, value lands in the store correctly.

### 6.3 Milestone 6E-3 tests

- `TestStartup_RefusesOnCutoverDivergence` — sidecar
  cutover = 100, snapshot header cutover = 200; boot must
  return `ErrEnvelopeCutoverDivergence` per 6C-4.
- `TestEnableRaftEnvelope_RequiresBootstrap` (already in
  6E-1) — `ErrEncryptionNotBootstrapped` typed return.
- `TestGetCapability_LocalEpochOutOfRange` — node's local
  epoch is below the cluster's `min_observed_epoch`; RPC
  returns `ErrLocalEpochOutOfRange`.

### 6.4 5-lens self-review for each milestone

Each implementation PR runs the standard 5-lens self-review
per CLAUDE.md. Particular attention by milestone:

- **6E-1**: data consistency (cutover index correctness on
  the FSM apply), data loss (no entry can be lost because
  no wrap/unwrap activates yet).
- **6E-2**: data loss (the runtime cutover window is the
  only window where a malformed dispatch could halt apply
  cluster-wide; the barrier is what closes it),
  concurrency (the wrap flag must be atomic; barrier
  state machine is a single goroutine on the leader),
  data consistency (strict-`>` index dispatch invariant).
- **6E-3**: data consistency (startup refusal on
  divergence is the last line of defense against silent
  keystore drift); no data loss because guards are
  read-side.

## 7. Rollout / migration

6E ships in three sequential PRs; the cluster can stay in
Phase 1 indefinitely after each milestone. The cutover itself
is an operator-initiated `enable-raft-envelope` command;
there is no auto-flip. Pre-cutover snapshots are v1 (Stage
8a, ceiling only); post-cutover snapshots are v2 (Stage 8a,
ceiling + cutover). 8a's v2 reader is already on every node,
so a fresh follower joining mid-Phase-2 reconstructs the
dispatch boundary from the snapshot header.

**Sequencing with parent design's stage table**:

| Stage | Status | Notes |
|---|---|---|
| 6D | shipped | Phase-1 storage envelope cutover |
| 7 | shipped | Writer registry + deterministic nonce |
| 8a | shipped | Snapshot header v2 cutover carriage |
| **6E-1** | this slice — milestone 1 | Admin RPC + sidecar plumbing |
| **6E-2** | this slice — milestone 2 | Engine unwrap + coord wrap + barrier (the atomic flip) |
| **6E-3** | this slice — milestone 3 | 6C-4 fail-closed guards |
| 8b | independent sibling | WAL coverage |
| 9 | future | KMS + compress + rotation/retire/rewrite + Jepsen |

## 8. After 6E

- **Stage 8b** — WAL coverage (Raft log encryption on disk).
  Not blocked on 6E; sibling slice.
- **Stage 9** — KMS-backed wrappers, compression,
  rotation/retire/rewrite, full Jepsen coverage of Phase-2
  cutover under partition + slow-follower + leader-flip.
