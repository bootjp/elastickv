# Stage 6D — `enable-storage-envelope` admin RPC + §7.1 Phase-1 cutover

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-18 |
| Parent design | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) |
| Blockers (now satisfied) | 6B (KEK plumbing), 6C-1 / 6C-2 (startup guards), 6C-2d (`ErrSidecarBehindRaftLog` wiring) |
| Bundles | 6C-3 (`ErrNodeIDCollision` + `ErrLocalEpochRollback` cluster-wide guards) |

## 0. Why this doc exists

Stage 6D is the first cutover that actually flips a cluster-wide
encryption state from OFF to ON. Per the encryption design doc
the cutover is described abstractly in §7.1 and the RPC in §6.6;
this doc pins the **as-implemented** decomposition (sub-PR
shape, wire-format addition, FSM apply path, storage-layer
toggle, capability fan-out helper, bundled 6C-3 guards, refusal
posture) so reviewers see one self-contained record before any
code lands.

The parent design doc lists 6D as a single milestone, but the
implementation surface is large enough (proto change + admin RPC
+ FSM apply path + storage-layer hook + capability fan-out
helper + 6C-3 cluster-wide guards) that shipping it as one PR
would be a 1000+ LOC review. The decomposition below splits it
into reviewable chunks while keeping the cutover semantics
atomic in the user-visible RPC.

## 1. Scope

### In scope (this milestone — 6D)

- **Wire format**: a new sub-tag value
  `RotateSubEnableStorageEnvelope` (concrete byte: `0x04`,
  matching the reservation comment block in
  `internal/encryption/fsmwire/wire.go` that already declares
  `0x04 — enable-storage-envelope (Stage 6, §7.1 Phase 1)`)
  on the §5.2 `OpRotation` payload's sub-tag byte. The existing
  `RotateSubRotateDEK` is `0x01` (shipped in Stage 4); reusing
  `0x01` for the cutover would break rolling-upgrade
  compatibility because pre-6D apply paths would interpret the
  cutover entry as a malformed RotateDEK (missing wrapped DEK)
  and halt the FSM. `0x04` is the only byte already reserved
  for this purpose by the wire-format authority. No new opcode;
  reuses the existing 0x05 rotation pipeline.
- **`enable-storage-envelope` admin RPC** in
  `proto/encryption_admin.proto` — new `EnableStorageEnvelope`
  unary RPC on the existing `EncryptionAdmin` service. Leader-only
  (returns `FailedPrecondition` with leader hint on followers,
  same as `RotateDEK` / `BootstrapEncryption`).
- **Voters ∪ Learners capability fan-out helper** in
  `internal/admin/capability_fanout.go` — calls `GetCapability` on
  every member of every Raft group in the route catalog and
  reports the verdict. Pre-condition for the cutover RPC. Reused
  later by Stage 6E.
- **FSM apply path**: extends `ApplyRotation` in
  `internal/encryption/applier.go` to handle the new sub-tag —
  sets `sidecar.StorageEnvelopeActive = true` AND records the
  applying raft index in a new `StorageEnvelopeCutoverIndex`
  sidecar field (symmetric with the existing
  `RaftEnvelopeCutoverIndex` field; see §6.4). Both writes land
  inside the same `WriteSidecar` fsync that 6C-2d uses for
  `RaftAppliedIndex` advancement.
- **§6.2 storage-layer toggle**: `store/mvcc_store.go` (and the
  underlying `store/lsm_store.go`) read `StorageEnvelopeActive`
  from the in-process sidecar view at write time. When `true`,
  encode the §4.1 envelope with `encryption_state = 0b01`. When
  `false`, write cleartext with `encryption_state = 0b00`. Read
  path is already dispatch-on-bit and unchanged.
- **Bundled 6C-3 startup guards**:
  - `ErrNodeIDCollision` — at startup, walk the local
    route-catalog snapshot (the default group's catalog is
    authoritative — same scope rule as the 6C-2d guard), hash
    each member's `full_node_id` to its 16-bit `node_id`, and
    refuse the local node's boot if any two distinct
    `full_node_id`s collide on the 16-bit slice. This protects
    the §4.1 16-bit nonce field from silent reuse across nodes.
    The check is local-snapshot-only — no `GetCapability`
    fan-out — because the startup-guard phase runs before the
    gRPC server is up; see §5.1 "Why local-snapshot-only" for
    the full rationale.
  - `ErrLocalEpochRollback` — at startup, read the LOCAL Pebble
    writer-registry record for `(full_node_id,
    active_storage_dek_id)`. The writer-registry is local state
    on every node post-bootstrap (every node applies the same
    `OpRegistration` and `OpBootstrap` entries); no RPC is
    required. Compare against
    `sidecar.Keys[active_storage_dek_id].LocalEpoch`. Refuse if
    `sidecar <= registry` (strict-ahead — see §5.2 for the
    equality-case rationale). This catches a stale node restarted
    on a sidecar older than the cluster's writer registry (e.g.
    sidecar restored from an old backup). Same
    local-state-only posture as `ErrNodeIDCollision`: the
    startup-guard phase has no gRPC server up to host a
    `ResyncSidecar` RPC, and the local Raft-applied state is
    authoritative.
- **CLI** `elastickv-admin encryption enable-storage-envelope` —
  invokes the new RPC; prints the resulting applied index and the
  capability-fan-out summary.

### Out of scope (deferred)

- The `rewrite` migration command (§7.1 step 6) — defers to
  Stage 9 per the parent doc.
- The `enable-raft-envelope` cutover (Phase 2) — Stage 6E.
- `--encryption-rotate-on-startup` ergonomics — Stage 6F.
- Reading historical cleartext values is already covered by
  §7.1's per-version `encryption_state` bit; no read-path change
  is part of 6D.

## 2. Wire format addition

### 2.1 New sub-tag on `OpRotation`

Today `fsmwire.RotationPayload.SubTag` carries one shipping value
plus a reserved block in the wire package's comment:

```go
RotateSubRotateDEK             = 0x01  // Stage 4 (shipped)
// 0x02 — rewrap-deks       (Stage 9, §5.4 / §5.5)        — reserved
// 0x03 — retire-dek        (Stage 9, §5.4)               — reserved
// 0x04 — enable-storage-envelope (Stage 6, §7.1 Phase 1) — reserved
// 0x05 — enable-raft-envelope    (Stage 6, §7.1 Phase 2) — reserved
```

Stage 6D promotes the `0x04` reservation to a declared constant:

```go
RotateSubEnableStorageEnvelope = 0x04
```

The byte matches the existing reservation block; declaring
`RotateSubEnableStorageEnvelope = 0x01` would collide with the
already-shipped `RotateSubRotateDEK` and break rolling-upgrade
compatibility (pre-6D apply paths reading a cutover entry as a
RotateDEK would halt the FSM on the missing wrapped DEK
constraint).

A rotation entry with `SubTag = 0x04` carries:

| Field | Type | Notes |
|---|---|---|
| `SubTag` | `uint8` | `0x04` |
| `Purpose` | `uint8` | MUST be `PurposeStorage` (`0x01`); any other value → halt apply with `ErrEncryptionApply` |
| `DEKID` | `uint32` | MUST equal `sidecar.Active.Storage` at the leader at propose time; the applier re-checks at apply time |
| `Wrapped` | `[]byte` | MUST be empty; the cutover does NOT add a new DEK |
| `ProposerRegistration` | `RegistrationPayload` | MUST have `DEKID = sidecar.Active.Storage`, `LocalEpoch` ≥ current registry record for that node (the proposer's epoch is expected to be at least the registry's current value; the strict-ahead invariant from §5.2 means after a normal boot it is strictly ahead, but a leader proposing the cutover on the same boot where it registered may legitimately be at equality, so the constraint accepts `=` as well as `>`). A proposer with `LocalEpoch < registry` is stale and the proposal is rejected. |

Constraints (validated in both the
`EnableStorageEnvelope` mutator on the server before propose
AND in `ApplyRotation` at apply time, in line with the
defense-in-depth posture used by `RotateSubRotateDEK`):

1. `Purpose == PurposeStorage` — flipping storage envelope ON
   does not affect the raft DEK; using any other purpose value
   is a malformed proposal.
2. `Wrapped == nil` — empty wrapped DEK distinguishes the
   cutover sub-tag from `RotateSubRotateDEK` which always carries
   a fresh wrapped DEK. A non-empty `Wrapped` on this sub-tag is
   a malformed proposal.
3. `DEKID == sidecar.Active.Storage` at apply time. **Race
   posture for a concurrent `RotateDEK`:** if a `RotateDEK`
   entry commits between the cutover's propose and its apply,
   `sidecar.Active.Storage` advances to the new DEK and the
   cutover's `DEKID` no longer matches. This is NOT routed
   through `ErrEncryptionApply` (halt-on-apply) — that would
   escalate a normal admin race into a cluster-stopping
   condition. Instead, the apply path treats the
   `DEKID != Active.Storage` case as a benign no-op:
   `ApplyRotation` advances `RaftAppliedIndex` (so the entry
   is consumed and not replayed) and records the entry as a
   `ErrCutoverDEKIDStale` outcome on the §6.4 response detail
   ride-along. The cutover-RPC mutator sees the stale outcome
   and surfaces `FailedPrecondition: ErrCutoverDEKIDStale` to
   the operator with a hint to retry against the new
   `Active.Storage` — exactly the retryable-precondition
   shape the codex finding asks for. The §4.1
   envelope-encoder invariant ("the active DEK is the only
   key that can encode a new envelope") is still preserved
   because the cutover entry is consumed without flipping
   `StorageEnvelopeActive`, leaving the cluster in the
   same state as if the cutover had never been proposed.
4. `sidecar.StorageEnvelopeActive == false` at apply time —
   idempotency guard. A duplicate cutover proposal (e.g.
   operator retries the RPC after a network blip and the first
   call already committed) is consumed by the apply path
   *without* re-flipping the field; the §6.4 stable cutover
   index (`StorageEnvelopeCutoverIndex`) was set on the FIRST
   apply and is NOT overwritten on subsequent applies, so any
   number of concurrently-committed duplicates resolve to the
   same idempotency-token value. The cutover-RPC mutator
   serializes overlapping calls on the propose side (a
   per-default-group mutator lock — `RotateDEK` and
   `EnableStorageEnvelope` share the same lock so they cannot
   interleave between propose and apply), so the
   "concurrent-overlap" race the codex finding identifies is
   bounded to "at most one cutover entry can be in flight at a
   time across all in-flight admin RPCs". Subsequent RPCs hit
   the §3.2 `AlreadyExists` path with the original
   `StorageEnvelopeCutoverIndex` from §6.4.

### 2.2 No new opcode

The cutover stays on `OpRotation = 0x05` per the parent design's
§5.5 opcode table. Old binaries that don't understand `SubTag =
0x04` halt the apply on the existing "unknown sub-tag" branch of
`ApplyRotation` (the parent doc's "old binaries fall through
with no matching case" safety property still applies; the
already-shipped `SubTag = 0x01` for `RotateSubRotateDEK` is
unaffected). For the cluster to reach the point of proposing the
cutover, the Voters ∪ Learners capability gate has already
confirmed every member runs a binary that knows the new sub-tag.

**Two-site whitelist change required in 6D-4.** The decoder side
of the wire-format check is currently in
`internal/encryption/fsmwire/wire.go::readRotationSubTag`, which
returns `ErrFSMWireSubtag` for any sub-tag other than
`RotateSubRotateDEK` (`wire.go:499`). The decoder rejects the
payload BEFORE `ApplyRotation` ever sees it, so a 6D-4
implementation that adds the new case only to the applier switch
would still halt every cutover entry with `ErrFSMWireSubtag`.
Stage 6D-4 must update BOTH sites:

  1. `readRotationSubTag` to whitelist `0x04` alongside `0x01`
     (the `wire.go:492-493` comment already anticipates this:
     "later stages (rewrap, retire, enable-flag) will whitelist
     new values").
  2. `ApplyRotation`'s sub-tag switch in
     `internal/encryption/applier.go` to dispatch the new
     sub-tag to the cutover handler.

The same two-site change applies to every future sub-tag
addition (`rewrap-deks`, `retire-dek`, `enable-raft-envelope`)
— a single-site fix would silently regress the others.

## 3. Admin RPC API

### 3.1 Proto addition

```protobuf
service EncryptionAdmin {
  // ...existing RPCs...
  rpc EnableStorageEnvelope (EnableStorageEnvelopeRequest)
      returns (EnableStorageEnvelopeResponse) {}
}

message EnableStorageEnvelopeRequest {
  // proposer_node_id and proposer_local_epoch fill the
  // ProposerRegistration on the wire payload, same shape as
  // RotateDEKRequest. The server validates:
  //
  //   - proposer_node_id != 0 — zero is a reserved sentinel
  //     across the mutator paths (BootstrapEncryption,
  //     RotateDEK, RegisterEncryptionWriter all reject it via
  //     ErrZeroFullNodeID); accepting it here would weaken
  //     the writer-registry collision and rollback
  //     invariants. Reject with InvalidArgument wrapping
  //     ErrZeroFullNodeID.
  //   - proposer_local_epoch <= 0xFFFF — the §4.1 16-bit
  //     nonce field is carried as uint32 on the wire (proto3
  //     has no uint16); values above 0xFFFF return
  //     ErrLocalEpochOutOfRange.
  //
  // Both checks fire at the server boundary before any Raft
  // proposal is composed. ApplyRotation re-validates them at
  // apply time (defense-in-depth, matching the
  // RotateSubRotateDEK posture).
  uint64 proposer_node_id    = 1;
  uint32 proposer_local_epoch = 2;
}

message EnableStorageEnvelopeResponse {
  // applied_index is the raft index of the cutover entry. On a
  // freshly-successful call this is the entry the leader just
  // proposed and waited to apply. On a retried call (see
  // was_already_active below), this is the recorded
  // sidecar.StorageEnvelopeCutoverIndex from the ORIGINAL
  // cutover (see §6.4) — stable across arbitrary subsequent
  // encryption-relevant Raft activity.
  uint64 applied_index = 1;

  // capability_summary records which (full_node_id) members were
  // probed during the pre-flight gate and what they reported.
  // Lets the operator see the membership view the leader used to
  // approve the cutover — useful for post-mortem if the leader's
  // route-catalog view diverges from the operator's expectation.
  // Empty on idempotent retries (was_already_active=true);
  // the membership view of the original cutover is not
  // retained.
  repeated CapabilityVerdict capability_summary = 2;

  // cutover_index_unknown is set by the defensive branch
  // described in §6.4 — only fires if a sidecar reports
  // StorageEnvelopeActive=true with
  // StorageEnvelopeCutoverIndex=0 (operationally impossible
  // under normal apply; hedges against future schema rollback /
  // hand-edited sidecar). On a healthy cluster this stays
  // false; applied_index then carries
  // sidecar.RaftAppliedIndex as a best-effort fallback. The
  // CLI surfaces it as a warning.
  //
  // This field is ONLY relevant when was_already_active=true.
  // On a fresh cutover (was_already_active=false),
  // applied_index comes directly from the Raft apply result,
  // not from reading sidecar.StorageEnvelopeCutoverIndex, so
  // the defensive branch cannot fire and this field is always
  // false.
  bool   cutover_index_unknown = 3;

  // was_already_active distinguishes a fresh cutover (false:
  // this call proposed the entry and waited for apply) from
  // an idempotent retry (true: the cutover had already been
  // applied before this call; applied_index carries the
  // ORIGINAL cutover index from sidecar.StorageEnvelopeCutoverIndex).
  //
  // The gRPC status code is OK in both cases, NOT
  // AlreadyExists. Returning AlreadyExists would set the
  // status to non-OK, and unary gRPC drops the response
  // message body on non-OK status (the generated `(*Response,
  // error)` shape only delivers the message when err == nil).
  // The idempotency contract therefore lives on the success
  // path with this boolean as the discriminator. The CLI
  // prints a different message for the two cases.
  bool   was_already_active = 4;
}

message CapabilityVerdict {
  uint64 full_node_id     = 1;
  bool   encryption_capable = 2;
  string build_sha          = 3;
  bool   sidecar_present    = 4;
}
```

### 3.2 Server semantics

`EnableStorageEnvelope` runs ONLY on the default group's
leader. Server-side sequence:

```text
1. Validate inputs: `proposer_node_id != 0` (reject with
   `InvalidArgument` wrapping `ErrZeroFullNodeID`),
   `proposer_local_epoch <= 0xFFFF` (reject with
   `InvalidArgument` wrapping `ErrLocalEpochOutOfRange`).
   Both checks at the server boundary before any Raft
   proposal is composed; `ApplyRotation` re-validates them at
   apply time (defense-in-depth).
2. Verify Stage 6B mutators are enabled (existing triple gate
   via encryptionMutatorsEnabled — encryption flag + KEK +
   capability advertised).
3. Verify we are the default-group leader. Followers return
   FailedPrecondition with leader hint, same as RotateDEK.
4. Verify sidecar has Active.Storage != 0 (bootstrap committed)
   — if not, return FailedPrecondition with hint to run
   BootstrapEncryption first. The cutover does NOT
   implicitly bootstrap; the parent design's §7.1 step 4
   "DEK bootstrap if necessary" reads as a separate explicit
   command in 6D.
5. Verify sidecar.StorageEnvelopeActive == false — if already
   active, return gRPC OK with was_already_active=true and
   applied_index = sidecar.StorageEnvelopeCutoverIndex (the
   original cutover entry's apply index, recorded by §6.4).
   This is stable across subsequent encryption-relevant Raft
   entries that advance the generic RaftAppliedIndex. The OK
   status is load-bearing: returning AlreadyExists would set
   the gRPC status to non-OK, and unary gRPC drops the
   response message body on non-OK status — so applied_index
   would be unreachable to the client. See §6.4 for the
   `cutover_index_unknown = true` defensive-branch fallback
   when StorageEnvelopeCutoverIndex == 0 on a sidecar that
   also reports StorageEnvelopeActive == true
   (operationally impossible under normal apply but hedged
   against future schema rollback / hand-edited sidecar).
   Skip step 6 (capability fan-out) on the
   was_already_active=true path — the original cutover already
   passed the gate; re-running it would add latency to
   no-op calls.
6. Run the Voters ∪ Learners capability fan-out (§4). On any
   member reporting encryption_capable=false or unreachable
   within one election timeout, return
   FailedPrecondition wrapping ErrCapabilityCheckFailed with
   the offending node IDs.
7. Compose the OpRotation payload with
   SubTag=RotateSubEnableStorageEnvelope, propose through the
   default group's leader.
8. Wait for apply to complete, return the applied index +
   capability_summary.
```

Server-side outcomes (failure cases map to a typed wrapped
error; the idempotent-retry case is a success — see §11.2 for
the rationale):

| Cause | gRPC code | Wrapped sentinel / discriminator |
|---|---|---|
| `proposer_node_id == 0` | `InvalidArgument` | existing `ErrZeroFullNodeID` (reserved sentinel across all mutator paths) |
| `proposer_local_epoch > 0xFFFF` | `InvalidArgument` | existing `ErrLocalEpochOutOfRange` (16-bit nonce-field bound) |
| Encryption disabled / KEK absent | `FailedPrecondition` | existing 6B `ErrEncryptionMutatorsDisabled` |
| Caller is a follower | `FailedPrecondition` | existing `ErrNotLeader` |
| Sidecar missing or `Active.Storage == 0` | `FailedPrecondition` | new `ErrEncryptionNotBootstrapped` |
| Already active (idempotent retry path) | `OK` | response's `was_already_active=true` and `applied_index = sidecar.StorageEnvelopeCutoverIndex` (the original cutover, §6.4). The OK code is intentional (see §3.2 step 5 + §11.2): unary gRPC drops the response body on non-OK status, so the idempotency contract must live on the success path. The CLI distinguishes the two outcomes by reading `was_already_active`. |
| Capability fan-out failed | `FailedPrecondition` | new `ErrCapabilityCheckFailed` (lists offending node IDs) |
| Apply halted (`SubTag` decode, idempotency, etc.) | `Internal` | existing `ErrEncryptionApply` |

## 4. Voters ∪ Learners capability fan-out

### 4.1 Helper location

New file: `internal/admin/capability_fanout.go`.

```go
package admin

type CapabilityVerdict struct {
    FullNodeID        uint64
    EncryptionCapable bool
    BuildSHA          string
    SidecarPresent    bool
    Reachable         bool   // false → RPC timed out or transport-level failure
}

type FanoutResult struct {
    Verdicts []CapabilityVerdict
    OK       bool   // true iff every verdict has Reachable && EncryptionCapable
}

// CapabilityFanout fans GetCapability out to every (voter ∪
// learner) of every Raft group in the supplied route-catalog
// snapshot. Returns within `timeout` regardless of how many
// members responded; missing responses surface as
// Reachable=false verdicts.
//
// All calls are concurrent. The helper holds at most one
// in-flight RPC per (group, node) — duplicates in the catalog
// (e.g. a node serving multiple groups) are deduplicated by
// full_node_id and probed exactly once.
func CapabilityFanout(
    ctx context.Context,
    routes RouteSnapshot,
    dial DialFunc,
    timeout time.Duration,
) (FanoutResult, error)
```

`DialFunc` reuses the existing admin connection pool (already
TLS-authenticated for `--adminTLSCertFile` setups) so no new
transport machinery is required.

### 4.2 Caller contract

The cutover RPC handler calls `CapabilityFanout` once and rejects
the proposal if `Result.OK` is false. The 6C-3
`ErrNodeIDCollision` startup guard does **not** reuse this helper
— see §5.1 below. The startup guard runs before the gRPC server
is up so it cannot dial peers; it walks the local route-catalog
snapshot instead. `CapabilityFanout` is exclusively for the
cutover-RPC handler path, where the gRPC server is already
serving and outbound connections to peers are usable.

The helper deliberately does NOT cache results. The parent
design pins fresh probing per cutover: "stale cached capability
state cannot trigger a premature cutover."

### 4.3 Timeout choice

`timeout` defaults to one `RaftElectionTimeout` (~1s in the
default config). Operators with WAN-latency clusters can pass a
longer value via a future `--encryption-cutover-timeout` flag,
deferred to Stage 6F.

A timeout firing on any member is a hard "no" — the cutover
returns `ErrCapabilityCheckFailed`. There is no partial-success
mode. Per the parent design: "If any member (voter or learner)
is unreachable or reports false within the window, the command
returns ErrCapabilityCheckFailed."

## 5. Bundled 6C-3 startup guards

### 5.1 `ErrNodeIDCollision`

**What it catches.** The §4.1 GCM nonce field is 16 bits of
`node_id` || 16 bits of `local_epoch` || 32 bits of counter.
`node_id` is derived as `xxhash(full_node_id) & 0xFFFF`. Two
nodes whose `full_node_id`s happen to hash to the same 16-bit
slice will reuse nonces under the same DEK with high probability
within a single epoch — catastrophic for AES-GCM.

**When it fires.** At startup, immediately AFTER the §9.1 6C-2d
gap guard (i.e. after the engine is opened) and BEFORE any
gRPC server starts serving.

**How it works.**
1. Read every voter+learner full_node_id from the local route-catalog
   snapshot (the default group's catalog is the authority for
   this — same scope rule as the 6C-2d guard).
2. For each, compute `node_id := xxhash(full_node_id) & 0xFFFF`.
3. If any two distinct `full_node_id`s map to the same `node_id`,
   return `pkgerrors.WithDetail(ErrNodeIDCollision, ...)`.

**Why local-snapshot-only (no fan-out).** The startup-guard
phase runs before the gRPC server is up, so a fan-out call
would require a separate dialing path. A local route-catalog
walk is sufficient because every node observes the same
catalog after applying the same Raft log — divergence in the
catalog itself is a separate problem (the catalog watcher
handles it, see `distribution/watcher.go`).

**Skip conditions.** Skip when `encryptionEnabled == false`
(no encryption → no nonce reuse risk). Skip when the
route-catalog snapshot is empty (e.g. fresh single-node
cluster pre-bootstrap; nothing to compare yet).

**Collision probability and mitigation.** A 16-bit `node_id`
derived via `xxhash(full_node_id) & 0xFFFF` carries a
birthday-bound collision risk of approximately
`N·(N−1) / (2·2¹⁶)` for N members — concretely
about **0.8% at N=32, about 12.5% at N=128, about 50% at
N=302** (the canonical √(2·d·ln 2) crossover). The 50%
point near N≈300 is the relevant threshold for "should we widen
the field"; small clusters stay well below 1%.

For a typical Raft-replicated cluster (3–7 voters, small
learner pool) the collision probability is on the order of
`N²/131072` — under 0.05% for N≤8 — but the failure mode is
"refuse to boot" rather than "silent nonce reuse", so even a
rare hit is operationally noisy.

Mitigations available to the operator when `ErrNodeIDCollision`
fires:

  1. **Re-roll the colliding member's `full_node_id`.** The
     `full_node_id` is a 64-bit operator-chosen identifier
     (typically a uint64 derived from hostname or assigned by
     the deployment tool). Picking a different value
     deterministically remaps the 16-bit slice. The Stage 6F
     `--encryption-rotate-on-startup` ergonomics flag will let
     operators retry the boot with a fresh `full_node_id`
     without a manual sidecar rewrite. Documented in the
     operator runbook delivered with Stage 6D-2.

  2. **Coordinated ID assignment** (cluster-wide convention).
     For new deployments, the operator can pre-compute the
     16-bit hash for each candidate `full_node_id` and only
     accept assignments that maintain a collision-free set.
     The 6D-2 CLI will ship a probe subcommand
     (`elastickv-admin encryption probe-node-id
     --full-node-id=<u64>`) that returns the derived 16-bit
     `node_id` and the current cluster's allocated set, so
     operators can verify before joining a node.

  3. **Future widening** (out of scope, noted for the record).
     The 16-bit `node_id` width is fixed by the §4.1 GCM
     nonce-field layout. Widening to 24 or 32 bits would
     require a wire-format change to the storage envelope —
     too large for a Stage 6 sub-PR; documented here so future
     work knows the ceiling exists.

### 5.2 `ErrLocalEpochRollback`

**What it catches.** A node's sidecar may be restored from an
older backup (e.g. operator copies an old `keys.json` while
debugging). The writer-registry record in Pebble (§4.1) is
authoritative for "the highest `local_epoch` this node has ever
used under each DEK"; a sidecar reporting a lower value
post-restart would let the node reuse nonces.

**When it fires.** Same phase as `ErrNodeIDCollision` (after
the 6C-2d gap guard, before serving).

**How it works.**
1. Read the local writer-registry record for `(full_node_id,
   active_storage_dek_id)` via Pebble directly (no Raft round
   trip — the registry is local state on every node post-bootstrap).
2. Compare against `sidecar.Keys[active_storage_dek_id].LocalEpoch`.
3. If `sidecar <= registry`, return `ErrLocalEpochRollback`
   wrapped with both values for operator triage. The check is
   `<=` (not strict `<`) because the §4.1 nonce-monotonicity
   invariant requires the sidecar's `local_epoch` to be
   **strictly ahead** of the registry record before this node
   resumes issuing nonces. On a normal restart the sidecar's
   epoch is incremented past whatever the registry recorded
   for the previous run, so `sidecar > registry` holds and the
   guard passes. An equality match means the sidecar's
   `local_epoch` has not advanced past the value the writer
   registry already saw the previous boot use — restarting on
   that sidecar would reissue `node_id‖local_epoch` with a
   counter reset to zero and recycle previously-used GCM
   nonces under the same DEK, which is exactly the divergence
   class this guard exists to prevent.

**Skip conditions.** Skip when `encryptionEnabled == false`.
Skip when `sidecar.Active.Storage == 0` (bootstrap not yet
committed; no DEK to compare against).

**Missing-registry-row posture.** When `encryptionEnabled ==
true` AND `sidecar.Active.Storage != 0` AND the local
writer-registry has no row for `(full_node_id,
active_storage_dek_id)`, the guard splits behaviour on the
§6.4 `sidecar.StorageEnvelopeActive` field:

  - If `StorageEnvelopeActive == false` (pre-cutover): allow
    startup. This is the legitimate freshly-joined-learner
    case — the node has applied the `OpBootstrap` entry (so
    `Active.Storage != 0`) but its own
    `RegisterEncryptionWriter` proposal has not been applied
    yet. The cluster is not yet generating encrypted-storage
    nonces, so there's no nonce-reuse risk to anchor against.
    The first write attempt after `RegisterEncryptionWriter`
    commits will establish the registry row, and the next
    restart will exercise the strict-ahead check.
  - If `StorageEnvelopeActive == true` (post-cutover): REFUSE
    with `ErrLocalEpochRollback` wrapped with a
    `missing_registry_row = true` detail. Once the cutover has
    fired, every active node MUST have a registry row before
    issuing any new GCM nonce under the active DEK; allowing a
    rowless node to start would let it issue nonces from
    `local_epoch=0` with no rollback anchor, recycling the
    nonce space the original registration was supposed to
    pin. Operators recover by replaying `RegisterEncryptionWriter`
    for this node (the registration is idempotent under §4.1
    case-2), after which the row is present and the guard
    passes.

### 5.3 Why both guards bundle here

6C-3 was deferred from 6C-1/6C-2 because both guards need a
cluster-wide membership view. The `ErrNodeIDCollision` guard
specifically needs the same route-catalog snapshot that the
cutover RPC's capability fan-out reads. Bundling them into the
same PR keeps the route-catalog read-path test fixtures shared,
and saves an admin RPC round-trip in operator workflows that
restart a node and then immediately run the cutover.

## 6. §6.2 storage-layer toggle

### 6.1 Read path: already done

§7.1 already specifies the per-version `encryption_state` bit
in MVCC metadata and the dispatch-on-bit decode. The current
codebase reads the bit and decodes accordingly (cleartext if
`0b00`, AEAD-unwrap if `0b01`). No 6D change to the read path.

### 6.2 Write path: new sidecar-state read

`store/mvcc_store.go::Put` (and the underlying
`store/lsm_store.go::PutAt`) currently always write
`encryption_state = 0b00` because the §6.2 toggle has never
been wired up. Stage 6D adds:

```go
// Pseudo-code; concrete signature lands in the implementation
// sub-PR (6D-5). The sidecar view is read once per Put (not
// per-MVCC-version) and is plain field-read on a sync.Map-
// backed snapshot — no lock contention.
func (s *mvccStore) putWithEncState(...) {
    encState := byte(0x00) // cleartext
    if s.encryption.StorageEnvelopeActive() {
        encState = 0x01 // §4.1 envelope
    }
    // ... existing encode path, with encState in the header byte ...
}
```

The `encryption.StorageEnvelopeActive()` accessor is added to
the same interface that already exposes
`encryption.Active.Storage` to the storage layer (the existing
read-side `Cipher` selection). It returns the in-process view
of `sidecar.StorageEnvelopeActive`, updated transactionally by
the FSM apply path when the cutover entry commits.

**Atomicity contract.** Every replica applies the cutover entry
deterministically through Raft, so all replicas flip
`StorageEnvelopeActive` at the same apply index. Reads before
that apply observe `false`; reads after observe `true`. There is
no per-replica drift window because the toggle is decided at
**apply time** on each replica's own FSM (the parent doc's
"Phase 1 doesn't need a proposal-quiescence barrier" rationale).

### 6.3 Mixed cleartext + encrypted within a single key

Already covered by §5.4 of the parent doc: a single key may have
older versions with `encryption_state = 0b00` (written
pre-cutover) and newer versions with `encryption_state = 0b01`
(written post-cutover). The MVCC read path dispatches per
version. The `rewrite` migration command (deferred to Stage 9)
backfills the old versions later; until then the mix is correct
by construction.

### 6.4 New sidecar field: `StorageEnvelopeCutoverIndex`

Stage 6D-4 adds a new field to the §5.1 sidecar struct,
symmetric with the existing `RaftEnvelopeCutoverIndex` reserved
for Stage 6E:

```go
type Sidecar struct {
    // ...existing fields...

    StorageEnvelopeActive       bool   `json:"storage_envelope_active"`
    StorageEnvelopeCutoverIndex uint64 `json:"storage_envelope_cutover_index"` // NEW in 6D-4
    RaftEnvelopeCutoverIndex    uint64 `json:"raft_envelope_cutover_index"`    // reserved for 6E
    RaftAppliedIndex            uint64 `json:"raft_applied_index"`             // 6C-2d
}
```

**Apply-time semantics.** When `ApplyRotation` handles the
`RotateSubEnableStorageEnvelope` sub-tag at raft index K, it
writes:

  - `sc.StorageEnvelopeActive = true`
  - `sc.StorageEnvelopeCutoverIndex = K`
  - `sc.RaftAppliedIndex` advanced to K via the existing
    `advanceRaftAppliedIndex` helper shipped in 6C-2d

All three writes land inside the single crash-durable
`WriteSidecar` fsync. Atomicity is provided by the §5.1
write-and-rename protocol — a crash mid-apply leaves either the
pre-cutover sidecar or the post-cutover sidecar on disk, never
a torn mix.

**Idempotency contract.** The Stage 6D-6 RPC handler reads
`sc.StorageEnvelopeCutoverIndex` directly when responding to
a retried call. The response carries gRPC status OK with
`was_already_active = true` and `applied_index =
sc.StorageEnvelopeCutoverIndex`. (The intuitive gRPC code
would be `AlreadyExists`, but unary gRPC drops the response
message body on non-OK status, so the idempotency contract
must live on the success path with `was_already_active` as
the discriminator — see §11.2.) Because the cutover-index
field is set at apply time and never re-written by later
encryption entries (only the cutover entry's apply sets it),
the response carries the **original** cutover index
regardless of how many subsequent encryption-relevant Raft
entries have advanced `RaftAppliedIndex`. Operators and
automation can use this as a stable idempotency token across
arbitrary cluster activity following the original cutover.

Older sidecars (pre-6D-4) decode the JSON with the field
absent, in which case `StorageEnvelopeCutoverIndex` defaults to
`0`. A `0` value on a sidecar where `StorageEnvelopeActive` is
also false is the "cutover hasn't happened yet" baseline. A `0`
value on a sidecar where `StorageEnvelopeActive == true` is
operationally impossible (the apply path writes both fields
together) but, defensively, the 6D-6 handler treats it as
"original index unknown — return the current `RaftAppliedIndex`
with a `cutover_index_unknown: true` flag in the response" so
the cutover-completed posture is still reported to the
operator. This branch should never fire in practice; it exists
only to harden against a hypothetical sidecar-edit attack or
a future schema rollback.

## 7. Decomposition into sub-PRs

| Sub-PR | Surface | Land order | Notes |
|---|---|---|---|
| **6D-1** | This design doc | First | Doc-only; reviewable as a self-contained record |
| **6D-2** | 6C-3 startup guards (`ErrNodeIDCollision` + `ErrLocalEpochRollback`) | Second | Cluster-wide guards bundled per parent doc §6C-3. No mutator changes. |
| **6D-3** | Capability fan-out helper in `internal/admin/` | Third | Used by the cutover RPC (6D-6) and reused later by Stage 6E. Standalone unit-testable. |
| **6D-4** | Wire format addition: `RotateSubEnableStorageEnvelope = 0x04` + `readRotationSubTag` whitelist (§2.2) + `ApplyRotation` sub-tag dispatch (writes `StorageEnvelopeActive=true` AND `StorageEnvelopeCutoverIndex=raftIdx` in sidecar; both writes inside one `WriteSidecar` fsync) | Fourth | No CLI, no §6.2 hookup yet. FSM-level testable with the existing applier_test patterns. Adds the new `StorageEnvelopeCutoverIndex uint64` field to the sidecar struct (§6.4). Operator-inert at this point. |
| **6D-5** | §6.2 storage-layer toggle (PutAt reads `StorageEnvelopeActive`) | Fifth | The §4.1 envelope encoder is already in `lsm_store.go`; this PR wires the toggle in front of it. Independent unit tests. Still operator-inert until 6D-6. |
| **6D-6** | `EnableStorageEnvelope` admin RPC + CLI command + integration test | Sixth | Composes 6D-3 + 6D-4 + 6D-5 into the user-visible cutover. End-to-end test exercises a single-node cluster doing Bootstrap → EnableStorageEnvelope → Put → read-back-via-envelope. |

Each sub-PR is independently revertable. 6D-2, 6D-3, 6D-4, 6D-5
land operator-inert (the cutover is not reachable from any CLI
until 6D-6 wires it).

## 8. Failure modes / refusal posture summary

| Scenario | Pre-Stage-6D | Post-Stage-6D |
|---|---|---|
| Operator runs `enable-storage-envelope` on cluster without KEK | RPC absent — undefined behaviour | `FailedPrecondition: ErrEncryptionMutatorsDisabled` |
| Operator runs it on follower | RPC absent | `FailedPrecondition: ErrNotLeader` with leader hint |
| Operator runs it before Bootstrap | RPC absent | `FailedPrecondition: ErrEncryptionNotBootstrapped` |
| Operator retries after success (idempotent) | RPC absent | `OK` with `was_already_active=true` and `applied_index = sidecar.StorageEnvelopeCutoverIndex` (the original cutover index). See §3.2 step 5 for why OK rather than AlreadyExists. |
| One learner unreachable during fan-out | RPC absent | `FailedPrecondition: ErrCapabilityCheckFailed` with that learner's node_id |
| Two nodes hash to the same `node_id` | Silent — would reuse nonces post-cutover | `ErrNodeIDCollision` at startup, refuse to boot |
| Sidecar restored from old backup | Silent — would reuse nonces post-restart | `ErrLocalEpochRollback` at startup, refuse to boot |
| Storage Put after a successful cutover | Wrote cleartext silently | Writes §4.1 envelope with `encryption_state = 0b01` |
| Storage Put on a key with pre-cutover versions | Cleartext-only | Existing versions stay cleartext; new versions are encrypted |

## 9. Test plan

### Unit tests (per sub-PR)

- **6D-2 (`ErrNodeIDCollision`)**: stub a route-catalog with two
  full_node_ids that hash to the same 16-bit slice → expect
  startup-guard error. Stub a route-catalog with no collision →
  expect nil. Stub empty catalog → expect nil. Stub
  `encryptionEnabled == false` → expect nil.
- **6D-2 (`ErrLocalEpochRollback`)**: stub a writer-registry
  record at `local_epoch=42` and a sidecar at `local_epoch=10` →
  expect startup-guard error with both values in the wrapped
  message. Sidecar equal to registry (`local_epoch=42` on both
  sides) → expect startup-guard error too (the §5.2 strict-ahead
  contract: a node may only resume issuing nonces when its
  sidecar is STRICTLY ahead of the registry record; the equality
  case would replay the same `(node_id, local_epoch)` prefix and
  reuse the counter under the same DEK). Sidecar strictly
  greater than registry → nil. `encryptionEnabled == false` →
  nil. Bootstrap not committed → nil. **Writer-registry has no
  record AND `StorageEnvelopeActive == false`** → nil (freshly
  joined learner that hasn't proposed a
  `RegisterEncryptionWriter` yet; the cluster is pre-cutover so
  no encrypted-storage nonces are being issued). **Writer-registry
  has no record AND `StorageEnvelopeActive == true`** → expect
  startup-guard error with `missing_registry_row = true` in the
  wrapped detail; the §5.2 missing-registry-row posture refuses
  startup because a rowless node post-cutover would issue
  nonces from `local_epoch=0` with no rollback anchor (operator
  recovers by replaying `RegisterEncryptionWriter` for this
  node, which is idempotent per §4.1 case-2).
- **6D-3 (fan-out)**: stub the `DialFunc` with a table-driven
  test of (every-capable, one-not-capable, one-unreachable,
  duplicate-nodes-deduplicated). Confirm `FanoutResult.OK`
  matches expectations and the verdict list is complete +
  deduplicated.
- **6D-4 (sub-tag dispatch)**: extend `applier_test.go` —
  invalid `Wrapped != nil` → halt apply. Invalid `Purpose` →
  halt apply. Mismatched `DEKID` → halt apply. Happy path
  (apply at raft index K) → `StorageEnvelopeActive` flips
  true, `StorageEnvelopeCutoverIndex == K` (the §6.4
  idempotency token — assertion is load-bearing: without it,
  the §3.2 / §6.4 idempotency contract would silently
  return 0 as the original cutover index on
  `was_already_active=true` retries), and
  `RaftAppliedIndex` advances. Idempotent re-apply at a
  later index → no-op (already active);
  `StorageEnvelopeCutoverIndex` stays at K, NOT the
  re-applying entry's index.
- **6D-5 (storage toggle)**: stub the sidecar state at
  `StorageEnvelopeActive=true`, write a value, confirm the
  on-disk MVCC version header carries `encryption_state=0b01`
  and the value bytes are an AEAD envelope. Flip the flag false
  → confirm cleartext. Same key, mixed versions → confirm read
  path returns the user payload regardless.
- **6D-6 (admin RPC)**: stub the FSM, the catalog, and the
  fan-out helper. Test the failure-mode matrix in §8 row-by-row.
  Specifically pin the idempotent-retry contract: stub a
  sidecar with `StorageEnvelopeActive=true` and
  `StorageEnvelopeCutoverIndex=K`, call the RPC, assert the
  response is gRPC `OK` with `was_already_active=true`,
  `applied_index=K`, `cutover_index_unknown=false`, and
  `capability_summary` empty. Plus the §6.4 defensive branch:
  stub the sidecar at `StorageEnvelopeActive=true` /
  `StorageEnvelopeCutoverIndex=0` (the operationally-impossible
  case), call the RPC, assert response is `OK` with
  `was_already_active=true`, `cutover_index_unknown=true`,
  and `applied_index = sidecar.RaftAppliedIndex` as the
  best-effort fallback.
  Integration test: build a single-node cluster, run
  `BootstrapEncryption` then `EnableStorageEnvelope` then a Put,
  read back via gRPC, assert ciphertext on disk via direct
  Pebble read.

### Property tests (`pgregory.net/rapid`)

- Generate a random sequence of (`Bootstrap`, `Rotate`,
  `EnableStorageEnvelope`, replay) operations and assert the
  applier reaches a consistent end state under arbitrary
  ordering of valid sequences.

### Jepsen workload (gated to 6D-6)

- The existing Jepsen DynamoDB workload extended with a
  mid-test `EnableStorageEnvelope` invocation. The workload
  must continue to pass linearizability under the cutover —
  no in-flight write is allowed to land at an applied index
  where the per-replica `StorageEnvelopeActive` view
  disagrees. This is the operational confirmation that the
  parent design's "Phase 1 doesn't need a proposal-quiescence
  barrier" rationale holds in practice.

## 10. Five-lens self-review of this design

1. **Data loss.** The cutover does not retire any DEK and does
   not delete any value. Pre-cutover versions stay cleartext;
   post-cutover writes are encrypted. The §5.4 mixed-version
   read path handles both. No path through this design loses a
   committed value.

2. **Concurrency / distributed failures.** Apply is
   deterministic across replicas. Every replica flips
   `StorageEnvelopeActive` at the same apply index. The fan-out
   is pre-flight — if any member is unreachable, the cutover
   does not propose. A leader change mid-cutover is handled by
   the existing `ErrNotLeader` retry path (operator re-runs;
   the new leader sees `StorageEnvelopeActive=true` if the
   entry already committed, returns OK with
   `was_already_active=true`).

3. **Performance.** Per-Put overhead post-cutover is one AES-GCM
   seal + 16-byte tag + per-version 1-byte header. Already
   benchmarked in earlier encryption stages. The
   `StorageEnvelopeActive()` accessor is a plain field read on a
   `sync.Map`-backed snapshot — no lock contention. The fan-out
   adds ~1s to the cutover RPC's wall time (one election timeout
   worth of concurrent probes) — paid once per cluster lifetime.

4. **Data consistency.** The cutover is itself a Raft entry, so
   its apply index is recorded in `RaftAppliedIndex` via the
   6C-2d advancement. The §9.1 guard then refuses startup if a
   later restart's sidecar is missing the cutover entry — the
   safety net is already in place. The MVCC read path's
   dispatch-on-`encryption_state` bit guarantees that the
   per-version decryption is correct regardless of when the
   value was written.

5. **Test coverage.** Five new unit-test surfaces are added (one
   per sub-PR). The Jepsen workload extension confirms the
   distributed behaviour. The bundled 6C-3 guards each have
   their own table-driven unit tests with all skip conditions
   exercised.

## 11. Open questions for the reviewer

1. **Sub-tag byte choice.** The chosen value is `0x04`, matching
   the existing reservation in
   `internal/encryption/fsmwire/wire.go` (`0x04 —
   enable-storage-envelope (Stage 6, §7.1 Phase 1)`). The current
   shipping value is `RotateSubRotateDEK = 0x01`, with `0x02`
   reserved for `rewrap-deks`, `0x03` for `retire-dek`, and
   `0x05` for `enable-raft-envelope` (Stage 6E). Choosing `0x04`
   honors the existing audit-anchored reservation block and
   avoids the rolling-upgrade hazard of reusing `0x01` (a
   cutover entry would be interpreted as a malformed RotateDEK
   by pre-6D apply paths).

2. **Idempotency response code.** Earlier rounds of this doc
   picked gRPC `AlreadyExists` for the duplicate-request
   semantic, but unary gRPC drops the response message body
   on any non-OK status (the generated `(*Response, error)`
   shape only delivers the message when `err == nil`).
   Picking `AlreadyExists` therefore makes the `applied_index`
   field unreachable on the retry path — the idempotency
   contract cannot be implemented as written. The doc now
   picks `OK` with a `was_already_active: bool` discriminator
   field so the response body carrying the original cutover
   index is actually delivered to the client. The CLI prints a
   different message for the two cases based on
   `was_already_active`. A third alternative — `AlreadyExists`
   with gRPC error details (`status.Status.details` carrying a
   proto attachment) — was considered and rejected as more
   boilerplate without a corresponding benefit: the
   discriminator-on-success-path approach is idiomatic for
   the existing CLI patterns and avoids the
   `status.Details()` unpacking that would otherwise be
   required.

3. **Fan-out helper location.** `internal/admin/` keeps it close
   to the gRPC server. An alternative is
   `internal/encryption/capability_fanout.go` — but the helper
   is reused by Stage 6E (raft envelope cutover), which is not
   strictly encryption-internal at the call-site level (Stage 6E
   threads through the admin RPC machinery the same way Stage
   6D does). The 6C-3 `ErrNodeIDCollision` startup guard does
   NOT reuse this helper — it consults the local route-catalog
   snapshot directly because the startup-guard phase has no
   gRPC server up. Picking `internal/admin/` matches the
   existing `internal/admin/config.go` precedent for shared
   admin
   helpers.

If a reviewer disagrees with any of these, the discussion lives
on the PR review of this doc, not on subsequent implementation
PRs. The choice in this doc is the working assumption for the
implementation sub-PRs that follow.
