# TSO Batch Slot Claims

Status: Partial
Author: bootjp
Date: 2026-08-29

## 1. Problem

Under Phase D the group-0 leader is the only issuer of persistence timestamps,
and `RaftTSOAllocator.ValidateDurableTimestamp` (`kv/tso_raft.go`) is what
enforces that: a write may only persist at a timestamp the TSO has durably
handed out. Today that check is a **range** test:

```go
floor := a.state.PhaseDFloor()
end := a.state.AllocationFloor()
if timestamp == 0 || timestamp > end { ...invalid... }
if timestamp <= floor { ...pre-phase-D... }
```

`AllocationFloor` is a single scalar: the highest committed batch **window end**.
It records that a window was reserved, not which offsets inside it were issued.

`BatchAllocator.refill` (`kv/tso.go`) commits that window end through
`commitAllocationFloor` (`kv/tso_raft.go`) *before* any offset in
`[base, end]` is handed to a caller, and `tryWindowAfter` then hands offsets out
locally with `w.offset.Add(1)` — no further contact with group 0. So from the
instant a window commits, every value in it validates, whether or not a real
write ever claimed it.

With the default `defaultTSOBatchSize = 256` (`main.go:58`) — the same value
`TSORuntimeController.installMode` uses for Phase D as for cutover — that is a
255-wide band of timestamps that validate but belong to nobody.

**Consequence.** A caller that reaches the internal listener can persist at an
unclaimed slot. When the owning `BatchAllocator` later reaches that offset it
hands the same timestamp to an unrelated write, so two distinct writes commit at
one timestamp. That breaks the uniqueness OCC ordering assumes: the conflict
check is `latestTS(key) > startTS`, and two commits sharing a timestamp can each
read the other as not-newer.

**Exposure (as of 2026-08-29; still open).** `adminTokenProtectedMethod`
(`adapter/admin_grpc.go`) matched only the `/Admin/` prefix, so `Internal.Forward`
was not behind the admin token. That was peer-port reach, not public-client
reach, but it was unauthenticated at the gRPC layer, and the window stayed open
for as long as the batch survived — unbounded under low write traffic.

An attempted bearer-token gate was withdrawn after review: old followers cannot
attach the new header during a rolling upgrade, the peer transport is plaintext,
and the legacy coordinator has a separate forwarding path. A safe staged rollout
and authenticated transport are prerequisites for §3.3. The slot-claim hole
itself still needs §3.2.

## 2. Non-goals

- Changing how timestamps are ordered or how OCC validates them.
- Removing reservation batching. Concrete claims may require a separate
  consensus before every return, including singleton consensus at low load.
  The explicit decision in section 4a supersedes the former no-per-Next rule.
- Anything about the legacy (pre-cutover) path, which does not use
  `ValidateDurableTimestamp` at all.

## 3. Options

### 3.1 Force `batchSize == 1` while Phase D is active

`installMode` selects the allocator per mode already, so Phase D could install a
batch allocator of size 1. Each `Next()` then commits exactly the value it is
about to return.

- Removes the multi-slot band entirely.
- Does **not** fully close the hole: a gap remains between
  `commitAllocationFloor` returning and the caller stamping its write. It
  narrows from "as long as the window lives" (unbounded) to one Raft round trip.
- Costs one group-0 Raft round trip per issued timestamp. This is the change
  that needs weighing: it is exactly the per-`Next()` consensus the HLC design
  avoids, applied to the Phase-D path.

### 3.2 Durable per-slot claim record

Validation proves the caller claimed the slot, rather than that the slot lies in
a reserved range. The allocator commits a concrete claim before returning it.
A per-owner watermark is insufficient: `NextAfter(base+6)` can issue `base+7`
without issuing any of `base+1..base+6`. Membership alone is also insufficient:
the claim must bind the owner, operation, intent and timestamp role.

- Structurally closes the hole, including the post-commit gap in 3.1.
- Needs a wire/on-disk decision: what a claim record is, who owns it, how it is
  compacted, and what happens to claims when leadership moves. That is why this
  is a design doc rather than a patch.

### 3.3 Authenticate the internal listener

Orthogonal and worth doing regardless, but it narrows *who* can exploit the gap
rather than closing it. Two elastickv nodes that legitimately reach each other
still can.

## 4. Recommendation

3.2 with owner/operation binding is the selected fix. Section 4a records the
explicit acceptance of its consensus cost; section 3.1 alone remains insufficient.
Authentication in section 3.3 is tracked separately and is also a production
activation prerequisite, not a substitute for slot claims.

## 4a. Decision (updated 2026-10-30)

The user explicitly selected pre-return `ClaimBatch` agreement: **wait for
group-0 commit and apply before returning an issued slot**. Additional latency
and low-load singleton consensus are accepted. This supersedes the 2026-09-15
provisional rejection of per-Next consensus, including conflicting historical
no-per-Next guidance. It does not select section 3.1: a one-slot reservation is
not an owner/operation-bound claim and does not close the commit/reply gap.

Reservations remain batched. A bounded claim combiner may combine concrete
requests, but must not claim skipped offsets or infer issuance from a range.

### Lifecycle

This document remains `_partial_`. Claim-aware Next, raw/one-phase admission,
sharded raw/prefix broadcast and Forward/Distribution paths are connected behind
explicit identity/authority configuration (section 8), not a completed runtime
rollout. Unconfigured legacy issuance still uses the old contract. It becomes
`_implemented_` only when every production issuer and receiver uses the bound
contract, the activation gates are installed and the original counterexamples
are green on those production paths.

## 5. Contract And Remaining Questions

1. Consensus before return is accepted, including singleton consensus.
2. Claims identify exact concrete slots, not owner watermarks.
3. Initial retention is no deletion: retain grants, closed grants and claims;
   stop at hard capacity rather than evicting live transaction evidence.
4. Production activation still requires authenticated owner derivation, decoder
   coverage of every voter AND learner, a membership creation fence and durable
   transaction/prepare/resolver bindings. None is established by a context bool,
   method name, process-local cache or caller-supplied owner label.

## 6. Test plan

- A validation test that an unissued slot inside a committed window is refused.
- A property test that no two `Next()` results share a timestamp across
  concurrent allocators.
- A leadership-change test: a window committed by the old leader must not
  validate slots the new leader has not issued.
- Whichever option lands, a benchmark on the Phase-D issuance path.

## 7. Concrete Ledger Kernel (Initial Stage)

This section records the initial fixed-candidate kernel and its historical
verification. Section 8 describes the later production-path integration. Its
benchmarks must not be substituted for this kernel's measurements, or vice versa.

`kv/tso_claim*.go` adds a separate typed contract. These package-private Raft
issuance entry points and the combiner are deliberately not installed in the
runtime, Distribution RPC or Forward handlers. They must not be advertised as
closing the existing production vulnerability.

### Record And Authority

- A grant fixes ID, owner, Raft term, base and count. Its commit advances the
  allocation floor but authorizes no timestamp use.
- A claim fixes grant ID, exact timestamp, stable claim ID, owner principal,
  operation/transaction ID, intent digest, start/commit role and credential
  hash. The replicated record contains no credential secret.
- The caller creates and durably retains the stable credential before issuance,
  and retains the exact concrete candidate before proposing it. On an unknown
  result it reconciles by the same credential/binding or replays the identical
  candidate; it must not assign a replacement slot because a lookup is absent.
  A higher-level allocator still needs that durable pending-attempt lifecycle.
- A proof is not trusted because its struct is populated. Validation compares
  the entire record with applied group-0 state, checks the credential hash and
  compares the binding with an independently derived expected binding.
- Owner labels are data, not authenticated identity. Production ingress must
  obtain the owner from authenticated transport and recompute the intent from
  the actual operation. Plaintext bearer transport is not sufficient.
- Start claims permit read/prepare timestamps. Commit claims permit one-phase,
  prepare-evidence, primary, secondary and resolution timestamps. Legitimate
  transaction phases reuse a claim; validation does not consume it. Cross-group
  intent derivation and durable binding propagation are not implemented yet.
- A new-term grant closes every old-term grant. Old committed claims remain
  available for retry/resolution, while old unclaimed offsets cannot be added.
  FSM replay consults only committed data, never RPCs or current local leadership.

### Bounds And Snapshot

The combiner accepts fixed candidates, holds at most 256 queued submissions,
collects at most 64 requests for at most 200 microseconds, and has a five-second
proposal budget. Each encoded batch is bounded by its fixed 153-byte records
plus its header. Low load produces a singleton batch. These bounds do not imply
that an overloaded caller is guaranteed service: queue saturation returns an
explicit capacity error.

Initial retention caps are 65,536 claims and 4,096 grants. No GC is implemented;
the serialized retention boundary must be zero. Capacity exhaustion fails
closed. A future GC must establish a durable cross-group lower bound for all
live intents, retries and resolver obligations before advancing that boundary.

The separate `EKVCLM01` component stores the scalar TSO state, term, retention
boundary, every grant/closed flag and every concrete claim. Encoding is sorted;
decoding rejects excessive counts, overflowed ranges, unknown roles/flags,
duplicates, overlap, truncation and trailing bytes. Restore cannot discard known
claims or reopen old grants. An old scalar snapshot creates no claims and cannot
replace a populated ledger. `EKVTSO06` remains reserved for the full control
manifest; integration with that manifest is still required. No production claim
activation record exists yet, so an activation state is not being silently
approximated by the presence of a ledger.

### Initial Production Integration Audit

The initial handoff identified the following integration work. Section 8 records
which paths have since been connected; this is not the current completion list.
Legacy range validation is NOT a fallback for an untrusted claim-aware write:

- `kv/tso.go`: Next/NextAfter, batch refill, fresh admission, begin-read helpers,
  pre-Phase-D vouchers and all durable persistence/start/commit validators.
- `kv/tso_runtime.go`, `kv/tso_raft.go`, `kv/coordinator_forwarder.go`: dynamic,
  shadow, leader-routed and coordinator wrappers must preserve typed proofs.
- `kv/coordinator.go`, `kv/sharded_coordinator.go`: operation identity and intent
  must precede allocation; raw, DEL_PREFIX, one-phase and distributed txn paths
  must preserve the intended binding. Read-start and commit intents require
  explicit distinct roles, not a method-derived trust flag.
- `kv/timestamp_admission.go`, `kv/transaction_timestamp_admission.go`: derive
  expected bindings from actual requests; caching solely by uint64 is unsafe.
- `kv/coordinator_prepare_evidence.go`, transaction metadata and lock resolver:
  persist the binding needed by prepare, primary, secondary and resolution.
  Reservation-above-a-bound is not evidence that the bound itself was claimed.
- `proto/internal.proto`, `proto/distribution.proto`, adapter Forward and
  Distribution glue: carry proofs, stable attempts, typed lookup and explicit
  protocol acknowledgments without silently accepting old responses.
- Store-owned prepared-lock evidence and admission enforcement, and
  bootstrap/peer-session/main-owned authenticated principal, membership decoder
  inventory and creation-fence integration need coordination with their owners.
  This kernel does not edit those modules or assume the separate transport work
  is already deployed.

### Evidence Classification

The independent read-only counterexamples used the actual production allocator
and FSM with a recording engine: unissued `base+1` validated, NextAfter skipped
six offsets, and local issuance left snapshot bytes unchanged. They are not a
distributed Raft fault campaign and the legacy path remains unresolved here.

The new concrete-claim tests cover unissued/skipped slots, immutable operation
bindings, proof forgery, reusable transaction phases, atomic batch rejection and
snapshot reconstruction. The snapshot-change test was observed RED before the
codec was connected, then GREEN. Separate tests use a native single-node Raft
WAL and reopen/new-term election. A separate subprocess test kills a native node
after its applied acknowledgment, without Engine.Close, and reconciles the claim
after WAL recovery. This single-node crash is not a multi-node
leadership-transfer campaign. Complete production red-to-green,
multi-node fault campaigns and end-to-end latency/throughput evidence remain
release requirements.

### Initial Native Measurement (2026-10-30)

On darwin/arm64, Apple M1 Max, Go 1.27.3, GOMAXPROCS 10:

```text
go test ./kv -run '^$' -bench '^BenchmarkTSOConcreteClaimNative$' -benchtime=128x -count=3 -timeout=3m
singleton: 9,924,829 / 9,091,112 / 9,050,449 ns/op; 1.000 claims/batch
parallel:    834,061 /   958,585 /   915,214 ns/op; 9.846 claims/batch
```

This is native single-node WAL consensus through the new combiner, not the
runtime's old Next path. Grant reservation and credential preparation are
outside the measurement. Parallel ns/op is aggregate wall time divided by
completed operations, not individual request latency. No multi-node network,
authenticated transport, durable caller-attempt storage, admission or transaction
cost is included. These numbers establish a local singleton-consensus cost and
actual combining, not a production performance claim.

### Initial Kernel Verification Handoff

- New concrete-claim tests passed with race detection and three repetitions:
  `go test -race ./kv -run '^TestTSO(ConcreteClaim|Claim)' -count=3 -timeout=2m`.
- Initial kernel whole-repository lint passed with zero issues using the existing configuration
  and `--timeout=5m --allow-serial-runners`. No lint rules were disabled.
- `go test -race -json ./kv -count=1 -timeout=10m` first failed because the
  sandbox denied localhost listener binds. The same suite outside the sandbox
  passed once in 111.005 seconds.
- A subsequent whole-KV race run failed only
  `TestMultiGroupPrimaryDecisionSurvivesProcessKill`: the secondary was already
  committed before its explicit recovery assertion. The identical test source
  in the separate read-only main fixture reproduced that assertion in 5 of 10
  race runs without any claim changes. This is an unresolved pre-existing
  instability in that checkout, not a clean final whole-suite pass. The parent
  owns the later crash-fixture correction; it has not been imported or retested
  here. No timeout, payload,
  iteration count or criterion was weakened, and that test was not edited.
- The original read-only counterexample tests were rerun through a Go overlay;
  the unissued-slot test remains RED on legacy Next/range validation, and the
  skipped-slot and byte-identical-snapshot diagnostics still reproduce.
- Jepsen was not run: the prescribed `/tmp/lein` executable is absent.

Raw whole-KV and baseline JSON logs are retained under the dedicated
`.cache/tso-durable-slot-claims/` evidence directory outside the writer worktree.
No commit, push or PR was performed. The runtime must not activate this kernel
until the production integration audit above is complete.

## 8. Production-Path Integration (2026-10-30, Partial)

### Issuance And Binding

`RaftTSOAllocator.ClaimTimestamp` combines signed intents, not locally selected
scalar candidates. The committed group-0 intent batch deterministically selects
the first concrete slot and fixes claim ID, owner, operation, intent, role and
credential hash. Retrying the same signed attempt returns that applied record;
it cannot rebind the ID, including after reply loss or a new grant/term. Skipped
NextAfter offsets are absent from the claim ledger. Replay uses only committed
data and signature verification, not RPC or leader-local authority callbacks.

The signing seed must be provisioned durably. Owner is the public-key
fingerprint, not a caller-provided owner label. Credentials derive from seed,
operation ID, role and intent; request intent is recomputed from actual ordered
mutations, read keys, route version, transaction metadata and participant/group
projections. A proof is checked against both that expected binding and the
applied group-0 record. Mutation ordering/multiplicity is preserved. Only the
declared commit-timestamp patch region is normalized, and its stamped bytes must
equal the claimed timestamp.

Configured Next/NextAfter return committed read-observation claims. Those claims
cannot authorize writes. A supplied transaction start remains a read watermark;
PREPARE and commit persistence use a separate commit-role claim. A shared txn
claim is not consumed by primary/secondary/resolution validation.

### Connected Paths

- Batch, Raft, dynamic, shadow, leader-routed and coordinator wrappers carry
  typed issuance/lookup. Distribution uses exact wire encodings and version
  acknowledgments; an old response is not a scalar fallback.
- Coordinate raw and one-phase dispatch retain the request manifest and proof.
  A redirect claims at the origin before Forward, so another data leader cannot
  issue the attempt under its own different seed. Forward validates the proof,
  actual payload and receiving group rather than creating a replacement claim.
- Sharded raw and DEL_PREFIX broadcasts carry a shared participant manifest.
  A prefix leaf explicitly selects one signed prefix index, never an arbitrary
  subset of point writes. Whole-broadcast admission, migration and route-floor
  checks remain before commits. Admission validates every request even when
  timestamps repeat or the admission floor is zero, under one shared budget.
- PREPARE stores a proof capsule and its hash in versioned locks at the concrete
  persistence claim. Resolver requests recover this retained evidence. COMMIT
  and ABORT require that capsule and original participant keys; another operation
  or a bare one-phase proof is insufficient. Proof validation/reconstruction is
  tested independently of the unfinished atomic cleanup apply dependency below.
- Decoder observation probes every configured voter and learner, checks term/
  configuration stability, and requires the prepared-claim apply capability for
  data groups. It is an observation, not persisted activation authority.

### Public Combiner And Measurement

The actual public ClaimTimestamp queue holds at most 256 pending requests plus
one active batch of at most 64. Collection waits 200 microseconds; each flush
has a five-second proposal/authorization budget. The fixed-width signed intent
batch is at most `18 + 64 * 253 = 16,210` bytes. Every call retains its own
authentication context. Low load commits a singleton. Saturation returns an
explicit capacity error. Concurrent actual BatchAllocator.Next tests verify
unique applied membership before return and observe non-singleton proposals;
queue-bound and per-call authority isolation tests run with race detection.
Public issuance and lookup also apply a five-second request budget (or an earlier
caller deadline); mutex contention is cancellable rather than an unbounded wait.

On darwin/arm64, Apple M1 Max, Go 1.27.3, GOMAXPROCS 10:

```text
go test ./kv -run '^$' -bench '^BenchmarkTSOProductionPublicNextNative$' -benchtime=128x -count=3 -timeout=2m
singleton: 7,664,421 / 7,001,761 / 5,369,487 ns/op; 1.000 claims/batch
parallel:    898,605 /   672,955 /   768,494 ns/op; 7.111 / 9.143 / 9.846 claims/batch
```

These measurements use the actual public Next -> signed ClaimTimestamp ->
native single-node WAL path, including signing, leader/applied fences and grant
creation. Pinned-key test authority excludes TLS/registry provisioning and
network RPC. Parallel ns/op is aggregate wall time/completed operations, not
individual latency. This is local evidence of real-path combining, not a
multi-node production throughput claim. These public-path repeats committed
128 requests in 18 / 14 / 13 intent batches, respectively. Raw output is retained
at `.cache/tso-durable-slot-claims/public-next-native-final.log` outside this writer
worktree. The kernel's 9.846 claims/batch in section 7 is NOT evidence for public
Next; the equal numeric value in the last public repeat is independently counted
by its native engine wrapper, not copied from that kernel benchmark.

### Evidence And Open Gates

The configured actual Next/NextAfter/FSM fixture rejects unissued/skipped slots
and changes/restores snapshot claims. The old direct uint64/range counterexample
remains RED on the unconfigured legacy path; it has not been relabeled fixed.
Raw/one-phase, sharded raw/multiple-prefix broadcast, unknown-reply attempt
reconstruction, and phase-evidence/patch-forgery tests exercise actual production
coordinator/admission/FSM code. Native Distribution/Forward tests include a lost
applied claim reply, origin/receiver distinct seeds, payload forgery, bare scalar
rejection and an old decoder response. These are not a distributed fault campaign.

Full runtime activation remains prohibited until the following are completed:

- The transport/control owner must implement `TSOClaimAuthority` using verified
  peer identity plus a registered signing key and active decoder/fence epoch.
  Required API: `AuthorizeTimestampClaim(ctx, [32]byte publicKey)` and
  `AuthorizeTimestampClaimLookup(ctx)`. Lookup authenticates the current caller
  while allowing resolution of an older retired signer. No production authority
  is installed here; the pinned-key implementation exists only in tests.
- Provision protected durable node seeds (the bounded local APIs in section 9
  are not startup/auth integration), register their public keys, verify
  every voter/learner decoder, persist the creation fence/activation epoch, then
  expose issuance. Wire the same gate into future member admission. The parent
  owns full-manifest composition; EKVTSO06 is not used by the claim component.
  Forward's reply acknowledgment is not a pre-write gate against an old receiver
  that ignores new fields. Decoder/creation fencing must precede the first
  forwarded proof, not be inferred from that reply. Bootstrap must provision
  `WithRaftTSOClaimIdentity` / `WithTSOClaimAuthority` and each receiving data
  group's `WithCoordinateTimestampClaimGroup` / `WithInternalTimestampClaimGroup`.
- Request manifests allow stable reconstruction while retained, and PREPARE
  capsules retain transaction resolution state. A crash-safe ingress pending-
  attempt journal/recovery protocol before PREPARE/raw publication is still
  required; an in-memory OperationGroup or test clone is not that lifecycle.
- The store owner must implement atomic
  `ApplyPreparedMutationsRaftAt(ctx, mutations, readKeys, originalStartTS,
  commitTS, expected []store.MVCCVersion, appliedIndex)`. It must preserve user/
  read-key OCC at originalStartTS and exempt only exact compared txn-internal
  prepared versions. The strict distributed PREPARE/COMMIT test remains RED
  without this capability. No widened read-start workaround or fake applier is
  used. Legacy durable resolution across activation and composed route retry
  integration also remain release work.
- Complete authenticated activation, multi-node leadership/crash campaigns,
  snapshot/control composition, retention obligations, full KV race/lint/Jepsen
  verification on the integrated sources, and deployment performance evidence.
  Initial no-GC retention still fails closed at its documented caps.

### Current Verification

- `go test -race ./kv -run '^TestTSO(ConcreteClaim|Claim|ProductionClaims(Next|Raw|Unknown|OnePhase|Public|Concurrent|Combiner|NewTerm|Sharded|Prepared))' -count=3 -timeout=3m`
  passed in 24.813 seconds. This selection does not include the unfinished
  distributed transaction test; it is not a whole-KV result.
- The additional actual public-claim native WAL lost-reply/reopen test passed
  with `-race -count=3 -timeout=2m` in 2.487 seconds. It recovers the identical
  applied record and rejects old-term unclaimed grant offsets. Graceful WAL
  reopen is distinct from the retained kernel subprocess kill test.
- After adding cancellable allocator-lock waits and public request budgets,
  the production-path race selection (including the lookup deadline test) passed
  three repetitions in 10.525 seconds with a three-minute command budget.
- `go test -race ./adapter -run '^TestTimestampClaim' -count=3 -timeout=2m`
  finally passed in 3.919 seconds, with localhost TCP enabled outside the sandbox.
  This includes actual origin Coordinate redirect and receiver distinct seeds.
  An earlier repeat exposed that the legacy route retry classifier treated a
  decoder mismatch as transient and could replace it with a deadline error.
  Claim routing now returns that mismatch immediately; the unchanged error
  assertion is supplemented by an exact one-RPC assertion. The intermediate
  failed repeat is not a passing result.
- `go test ./... -run '^$' -timeout=3m` passed compilation of every package;
  this command intentionally runs no tests. Current whole-repository lint
  returned zero issues using the unchanged configuration and five-minute budget.
- The read-only original counterexample overlay was rerun: unissued base+1
  rejection still exits 1 on legacy range admission; both skipped-offset and
  unchanged-snapshot diagnostics reproduce. The configured claim-aware path's
  separate green tests do not erase this RED legacy evidence.
- The strict distributed prepare/commit test is retained with its original
  success assertion and `RequireAllParticipants=true`; missing atomic store
  capability is a RED dependency, not a skipped success or fake applier.
- An extra broad race selection including `TestReadKeysCapacityStagedVisibility`
  hit its two-minute command timeout in that capacity test. It is not a passing
  compatibility result. No payload, iteration count, assertion or existing test
  source was reduced to make it pass. Final full-KV race/Jepsen remains pending;
  the parent's separate crash-fixture work was not duplicated.

No commit, push or PR has been performed. Connected paths and local green tests
do not promote this partial integration to full production completion.

## 9. Original-Start Correction And Local Seed Custody

### Original Start Versus Retry Baseline

The independent prepared-subset checkpoint fixture exposed a P2: after a durable
primary decision, `commitApplyStartTS` returns commitTS for legacy idempotency.
That value was also passed to concrete prepared-lock ownership validation, whose
lock still records the ORIGINAL startTS. The real COMMIT handler therefore
returned ErrTSOClaimInvalid even though retained proof and exact prepared
versions were valid, before reaching the missing lower store capability.

`handleCommitRequest` now carries originalStartTS separately through
`recordAndApplyCommit` and `applyCommitWithIdempotencyFallback`. Concrete expected-
version validation and the atomic applier receive originalStartTS. Legacy retry
logic alone retains applyStartTS. All callers were audited: one COMMIT chain and
the ABORT chain, which already used the original start. Store contracts, auth
callbacks and legacy retry rules were not weakened.

The maintained
`TestTSOProductionClaimsPreparedSubsetCheckpointRetainsOriginalStart` preserves
the independent fixture's assertions: real PREPARE, real primary-only checkpoint,
retained claim validation, two exact remaining prepared versions, then the real
COMMIT handler. Before the correction it failed with ErrTSOClaimInvalid; after it
reaches ErrTSOProtocolUnsupported for the absent store API. That is a diagnostic
P2 green, NOT a successful 2PC transaction. Before/after logs are retained outside
the writer under `.cache/tso-durable-slot-claims/prepared-subset-{before,after}.log`.

### Full Retry Composition Gate

`TestTSOProductionClaimsPreparedFullRetryAfterPrimaryCheckpoint` includes BOTH
already-resolved b and still-prepared c. It requires actual COMMIT success,
unchanged b, committed c, lock/intent cleanup and successful replay. It has no
Unsupported success exemption or fake applier. It remains RED on the local old
base, as does the original strict distributed prepare/commit success test.

Static inspection confirms a separate integration risk: missing primary lock
already suppresses b's user mutation, but `buildCommitStoreMutations` still adds
the primary commit-record Put whenever the request includes b, even if its
durable decision already exists. No new runtime finding is claimed before
composition. The strict atomic store never exempts user or commit-record writes
from original-start OCC. If the full retry fails there after composition, omit
only an independently verified exact already-applied decision/write; do not
widen startTS or exempt the commit namespace.

The reviewed five-file store component is frozen at tree
`a2d70352024977568a9f860510523e15f07b9133`, signed commit `3ec44fa5`, based on
`3eaa`. It requires that full control-store ancestry; it has NOT been copied
alone into this worktree's old `0d5c5c230` base. The parent composes it after
handoff and replaces the intermediate diagnostic Unsupported assertion with
actual success/data/cleanup/replay validation. Local lower-capability RED must
not be reported as composed 2PC green. The incoming independent export
`3e66d751df72361f968494246aacad0ba7ec9094` and private reviewer fixture remain
read-only.

### Read-Set Audit

Write-participant reads are checked by PREPARE's `validateTxnConflicts` and the
atomic store call carrying r.ReadKeys at the original startTS. PREPARE creates
locks/intents for mutation keys, not durable transaction read locks for every
read-only key. The retained claim capsule binds participant read keys, but that
binding is not a commit-time concurrency-validation certificate.

Existing primary/secondary COMMIT and resolver paths do not revalidate the full
read set. Read-only shard validation uses a linearizable barrier followed by
LatestCommitTS checks; its existing documented TOCTOU/SSI limitation remains.
Accordingly `applyPreparedClaimVersions` keeps the existing nil readKeys call;
this change does not invent late read validation after the primary decision,
which could reject a secondary that must finish. The lower API must preserve
ALL readKeys actually supplied at originalStartTS. Stronger transaction-wide
read serializability requires a separately specified validation/locking gate,
not a silent callback or store-OCC exception in this patch.

### Explicit Seed File APIs

`ProvisionTSOClaimIdentity(path, seed)` validates the caller-supplied 32-byte
seed through NewTSOClaimIdentity and publishes it once. `LoadTSOClaimIdentity`
only loads; missing/corrupt input never generates, replaces or repairs a key.
The fixed 72-byte format is EKVCKY01, 32 seed bytes and a standard SHA256
corruption checksum. The checksum is NOT authentication or registration proof.

The implementation uses standard anchored openat/no-follow I/O, exclusive temp
creation, file fsync, atomic no-replace rename and directory fsync. Linux uses
RENAME_NOREPLACE; Darwin uses RENAME_EXCL. Unsupported platforms fail closed.
Paths must be absolute/canonical; all ancestor and terminal symlinks are refused.
The final parent directory must already be current-UID-owned 0700; the regular
seed file must be current-UID-owned exactly 0600 with one hard link. No parent
directory is auto-created and concurrent provisioners cannot replace the winner.

This is a bounded local Unix mode/UID custody boundary. Ownership/permission
checks cover the FINAL parent directory and file, not every ancestor's access
policy or ACLs; it does not defend against privileged/same-UID writers or prove
cluster/node/incarnation identity. Native tests cover the Darwin filesystem;
Linux uses the corresponding syscall path but has not received runtime testing
in this worktree. No runtime/bootstrap wiring or auth callback was added.
Durable registration, cluster/node/incarnation binding, P-epoch/creation fences,
all-member decoder gates and the unknown-attempt journal remain unresolved.

Seed tests use real files and cover stable key/credential reconstruction,
missing-parent/no-generation behavior, unknown version, bad checksum, zero seed,
truncation/trailing bytes, modes, terminal/ancestor symlinks, hardlinks,
non-regular files and concurrent no-replace publication/temporary-file cleanup.
These tests do not establish a power-loss campaign or secure node provisioning.
