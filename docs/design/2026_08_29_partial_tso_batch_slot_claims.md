# TSO Batch Slot Claims

Status: Partial (superseded issuance decision; see section 4b)
Author: bootjp
Date: 2026-08-29

## 1. Problem

Under Phase D the group-0 leader is the only issuer of persistence timestamps,
and `RaftTSOAllocator.ValidateDurableTimestamp` (`kv/tso_raft.go:428`) is what
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
`commitAllocationFloor` (`kv/tso_raft.go:389`) *before* any offset in
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
- Removing batching. Per-timestamp consensus is explicitly what batching exists
  to avoid (`CLAUDE.md`: no Raft round trip per `Next()`).
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
a reserved range. Sketch: the allocator commits a claim (or a claim watermark
per owner) alongside issuing, and `ValidateDurableTimestamp` checks membership
rather than an interval.

- Structurally closes the hole, including the post-commit gap in 3.1.
- Needs a wire/on-disk decision: what a claim record is, who owns it, how it is
  compacted, and what happens to claims when leadership moves. That is why this
  is a design doc rather than a patch.

### 3.3 Authenticate the internal listener

Orthogonal and worth doing regardless, but it narrows *who* can exploit the gap
rather than closing it. Two elastickv nodes that legitimately reach each other
still can.

## 4. Recommendation

Historical recommendation, superseded by section 4b. Per-slot group-0 claims
close the range-validation hole, but put operation admission and retention into
the timestamp service and require a group-0 consensus round for every issued
slot. Section 4b separates issuance from durable write admission.

## 4a. Decision (2026-09-15)

**Phase D stays batched.** §3.1 is rejected as an interim: `CLAUDE.md` states
that no Raft round trip per `Next()` is a design invariant — it is what batching
exists for — and §3.1 would violate it on exactly the Phase-D path. Trading a
throughput regression for a window that narrows but does not close is not worth
that.

This was the 2026-09-15 decision. Section 4b supersedes the proposed group-0
claim location and changes the guarantee at which a concrete slot becomes
durably associated with an operation.

Landed now, independent of that decision:

- **§6's invariant tests that do not depend on the option chosen.** Uniqueness
  across concurrent allocators (direct and batched) and strict monotonicity:
  these must hold under §3.1, §3.2 or neither, so they are the fixed point any
  of those changes has to preserve.

Deliberately **not** added: a test asserting that an unclaimed slot inside a
committed window is refused. It is not refused today — that is the hole — and
asserting current behaviour there would lock in the bug.

### Lifecycle

This document is `_partial_`: the decision-independent half of §6 has shipped.
The revised contract and its rollout gates are in section 4b. The old group-0
per-slot claim is not an implementation requirement.

## 4b. Decision (2026-10-30): split issuance from admission

The timestamp service owns unique, ordered, failover-fenced timestamp issuance.
It does not own the operation manifest or persist an operation-to-slot claim.
The prior pre-return `ClaimBatch` decision is withdrawn: `Next()` returning a
value no longer means that exact slot has a durable group-0 claim. Unused
logical slots are allowed to become permanent gaps. A write becomes durably
bound to its timestamp when its owning data group applies the corresponding
Raft entry, not when `Next()` returns.

Group 0 should durably reserve a physical-millisecond band before exposing any
of its logical offsets. The persisted upper bound has all 16 logical bits set;
the serving leader allocates distinct offsets within that band in memory. A
new leader skips every offset of a previously committed band. Its first
allocation must also exceed the authoritative data-group commit floor. A band
cannot be reused after a term change, ceiling renewal, or ambiguous proposal.
The existing reservation response assumes `PreviousAllocationFloor < Base`;
serving multiple requests from one committed band requires an explicit wire
contract for the committed band and the prior *issued* cursor. Relabeling the
committed band end as a previous floor would be incorrect. An old peer must not
silently accept the new response semantics.

The receiving data-group leader must obtain an issuance from a trusted TSO
path, or verify a committed primary/home-group admission before accepting a
forwarded timestamp. A numeric timestamp merely inside the committed band is
not proof of issuance. Peer authentication is necessary but does not by itself
prove that a particular offset was handed out. Admission must reject external
pre-stamped creations that lack the new provenance; it must not fall back to
`ValidateDurableTimestamp` range membership.

Every raw write, one-phase commit, and PREPARE must atomically persist the
operation ID, persistence timestamp, canonical manifest digest, and creation
phase with its data-group Raft entry. Two indexes enforce both operation ID to
timestamp and timestamp to operation; exact retries are idempotent, and any
different binding is rejected before applying data. COMMIT/ABORT use the
durable PREPARE evidence in the same data group. For a multi-group transaction,
the primary/home group must persist the common binding first; secondaries must
verify that durable decision and the same full participant manifest before
PREPARE. A local secondary binding alone cannot prevent the same timestamp
from being reused for an unrelated operation in another group.

Read-only observations need unique, fenced timestamps but no data-group
operation binding. An unknown write outcome must be reconciled by the same
operation ID against the authoritative data group before a new timestamp can
be chosen. Activation waits for all current and future data-group members to
understand these records, their snapshot/restore format, the peer provenance
checks, and the Phase-D creation fence. Until then, the legacy protocol remains
unchanged and this document remains Partial.

Implementation order:

1. Data-group atomic binding store and replay/conflict tests. The current
   opt-in raw, one-phase and PREPARE FSM apply paths stage the binding and
   creation in one indexed Raft entry, recompute the manifest digest on every
   replica, and skip an exact replay. A local lookup checks both indexes for
   unknown-outcome reconciliation. This branch adds an opt-in applied-leader
   read barrier and a remote lookup RPC. The lookup rejects a changed leader
   term or expired peer session before returning, but no production
   reconciliation route is wired. Production admission rejects the new
   operation-ID field until authenticated issuance and activation are wired.
   Before replicated activation, FSM apply ignores that field for single and
   batched entries so mixed-version replicas retain legacy apply semantics;
   ingress still rejects newly bound requests. A focused integration test now
   carries native group-0 issuance through home preparation, atomic binding
   and data apply, and exact-ID retry. An in-process pending-ID gate now
   prevents concurrent or ambiguous retries from reissuing before the applied
   binding is observed. The home lookup verifies a stable leader term before
   and after its applied ReadIndex and lookup. Prepared requests now carry
   that term, and the native Raft event loop rejects a stale term before
   submitting the proposal. After a new-term applied barrier finds no binding,
   the process-local pending gate can reissue; a delayed old-term request is
   fenced even across process restart. This is not a production admission
   capability: Forward still rejects bound requests, and all-member activation
   and authenticated issuance remain required. A successful atomic data-group
   apply now clears the matching process-local pending attempt; failed apply or
   a different timestamp retains it. Unknown outcomes still require durable
   reconciliation before production activation. The applied read barrier runs
   outside the attempt mutex so Raft apply can finish and release a binding
   while a retry waits for ReadIndex; the mutex still serializes issuance after
   the barrier.
   The integration test
   uses a test issuer and method descriptor, not the production allocator and
   registered method. It is not the production Forward path. Memory and Pebble
   snapshot/restore tests now verify both binding indexes survive restore and
   still reject conflicting operation IDs; a retention policy is not defined.
2. Authenticated issuance and primary/home admission path, including unknown
   outcome reconciliation and cross-group PREPARE/COMMIT/ABORT evidence. This
   branch adds a versioned, physical-band-backed group-0 issuance RPC and an
   opt-in single-home raw binding preparation helper. Protected-bootstrap
   runtimes register issuance and home lookup as typed private peer-session
   methods; TLS plus a bearer token alone is rejected. The authenticated
   client checks the versioned response and has no legacy fallback. The
   handler option and client route are not installed in production: no
   replicated all-member activation gate exists yet. The public RawPut and
   RawDelete messages now carry an optional caller-stable operation ID through
   the operation group and raw request builders, but production validation
   rejects ID-bearing writes before timestamp issuance until durable admission
   is installed. The helper therefore cannot authorize production writes or
   serve as a substitute for primary/secondary evidence.
3. Physical-band group-0 reservation and versioned reservation response;
   prove no duplicate issuance across concurrent allocators, term changes,
   ambiguous proposals, and ceiling renewal. An opt-in local allocator now
   commits the band end and tests those transitions, including advancing one
   physical millisecond when the minimum leaves too few logical slots. It
   now has a versioned, default-deny wire route with explicit committed-band
   end and previous-issued cursor fields. An old peer response is rejected
   instead of being interpreted as the new contract. The route still rejects
   active Phase D and cannot authorize writes; production peer identity,
   activation, and data-group admission remain open.
4. Whole-cluster activation gate, migration and rolling-upgrade tests, native
   fault campaigns, Jepsen, and throughput/latency comparison with the old
   per-window allocator. No production activation before all gates pass.

### 4c. Two-group PREPARE admission record (design contract, not activated)

The first cross-group implementation is limited to exactly two data groups and
an explicit 16-byte nonzero operation ID. It does not change legacy PREPARE,
public Forward, or Phase D. A version-1 two-group home-admission record is distinct from
the existing per-group `TimestampBinding`: the latter hashes one
group's Raft request and cannot represent a full participant manifest.

- Canonical manifest v1 contains the operation ID, persistence timestamp,
  primary group ID, and exactly two participants with an identical transaction
  start timestamp in their PREPARE requests,
  sorted by distinct group ID. Each participant contains its complete PREPARE
  projection: ordered mutations (including transaction metadata), read keys,
  route version, and bypass keys. Transient admission term, peer receipt, and
  attestation fields are excluded. Reject unknown fields, duplicate groups,
  noncanonical encoding, or a local PREPARE whose projection differs. Hash the
  versioned canonical bytes with a domain-separated SHA-256; persist both the
  full digest and each participant's domain-separated projection digest.
- The primary data group's indexed Raft entry atomically applies its PREPARE
  and stages two immutable indexes: operation ID to (timestamp, full digest,
  both projection digests, primary group, phase) and timestamp to the same
  identity. An exact replay is idempotent. Any changed timestamp, participant,
  projection, phase, or primary group is a conflict. The primary's applied
  record must precede the secondary's PREPARE; a sent or acknowledged proposal
  without an applied record is not evidence.
- The secondary leader obtains the primary record only through a private,
  authenticated peer session to the current home leader. The home handler
  performs an applied ReadIndex and checks its leader term and session before
  and after lookup. The response identifies the home group, operation ID,
  timestamp, full and projection digests, phase, applied index, leader term,
  and record version. The secondary compares every field to
  its locally constructed canonical manifest before proposing. Missing,
  stale-term, ambiguous, malformed, or mismatched evidence fails closed.
- The secondary leader constructs an admission observation from that verified
  response; public Forward cannot supply or amend it. The secondary Raft entry
  carries the observation and canonical manifest. Every secondary replica
  checks its version, home identity, operation/timestamp, full and local
  projection digests, and the exact local PREPARE at apply. It atomically
  stages both secondary binding indexes, the observation, and PREPARE. An
  absent or incompatible observation rejects the entire entry. Replay after
  rotation or leader transfer uses the persisted observation, not the old
  peer session or a fresh RPC. New lookups require a current authenticated
  session and current home leader; an old TLS key never regains live access.
  This is durable leader-observed evidence under the trusted-member,
  crash-fault Raft model, not an independently verifiable Byzantine proof or
  a cryptographic quorum certificate.
- COMMIT/ABORT may only consume the matching durable PREPARE identity and
  record an immutable terminal decision. A secondary COMMIT additionally
  verifies the matching durable primary COMMIT decision; an unknown primary
  outcome retains the secondary PREPARE for resolution. A terminal record
  never releases its timestamp for another operation. No binding or
  attestation is garbage collected until an explicit retry/outcome-resolution
  horizon, snapshot retention rule, and all-participant terminal proof are
  specified and tested;
  in the interim retain them indefinitely across compaction and restore.

Failure cases to test before activation: secondary-before-home; wrong ID,
timestamp, primary group, participant set, or projection; forged/expired peer
credential or caller observation; home leader transfer between ReadIndex and reply;
secondary transfer between lookup and proposal; lost home PREPARE reply with
same-ID retry; crash after home apply but before secondary PREPARE; and restore
of either group before COMMIT/ABORT. A failed apply must leave neither local
binding nor data. Admission must be built from the authenticated applied
lookup by the trusted secondary leader; a caller-provided digest cannot replace it.
Mixed-version activation remains disabled until every voter and learner can
decode, validate, snapshot, and restore the existing binding and the new
home/secondary admission records.

Current implementation status: an opt-in, versioned Raft entry carries the
canonical manifest and the secondary leader's lookup observation. An enabled
FSM checks its local group, protected entry identity, full manifest, local
projection and observation, and atomically applies the exact embedded PREPARE
with the binding and record. Its reserved command prefix cannot alias a bare
legacy protobuf request. It rejects legacy PREPARE and COMMIT/ABORT at FSM
apply. The opt-in coordinator
derives the operation ID internally, proposes home first, reconciles an unknown
home reply through the authenticated applied lookup, and term-fences the
secondary proposal through a trusted PREPARE-only call. The manifest validator
requires that ID to equal the versioned derivation from
the primary key and transaction start timestamp, which is also the lock
resolver's durable-record lookup key. The path has a private
peer-session route for a remote group leader: the RPC carries
only the canonical manifest, and the destination leader constructs its own
applied home observation before term-fenced proposal. The peer method is not
publicly admitted, and the handler remains default-off in production. The
trusted PREPARE entry is package-private rather than a capability exposed by
startup coordinator wrappers. A separate versioned terminal entry now records
an immutable local COMMIT/ABORT decision with the binding, and a secondary
COMMIT requires an applied primary decision observation. The local-leader
coordinator reconciles a lost primary reply, and the lock resolver retains a
prepared lock when the required decision or local leadership is unavailable.
Remote-leader terminal proposals and their private peer route remain missing;
the local terminal path is not production-ready. Normal Dispatch is fenced
while this opt-in is configured, because legacy terminal COMMIT/ABORT cannot
consume the new identity. Public Forward cannot supply an observation. The
production all-member activation gate, remote terminal path, retention
horizon, and mixed-version rollout tests remain open. Production must not
install the opt-in coordinator lookup or enable the FSM command until those
gates are satisfied.

## 5. Open questions

1. ~~Is a per-timestamp group-0 round trip acceptable on the Phase-D path as an
   interim, or should Phase D stay batched until 3.2 ships?~~ **Resolved in §4a
   (2026-09-15): Phase D stays batched; §3.1 is rejected.**
2. ~~Should a group-0 claim be per-timestamp or a per-owner watermark?~~
   Superseded by section 4b. Group 0 does not retain operation claims.
3. What is the data-group binding retention and snapshot policy after a
   committed operation is no longer needed for idempotent retry or resolution?
   No expiry is safe until this is specified and verified.
4. How does the primary/home group attest a committed binding to secondary
   groups through authenticated peer transport, including primary failover and
   an unknown proposal outcome?
5. ~~How are physical-band reservation response fields versioned so old peers
   cannot mistake the committed band end for a prior-issued cursor?~~ Resolved
   by the opt-in versioned response and explicit fields in step 3. Production
   activation remains gated on the other requirements above.

## 6. Test plan

- A validation test that an unissued slot inside a committed window is refused.
- A property test that no two `Next()` results share a timestamp across
  concurrent allocators.
- A leadership-change test: a window committed by the old leader must not
  validate slots the new leader has not issued.
- Whichever option lands, a benchmark on the Phase-D issuance path.
- A data-group Raft-entry atomicity test: a failed data apply leaves neither
  binding nor write, and exact retry retains the same association.
- A two-index conflict test: one operation cannot acquire a second timestamp
  and one timestamp cannot be bound to a different operation or manifest.
- A cross-group test: a secondary refuses a timestamp without the same durable
  primary/home binding and full participant manifest.
- An authenticated Forward test: a numeric value inside a group-0 physical band
  but never issued is refused before proposing any data-group write.
- A leader-transfer/crash test: unissued offsets from the old physical band
  remain gaps, and the new term never reuses them.
