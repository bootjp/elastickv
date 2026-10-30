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

1. Data-group atomic binding store and replay/conflict tests.
2. Authenticated issuance and primary/home admission path, including unknown
   outcome reconciliation and cross-group PREPARE/COMMIT/ABORT evidence.
3. Physical-band group-0 reservation and versioned reservation response;
   prove no duplicate issuance across concurrent allocators, term changes,
   ambiguous proposals, and ceiling renewal.
4. Whole-cluster activation gate, migration and rolling-upgrade tests, native
   fault campaigns, Jepsen, and throughput/latency comparison with the old
   per-window allocator. No production activation before all gates pass.

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
5. How are physical-band reservation response fields versioned so old peers
   cannot mistake the committed band end for a prior-issued cursor?

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
