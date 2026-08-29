# TSO Batch Slot Claims

Status: Proposed
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

**Exposure.** `adminTokenProtectedMethod` (`adapter/admin_grpc.go:515`) matches
only the `/Admin/` prefix, so `Internal.Forward` is not behind the admin token.
This is peer-port reach, not public-client reach, but it is unauthenticated at
the gRPC layer, and the window is open for as long as the batch survives —
unbounded under low write traffic.

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

3.2 is the fix; 3.1 is a mitigation whose cost is a throughput regression on the
Phase-D path and therefore an operator-visible tradeoff, not an implementation
detail. **This document exists to get that tradeoff decided before either lands.**
3.3 should be tracked separately.

## 5. Open questions

1. Is a per-timestamp group-0 round trip acceptable on the Phase-D path as an
   interim, or should Phase D stay batched until 3.2 ships?
2. Should a claim be per-timestamp or a per-owner watermark? A watermark is far
   cheaper and still refuses any slot ahead of what an owner actually issued.
3. What is the retention story for claims across leadership change and snapshot?

## 6. Test plan

- A validation test that an unissued slot inside a committed window is refused.
- A property test that no two `Next()` results share a timestamp across
  concurrent allocators.
- A leadership-change test: a window committed by the old leader must not
  validate slots the new leader has not issued.
- Whichever option lands, a benchmark on the Phase-D issuance path.
