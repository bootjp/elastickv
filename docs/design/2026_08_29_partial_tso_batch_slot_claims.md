# TSO Batch Slot Claims

Status: Partial
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

**Exposure (as of 2026-08-29; closed by §4a).** `adminTokenProtectedMethod`
(`adapter/admin_grpc.go`) matched only the `/Admin/` prefix, so `Internal.Forward`
was not behind the admin token. That was peer-port reach, not public-client
reach, but it was unauthenticated at the gRPC layer, and the window stayed open
for as long as the batch survived — unbounded under low write traffic.

§4a closed this route: `Internal.Forward` is now inside the gate and the write
forward carries the token. The underlying hole below is untouched — a caller who
does hold the token can still persist at an unclaimed slot — which is why §3.2
is still the fix.

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

## 4a. Decision (2026-09-15)

**Phase D stays batched.** §3.1 is rejected as an interim: `CLAUDE.md` states
that no Raft round trip per `Next()` is a design invariant — it is what batching
exists for — and §3.1 would violate it on exactly the Phase-D path. Trading a
throughput regression for a window that narrows but does not close is not worth
that.

§3.2 remains the fix and needs its own proposal, because the claim record is a
wire/on-disk decision (§5 questions 2 and 3).

Landed now, both decision-independent:

- **§3.3 for `Internal.Forward`.** It was the only forward outside
  `adminTokenProtectedMethod` while `ForwardAdminProposal` and
  `ForwardLeaseRead` were inside it, which made it the cheapest route to the
  validation path for anything with peer-port reach. Both halves shipped: the
  server gate, and the token on the outbound write forward — protecting the
  method without the credential would have broken forwarding rather than
  authenticated it. An empty token still disables both ends, so an unconfigured
  cluster is unaffected.

  **Rolling upgrades.** Both halves ship in one binary, but a cluster does not
  upgrade in one step. In a cluster that already has an admin token configured,
  a node still running the older binary does not attach the header, so once an
  upgraded node becomes group leader, writes entering an older follower and
  forwarded to it are refused with `Unauthenticated` until that follower is
  upgraded. **Upgrade every node before relying on the gate.** This is the same
  window `ForwardLeaseRead` already shipped with -- it is gated server-side and
  attaches the token, with no staged flag -- so this follows the established
  rollout rather than inventing a second mechanism for the sibling RPC. A
  cluster with no admin token configured is unaffected on both ends.

  **The token is not confidential.** Every peer dial goes through
  `internal.GRPCDialOptions`, which is `insecure.NewCredentials()` only; the
  server has no peer-TLS option at all. So the bearer token crosses the node
  network in cleartext -- as does the Raft traffic beside it, which an observer
  on that network can already read and inject. The gate therefore raises the
  bar against *reaching* the peer port, not against *observing* it, which is
  the right reading of what §1's exposure was: unauthenticated reach to the
  durable-timestamp validation path. Withholding the token on an insecure
  connection would disable the gate in every existing deployment rather than
  harden it, so confidentiality is left to peer mTLS, tracked separately.

  `RelayPublish`, `ExportRangeVersions`, `ImportRangeVersions` and
  `PromoteStagedVersions` remain outside the gate. Each needs the same
  two-sided treatment, and their clients are built through the migration
  factory rather than in this package, so they are tracked separately rather
  than half-wired here.

- **§6's invariant tests that do not depend on the option chosen.** Uniqueness
  across concurrent allocators (direct and batched) and strict monotonicity:
  these must hold under §3.1, §3.2 or neither, so they are the fixed point any
  of those changes has to preserve.

Deliberately **not** added: a test asserting that an unclaimed slot inside a
committed window is refused. It is not refused today — that is the hole — and
asserting current behaviour there would lock in the bug.

### Lifecycle

This document is `_partial_`: §3.3 and the decision-independent half of §6 have
shipped, while §3.2 — the durable per-slot claim that actually closes the hole —
has not. It becomes `_implemented_` only when a claim record lands and the
unclaimed-slot test in §6 can be written as a passing assertion.

## 5. Open questions

1. ~~Is a per-timestamp group-0 round trip acceptable on the Phase-D path as an
   interim, or should Phase D stay batched until 3.2 ships?~~ **Resolved in §4a
   (2026-09-15): Phase D stays batched; §3.1 is rejected.**
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
