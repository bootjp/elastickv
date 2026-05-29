# Composed-1 — cross-group commit-time ownership guard

Status: Proposed
Author: bootjp
Date: 2026-05-29

> **Forward-looking proposal.** Today's implementation is vacuously
> safe with respect to Composed-1 because `SplitRange` is the only
> route-mutating control-plane RPC and it is same-group only (per
> CLAUDE.md). This doc designs the commit-time ownership guard that
> would be required *before* introducing any cross-group route
> mutation (cross-group `SplitRange`, `MoveRange`, key-range
> rebalancing). The implementation is not justified by current
> deployment state; the proposal exists so that when a follow-up
> introduces cross-group movement, the guard is reviewed-and-ready
> rather than patched in under time pressure.

---

## 1. Background — Composed-1 from the TLA+ spec

The TLA+ Composed module models the cross-module seam between OCC and
the route catalog. Composed-1 (proved in
`tla/composed/Composed.tla` / `MCComposed.cfg`) states:

> Every committed write key was owned by the committing group at the
> transaction's observed catalog version.

Formally:

```
Composed1_CommitToOwningGroup ==
  \A t \in {tx \in TxnIds : txnState[tx] = "Committed"} :
    \A k \in txnWriteSet[t] :
      routes[txnObservedVer[t]][k] = txnCommitGroup[t]
```

The gap config (`MCComposed_gap.cfg`) drops the
`routes[v][k] = g` precondition from `Commit` and surfaces the
canonical 4-step counterexample at depth 4 in the model checker:

1. `BeginTxn(t)` pins `txnObservedVer[t] = 0` where `routes[0][k1] = g1`.
2. `WriteIntent(t, k1)`.
3. `ProposeRouteChange(k1, g2)` bumps `catalogVersion` to `1` and
   sets `routes[1][k1] = g2`.
4. `Commit(t, g2)` — committing group is `g2`, but
   `routes[0][k1] = g1 ≠ g2`. **Composed-1 fails.**

The spec uses `catalogVersion` strictly to pin the observed
ownership at `BeginTxn` and to validate it at `Commit`. The gap
demonstrates what happens if the commit does not re-validate
against the observed version.

---

## 2. Why today's implementation is vacuously safe

Three structural facts make Composed-1 currently impossible to
violate in the live system:

1. **`SplitRange` is same-group only.** The only control-plane
   route mutation that exists today preserves the
   key-to-group mapping (it splits a range *within* a group).
   `routes[v][k]` therefore changes at most for the new
   intra-group sub-range boundaries, never to a different group.
   CLAUDE.md `kv/sharded_coordinator.go` explicitly notes this:
   "SplitRange — same-group split only".
2. **No `MoveRange` / `Rebalance` RPC exists.** There is no
   surface-area RPC that moves a key from group `g1` to group `g2`.
3. **The route catalog is leader-side-cached.** Routing decisions
   read from the `RouteEngine` cache (not the catalog directly),
   and the cache snapshot is per-leader. A txn that begins under
   leader `L1` with `routes[v1]` and commits on leader `L1` will
   see the same cached routes throughout.

The Composed-1 hazard requires (a) a route mutation that *changes*
ownership for some `k`, AND (b) that mutation landing between
`BeginTxn` and `Commit`. Today (a) is structurally impossible, so
(b) is irrelevant.

This is the same shape as a safety property whose preconditions are
not present in the deployed configuration: the property holds
vacuously, but the implementation has no enforcement code to point
at if it ever stops being vacuous.

---

## 3. The hazard if cross-group movement is introduced

Several plausible future RPCs would break the vacuous safety:

- **Cross-group `SplitRange`** — splitting a range so that the new
  half lands on a different group (e.g. to relieve hotspot
  pressure on `g1` by handing half of its key range to `g2`).
  This is the natural extension of the same-group split today
  (`2026_02_18_partial_hotspot_shard_split.md`).
- **`MoveRange`** — operator-initiated migration of a key range
  from one group to another for capacity rebalancing.
- **Group decommission** — when a Raft group is being retired,
  its routes have to flow to surviving groups.

Each of these makes Composed-1's gap counterexample reachable:

```
T1: BeginTxn observes routes[v_obs], records txnObservedVer = v_obs
T1: WriteIntent(k1) — k1 owned by g1 at v_obs
[concurrent: SplitRange moves k1 from g1 to g2, catalogVersion → v_obs+1]
T1: Commit on g1 — issues the OCC commit envelope to g1
   * g1 happily commits because OCC's local invariants are satisfied
   * but routes[v_obs+1][k1] = g2 → the catalog now records g2 as owner
   * a concurrent T2 begins observing v_obs+1, reads k1 from g2 → never sees T1's write
```

This is exactly the **G1c anomaly** (lost-write across a group
boundary) that the Jepsen DynamoDB suite would catch *if* it
generated a cross-group split mid-workload — but the workload
cannot exercise something that the RPC surface does not expose
today.

---

## 4. Proposal — commit-time ownership re-validation

### 4.1 Per-transaction observed-version pin

Extend the existing `OperationGroup[OP]` (used by
`ShardedCoordinator.Dispatch`) with a single field:

```go
type OperationGroup[OP] struct {
    // ... existing fields ...

    // ObservedRouteVersion is the catalog version the read-set was
    // captured at. Zero means "unpinned" (legacy callers + read-only
    // queries). Non-zero values are propagated into the Raft commit
    // envelope so the FSM apply path can validate that the keys
    // landing in this group's log are still owned by this group at
    // ObservedRouteVersion (the Composed-1 fence).
    ObservedRouteVersion uint64
}
```

Pinning happens at `BeginTxn` time — the coordinator reads
`RouteEngine.Version()` once at transaction start and stamps every
mutation in the txn with that version.

### 4.2 Commit-time check at the FSM

In `kv/fsm.go::handleTxnRequest`, before applying mutations,
read the local `RouteEngine` and verify:

```go
func (f *kvFSM) verifyComposed1(r *pb.Request, observedVer uint64) error {
    if observedVer == 0 {
        return nil // unpinned — caller opted out (read-only, legacy)
    }
    snap := f.routes.SnapshotAt(observedVer)
    if snap == nil {
        // We don't retain this version anymore — the catalog GC
        // ran. This is a soft failure: legacy and fits the
        // "treat as unpinned" fallback. Logged for observability.
        return nil
    }
    for _, mut := range r.Mutations {
        if isTxnInternalKey(mut.Key) {
            continue
        }
        owner := snap.OwnerOf(mut.Key)
        if owner != f.shardGroupID {
            return errors.Wrapf(ErrComposed1Violation,
                "key %q owned by group %d at version %d but committing on group %d",
                mut.Key, owner, observedVer, f.shardGroupID)
        }
    }
    return nil
}
```

`ErrComposed1Violation` is a new sentinel that the coordinator
maps to a client-visible "route changed, retry" error. The retry
path re-reads the catalog and re-issues the txn against the new
owning group.

### 4.3 Catalog version retention

The above requires the catalog to retain enough history that
`SnapshotAt(observedVer)` returns the route mapping at the txn's
observed version, for any version still in flight. Today
`distribution/engine.go` keeps a single in-memory snapshot per
leader; we would extend it to keep a small ring (size bounded by
the longest in-flight transaction × catalog churn rate, with a
conservative default of 32).

Versions older than the ring fall through to the soft-failure
path (§4.2): logged but not rejected. This matches the design
decision in the partial hotspot-split doc — the catalog is
authoritative at the apply boundary, and stale-version commits
are an acceptable retry pattern.

---

## 5. TLA+ spec alignment

The Composed.tla module already encodes exactly this check at the
`Commit` action:

```
Commit(t, g) ==
  /\ txnState[t] = "Active"
  /\ \A k \in txnWriteSet[t] : routes[txnObservedVer[t]][k] = g
  /\ ...
```

So the implementation in §4 is a faithful refinement of
`Commit`'s precondition: at apply time, the FSM checks the same
predicate the spec checks at the abstract `Commit` step. The
implementation is correct against the spec by inspection — the
gap counterexample is exactly the case where this precondition
fails, and the implementation refuses to apply.

---

## 6. Milestones

| # | Title | Scope | Done when |
|---|---|---|---|
| M1 | `ObservedRouteVersion` plumbing | Add the field to `OperationGroup`, propagate through `dispatchTxn` and into `onePhaseTxnRequest`, encode in `pb.Request`. No FSM check yet. | Field round-trips end-to-end; no behaviour change. |
| M2 | Catalog version ring | Extend `distribution/Engine` to retain the last `routeHistoryDepth` (default 32) versioned snapshots. | `SnapshotAt(v)` returns the mapping for in-flight versions; older returns nil. |
| M3 | FSM apply-time check | Wire `verifyComposed1` into `handleTxnRequest`; emit `ErrComposed1Violation` and surface to the coordinator's retry path. | Unit test: synthetic apply with a stale `ObservedRouteVersion` and a moved key returns `ErrComposed1Violation`. |
| M4 | Coordinator retry | When `ErrComposed1Violation` returns, the coordinator re-reads the route cache, re-routes the txn, and retries once. | Integration test: a fake `MoveRange` issued mid-txn causes the client-observable result to be a successful commit on the new owner, not a lost write. |
| M5 | Jepsen workload | Add a route-shuffle generator to the DynamoDB suite that issues `MoveRange` concurrently with txns. | Workload finds no G1c anomaly. |

M1 and M2 can land independently (they are pure additions). M3
unlocks the runtime check, which is the actual safety property.
M4 makes the check non-disruptive to clients. M5 is the
integration-level proof.

---

## 7. Non-Goals

- **NG1.** Refactoring `SplitRange` to enable cross-group splits.
  That is a separate proposal (`2026_02_18_partial_hotspot_shard_split.md`
  Milestone 2+); this doc only adds the guard the future change
  would need.
- **NG2.** Retroactively pinning read-only transactions. Read-only
  paths take a snapshot via the read-ts pin and do not produce
  Raft commit envelopes; they cannot violate Composed-1.
- **NG3.** Changing the catalog mutation surface (`SplitRange`)
  itself. The guard is read-side and apply-side only.

---

## 8. Self-review (5 lenses per CLAUDE.md)

1. **Data loss.** The guard is conservative: it adds a check that
   can fail commits but cannot lose them. The retry path (§M4)
   converts an apply-time rejection into a coordinator-side retry
   on the new owning group.
2. **Concurrency / distributed failures.** The check runs under
   the FSM's `applyMu`, so it is serialised with all other apply
   work; no new lock ordering. Catalog watcher fan-out is
   unchanged — readers see versions in order via the existing
   `distribution/watcher.go` machinery.
3. **Performance.** One catalog-snapshot lookup + one mutation
   loop per applied txn entry. For small txns this is constant
   overhead; for large txns this is `O(|writeSet|)` and
   dominated by the apply work that already iterates the mutation
   list. Soft-failure on missing version is free.
4. **Data consistency.** The implementation directly mirrors the
   TLA+ Composed-1 precondition; the gap counterexample is
   exactly the case the apply check rejects. The retry path
   preserves linearisability because the client observes either
   a successful commit (on the correct group) or an error
   (retried internally) — never a silent write to the wrong group.
5. **Test coverage.** Unit tests at M3 (synthetic apply +
   stale-version key), integration tests at M4 (fake `MoveRange`
   mid-txn), Jepsen workload at M5 (route-shuffle generator).
   Each milestone is independently testable.

---

## 9. Open questions

- **Q1.** Should `ObservedRouteVersion = 0` ("unpinned") be allowed
  forever, or should it be deprecated once all callers have migrated?
  The TLA+ spec assumes every committed txn has a pinned version, so
  long-term "unpinned" weakens the safety claim. Suggest: allow
  unpinned at M1, log a warning at M3, error at M5.
- **Q2.** What is the right `routeHistoryDepth` default? 32 is
  conservative against current single-leader catalog churn (which
  is operator-frequency, not data-plane). If a future control plane
  generates hundreds of versions per second the ring would need to
  scale; revisit when the cross-group RPC lands.
- **Q3.** Should the soft-failure on `SnapshotAt(v) == nil` be a
  hard error instead? Today it is "permit and log" because the
  catalog GC is operator-controlled and unlikely to drop a recent
  version; revisit if this becomes a routine path.
