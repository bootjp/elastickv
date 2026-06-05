# Composed-1 — cross-group commit-time ownership guard

Status: Partial — M1–M4 shipped via PR #900; M5a shipped via PRs #911 / #916 / #924 / #925; M5b (route-shuffle nemesis) still open.  See companion doc `2026_06_02_partial_composed1_m5_jepsen_route_shuffle.md` for the detailed M5 design and per-milestone state.
Author: bootjp
Date: 2026-05-29 (renamed *_proposed_* → *_partial_* on 2026-06-04)

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
[concurrent: MoveRange moves k1 from g1 to g2, catalogVersion → v_obs+1]
T1: Commit on g1 — issues the OCC commit envelope to g1
   * g1 happily commits because OCC's local invariants are satisfied
   * routes[v_obs][k1] = g1 → an observed-version check ALSO passes
   * but routes[v_obs+1][k1] = g2 → the catalog now records g2 as owner
   * a concurrent T2 begins observing v_obs+1, reads k1 from g2 → never sees T1's write
```

This is exactly the **G1c anomaly** (lost-write across a group
boundary) that the Jepsen DynamoDB suite would catch *if* it
generated a cross-group split mid-workload — but the workload
cannot exercise something that the RPC surface does not expose
today.

**Important: this means an observed-version-only check is
insufficient.** Composed-1 as stated in `Composed.tla`
(`routes[txnObservedVer[t]][k] = txnCommitGroup[t]`) is satisfied
by the trace above — `routes[v_obs][k1] = g1` and `g1` is also the
committing group — yet the trace exhibits a G1c lost-write because
*future* readers route to `g2`. The implementation therefore needs
both the spec-level check AND an additional fence against late
commits to a no-longer-current owner. §4.4 below specifies that
second fence; codex P1 on the first revision of this doc surfaced
the gap and is the reason §4.4 exists.

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
verify both the observed-version owner AND the current-version
owner (see §4.4 for why both are required):

```go
// PREREQUISITE: kvFSM gains two fields that do not exist today —
//   routes        kv.RouteHistory  // versioned snapshot provider, §4.3
//   shardGroupID  uint64           // owning Raft group's ID
// Both are wired through NewKvFSMWithHLC / NewKvFSMWithOptions at
// shard-group construction. The struct extension is M1 / M3 scope
// (see §6); the snippet below is illustrative of the apply-time
// contract, not literal copy-paste.
func (f *kvFSM) verifyComposed1(r *pb.Request, observedVer uint64) error {
    if observedVer == 0 {
        return nil // unpinned — caller opted out (read-only, legacy)
    }

    // (a) Observed-version check — the spec-level Composed-1
    //     predicate. Confirms the commit is going to a group that
    //     was a legitimate owner at txn read-set capture time.
    observedSnap := f.routes.SnapshotAt(observedVer)
    if observedSnap == nil {
        // Catalog GC dropped this version.  Treated as a hard,
        // retryable error: we cannot prove the spec-level
        // predicate without the snapshot.  Coordinator retries
        // against the new catalog (gemini medium / codex P2
        // on the first revision of this doc — Composed-1 must
        // not silently degrade under aggressive catalog GC).
        return errors.WithStack(ErrComposed1VersionGCd)
    }
    for _, mut := range r.Mutations {
        if isTxnInternalKey(mut.Key) {
            continue
        }
        owner := observedSnap.OwnerOf(mut.Key)
        if owner != f.shardGroupID {
            return errors.Wrapf(ErrComposed1Violation,
                "key %q owned by group %d at version %d but committing on group %d",
                mut.Key, owner, observedVer, f.shardGroupID)
        }
    }

    // (b) Current-version cross-version-read fence — see §4.4.
    //     Rejects late commits to a no-longer-current owner even
    //     when the observed-version check above would have passed
    //     (this is what the codex P1 trace in §3 exhibits).
    return f.verifyCurrentVersionOwner(r)
}
```

`ErrComposed1Violation` and `ErrComposed1VersionGCd` are new
sentinels that the coordinator maps to a client-visible "route
changed, retry" / "catalog version evicted, retry" error. Both
retry paths re-read the catalog and re-issue the txn against the
new owning group.

### 4.3 Catalog version retention

The above requires the catalog to retain enough history that
`SnapshotAt(observedVer)` returns the route mapping at the txn's
observed version, for any version still in flight. Today
`distribution/engine.go` keeps a single in-memory snapshot per
leader; we would extend it to keep a small ring (size bounded by
the longest in-flight transaction × catalog churn rate, with a
conservative default of 32).

Versions older than the ring **return `ErrComposed1VersionGCd`**,
not a soft pass. The coordinator's existing retry path
(§M4) re-reads the catalog and re-routes the txn — the same
client-visible behaviour as a regular `ErrComposed1Violation`.
Failing closed here matters because a soft-pass would let the
guard be bypassed exactly in the cases (long-running transactions,
high catalog churn) where the cross-version-read hazard is most
likely; the spec safety property would then depend on a
best-effort retention depth rather than the invariant
(gemini medium / codex P2 on the first revision of this doc).

The ring depth becomes a *liveness* parameter — too small and
long-running txns get spurious retries — rather than a *safety*
parameter. The default 32 stays.

### 4.4 Current-version cross-version-read fence

The observed-version check in §4.2(a) is the literal refinement of
the spec's `Commit` precondition. It is not, however, sufficient
to prevent the §3 `MoveRange` G1c trace: in that trace the
observed-version check passes (`g1` owned `k1` at `v_obs`), yet
the write lands on `g1` while readers at `v_obs+1` route to `g2`.

This fence rejects that case. At apply time, the FSM also checks
the **current** owner:

```go
func (f *kvFSM) verifyCurrentVersionOwner(r *pb.Request) error {
    currentSnap := f.routes.Current() // latest applied catalog version
    for _, mut := range r.Mutations {
        if isTxnInternalKey(mut.Key) {
            continue
        }
        if currentSnap.OwnerOf(mut.Key) != f.shardGroupID {
            return errors.Wrapf(ErrComposed1Violation,
                "key %q has moved off this group (current owner %d) since txn observed; retry",
                mut.Key, currentSnap.OwnerOf(mut.Key))
        }
    }
    return nil
}
```

Why an apply-time check (not pre-Raft): the catalog can move
between the pre-Raft routing decision and the FSM apply step. The
FSM is the single serialisation point that observes the
authoritative catalog state via the route-engine watcher, so it
is the only place that can guarantee the routes have not shifted
between routing and persistence.

**Alternative considered.** A `MoveRange` *drain protocol*
(`MoveRange` waits for all in-flight commits on the old owner
before activating the new owner) would also close the gap, with
the advantage that no apply-time current-version check is
required. We prefer the apply-time fence because (i) it is local
to the FSM and does not need a cross-group quiescence step, (ii)
the retry cost is bounded by the txn-retry budget that already
exists, and (iii) a drain protocol introduces a head-of-line
blocking surface for `MoveRange`. The drain approach is
documented here in case future operational experience suggests
the retry tail latency is worse than the drain pause.

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

§4.2(a) refines this predicate at apply time: the FSM checks the
same condition the spec checks at the abstract `Commit` step. The
gap counterexample is exactly the case where this predicate
fails, and the implementation refuses to apply.

§4.4's current-version fence is a **strictly stronger** apply-time
contract than the spec's `Commit` precondition. The spec's
Composed-1 invariant
(`routes[txnObservedVer[t]][k] = txnCommitGroup[t]`) is preserved
by §4.2(a) alone — but as noted in §3, it does not by itself
prevent the cross-version-read G1c trace, because the spec does
not model inter-version read consistency at the same level of
detail.  §4.4 fences that residual hazard in the implementation.

A natural follow-up to this proposal would be to extend
`Composed.tla` with a `currentVersion` notion and a Composed-1a
invariant
(`\A t \in Committed : routes[currentVersionAtCommit[t]][k] = txnCommitGroup[t]`),
so the apply-time fence is also spec-checked by TLC. That
extension is out of scope for this proposal because it changes
the M5 (`tla/composed`) module's state shape; tracked as an open
follow-up in `docs/design/2026_05_28_partial_tla_safety_spec.md`'s
M6+ slot.

---

## 6. Milestones

| # | Title | Scope | Done when |
|---|---|---|---|
| M1 | `ObservedRouteVersion` plumbing | Add the field to `OperationGroup`, propagate through `dispatchTxn` and into `onePhaseTxnRequest`, encode in `pb.Request`. No FSM check yet. | Field round-trips end-to-end; no behaviour change. |
| M2 | Catalog version ring + `kvFSM` extension | Extend `distribution/Engine` to retain the last `routeHistoryDepth` (default 32) versioned snapshots, and add `routes kv.RouteHistory` + `shardGroupID uint64` fields to `kvFSM` (today's struct has neither — see §4.2 prerequisite block). Wire both through `NewKvFSMWithHLC` / a new `NewKvFSMWithOptions`. | `SnapshotAt(v)` returns the mapping for in-flight versions; older returns nil; FSM struct compiles with the new fields wired from shard-group construction. |
| M3 | FSM apply-time check | Wire `verifyComposed1` (observed-version §4.2(a) + current-version §4.4) into `handleTxnRequest`; emit `ErrComposed1Violation` (route shift) and `ErrComposed1VersionGCd` (retention miss) and surface both to the coordinator's retry path. | Three unit tests: (i) stale `ObservedRouteVersion` with the key moved → `ErrComposed1Violation`; (ii) `ObservedRouteVersion` older than ring → `ErrComposed1VersionGCd`; (iii) observed-version check passes but current-version differs → `ErrComposed1Violation` (the §3 codex P1 trace). |
| M4 | Coordinator retry | When either Composed-1 sentinel returns, the coordinator re-reads the route cache, re-routes the txn, and retries once. | Integration test: a fake `MoveRange` issued mid-txn causes the client-observable result to be a successful commit on the new owner, not a lost write. |
| M5 | Jepsen workload | Add a route-shuffle generator to the DynamoDB suite that issues `MoveRange` concurrently with txns. | Workload finds no G1c anomaly. |

M1 can land independently. M2 is a single PR because the
`kvFSM` struct extension and the engine ring are tightly coupled
through the constructor wiring. M3 unlocks the runtime check,
which is the actual safety property. M4 makes the check
non-disruptive to clients. M5 is the integration-level proof.

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

1. **Data loss.** The guards (§4.2(a) observed-version, §4.4
   current-version, §4.3 retention-miss) are all conservative:
   they add checks that can fail commits but cannot lose them.
   Every rejection emits a sentinel the coordinator's retry path
   (§M4) converts into a successful commit on the correct owner.
   Failing closed on the §4.3 retention-miss case (rather than
   the original soft pass) closes the silent-degradation hazard
   gemini medium + codex P2 flagged on the first revision of
   this doc.
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
  scale; revisit when the cross-group RPC lands. Note: this is a
  *liveness* dial only — too small means more retries, not a safety
  regression, because §4.3 now fails closed on a ring miss.
- **Q3.** Apply-time current-version fence (§4.4) vs. `MoveRange`
  drain protocol. The current proposal picks the apply-time fence;
  the drain protocol is documented as an alternative. Revisit if
  operational data shows the retry tail latency from the fence is
  worse than the drain pause. Resolution can wait until the first
  cross-group RPC is on a real roadmap.
