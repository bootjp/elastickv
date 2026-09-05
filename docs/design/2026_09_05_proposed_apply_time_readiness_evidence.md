# Apply-time target readiness must not read the catalog watcher

Status: Proposed
Author: bootjp
Date: 2026-09-05

Parent: [2026_06_11_implemented_hotspot_split_milestone2_migration.md](2026_06_11_implemented_hotspot_split_milestone2_migration.md)

## 1. The problem

`kvFSM.verifyTargetReadinessForRange` runs during Raft apply and resolves the
routes it checks through `currentShardRoutesForRouteRange`, which calls
`f.routes.Current()` — the route engine's *current* cached view.

That view is filled by the catalog watcher from the default group's catalog, over
a stream with **no ordering relationship to the target group's Raft log**. Two
voters applying the same entry at the same index can therefore hold different
catalog versions: the leader may already see the cutover descriptor while a
follower is still behind.

This is the failure mode `CLAUDE.md` records for the `8668bdce` revert — apply
must not depend on state that is not replicated through the log it is applying.

## 2. Why it is not merely a stale read

`targetReadinessStatesSatisfied` returns `ErrRouteCutoverPending` when the routes
it can see do not yet satisfy the replicated readiness guard. Before this change
that was an ordinary FSM response, so the Raft engine advanced the applied index
and the lagging voter **skipped a committed write** while the leader applied it.
The divergence was silent and permanent: nothing re-delivers the entry.

## 3. What has already been done

`applyRequest` now converts `ErrRouteCutoverPending` into a halt
(`ErrTargetReadinessApply`). A voter that cannot prove readiness stops instead of
advancing past the entry, so the divergence becomes a loud, recoverable stop
rather than silent data loss.

This is a containment fix, not a correctness fix. It converts "wrong answer" into
"no answer", which is the right trade for an apply path, but a follower that is
merely behind on catalog delivery will now halt where it previously (wrongly)
continued. That is strictly safer and strictly noisier.

## 4. Options for making apply deterministic

### 4.1 Derive readiness from the entry (preferred)

Requests already carry `observed_route_version`, and the base branch established
the pattern: `routeFloorSnapshotForRequest` resolves
`f.routes.SnapshotAt(observedVer)` and fails with `ErrComposed1VersionGCd` when
that version is no longer retained. Readiness could resolve the same way, making
the verdict a pure function of (entry, replicated readiness guard, retained
catalog history).

Open questions:

- Unpinned entries have no observed version. Do they skip the readiness check
  (matching how the write-floor check treats them), or is pinning mandatory once
  a migration is armed?
- `SnapshotAt` needs the version to still be retained. What is the retention
  bound relative to the apply lag a halted follower can accumulate?

### 4.2 Replicate the routes the guard needs

The readiness guard itself is already replicated through the target group
(`raftEncodeTargetReadiness`). The route bounds it is checked against could be
carried in that same entry, so apply never consults the catalog cache at all.
Larger entries, but no dependency on catalog delivery order.

### 4.3 Gate on the guard alone

`targetReadinessStatesSatisfied` currently needs both the replicated guard *and*
a matching catalog view. If the guard carried enough to decide by itself, the
catalog read disappears. This is 4.2 with the bounds folded into the existing
state rather than the entry.

## 5. Recommendation

4.1, because the mechanism already exists on the write-floor path and reuses the
retention story rather than inventing one. 4.2 is the fallback if the retention
bound in §4.1 cannot be made to cover realistic follower lag.

## 6. Until then

The halt in §3 stands. It is not a substitute for this document: a cluster whose
catalog delivery lags will see followers halt, which is safe but disruptive, and
that pressure is the reason to land §4 rather than leave the halt as the answer.
