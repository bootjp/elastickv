# TTL-inline migration of legacy-route rows across leaders

Status: Proposed
Author: bootjp
Date: 2026-10-17

Related:
- [2026_04_17_implemented_ttl_inline_value.md](2026_04_17_implemented_ttl_inline_value.md): Phase 2 (background migration) and Phase 3 (removing the legacy `!redis|ttl|` read fallback).
- [2026_04_16_implemented_centralized_tso.md](2026_04_16_implemented_centralized_tso.md) §7.4: Phase D.
- [2026_09_26_proposed_serializable_isolation_audit.md](2026_09_26_proposed_serializable_isolation_audit.md): gap G10 and fix A0b.
- bootjp/elastickv#1278: the per-group migration this proposal builds on, and the review threads that deferred this case to a separate design.

## 1. Problem

After bootjp/elastickv#1278, each node migrates TTL anchors only in the data groups it leads. Each group's rows come from `ShardStore.ScanGroupOwnedAt`. That scan also serves hash, set, zset and stream metadata still stored at the route of its **raw** key:
- such a row sits in group L, while its user key belongs to group P;
- the scan serves it only when the node leading L also leads P, and P holds no version of the key at the read timestamp (`ownerHoldsNoVersionAt`);
- that is exactly when a point read falls back to the legacy route (`legacyPointRouteKey`, `routeMissStopsPointFallback`).

When different nodes lead L and P, nothing processes the row:
- L's leader cannot tell whether P holds the key. `routeHasVersionAtOrBefore` answers only on P's leader.
- P's leader plans scans only for the groups it leads, so it never reads L.
- Only P's leader may issue the rewrite's timestamp. This is the `CLAUDE.md` HLC rule, which the migrator keeps by dispatching only for keys it leads (`IsLeaderForKey`).

Reads stay correct meanwhile, through the dual-route point-read fallback and the legacy `!redis|ttl|` fallback. But those rows keep their legacy TTL anchors indefinitely, so **Phase 3 of the TTL-inline design (disabling the fallback) cannot proceed** on a cluster that has any of them.

How common this is depends on the catalog:
- Before user-key routing for these families, their metadata was routed by its raw bytes. `!hs|meta|…`, `!st|meta|…`, `!zs|meta|…` and `!stream|meta|…` sort into the `!` region of the keyspace, which usually belongs to one route.
- So a cluster's legacy rows typically sit in one group L, and their owners spread over every group.
- With leaders spread across nodes, most of those rows fall into this case.

Before a36dac7f (2026-07-22), every node's whole-prefix `ScanAt` reached these rows; a36dac7f traded that coverage away to stop whole-keyspace scans from every node.

## 2. Goals and non-goals

Goals:
- Every legacy-route row is migrated, or shown not to exist, so Phase 3 can be gated on evidence.
- Keep the HLC rule: no persistence timestamp from a node that does not lead the written key's group (or from anything but the TSO under Phase D).
- Keep the per-tick cost bounded and proportional to the legacy backlog, not to the keyspace.
- Do not reintroduce whole-prefix scans from every node.

Non-goals:
- Changing how point reads resolve legacy routes. The fallback stays until the rows are gone.
- Families without a legacy point route: strings, HLL, lists, the TTL index.
- Fixing G10 in general. This design depends on A0b or Phase D; it does not implement them.

## 3. Options

### A. Owner-side scans of foreign legacy groups

P's leader also scans the hash, set, zset and stream meta prefixes in every group it does not lead whose routes cover those raw prefixes. It serves the rows whose owner is P (a local presence check) and rewrites them as today.

- **Timestamps:** compliant in every TSO mode, because P's leader writes P's keys.
- **Cost:** every node pages through every legacy group's four meta ranges over RPC to that group's leader, each tick. The rows it can use are only those owned by groups it leads; the rest are transferred and dropped. These are the remote whole-prefix scans a36dac7f removed.
- **The range never shrinks.** The rewrite goes to P and L's copy stays, so every node rescans it on every pass, forever.

### B. Legacy group's leader checks the owner and forwards the rewrite

L's leader already scans L. For a legacy-route row whose owner P is led elsewhere:
1. Ask P's leader whether P holds a version of the key at `readTS`. The authoritative probe exists: `routeHasVersionAtOrBeforeRemote` sends `RawLatestCommitTS` with `VersionVisibleAtTs`, served by `VersionExistsAtOrBeforeGroupWithReadFence`.
2. If P holds none, dispatch the rewrite for P's key with `StartTS = readTS`. P's leader validates it with OCC. A write to the key after `readTS` conflicts and the next pass retries.

The commit timestamp must not come from L's leader:
- **Phase D:** the TSO (group 0) issues all timestamps, so the migrator's existing `NextTimestampAfterThrough` call is compliant wherever it runs.
- **A0b (legacy mode):** a non-leader dispatch is forwarded **unstamped** and P's leader allocates. `Internal.Forward` rejects a pre-stamped commit timestamp, so the migrator must dispatch unstamped for keys it does not lead.
- **Neither in place:** stamping on L's leader is exactly G10. Option B must stay disabled, and the row stays as today.

Properties:
- **Cost:** one presence RPC per such row, plus the forwarded write. Both are bounded by L's one page per tick, and the work is done once per legacy row, not once per node.
- **No new scans.** L's leader is already scanning L.

### C. Evacuate stale legacy copies

Once P holds a version of the key, L's copy is shadowed for good. Compaction keeps the latest version at or below the horizon, tombstones included (`TestMVCCStore_Compact_Delete`), so the fallback never reaches L's copy again. Deleting it costs nothing in correctness and buys two things:
- the migrator stops rescanning it;
- once no group holds legacy-route rows, the dual-route fallback (an extra route lookup and a version probe on misses) can be retired.

The delete is a write into **L** at the raw key, issued by L's leader, so it is compliant in every TSO mode. However, the normal dispatch path routes the raw key to P by its user key, and **a plain `Del` would delete P's copy**. Option C needs an explicit-group delete: "delete this raw key in group L, if L's route still covers it". `ShardStore` has explicit-group reads (`GetGroupAt`, `ScanGroupAt`) but no explicit-group write.

### D. Measure first

A cheap local count, on each group's leader, of legacy-route rows by owner group, plus a "full pass finished" signal per plan. Measurement alone migrates nothing, but it tells operators whether options A to C matter for their cluster and gives Phase 3 a concrete gate.

## 4. Proposal

| Milestone | Change | Gated on |
|---|---|---|
| M1 | **Measure.** Per-group-leader counters of legacy-route rows the scan sees, split into: served (owner led locally), owner led elsewhere, owner holds a newer version. Also a per-plan "pass complete" timestamp. Expose them as metrics and in an admin RPC. | bootjp/elastickv#1278 |
| M2 | **Option B.** L's leader resolves owner-led-elsewhere rows with the remote presence probe and forwards the rewrite. It dispatches unstamped under A0b and TSO-stamped under Phase D. Disabled, with the row skipped as today, when neither holds. | M1; A0b or Phase D |
| M3 | **Option C.** Explicit-group delete of L's copy once P holds a version, in a later pass, by L's leader. | M2; an explicit-group delete path |
| M4 | **Phase 3 gate.** Document that the legacy fallback may be disabled only when M1 reports zero legacy-route rows with the owner led elsewhere, and zero served-but-unmigrated rows, after a complete pass on every group. | M1 to M3 |

Option A is rejected. It restores the cost profile a36dac7f removed and never converges, because the copies it reads are never removed.

## 5. Cost

- **M1:** no extra reads. The counters classify rows the page already reads; a classification is one in-memory route lookup.
- **M2:** at most one presence RPC and one forwarded write per row with a remote owner, within L's existing page (≤ 512 rows per plan per tick). If profiles show RPC latency dominating the 5s tick budget, batch the probes per owner group (a `RawVersionsVisibleAt` RPC taking many keys). That is an optimisation, not a prerequisite.
- **M3:** one explicit-group delete per evacuated row, once. After evacuation, L's legacy range is empty and its plans read nothing.

## 6. Correctness

- **OCC:** the rewrite's `StartTS` is the page's `readTS`. A user write to the key at P after `readTS` makes the rewrite conflict, and the row is reconsidered on a later pass.
- **Presence-check races:** if P gains a version between the probe and the commit, the rewrite conflicts with it, because it writes the same key. If P's leadership moves, the probe is answered only by a current leader (it reports "cannot answer" otherwise), and the forward is re-routed or fails.
- **Timestamps:** never issued by L's leader for P's key. Under Phase D they come from group 0; under A0b P's leader allocates. Without either, M2 stays off.
- **Staged visibility:** rows whose route, at L or P, is under staged visibility stay deferred, as in bootjp/elastickv#1278.
- **M3 safety:** delete L's copy only when P holds a version at the delete's own read timestamp. The explicit-group delete must refuse if L's route no longer covers the raw key, which can happen after a catalog change.

## 7. Test plan

- `kv`: a real split `ShardStore`, with P behind a `followerProxyEngine` whose leader is a fake RawKV server. Cover the probe answering "none", "has a version" and "cannot answer". Cover unstamped forwarding under A0b and TSO stamping under Phase D. M2 must stay off when neither holds.
- `adapter`: compactor tests where L and P have different leaders. The legacy row is migrated into P exactly once; a concurrent user write at P wins.
- M3: an explicit-group delete removes L's copy only, never P's, and refuses after the route moves.
- Jepsen: the Redis workloads with a seeded legacy-route backlog and leader shuffles, checking linearizable reads of the affected keys during migration and evacuation.

## Decisions

- **The cross-leader case is handled outside bootjp/elastickv#1278.** That PR keeps leaving such rows untouched, and Phase 3 stays blocked until this design lands. (Maintainer decision, 2026-10-17.)
- Proposed, pending review: reject option A. Implement options D, B and C in that order, with B gated on A0b or Phase D.

## Open questions

1. Should M2 wait for Phase D alone, which may come sooner, or also accept A0b?
2. Where does the explicit-group delete belong: a `ShardStore` method behind a coordinator capability, or a dedicated internal RPC?
3. Can a cluster's operators run M1 as a one-shot audit command, without waiting for a full background pass?
