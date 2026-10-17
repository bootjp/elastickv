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

**Batches are partitioned by owner group.** The migrator batches up to 32 keys per OCC transaction, and one scan page can hold rows owned by several remote groups. Under A0b no single leader can stamp a batch that writes several groups without one of them receiving a foreign-group timestamp. M2 therefore groups rewrites by owner group and chunks each group's rows into transactions of at most 32 keys, each forwarded to that group's leader. A page with many rows of one owner produces several transactions for it. A batch spanning groups is allowed only under Phase D, where group 0 stamps everything.

**A relocation always writes.** The rewrite goes through the handler's `buildElems`, which uses `putIfChanged`. A legacy-route row already in inline format, written after TTL-inline encoding shipped but before user-key routing, produces no mutation, because L's value equals the desired value. The row would then count as settled with nothing copied into P, and reappear on every pass. When the presence probe says P holds no version, M2 forces the metadata `Put` into P, plus the TTL-index sync. Value equality is a valid no-op only once P holds the row. The same-node path of bootjp/elastickv#1278 has the same no-op for already-inline legacy rows, and M2 applies the forced `Put` there too, so M3 can evacuate those rows as well.

**Progress is checkpointed within the page.** Today a plan's cursor advances only after the whole page has been processed, and every plan shares one 5s tick. A page of remote-owner rows, each needing a probe and a forward, could exhaust the tick before the page ends. The next tick would then restart on the same page and repeat the same probes, starving every row behind them. M2 must:
- probe in batches per owner group, one RPC carrying many keys (`RawVersionsVisibleAt`), with the owner groups' batches issued concurrently;
- checkpoint only a contiguous prefix of the page. Owner groups interleave in scan order (`P1, P2, P1, …`) and their batches finish in any order, so the cursor may advance only to the last row of the longest prefix in which every row is settled. A row is settled when its transaction committed, or when it needed no work: owned, skipped, or the owner already holds a version. A tick that runs out of budget resumes behind that prefix and redoes at most the unsettled rows after it; redoing a committed rewrite is a no-op (`putIfChanged`).

Properties:
- **Cost:** one batched presence RPC per owner group per page, plus one forwarded transaction per 32 rows of each owner group, bounded by L's one page per tick.
- **Repeated probes.** The work is not done once per row while L's copy exists. After the rewrite, the copy stays in L, and every later pass re-probes it only to learn that P now holds a version. That recurring traffic lasts until M3 removes the copy. §5 budgets it, and M2 and M3 ship together so the recurrence is short-lived.
- **No new scans.** L's leader is already scanning L.

### C. Evacuate stale legacy copies

Once P holds a version of the **metadata** key, L's copy of that row is shadowed for good. Compaction keeps the latest version at or below the horizon, tombstones included (`TestMVCCStore_Compact_Delete`), so the fallback never reaches L's copy again. Deleting it costs nothing in correctness, and the migrator stops rescanning and re-probing it.

This does **not** let the dual-route fallback be retired. A collection created before user-key routing may also have field, member, score or entry rows that live only in L. Evacuating its metadata row moves none of them, and a point read such as `HGET` still needs the legacy route to find them. Retiring the fallback needs a separate migration of every row family, with its own gate. That is out of scope here (§2).

**Enumerating the candidates.** The rows M3 deletes are exactly the ones the migrator's scan does not serve: `ScanGroupOwnedAt` skips a legacy-route row once its owner holds any version, a tombstone included. Scans that canonicalize wide-column rows drop it as well, because the owner's tombstone stops the point-read fallback. M3 therefore reads L's raw stored page, `scanRouteAtForwardStoredPage` without the point-read pass, and keeps legacy-route rows whose owner, a group other than L, holds a version at the read timestamp. Rows shadowed by a tombstone at P are candidates like any other.

The delete is a write into **L** at the raw key, issued by L's leader, so it is compliant in every TSO mode. However, the normal dispatch path routes the raw key to P by its user key, and **a plain `Del` would delete P's copy**. Option C needs an explicit-group delete: "delete this raw key in group L, if L's route still covers it". `ShardStore` has explicit-group reads (`GetGroupAt`, `ScanGroupAt`) but no explicit-group write.

### D. Measure first

A cheap local count, on each group's leader, of legacy-route rows by owner group, plus a "full pass finished" signal per plan. Measurement alone migrates nothing, but it tells operators whether options A to C matter for their cluster and gives Phase 3 a concrete gate.

## 4. Proposal

| Milestone | Change | Gated on |
|---|---|---|
| M1 | **Measure.** Per-group-leader counts of legacy-route rows the scan sees, split into: served (owner led locally, holds nothing), owner led locally and holds a version, owner led elsewhere. Without a remote probe, M1 cannot tell whether a remote owner holds the key; M2 adds that split. Counts accumulate per pass and are published atomically with that pass's completion epoch when the plan's cursor wraps. A leadership change discards the in-progress pass and invalidates the published snapshot, since a new leader starts a new pass. Expose the published snapshots as metrics and in an admin RPC. | bootjp/elastickv#1278 |
| M2 | **Option B.** L's leader resolves owner-led-elsewhere rows with batched, concurrent presence probes per owner group. It forwards each owner group's rewrites in transactions of at most 32 keys: unstamped under A0b, TSO-stamped under Phase D. A relocation forces the `Put` when the owner holds nothing. The plan cursor advances only over the contiguous prefix of settled rows. It extends M1's counts with "owner led elsewhere and holds a version". Disabled, with the row skipped as today, when neither A0b nor Phase D holds. | M1; A0b or Phase D; `RawVersionsVisibleAt` |
| M3 | **Option C.** By L's leader, in a later pass: enumerate L's raw page and delete L's metadata copy with an explicit-group delete, when the current owner is a group other than L and holds a version, at the delete's route version. Ships together with M2, which otherwise re-probes every rewritten row on every pass. | M2; an explicit-group delete path |
| M4 | **Phase 3 gate.** Document that the legacy fallback may be disabled only when, for every group, the latest published M1 snapshot has a valid completion epoch from after the rollout and reports zero legacy-route rows with the owner led elsewhere and zero served-but-unmigrated rows. Counts and epoch come from the same pass, so in-progress zeroes after a reset can never satisfy the gate. | M1 to M3 |

Option A is rejected. It restores the cost profile a36dac7f removed and never converges, because the copies it reads are never removed.

## 5. Cost

- **M1:** no remote reads. The counters classify rows the page already reads, by an in-memory route lookup plus, for a locally led owner, the local version probe bootjp/elastickv#1278 already performs. Whether a remote owner holds the key is left to M2, which issues the probe anyway.
- **M2:** per page (≤ 512 rows per plan per tick), one batched presence RPC per remote owner group, plus one forwarded transaction per 32 of that group's rows, issued concurrently across owner groups. Batching is a convergence requirement, not an optimisation: sequential per-row RPCs could exhaust the tick on one page every time.
- **M2 before M3 evacuates:** every pass re-probes each rewritten row still present in L. That is one batched RPC per owner group per page, recurring for the backlog's size until M3 removes the copies. With M2 and M3 shipped together, a row is re-probed at most until the pass after its rewrite. If M3 is disabled or failing, M1's "owner holds a newer version" counter shows the recurring set.
- **M3:** one explicit-group delete per evacuated metadata row, once. After evacuation, L's legacy metadata range is empty and the TTL-inline plans read nothing there. Collection field, member, score and entry rows stay in L (§3 C).

## 6. Correctness

- **OCC:** the rewrite's `StartTS` is the page's `readTS`. A user write to the key at P after `readTS` makes the rewrite conflict, and the row is reconsidered on a later pass.
- **Presence-check races:** if P gains a version between the probe and the commit, the rewrite conflicts with it, because it writes the same key. If P's leadership moves, the probe is answered only by a current leader (it reports "cannot answer" otherwise), and the forward is re-routed or fails.
- **Timestamps:** never issued by L's leader for P's key. Under Phase D they come from group 0; under A0b P's leader allocates. Without either, M2 stays off.
- **Staged visibility:** rows whose route, at L or P, is under staged visibility stay deferred, as in bootjp/elastickv#1278.
- **M3 safety:** delete L's copy only when, at the delete's own read timestamp and route version:
  - the key's current owner is a group other than L;
  - that owner holds a version of the key;
  - L's route still covers the raw key.

  The explicit-group delete carries that route version and refuses if the catalog has moved past it. A catalog change can make L the owner, and then P's "version" would be the very row being deleted. A delete decided against an older catalog could remove the collection's only current metadata copy.

## 7. Test plan

- `kv`: a real split `ShardStore`, with P behind a `followerProxyEngine` whose leader is a fake RawKV server. Cover the probe answering "none", "has a version" and "cannot answer". Cover unstamped forwarding under A0b and TSO stamping under Phase D. M2 must stay off when neither holds.
- `adapter`: compactor tests where L and P have different leaders. The legacy row is migrated into P exactly once; a concurrent user write at P wins. One page holding rows of two remote owner groups produces one transaction per owner group. With interleaved owners (`P1, P2, P1`), a tick whose budget runs out after P1's batch commits but before P2's resumes behind the settled prefix and never skips the P2 row. A group with more than 32 rows on the page commits in several transactions. An already-inline legacy row whose owner holds nothing is copied into P, on both the cross-leader and the same-node path. M1: a reset pass never publishes zeroes before its completion, and a leadership change invalidates the snapshot.
- M3:
  - an explicit-group delete removes L's copy only, never P's;
  - it refuses after the route moves, and when the current owner has become L;
  - rows shadowed by a tombstone at P are found by the raw enumeration and evacuated.
- Jepsen: the Redis workloads with a seeded legacy-route backlog and leader shuffles, checking linearizable reads of the affected keys during migration and evacuation.

## Decisions

- **The cross-leader case is handled outside bootjp/elastickv#1278.** That PR keeps leaving such rows untouched, and Phase 3 stays blocked until this design lands. (Maintainer decision, 2026-10-17.)
- Proposed, pending review: reject option A. Implement option D first, then B and C together, with B gated on A0b or Phase D.
- Proposed, pending review: retiring the dual-route fallback is not a goal of this design. It needs a migration of every row family of pre-routing collections, not only their metadata.

## Open questions

1. Should M2 wait for Phase D alone, which may come sooner, or also accept A0b?
2. Where does the explicit-group delete belong: a `ShardStore` method behind a coordinator capability, or a dedicated internal RPC?
3. Can a cluster's operators run M1 as a one-shot audit command, without waiting for a full background pass?
