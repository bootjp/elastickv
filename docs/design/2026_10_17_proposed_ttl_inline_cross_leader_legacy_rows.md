# TTL-inline migration of legacy-route rows across leaders

Status: Proposed
Author: bootjp
Date: 2026-10-17

Related:
- [2026_04_17_implemented_ttl_inline_value.md](2026_04_17_implemented_ttl_inline_value.md): Phase 2 (background migration) and Phase 3 (removing the legacy `!redis|ttl|` read fallback).
- [2026_04_16_implemented_centralized_tso.md](2026_04_16_implemented_centralized_tso.md) §7.4: Phase D.
- [2026_09_26_proposed_serializable_isolation_audit.md](2026_09_26_proposed_serializable_isolation_audit.md): gaps G0 (across groups) and G10, fixes A0 and A0b.
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
- Fixing G10 or G0 in general. This design depends on A0b or Phase D and implements neither; it closes G0 only for its own relocations (§3 B).

## 3. Options

### A. Owner-side scans of foreign legacy groups

P's leader also scans the hash, set, zset and stream meta prefixes in every group it does not lead whose routes cover those raw prefixes. It serves the rows whose owner is P (a local presence check) and rewrites them as today.

- **Timestamps:** compliant in every TSO mode, because P's leader writes P's keys.
- **Cost:** every node pages through every legacy group's four meta ranges over RPC to that group's leader, each tick. The rows it can use are only those owned by groups it leads; the rest are transferred and dropped. These are the remote whole-prefix scans a36dac7f removed.
- **Cost bound.** Take N nodes and G_L groups that hold legacy rows, with one page of at most 512 rows per plan per tick (30s). Each node scans the four meta plans of every legacy group it does not lead. That is up to 4 · N · G_L remote page RPCs per tick across the cluster, carrying up to 2,048 · N · G_L rows. With N = 5 and G_L = 1, that is 20 RPCs and up to 10,240 rows every 30s. All of them compete for the shared 5s tick budget with the node's own plans. A node can use only the rows whose owner it leads, about 1/N of them, so the cluster reads each row about N times per pass.
- **The range never shrinks.** The rewrite goes to P and L's copy stays, so every node rescans it on every pass, forever. That cost lasts after the backlog is migrated.

#### A′. Owner-side scans that delete L's copy in the same transaction

The relocation into P and an explicit-group delete of L's raw copy run in one transaction, so L's range shrinks and the scans converge.
- **Convergence:** each row is read about N times per pass until it is deleted, so a backlog of B rows costs about N · B row reads in total. Once the range is empty, each plan reads one empty page per tick.
- **Timestamps:** the transaction writes two groups, so it is a cross-group 2PC with P as primary and L as secondary. Its commit timestamp must come from the primary group's leader or the TSO. The audit lists that as **remaining** A0/A0b work: "allocate the 2PC commit timestamp after all PREPAREs succeed, from the primary group's leader or the TSO". Until it lands, a cross-group commit is coordinator-stamped, which is G10. A′ is therefore compliant only under Phase D.
- **New machinery:** a 2PC participant that writes an explicit group. Today 2PC routes each mutation by its key, and L's raw key routes to P.
- **Still N-fold:** it keeps the per-tick bound above until the backlog is empty, and every node still scans groups it does not lead.

### B. Legacy group's leader checks the owner and forwards the rewrite

L's leader already scans L, but `ScanGroupOwnedAt` filters out a legacy-route row whose owner is led elsewhere before the migrator sees it. **The scan must report the rows it skips.** Beside the served rows, the page returns each skipped legacy-route row with its owner group and the reason it was skipped:
- the owner is led elsewhere;
- the owner, led locally, holds a version;
- a route is under staged visibility.

These rows are candidates and inputs to M1's counts, not rows to rewrite as served. They come from the same stored page, so reporting them costs no extra read.

For a legacy-route row whose owner P is led elsewhere:
1. Ask P's leader whether P holds a version of the key at `readTS`. The authoritative probe exists: `routeHasVersionAtOrBeforeRemote` sends `RawLatestCommitTS` with `VersionVisibleAtTs`, served by `VersionExistsAtOrBeforeGroupWithReadFence`.
2. If P holds none, dispatch the rewrite for P's key with `StartTS = readTS`. P's leader validates it with OCC. A write to the key after `readTS` conflicts and the next pass retries.

`RawLatestCommitTS` answers one key per RPC. The batched form, `RawVersionsVisibleAt`, is a **new** `RawKV` RPC with the same semantics per key: the same read fence and "cannot answer" on a non-leader. It is a proto change.

The commit timestamp must not come from L's leader:
- **Phase D:** the TSO (group 0) issues all timestamps, so the migrator's existing `NextTimestampAfterThrough` call is compliant wherever it runs. Phase D is available on main today, opt-in through `--tsoPhaseDEnabled`.
- **A0b (legacy mode):** a non-leader dispatch is forwarded **unstamped** and P's leader allocates. `Internal.Forward` rejects a pre-stamped commit timestamp, so the migrator must dispatch unstamped for keys it does not lead. Every M2 transaction writes only P (the metadata row and its `!redis|ttl|` row both route by user key), so it is a single-shard one-phase transaction. That is the case A0b's first slice forwards unstamped (`forwardSingleShardTxnUnstamped`). A0b is not on main yet: that slice is on an unpushed branch.
- **Neither in place:** stamping on L's leader is exactly G10. Option B must stay disabled, and the row stays as today.

**A relocation requires absence at apply.** OCC alone does not protect a relocation. The FSM rejects a write when the key's latest version is newer than `StartTS` (`validateConflicts`, `checkConflicts*`). But `readTS` is the global snapshot, the maximum applied watermark over all groups. A user write to P's key with a commit timestamp at or below `readTS` that has not yet applied on P is therefore invisible to both the probe and the OCC check. This is G0 across groups (audit §A0, "Multi-group deployments"). A user write to a legacy-route hash reads the old metadata through the fallback and writes P. If it lands first, the relocation, derived from L's older row, overwrites it at a newer timestamp, and the user's write is lost.

The relocation's precondition is stronger than OCC: P holds **no** version of the key at all, whatever its timestamp. M2 makes the FSM check exactly that. Each relocated metadata mutation carries an apply-time condition, "conflict if the key has any version", which P's FSM evaluates at apply, in log order:
- A user write applied before the relocation makes it conflict, whatever its timestamp.
- A user write applied after it conflicts as usual when its `StartTS` is below the relocation's commit timestamp. If its `StartTS` is above that timestamp, it read P before the relocation applied: that is G0 on the user's side, as for any transaction. It still loses nothing, because it was derived from the same legacy row the relocation copied, and the relocation carries no user data of its own.
- The presence probe stays as a pre-filter. Without it, every batch holding a row P already holds would conflict on every pass.
- The condition needs a new mutation field, which is a wire change. An old FSM would ignore the field and silently drop the check, so M2 runs only once every voter of P reports the capability. Until then, the same-node path keeps today's behaviour.
- The same-node relocation path of bootjp/elastickv#1278 has the same exposure (its open review thread on `ownerHoldsNoVersionAt`), and M2 applies the condition there too.

L's side cannot go stale the same way. Nothing writes L's raw metadata copy: user writes route by user key to P, and only M3 deletes the copy. A catalog move of L's range puts the route under staged visibility, and the row is deferred.

**Batches are partitioned by owner group.** The migrator batches up to 32 keys per OCC transaction, and one scan page can hold rows owned by several remote groups. Under A0b no single leader can stamp a batch that writes several groups without one of them receiving a foreign-group timestamp. M2 therefore groups rewrites by owner group and chunks each group's rows into transactions of at most 32 keys, each forwarded to that group's leader. A page with many rows of one owner produces several transactions for it. A batch spanning groups is allowed only under Phase D, where group 0 stamps everything.

**Batches are bound to the catalog version they were grouped under.** A split or move between grouping and commit can leave one batch's keys owned by both P and a new group Q. Each batch records the catalog version used to resolve its owners. Before dispatch, the migrator re-resolves the owners. If the catalog has moved past that version, it repartitions the rows under the new catalog and re-probes the owners that changed. The forward carries the version, and P's leader refuses it if any key no longer routes to P. A refused batch leaves its rows unsettled, so the next tick redoes them under the current catalog.

**A relocation always writes.** The rewrite goes through the handler's `buildElems`, which uses `putIfChanged`. A legacy-route row already in inline format, written after TTL-inline encoding shipped but before user-key routing, produces no mutation, because L's value equals the desired value. The row would then count as settled with nothing copied into P, and reappear on every pass. When the presence probe says P holds no version, M2 forces the metadata `Put` into P, plus the TTL-index sync. Value equality is a valid no-op only once P holds the row. The same-node path of bootjp/elastickv#1278 has the same no-op for already-inline legacy rows, and M2 applies the forced `Put` there too, so M3 can evacuate those rows as well.

**Progress is checkpointed within the page.** Today a plan's cursor advances only after the whole page has been processed, and every plan shares one 5s tick. A page of remote-owner rows, each needing a probe and a forward, could exhaust the tick before the page ends. The next tick would then restart on the same page and repeat the same probes, starving every row behind them. M2 must:
- probe in batches per owner group, one RPC carrying many keys (`RawVersionsVisibleAt`), with the owner groups' batches issued concurrently;
- checkpoint only a contiguous prefix of the page. Owner groups interleave in scan order (`P1, P2, P1, …`) and their batches finish in any order, so the cursor may advance only to the last row of the longest prefix in which every row is settled. A row is settled when its transaction committed, or when it needed no work: owned, skipped, or the owner already holds a version. A tick that runs out of budget resumes behind that prefix and redoes at most the unsettled rows after it. A relocation that committed without being recorded as settled is not written twice: the redo re-probes it, finds P's version, and settles it with no work. A redo that raced past the probe conflicts on the absence condition.

Properties:
- **Cost:** one batched presence RPC per owner group per page, plus one forwarded transaction per 32 rows of each owner group, bounded by L's one page per tick.
- **Repeated probes.** The work is not done once per row while L's copy exists. After the rewrite, the copy stays in L, and every later pass re-probes it only to learn that P now holds a version. That recurring traffic lasts until M3 removes the copy. §5 budgets it, and M2 and M3 ship together so the recurrence is short-lived.
- **No new scans.** L's leader is already scanning L.

### C. Evacuate stale legacy copies

Once P holds a version of the **metadata** key, L's copy of that row is shadowed for good. Compaction keeps the latest version at or below the horizon, tombstones included (`TestMVCCStore_Compact_Delete`), so the fallback never reaches L's copy again. Deleting it costs nothing in correctness, and the migrator stops rescanning and re-probing it.

This does **not** let the dual-route fallback be retired. A collection created before user-key routing may also have field, member, score or entry rows that live only in L. Evacuating its metadata row moves none of them, and a point read such as `HGET` still needs the legacy route to find them. Retiring the fallback needs a separate migration of every row family, with its own gate. That is out of scope here (§2).

**Enumerating the candidates.** The rows M3 deletes are exactly the ones the migrator's scan does not serve: `ScanGroupOwnedAt` skips a legacy-route row once its owner holds any version, a tombstone included. Scans that canonicalize wide-column rows drop it as well, because the owner's tombstone stops the point-read fallback. M3 therefore reads L's raw stored page, `scanRouteAtForwardStoredPage` without the point-read pass, and keeps legacy-route rows whose owner, a group other than L, holds a version at the read timestamp. Rows shadowed by a tombstone at P are candidates like any other.

The delete is a write into **L** at the raw key, issued by L's leader, so it is compliant in every TSO mode. However, the normal dispatch path routes the raw key to P by its user key, and **a plain `Del` would delete P's copy**. Option C needs an explicit-group delete: "delete this raw key in group L, if L's route still covers it". `ShardStore` has explicit-group reads (`GetGroupAt`, `ScanGroupAt`) but no explicit-group write.

**M3 progress is checkpointed within the page, like M2's.** A raw page full of shadowed rows needs a presence probe and a delete per row. Done one row at a time, that could exhaust the shared 5s tick before the page ends, and every later tick would restart on the same page. M3 therefore:
- probes in batches per owner group (`RawVersionsVisibleAt`), issued concurrently across groups;
- deletes in batches of at most 32 keys. Every key in a batch lives in L, so one explicit-group transaction by L's leader covers it;
- keeps its own cursor per plan and advances it only over the longest contiguous prefix of settled rows. A row is settled when its delete committed or it is not a candidate.

The cursor is in memory, like the existing plan cursors. A leader change restarts the pass, which redoes only idempotent work: deleting an absent row is a no-op.

**Why not delete in the relocation transaction (B′).** L's leader could relocate into P and delete L's copy in one transaction, which would save M3's second probe per row. That transaction writes two groups, so it is a cross-group 2PC with the same blockers as A′:
- its commit timestamp needs the unfinished A0/A0b 2PC allocation, or Phase D;
- it needs a 2PC participant that writes an explicit group.

A separate single-group delete by L's leader is compliant in every TSO mode and needs no 2PC. The price is one batched probe per owner group per page in the pass after the rewrite.

### D. Measure first

Measurement alone migrates nothing, but it tells operators whether options A to C matter for their cluster and gives Phase 3 a concrete gate. Three forms:
- **D1, background counters.** On each group's leader, count the legacy-route rows the scan serves or skips, by owner group and reason, plus a "full pass finished" signal per plan. No extra reads (M1).
- **D2, one-shot audit.** An admin RPC on each group's leader runs the same classification once over the four meta ranges at one `readTS`, paging from start to end with no rewrites. It returns counts by bucket and owner group. It reads the whole legacy range once, locally, and finishes in minutes rather than a background pass spread over many 30s ticks. Like D1 it makes no remote probes, so rows whose owner is led elsewhere are counted without the "holds a version" split. With `--probe`, it adds batched presence probes per owner group.
- **D3, offline count.** `cmd/elastickv-snapshot-decode` already reads a group's `.fsm` files. Counting legacy-route rows also needs the route catalog, which lives in the default group's snapshot. This touches no live node, but the snapshot is only as current as its last checkpoint, and no snapshot can see whether a remote owner holds the key.

If D1 or D2 reports no owner-led-elsewhere rows on every cluster that matters, M2 can wait. M3 is still needed in that case. It evacuates the copies the same-node path of bootjp/elastickv#1278 leaves in L, and M4's "owner led locally and holds a version" bucket must reach zero. So M3 would ship on its own.

## 4. Proposal

| Milestone | Change | Gated on |
|---|---|---|
| M1 | **Measure.** Per-group-leader counts of the legacy-route rows the scan serves or reports as skipped (§3 B), split into: served (owner led locally, holds nothing), owner led locally and holds a version, owner led elsewhere, deferred by staged visibility. Without a remote probe, M1 cannot tell whether a remote owner holds the key; M2 adds that split. Counts accumulate per pass of each plan (one plan per family: hash, set, zset, stream). When that plan's cursor wraps, they are published atomically with the pass's completion epoch and the catalog version the pass ran under. A pass that sees the catalog version change restarts. A leadership change discards the in-progress pass and invalidates the group's published snapshots, since a new leader starts new passes. Expose the published snapshots, keyed by group and plan, as metrics and in an admin RPC. The same RPC runs the one-shot audit (§3 D2). | bootjp/elastickv#1278 |
| M2 | **Option B.** L's leader takes the owner-led-elsewhere rows its scan reports as skipped and resolves them with batched, concurrent presence probes per owner group. It forwards each owner group's rewrites in transactions of at most 32 keys: unstamped under A0b, TSO-stamped under Phase D. Each transaction is bound to the catalog version its owners were resolved under, and is repartitioned or refused once the catalog moves. A relocation forces the `Put` when the owner holds nothing, and carries the apply-time absence condition (§3 B), on this path and on the same-node path. The plan cursor advances only over the contiguous prefix of settled rows. It extends M1's counts with "owner led elsewhere and holds a version". Disabled, with the row skipped as today, when neither A0b nor Phase D holds, or when a voter of P lacks the absence condition. | M1; A0b or Phase D; `RawVersionsVisibleAt`; the absence condition |
| M3 | **Option C.** By L's leader, in a later pass: enumerate L's raw page and delete L's metadata copy with an explicit-group delete, when the current owner is a group other than L and holds a version, at the delete's route version. Ships together with M2, which otherwise re-probes every rewritten row on every pass. If M1 shows that M2 can wait (§3 D), M3 ships alone for the copies the same-node path leaves. | M1; an explicit-group delete path |
| M4 | **Legacy-route gate for Phase 3.** For every group and every plan, the latest published M1 snapshot must meet all of these:<br>• it is valid and has a completion epoch from after the rollout;<br>• it reports zero in every bucket: owner led elsewhere (holding a version or not), served but unmigrated, owner led locally and holding a version, and deferred by staged visibility. A nonzero locally shadowed bucket means M3 has not yet evacuated rows whose owner shares L's leader;<br>• it carries the same catalog version as every other gated snapshot, and that version is still current.<br>Counts, epoch and catalog version come from the same pass, so in-progress zeroes after a reset never satisfy the gate. A row moved between groups by a catalog change cannot hide between two passes either. This is **one additional** Phase 3 condition, not the whole gate. The TTL-inline design's other prerequisites still apply: every node has migrated, and legacy bare string keys, which no prefix scan covers, have been ruled out or rewritten. | M1 and M3; M2 wherever M1 reports owner-led-elsewhere rows |

Option A is rejected. It restores the cost profile a36dac7f removed and never converges, because the copies it reads are never removed. A′ converges, but it still reads every row about N times per pass, and it needs the unfinished cross-group 2PC timestamp work or Phase D.

## 5. Cost

Per tick (30s, shared 5s budget, at most 512 physical rows per plan page), with N nodes, G groups, and G_L groups holding legacy rows:

| Option | Who scans | Remote reads per tick, cluster-wide | Writes per tick | Converges |
|---|---|---|---|---|
| A | every node, each legacy group it does not lead | up to 4 · N · G_L page RPCs (≤ 2,048 · N · G_L rows) | ≤ 16 relocation txns per useful page | no: L's copies stay, so the cost is permanent |
| A′ | as A | as A, until the range is empty | as A, as cross-group 2PCs | yes, after about N · B row reads in total |
| B (M2) | L's leader only, its existing pages | per page: ≤ min(G, 512) batched probes | per page: ≤ 16 + G one-phase txns, each to one owner group | yes, with M3 |
| C (M3) | L's leader only, its raw page | per page: ≤ min(G, 512) batched probes | per page: ≤ 16 explicit-group deletes, all in L | yes: each row is deleted once |
| D1 (M1) | L's leader only, its existing pages | none | none | not applicable |
| D2 | each group's leader, once | none (or batched probes with `--probe`) | none | not applicable |

For B and C, the 16 is ⌈512 / 32⌉. The extra G comes from a page split across owner groups, each owner group's last chunk being partial. Unlike A and A′, neither depends on N: a node with no legacy group does no extra work.

- **M1:** no remote reads. The counters classify rows the page already reads, by an in-memory route lookup plus, for a locally led owner, the local version probe bootjp/elastickv#1278 already performs. Whether a remote owner holds the key is left to M2, which issues the probe anyway.
- **M2:** per page (≤ 512 rows per plan per tick), one batched presence RPC per remote owner group, plus one forwarded transaction per 32 of that group's rows, issued concurrently across owner groups. Batching is a convergence requirement, not an optimisation: sequential per-row RPCs could exhaust the tick on one page every time.
- **M2 before M3 evacuates:** every pass re-probes each rewritten row still present in L. That is one batched RPC per owner group per page, recurring for the backlog's size until M3 removes the copies. With M2 and M3 shipped together, a row is re-probed at most until the pass after its rewrite. If M3 is disabled or failing, M1's "owner holds a newer version" counter shows the recurring set.
- **M3:** per raw page, one batched presence RPC per remote owner group, plus one explicit-group delete transaction per 32 candidates. Each metadata row is evacuated once. After evacuation, L's legacy metadata range is empty and the TTL-inline plans read nothing there. Collection field, member, score and entry rows stay in L (§3 C).

## 6. Correctness

- **OCC:** the rewrite's `StartTS` is the page's `readTS`. A user write to the key at P after `readTS` makes the rewrite conflict, and the row is reconsidered on a later pass.
- **Writes below `readTS` (G0 across groups):** `readTS` is the maximum applied watermark over all groups. P can therefore accept a user write with a commit timestamp at or below `readTS` after the probe has answered, and `latest > StartTS` would not catch it. The apply-time absence condition (§3 B) rejects the relocation whenever P holds any version. That closes this case for relocations without a per-row read-index round trip, and without waiting for A0's cross-group fix. Rewrites of rows P already owns keep plain OCC, and stay exposed to G0 like every other transaction until A0 lands.
- **Presence-check races:** if P gains a version between the probe and the commit, the rewrite conflicts with it, because it writes the same key. If P's leadership moves, the probe is answered only by a current leader (it reports "cannot answer" otherwise), and the forward is re-routed or fails.
- **Timestamps:** never issued by L's leader for P's key. Under Phase D they come from group 0; under A0b P's leader allocates. Without either, M2 stays off.
- **Staged visibility:** rows whose route, at L or P, is under staged visibility stay deferred, as in bootjp/elastickv#1278. M1 counts them, so the M4 gate does not pass while any remain.
- **Catalog changes during M2:** a batch whose owners were resolved under an older catalog version is repartitioned before dispatch, or refused by P's leader. Under A0b, no forward therefore writes a group other than the leader's own.
- **Catalog changes during the gate:** each M1 snapshot carries its pass's catalog version, and M4 requires one current version across all of them. A row moved into a group after that group's pass completed fails the gate until every pass has run again under the new version.
- **M3 safety:** delete L's copy only when, at the delete's own read timestamp and route version:
  - the key's current owner is a group other than L;
  - that owner holds a version of the key;
  - L's route still covers the raw key.

  The explicit-group delete carries that route version and refuses if the catalog has moved past it. A catalog change can make L the owner, and then P's "version" would be the very row being deleted. A delete decided against an older catalog could remove the collection's only current metadata copy.

## 7. Test plan

- `kv`: a real split `ShardStore`, with P behind a `followerProxyEngine` whose leader is a fake RawKV server. Cover the probe answering "none", "has a version" and "cannot answer". Cover unstamped forwarding under A0b and TSO stamping under Phase D. M2 must stay off when neither holds.
- `kv`, `RawVersionsVisibleAt`: per-key answers match `RawLatestCommitTS` with `VersionVisibleAtTs` for a mixed batch (absent, visible, tombstoned, newer than `ts`), and a non-leader reports "cannot answer" for the whole batch.
- `kv`, absence condition, written first as a failing reproduction in the style of `TestApplyOrder_GlobalSnapshotMissesLaggingGroupWrite`:
  - two groups; a user write to P's key at `c1` is parked before its proposal while another group's write at `c2 > c1` applies;
  - the migrator takes `readTS = c2`, the probe reports P empty, and the parked write lands on P;
  - the relocation must then conflict, and P must keep the user's value.

  Before the condition exists, the test shows the user's write overwritten. Cover the same-node path too. A relocation applied before a parked user write must make that write conflict. A group with a voter lacking the capability must keep M2 off.
- `adapter`: compactor tests where L and P have different leaders. The legacy row is migrated into P exactly once; a concurrent user write at P wins. One page holding rows of two remote owner groups produces one transaction per owner group. With interleaved owners (`P1, P2, P1`), a tick whose budget runs out after P1's batch commits but before P2's resumes behind the settled prefix and never skips the P2 row. A group with more than 32 rows on the page commits in several transactions. An already-inline legacy row whose owner holds nothing is copied into P, on both the cross-leader and the same-node path. A split of P between grouping and dispatch repartitions the batch, and P's leader refuses a forward carrying a key it no longer owns.
- `kv`: the group scan reports every skipped legacy-route row with its owner group and reason (owner led elsewhere, owner holds a version, staged) and does not serve it.
- M1:
  - a reset pass never publishes zeroes before its completion;
  - a leadership change invalidates the snapshots;
  - each plan publishes its own snapshot;
  - a catalog change during a pass restarts it.
- M3 progress: a tick whose budget runs out mid-page resumes behind the settled prefix and still reaches the rows after it. A page with more than 32 candidates deletes them in several transactions.
- M4: the gate fails when one plan of a group still has rows while the others report zero, and when only the "owner led locally and holds a version" bucket is nonzero. It also fails when snapshots carry different catalog versions, or a version that is no longer current. It does not replace the bare-string prerequisite.
- M3:
  - an explicit-group delete removes L's copy only, never P's;
  - it refuses after the route moves, and when the current owner has become L;
  - rows shadowed by a tombstone at P are found by the raw enumeration and evacuated.
- M1 one-shot audit (D2): on a real split `ShardStore`, it returns the same buckets as a completed background pass over the same data, and it writes nothing.
- Jepsen: the Redis workloads with a seeded legacy-route backlog and leader shuffles, checking linearizable reads of the affected keys during migration and evacuation.
  - The CI workloads write only inline-format data, so they never exercise this path (bootjp/elastickv#1278 notes the same). Seeding needs the explicit-group write M3 introduces, behind a test-only flag, because any routed write of a legacy key lands in P.
  - Place L's and P's leaders on different nodes (`cmd/raftadmin` leadership transfer), then run hash, set, zset and stream operations with TTLs on the seeded keys under the partition and leader-shuffle nemeses.
  - Check that reads, including `TTL`, stay linearizable while M2 and M3 run, and that every M1 bucket ends at zero.
  - Also run the existing `elastickv.redis-workload` and `elastickv.redis-zset-safety-workload` to show no regression.

## Decisions

- **The cross-leader case is handled outside bootjp/elastickv#1278.** That PR keeps leaving such rows untouched, and Phase 3 stays blocked until this design lands. (Maintainer decision, 2026-10-17.)
- Proposed, pending review: reject option A. Implement option D first, then B and C together, with B gated on A0b or Phase D.
- Proposed, pending review: reject A′. Deleting in the same transaction makes owner-side scans converge, but they keep the N-fold read cost, and the cross-group 2PC needs commit-timestamp work A0/A0b has not finished.
- Proposed, pending review: evacuation is a separate single-group delete by L's leader (M3), not part of the relocation transaction (B′). It stays compliant in every TSO mode and needs no 2PC, at the cost of one probe per row in a later pass.
- Proposed, pending review: a relocation is guarded by an apply-time absence condition, not only by OCC at `readTS`. This closes G0 across groups for relocations, on both the cross-leader and the same-node path, without a per-row fence.
- Proposed, pending review: M1 ships first, with the one-shot audit (D2). Its results decide whether M2 is needed on the clusters that matter. M3 is needed either way.
- Proposed, pending review: retiring the dual-route fallback is not a goal of this design. It needs a migration of every row family of pre-routing collections, not only their metadata.

## Open questions

1. Should M2 wait for Phase D alone, which is opt-in on main today, or also accept A0b once it lands?
2. Where does the explicit-group delete belong: a `ShardStore` method behind a coordinator capability, or a dedicated internal RPC?
3. Should the absence condition be a field on `pb.Mutation`, or a transaction-level list of keys that must be absent? The field is simpler; the list keeps the condition out of every other mutation path.
