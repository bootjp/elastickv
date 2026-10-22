# TTL-inline migration of legacy-route rows across leaders

Status: Proposed
Author: bootjp
Date: 2026-10-17

Related:
- [2026_04_17_implemented_ttl_inline_value.md](2026_04_17_implemented_ttl_inline_value.md): Phase 2 (background migration) and Phase 3 (removing the legacy `!redis|ttl|` read fallback).
- [2026_04_16_implemented_centralized_tso.md](2026_04_16_implemented_centralized_tso.md) §7.4: Phase D.
- [2026_09_26_proposed_serializable_isolation_audit.md](2026_09_26_proposed_serializable_isolation_audit.md): gaps G0 (across groups) and G10, fixes A0 and A0b.
- bootjp/elastickv#1278 (merged to main as `e569513`): the per-group migration this proposal builds on, and the review threads that deferred this case to a separate design. Its `ScanGroupOwnedAt`, `ownerHoldsNoVersionAt` and plan rotation are on main, as is the `SyncOnce` ordering §3 C changes.

## 1. Problem

After bootjp/elastickv#1278, each node migrates TTL anchors only in the data groups it leads. Each group's rows come from `ShardStore.ScanGroupOwnedAt`. That scan also serves hash, set, zset and stream metadata still stored at the route of its **raw** key:
- such a row sits in group L, while its user key belongs to group P;
- the scan serves it only when the node leading L also leads P, and P holds no version of the key at the read timestamp (`ownerHoldsNoVersionAt`);
- that is exactly when a point read falls back to the legacy route (`legacyPointRouteKey`, `routeMissStopsPointFallback`).

When different nodes lead L and P, nothing processes the row:
- L's leader cannot tell whether P holds the key. `routeHasVersionAtOrBefore` answers only on P's leader.
- P's leader plans scans only for the groups it leads, so it never reads L.
- Only P's leader may issue the rewrite's timestamp. This is the repository's HLC rule, which the migrator keeps by dispatching only for keys it leads (`IsLeaderForKey`).

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
- Fixing G10 or G0 in general. This design needs neither A0b nor Phase D, because every relocation is dispatched by the owner group's own leader, and it closes G0 only for its own relocations (§3 B).

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

For a legacy-route row whose owner P is led elsewhere, L's leader does **not** build the rewrite itself. Building it reads the key's legacy TTL anchor twice: `legacyTTLMillisAt` and `ttlIndexSyncElem` both call `GetAt(redisTTLKey(userKey), readTS)`. That key routes to P, so on L's leader every row would cost up to two sequential remote reads, which no batching of the presence probe bounds. One 512-row page could then use up the 5s tick before its first transaction is dispatched, and the next tick would restart at the same cursor. Instead, P's leader builds and dispatches the rewrite:
1. L's leader groups the rows by owner group. For each group it sends at most 32 rows per request to P's leader, in a new peer-only `Internal` RPC, `RelocateLegacyRows`. A request carries each row's raw key and value as read from L, the `readTS` they were read at, and the catalog version the owners were resolved under.
2. P's leader serves the request locally, behind its read fence:
   - it refuses the request if the catalog has moved past that version, or if any key no longer routes to P;
   - it checks each key's presence in P at `readTS` (`VersionExistsAtOrBefore`, the check the remote probe already uses);
   - it reads the TTL anchors from its own store and builds the elements with the handler's `buildElems`, forcing the metadata `Put`;
   - it dispatches one transaction with `StartTS = readTS` and M1b's absence condition on every relocated metadata key.

   It answers per row: relocated, already held, conflicted, or refused.
3. L's leader settles each row from that answer.

The presence check thus costs no separate round trip, and the TTL-anchor reads are local to P. M3 and D2's `--probe` still probe P from L's leader, because they only need presence. They use the batched form already on main, so they need no new RPC: `RawLatestCommitTSRequest.key_batch` (`pb.EncodeRawLatestCommitTSKeyBatch`) carries many keys, answered in `version_visible_results` by `ShardStore.VersionsExistAtOrBeforeGroupWithReadFence`. It has the same semantics per key as the single-key probe: the same read fence, and "cannot answer" for the whole batch on a non-leader. Its client is `ShardStore.routeHasVersionsAtOrBeforeRemote`, which today serves the S3 bucket-auxiliary owner probe. The server decodes at most `maxGRPCScanLimit` (1,025) keys per request, more than one 512-row page.

**Timestamps.** P's leader dispatches the relocation as the leader of every key it writes, like any other write it serves. In legacy HLC mode the commit timestamp therefore comes from P's leader, and under Phase D from the TSO. L's leader never dispatches for P's keys, stamped or unstamped, so M2 does not depend on A0b or Phase D. (An earlier revision dispatched from L's leader, which needed A0b's unstamped forwarding or Phase D; stamping on L's leader would be G10.) Every relocation transaction writes only P: the metadata row and its `!redis|ttl|` row both route by user key.

**A relocation requires absence at apply.** OCC alone does not protect a relocation. The FSM rejects a write when the key's latest version is newer than `StartTS` (`validateConflicts`, `checkConflicts*`). But `readTS` is the global snapshot, the maximum applied watermark over all groups. A user write to P's key with a commit timestamp at or below `readTS` that has not yet applied on P is therefore invisible to both the probe and the OCC check. This is G0 across groups (audit §A0, "Multi-group deployments"). A user write to a legacy-route hash reads the old metadata through the fallback and writes P. If it lands first, the relocation, derived from L's older row, overwrites it at a newer timestamp, and the user's write is lost.

The relocation's precondition is stronger than OCC: P holds **no** version of the key at all, whatever its timestamp. Milestone M1b makes the FSM check exactly that, on every relocation path; M2 then uses it for cross-leader relocations. Each relocated metadata mutation carries an apply-time condition, "conflict if the key has any version", which P's FSM evaluates at apply, in log order:
- A user write applied before the relocation makes it conflict, whatever its timestamp.
- A user write applied after it conflicts as usual when its `StartTS` is below the relocation's commit timestamp. If its `StartTS` is above that timestamp, it read P before the relocation applied: that is G0 on the user's side, as for any transaction. It still loses nothing, because it was derived from the same legacy row the relocation copied, and the relocation carries no user data of its own.
- The presence probe stays as a pre-filter. Without it, every batch holding a row P already holds would conflict on every pass.
- The condition needs a new mutation field, which is a wire change. An old FSM would ignore the field and silently drop the check, so the gate covers every replica that can apply P's log, not only its voters. A learner still catching up would otherwise replay a relocation without the check. If P already holds the key, that learner overwrites it while the upgraded replicas reject the entry, and its FSM diverges before it is ever promoted. M1b's guarded relocations, and so M2, therefore run only after P replicates a capability marker, which P's leader proposes only once every voter and learner in its configuration reports the capability. Reporting the capability includes a **downgrade fence**: before it reports, each replica persists a new form of its `raft-engine` marker. Today's binaries refuse to open a data directory whose marker they cannot parse (`readRaftEngineMarker` → `parseRaftEngineType`), so an existing voter or learner restarted in place on an old binary fails at startup instead of replaying relocation entries without the check. This is how A7 fences its own FSM changes, and no membership change is needed for it to apply.

  Two more rules close the gaps around membership:
  - **The marker is bound to the configuration it was checked against.** A conf change that passed admission before activation can still apply after the leader checked capabilities. So the marker entry carries the configuration it was validated for: the voter and learner IDs and the index of the last applied conf change. P's FSM applies the marker only if that configuration is still current at the marker's log position, and otherwise ignores it; the leader then rechecks and proposes again. Conversely, a leader refuses to propose a conf change that would add a replica lacking the capability once its log contains the marker, applied or not. A new leader sees the marker in its log, so this holds across leader changes. Every conf change ordered before the marker in the log therefore fails the marker's check, and every one ordered after it was admitted against the marker.
  - **A joining replica fences its directory before its FSM changes.** A node that joins P after activation creates P's data directory then, long after it first advertised the capability. An M1b binary therefore creates every new group directory with the fenced `raft-engine` marker form, before any snapshot restore or log replay touches the FSM. Admission also requires the joining replica to report that P's directory carries the fence. From then on, a membership change that would add a replica lacking the capability is refused: a fresh learner joins by log replay or by snapshot, and either may reach relocation entries. This is the rollout rule the serializable isolation audit's A7 sets for FSM semantic changes. Until the marker is in place, the same-node path keeps today's behaviour, and M3 stays off for keys owned by that group.
- The same-node relocation path of bootjp/elastickv#1278 has the same exposure (its open review thread on `ownerHoldsNoVersionAt`). M1b applies the condition there, independently of M2: the same-node path relocates rows today, and M3 deletes L's copy after such a relocation, so M3 must never ship before the guard does.

L's side cannot go stale the same way. Nothing writes L's raw metadata copy: user writes route by user key to P, and only M3 deletes the copy. A catalog move of L's range puts the route under staged visibility, and the row is deferred.

**Batches are partitioned by owner group.** The migrator batches up to 32 keys per OCC transaction, and one scan page can hold rows owned by several remote groups. Each `RelocateLegacyRows` request goes to one owner group's leader, which may stamp only its own keys, so M2 groups rows by owner group and chunks each group's rows into requests of at most 32 keys. A page with many rows of one owner produces several requests for it.

**Batches are bound to the catalog version they were grouped under.** A split or move between grouping and commit can leave one batch's keys owned by both P and a new group Q. Each batch records the catalog version used to resolve its owners. Before dispatch, the migrator re-resolves the owners. If the catalog has moved past that version, it repartitions the rows under the new catalog. The request carries the version, and P's leader refuses it if any key no longer routes to P. A refused batch leaves its rows unsettled, so the next tick redoes them under the current catalog.

**A relocation always writes.** The rewrite goes through the handler's `buildElems`, which uses `putIfChanged`. A legacy-route row already in inline format, written after TTL-inline encoding shipped but before user-key routing, produces no mutation, because L's value equals the desired value. The row would then count as settled with nothing copied into P, and reappear on every pass. When the presence probe says P holds no version, M2 forces the metadata `Put` into P, plus the TTL-index sync. Value equality is a valid no-op only once P holds the row. The same-node path of bootjp/elastickv#1278 has the same no-op for already-inline legacy rows, and M2 applies the forced `Put` there too, so M3 can evacuate those rows as well.

**Progress is checkpointed within the page.** Today a plan's cursor advances only after the whole page has been processed, and every plan shares one 5s tick. A page of remote-owner rows, each needing a probe and a forward, could exhaust the tick before the page ends. The next tick would then restart on the same page and repeat the same probes, starving every row behind them. M2 must:
- send one `RelocateLegacyRows` request per owner group and 32 rows, with the owner groups' requests issued concurrently;
- checkpoint only a contiguous prefix of the page. Owner groups interleave in scan order (`P1, P2, P1, …`) and their batches finish in any order, so the cursor may advance only to the last row of the longest prefix in which every row is settled. A row is settled when its transaction committed, or when it needed no work: owned, skipped, or the owner already holds a version. A tick that runs out of budget resumes behind that prefix and redoes at most the unsettled rows after it. A relocation that committed without being recorded as settled is not written twice: the redo re-probes it, finds P's version, and settles it with no work. A redo that raced past the probe conflicts on the absence condition.

Properties:
- **Cost:** one `RelocateLegacyRows` request per 32 rows of each owner group, bounded by L's one page per tick. P's leader does the presence check and the TTL-anchor reads locally.
- **Repeated probes.** The work is not done once per row while L's copy exists. After the rewrite, the copy stays in L, and every later pass re-probes it only to learn that P now holds a version. That recurring traffic lasts until M3 removes the copy. §5 budgets it, and M2 and M3 ship together so the recurrence is short-lived.
- **No new scans.** L's leader is already scanning L.

### C. Evacuate stale legacy copies

Once P holds a version of the **metadata** key, L's copy of that row is shadowed for good. Compaction keeps the latest version at or below the horizon, tombstones included (`TestMVCCStore_Compact_Delete`), so the fallback never reaches L's copy again. Deleting it costs nothing in correctness, and the migrator stops rescanning and re-probing it.

This does **not** let the dual-route fallback be retired. A collection created before user-key routing may also have field, member, score or entry rows that live only in L. Evacuating its metadata row moves none of them, and a point read such as `HGET` still needs the legacy route to find them. Retiring the fallback needs a separate migration of every row family, with its own gate. That is out of scope here (§2).

**Enumerating the candidates.** The rows M3 deletes are exactly the ones the migrator's scan does not serve: `ScanGroupOwnedAt` skips a legacy-route row once its owner holds any version, a tombstone included. Scans that canonicalize wide-column rows drop it as well, because the owner's tombstone stops the point-read fallback. M3 therefore reads L's raw stored page, `scanRouteAtForwardStoredPage` without the point-read pass, and keeps legacy-route rows whose owner, a group other than L, holds a version at the read timestamp. Rows shadowed by a tombstone at P are candidates like any other.

The delete is a write into **L** at the raw key, issued by L's leader, so it is compliant in every TSO mode. However, the normal dispatch path routes the raw key to P by its user key, and **a plain `Del` would delete P's copy**. Option C needs an explicit-group operation: "remove this raw key from group L, if L's route still covers it". `ShardStore` has explicit-group reads (`GetGroupAt`, `ScanGroupAt`) but no explicit-group write.

**The removal is a purge, not a tombstone.** An ordinary delete leaves a tombstone in L stamped after P's version. If the user-key range later moves from P back to L, the migration imports P's versions with their original commit timestamps and merges them with L's data by newest version. L's newer tombstone then shadows the imported metadata, and the collection appears deleted after cutover. M3 therefore proposes a replicated `PurgeShadowedVersions(rawKey)` entry in L. Applying it removes L's versions of that raw key physically and writes nothing, so a later import is never shadowed.

Removing versions changes what a read below P's earliest version would see: such a read falls back to L and would find nothing instead of L's copy. Each replica's own retention floor (`minRetainedTS`) cannot guard this, because floors are local and differ. A lagging replica that later becomes leader would still accept a read between its floor and P's earliest version, and that read needs L's copy. M3 therefore first establishes a **replicated read floor** in L:
- L's leader proposes `RaiseReadFloor(T)`, where T is at most its own `minRetainedTS` and at most the oldest replicated backup pin in L (`ApplyPinWithDeadlineForGroup`).
- Every replica records T when it applies the entry. From then on, every read in L below T fails with `ErrReadTSCompacted`, on that replica and on any future leader.
- The floor only rises.

This only makes reads fail earlier than a lagging replica's local floor would, which the retention window already allows.

Only after the floor has applied does L's leader propose purges, and only for rows whose owner's earliest version is at or below T. A read that could still fall back to L is then below T and refused everywhere. The batched probe M3 uses must return that earliest version timestamp along with presence. This needs a new response field, which old servers leave unset, so a row whose probe does not report it is not purged. Replicas apply both entries unconditionally, which keeps apply deterministic.

**Both new entry types ship in M1b.** `RaiseReadFloor` and `PurgeShadowedVersions` are FSM changes like the absence condition. Their decoders and apply logic are part of M1b, so M1b's capability marker, downgrade fence and admission rules cover them. A replica running an M1b binary can apply every entry an M3 leader proposes. A separate M3 marker would otherwise be needed, and is not.

**M3 progress is checkpointed within the page, like M2's.** A raw page full of shadowed rows needs a presence probe and a delete per row. Done one row at a time, that could exhaust the shared 5s tick before the page ends, and every later tick would restart on the same page. M3 therefore:
- probes in batches per owner group (`RawLatestCommitTS` with a `key_batch`), issued concurrently across groups;
- purges in batches of at most 32 keys. Every key in a batch lives in L, so one explicit-group `PurgeShadowedVersions` entry by L's leader covers it;
- keeps its own cursor per plan and advances it only over the longest contiguous prefix of settled rows. A row is settled when its purge committed or it is not a candidate;
- runs as its own entries in the TTL-inline plan rotation, not after the M2 plans in a fixed order, so M2's recurring probes can never use up every tick before M3 starts (§4).

**Tick slot.** The plan rotation alone is not enough. Today `DeltaCompactor.SyncOnce` runs the delta-compaction handlers (`compactBackgroundHandlers`) first and calls `migrateTTLInlineOnce` only if the shared 5s budget is still live. Under sustained delta-compaction load, every TTL-inline plan is skipped, M2's and M3's included, and so is the migrator's paging that M1 counts. M1 therefore merges the two loops into one top-level rotation: the delta handlers and the migration plans take turns from one cursor, and a tick that runs out of budget hands the next tick the entry after the last one it ran. A busy delta handler then delays migration by at most one rotation and can no longer starve it. The change also covers bootjp/elastickv#1278's existing plans, which have the same exposure.

The cursor is in memory, like the existing plan cursors. A leader change restarts the pass, which redoes only idempotent work: purging an absent row is a no-op.

**Why not delete in the relocation transaction (B′).** L's leader could relocate into P and delete L's copy in one transaction, which would save M3's second probe per row. That transaction writes two groups, so it is a cross-group 2PC with the same blockers as A′:
- its commit timestamp needs the unfinished A0/A0b 2PC allocation, or Phase D;
- it needs a 2PC participant that writes an explicit group.

A separate single-group delete by L's leader is compliant in every TSO mode and needs no 2PC. The price is one batched probe per owner group per page in the pass after the rewrite.

### D. Measure first

Measurement alone migrates nothing, but it tells operators whether options A to C matter for their cluster and gives Phase 3 a concrete gate. Three forms:
- **D1, background counters.** On each group's leader, count the legacy-route rows the scan serves or skips, by owner group and reason, plus a "full pass finished" signal per plan. No extra reads (M1).
- **D2, one-shot audit.** An admin RPC on each group's leader runs the same classification once over the four meta ranges at one `readTS`, paging from start to end with no rewrites. It returns counts by bucket and owner group. It reads the whole legacy range once, locally, and finishes in minutes rather than a background pass spread over many 30s ticks. A run that long must hold its snapshot:
  - **Read pin.** The run pins its `readTS` in the node's `ActiveTimestampTracker` (`Pin`, released when the run ends or fails), which the FSM compactor honours. Otherwise compaction could drop a version a later page needs.
  - **One catalog version.** The run records the catalog version at its start and classifies every page under it. If the catalog moves before the run ends, the run is discarded and reports "restarted", the same rule M1 applies to a background pass. Otherwise pages classified under different route maps could add up to a false zero.
  - **Leadership.** A leadership change of the group mid-run discards the run too.

  A discarded run publishes nothing, so it can never feed M4 or a decision to defer M2. Like D1 it makes no remote probes, so rows whose owner is led elsewhere are counted without the "holds a version" split. With `--probe`, it adds batched presence probes per owner group. Those probes read P, which the audit's local pin does not protect: P's retention horizon can pass the audit's `readTS`, and a probe at that `readTS` would fail with `ErrReadTSCompacted`. So the probes do not use the pinned `readTS`. Each asks P's leader at a fresh read timestamp of its own. Presence only grows: compaction keeps the latest version at or below the horizon, tombstones included, so once P holds a version it keeps one. A later probe can therefore only move a row from "P holds nothing" to "P holds a version", never back. The split is informational. Neither M4 nor the M2-deferral rule uses it, since both require the whole owner-led-elsewhere bucket to be zero.
- **D3, offline count.** `cmd/elastickv-snapshot-decode` already reads a group's `.fsm` files. Counting legacy-route rows also needs the route catalog, which lives in the default group's snapshot. This touches no live node, but the snapshot is only as current as its last checkpoint, and no snapshot can see whether a remote owner holds the key.

M2 can wait only while no row needs a relocation that the same-node path could miss. A zero owner-led-elsewhere count says only where leaders sit now. If rows are still served (owner led locally and holding nothing) and P's leadership later moves away from L's node, those rows become owner-led-elsewhere rows: the same-node path skips them, and M3 cannot delete them because P is empty. So M2 may be deferred on a cluster only once D1 or D2 reports zero in both the owner-led-elsewhere and the served buckets, and the decision is re-checked at every published M1 snapshot. If either bucket turns nonzero, M2 ships. M4 enforces the same thing, because it requires both buckets to be zero. M3 is still needed in that case. It evacuates the copies the same-node path of bootjp/elastickv#1278 leaves in L, and M4's "owner led locally and holds a version" bucket must reach zero. So M3 would ship without M2, but never without M1b: the absence guard on the same-node path is what keeps those relocations from overwriting a user write that landed below `readTS`.

## 4. Proposal

| Milestone | Change | Gated on |
|---|---|---|
| M1 | **Measure.** Per-group-leader counts of the legacy-route rows the scan serves or reports as skipped (§3 B), split into: served (owner led locally, holds nothing), owner led locally and holds a version, owner led elsewhere, deferred by staged visibility. Without a remote probe, M1 cannot tell whether a remote owner holds the key; M2 adds that split. Counts accumulate per pass of each plan (one plan per family: hash, set, zset, stream). When that plan's cursor wraps, they are published atomically with the pass's completion epoch and the catalog version the pass ran under. A pass that sees the catalog version change restarts. A leadership change discards the in-progress pass and invalidates the group's published snapshots, since a new leader starts new passes. Expose the published snapshots, keyed by group and plan, as metrics and in an admin RPC. The same RPC runs the one-shot audit (§3 D2). M1 also puts the migration plans into the compactor's top-level rotation (§3 C, "Tick slot"), since its counts come from the migrator's own passes. | bootjp/elastickv#1278 |
| M1b | **Absence guard.** The apply-time absence condition (§3 B) as a new mutation field, plus the decoders and apply logic for M3's `RaiseReadFloor` and `PurgeShadowedVersions` entries (§3 C), with one capability marker on every voter and learner, the downgrade fence in each replica's `raft-engine` marker, and the refusal of replicas that lack the capability. It applies to every relocation, starting with the same-node path of bootjp/elastickv#1278, which also gets the forced `Put`. It needs no cross-leader forwarding and ships on its own. | M1 |
| M2 | **Option B.** L's leader takes the owner-led-elsewhere rows its scan reports as skipped and sends them to each owner group's leader in `RelocateLegacyRows` requests of at most 32 rows, concurrently across owner groups. P's leader checks presence, reads the TTL anchors, builds the rewrite and dispatches it as the leader of its own keys. Each transaction is bound to the catalog version its owners were resolved under, and is repartitioned or refused once the catalog moves. A relocation forces the `Put` when the owner holds nothing, and carries M1b's apply-time absence condition (§3 B). The plan cursor advances only over the contiguous prefix of settled rows. It extends M1's counts with "owner led elsewhere and holds a version". Disabled, with the row skipped as today, until P has replicated M1b's capability marker (§3 B). | M1; M1b; the `RelocateLegacyRows` RPC |
| M3 | **Option C.** By L's leader, in a later pass: enumerate L's raw page and purge L's metadata copy with an explicit-group `PurgeShadowedVersions` entry, which writes no tombstone. It purges when the current owner is a group other than L, holds a version, and that version is at or below L's replicated read floor (`RaiseReadFloor`, proposed first), at the purge's route version. Ships together with M2, which otherwise re-probes every rewritten row on every pass. M3's plans (one per meta family and legacy group) join the same flattened rotation as the TTL-inline plans (`migrateTTLInlineOnce`, `ttlInlinePlanCursor`), so an M2 plan that uses up a tick's budget hands the next tick the plan after it, and M3 gets one page per full rotation. Otherwise a fixed order would let a recurring M2 workload starve M3, and since M2 leaves every row in L, the design would never converge. If M1 shows that M2 can wait (§3 D), M3 ships without M2 for the copies the same-node path leaves, but always after M1b. | M1; M1b; an explicit-group delete path |
| M4 | **Legacy-route gate for Phase 3.** For every group and every plan, the latest published M1 snapshot must meet all of these:<br>• it is valid and has a completion epoch from after the rollout;<br>• it reports zero in every bucket: owner led elsewhere (holding a version or not), served but unmigrated, owner led locally and holding a version, and deferred by staged visibility. A nonzero locally shadowed bucket means M3 has not yet evacuated rows whose owner shares L's leader;<br>• it carries the same catalog version as every other gated snapshot, and that version is still current.<br>Counts, epoch and catalog version come from the same pass, so in-progress zeroes after a reset never satisfy the gate. A row moved between groups by a catalog change cannot hide between two passes either. This is **one additional** Phase 3 condition, not the whole gate. The TTL-inline design's other prerequisites still apply: every node has migrated, and legacy bare string keys, which no prefix scan covers, have been ruled out or rewritten. | M1 and M3; M2 wherever M1 reports owner-led-elsewhere rows |

Option A is rejected. It restores the cost profile a36dac7f removed and never converges, because the copies it reads are never removed. A′ converges, but it still reads every row about N times per pass, and it needs the unfinished cross-group 2PC timestamp work or Phase D.

## 5. Cost

Per tick (30s, shared 5s budget, at most 512 physical rows per plan page), with N nodes, G groups, and G_L groups holding legacy rows:

| Option | Who scans | Remote reads per tick, cluster-wide | Writes per tick | Converges |
|---|---|---|---|---|
| A | every node, each legacy group it does not lead | up to 4 · N · G_L page RPCs (≤ 2,048 · N · G_L rows) | per useful page: ≤ Σ_P ⌈n_P / 32⌉ ≤ 16 + G relocation txns, one owner group each | no: L's copies stay, so the cost is permanent |
| A′ | as A | as A, until the range is empty | as A (≤ 16 + G per page), each a cross-group 2PC writing L and one P | yes, after about N · B row reads in total |
| B (M2) | L's leader only, its existing pages | per page: ≤ Σ_P ⌈n_P / 32⌉ ≤ 16 + G `RelocateLegacyRows` requests; P's leader reads presence and TTL anchors locally | per page: one one-phase txn per request, each in one owner group | yes, with M3 |
| C (M3) | L's leader only, its raw page | per page: ≤ min(G, 512) batched probes | per page: ≤ 16 explicit-group purges, all in L | yes: each row is purged once |
| D1 (M1) | L's leader only, its existing pages | none | none | not applicable |
| D2 | each group's leader, once | none (or batched probes with `--probe`) | none | not applicable |

For A, A′, B and C, the 16 is ⌈512 / 32⌉, where n_P is the number of a page's rows owned by group P. The extra G comes from a page split across owner groups, each owner group's last chunk being partial. A page whose 512 rows all belong to one remote owner therefore costs 16 `RelocateLegacyRows` requests, not one. C's probes are not chunked: one batched probe carries up to 1,025 keys, so C needs at most one per owner group per page. Unlike A and A′, neither depends on N: a node with no legacy group does no extra work.

**Convergence.** B and C keep the existing migrator's pace: one legacy group drains at most 512 rows per meta family per 30s tick, about 61,000 rows per family per hour. A backlog of 1,000,000 hash meta rows therefore takes at least about 1,954 ticks, roughly 16 hours, plus the M3 pass behind it, and longer if ticks time out and back off for 1m. The design accepts that pace rather than adding a faster path; the one-shot audit (D2) sizes the backlog first, so operators know the expected duration before relying on M4. The probes and forwards run under the tick's context. `proxyForwardTimeout` (5s) equals the whole tick budget, so it never fires first: a slow owner group uses up the tick, and the settled-prefix checkpoint keeps the work already finished.

- **M1:** no remote reads. The counters classify rows the page already reads, by an in-memory route lookup plus, for a locally led owner, the local version probe bootjp/elastickv#1278 already performs. Whether a remote owner holds the key is left to M2, which issues the probe anyway.
- **M2:** per page (≤ 512 rows per plan per tick), one `RelocateLegacyRows` request per 32 of each remote owner group's rows, issued concurrently across owner groups. P's leader serves each with local reads only: one presence check and up to two TTL-anchor reads per row, all in its own store. Batching, and building the rewrite on P, are convergence requirements, not optimisations: sequential per-row RPCs, including the TTL-anchor reads `buildElems` makes, could exhaust the tick on one page every time.
- **M2 before M3 evacuates:** every pass sends each rewritten row still present in L to P's leader again, which answers "already held". That is up to Σ_P ⌈n_P / 32⌉ `RelocateLegacyRows` requests per page, the same as the first pass, recurring for the backlog's size until M3 removes the copies. With M2 and M3 shipped together, a row is re-probed at most until the pass after its rewrite. If M3 is disabled or failing, M1's "owner holds a newer version" counter shows the recurring set.
- **M3:** per raw page, one batched presence RPC per remote owner group, plus one explicit-group purge entry per 32 candidates. Each metadata row is evacuated once. After evacuation, L's legacy metadata range is empty and the TTL-inline plans read nothing there. Collection field, member, score and entry rows stay in L (§3 C).

## 6. Correctness

- **OCC:** the rewrite's `StartTS` is the page's `readTS`. A user write to the key at P after `readTS` makes the rewrite conflict, and the row is reconsidered on a later pass.
- **Writes below `readTS` (G0 across groups):** `readTS` is the maximum applied watermark over all groups. P can therefore accept a user write with a commit timestamp at or below `readTS` after the probe has answered, and `latest > StartTS` would not catch it. The apply-time absence condition (§3 B) rejects the relocation whenever P holds any version. That closes this case for relocations without a per-row read-index round trip, and without waiting for A0's cross-group fix. Rewrites of rows P already owns keep plain OCC, and stay exposed to G0 like every other transaction until A0 lands.
- **Presence-check races:** if P gains a version between the probe and the commit, the rewrite conflicts with it, because it writes the same key. If P's leadership moves, the probe is answered only by a current leader (it reports "cannot answer" otherwise), and the forward is re-routed or fails.
- **Timestamps:** never issued by L's leader for P's key. P's leader dispatches every relocation of its own keys: its HLC stamps them in legacy mode, and the TSO under Phase D.
- **Staged visibility:** rows whose route, at L or P, is under staged visibility stay deferred, as in bootjp/elastickv#1278. M1 counts them, so the M4 gate does not pass while any remain.
- **Catalog changes during M2:** a batch whose owners were resolved under an older catalog version is repartitioned before dispatch, or refused by P's leader. No relocation therefore writes a group other than its dispatching leader's own.
- **Catalog changes during the gate:** each M1 snapshot carries its pass's catalog version, and M4 requires one current version across all of them. A row moved into a group after that group's pass completed fails the gate until every pass has run again under the new version.
- **M3 safety:** delete L's copy only when, at the delete's own read timestamp and route version:
  - the key's current owner is a group other than L;
  - that owner holds a version of the key;
  - L's route still covers the raw key;
  - the owner's earliest version is at or below L's replicated read floor, which every replica and future leader enforces, so no served read can still fall back to L's copy.

  The removal is a purge, never a tombstone, so a later move of the range back into L cannot be shadowed by it.

  The explicit-group purge carries that route version and refuses if the catalog has moved past it. A catalog change can make L the owner, and then P's "version" would be the very row being deleted. A delete decided against an older catalog could remove the collection's only current metadata copy.

## 7. Test plan

- `kv`: a real split `ShardStore`, with P behind a `followerProxyEngine` whose leader is a fake RawKV server. Cover the batched probe (for M3) answering "none", "has a version" and "cannot answer". Cover `RelocateLegacyRows` on P's leader: relocated, already held, conflicted, and refused on a catalog move or on a non-leader. The commit timestamp comes from P's leader in legacy mode and from the TSO under Phase D. Building the rewrite reads the TTL anchors only from P's own store: a counting store on L must see no `redisTTLKey` reads for cross-leader rows.
- `kv`, batched probe through M2's client: per-key answers of `RawLatestCommitTS` with a `key_batch` match the single-key probe for a mixed batch (absent, visible, tombstoned, newer than `ts`), and a non-leader reports "cannot answer" for the whole batch. The existing S3 auxiliary probe tests cover the server; this covers the migrator's use of it per owner group.
- `kv`, absence condition (M1b), written first as a failing reproduction in the style of `TestApplyOrder_GlobalSnapshotMissesLaggingGroupWrite`:
  - two groups; a user write to P's key at `c1` is parked before its proposal while another group's write at `c2 > c1` applies;
  - the migrator takes `readTS = c2`, the probe reports P empty, and the parked write lands on P;
  - the relocation must then conflict, and P must keep the user's value.

  Before the condition exists, the test shows the user's write overwritten. Cover the same-node path too. A relocation applied before a parked user write must make that write conflict. A group with a voter or a learner lacking the capability must keep M2 off, and once P's capability marker is in place, adding a replica that lacks the capability is refused. A replica whose `raft-engine` marker carries the fence, restarted on a binary from before M1b, fails at startup. A learner added by a conf change that applies between the leader's capability check and the marker makes the marker apply as a no-op. A node that joins after activation writes the fenced marker into P's new directory before its first snapshot restore.
- `adapter`: compactor tests where L and P have different leaders. The legacy row is migrated into P exactly once; a concurrent user write at P wins. One page holding rows of two remote owner groups produces one transaction per owner group. With interleaved owners (`P1, P2, P1`), a tick whose budget runs out after P1's batch commits but before P2's resumes behind the settled prefix and never skips the P2 row. A group with more than 32 rows on the page commits in several transactions. An already-inline legacy row whose owner holds nothing is copied into P, on both the cross-leader and the same-node path. A split of P between grouping and dispatch repartitions the batch, and P's leader refuses a forward carrying a key it no longer owns.
- `kv`: the group scan reports every skipped legacy-route row with its owner group and reason (owner led elsewhere, owner holds a version, staged) and does not serve it.
- M1:
  - a reset pass never publishes zeroes before its completion;
  - a leadership change invalidates the snapshots;
  - each plan publishes its own snapshot;
  - a catalog change during a pass restarts it.
- Plan rotation: with M2 plans whose pages always use up the tick's budget, M3's plans still each run once per rotation, and the backlog of evacuable rows shrinks.
- Top-level rotation (M1): with a delta-compaction handler that always uses up the tick's budget, every migration plan still runs once per top-level rotation, and M1 still publishes its pass snapshots. Before the change, the same setup runs no migration plan at all.
- M1b ordering: with M1b and M3 but no M2, a same-node relocation whose owner received a parked user write below `readTS` conflicts, and M3 then finds the owner holding the user's version and evacuates only L's copy. A build with M3 and without M1b must not be possible to enable.
- M3 progress: a tick whose budget runs out mid-page resumes behind the settled prefix and still reaches the rows after it. A page with more than 32 candidates deletes them in several transactions.
- M4: the gate fails when one plan of a group still has rows while the others report zero, and when only the "owner led locally and holds a version" bucket is nonzero. It also fails when snapshots carry different catalog versions, or a version that is no longer current. It does not replace the bare-string prerequisite.
- M3:
  - an explicit-group purge removes L's copy only, never P's, and writes no tombstone;
  - it refuses after the route moves, and when the current owner has become L;
  - rows shadowed by a tombstone at P are found by the raw enumeration and evacuated;
  - after the purge, moving the user-key range from P back to L imports P's metadata and the collection stays readable (reproduce first with an ordinary delete, which shadows the import);
  - a row whose owner's earliest version is above L's replicated read floor is not purged until the floor passes it;
  - after `RaiseReadFloor(T)` applies, a follower whose local `minRetainedTS` is below T refuses reads below T, and keeps refusing them after it becomes leader;
  - an M3 leader's purge applies on a replica running the M1b binary, and the M1b marker does not activate on a group with a replica that cannot decode both new entry types.
- M1 one-shot audit (D2): on a real split `ShardStore`, it returns the same buckets as a completed background pass over the same data, and it writes nothing. It holds an `ActiveTimestampTracker` pin at its `readTS` for the whole run and releases it on success and on failure; a compaction run during the audit removes no version the audit still reads. A catalog change or leadership change mid-run discards the run and publishes nothing. With `--probe`, a compaction on P past the audit's `readTS` does not fail the run, because the probes read P at fresh timestamps.
- Jepsen: the Redis workloads with a seeded legacy-route backlog and leader shuffles, checking linearizable reads of the affected keys during migration and evacuation.
  - The CI workloads write only inline-format data, so they never exercise this path (bootjp/elastickv#1278 notes the same). Seeding needs the explicit-group write M3 introduces, behind a test-only flag, because any routed write of a legacy key lands in P.
  - Place L's and P's leaders on different nodes (`cmd/raftadmin` leadership transfer), then run hash, set, zset and stream operations with TTLs on the seeded keys under the partition and leader-shuffle nemeses.
  - Check that reads, including `TTL`, stay linearizable while M2 and M3 run, and that every M1 bucket ends at zero.
  - Also run the existing `elastickv.redis-workload` and `elastickv.redis-zset-safety-workload` to show no regression.

## Decisions

- **The cross-leader case is handled outside bootjp/elastickv#1278.** That PR keeps leaving such rows untouched, and Phase 3 stays blocked until this design lands. (Maintainer decision, 2026-10-17.)
- Proposed, pending review: reject option A. Implement option D first, then B and C together. B builds and dispatches each relocation on the owner group's leader, so it needs neither A0b nor Phase D.
- Proposed, pending review: reject A′. Deleting in the same transaction makes owner-side scans converge, but they keep the N-fold read cost, and the cross-group 2PC needs commit-timestamp work A0/A0b has not finished.
- Proposed, pending review: evacuation is a separate single-group delete by L's leader (M3), not part of the relocation transaction (B′). It stays compliant in every TSO mode and needs no 2PC, at the cost of one probe per row in a later pass.
- Proposed, pending review: a relocation is guarded by an apply-time absence condition, not only by OCC at `readTS`. This closes G0 across groups for relocations, on both the cross-leader and the same-node path, without a per-row fence.
- Proposed, pending review: M1 ships first, with the one-shot audit (D2) and the top-level rotation. Its results decide whether M2 is needed on the clusters that matter. M3 is needed either way.
- Proposed, pending review: P's leader builds and dispatches cross-leader relocations (`RelocateLegacyRows`), from the raw rows L's leader sends. Building them on L's leader would cost up to two remote TTL-anchor reads per row and would need A0b or Phase D for the commit timestamp.
- Proposed, pending review: M2 is deferred on a cluster only while both the owner-led-elsewhere and the served buckets are zero, re-checked at every published M1 snapshot, because a leadership move turns served rows into cross-leader rows.
- Proposed, pending review: the absence guard is its own milestone (M1b), independent of cross-leader forwarding. M3 never ships before it, because the same-node path already relocates rows that M3 then deletes from L.
- Proposed, pending review: retiring the dual-route fallback is not a goal of this design. It needs a migration of every row family of pre-routing collections, not only their metadata.

## Open questions

1. Should `RelocateLegacyRows` be its own `Internal` RPC, or a mode of `Forward` that carries rows instead of mutations?
2. Where does the explicit-group purge belong: a `ShardStore` method behind a coordinator capability, or a dedicated internal RPC?
3. Should the absence condition be a field on `pb.Mutation`, or a transaction-level list of keys that must be absent? The field is simpler; the list keeps the condition out of every other mutation path.
4. Should the replicated read floor be group-wide, as proposed, or scoped to the purged raw keys, so that other reads in L keep their full retention window? The alternative to purging at all is teaching the import path to ignore tombstones written by evacuation.
