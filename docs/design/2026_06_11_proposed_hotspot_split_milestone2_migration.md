# Hotspot Shard Split — Milestone 2: Migration Plane

Status: Proposed
Author: bootjp
Date: 2026-06-11

Parent: [2026_02_18_partial_hotspot_shard_split.md](2026_02_18_partial_hotspot_shard_split.md).
M1 (control plane) is as-built in [2026_02_18_partial_hotspot_split_milestone1_pr.md](2026_02_18_partial_hotspot_split_milestone1_pr.md).

## 1. Background

M1 landed the control plane:

- `distribution/catalog.go` — durable `RouteDescriptor` records in the default Raft group's MVCC state.
- `distribution/engine.go` — in-memory route cache + history ring (consumed by Composed-1 in `kv/fsm.go`).
- `distribution/watcher.go` — applies versioned catalog snapshots into the engine.
- `proto/distribution.proto` — `SplitRange` RPC; M1 implementation splits a range **inside the same Raft group** only (no data movement, no target field on the wire).
- `RouteState` already reserves `WRITE_FENCED`, `MIGRATING_SOURCE`, `MIGRATING_TARGET` for M2 (`distribution/catalog.go:51-58`).

Composed-1 cross-group commit guard (PR #927) and the routeKey normalization fix (PR #932) close the apply-time hazards that an unsafe M2 cutover would trip. M2 builds on top of both.

M2 does **not** ship hotspot detection or auto-split scheduling (M3).

## 2. Goals and Non-Goals

### 2.1 Goals

1. `SplitRange` can move the right child of a split to a different Raft group, atomically with respect to readers and writers.
2. The migration is **resumable**: a leader change during BACKFILL / DELTA_COPY / CUTOVER recovers without operator intervention or data loss.
3. The FENCE phase rejects writes on the moving range with a retryable error code (so adapter callers retry transparently).
4. No write loss, no MVCC visibility regression, no Composed-1 violation at the cutover boundary.
5. The same-group `SplitRange` path from M1 keeps working byte-for-byte — no behavior change for callers that omit the new fields.

### 2.2 Non-Goals

1. Automatic hotspot detection / scheduler — M3.
2. Online range merge (inverse of split) — out of scope for M2.
3. Adding or removing Raft groups dynamically. Target group must already exist in `--raftGroups`.
4. Cross-region migration. M2 assumes all groups are reachable on the cluster gRPC mesh.
5. Per-tenant migration policy, throttling, or QoS.

## 3. Data Model Additions

### 3.1 SplitJob

A split job is durable state for the migration. Persist under reserved keys in the default group, alongside the route catalog.

Key layout:

- `!dist|meta|next_job_id` — uint64, allocator for `job_id`.
- `!dist|job|<job_id>` — encoded `SplitJob` record (in-flight + terminal until GC).
- `!dist|jobhist|<terminal_at_ms_be>|<job_id>` — moved here at GC time for short-term audit; periodically truncated.

`SplitJob` fields:

| Field | Type | Notes |
|---|---|---|
| `job_id` | uint64 | Allocated from `next_job_id`. |
| `source_route_id` | uint64 | Route being split. |
| `split_key` | bytes | Logical (routing-key) boundary. |
| `target_group_id` | uint64 | Where the right child lands; equals source group for same-group split. |
| `phase` | enum | `PLANNED` / `BACKFILL` / `FENCE` / `DELTA_COPY` / `CUTOVER` / `CLEANUP` / `DONE` / `FAILED` / `ABANDONED`. |
| `retry_phase` | enum | Durable restart target used only while `phase == FAILED` and before CUTOVER. Set atomically when the migrator moves a job to `FAILED`, choosing the last safe idempotent phase to replay (`BACKFILL`, `FENCE`, or `DELTA_COPY`) from the phase that failed and the side effects already acknowledged. `NONE` outside `FAILED`. `RetrySplitJob` must CAS back to this field's value; it must not infer a phase from cursors, `last_error`, or wall-clock diagnostics after a restart. |
| `snapshot_ts` | uint64 | **Provisional** HLC commit-ts pinned at BACKFILL entry — the ts BACKFILL iterates at, not the DELTA_COPY lower bound. Selected by §6.1.0 step 1 after the source-group migration write tracker is armed: `min(HLC.Next(), snapshot_min_admitted_ts - 1)` over current in-flight prepares / tracker rows. The authoritative DELTA_COPY lower bound is `delta_floor`, finalised only after the fence (codex round-14 P1 line 405; codex round-15 P1 — fence before boundary). |
| `snapshot_min_admitted_ts` | uint64 | **Running** minimum `commit_ts` of migration-window writes admitted for the moving range — keyed on any point mutation / locked key whose `routeKey()` falls in `[routeStart, routeEnd)`, and on any range mutation whose route-key footprint intersects it. It covers raw requests (`handleRawRequest` at `r.Ts`, including `Op_DEL_PREFIX` range tombstones), one-phase txns (`handleOnePhaseTxnRequest` at `TxnMeta.CommitTS`), and prepared txns on **any** locked key (primary or secondary; codex round-16 P1). Sampled from the source-group migration write tracker armed before `snapshot_ts` selection (§6.1.0 step 1) plus the live §3.2a.0a `!txn|lock|` drain for backward-compat locks that predate the tracker. It is **not** computed from every historical committed MVCC version in the range; old retained history that BACKFILL already copied must not lower the floor. Monotone-down; persisted on each advance. Final once `post_fence_drain_completed = true` — no new raw/one-phase write, `DEL_PREFIX`, or prepare can land on any key in the moving range after the fence, and every pre-fence prepare has either resolved or aborted. `0` means no migration-window write was observed. |
| `write_tracker_armed` | bool | Default-group witness that the source group has durably applied `ArmMigrationWriteTracker(job_id, route_start, route_end)` before BACKFILL opens. The tracker records every later write whose point or range footprint routes into the moving range: raw writes and one-phase txns at apply time, and prepares at admission time so same-tick land-and-resolve cases that a polling drain could miss are still represented. A restart must not pick `snapshot_ts` or start BACKFILL until this is true. |
| `delta_floor` | uint64 | Authoritative DELTA_COPY lower bound, fixed at the §3.2a post-fence-drain → step-2 boundary as `min(snapshot_ts, snapshot_min_admitted_ts - 1)` (§6.1.0 step 3). DELTA_COPY exports `(delta_floor, fence_ts]`. Ensures every migration-window low-ts write with `commit_ts ≤ snapshot_ts` (raw point write, `DEL_PREFIX` range tombstone, one-phase, or prepared; caller-supplied / lagging-clock ts accepted while the source was still `Active`) satisfies `commit_ts > delta_floor` and is captured. Persisted so a DELTA_COPY resume uses the same lower bound. |
| `post_fence_drain_completed` | bool | True once §3.2a step 0b's post-fence intent-lock drain has returned empty (after step 1's voter-ACK barrier). Restart-safe: a migrator restart between step 0b and step 2 (`fence_ts` pin) knows whether to re-run the drain (closes codex round-14 P1 line 171). |
| `fence_ts` | uint64 | HLC commit-ts pinned at FENCE → DELTA_COPY entry. |
| `cutover_version` | uint64 | Expected catalog version that the CUTOVER write will produce (`current_catalog_version + 1` under the CAS input). Fixed and persisted before `cutover_read_fence_state = ARMING`, then verified as the produced version when the CUTOVER CAS lands. Used by §7.2 read-fence and §4.2 GC. |
| `cutover_read_fence_state` | enum | `NONE` / `ARMING` / `ARMED` / `CLEARING` restart witness for §7.2.2e. The migrator first persists `ARMING` on the default-group SplitJob **before** proposing `ArmCutoverReadFence` to the source group, with the expected `cutover_version` already fixed. Once the source-group Raft log has applied the pending read-fence barrier for the moving range and expected version, the migrator CASes the state to `ARMED`. A restart at `ARMING` re-proposes/probes the source fence idempotently and then records `ARMED`; a restart at `ARMED` either completes the CUTOVER CAS with the same expected version or clears the source fence on abort. `CLEARING` is persisted before `ClearCutoverReadFence` so abandon/rollback also resumes idempotently. |
| `target_staged_readiness_state` | enum | `NONE` / `ARMING` / `ARMED` / `CLEARING` restart witness for §6.4 step 0. The migrator persists `ARMING` before proposing `ApplyTargetStagedReadiness(job_id, route_start, route_end, expected_cutover_version, migration_job_id)` to the target group, records `ARMED` only after the target Raft apply ACK, and clears through `CLEARING` after the default-group promotion-complete witness or on pre-CUTOVER rollback. This makes a target leader fail closed instead of serving live-only reads while its catalog watcher is behind the CUTOVER descriptor. |
| `cursor` | bytes | Opaque (raw_key, commit_ts) export position for BACKFILL / DELTA_COPY resume — addresses an MVCC version, not a raw key, so a hot key with many committed versions can be chunked safely across batches (see §6.1.1). |
| `max_imported_ts` | uint64 | Default-group witness for the largest `commit_ts` ever included in a non-empty ack'd import batch for this job. Empty scan-progress chunks (§6.2) advance bracket cursors but do not change this value because they contain no timestamp. The target group also persists the same full HLC value locally under `!migstage|hlc_floor|<job_id>` (§6.2.1) so restart/snapshot restore can re-observe the logical half before serving post-CUTOVER writes. |
| `target_promotion_done` | bool | Default-group witness that the target group has durably completed §6.4 promotion using its target-local `PromotionState`. This is **not** a cursor; promotion progress is owned by the target Raft group so a default-group crash cannot skip target-local staged rows. |
| `promotion_completed_ts` | uint64 | HLC commit-ts of the default-group CAS that cleared `staged_visibility_active` after the target reported promotion complete. Diagnostic / audit only; the job remains in CLEANUP until source/target cleanup ACKs are durable and the final CLEANUP → DONE history move lands. |
| `fence_catalog_version` | uint64 | Catalog version produced by the FENCE catalog write. It labels the `ApplyMigrationWriteFence(job_id, route_start, route_end, fence_catalog_version)` tuple and lets the source-group durable fence record prove which catalog transition it protects; it is **not** itself the ACK condition. Set at FENCE entry. |
| `fence_ack_cursor` | bytes | Opaque encoding of the per-voter ACK progress for §3.2a's fence-apply barrier. Each ACK means that voter has applied the source-group Raft entry that persists `!migfence|<job_id>` for the exact `(route_start, route_end, fence_catalog_version)` tuple, optionally carrying the source Raft apply index/term for diagnostics. A watcher-only `catalog_version_applied >= fence_catalog_version` heartbeat is insufficient and must not advance this cursor. Restart-safe: a migrator restart resumes from the pending voter list and re-proposes/probes the durable fence record rather than re-polling completed voters. |
| `bracket_progress` | repeated `BracketProgress` | Per-family export bracket state for §6.3.1's parallel exports. Each entry carries `(bracket_id uint64, family uint32, export_phase enum, cursor bytes, done bool, scanned_bytes uint64, accepted_rows uint64, last_acked_batch_seq uint64)`. `bracket_id` is stable for the life of the SplitJob and forms part of the §6.1.1 importer idempotency key `(job_id, bracket_id, batch_seq)`. `last_acked_batch_seq` is monotone across the whole job, not reset per phase. When BACKFILL completes, the `BACKFILL -> DELTA_COPY` transition rewrites every bracket to `export_phase=DELTA_COPY`, clears `cursor`, `done`, `scanned_bytes`, and `accepted_rows`, and preserves `last_acked_batch_seq`; the first DELTA_COPY import uses `batch_seq = last_acked_batch_seq + 1`. This explicit phase boundary reopens exhausted BACKFILL brackets without reusing importer idempotency keys. A leader flap resumes the current phase of each bracket independently. `phase` advances from BACKFILL/DELTA_COPY only when every entry for that phase has `done = true`. |
| `source_retention_pin_ts` | uint64 | Source-group MVCC retention floor installed before BACKFILL opens (§6.1.0a). Initially `snapshot_ts`; once `delta_floor` is finalised it is lowered to `min(source_retention_pin_ts, delta_floor)` so DELTA_COPY's full `(delta_floor, fence_ts]` window remains readable. Released only after successful source cleanup or the abandon/FAILED cleanup path disarms the job. |
| `last_error` | string | Most recent transient failure (for operator diagnosis). |
| `started_at`, `updated_at`, `terminal_at` | int64 | Wall-clock ms; **diagnostic only**, must not be read by any ordering-sensitive logic (CLAUDE.md HLC rule). `terminal_at` is set when phase enters DONE or ABANDONED; FAILED keeps the job live and does not set a terminal history key. |

Encoding mirrors `RouteDescriptor` (a single-byte codec version prefix + protobuf body). Reserve a new codec constant `catalogJobCodecVersion = 1`.

Target-local promotion/readiness state is **not** stored in this default-group `SplitJob`. The target group owns small Raft-applied records under `!migstage|ready|<job_id>` with `(route_start, route_end, expected_cutover_version, migration_job_id, armed bool)` and under `!migstage|promote|<job_id>` with `(promote_cursor bytes, done bool, promoted_rows uint64, last_error string)`. `ApplyTargetStagedReadiness` creates the former before CUTOVER, and `PromoteStaged` updates the latter in the same target-group apply that copies/deletes staged rows (§6.4). The default group only records restart witnesses and the final promotion witness (`target_promotion_done = true`) after the target reports `done=true`.

#### 3.1.1 Job retention (bounded storage)

Without a retention policy the `!dist|job|*` namespace grows unboundedly across a long-lived cluster (gemini medium on PR #945). M2 ships these bounds:

- **Live cap**: M2 v1 allows at most `maxLiveJobs = 1` non-terminal SplitJob in the catalog at any time. `SplitRange` rejects a second request with `ErrTooManyInFlightJobs` while any job is live, including retryable `FAILED` jobs. This matches §8's single migrator goroutine and avoids under-specified interactions between overlapping source/target fences, HLC floors, staged-readiness barriers, and cleanup. M3 may raise the cap only after adding explicit conflict detection for overlapping route intervals, target readiness records, and cleanup cursors.
- **Audit retention**: terminal jobs (`DONE` and `ABANDONED`) are moved by the migrator from `!dist|job|<id>` to `!dist|jobhist|<terminal_at_ms_be>|<id>` after their cleanup obligations complete. `FAILED` is intentionally kept in the live namespace so `RetrySplitJob` / `AbandonSplitJob` can act on the full job body; it moves to history only after a successful retry reaches `DONE` or an abandon reaches `ABANDONED`. The history key sorts by terminal time so a single bounded prefix-scan trims oldest entries.
- **History bound**: `maxJobHistory = 1_000` total entries OR retention age `historyTTL = 7d`, whichever is tighter. The migrator runs a GC sweep at most once per minute on the default-group leader; it batch-deletes excess entries with a single Raft proposal.
- **Listing**: `ListSplitJobs` returns live + history (default cap 200 newest), with `since_terminal_at_ms` / `phase` filters; operators page through history via cursor.

Bounded both by count (worst case ≤ `1 + maxJobHistory ≈ 1k` records ≈ low-MB storage) and by time. The cap on live jobs also prevents `ListSplitJobs` enumeration from blowing the response.

### 3.2 RouteState transitions during a job

Same-group split (M1): `Active → (atomic CAS) → Active+Active`.

Cross-group split (M2) — **catalog stays single-source-of-truth, never holds an overlapping route**:

```text
catalog snapshot evolution                migrator action
============================              ====================
[ source.Active(full range) ]
        |
        |  PLANNED → BACKFILL              SplitJob created on default group;
        |                                  source-group write tracker armed
        |                                  before snapshot_ts is chosen; no
        |                                  catalog mutation. Target accepts
        |                                  imports into shadow keyspace.
        v
[ source.Active(full range) ]              BACKFILL: chunked export from
                                           source @ snapshot_ts; idempotent
                                           import into target's shadow space.
        |
        |  BACKFILL → FENCE                Atomic catalog write happens first:
        |                                  source's
        v                                  right child split out as a NEW
[ source.Active(left)           ]          RouteDescriptor with state =
[ source.WriteFenced(right)     ]          WriteFenced, raft_group_id =
                                           source. Catalog snapshot remains
                                           non-overlapping. Coordinator/FSM
                                           reject new writes/prepares on the
                                           fenced route; prepared-txn
                                           resolution is allowed only for
                                           existing tracked intents.
        |
        |  FENCE → DELTA_COPY              Migrator waits for source-side
        |                                  fence ACK (§3.2a) — every source-
        |                                  group voter has applied the
        |                                  WriteFenced route — and the post-
        |                                  fence drain returns empty, then
        |                                  finalises delta_floor (§6.1.0) and
        |                                  pins fence_ts. THEN copies
        |                                  (delta_floor, fence_ts] from
        |                                  source to target shadow space.
        v
[ source.Active(left)           ]
[ source.WriteFenced(right)     ]
        |
        |  DELTA_COPY → CUTOVER            Migrator first proposes a final
        v                                  target-local full HLC floor
[ source.Active(left)           ]          (`!migstage|hlc_floor|<job_id>`,
[ source.WriteFenced(right)     ]          SetPhysicalCeiling(ms)+Observe(ts))
                                           so the target HLC > every import
                                           (§6.2.1), applies the target-side
                                           staged-readiness barrier
                                           (§6.4 step 0), then arms the
                                           source-side cutover read fence
                                           (§7.2.2e). Only after the target
                                           can fail closed without staged
                                           metadata and the source can reject
                                           stale reads does the default group
                                           publish the atomic catalog write under
                                           CAS(expected_catalog_version):
                                             (a) source.WriteFenced(right)
                                                  → removed
                                             (b) target.Active(right) inserted
                                                  with raft_group_id=target
                                           Catalog snapshot still
                                           non-overlapping; version+=1
                                           atomically. `cutover_version`
                                           recorded in the SplitJob.
[ source.Active(left)           ]
[ target.Active(right)          ]
        |
        |  CUTOVER → CLEANUP               Source group's leader deletes
        v                                  moved user-data keys from its
[ source.Active(left)           ]          MVCC store after the read-fence
[ target.Active(right)          ]          grace window (§7.2). Cleanup walks
                                           the same disjoint bracket plan and
                                           routeKey filter as export; control
                                           prefixes are never migrated or
                                           cleaned as data.
        |
        |  CLEANUP → DONE                  Job moved to !dist|jobhist|<...>.
        v
[ source.Active(left)           ]
[ target.Active(right)          ]
```

Key invariant: **catalog snapshots never contain overlapping ranges**. The target shadow keyspace lives outside the catalog (BACKFILL / DELTA_COPY land into private `!dist|migstage|<job_id>|<raw_key>` keys on the target group's MVCC store; CUTOVER **does not** bulk-rename them — promotion is incremental and runs as a target-side background job after CUTOVER, see §6.4). This keeps `routesFromCatalog` + `validateRouteOrder` (`distribution/engine.go:496-527`) green throughout the migration — addresses codex P1 on overlapping routes (PR #945 review). RouteState `MigratingSource` / `MigratingTarget` are reserved but unused in M2; they remain available for a future merge / multi-stage migration design.

Reserved-control invariant: M2 cross-group migration rejects any moving interval that intersects a migration/control namespace: the default-group `!dist|` catalog namespace (including `!dist|meta|`, `!dist|route|`, `!dist|job|`, `!dist|jobhist|`, and staged data under `!dist|migstage|`), target-local `!migstage|` records (`!migstage|ack|`, `!migstage|hlc_floor|`, `!migstage|ready|`, `!migstage|promote|`), and source-local migration state (`!migwrite|`, `!migfence|`). Those keys are recovery metadata, not user data; if a split exported or cleaned them as ordinary rows it could corrupt another migration's cursor, HLC floor, target readiness proof, promotion state, or fence. `SplitRange` therefore returns `ErrReservedRange` before creating the SplitJob when the requested right child intersects any of these prefixes. This is a hard validation rule, not a best-effort cleanup filter.

The same reservation is enforced on the **data-plane mutation path**, not only at `SplitRange`. Before any user-visible `PUT`, `DEL`, raw `Op_DEL_PREFIX`, one-phase txn, or prepare can create an MVCC version or intent, the FSM runs a migration-control-prefix pre-gate over the mutation footprint. Any footprint intersecting `!dist|`, `!dist|migstage|`, `!migstage|`, `!migwrite|`, `!migfence|`, or another registered migration-control prefix is rejected with `ErrReservedRange` / a retryable adapter error. `Op_DEL_PREFIX ""` therefore cannot bulk-delete catalog or migration-control rows, and a user-supplied prefix like `!migstage|` cannot be treated as ordinary data. Internal maintenance code that needs to mutate those records uses typed catalog/migration APIs, not RawKV user-plane requests. This gate is intentionally independent of route ownership and `WriteFenced`: it runs even when no migration is active, so control-prefix safety is not conditional on a particular SplitJob.

The Composed-1 gate rejects an apply on the wrong group at the observed catalog version; CUTOVER bumps `catalog_version` so any straggler write from an unaware coordinator fails closed at apply time on either side.

### 3.2a Source-side fence-apply barrier — fence_ts must follow the source's actual write-reject point (closes codex round-6 P1 line 115)

Codex round-6 P1 on PR #945 (line 115): the FENCE catalog write commits on the default group, but the source FSM only learns about the new `WriteFenced` state through the watcher's asynchronous catalog snapshot apply (`distribution/watcher.go:11`, `:87-98`). If the migrator records `fence_ts = source.LastCommitTS()` and starts DELTA_COPY immediately after the FENCE catalog write, the source leader may still accept writes for the moved keys for one watcher-tick worth of time after the catalog commit — those writes get a `commit_ts > fence_ts`, fall outside DELTA_COPY's `(delta_floor, fence_ts]` window, and are silently left on the source at CUTOVER. Result: write loss after CUTOVER hands the routing to the target.

M2 fixes this with a **fence-first drain gate** on the FENCE → DELTA_COPY transition (closes codex round-18 P1 — new prepares must be gated before waiting for an empty drain):

0. **Publish `WriteFenced` before waiting for the drain to become empty.** The migrator does **not** wait for an empty prepared-txn drain while the source route is still `Active`; that can starve forever under steady transactional load because every resolved intent can be replaced by a new prepare before the next drain tick. Instead, `BACKFILL → FENCE` first performs the atomic default-group catalog write that splits out `source.WriteFenced(right)` and records `fence_catalog_version`. From this point onward, once the source-group durable fence barrier in step 1 has applied, new writes and new prepares for the moving range are rejected synchronously by `verifyRouteNotFenced`, including raw `DEL_PREFIX` requests whose prefix range intersects the moving route. Low-ts writes that land during the catalog/write-fence gap are still safe: the source-group migration write tracker from §6.1.0 step 1 records raw point writes, `DEL_PREFIX` range tombstones, and one-phase writes at apply time, and records prepares at admission time; the post-fence drain below waits for prepared txns to commit or abort before `fence_ts` is picked.

   Existing prepared txns are allowed to resolve during FENCE, but only through the narrow resolution lane in §7.1: `Phase_ABORT` is always allowed, and `Phase_COMMIT` is allowed only while `post_fence_drain_completed=false` and only when the matching pre-existing `!txn|lock|` row is present for that `(primary, start_ts, commit_ts)`. New prepares are not allowed through that lane. This avoids stranding a prepared txn, while still guaranteeing that after the drain returns empty there is no unresolved intent left to resolve on either source or target.

   **0a. The lock drain must be route-faithful, not raw-key-bracketed (closes codex P1 on PR #945 — txn-lock drain bounds).** A txn intent lock is stored at `txnLockKey(userKey) = !txn|lock| || userKey` (`kv/txn_keys.go`), i.e. the lock sorts by its **raw user key**, but a route's `[routeStart, routeEnd)` bounds live in the **routing-key** namespace. For families whose raw storage key differs from their routing key these two namespaces diverge, so a drain bracketed by `[txnLockKey(routeStart), txnLockKey(routeEnd))` silently misses locks that belong to the moving range. Concretely (verified against `routeKey` / `dynamoRouteFromTablePrefixedKey` in `kv/shard_key.go`): a DynamoDB item lock is stored at `!txn|lock|!ddb|item|<table>|...` while its routing key is `!ddb|route|table|<table>` (table-segment routing); SQS locks store at `!txn|lock|!sqs|...` but route to `!sqs|route|global`; Redis/list/S3 internal families have the same raw-vs-routing split. Because `!ddb|item|...` sorts well before `!ddb|route|table|...` (`i` < `r`), a DynamoDB item lock falls **outside** any `[txnLockKey(routeStart), txnLockKey(routeEnd))` bracket whose bounds are `!ddb|route|table|...` — the drain reports empty while a prepared lock is still live, and FENCE proceeds with an unresolved prepare in the moving range (stranded `Phase_COMMIT`, or a snapshot boundary in §6.1.0 that drops the eventual committed version). This is the same routing-vs-raw-key split §6.3.1 already had to handle for **data** export: a raw-range scan never visits internal-family keys that sort outside `[routeStart, routeEnd)`. The lock drain therefore reuses §6.3.1's mechanism rather than a raw bracket. The drain is specified as:
   - **Scan the full `!txn|lock|` family bracket** `[txnLockPrefix(), txnLockPrefixEnd())` (the §6.3.1 `familyTxnLock` bracket bounds), AND
   - **filter each row by `routeKey(lockedKey) ∈ [routeStart, routeEnd)`**, where `lockedKey = key[len("!txn|lock|"):]` is the embedded raw user key and `routeKey()` (`kv/shard_key.go`) normalizes it to its routing key — exactly the §6.3 `RouteKeyFilter` predicate the data export already uses. A lock whose `routeKey(lockedKey)` falls inside the moving range counts toward the drain regardless of where its raw bytes sort.

   Where a per-family `EncodeBracketStart(routeStart)` hook exists (§6.3.1), the migrator MAY tighten the family-wide `!txn|lock|` bracket to the moving slice as an optimization, but the `routeKey()` filter remains the correctness contract. The cost is bounded: the txn-lock keyspace is small and transient (one row per in-flight prepare, all resolving within a txn-TTL), so a full `!txn|lock|` family scan per drain tick is cheap — this is not the full data scan §6.3.1's scan-budget pacing guards against. The same route-faithful drain definition is used by step 0b (post-fence drain) and by §6.1.0's pre-BACKFILL live-lock scan; both are the identical "`!txn|lock|` family scan + per-row `routeKey` filter" operation, never a raw `[txnLockKey(routeStart), txnLockKey(routeEnd))` bracket.

   **0b. Post-fence drain (closes codex round-14 P1 line 171 — the catalog-write race).** After step 1's unanimous voter-ACK returns and before picking `fence_ts`, the migrator runs the route-faithful intent-lock drain — the same `!txn|lock|` family scan filtered by `routeKey(lockedKey) ∈ [routeStart, routeEnd)` from §3.2a.0a (not a raw `[txnLockKey(routeStart), txnLockKey(routeEnd))` bracket, which would miss DynamoDB/SQS/Redis/list/S3 locks per §3.2a.0a). By the time this drain starts, every source voter rejects new prepares synchronously, so the drain contains only intents that existed before the last voter applied `WriteFenced`; each either commits through the narrow §7.1 resolution lane or is aborted by the lock resolver (`kv/lock_resolver.go`) within its TTL. The wait is therefore bounded by at most one txn-TTL and cannot starve under a steady workload. The migrator logs `last_error = "post-fence drain pending: <N> intent locks in moving range"` while this drain runs. The SplitJob records `post_fence_drain_completed` so a migrator restart between the drain and step 2 resumes correctly.

   **Boundary finalisation is gated on this drain (codex round-15 P1 — fence before the snapshot boundary).** §6.1.0's migration write tracker records a *running minimum* (§6.1.0 step 2) from before `snapshot_ts` is selected through this drain — it is not a single pre-BACKFILL snapshot. The migrator does **not** finalise the DELTA_COPY lower bound `delta_floor` until step 0b's drain has returned empty, because only then is it true that no new raw point write, `DEL_PREFIX`, one-phase txn, or prepare can land in the moving range (step 1 made every voter reject via `verifyRouteNotFenced`, and the drain confirmed no residual in-flight prepare remains). Finalising `delta_floor` before this point would re-create the lost-low-ts-write hazard: a coordinator can submit a raw request, one-phase txn, or prepare with a low caller-supplied / lagging-clock `commit_ts ≤ snapshot_ts` while the source is still `Active`, and that commit would escape both BACKFILL and a `(snapshot_ts, fence_ts]` window. The tracker-backed running minimum + post-drain finalisation (§6.1.0 step 3) is what pushes `delta_floor` below every such migration-window write without scanning old retained MVCC history.


1. **Source-group durable write-fence barrier, ACKed by every source-group voter.** After the FENCE catalog write commits, the migrator does **not** immediately pick `fence_ts` and does **not** rely on an in-memory watcher ACK alone. It proposes `ApplyMigrationWriteFence(job_id, route_start, route_end, fence_catalog_version)` through the source group's Raft log. The source FSM persists a local fence entry such as `!migfence|<job_id>` in source-group state/snapshots and makes `verifyRouteNotFenced` consult this table before the watcher-populated route snapshot. The entry is idempotent for the same tuple and rejects conflicting bounds/version for the same job. Startup, snapshot restore, and leadership acquisition must replay/load these entries before serving writes; a node that cannot load them fails closed for the affected range until it catches up.

   The migrator advances only after the source-group proposal is committed and **every current voter has applied it** (the same voter list comes from `engine.Configuration(ctx)`, already read by the migrator for §3.4 routing). A heartbeat field such as `source_write_fence_applied_version` may report progress, but the progress being reported is the source-group Raft-applied fence record, not just `catalog_version_applied` in `distribution/watcher.go`. This guarantees every node that could be elected source leader already has a durable local rejection rule for the moving range, so any write that reaches its FSM apply path after this point is rejected by §7.1's pre-gate even if the catalog watcher is stale or the process restarts.
2. **`delta_floor` finalised, then `fence_ts` pinned, after the durable barrier and post-fence drain.** After step 1 returns **and step 0b's post-fence drain has returned empty**, the migrator finalises `delta_floor = min(snapshot_ts, snapshot_min_admitted_ts - 1)` (§6.1.0 step 3) and then queries the source group leader for `LastCommitTS()` and pins that as `fence_ts`. Because the durable write-fence entry is now applied on every voter, no in-flight write with `commit_ts > fence_ts` can land for the moved range — the apply path rejects them with `ErrRouteWriteFenced` at the FSM gate. DELTA_COPY's `(delta_floor, fence_ts]` window therefore captures every committed write whose `commit_ts > delta_floor` — including any post-scan raw, one-phase, or prepared write that committed with `commit_ts ≤ snapshot_ts` while the source was still `Active` (`delta_floor < its commit_ts`) — completing the BACKFILL+DELTA_COPY semantic with no lost low-ts write.

Timing budget: the migrator's heartbeat-driven loop already runs at one tick per `hlcRenewalInterval` (1s), and `cap_migration_v2` heartbeats carry `catalog_version_applied` already (§11.1 reuses the same field). The fence-apply ACK therefore typically completes in one tick. A non-responsive voter blocks the phase — the migrator logs `last_error = "fence ack pending: nodes [<N1>, <N2>]"` and either (a) the operator intervenes, or (b) Raft removes the unresponsive voter via the existing leader-loss path. **Importantly the migrator does NOT advance phase on a quorum ACK** — every voter must ACK, because any voter could later become the leader and would re-serve writes for the moved range with the pre-FENCE state if its catalog hadn't applied. Quorum is necessary for catalog-write durability; **unanimous voter-ACK is necessary for fence safety**.

Crash safety: the fence-ack-pending state is durable in the SplitJob (`fence_ack_cursor` tracks which voters have ACK'd). A migrator restart re-proposes/probes `ApplyMigrationWriteFence` with the same tuple and resumes from the pending voter list. No data is at risk during this wait — the catalog FENCE state and the source-group local fence record are both durable once applied; the migrator just doesn't commit DELTA_COPY's `fence_ts` until every possible source leader has the local fence.

CUTOVER uses a separate source-side read-fence arm, not a post-hoc watcher wait: before the default group publishes `target.Active`, §7.2.2e installs a pending read fence in the source group's Raft log for the exact moving range and expected `cutover_version`. The source can then reject any stale read that still reaches it even if its catalog watcher has not applied the final CUTOVER snapshot yet. The fence-apply ACK above is specifically about **writes during DELTA_COPY**, where the data move happens; the CUTOVER read barrier is the symmetric reader-side protection.

## 4. State Machine and Recovery

The migrator is a goroutine on the **default-group leader**. State lives in the catalog (durable); the goroutine is a thin reconciler that picks up wherever the last leader stopped.

Phase transitions are themselves applied through the default Raft group so every node sees the same job state. A transition writes the new phase + cursor + ts to `!dist|job|<id>` via a CAS on the prior phase — concurrent migrators on a leadership flap can't race.

Failure semantics per phase:

| Phase | Crash safety |
|---|---|
| `PLANNED` | Job record durable; no side effects yet. |
| `BACKFILL` | Source-group migration write tracker is armed before `snapshot_ts` is picked; cursor persisted after each chunk batch (configurable, default every 256 keys); on resume the migrator re-reads cursor and continues. Idempotent ImportRangeVersions (see §6) means re-sending a batch is harmless. |
| `FENCE` | Catalog state is `WriteFenced`; coordinator rejects new writes/prepares until phase advances, while existing prepared txns may resolve through the narrow §7.1 lane until the post-fence drain is empty. A leader flap here keeps the fence (state is durable). The §6.1.0 step-2 running minimum (`snapshot_min_admitted_ts`), `post_fence_drain_completed`, and `delta_floor` are persisted as they advance, so a flap before `delta_floor` is finalised re-reads the migration write tracker + route-faithful drain and recomputes the same bound (the running minimum is monotone-down and durable). |
| `DELTA_COPY` | Same as BACKFILL — chunked + cursor-persisted. `delta_floor` (lower bound) and `fence_ts` (upper bound) are recorded at the FENCE → DELTA_COPY transition, so a resumed DELTA_COPY uses the same `(delta_floor, fence_ts]` window. |
| `CUTOVER` | Single atomic catalog write under CAS on `expected_catalog_version`. Either fully visible or fully not — no partial split. |
| `CLEANUP` | After grace period (`readFenceGrace`, default **30 s** per §7.2.4 — comfortably exceeds lease TTL and watcher tick × 2; matches §12 OQ-3 resolution). GC is idempotent (delete-if-version-≤-fence_ts) and operates only on user data found by the disjoint bracket plan (§6.3.1); cross-group jobs intersecting migration/control namespaces are rejected before SplitJob creation (§3.2). |
| `FAILED` | Operator-visible **live** state. The job remains under `!dist|job|<id>` with all cursors/fences/pins intact until a manual `RetrySplitJob` or `AbandonSplitJob` RPC. `RetrySplitJob` CASes the job back to the durable `retry_phase` (`BACKFILL`, `FENCE`, or `DELTA_COPY`) without deleting side effects; idempotent imports/fences make replay safe. A FAILED job with `retry_phase=NONE` is corrupt and must reject retry rather than guessing. `AbandonSplitJob` runs the cleanup protocol below and then moves the job to `ABANDONED` history. |
| `ABANDONED` | Terminal audit state after a pre-CUTOVER job is explicitly abandoned and the source/target per-job cleanup protocol has completed. No retry is allowed. |

`PLANNED → BACKFILL` can be rolled back by `AbandonSplitJob` (no data moved yet). `BACKFILL` / `FENCE` / `DELTA_COPY` can be abandoned only through the cleanup protocol below. After CUTOVER the job is one-way; rollback would require a reverse migration, which is M2-out-of-scope.

### 4.2 Job garbage collection

The retention policy from §3.1.1 runs as a CLEANUP-tail sweep on the default-group leader. Concretely:

1. On a phase transition `CUTOVER → CLEANUP`, the migrator records `terminal_at` candidate, waits for the read-fence grace window (§7.2.4), then runs §6.4 step 5's target cleanup and §6.5's source cleanup/disarm. The job stays live under `!dist|job|<id>` until both cleanup ACKs are durable.
2. On `CLEANUP → DONE`, the migrator issues one Raft proposal that (a) deletes `!dist|job|<id>`, (b) writes `!dist|jobhist|<terminal_at_ms_be>|<id>` with the final body. This proposal is allowed only after target-local ack / HLC-floor / readiness / promotion records are gone and the source write tracker / write fence / cutover read fence / retention pin are disarmed.
3. On `* → FAILED`, the job stays live under `!dist|job|<id>` and is **not** counted against `maxJobHistory`. The live cap in §3.1.1 includes retryable FAILED jobs so a cluster cannot accumulate infinite failed jobs without operator action.
4. On `AbandonSplitJob` before CUTOVER, the migrator first runs a bounded per-job cleanup protocol, then moves the final `ABANDONED` body to `!dist|jobhist|<terminal_at_ms_be>|<id>`:
   - source group: disarm `ArmMigrationWriteTracker`, delete `!migwrite|<job_id>|*`, clear any `!migfence|<job_id>` durable write fence / pending cutover read fence, roll `WriteFenced` back to `Active` if FENCE had landed, and release `source_retention_pin_ts`;
   - target group: delete staged rows under `!dist|migstage|<job_id>|*`, import ack records under `!migstage|ack|<job_id>|*`, `!migstage|hlc_floor|<job_id>`, `!migstage|ready|<job_id>`, and `!migstage|promote|<job_id>` in cursor-bounded batches;
   - default group: only after the source and target cleanup ACKs are durable, CAS the live job to `ABANDONED` history.
5. Once per minute the leader scans `!dist|jobhist|` for entries older than `historyTTL` (default 7d) OR exceeding `maxJobHistory` (1k) and deletes the oldest excess in one proposal.

GC failure modes are bounded: the sweep is idempotent (delete-if-exists), and a stale leader's proposal lands as a no-op on the new leader's apply because the keys are already gone.

## 5. Wire / RPC Changes

### 5.1 `proto/distribution.proto`

Extend `RouteDescriptor` to carry the staged-visibility flag and migration job ID that §6.4 step 2's MVCC merge read needs (closes claude round-11 P1 on PR #945: the target FSM cannot identify which `!dist|migstage|<job_id>|*` prefix to merge without `migration_job_id` on the route snapshot):

```proto
message RouteDescriptor {
  // ... existing M1 fields (raft_group_id, route_id, start/end, state) ...

  // Milestone 2: set on CUTOVER, cleared on CLEANUP→DONE in the same
  // atomic proposal. While true, the target FSM's read path runs the
  // staged+live MVCC merge of §6.4 step 2 against the
  // !dist|migstage|<migration_job_id>|* prefix; once cleared the route
  // is indistinguishable from any other Active route.
  bool   staged_visibility_active = N;
  // Identifies the !dist|migstage|<id>|* prefix the §6.4 step 2 merge
  // reads. Non-zero iff staged_visibility_active is true; cleared in
  // the same CLEANUP→DONE proposal. The target FSM needs this on the
  // RouteSnapshot so per-request reads can find the staged prefix
  // without an extra lookup against the SplitJob catalog.
  uint64 migration_job_id         = N+1;
}
```

These fields are persisted on the default-group `RouteDescriptor` record itself, propagated through the normal catalog watcher snapshot, and included in the same CUTOVER CAS that publishes `target.Active(right)`. They are not transient SplitJob-only metadata: a target leader that only has its route snapshot must be able to decide whether staged reads are active and which `!dist|migstage|<migration_job_id>|*` prefix to merge. Because a freshly refreshed coordinator can route to the target before the target watcher has applied that descriptor locally, CUTOVER is also preceded by the target-group staged-readiness barrier in §6.4 step 0; a target with the readiness record but without the matching route descriptor fails closed instead of serving live-only data. Existing descriptors decode as `staged_visibility_active=false` and `migration_job_id=0`; the default-group promotion-complete CAS clears both fields only after the target-local promotion completion proof in §6.4 step 5, and the final CLEANUP→DONE move happens after the per-job source/target cleanup ACKs are durable.

Extend `SplitRangeRequest` and add job-introspection RPCs:

```proto
message SplitRangeRequest {
  uint64 expected_catalog_version = 1;
  uint64 route_id = 2;
  bytes split_key = 3;
  // Milestone 2: optional. Zero (or equal to the source's group) keeps
  // the M1 same-group split semantic. Non-zero kicks off a cross-group
  // migration job and returns immediately with the job id.
  uint64 target_group_id = 4;
  // Per-job pacing knobs (§8). Reserved string-map so we can add new
  // knobs without a proto break. Known keys today:
  //   "chunk_bytes"           — BACKFILL / DELTA_COPY chunk size (bytes)
  //   "inter_chunk_pacing_ms" — sleep between chunks (ms)
  //   "fence_grace_ms"        — readFenceGrace override (ms) for the job
  // Unknown keys are accepted (forward-compat) and logged at
  // SplitJob create time, but do not affect behaviour.
  map<string, string> options = 5;
}

message SplitRangeResponse {
  uint64 catalog_version = 1;
  // For same-group splits (M1), left/right are populated immediately.
  RouteDescriptor left = 2;
  RouteDescriptor right = 3;
  // For cross-group splits (M2), only job_id is populated; left/right
  // become observable via ListRoutes after CUTOVER completes.
  uint64 job_id = 4;
}

message GetSplitJobRequest {
  uint64 job_id = 1;
}

message GetSplitJobResponse {
  SplitJob job = 1;
}

message ListSplitJobsRequest {
  // Inclusive lower bound on terminal_at (ms). 0 = no lower bound.
  // §3.1.1's history retention orders entries by terminal_at_ms_be
  // so paging through the audit history just bumps this on each call.
  uint64 since_terminal_at_ms = 1;
  // Optional phase filter; empty = no filter. Matches a SplitJob's
  // `phase` field exactly. Supports per-phase audit queries.
  string phase = 2;
  // Pagination cursor. Empty = first page; opaque bytes from a prior
  // response's next_page_cursor for subsequent pages. The default cap
  // (§3.1.1) is 200 newest entries per response.
  bytes page_cursor = 3;
}

message ListSplitJobsResponse {
  repeated SplitJob jobs = 1;
  // Opaque pagination cursor. Empty = caller has reached the last page;
  // non-empty = pass back as ListSplitJobsRequest.page_cursor to fetch
  // the next page. The encoding is internal to the catalog (typically
  // the terminal_at_ms_be of the last entry returned).
  bytes next_page_cursor = 2;
}

message AbandonSplitJobRequest {
  uint64 job_id = 1;
  // Operator may abandon only PLANNED / BACKFILL / FENCE / DELTA_COPY.
  // CUTOVER + later is rejected (data is partially landed in target).
}

message AbandonSplitJobResponse {}

message RetrySplitJobRequest {
  uint64 job_id = 1;
  // Allowed only while phase == FAILED and before CUTOVER. The default-group
  // CAS moves the job back to the recorded retry_phase; source/target side
  // effects are reused rather than cleaned first.
}

message RetrySplitJobResponse {}

service Distribution {
  // ... existing M1 RPCs ...
  rpc GetSplitJob (GetSplitJobRequest) returns (GetSplitJobResponse) {}
  rpc ListSplitJobs (ListSplitJobsRequest) returns (ListSplitJobsResponse) {}
  rpc AbandonSplitJob (AbandonSplitJobRequest) returns (AbandonSplitJobResponse) {}
  rpc RetrySplitJob (RetrySplitJobRequest) returns (RetrySplitJobResponse) {}
}
```

`SplitJob` is a separate message mirroring the persistent fields in §3.1.

### 5.2 `proto/internal.proto`

Add the data-movement RPCs (leader-to-leader, server-streaming for BACKFILL):

```proto
service Internal {
  // ... existing RPCs ...
  rpc ExportRangeVersions (ExportRangeVersionsRequest)
      returns (stream ExportRangeVersionsResponse) {}
  rpc ImportRangeVersions (ImportRangeVersionsRequest)
      returns (ImportRangeVersionsResponse) {}
}

message ExportRangeVersionsRequest {
  // Raw scan bounds — what the server iterator's [start, end) range
  // is in the MVCC storage key space. For a §6.3.1 internal-family
  // bracket these are the family-wide bounds (e.g. txnLockPrefix() →
  // txnLockPrefixEnd()), which scan ALL of the family cluster-wide.
  bytes range_start = 1;
  bytes range_end = 2;
  uint64 max_commit_ts = 3;  // snapshot_ts (BACKFILL) or fence_ts (DELTA_COPY)
  // DELTA_COPY's lower bound is delta_floor (§3.1 / §6.1.0 step 3), NOT
  // snapshot_ts: a post-scan raw/one-phase/prepared write with commit_ts ≤
  // snapshot_ts must still land in (delta_floor, fence_ts], and delta_floor
  // = min(snapshot_ts, snapshot_min_admitted_ts - 1) is finalised after the
  // fence so that delta_floor < every such commit_ts. Sending snapshot_ts
  // here would re-exclude exactly the versions delta_floor was computed to
  // capture (codex round-16 P1 — use delta_floor as DELTA_COPY lower bound).
  uint64 min_commit_ts = 4;  // 0 for BACKFILL; delta_floor for DELTA_COPY
  bytes cursor = 5;          // resume token; empty on first call
  uint32 chunk_bytes = 6;    // soft cap, server may exceed by one row
  // Logical moving range bounds — what the server's KeyFilter (§6.3)
  // narrows the raw iterator to (closes claude round-12 P1 on PR #945:
  // without these, a family-wide raw bracket cannot be narrowed to the
  // moving slice on the server side and would export every cluster-
  // wide txn lock / list / redis / etc. to the target). The server
  // constructs RouteKeyFilter(route_start, route_end) from these and
  // applies it before adding a row to the chunk. Required on every
  // call; the migrator already has them on its SplitJob.
  bytes route_start = 7;
  // Empty == +infinity (open-ended right edge). The internal RPC server
  // normalizes len(route_end)==0 to nil before building RouteKeyFilter; the
  // filter also checks len(rangeEnd)>0 defensively (§6.3) so the rightmost
  // route cannot reject every non-empty routing key by comparing against [].
  bytes route_end = 8;
  // §6.3.1 scan budget: hard cap on the bytes the server iterator may
  // scan (accepted AND rejected rows both count) before it must return
  // whatever it has accepted plus next_cursor over the last-scanned
  // position. Bounds the per-call wall-clock cost of a family-wide
  // sparse bracket whose moving slice is empty or tiny — without it a
  // single call could scan an arbitrarily large unrelated internal-
  // family prefix (e.g. a cluster-wide 10 GB !sqs| range) to prove
  // done=true. 0 means "use the server default" (4 × chunk_bytes, §6.3.1).
  uint64 max_scanned_bytes = 9;
}

message ExportRangeVersionsResponse {
  repeated MVCCVersion versions = 1;
  bytes next_cursor = 2;  // empty when exhausted
  bool done = 3;
}

message MVCCVersion {
  bytes key = 1;        // raw storage key (NOT routeKey-normalized)
  uint64 commit_ts = 2;
  bool tombstone = 3;
  bytes value = 4;      // logical value; omitted when tombstone == true
  // Internal-key family (txn/list/redis) tag, so the importer can
  // dispatch into the matching store helper instead of inferring
  // from the byte prefix.
  uint32 key_family = 5;
  // Store-level TTL deadline in HLC timestamp units; 0 means no TTL.
  // Preserved from VersionedValue.ExpireAt / Pebble's value header so
  // PutWithTTLAt / ExpireAt values keep expiring at the original time.
  // Tombstones must carry expire_at=0.
  uint64 expire_at = 6;
}

message ImportRangeVersionsRequest {
  uint64 job_id = 1;
  // May be empty for a scan-budget progress chunk (§6.2): the importer
  // still acks bracket_id/batch_seq/cursor but performs no MVCC writes or
  // HLC advancement.
  repeated MVCCVersion versions = 2;
  // Exporter resume token (§6.1.1): carried so the migrator can persist
  // it back on ack and resume the bracket after a restart. NOT the
  // idempotency key — see bracket_id / batch_seq below.
  bytes cursor = 3;
  // §6.1.1 importer idempotency key. Because §6.3.1 runs export brackets
  // in parallel, dedup keys on (job_id, bracket_id, batch_seq), not on
  // (job_id, cursor) which parallel brackets would clobber.
  //   bracket_id — the §6.3.1 per-bracket id (stable for the SplitJob's
  //                life; recorded in bracket_progress).
  //   batch_seq  — monotonic per-bracket counter the migrator advances
  //                on every successful ack; a retry of
  //                (job_id, bracket_id, batch_seq ≤ stored) is a no-op.
  uint64 bracket_id = 4;
  uint64 batch_seq = 5;
}

message ImportRangeVersionsResponse {
  // Cursor durably acked by the target for (job_id, bracket_id, batch_seq).
  // On duplicate batch_seq the target returns the stored value.
  bytes acked_cursor = 1;
}
```

Streaming the export keeps the migrator memory-bounded; a single `ImportRangeVersions` RPC per chunk lets the importer ack the cursor (the migrator persists it in the SplitJob on ack).

## 6. MVCC Range Export / Import

### 6.1 Source side (`store/mvcc_store.go`, `store/lsm_store.go`)

The `store` package is generic and **must not** import `kv` or know about routing keys (gemini medium on PR #945). Filtering is injected as a delegate from the caller.

#### 6.1.0 `snapshot_ts` selection and DELTA_COPY lower bound — must dominate every low-ts write admitted before the fence (closes codex round-14 P1 line 405; closes codex round-15 P1 — fence-before-boundary; closes codex round-16 P1 — track prepares for moved keys, not only moved primaries; closes codex round-20 P1 — raw/one-phase low-ts writes)

The corner case: 2PC's `commit_ts` is allocated by the coordinator at **prepare time** (via `HLC.Next()` / `NextFenced()` on the prepare path), not at commit-apply time. Concretely, a transaction `T` can sequence as:

1. `t = t_prepare`: coordinator picks `commit_ts(T)` and writes intent locks on the participant groups.
2. `t = t_snapshot`: migrator picks a snapshot ts for BACKFILL, with `t_prepare < t_snapshot` so `commit_ts(T) < snapshot_ts` is possible.
3. `t = t_commit_apply`: coordinator's `Phase_COMMIT` lands on the source FSM. The MVCC version `(K, commit_ts(T))` is written **after** `t_snapshot`.

If the migrator starts BACKFILL at `snapshot_ts` and the export reads the snapshot at exactly `maxCommitTS = snapshot_ts`, the committed value lands at `commit_ts(T) ≤ snapshot_ts` but is **not yet present in the store** when BACKFILL iterates. A naïve DELTA_COPY `(snapshot_ts, fence_ts]` window then excludes it (because `commit_ts(T) ≤ snapshot_ts`), and the FENCE drain alone doesn't help because the drain only waits for intents to disappear, not for past-committed-but-not-yet-applied versions to surface. Net result: a committed write the client has already observed is silently dropped from the target.

**Why a single pre-BACKFILL scan is insufficient (codex round-15 P1 — fence before fixing the boundary).** An earlier revision picked `snapshot_ts = min(HLC.Next(), min_admitted_commit_ts - 1)` from **one scan taken before BACKFILL** and trusted it as the DELTA_COPY lower bound. That scan only sees writes known at scan time, but **the source route stays `Active` until the much-later FENCE transition (§3.2a)** — so a coordinator can apply a *new* raw request, one-phase txn, or prepare after the scan with `commit_ts ≤ snapshot_ts` and still have it accepted by the source. Two verified mechanisms let a post-scan write carry such a low `commit_ts`:

- **Caller-supplied commit_ts.** `resolveDispatchCommitTS` uses a caller-supplied `commitTS` directly (it only `Observe`s it; `kv/coordinator.go:872-876`), and `HLC.Observe` is monotone-up-only — it never raises a caller's ts, so a caller-supplied `commit_ts ≤ snapshot_ts` is accepted and used as-is. The caller-supplied-StartTS/CommitTS path is live for the S3/SQS/DynamoDB adapters today (`kv/sharded_coordinator.go:638-665`).
- **Lagging-clock / low logical half.** Even on the coordinator-allocated path, `NextFenced` only floors the **physical** half at the Raft-agreed ceiling (`kv/hlc.go:171-176`); the logical lower 16 bits are a free in-memory counter (`kv/hlc.go:181-190`), and `snapshot_ts = min_admitted_commit_ts - 1` can sit below the leader's own current `HLC.Next()`, so a fresh allocation within the same physical millisecond can be `≤ snapshot_ts`.

Such a post-scan write can commit after the BACKFILL iterator has passed its key (so it is invisible to BACKFILL) and be `≤ snapshot_ts` (so a `(snapshot_ts, fence_ts]` DELTA_COPY excludes it) → lost write. Prepared txns are only one instance of the problem: `handleRawRequest` writes at `r.Ts` (including `Op_DEL_PREFIX`, which writes tombstones for every visible key under a prefix), and `handleOnePhaseTxnRequest` writes at `TxnMeta.CommitTS`, with no intent lock or other durable marker unless the migration write tracker records them explicitly. The boundary must therefore be **fixed only after writes for the moving range are fenced**, i.e. after no new raw point write, `DEL_PREFIX`, one-phase txn, or prepare can land — which is exactly §3.2a step 1's unanimous `verifyRouteNotFenced` apply plus step 0b's post-fence drain for already-prepared txns.

**The fix is to record migration-window writes before choosing `snapshot_ts`, then fence new writes before fixing the boundary.** The single pre-BACKFILL scan becomes a *provisional* snapshot for the early bulk copy; the boundary that DELTA_COPY trusts is finalised after the fence. Concretely:

1. **Arm the migration write tracker, then choose the provisional snapshot for BACKFILL.** Before opening BACKFILL, the migrator proposes `ArmMigrationWriteTracker(job_id, route_start, route_end)` to the source group and waits for it to apply; only then may it set `write_tracker_armed=true` on the default-group SplitJob and choose `snapshot_ts`. The tracker is source-group Raft state. From that apply onward:

   - `handleRawRequest` appends a tracker row for each point mutation whose `routeKey(mut.Key) ∈ [routeStart, routeEnd)`, using the request's `commit_ts = r.Ts`, in the same apply that writes the MVCC version. For `Op_DEL_PREFIX`, the tracker computes the same prefix footprint as §7.1: an empty prefix intersects every route, and a non-empty prefix covers the half-open raw prefix interval `[prefix, prefixScanEnd(prefix))` mapped through the route-key-normalized prefix planner. If that footprint intersects the moving range, or if the prefix is an internal/family-wide prefix that cannot prove disjoint from the moving range, the tracker appends a row using the request's `commit_ts = r.Ts` in the same apply that writes the prefix tombstones. It does not need one row per deleted key; the row records that the DELTA_COPY lower bound must include this range tombstone.
   - `handleOnePhaseTxnRequest` appends a tracker row for each written mutation whose `routeKey(mut.Key)` is in the moving range, using `commit_ts = TxnMeta.CommitTS`, in the same apply that writes the MVCC version. A dedup no-op for `PrevCommitTS` does not append a row because no new version is written.
   - The prepare apply path appends a tracker row such as `!migwrite|<job_id>|<route_key>|<commit_ts>|prepare|<primary_key>|<start_ts>` in the same apply that writes the txn lock when `routeKey(lockedKey) ∈ [routeStart, routeEnd)`. This records **admission**, not later polling, so a prepare that lands and resolves between two migrator ticks is still represented.

   M2-PR0 must make the last bullet true on the live prepare path, not only in the lock-value codec. Today the multi-shard prepare plumbing can reach `handlePrepare` with only `start_ts` and a zero/absent `commit_ts` in the txn metadata. PR0 changes that contract: the coordinator allocates and fences `commit_ts` before emitting PREPARE, the prepare request and txn metadata carry that exact value to every participant, `handlePrepare` writes it into each `txnLock`, and the migration write tracker records the same non-zero value in the same Raft apply before the lock becomes visible. The later COMMIT phase must reuse that already-admitted `commit_ts`; it must not allocate a replacement timestamp. Once both sides advertise `cap_migration_v2`, a newly written prepare lock whose metadata lacks a non-zero `commit_ts` is rejected as a protocol error; only pre-upgrade lock values may take the legacy direct-commit-record lookup / pending-drain fallback.

   After the tracker is armed, run the **route-faithful lock scan** of §3.2a.0a — the `!txn|lock|` family bracket `[txnLockPrefix(), txnLockPrefixEnd())` filtered by `routeKey(lockedKey) ∈ [routeStart, routeEnd)`, **not** a raw `[txnLockKey(routeStart), txnLockKey(routeEnd))` bracket (which would miss DynamoDB/SQS/Redis/list/S3 item locks whose raw storage key sorts outside the routing-key bounds — see §3.2a.0a). For each accepted pre-tracker lock, decode its recorded `commit_ts` from the M2-PR0 lock-value field. If the field is absent or zero because the lock was written before M2-PR0, fall back to the direct commit-record point lookup described in step 2; if neither source yields a timestamp because the legacy lock is still only prepared, keep the drain pending rather than guessing a floor. Take `min_admitted_commit_ts = min(commit_ts for all in-flight pre-tracker prepares accepted by the filter and all tracker rows so far)`; `∞` if none. Pick the provisional `snapshot_ts = min(HLC.Next(), min_admitted_commit_ts - 1)` and start BACKFILL at it. Over-copy is harmless — a low `snapshot_ts` just hands more work to DELTA_COPY.
2. **Track the running minimum until the fence blocks new writes — over every *key or range footprint* in the moving range, not only moved primaries (closes codex round-16 P1; closes codex round-18 P1 on historical-version over-scan; closes codex round-20 P1 on raw/one-phase low-ts writes; closes codex round-21 P1 on `DEL_PREFIX` range writes).** From BACKFILL through §3.2a step 0b's post-fence drain, the migrator maintains `snapshot_min_admitted_ts` = the smallest `commit_ts` in the source-group migration write tracker plus any still-live pre-tracker locks whose locked key resolves into the moving range (monotone-down, persisted on each advance). The earlier wording — "scan committed MVCC versions in the moving range with `commit_ts ≤ snapshot_ts`" — was too broad: it includes every old retained version that BACKFILL already copied, so a years-old historical version could lower `delta_floor` near zero and turn DELTA_COPY into another full backfill. The tracker is the correctness backstop for same-tick land-and-resolve prepares, for raw point / one-phase writes that create no intent lock, and for `DEL_PREFIX` range tombstones whose prefix intersects the moving route; ordinary committed MVCC history is copied by BACKFILL/DELTA_COPY but is **not** allowed to lower the boundary.

   The tracker is keyed by the **locked key**, not the primary, so a secondary lock in the moving range counts even when the txn's primary routes elsewhere. Verified against `kv/fsm.go:1136-1142` and `kv/txn_keys.go:50-58`: the durable `!txn|cmt|<primaryKey>|<startTS>` commit record is written **only when committing the primary** (`if committingPrimary`) and is keyed by `primaryKey`, so for such a txn the commit record's `routeKey()` falls **outside** `[routeStart, routeEnd)` — a commit-record family scan filtered by `routeKey()` never sees it. The migrator may still use the exact `PrimaryKey` + `StartTS` decoded from a live lock to do a **direct point lookup** for backward-compat locks where the M2-PR0 `commit_ts` field is absent or zero, but it never relies on a route-faithful commit-record-family scan for moved-secondaries.

   The minimum is only **final** once §3.2a step 1's unanimous voter-ACK has made every source voter reject new writes/prepares (`verifyRouteNotFenced`) **and** step 0b's post-fence drain has returned empty — at that point no future raw point write, `DEL_PREFIX`, one-phase write, or prepare can land on any key in the moving range, and every migration-window write was recorded by the tracker or was a pre-tracker prepare lock sampled by the drain path. Tracker rows are retained until `delta_floor` is finalised, then deleted by the same cleanup that clears the import ack state for the job.
3. **Finalise the boundary after the post-fence drain.** When step 0b's drain returns empty (`post_fence_drain_completed = true`), the migrator reads the migration write tracker and live-lock scan **one last time**; the lock side returns empty by construction (no in-flight prepare survives the drain), and the tracker yields the final running minimum over every migration-window write — raw point write, `DEL_PREFIX` range tombstone, one-phase write, prepared primary, or prepared secondary — that resolved into the moving range. The DELTA_COPY lower bound is then fixed as `delta_floor = min(snapshot_ts, snapshot_min_admitted_ts - 1)` and DELTA_COPY exports the window **`(delta_floor, fence_ts]`** (not `(snapshot_ts, fence_ts]`). Any post-scan write that committed with `commit_ts ≤ snapshot_ts` was recorded by the step-2 tracker, so `delta_floor ≤ snapshot_min_admitted_ts - 1 < commit_ts`, i.e. `commit_ts > delta_floor` and the version falls **inside** DELTA_COPY's window — it is no longer dropped. `delta_floor` is persisted on the SplitJob alongside `snapshot_min_admitted_ts` so a DELTA_COPY resume uses the same lower bound.

This is the reviewer's "block/drain before choosing this boundary," expressed as: keep tracking every admitted write's `commit_ts` until the fence blocks new ones, then fix the DELTA_COPY floor below all low-ts migration-window writes. The alternative — "before BACKFILL, wait for all currently-prepared transactions to resolve, then `snapshot_ts = HLC.Next()`" — does **not** suffice on its own here precisely because the source stays `Active` after that wait and fresh low-ts raw/one-phase/prepared writes can keep arriving until the fence; the fence-then-floor ordering is what makes the boundary safe.

Edge case: if a migration-window write has an unusually low caller-supplied `commit_ts`, `delta_floor` may push the DELTA_COPY window below recent committed history. That is acceptable — `(delta_floor, fence_ts]` is a proper superset of the BACKFILL-missed window and re-exports the same versions idempotently (§6.2 dedups on `(job_id, bracket_id, batch_seq)`). The bound is now tied to the number and timestamps of writes admitted during this migration, not to arbitrary old retained MVCC versions, so it cannot be lowered merely because the range has long history. The target HLC floor in §6.2.1 still uses `max_imported_ts` (which includes DELTA_COPY's exported versions) so the target-side time-travel invariant is unchanged.

#### 6.1.0a Source MVCC retention pin — export windows must remain readable

Before BACKFILL opens, the migrator proposes `InstallMigrationRetentionPin(job_id, route_start, route_end, min_ts=snapshot_ts)` on the source group and records `source_retention_pin_ts = snapshot_ts` in the SplitJob. The pin is route-scoped and bracket-aware: compaction may still discard unrelated keys, but for any raw row accepted by the §6.3.1 `RouteKeyFilter` it must retain committed versions and tombstones needed to answer `ExportVersions` at `snapshot_ts`. When §3.2a finalises `delta_floor`, the migrator updates the same source-group pin to `min_ts = min(source_retention_pin_ts, delta_floor)` before DELTA_COPY starts, so the full `(delta_floor, fence_ts]` window remains readable even when a low caller-supplied write pushed `delta_floor` below the provisional snapshot.

The pin is released only after successful source cleanup (§6.5) or after the abandon/FAILED cleanup protocol in §4.2 disarms the job. A source restart or snapshot restore must reload active migration retention pins before running compaction. This is a correctness fence, not only an optimization: if `MinRetainedTS` or tombstone compaction prunes a version before the bracket cursor reaches it, the target can miss historical visibility or resurrect deleted data after CUTOVER.

New method on `MVCCStore`:

```go
// KeyFilter is the in-package contract: the caller decides which raw
// keys belong to the slice of MVCC space being exported. nil means
// "accept every key in [start, end)". The decision MUST be deterministic
// and pure — the export streams it on the iteration goroutine.
type KeyFilter func(rawKey []byte) bool

// ExportVersions iterates raw committed MVCC versions in the half-open
// range [start, end) whose commit_ts is in (minCommitTS, maxCommitTS],
// resuming from cursor. It includes delete tombstones and versions whose
// ExpireAt would make them invisible to a reader at maxCommitTS; it does
// NOT apply read-visible filtering. It excludes only in-flight intent
// locks (!txn|lock|...) because those are not committed MVCC versions the
// importer should materialise. Returns a slice of versions (capped roughly
// at chunkBytes) and the next cursor, plus a sentinel when exhausted.
// The cursor is opaque (see §6.1.1) — it addresses an MVCC version
// (raw key + commit_ts), not just a raw key, so a single hot key's
// history is allowed to span batches without losing or duplicating
// versions.
// `accept` (nil = no filtering) is the §6.3 decoupling seam.
// `maxScannedBytes` is the §6.3.1 scan budget: a hard cap on the bytes
// the iterator may scan — counting BOTH accepted and rejected rows —
// before it must return whatever it has accepted plus the next cursor
// over the last-scanned position (done=false). It bounds the per-call
// cost of a sparse family-wide bracket whose moving slice is empty or
// tiny, where `accept` rejects most rows and they do not count against
// chunkBytes. The server handler normalizes wire max_scanned_bytes=0
// to the default 4 × chunkBytes before calling this method, and the
// store layer repeats that normalization for maxScannedBytes <= 0 as a
// defensive default; zero is not interpreted as "unbounded" on export
// RPC paths. chunkBytes still caps the accepted-row payload.
ExportVersions(ctx context.Context, start, end []byte, minCommitTS, maxCommitTS uint64,
    cursor []byte, chunkBytes, maxScannedBytes int, accept KeyFilter) ([]MVCCVersion, []byte, bool, error)
```

The function uses the existing MVCC storage order and value decoder, but deliberately **does not** use the read-visible snapshot filter as its row source. A read at `maxCommitTS` hides tombstones and values whose `ExpireAt <= maxCommitTS`; migration must still copy those committed versions so the target preserves deletes, TTL expiry, and historical reads across CUTOVER. `accept` is consulted before a row is added to the chunk; rejected rows do not advance the chunk-bytes budget so the caller sees deterministic per-call shapes — but they **do** advance the `maxScannedBytes` budget, so a sparse bracket cannot scan an unbounded prefix to prove `done=true` (§6.3.1).

#### 6.1.1 Cursor granularity — addresses a scanned MVCC position, not just a raw key (closes codex P2; refined for sparse brackets per coderabbit round-12 Major / codex round-14 P2)

Codex P2 on PR #945: a "next-key" cursor breaks for hot keys with version histories larger than `chunkBytes` — the migrator would either have to send the whole version chain in one unbounded chunk (blowing the memory bound) or skip/duplicate the tail after a restart. The cursor must therefore address an **MVCC position**, not a raw key.

Concretely, the cursor is opaque on the wire but encodes the tuple `(raw_key, commit_ts, scanned_position_tag)` of the **last scanned MVCC position** — the position the iterator advanced past in this chunk's tail, regardless of whether the version at that position was emitted or rejected (closes coderabbit round-12 Major line 429 and codex round-14 P2 line 540 on PR #945). The earlier "last emitted" wording assumed every iterated row was emitted; that is true for the dense default path but is incorrect for §6.3.1's sparse-bracket / scan-budget-bounded path where the `accept KeyFilter` rejects most rows before `maxScannedBytes` is hit. The corrected definition is:

- **`raw_key, commit_ts`** — the MVCC position the iterator advanced past in this call's tail. If the last action was an emit, this is the emitted version's `(raw_key, commit_ts)`. If the last action was a reject (skipped by `accept` or by an internal-family filter), this is the rejected row's `(raw_key, commit_ts)`. In either case the iterator's "where I have looked so far" boundary is captured exactly.
- **`scanned_position_tag`** — a 1-bit tag (`0 = last-emitted, 1 = last-scanned-but-not-emitted`) distinguishing the two cases above. The importer's idempotency check (§6.2) keys on `(job_id, bracket_id, batch_seq)` rather than on this cursor, so the tag is informational for the importer — but the **exporter** needs it to know whether to start the next iteration "strictly past the last-emitted version" (tag=0, the original semantic — guarantees no duplicate) or "strictly past the last-scanned position even though nothing was emitted" (tag=1 — guarantees no infinite rescan of a sparse window).

Resume is **exclusive on the recorded position**: the next call iterates strictly past `(raw_key, commit_ts)` under the export's `(raw_key ASC, commit_ts DESC)` order (newest-first within a key, matching the MVCC iterator already used by snapshot reads in `store/mvcc_store.go`), so the version the cursor names is **not** re-iterated (avoids duplicate work) and the very next version in iteration order **is** iterated (no skip, no infinite rescan of rejected windows). The earlier wording — "encoded as the exclusive `(raw_key, commit_ts)` of the *next-to-emit* row" — was internally inconsistent on two axes: "next-to-emit" + "exclusive resume" reads as "skip the version the cursor names" (would silently drop one MVCC version per chunk boundary for a hot key whose history spans chunks); and "last emitted" alone gave no resume point for a sparse bracket that accepted zero rows before `maxScannedBytes`, leaving the cursor unable to advance at all (would infinitely rescan the same rejected window). The "last-scanned position + tag" form is the only one that preserves no-duplicate AND no-skip AND no-rescan simultaneously. Three properties follow:

- **Hot-key safe.** A chunk may contain only versions of a single raw key, and the next chunk picks up the next-older version of the same key without any all-or-nothing constraint on the key's history.
- **Resume-safe across restarts.** Restarting from a persisted opaque cursor lands on the exact same MVCC version as a continuous run — no skipped versions (the cursor is exclusive on the resume side) and no duplicates (the importer's idempotency check, §6.2, dedups any one resume that overlaps an ack window).
- **Importer doesn't care about key boundaries.** Versions arrive in `(raw_key, commit_ts DESC)` order; the importer just applies them. No "this batch is the rest of key K" handshake is needed.

The opaque encoding is `varint(len(raw_key)) || raw_key || uvarint(commit_ts) || byte(scanned_position_tag)`; an empty cursor signals "start from the beginning." Future evolution can add fields after the tag byte because the consumer never parses the cursor by hand — only the exporter reads its own encoding.

**Importer idempotency keying (closes codex round-14 P2 line 439 — bracket-parallel cursor collision).** Because §6.3.1 runs export brackets in parallel, the importer's idempotency key cannot be `(job_id, cursor)` alone: bracket A can ack a batch with cursor `cA`, bracket B can then overwrite the stored cursor with `cB`, and a retry of A's already-acked batch no longer matches the stored cursor (depending on the importer's matching rule, that either re-applies side effects or drops the retry while ack state diverges). The corrected idempotency key is `(job_id, bracket_id, batch_seq)` where:

- `bracket_id` is the per-bracket identifier the migrator assigns when it slices the export space in §6.3.1 (`Stable for the life of the SplitJob` — recorded in `bracket_progress`).
- `batch_seq` is a monotonic per-bracket counter the migrator advances on every successful ImportVersions ack.

The importer persists `(job_id, bracket_id) → (max_applied_batch_seq, acked_cursor)` under `!migstage|ack|<job_id>|<bracket_id>`; a retry of `(job_id, bracket_id, batch_seq ≤ stored)` is a no-op that returns the stored `acked_cursor`. The cursor is still carried in the ImportVersions request — it is used by the **exporter** to resume after a restart — but it is no longer the dedup key. This separation means cursor semantics can be refined (last-emitted vs last-scanned + tag, above) without touching importer idempotency.

Zero-version progress chunks are first-class batches. When §6.3.1's scan budget is hit after accepting zero rows, the exporter returns `versions=[]`, `done=false`, and a cursor tagged as last-scanned (`scanned_position_tag = 1`). The migrator still sends that chunk through `ImportVersions` with the next per-bracket `batch_seq`; the importer records `(max_applied_batch_seq, acked_cursor)` and returns success without writing MVCC versions or advancing clocks. The migrator then CASes the default-group `SplitJob.bracket_progress` entry to the same `(cursor, last_acked_batch_seq)`. A leader flap may re-export the same sparse window once, but the target-side ack makes the retry idempotent and lets the new migrator advance the durable SplitJob cursor instead of resending the rejected window forever.

The store-side type stays `[]byte` and the exporter alone owns the codec, preserving the §6.1 decoupling seam: the migrator hands the cursor back as opaque bytes on every `ExportVersions` call.

### 6.2 Target side

New method:

```go
// ImportVersions writes the given versions idempotently. The dedup key
// is (jobID, bracketID, batchSeq) — NOT (jobID, cursor) — because §6.3.1
// runs export brackets in parallel and a single cursor slot would be
// clobbered across brackets (§6.1.1). A second call with
// batchSeq ≤ the recorded max for (jobID, bracketID) MUST be a no-op:
// the importer records the per-bracket high-water mark plus acked cursor
// under !migstage|ack|<jobID>|<bracketID> so a network retry doesn't
// double-write or lose scan progress. The opaque `cursor` is the
// exporter's resume token (§6.1.1) — carried so the migrator can persist
// it back on ack, but it is NOT the dedup key. versions may be empty when
// a sparse bracket hits maxScannedBytes before accepting a row; that still
// advances (bracketID, batchSeq, cursor) but performs no MVCC writes and
// no HLC/metaLastCommitTS advancement (§6.2.1). Non-empty imports
// apply tombstones via DeleteAt and non-tombstones via PutAt(value,
// commit_ts, expire_at), then atomically advance metaLastCommitTS, the
// target-local full-HLC floor, and the node-local physical ceiling to at
// least max(commit_ts) of the batch (§6.2.1).
ImportVersions(ctx context.Context, jobID, bracketID, batchSeq uint64,
    versions []MVCCVersion, cursor []byte) ([]byte, error)
```

Idempotency: persist `(jobID, bracketID) → (max_applied_batch_seq, acked_cursor)` under `!migstage|ack|<jobID>|<bracketID>` after each batch's apply (the bracket dimension is required — a single `!migstage|cursor|<jobID>` record would be clobbered by parallel brackets per §6.1.1), and on a duplicate request drop any batch whose `batchSeq` is `≤` the recorded high-water mark while returning the stored `acked_cursor`. The `cursor` is also persisted on the SplitJob's `bracket_progress` entry for exporter resume, separate from the dedup record. Empty batches follow the same ack path; they are not skipped. Non-empty batches preserve the complete MVCC value header relevant to visibility: `tombstone` and `expire_at` are part of the imported version, not recomputed on the target.

#### 6.2.1 Target HLC advancement — preventing post-CUTOVER time travel (closes codex P1)

Codex P1 on PR #945: when the source range contains commit timestamps higher than the target group's current `LastCommitTS` / HLC, an import that only writes versions + cursor leaves the target's clock below the staged data. After CUTOVER the target's first new write at `Next() = max(wall, ceiling)` can therefore receive a `commit_ts` **smaller** than imported values — MVCC visibility then resurrects the pre-cutover value on a snapshot read at the new commit_ts, an unambiguous time-travel hazard.

The fix runs inside `ImportVersions`. The later `PromoteStaged` apply only copies already-imported versions from staged keys into live keys with their original `commit_ts`, `tombstone`, and `expire_at` header; it must not mint or observe a new timestamp.

1. **Per-batch advance for non-empty batches.** On every apply of an `ImportVersions` batch with at least one version, the target FSM computes `batchMax = max(versions[i].commit_ts)` and **atomically** (under the same FSM apply lock that mutates the MVCC store):
   - sets `metaLastCommitTS = max(metaLastCommitTS, batchMax)`,
   - sets target-local `!migstage|hlc_floor|<jobID> = max(current_floor, batchMax)` — this record stores the **full** HLC value, including the logical lower 16 bits, and lives in the target group's Raft state / snapshots rather than only in the default-group SplitJob,
   - calls `hlc.SetPhysicalCeiling(hlcPhysicalMs(batchMax))` — the **physical-ms component only**, not the full HLC `commit_ts` (closes codex round-5 P1 on PR #945 line 367). `SetPhysicalCeiling`'s signature is `int64 ms` (`kv/hlc.go:222`) and is interpreted as Unix milliseconds; passing the full encoded ts `commit_ts = ms<<16 | logical` would advance the ceiling to ~`commit_ts<<16` worth of milliseconds — `2^16 ≈ 65 536` × further in the future than intended — and weaken the lease-ceiling fence until wall time catches up. The conversion is `hlcPhysicalMs(ts) = int64(ts >> 16)` (the same shift `HLC.Next()` uses to extract the physical half, `kv/hlc.go:128-145`), AND
   - calls `hlc.Observe(batchMax)` so the current leader's in-memory `last` also tracks the full high-water mark — keeps `Next()`'s logical-half advancement above the imported versions while this process stays alive.

   `SetPhysicalCeiling` is monotone — a lower argument is a no-op — so duplicate / out-of-order batches are safe. `Observe` is similarly monotone.

   Empty `versions=[]` progress chunks have no `batchMax`. They still advance the per-bracket `(batch_seq, cursor)` ack described in §6.1.1 / §6.2, but they MUST NOT change `metaLastCommitTS`, target-local `!migstage|hlc_floor|<jobID>`, call `SetPhysicalCeiling`, call `Observe`, or update `SplitJob.max_imported_ts`. This keeps sparse-bracket progress durable without inventing a timestamp for a batch that imported no committed version.
2. **Two monotone witnesses with different owners.** The default-group SplitJob carries `max_imported_ts` (§3.1) — the high-water mark of all non-empty ack'd import batches for this job. The migrator updates it whenever it advances a bracket's `cursor` / `last_acked_batch_seq` in `bracket_progress` on ack of a non-empty batch (§6.1.1), so the coordinator can reason about CUTOVER. The target group owns the enforcement copy under `!migstage|hlc_floor|<jobID>`; target startup and snapshot restore must be correct even if the default-group leader is unavailable.
3. **CUTOVER precondition.** Before the migrator issues the CUTOVER catalog write, it ensures the target group's durable full-HLC floor is at least `max_imported_ts` by issuing a final target-group proposal (not default-group) that updates `!migstage|hlc_floor|<jobID> = max(current_floor, max_imported_ts)`, calls `SetPhysicalCeiling(hlcPhysicalMs(max_imported_ts))`, and calls `Observe(max_imported_ts)`. The proposal is gated by `cap_migration_v2` (§11.1) and is the **last** target-side write before CUTOVER. If the target HLC is already above this value (e.g. due to per-batch advancement), the proposal is a no-op except for confirming the durable floor; if a follower flap or snapshot restore dropped in-memory `HLC.last`, this proposal closes the live-leader gap before the catalog is published.
4. **Restart/snapshot invariant.** `metaLastCommitTS` is not enough: current `HLC.Next()` reads in-memory `HLC.last` and the physical ceiling, but does **not** consult `metaLastCommitTS`. A restored node that only replays `SetPhysicalCeiling(hlcPhysicalMs(max_imported_ts))` can issue `(ms<<16)|0` even when an imported version was `(ms<<16)|logical>0`. Therefore, on target FSM startup, snapshot restore, and leadership acquisition before serving writes for any route whose migration high-water mark is non-zero, the target computes `fullFloor = max(store.LastCommitTS(), all !migstage|hlc_floor|*)`, then calls `hlc.SetPhysicalCeiling(hlcPhysicalMs(fullFloor))` and `hlc.Observe(fullFloor)`. Re-observing the **full** HLC value is the preferred design because it preserves logical-half monotonicity without artificially jumping the physical ceiling by one millisecond; if an implementation cannot load the full value, the fail-safe alternative is `SetPhysicalCeiling(hlcPhysicalMs(fullFloor)+1)` before accepting writes.
5. **Post-CUTOVER write-side invariant, including caller-supplied timestamps.** A target-group `Next()` after CUTOVER is strictly greater than every imported `commit_ts` because the live HLC has observed the durable full floor before writes are admitted. Reads at any post-CUTOVER `Next()` therefore see imported versions as historical and a new live write as the most-recent committed version — no resurrection, no time travel.

   This `Next()` invariant is not enough for adapters that pass a caller-supplied `commit_ts` through `resolveDispatchCommitTS` unchanged. While a target route has `staged_visibility_active=true`, the target FSM must also enforce a write-side floor before any live MVCC write or prepare lock is created: for the route's `migration_job_id`, load the target-local full-HLC floor `!migstage|hlc_floor|<job_id>` (which is `>= max_imported_ts`) and reject any raw write, one-phase txn, or prepared txn whose supplied / already-allocated `commit_ts <= floor` with `ErrMigrationTimestampTooLow` (mapped to a retryable coordinator error that refreshes and reallocates unless the caller explicitly requires that timestamp). This check runs on the target apply path next to the staged/live merge flag, not only in the coordinator, so a stale or direct RawKV caller cannot insert a low live version below an already-imported staged version. Coordinator-allocated writes pass naturally because `Next()` has observed the full floor; the rejection exists only for externally supplied or replayed low timestamps. Once promotion completes and CLEANUP clears `staged_visibility_active`, the route becomes ordinary MVCC again and no migration-specific timestamp floor remains.
6. **Same-group split (M1) unaffected.** Same-group split never crosses the FSM apply boundary, so its `Next()` is already bounded by the source's HLC; the new advance path is a no-op when `target_group_id == source_group_id`.

This reuses the same Raft-agreed monotone physical-ceiling primitive that PR #927 / Composed-1 already relies on, plus a small target-local full-HLC floor for the logical half. Test coverage: §10.1 unit gains an explicit `kv/migrator_hlc_advance_test.go` that asserts (a) `metaLastCommitTS`, target-local `!migstage|hlc_floor|<jobID>`, and HLC ceiling are advanced by `max(batch.commit_ts)`, (b) the CUTOVER pre-write proposal closes any gap left by a missing batch, (c) restart/snapshot restore re-observes a full `max_imported_ts = ms<<16|logical` so the first post-restore `Next()` is `> max_imported_ts`, and (d) a control that restores only the physical ceiling can emit `ms<<16|0` and fails. Property test extends the §10.1 export-chunks/import-acks/leader-flap sequence with a `Next()` invocation after CUTOVER and after snapshot restore, asserting strict monotonicity vs. every imported ts.

### 6.3 Internal-key coverage and the routeKey delegate

§9 of the parent doc enumerates the key families. The migrator builds a `KeyFilter` closure in `kv/` (where `routeKey()` lives) and passes it to `store.ExportVersions`:

```go
// kv/migrator_filter.go (sketch)
func RouteKeyFilter(rangeStart, rangeEnd []byte) store.KeyFilter {
    return func(rawKey []byte) bool {
        rKey := routeKey(rawKey)
        // half-open [rangeStart, rangeEnd) in the routing-key namespace.
        if bytes.Compare(rKey, rangeStart) < 0 { return false }
        if len(rangeEnd) > 0 && bytes.Compare(rKey, rangeEnd) >= 0 { return false }
        return true
    }
}
```

The closure ensures internal keys (`!txn|...`, `!lst|...`, Redis collection families, etc.) land in the same shard as their logical owner without `store` ever importing `kv`. `rangeEnd` uses the wire convention from §5.2: nil or empty means `+infinity`. The `len(rangeEnd) > 0` check is therefore part of the correctness contract, not just a convenience — an empty-but-non-nil slice must not make every non-empty `routeKey(rawKey)` compare `>= rangeEnd` and reject the entire rightmost route. The importer dispatches by `key_family` into the matching store helper (in a future kv-level `Import` wrapper) to preserve per-family invariants (e.g., list head pointer updates).

#### 6.3.1 Internal-family brackets — raw range alone misses internal state (closes codex P1 line 310)

The above closure correctly *rejects* a raw key that does not belong to the moving range, but it cannot *find* internal-family keys that the caller never iterates over in the first place. Codex P1 round-4 on PR #945 (line 310): for a moving user-key interval `[foo, bar)`, an iteration over the raw range `[foo, bar)` never visits `!txn|lock|foo`, `!lst|meta|foo`, `!redis|str|foo`, `!ddb|item|<tbl>|foo`, `!ddb|meta|table|<tbl>`, `!ddb|meta|gen|<tbl>`, `!sqs|...`, `!s3|...`, because each of those prefixes sorts **outside** `[foo, bar)` in the raw MVCC key space — the routeKey filter rejects nothing because those bytes were never iterated. Without the fix below, CUTOVER would leave intent locks, list metadata, DynamoDB table schema / generation, and adapter-private state behind on the source and the target would serve reads against a half-populated MVCC space — silent data loss.

M2 therefore runs **multiple parallel exports per migration**, one per relevant raw-key family, each driven by the same `KeyFilter` so `routeKey()` remains the single source of truth for "does this key belong to the moving range?". The migrator computes the bracket list from the moving range's logical scope:

```go
// kv/migrator_export_plan.go (sketch)
//
// Each bracket is one (start, end) raw-key band the migrator
// ExportVersions over. The KeyFilter rejects any key whose
// routeKey() falls outside the moving range — necessary because
// each bracket below is FAMILY-WIDE (every txn lock cluster-wide,
// not just the moving range's), and the filter narrows it back to
// the moving slice.
type ExportBracket struct {
    Start, End []byte        // raw key bounds for ExportVersions
    Family     uint32        // MVCCVersion.key_family tag
    ExcludeKnownInternal bool // true only for familyUser
}

func PlanExportBrackets(routeStart, routeEnd []byte) []ExportBracket {
    return []ExportBracket{
        // User-key bracket itself, in the routing-key namespace. This bracket
        // rejects any raw key with a known internal prefix; those rows are
        // exported exactly once by the explicit family brackets below.
        {Start: routeStart, End: routeEnd, Family: familyUser, ExcludeKnownInternal: true},
        // Each internal-family raw-key band. routeKey() decodes the
        // family-prefixed raw key back to its routing key, so the
        // filter rejects any entry whose routing key falls outside
        // [routeStart, routeEnd).
        {Start: txnLockPrefix(),  End: txnLockPrefixEnd(),  Family: familyTxnLock},
        {Start: txnCmtPrefix(),   End: txnCmtPrefixEnd(),   Family: familyTxnCommit},
        {Start: txnRbPrefix(),    End: txnRbPrefixEnd(),    Family: familyTxnRollback},
        {Start: txnMetaPrefix(),  End: txnMetaPrefixEnd(),  Family: familyTxnMeta},
        {Start: txnIntPrefix(),   End: txnIntPrefixEnd(),   Family: familyTxnInternal},
        {Start: lstMetaPrefix(),      End: lstMetaPrefixEnd(),      Family: familyListMeta},
        {Start: lstMetaDeltaPrefix(), End: lstMetaDeltaPrefixEnd(), Family: familyListMetaDelta},
        {Start: lstItmPrefix(),       End: lstItmPrefixEnd(),       Family: familyListItem},
        {Start: lstClaimPrefix(),     End: lstClaimPrefixEnd(),     Family: familyListClaim},
        {Start: redisPrefix(),         End: redisPrefixEnd(),         Family: familyRedisLegacy}, // !redis|...
        {Start: redisHashWidePrefix(), End: redisHashWidePrefixEnd(), Family: familyRedisHash},   // !hs|...
        {Start: redisSetWidePrefix(),  End: redisSetWidePrefixEnd(),  Family: familyRedisSet},    // !st|...
        {Start: redisZSetWidePrefix(), End: redisZSetWidePrefixEnd(), Family: familyRedisZSet},   // !zs|...
        {Start: streamMetaPrefix(),  End: streamMetaPrefixEnd(),  Family: familyRedisStreamMeta},
        {Start: streamEntryPrefix(), End: streamEntryPrefixEnd(), Family: familyRedisStreamEntry},
        {Start: ddbTableMetaPrefix(), End: ddbTableMetaPrefixEnd(), Family: familyDdbTableMeta},
        {Start: ddbTableGenerationPrefix(), End: ddbTableGenerationPrefixEnd(), Family: familyDdbTableGeneration},
        {Start: ddbItemPrefix(),  End: ddbItemPrefixEnd(),  Family: familyDdbItem},
        {Start: ddbGSIPrefix(),   End: ddbGSIPrefixEnd(),   Family: familyDdbGSI},
        {Start: sqsPrefix(),      End: sqsPrefixEnd(),      Family: familySqs},
        {Start: s3Prefix(),       End: s3PrefixEnd(),       Family: familyS3},
        // A future adapter MUST add its bracket here at the same PR
        // that teaches routeKey() about its family — both are
        // required to land internal-family migration end-to-end.
    }
}
```

The bracket plan is **disjoint by construction**. `familyUser` is not "whatever bytes fall in `[routeStart, routeEnd)`"; it is user MVCC rows after excluding `knownInternalPrefixes` (`!txn|`, `!redis|`, `!lst|`, `!hs|`, `!st|`, `!zs|`, concrete stream storage prefixes `!stream|meta|` and `!stream|entry|`, `!ddb|`, `!sqs|`, `!s3|`, `!dist|`, `!migstage|`, `!migwrite|`, `!migfence|`, etc.). Every excluded prefix must either have an explicit family bracket below or be a reserved control prefix that `SplitRange` rejects up front. This prevents the user bracket from exporting the same `(raw_key, commit_ts)` that an internal-family bracket exports; importer idempotency is per `(job_id, bracket_id, batch_seq)` and is not allowed to mask cross-bracket duplicates.

M2 also extends `routeKey()` for every store-owned collection prefix that persists live user-visible state. Today `routeKey()` handles the `!redis|...` family and base list meta/item keys, but list reads and POP protection also consume `store.ListMetaDeltaPrefix` (`!lst|meta|d|`) and `store.ListClaimPrefix` (`!lst|claim|`). The decoder must therefore check `store.ExtractListUserKeyFromDelta` and `store.ExtractListUserKeyFromClaim` before the broader base-meta `store.ExtractListUserKey` path; `!lst|meta|d|` has `!lst|meta|` as a byte prefix, so the order is load-bearing. Current hash/set/zset/stream state is stored under `store.HashMetaPrefix`, `store.HashFieldPrefix`, `store.HashMetaDeltaPrefix`, `store.SetMetaPrefix`, `store.SetMemberPrefix`, `store.SetMetaDeltaPrefix`, `store.ZSetMetaPrefix`, `store.ZSetMemberPrefix`, `store.ZSetScorePrefix`, `store.ZSetMetaDeltaPrefix`, `store.StreamMetaPrefix`, and `store.StreamEntryPrefix`. Those layouts carry the logical Redis user key after a length-prefixed segment, so the decoder must call the matching `store.ExtractHashUserKeyFrom{Meta,Field,Delta}`, `store.ExtractSetUserKeyFrom{Meta,Member,Delta}`, `store.ExtractZSetUserKeyFrom{Meta,Member,Score,Delta}`, and `store.ExtractStreamUserKeyFrom{Meta,Entry}` helpers. The `KeyFilter` then accepts an internal-family raw key whose routing key falls inside the moving range and rejects others. Adding a bracket without a matching `routeKey()` decoder is a correctness bug, and the export-plan tests below cover both halves together.

List export includes base meta, meta-delta, item, and claim families because all four can affect the post-CUTOVER value or OCC behavior of a Redis list. Hash/set/zset use non-overlapping umbrella brackets (`!hs|`, `!st|`, `!zs|`) rather than separate meta/delta brackets because their delta prefixes are nested under the meta prefix (`!hs|meta|d|` under `!hs|meta|`, etc.). The import dispatch still inspects the raw subprefix inside the bracket to preserve per-family invariants. Stream uses its two concrete prefixes (`!stream|meta|`, `!stream|entry|`) instead of an umbrella `!stream|` bracket, matching `knownInternalPrefixes`: a user key such as `!stream|foo` is legal and must remain a user-key row, while only the concrete meta/entry prefixes are internal stream storage.

The Redis collection brackets are mandatory, not an optimization. Redis strings, TTL rows, HLL rows, and legacy/blob collection state use `!redis|...`; list state spans `!lst|meta|`, `!lst|meta|d|`, `!lst|itm|`, and `!lst|claim|`; modern hash/set/zset state lives under the store-owned `!hs|`, `!st|`, and `!zs|` prefixes, while stream state uses only the concrete `!stream|meta|` and `!stream|entry|` prefixes above. Copying only `!redis|...` during a cross-group split would make the target serve empty or partial `LRANGE`, `HGETALL`, `SMEMBERS`, `ZRANGE`, and `XRANGE` results after CUTOVER, and omitting list claims can lose POP claim protection. Stream metadata and stream entries both have to be copied: `KEYS` may expose a stream through metadata, but migration must preserve every entry for `XRANGE` / `XREAD`. A user key named `!stream|foo` remains in `familyUser` and must not be captured by an umbrella stream bracket.

The DynamoDB metadata brackets are mandatory, not an optimization. `routeKey()` maps `!ddb|meta|table|<table>` and `!ddb|meta|gen|<table>` to the same `!ddb|route|table|<table>` route as `!ddb|item|<table>|...` and `!ddb|gsi|<table>|...` (`kv/shard_key.go`). The adapter reads those keys directly in `loadTableSchemaAt` and `loadTableGenerationAt` (`adapter/dynamodb.go`), so copying item/GSI rows without schema/generation leaves the target with data that DynamoDB treats as a missing table or generation 0. A future DynamoDB storage family must therefore add both a `routeKey()` decoder and a bracket in the same PR.

The migrator runs the brackets **in parallel up to `--migrationExportFanout` (default 4)** within a single phase (BACKFILL or DELTA_COPY): each bracket gets its own opaque cursor (the §6.1.1 codec is extended to key on `(family, raw_key, commit_ts)`), so a bracket can be paused/resumed independently and the §9 resumability matrix applies per bracket. The migrator records per-bracket `cursor` and `done` flags on the SplitJob; the phase advances only when **every bracket** reports `done = true`. Cost is at most `fanout`× the gRPC frame budget and is bounded by the static bracket list, not by data volume.

**Bounded scan for sparse / out-of-range brackets (closes codex P2 round-5 line 445).** "Returns its first chunk empty and `done=true` immediately" is only true when the iterator can short-circuit; under the §6.1 contract, `accept` rejections do **not** count against `chunkBytes` (so the caller sees deterministic chunk shapes), which would otherwise let a family-wide raw range scan an arbitrarily large prefix to prove `done=true` for a moving range with no intersection. The fix has two layers:

- **Scan-budget pacing.** Each `ExportVersions` call carries a separate scan budget — the `maxScannedBytes` parameter on the store-layer signature above, carried on the wire as `ExportRangeVersionsRequest.max_scanned_bytes` (default 4 × `chunkBytes`; `0` on the wire means "use the server default", mirroring how the migrator leaves other tuning fields unset) — that **counts both accepted and rejected rows** as iteration cost, where `chunkBytes` alone counts only accepted rows. The internal RPC server translates wire `0` to `4 × chunkBytes` before calling `store.ExportVersions`, and the store repeats the same `<= 0` normalization defensively so a sparse family bracket can never become unbounded by accidentally passing the wire sentinel through. When the iterator hits that budget without filling the accepted chunk, it returns whatever it has accepted so far + the next cursor (over the *rejected* tail position — the §6.1.1 `scanned_position_tag = 1` case, so the next call resumes strictly past the already-rejected window). `done = false`, so the next chunk picks up where the rejection scan left off. Without this field on the contract, a Redis migration where the SQS family has a cluster-wide 10 GB raw prefix would block on a single `ExportVersions` call for the entire prefix duration.
- **Route-key sub-prefix indexing where the family layout allows it.** Each internal-family bracket's raw key starts with a known fixed prefix (e.g. `!txn|lock|`, `!lst|meta|`) followed by the routing-key bytes (mostly — adapters like SQS encode queue ID first; per-family `EncodeBracketStart(routeStart)` is the per-family hook). When such a hook is available, the bracket's `Start` / `End` are tightened to `(familyPrefix || EncodeBracketStart(routeStart), familyPrefix || EncodeBracketStart(routeEnd))` instead of the family-wide bounds, reducing the scan budget consumption from "full family" to "moving slice within family." Families without an encoder hook (initially `!s3|...`, until the design records its routing-key encoding) keep the family-wide bracket + the scan-budget pacing above; the scan-budget alone is sufficient for correctness, the encoder hook is a per-family optimization that lands in the matching adapter's PR.

These two layers together preserve the §9 resumability matrix per bracket (cursor still advances through both accepted and rejected rows; a leader flap resumes at the persisted scan cursor) and bound the per-chunk wall-clock cost regardless of how sparse the bracket happens to be. The `keys per bracket` distribution AND `rejected_rows per bracket` are §7.3 / §11.x metrics so operators can spot a runaway family scan and prioritize adding an encoder hook for that adapter.

A simple safety check: every exported key in the migrator's send buffer must map back to the moving range under `routeKey()`. The migrator asserts this on every row before the gRPC send; an assertion failure aborts the job and surfaces in `last_error`. The assertion now guards two failure modes: (i) a future internal-family being added that someone forgot to teach `routeKey()`, and (ii) a bracket being added without a matching `routeKey()` decoder. Reuses the same routeKey assertion §6.3 already specifies — no new code surface beyond the bracket list.

### 6.4 Incremental staged-to-live promotion — CUTOVER is constant-time (closes codex P2 on PR #945)

Codex P2 round-3 on PR #945 (line 147): "staging every imported row under `!dist|migstage|...` and then promoting the whole range via a single FSM apply makes CUTOVER proportional to the entire migrated range. For any split larger than the Raft proposal/apply budget, this defeats the chunked BACKFILL/DELTA_COPY design and can block or fail exactly when the catalog must switch atomically." The proposal must therefore avoid a per-key bulk move at cutover. M2 ships an incremental promotion path that keeps CUTOVER a constant-time catalog write while preserving the visibility fence:

0. **Target staged-readiness barrier before the target can become Active.** After DELTA_COPY completes and after the final target full-HLC floor proposal in §6.2.1, but before the default-group CUTOVER CAS can publish `target.Active(right)`, the migrator persists `target_staged_readiness_state = ARMING` on the default-group SplitJob and proposes `ApplyTargetStagedReadiness(job_id, route_start, route_end, expected_cutover_version, migration_job_id, max_imported_ts)` to the target group. The target FSM persists `!migstage|ready|<job_id>` in its own Raft state/snapshots and treats that record as a fail-closed guard for the moving route interval.

   While the readiness record is armed, any target read or write whose route-key-normalized point/range intersects `[route_start, route_end)` must prove one of two things before touching live MVCC: either the target's local route snapshot contains `staged_visibility_active=true` with the matching `migration_job_id` and catalog version, or the default-group ownership lookup says the CUTOVER has not committed yet and the request should retry. If neither proof is available (watcher lag, ownership RPC timeout, snapshot restore before route replay), the target returns `ErrRouteCutoverPending` / `ErrRouteOwnershipUnknown` and **must not** read live-only data or accept a live write for the interval. This closes the target-side mirror of §7.2.2e: a fresh coordinator may learn the new target route from the default group before the target FSM has applied the same descriptor locally, so the target must fail closed until it can run the staged/live merge and timestamp floor.

   After the target apply ACK, the migrator CASes the SplitJob to `target_staged_readiness_state = ARMED`; a restart at `ARMING` probes/re-proposes the same target record idempotently. A pre-CUTOVER rollback transitions through `CLEARING` and removes the target readiness record before returning to DELTA_COPY/FAILED. On the successful path the record remains until after the default-group promotion-complete witness has cleared `staged_visibility_active` and the target observes the cleared descriptor; only then can target-local cleanup delete `!migstage|ready|<job_id>` together with the HLC floor and promotion proof (§6.4 step 5).

1. **CUTOVER itself is one catalog write — no per-key work.** The CUTOVER FSM apply on the default group does **only**:
   - (a) CAS-bump the catalog version,
   - (b) remove the source's `WriteFenced(right)` route,
   - (c) insert the target's `Active(right)` route with `raft_group_id = target`,
   - (d) populate `cutover_version` on the SplitJob and stamp `staged_visibility_active = true` on the new target route.

   No iteration over the migrated key range, no bulk rename, no per-key proposals. The Raft proposal carrying this apply is `O(1)` in the migrated data size — bounded by a handful of catalog descriptors and the SplitJob record.

2. **Staged keyspace participates in MVCC merge — raw newest candidate wins (closes codex P1 line 408 and codex round-18 P1 line 705).** After CUTOVER the target FSM's read path treats the staged area as **a second source of raw MVCC versions for the same logical key**, not as an authoritative override and not as a reader-visible fallback. For a read for key `K` at `read_ts` in the moved range with `staged_visibility_active = true`:
   - read the **raw newest live MVCC candidate of `K` with `commit_ts ≤ read_ts`**, including its `tombstone` and `expire_at` header, without applying the read-visible filter that hides deletes / expired TTL values;
   - read the **raw newest staged MVCC candidate of `K` with `commit_ts ≤ read_ts`** under `!dist|migstage|<job_id>|<K>` with the same raw header fields;
   - choose the candidate with the greater `commit_ts` across both sources. If neither exists, return "not found". If the winning candidate has `tombstone=true`, return "not found". If the winning candidate has `expire_at != 0 && expire_at <= read_ts`, return "not found". Only otherwise return the winning candidate's value.

   The "raw candidate first, visibility filter after the cross-source max" ordering is required during partial promotion. A standard snapshot read would treat a live tombstone / expired version as absent, then incorrectly fall back to an older staged value. Example: staged has `K@10=value` and `K@20=tombstone`; the promoter copies/deletes the `K@20` row into live first and deletes that staged row, while staged `K@10` remains. At `read_ts >= 20`, the raw merge sees live `K@20` as the winner and returns not found. A reader-visible live lookup would see "absent" and resurrect staged `K@10` until the promoter reaches it.

   This is the standard MVCC visibility rule applied **after** combining two column-family-like sources, not a "stage-shadows-live" fallback. The earlier wording — "first look at staged, fall back to live when staged is absent" — was wrong precisely because §6.2.1's HLC floor guarantees a live write after CUTOVER has `commit_ts > max_imported_ts`, so for any `read_ts ≥ live_write.commit_ts` the live version is the most-recent committed value and a `staged-first-then-live-fallback` would return the older staged value while the staged entry still exists — exactly the visibility regression codex P1 line 408 flagged. The raw-merge form is correct because:
   - For `read_ts < live_write.commit_ts`: live has no raw candidate ≤ read_ts (or only an older one), staged's imported `commit_ts ≤ max_imported_ts < live_write.commit_ts`, so the merge correctly surfaces whichever visible winning value is newest at `read_ts` (typically the staged one for `read_ts ∈ (max_imported_ts, live_write.commit_ts)`).
   - For `read_ts ≥ live_write.commit_ts`: the live version's `commit_ts > max_imported_ts ≥ every staged version's commit_ts`, so the merge correctly returns the live value unless that live winner is itself a delete/expired value, in which case it correctly returns not found without falling back.
   - For never-overwritten keys post-CUTOVER: only the staged raw candidate exists ≤ read_ts, so the merge returns it if visible and returns not found if the staged winner is a tombstone/expired TTL version.
   - For a key with multiple staged versions (the §6.1.1 hot-key case): the staged raw-candidate iterator returns the newest staged version ≤ read_ts, the live raw-candidate iterator returns the newest live version ≤ read_ts, and the merge applies visibility only to the greater of the two. Correct in every interleaving, including partial promotion of hidden versions.

   Writes always land in the live keyspace — the CUTOVER bump already routed writers to the target group via the catalog, and §6.2.1's full-HLC floor guarantees their `commit_ts` is strictly greater than every imported `commit_ts`, so a live write shadowing a staged version is the *correct* MVCC visibility AND the merge rule above surfaces it correctly. This gives reads access to the imported data the instant CUTOVER lands, with **no promotion work blocking CUTOVER itself**, and with no staleness window where staged shadows a fresher live write.

   Per-family adapter helpers (Redis list head pointers, DynamoDB GSI shadow rows, etc.) follow the same merge rule on the corresponding internal-family keys — the merge operates on raw keys, so the per-family invariants the `key_family` dispatch (§6.3.1) already enforces still apply.

   **2a. Range-scan merge iterator (closes codex round-14 P1 line 562).** The point-read rule above generalises to range reads via a **raw-candidate merged iterator**, not by scanning live alone and not by merging only reader-visible rows. For `ScanAt(read_ts, scan_start, scan_end)` or `RawScanAt(...)` on a target route with `staged_visibility_active = true` and `scan` overlapping the route's `[routeStart, routeEnd)`, the target FSM constructs a merged iterator that:
   - walks raw live MVCC candidates in `[scan_start, scan_end)` at `read_ts`, including tombstones and TTL headers, AND
   - walks raw staged MVCC candidates under `!dist|migstage|<migration_job_id>|<encode(scan_start)..encode(scan_end))` at `read_ts`, with the same headers and raw-key window,
   - and merges by `(logical_key, commit_ts)` in `(raw_key ASC, commit_ts DESC)` order. For each logical key, the merged iterator chooses the newest candidate (`max commit_ts ≤ read_ts`) from across **both** sources, then emits it only if the winner is neither a tombstone nor expired at `read_ts`. If the newest candidate is hidden, the iterator suppresses that logical key and MUST NOT emit an older candidate from the other source.

   The merge is implemented in `store/staged_merge_iterator.go` (added in M2-PR6 alongside the staged-merge point read). It extends the existing MVCC iterators/decoders with a raw-candidate mode — no new ordering primitive — and its memory cost is the standard two-cursor merge, not buffering the full result set. The iterator honours adapter pagination (DynamoDB `Limit` / `ExclusiveStartKey`, Redis `SCAN` cursor, gRPC `RawScan` limits) by streaming until the limit hits, exactly like the existing iterators.

   Why this matters: without the merge iterator, a post-CUTOVER `Scan(ScanStart, ScanEnd)` on the moved range would walk only the **live** keyspace — empty for any range that hasn't been promoted yet. DynamoDB `Scan`/`Query`, Redis `KEYS`/`SCAN`/range `LRANGE`-equivalent (where the underlying key is range-scanned for the list head's encoded position), and the gRPC `RawScan` API would all return empty or partial results in the window between CUTOVER and the promoter's catch-up — a user-visible data-disappearance bug. The merged iterator closes that window: as long as `staged_visibility_active = true`, range reads see the same union of staged + live versions that point reads do, so the user-observable state is correct from CUTOVER onward. Once the promoter clears the staged prefix and CLEANUP toggles `staged_visibility_active = false` (step 5), the merged iterator falls back to live-only and the route is indistinguishable from any other Active route.

3. **Background incremental promoter on the target group.** Immediately after CUTOVER the target group's FSM starts a **leader-local promoter goroutine** that walks the `!dist|migstage|<job_id>|*` prefix in cursor-resumable chunks (the same `chunkBytes` / pacing knobs §6.1 and the migrator already use). Promotion progress is target-local Raft state under `!migstage|promote|<job_id>`, never a cursor in the default-group SplitJob. For each staged batch the leader proposes one `PromoteStaged` Raft entry that:
   - copies the staged key into its live position with the original imported `commit_ts` — **always**, regardless of whether a newer live version at a higher `commit_ts` exists. The MVCC store naturally holds both `K@commit_ts=3` (the staged version being promoted) and `K@commit_ts=10` (a later live write) at different commit timestamps; a snapshot read at any `read_ts ∈ [3, 10)` correctly sees the imported `K@3`, and a read at `read_ts ≥ 10` correctly sees `K@10`. The idempotency guard for this copy is "**no-op if a live version at the same `commit_ts` already exists**" — that means this exact staged version was already promoted in a prior batch before a leader flap re-tried it. It is **not** "no-op when a newer live version exists," which would silently drop the imported version from MVCC history and make snapshot reads at older `read_ts` return wrong results (closes claude round-3 P1, 5-round flag). History pruning belongs to `kv/compactor.go`, not the promoter,
   - physically deletes the exact staged MVCC row it just copied, identified by `(job_id, raw_key, commit_ts)` under `!dist|migstage|<job_id>|*`. This is a storage-level remove of the staged version / control row, not a user-visible `DeleteAt` tombstone with a fresh timestamp. Leaving a tombstone in the staged prefix would keep the merge iterator and empty-prefix proof non-empty forever, and a normal MVCC tombstone could hide older staged rows incorrectly during partial promotion,
   - advances the target-local `PromotionState.promote_cursor`.
   Each batch is a small bounded proposal, exactly like BACKFILL — there is no single oversized apply.

4. **Promotion is restart-safe.** The promoter is leader-only; a leader flap restarts from the target-local `PromotionState.promote_cursor`. Because the cursor update, live copy, and staged delete happen in the same target-group Raft apply, a crash cannot persist a cursor that skips rows whose promotion did not durably apply. Concurrent reads keep using the raw-candidate staged/live merge path (step 2) for whatever has not yet been promoted, so the user-visible state is correct throughout.

5. **CLEANUP → DONE precondition and target-local cleanup.** The target group first completes promotion locally: a bounded target-group Raft apply verifies the staged prefix is empty and marks `!migstage|promote|<job_id>.done = true` while retaining the import acks, HLC floor, readiness record, and promotion proof as recovery evidence. Only after the migrator observes that target-local `done=true` report does it issue a default-group promotion-complete CAS that clears `staged_visibility_active` / `migration_job_id` from the target route and records `target_promotion_done = true` plus `promotion_completed_ts`; the SplitJob remains in CLEANUP until the source and target cleanup ACKs below are durable. This ordered two-owner handoff is required: the default group must never clear the merge flag based on a cursor it owns, because a crash between target and default writes could otherwise skip unpromoted staged rows or hide staged rows before the target copy is durable. If the target completion apply lands but the default CAS does not, reads still pay the merge cost over an empty staged prefix until the migrator retries the CAS; correctness is preserved.

   After that default-group promotion-complete witness is durable and the target has observed the cleared route descriptor, the migrator runs a bounded target-local cleanup that deletes per-bracket import-dedup records under `!migstage|ack|<job_id>|*`, `!migstage|hlc_floor|<job_id>`, `!migstage|ready|<job_id>`, and `!migstage|promote|<job_id>`. Deleting these before the default witness is forbidden: a restart between target completion and default CAS would otherwise lose the proof needed to decide whether the merge flag can be cleared safely. Deleting them after the witness is safe because no route with `staged_visibility_active=true` references the job, and future target writes no longer need the migration timestamp floor. Only after this target-local cleanup ACK and §6.5's source disarm ACK are durable does the default group move the job to DONE history.

6. **Edge cases.**
   - **Hot key with many versions** (codex P2 on cursor granularity, §6.1.1): the promoter iterates the staged prefix in `(raw_key ASC, commit_ts DESC)` order using the same opaque-cursor codec; the raw-candidate staged/live merge path treats a key with multiple staged versions identically to the MVCC iterator. No version chain is materialised in one apply.
   - **Concurrent writes during promotion**: a live write at `commit_ts > max_imported_ts` lands ahead of the staged version it shadows. The MVCC store holds **both** `K@commit_ts=staged_ts` (the promoted staged version) **and** `K@commit_ts=live_ts` (the live write) at different commit timestamps — there is no overwrite, only **version coexistence** (closes claude round-8 wording nit). `metaLastCommitTS` advances to track the Raft commit sequence on the live side but does **not** block `PromoteStaged`'s historical insert; phrasing this as "keeps the staged copy from overwriting" was misleading because it could be misread as a conditional gate that skips the staged copy entirely — the exact §6.4-step-3 bug the round-3 fix removed. The §6.4-step-2 merge read at any `read_ts ≥ live_write.commit_ts` returns the live value (newest wins); at any `read_ts ∈ (max_imported_ts, live_write.commit_ts)` returns the imported value (live has no version `≤ read_ts` at that point; staged has one). Both correct.
   - **AbandonSplitJob after CUTOVER** is still rejected (§4); abandoning post-CUTOVER would leave half-promoted state visible.

7. **Per-PR landing.** M2-PR6 ships the raw-candidate staged/live merge read path + `PromoteStaged` apply (mechanics); M2-PR7 ships the background promoter + CLEANUP precondition. PR4-PR5 land the catalog flag and the SplitJob fields needed by PR6/PR7 so the wire is in place before the runtime turns on. The incremental design is therefore a strict superset of the chunked BACKFILL/DELTA_COPY semantics — the same chunk sizing, the same cursor codec, the same leader-flap recovery — applied to the promotion step instead of one bulk apply at CUTOVER.

The result: CUTOVER is a single bounded catalog write, never `O(migrated range)`, and the visibility fence is preserved by the per-route flag plus the raw-candidate staged/live merge. The migrator still gets resumable, bounded apply costs for the actual data move; the catalog switch itself is decoupled from the data volume. This closes codex P2 line 147 on PR #945 without weakening any consistency property already established by §6.2.1 (HLC floor) or §7.2 (read fence).

### 6.5 Source cleanup uses the same disjoint bracket plan

After the read-fence grace window (§7.2.4) and only after target promotion has made the moved range durable on the target, the source cleanup does **not** delete only the route's raw `[routeStart, routeEnd)` user-key interval. Internal families have the same raw-vs-routing split as export: DynamoDB metadata/items, Redis collection rows, list claims, SQS/S3 state, and txn records can all sort outside the logical route interval while still belonging to the moved route. Cleanup therefore reuses `PlanExportBrackets(routeStart, routeEnd)` with the same `RouteKeyFilter` and disjoint `familyUser` internal-prefix exclusion from §6.3.1.

Each source cleanup batch is a source-group Raft proposal that scans one bracket with the same `maxScannedBytes` pacing and deletes only versions whose `commit_ts <= fence_ts` for rows accepted by the route-key filter. The cursor is bracket-scoped and persisted in the SplitJob, exactly like export progress. Unknown internal prefixes or reserved migration-control prefixes fail closed: they are either rejected by `SplitRange` before job creation (§3.2) or left untouched for a future adapter-specific bracket. A generic raw range delete is not an allowed implementation because it would miss internal-family state and could delete control records if a future prefix is added incorrectly.

After the final source-data cleanup cursor reaches `done=true`, the source group runs a separate Raft-applied disarm step for the job: delete `!migwrite|<job_id>|*`, clear `!migfence|<job_id>` and any pending `ArmCutoverReadFence(job_id, expected_cutover_version)` entry, and release `source_retention_pin_ts`. The job must not move to DONE history until this source ACK and the target-local cleanup ACK above are durable. This keeps the successful path symmetric with `AbandonSplitJob`: recovery never leaves a completed migration carrying write trackers, read fences, or retention pins that can reject unrelated future traffic.

## 7. Coordinator / FSM Integration

### 7.1 FENCE write rejection

Coordinator-side:

- `kv/sharded_coordinator.go` consults the engine's per-route state. Routes in `WriteFenced` reject writes with a new sentinel `ErrRouteWriteFenced`, mapped to:
  - gRPC: `codes.Aborted` + status detail `{kind:"route_write_fenced", route_id, retry_after_ms}`.
  - Redis adapter: `MOVED`-style retry (existing path).
  - DynamoDB adapter: throttled error so the SDK retries with backoff.

FSM-side defense in depth — **must run on an unconditional apply path** (closes codex round-3 P1 on PR #945):

- The naïve placement — extend `verifyOwnerFromSnapshot` so it also rejects routes in `WriteFenced` — does NOT work. `verifyComposed1` short-circuits when `ObservedRouteVersion == 0` (`kv/fsm.go:608-611`), and there are several **intentional zero-version write paths today**: read/write txns with `ReadKeys`, caller-supplied `StartTS`, and resolver-claimed keys all dispatch with `ObservedRouteVersion = 0` (`kv/sharded_coordinator.go:694-754`, M3 sibling §3.5 + auto-pin policy). A stale coordinator in any of those flows would forward a write to the source during FENCE and the FSM defense would never run. The fence must be enforced **before** any version-conditional early return.
- M2 therefore introduces a **separate, unconditional pre-gate** `verifyRouteNotFenced(mutations)` in `kv/fsm.go` that the apply path consults **before** `verifyComposed1`. It computes each mutation's write footprint against the **current** catalog snapshot and rejects with `ErrRouteWriteFenced` when **any** footprint intersects a route whose `RouteState == WriteFenced`. For ordinary `PUT` / `DEL` / prepare / one-phase commit mutations, the footprint is the single `routeKey(mut.Key)` point, matching `verifyOwnerFromSnapshot`. For raw `Op_DEL_PREFIX`, the footprint is a range write: an empty prefix means every non-transactional key (`kv/transcoder.go:10-12`, `kv/sharded_coordinator.go:631-635`, `kv/fsm.go:482-527`), and a non-empty prefix covers the half-open prefix interval `[prefix, prefixScanEnd(prefix))` with an empty end meaning `+∞`. The gate rejects if that interval, after the same route-key normalization used by scan ownership (§7.2.2b), intersects any WriteFenced route. It must not decide from `routeKey(prefix)` alone: a prefix can start outside the moving child while still matching keys inside it (closes codex round-21 P1). No dependency on `ObservedRouteVersion`, no early return on `0`; the gate runs on every applied write request unconditionally. The gate has the same `Phase_ABORT` bypass as the Composed-1 gate (for the same lock-release reason in §3.5 of the parent partial doc). `Phase_COMMIT` is **not** a blanket bypass: it is allowed only while the SplitJob is still in the post-fence drain window (`post_fence_drain_completed=false`) and only if every committed mutation resolves a matching pre-existing `!txn|lock|` row for the same `(primary_key, start_ts, commit_ts)` in the moving range. That narrow lane lets already-prepared txns resolve after the fence has stopped new prepares (closing codex round-18 P1's starvation issue) without letting a stale coordinator create new state. Once the post-fence drain returns empty, the lane closes and any later `Phase_COMMIT` for the moved range is rejected; after CUTOVER the target has no historical intent for that key, by construction.
- Concretely, the apply path becomes: `verifyRouteNotFenced` → `verifyComposed1` → existing handlers. `verifyRouteNotFenced` reads the current `RouteSnapshot` once (the same one `verifyComposed1` would have read at the current-version check) and evaluates a small `mutationFootprints(mut)` helper: point lookups call `OwnerOf(routeKey(mut.Key))`, while `Op_DEL_PREFIX` calls `IntersectingRoutes(normalizePrefixFootprint(mut.Key))`. A raw user prefix normalizes to the raw prefix interval; known internal families (`!lst|meta|d|`, `!lst|claim|`, `!hs|`, `!st|`, `!zs|`, concrete stream prefixes `!stream|meta|` and `!stream|entry|`, DynamoDB/S3/SQS families) use the same decoders as the export bracket planner and read-side scan gate when the prefix contains enough bytes to recover logical owner bounds. A family-wide internal prefix that does not carry an owner segment, or any unrecognized internal prefix, fails closed with `ErrRouteWriteFenced` if the current snapshot contains any WriteFenced route, because the FSM cannot prove it is disjoint. `kv/sharded_coordinator.go` may preflight the same intersection before broadcasting `DEL_PREFIX`, but correctness is source-FSM-local: every broadcast recipient re-runs the gate before `handleDelPrefix`, so a stale coordinator cannot slip source tombstones in after FENCE. Cost is bounded by one route-interval lookup for prefix deletes and the existing per-mutation `OwnerOf` call for point writes. The two gates stay independent so a future change to one cannot regress the other (same isolation rationale as `verifyReadOwner` next to `verifyOwnerFromSnapshot` in §7.2.5).
- Independently of `WriteFenced`, the same unconditional pre-gate sequence starts with `verifyNoMigrationControlPrefix(mutations)`. It rejects any user-plane mutation footprint that intersects the registered migration/control prefixes from §3.2 (`!dist|`, `!dist|migstage|`, `!migstage|`, `!migwrite|`, `!migfence|`, etc.) before a key reaches `handleRawRequest`, `handleOnePhaseTxnRequest`, `handlePrepare`, or `handleDelPrefix`. This covers point keys and prefix-delete ranges, including `Op_DEL_PREFIX ""`; an empty prefix is a user-data request, not permission to delete catalog, staged import rows, HLC floors, readiness records, promotion records, or source fences. The implementation may share the `mutationFootprints` interval helper with `verifyRouteNotFenced`, but it must not be conditional on there being an active WriteFenced route. Typed catalog/migration Raft proposals are the only writers to those prefixes.
- Forward-compatibility hook (M3): a follow-on change can flip the `ObservedRouteVersion == 0` short-circuit off entirely once every M2-shipped flow stamps a non-zero version, at which point `verifyRouteNotFenced` collapses into a `verifyComposed1` invariant. M2 ships the safe form (unconditional pre-gate) so the FENCE rejection cannot be silently bypassed before that cleanup lands.

Reads during FENCE / DELTA_COPY are **not** rejected: `ShardStore.GetAt` continues to serve from source. Snapshot reads at any `commit_ts ≤ fence_ts` remain consistent because the source's MVCC history is unchanged through DELTA_COPY. The post-CUTOVER read-side fence is §7.2.

### 7.2 Post-CUTOVER read fence (closes codex P1)

Codex P1 on PR #945: today the read path resolves the group from the local engine and serves/proxies the read **without carrying an observed catalog version** (`kv/shard_store.go:38-53`, `kv/shard_store.go:1471-1477`), while `verifyComposed1`'s `verifyOwnerFromSnapshot` (`kv/fsm.go:753-773`) only inspects mutations. After CUTOVER, a coordinator or source-group leader that has not applied the new catalog yet would keep routing reads of the moved key to the source — returning the stale pre-cutover value while writes have already landed on the target. M2 must close this read path symmetrically to writes.

#### 7.2.1 Read request carries an observed catalog version

Add `read_route_version uint64` to the read-path message (`pb.ReadRequest`, including the lease-read envelope used by `ShardStore.GetAt` / `.ScanAt`). The coordinator stamps the engine's catalog version into every read at the moment it picks a group. This is the read-side equivalent of `ObservedRouteVersion` on writes.

#### 7.2.2 Source FSM rejects reads after CUTOVER — authoritative ownership check, not version comparison (closes codex P1 line 459)

The original wording — "the FSM returns `ErrRouteMoved` when `read_route_version` is strictly less than the FSM's last-applied catalog version, and the key is no longer owned" — is **insufficient**. Codex P1 round-4 on PR #945: it misses the common post-CUTOVER case where the coordinator and the source FSM are **equally stale at the pre-CUTOVER version**. In that window:

1. A *different* coordinator, refreshed against the new catalog, routes a write of key `K` to the target group. The write commits there with `commit_ts > max_imported_ts` (§6.2.1).
2. The original stale coordinator routes a read of `K` to the source FSM with `read_route_version == source_local_version` (both at the pre-CUTOVER version).
3. Under the strict-less-than rule, `read_route_version` equals `source_local_version`, the comparison passes, and the source serves the old value — even though the target has a fresher committed write.

This is the same hazard the §7.2 read fence was added to prevent; the strict-less-than form simply doesn't catch the equal-stale case. The fix replaces version comparison with an **authoritative ownership check** scoped on either the local or the cutover-version snapshot, whichever exists:

- If the source FSM **has applied** CUTOVER (`source_local_version ≥ cutover_version`): the source no longer owns the key under its current snapshot. **Unconditionally** return `ErrRouteMoved{new_route_version, new_group_id}` regardless of `read_route_version`. This is the simple case the old wording also handled.
- If the source FSM **has NOT applied** CUTOVER (`source_local_version < cutover_version`), but the **default group has** (the migrator already populated `cutover_version` on the SplitJob and that proposal has committed, observable via the watcher's view of the catalog version known to the source group leader): the source must consult an out-of-band authoritative signal because its local FSM snapshot still says it owns the key. M2 ships two layers:
  1. **Heartbeat-piggybacked watermark.** The default-group leader's existing distribution heartbeat to every node already carries `catalog_version` (§11.1 capability bit lives there). Extend it to also carry `max_cutover_version_seen` (monotone, never decreases). The source-group FSM stores this as a leader-local atomic and consults it on every read fence: if `max_cutover_version_seen > source_local_version`, the source treats itself as "potentially stale at the cutover boundary" and the read fence runs the next bullet.
  2. **Cutover-aware ownership query.** When the watermark indicates a possible stale read, the source-group leader makes a single deadline-bounded `GetRouteOwnership(routeKey(K), catalog_version=max_cutover_version_seen)` RPC to the default-group leader. The default-group leader answers from its current catalog snapshot. If the answer is "owned by another group" the source returns `ErrRouteMoved{new_route_version=max_cutover_version_seen, new_group_id}` to the coordinator; if owned by the source (no cutover landed on this key after all — the watermark covers an *unrelated* migration) the source returns the value normally. If the RPC times out, the default-group leader is unreachable, or the answer cannot be verified at `max_cutover_version_seen`, the source returns `ErrRouteOwnershipUnknown{route_version=max_cutover_version_seen, retry_after_ms}` and **must not read source MVCC**. Treating an unknown answer as "source-owned" would reopen the equal-stale stale-read window, so this path is fail-closed and retryable.
- Successful ownership answers are cached on the source-group leader per `(routeKey, max_cutover_version_seen)` for one second to keep the fence cheap; the cache invalidates the moment `max_cutover_version_seen` advances. Transport errors, timeouts, and `ErrRouteOwnershipUnknown` are not cached as source-owned answers.

Crucially, this catches **the equal-stale window** the strict-less-than form missed: both coordinator and source are at the pre-CUTOVER version, but the heartbeat watermark on the source is already at the post-CUTOVER version (because the default-group leader propagated it as soon as the catalog write committed), so the source consults the default-group leader and gets the authoritative "no longer yours" verdict before serving a stale value.

Two sub-cases of the legacy framing are preserved:

- **Coordinator stale, source up-to-date**: source's `source_local_version ≥ cutover_version` and source no longer owns the key → unconditional `ErrRouteMoved`. Coordinator refreshes → re-routes to target → succeeds.
- **Source stale, coordinator further ahead (`read_route_version > source_local_version`)**: the FSM blocks on a **condition variable signalled by the apply goroutine** when it advances `appliedIndex`, with a timeout of `min(remaining_lease_TTL, 200 ms)` (the FSM tracks `appliedIndex` natively; the condition variable wraps it with a one-shot notifier consumed by the read-fence path). On the signal the FSM re-evaluates ownership; if `source_local_version` has now caught up to `read_route_version` AND the source still owns the key under the new snapshot, the read proceeds. If `source_local_version` is still behind on timeout, OR if the catch-up reveals the source no longer owns the key, the FSM returns `ErrRouteMoved`. This avoids a wedge while keeping the read consistent (closes claude round-2 spec-mechanism flag, 4-round flag).

#### 7.2.2a Equal-stale window — concrete walkthrough

To make the §7.2.2 fix concrete:

1. `t=0`: catalog at version `v`. Coordinators C1 and C2 both at `v`. Source FSM at `v`. CUTOVER not yet issued.
2. `t=1`: after §7.2.2e has already armed the source-side pending read fence for expected version `v+1`, the migrator issues CUTOVER on default group. Catalog committed at `v+1`. Default-group leader's outbound heartbeat starts carrying `max_cutover_version_seen = v+1` immediately.
3. `t=2`: source-group leader receives the heartbeat. Its local atomic `max_cutover_version_seen` advances to `v+1`. Its FSM-local `source_local_version` is **still `v`** because the watcher hasn't yet routed the catalog update through the source group's own Raft log.
4. `t=3`: C2 watches the catalog, sees `v+1`, refreshes its engine, routes its write of `K` to the target. Target commits the write with `commit_ts > max_imported_ts`.
5. `t=4`: C1 (still at `v`) routes a read of `K` to the source FSM with `read_route_version = v == source_local_version`. **Equal stale.**
6. `t=5`: source FSM sees `max_cutover_version_seen (v+1) > source_local_version (v)`. Runs `GetRouteOwnership(routeKey(K), v+1)` against the default-group leader. Answer: "owned by target group." Source returns `ErrRouteMoved{new_route_version=v+1, new_group_id=target}` to C1.
7. `t=6`: C1 refreshes, retries on target, sees C2's fresh write.

Without the watermark + ownership-query path, step 6 would have returned the pre-CUTOVER value because the strict-less-than version check passed.

#### 7.2.2b Scan-interval ownership gate — point-key check is insufficient for scans (closes codex P1 round-9 on PR #945)

Codex round-9 P1: §7.2.2's ownership check resolves the scan's **start key** against `routeKey(scan_start)`, but a `ScanAt(start, end)` or `ReverseScanAt(start, end)` spans **a range**, and a stale-coordinator scan that straddles the post-CUTOVER split boundary can pass the point-key check while serving stale data for the moved sub-range. Concrete failure scenario (verified against `kv/shard_store.go:164-228`):

1. After CUTOVER, the catalog is at version `v+1`: `source.Active(left: [A, M))`, `target.Active(right: [M, Z))`.
2. A coordinator stale at version `v` still sees `source.Active(full: [A, Z))` — one route.
3. Client calls `ScanAt([A, Z), ts)`. The coordinator's `routesForScan` calls `engine.GetIntersectingRoutes([A, Z))` against the stale engine → returns the single `source.Active(full)` route, and dispatches the entire scan to the source group.
4. Source FSM's §7.2.2 point-key check resolves `routeKey(A)` → still owned by source under its **current** catalog (left half is `source.Active([A, M))`). Check passes.
5. Source FSM executes the full `[A, Z)` scan over its MVCC store, returning **live data for `[A, M)` AND stale pre-CUTOVER data for `[M, Z)`** — the target has already received post-CUTOVER writes for that range, but the source has the historical versions for those keys still resident (CLEANUP grace window). Client sees a snapshot that mixes pre- and post-CUTOVER state across the boundary. This is the read-side analogue of the §3.2a write-loss window the source-side fence-apply barrier closes.

This is not hypothetical — `ScanAt` / `ReverseScanAt` underlie Redis `KEYS` / `HSCAN` / `LRANGE`, DynamoDB `Scan` / GSI `Query`, S3 `ListObjects`, etc. Any such call straddling the split boundary in the equal-stale window would observe the regression.

The fix is a **scan-interval ownership gate** on the source FSM, parallel to §7.2.2's point-key gate. The gate never proves ownership from raw MVCC scan bytes alone. Before either branch consults the catalog it first converts the request into one or more **route-key intervals**:

- User-key scans use the normal `[start, end)` routing interval (`routeKey(raw) == raw`).
- Internal per-user scans (list meta/item/delta/claim, Redis hash/set/zset/stream, DynamoDB item/GSI, SQS/S3, etc.) decode their logical owner with the same `routeKey()` helpers as §6.3.1, then check that owner key or the corresponding logical interval. For example `!lst|meta|d|<len><user>...`, `!lst|claim|<len><user>...`, `!hs|...`, `!st|...`, and `!zs|...` are checked against the decoded Redis user key, not against their raw `!lst|...` / `!hs|...` prefixes.
- Family-wide internal scans (for example a compactor or migration bracket scanning an entire raw prefix) must carry explicit logical bounds `(route_start, route_end)` from the caller. If the FSM cannot derive or receive route-key intervals for a raw internal scan, it returns `ErrRouteOwnershipUnknown` and **must not scan source MVCC**. Treating raw `GetIntersectingRoutes(start, end)` as proof would misclassify internal rows whose raw prefix sorts outside their logical route.

Like the point-key gate it has **two branches** keyed on the watermark, so it catches both the "coordinator-stale, source-up-to-date" and the "coordinator-and-source-equal-stale" cases (closes claude round-10 P1 on equal-stale scans, PR #945):

- **Branch 1 — source has applied CUTOVER (`source_local_version >= cutover_version`).** The FSM calls `GetIntersectingRoutes(route_start, route_end)` for each normalized route-key interval against its **own** current catalog snapshot. If **every** intersecting route is owned by this group, the scan proceeds. If **any** intersecting route is owned by a different group, the FSM returns `ErrRouteMoved{new_route_version, new_group_id, sub_range_start, sub_range_end}` for the **first** foreign-owned sub-range encountered in route-key order.
- **Branch 2 — source has NOT applied CUTOVER but `max_cutover_version_seen > source_local_version` (equal-stale, watcher lag).** The local snapshot still says `source.Active(full: [A, Z))` — a single route, no foreign owner visible — so Branch 1 alone would silently pass a cross-boundary scan. The gate therefore additionally calls `GetIntersectingRoutes(route_start, route_end, catalog_version = max_cutover_version_seen)` for the normalized route-key intervals as a deadline-bounded RPC to the **default-group leader** (mirroring exactly §7.2.2's point-key extension — same watermark trigger, same RPC target, same 1 s cache TTL with cache key `(route_intervals, max_cutover_version_seen)`). The default-group leader answers from its current catalog snapshot. If any returned route is owned by another group, the source FSM returns `ErrRouteMoved{max_cutover_version_seen, foreign_group_id, sub_range_start, sub_range_end}` for the first foreign-owned sub-range. If every returned route is still source-owned (the elevated watermark covers an *unrelated* migration that did not touch this scan range), the scan proceeds normally. If interval normalization fails, the RPC times out, the default-group leader is unreachable, or the route set cannot be verified at `max_cutover_version_seen`, the FSM returns `ErrRouteOwnershipUnknown{route_version=max_cutover_version_seen, sub_range_start=route_start, sub_range_end=route_end, retry_after_ms}` and **must not scan source MVCC**.
- **Coordinator path identical in both branches.** The coordinator's existing `WatcherRefresh` + retry path (§7.2.3) re-issues the scan; on retry the fresh engine returns the correct two routes and `routesForScan` splits the scan into `[A, M)` to source and `[M, Z)` to target, both fenced correctly.
- **The §7.2.2 point-key gate still runs** for the scan's start key — the new check is **in addition to**, not in place of, so the watermark / equal-stale logic at the start key continues to apply.

Implementation cost: route-interval normalization plus one additional `GetIntersectingRoutes` call per normalized interval on the FSM. Branch 1 (the common case once watcher catches up) is local-only. Branch 2 adds at most one cached successful RPC result from the default-group leader per `(route_intervals, max_cutover_version_seen)` per second — bounded by the same cache as §7.2.2's point-key extension. Failed / timed-out lookups and failed interval-normalization attempts fail closed and are not cached as proof of source ownership. Memory: bounded by the number of routes intersecting the normalized intervals (typically 1–2; the rare cross-multi-boundary scan is the same `O(routes_in_range)` the coordinator already pays).

The §7.2.2a equal-stale walkthrough is extended in §7.2.2c (Branch 1 scenario) and §7.2.2d (Branch 2 / equal-stale scenario) with parallel scan-shape walkthroughs to demonstrate both branches catch their case.

#### 7.2.2c Equal-stale scan walkthrough

Continuing from §7.2.2a's setup, with the post-CUTOVER catalog v+1 = `source.Active(left: [A, M))`, `target.Active(right: [M, Z))`:

1. `t=4`: stale coordinator C1 issues `ScanAt([A, Z), ts=now)` with `read_route_version = v`.
2. Coordinator's `routesForScan` against its v-catalog returns `[source.Active(full: [A, Z))]` → entire scan dispatched to source group.
3. `t=5`: source FSM applies the scan. §7.2.2 point-key gate resolves `routeKey(A)` against the current snapshot at `v+1` → owned by source (left half). Point-key check passes.
4. **§7.2.2b scan-interval gate fires**: source FSM calls `GetIntersectingRoutes([A, Z))` against its `v+1` catalog → returns `[source.Active(left: [A, M)), target.Active(right: [M, Z))]`. The second route is owned by target, not source. The FSM returns `ErrRouteMoved{v+1, target_group_id, M, Z}` for the moved sub-range.
5. `t=6`: C1 receives `ErrRouteMoved`, runs `WatcherRefresh`, advances its engine to `v+1`, and re-routes the scan as two sub-scans: `[A, M)` to source and `[M, Z)` to target. Both succeed; the merged result is the consistent post-CUTOVER snapshot. No stale data for `[M, Z)` was ever returned.

The gate runs at apply time on the source FSM, not on the coordinator, so even a coordinator with a paused `WatcherRefresh` cannot serve mixed pre/post-CUTOVER data — the source FSM is the chokepoint that knows the authoritative current catalog.

#### 7.2.2d Equal-stale cross-boundary scan walkthrough — Branch 2

Same `t=0..t=2` setup as §7.2.2a (the §7.2.2e pending source read fence was armed before CUTOVER, CUTOVER commits at `v+1`, default-group heartbeat carries `max_cutover_version_seen = v+1` immediately, source-group leader has received the heartbeat but its FSM watcher has **not yet** applied `v+1` — `source_local_version` is still `v`):

1. `t=3`: source-group leader's local atomic `max_cutover_version_seen = v+1`, but FSM-local `source_local_version = v`. Source FSM snapshot still says `source.Active(full: [A, Z))`.
2. `t=4`: stale coordinator C1 issues `ScanAt([D, Z), ts=now)` where `D ∈ [A, M)` — scan starts in the non-moved left half and ends in the moved right half. `read_route_version = v`.
3. Coordinator's `routesForScan` against its v-catalog returns `[source.Active(full: [A, Z))]` → entire scan dispatched to source group.
4. Source FSM receives the scan. §7.2.2 **point-key gate** runs: `max_cutover_version_seen (v+1) > source_local_version (v)` → equal-stale watermark fires; FSM calls `GetRouteOwnership(routeKey(D), v+1)` to the default-group leader → answer: "owned by source (left half)". Point-key check passes (the scan's START key is still source-owned).
5. **§7.2.2b scan-interval gate Branch 2 fires** (because watermark is elevated): FSM calls `GetIntersectingRoutes([D, Z), catalog_version = v+1)` to the default-group leader → returns `[source.Active(left: [D, M)), target.Active(right: [M, Z))]`. The second route is owned by target. The FSM returns `ErrRouteMoved{v+1, target_group_id, M, Z}` for the moved sub-range. **The gap §7.2.2c's Branch 1 would have left open is now closed** — without Branch 2, step 5 would have run `GetIntersectingRoutes` against the FSM's own `v` snapshot, returned the single `source.Active(full)` route, and served the entire scan stale.
6. `t=5`: C1 receives `ErrRouteMoved`, runs `WatcherRefresh`, advances to `v+1`, and re-issues the scan as two sub-scans: `[D, M)` to source and `[M, Z)` to target. Both succeed; the merged result is consistent. No stale data for `[M, Z)` was ever returned.

Successful Branch 2 answers from the default-group leader are cached for 1 second per `(route_intervals, max_cutover_version_seen)`, so a burst of cross-boundary scans during the watcher-lag window pays at most one successful RPC per second across all of them. Timeout / unreachable / unverifiable answers and interval-normalization failures return `ErrRouteOwnershipUnknown` and are not cached as source-owned. Once the watcher catches up (`source_local_version >= cutover_version`), Branch 1 takes over and the local snapshot is authoritative — the cached Branch 2 results are no longer consulted.

#### 7.2.2e Source-side cutover read-fence arm — no admitted pre-watcher stale-read window

Codex round-17 P1 on PR #945 flagged the remaining gap: if the default group publishes `target.Active` before the source group has either applied the CUTOVER catalog snapshot or received `max_cutover_version_seen`, a refreshed coordinator can already write/read on the target while a stale coordinator can still route a read or cross-boundary scan to the source. Branch 1 and Branch 2 above only fire after one of those source-side signals exists, so a design that merely accepts the watcher interval as "small" violates M2's atomic reader/writer cutover goal.

M2 therefore makes target visibility depend on a synchronous source-side read barrier:

1. After DELTA_COPY completes and after the target full-HLC floor proposal in §6.2.1, the migrator fixes `expected_cutover_version = current_catalog_version + 1`, persists `target_staged_readiness_state = ARMING`, and applies §6.4 step 0's `ApplyTargetStagedReadiness` proposal on the target group. Only after the target ACK does it record `target_staged_readiness_state = ARMED`. This closes the target-side watcher-lag gap before the source read barrier is armed.
2. With target readiness armed, but **before** any source-group read-fence proposal, the migrator persists `cutover_read_fence_state = ARMING` on the default-group SplitJob. This default-group intent is durable proof that recovery must reconcile the source-side fence even if the leader dies during the next RPC.
3. With `ARMING` durable, the migrator proposes `ArmCutoverReadFence(job_id, route_start, route_end, expected_cutover_version, target_group_id)` to the **source group**. The source FSM applies this as source-group Raft state: a pending-fence entry keyed by `(job_id, route_start, route_end, expected_cutover_version)`. The operation is idempotent for the same tuple and rejects a conflicting expected version for the same job.
4. After the source-group apply ACK, the migrator CASes the default-group SplitJob from `ARMING` to `ARMED`. From the moment the source apply lands until the source watcher applies CUTOVER and normal §7.2.2 Branch 1 takes over, any point read or scan whose **route-key-normalized interval** intersects the moving range and whose `read_route_version < expected_cutover_version` **must not read source MVCC**. If a raw internal scan cannot be normalized to route-key intervals, the source fails closed with `ErrRouteOwnershipUnknown` / retry rather than proving ownership from raw bytes. If the source has already learned that the default-group CAS committed (via watcher, heartbeat watermark, or the ownership RPC), it returns `ErrRouteMoved{expected_cutover_version, target_group_id}`. Otherwise it returns `ErrRouteCutoverPending{retry_after_ms}`; coordinators treat it like `ErrRouteMoved` with a short wait + `WatcherRefresh` retry. A transient retry is acceptable; serving stale source data is not.
5. Only after both `target_staged_readiness_state = ARMED` and `cutover_read_fence_state = ARMED` are durable does the migrator issue the default-group CUTOVER catalog CAS that removes `source.WriteFenced(right)` and inserts `target.Active(right)`. This ordering delays target visibility/writes until the target can fail closed for missing staged metadata, the source can fence stale reads, and the default group has recovery witnesses for both facts.
6. If the catalog CAS fails (expected version mismatch, abandoned job, leadership loss), the migrator first CASes the SplitJob to `CLEARING`, then proposes `ClearCutoverReadFence(job_id, expected_cutover_version)` to the source group and `ClearTargetStagedReadiness(job_id, expected_cutover_version)` to the target group, and only after both clear ACKs return sets the two states to `NONE` before returning the job to DELTA_COPY/FAILED. While either fence is armed and the CAS is not committed, clients see bounded retry instead of stale reads or live-only target reads.

Crash safety: because `ARMING` is persisted before each target/source proposal, a default-group leader loss after either side ACKs but before the corresponding `ARMED` write cannot strand an unowned guard. The new migrator first reconciles target readiness: at `target_staged_readiness_state = ARMING` it probes/re-proposes `ApplyTargetStagedReadiness` idempotently and records `ARMED`; at `ARMED` it either completes the same CUTOVER CAS or transitions through `CLEARING` and removes the target readiness record on abort. It then reconciles the source fence the same way: at `cutover_read_fence_state = ARMING`, probes/re-proposes `ArmCutoverReadFence` idempotently on the source group, and records `ARMED`; at `ARMED`, recovery either completes the same CAS or transitions through `CLEARING` and clears the source fence. At `CLEARING`, recovery keeps issuing the target/source clear proposals until both ACK, then records both states as `NONE`. Abandon/rollback follows the same clearing path for both `ARMING` and `ARMED`; it does not skip cleanup merely because an `ARMED` bit was never written. Source- or target-group leader changes are also safe because both guards are committed before the target route is published; a new leader applies committed entries before serving reads/writes for the moving range.

This barrier is intentionally stronger than the heartbeat watermark. The watermark and authoritative ownership RPC remain as the steady-state read fence after CUTOVER, and they cover unrelated elevated-watermark cases, but they are no longer the only protection during the first watcher tick.

#### 7.2.2f Read-serving chokepoint — every source read path must call the fence before touching MVCC

The sections above say "source FSM" because the ownership decision is source-group-local, but the repo's read serving path is not a single Raft apply hook. M2 therefore places the read fence in a shared source-group read helper that runs before any local store lookup:

- `kv.ShardStore.GetAt`, `ScanAt`, and `ReverseScanAt` call the helper after the lease/read-index check selects the source group but **before** `g.Store.GetAt` / `ScanAt` touches MVCC.
- `adapter/grpc.go` `RawGet` / `RawScanAt` and any RawKV entrypoint that can read `r.store` directly must route through the same helper; direct calls into `store.MVCCStore` are allowed only for internal maintenance paths that do not serve client reads and do not overlap an Active/WriteFenced route.
- Local-leader reads and proxied reads both carry `read_route_version` and the normalized point key / scan intervals into the helper. A proxy target cannot downgrade or omit these fields.
- Point reads run §7.2.2 and scans run §7.2.2b. If normalization of an internal raw prefix is impossible, the helper returns `ErrRouteOwnershipUnknown` / retry and the caller must not fall back to direct source MVCC.

The helper owns the pending `ArmCutoverReadFence` table, `max_cutover_version_seen`, the one-second successful ownership cache, and the fail-closed timeout behavior. This is a placement requirement, not a new semantic rule: the invariant is that every user-visible source read path has exactly one chance to prove route ownership before it can observe source MVCC. A red implementation that wires the check only into `kv/fsm.go` but leaves `ShardStore.GetAt` / `ScanAt` or `RawGet` / `RawScanAt` calling `Store` directly is incorrect and must fail the §10.1 read-fence tests.

#### 7.2.3 Coordinator-side fallback

If the engine's local catalog is behind the FSM's `new_route_version`, the coordinator does a single best-effort `WatcherRefresh` and retries once. `ErrRouteOwnershipUnknown` and `ErrRouteCutoverPending` use the same retry lane with the supplied `retry_after_ms`: wait briefly, run `WatcherRefresh`, and retry at the refreshed catalog version. Repeated `ErrRouteMoved` / `ErrRouteOwnershipUnknown` surfaces to the client only on persistent inconsistency or default-group unreachability (alert-worthy). The coordinator must not reinterpret either retryable error as permission to read from the source.

#### 7.2.4 Grace window before CLEANUP

CLEANUP deletes the moved user-data range from source's MVCC after a configurable grace window (`readFenceGrace`, default 30 s — chosen to comfortably exceed both lease TTL and watcher tick × 2). It never treats the default-group `!dist|` control namespace as migratable data; jobs that would intersect it are rejected before creation (§3.2).

**No pre-watcher stale-read admission after CUTOVER (closes codex round-17 P1 on PR #945).** The `CatalogWatcher` may still take up to one polling interval (`defaultCatalogWatcherInterval = 100 ms`, `distribution/watcher.go`) to apply the CUTOVER catalog write locally on the source group, and the heartbeat-carried `max_cutover_version_seen` may arrive on a different schedule. M2 does **not** rely on either propagation path for the first instant after CUTOVER. The §7.2.2e source-side `ArmCutoverReadFence` barrier is already Raft-applied on the source before `target.Active` is published, so a stale coordinator that reaches the source during that watcher interval gets `ErrRouteMoved` / `ErrRouteCutoverPending` instead of source MVCC data. Fresh coordinators may route to the target only after the catalog CAS commits; by then stale-source reads are already fenced. The admitted stale-read window is therefore zero; the 30 s grace window is only about how long source retains physical data for retry/lag tolerance, not about allowing reads to observe it.

During the grace window:

- Source still has the data physically; the read fence above turns it into `ErrRouteMoved` rather than serving the value.
- After the window the data is gone; a stale read attempt naturally returns "not found" via the read fence (the FSM has already rotated past the cutover version).

This protects the operational corner case where a coordinator's `WatcherRefresh` is paused (CPU saturation, GC pause) for tens of seconds.

#### 7.2.5 Composed-1 alignment

`verifyOwnerFromSnapshot` keeps its current write-only behaviour. The new read-fence lives on a parallel path (`verifyReadOwner` next to it in `kv/fsm.go`) so the two stay independent — a future change to read-fence policy doesn't risk regressing the write gate the Jepsen suite (PR #932) already trusts.

### 7.3 CUTOVER coherence with Composed-1

The Composed-1 write gate compares observed catalog version against current. CUTOVER bumps the version by 1; any in-flight txn pinned at the pre-CUTOVER version that lands on the target sees its observed-version owner ≠ target → ErrComposed1Violation → coordinator retries at the new version → routes to the target's new active route. This is the same flow PR #927 exercised; M2 reuses it for writes, and §7.2 adds the parallel read mechanism.

### 7.4 Engine route lookup invariant

After CUTOVER, the engine's per-route ranges remain non-overlapping (per §3.2, the catalog only ever holds non-overlapping routes — the FENCE-time split out + the CUTOVER-time group rebind are each atomic and overlap-free). `Engine.GetRoute(routeKey(k))` therefore continues to return exactly one route, matching M1's invariant. No `OwnerOf` change is needed beyond what PR #932 already shipped.

## 8. Migrator Runtime

`distribution/migrator.go` (new):

- Single goroutine per default-group leader. On leadership election the runtime starts it; on loss it cancels.
- Reads `!dist|job|*` on startup, picks up the oldest non-terminal job, drives it. After completion picks the next.
- Per-phase work uses the gRPC mesh (internal RPCs above) to the source/target group leaders. The migrator does **not** need direct Raft access.
- One job in flight at a time in M2 (simpler reasoning about FENCE / DELTA_COPY ordering). M3+ may relax this.

Throttling: chunk-bytes default 1 MiB, inter-chunk pacing default 5 ms. Both configurable per-job via `SplitRangeRequest.options` (string-map field; reserved for future without proto break).

## 9. Idempotency and Resumability Matrix

| Failure mode | Recovery action | Visible to client |
|---|---|---|
| Leader loss during BACKFILL | New leader resumes from persisted cursor; the source-group migration write tracker remains armed and the §6.1.0 step-2 running minimum (`snapshot_min_admitted_ts`) is re-read from tracker rows plus live locks, staying monotone-down across the flap. | None — job timeline lengthens. |
| Leader loss during FENCE | Catalog state is `WriteFenced` already, so coordinators still reject new writes/prepares while existing prepared txns may resolve through the narrow §7.1 lane. New migrator advances. The running minimum and `post_fence_drain_completed` are durable, so the new migrator re-reads the migration write tracker + route-faithful drain rather than re-deriving from a single stale scan. | Continued retryable rejection until DELTA_COPY → CUTOVER. |
| Leader loss between post-fence drain and `delta_floor` finalisation (§6.1.0 step 3) | `delta_floor` is the deterministic `min(snapshot_ts, snapshot_min_admitted_ts - 1)` of durable inputs, so the new migrator recomputes the identical value; no new raw/one-phase write or prepare can lower the running minimum because the fence already rejects them. | None. |
| Leader loss after target staged-readiness `ARMING` but before/while the target applies it | New migrator sees `target_staged_readiness_state = ARMING`, revalidates the expected catalog version, and idempotently probes/re-proposes `ApplyTargetStagedReadiness`. If the target already applied the record, the probe observes it; if not, the re-proposal creates it. The migrator records `ARMED` before arming the source read fence or attempting CUTOVER. | Target route is not published while readiness is missing; no live-only target reads. |
| Leader loss after target staged-readiness is `ARMED` but before default-group CUTOVER CAS | New migrator sees `target_staged_readiness_state = ARMED` and either completes the same CUTOVER CAS or transitions through `CLEARING` and clears `!migstage|ready|<job_id>` on abort. If a fresh coordinator reaches the target during recovery after CUTOVER landed elsewhere, the target guard fails closed until the route descriptor with `staged_visibility_active` is local. | Brief retryable target reads/writes; no live-only target visibility. |
| Leader loss after default-group `ARMING` intent but before/while source-side cutover read fence is armed | New migrator sees `cutover_read_fence_state = ARMING`, revalidates the expected catalog version, and idempotently probes/re-proposes `ArmCutoverReadFence`. If the source had already applied the fence, the probe observes the matching pending entry; if not, the re-proposal creates it. The migrator then records `ARMED` before attempting CUTOVER. Target is not published while state is only `ARMING`. | Brief retryable reads only if the source fence already landed; no stale source value and no stranded fence. |
| Leader loss after source-side cutover read fence is `ARMED` but before default-group CUTOVER CAS | New migrator sees `cutover_read_fence_state = ARMED`, revalidates the expected catalog version, and either completes the same CUTOVER CAS or transitions through `CLEARING` and clears `ArmCutoverReadFence` on abort. Source returns `ErrRouteCutoverPending`/`ErrRouteMoved` rather than serving stale reads while the fence is armed. | Brief retryable reads; no stale source value. |
| Leader loss during CUTOVER | The CAS catalog write either landed (new version visible, job → CLEANUP) or not (job stays in DELTA_COPY, redo). Never partial. | None. |
| Leader loss during target promotion | The target leader resumes from target-local `!migstage|promote|<job_id>.promote_cursor`; cursor advancement, live copy, and staged delete are one target-group Raft apply, so a persisted cursor cannot skip rows whose copy did not land. Default-group SplitJob state is only a witness and never drives the cursor. | None — reads keep using the raw staged/live merge until promotion finishes. |
| Target promotion finished but default-group promotion-complete CAS did not land | Target-local `PromotionState.done=true` and empty staged prefix are durable, and target-local ack / HLC-floor / readiness / promotion records are retained as recovery proof. The new migrator retries the default-group CAS that clears `staged_visibility_active` / `migration_job_id` and records `target_promotion_done=true`; only after the target observes the cleared descriptor does it delete those records. Until then, reads pay the merge cost over an empty staged prefix. | None. |
| Target restart or snapshot restore after import / HLC-floor proposal | The restored target computes `fullFloor = max(store.LastCommitTS(), all !migstage|hlc_floor|*)`, then calls `SetPhysicalCeiling(hlcPhysicalMs(fullFloor))` and `Observe(fullFloor)` before accepting writes for routes with a non-zero migration high-water mark. The first post-restore `Next()` is therefore `> max_imported_ts`, including when the imported max has a non-zero logical half. | None. |
| Promotion copies a tombstone / expired version before older staged rows | The staged/live read path merges raw MVCC candidates before applying visibility. A hidden live winner suppresses the key and never falls back to an older staged candidate, so partial promotion cannot resurrect deleted or expired data. | None. |
| Target group temporarily unreachable | Migrator retries with backoff; phase doesn't advance. | None. |
| Source group temporarily unreachable | Same. Reads to source continue to fail with whatever transport error the adapter raises (independent of split). | Existing per-RPC retry. |
| Duplicate ImportRangeVersions RPC | Importer compares the request's `(bracket_id, batch_seq)` to the per-bracket high-water mark under `!migstage|ack|<job_id>|<bracket_id>` (§6.1.1); drops any batch at or below it; returns the stored `acked_cursor`. Empty progress chunks use the same path, so a scan-budget-only chunk advances cursor / batch_seq without MVCC writes or HLC advancement. | None. |
| Operator `AbandonSplitJob` during BACKFILL/FENCE/DELTA_COPY | Migrator pauses the job, rolls catalog state back to `Active` if FENCE had landed, disarms the source write tracker / durable fence / retention pin, deletes target staged rows and target-local ack/HLC-floor/readiness/promotion records in bounded batches, then moves the job to `ABANDONED` history. | Brief retryable rejection clears; no leaked per-job source/target state. |
| Crash between job state writes | Job state is **last-write-wins** on CAS; replaying a phase is safe by construction (idempotent imports, bounded ts windows). | None. |

## 10. Test Strategy

### 10.1 Unit

- `distribution/migrator_test.go`: state machine — exhaustive phase transitions, abandon paths, CAS contention. Assert M2's live cap rejects a second non-terminal SplitJob (including `FAILED`) with `ErrTooManyInFlightJobs`, and that the migrator never drives two jobs concurrently until an explicit M3 concurrency design lands. FAILED transitions must set durable `retry_phase`, `RetrySplitJob` must resume exactly that phase, and a corrupt FAILED job with `retry_phase=NONE` must reject retry rather than deriving a phase from cursors or `last_error`.
- `kv/migrator_snapshot_boundary_test.go`: §6.1.0 fence-before-boundary (codex round-15 P1). Assert (a) `ArmMigrationWriteTracker` is source-applied before `snapshot_ts` is chosen and BACKFILL opens; (b) `snapshot_min_admitted_ts` is a monotone-down running minimum across BACKFILL + FENCE-apply from tracker rows plus live pre-tracker locks — a raw point request using `r.Ts`, a raw `DEL_PREFIX` whose prefix interval intersects the moving range, a one-phase txn using `TxnMeta.CommitTS`, and a prepare that land **after** BACKFILL opens with low caller-supplied `commit_ts ≤ snapshot_ts` all lower it immediately via the tracker, even if the prepare lock resolves before the next migrator tick; (c) `delta_floor = min(snapshot_ts, snapshot_min_admitted_ts - 1)` is finalised **only** after `post_fence_drain_completed = true`, and once `verifyRouteNotFenced` rejects new writes/prepares the running minimum cannot move; (d) DELTA_COPY's window is `(delta_floor, fence_ts]`, so each post-scan low-ts committed version / range tombstone is `> delta_floor` and is exported; **(e) moved-secondary coverage (codex round-16 P1):** a multi-shard txn whose **primary key is outside** the moving range but whose **secondary key is inside** it, with `commit_ts ≤ snapshot_ts`, lowers `snapshot_min_admitted_ts` — proving the running minimum is keyed on `routeKey(lockedKey)` per key (via the write tracker and/or live secondary lock) and NOT only on the txn's primary. Red control 1: finalising `delta_floor = snapshot_ts` before the fence (the old single-pre-scan form) drops the late low-ts write — must fail. Red control 2: scoping the running minimum to moved *primaries* only (the pre-round-16 form) drops the moved-secondary's version — must fail. Red control 3: using a committed-MVCC-version scan over all `commit_ts ≤ snapshot_ts` to lower the floor makes an old retained version force near-full DELTA_COPY and must fail. Red control 4: tracking prepares only, without raw/one-phase apply tracker rows, drops a post-scan low-ts raw or one-phase write and must fail. Red control 5: treating `DEL_PREFIX` as `routeKey(prefix)` instead of a prefix range fails to lower the floor for a prefix that starts outside the moving route but tombstones keys inside it. Restart case: a flap between post-fence drain and finalisation recomputes the identical `delta_floor` from durable tracker/drain inputs (§9 row).
- `kv/migrator_lock_drain_test.go`: route-faithful txn-lock drain (§3.2a.0a). Table-driven over key families. **Must include a DynamoDB-item-lock case**: a lock stored at `!txn|lock|!ddb|item|<table>|<pk>` whose routing key is `!ddb|route|table|<table>` MUST be counted by a drain over the moving range `[!ddb|route|table|<table>, !ddb|route|table|<table>\xff)` — proving the drain scans the `!txn|lock|` family and filters by `routeKey(lockedKey)`, NOT by a raw `[txnLockKey(routeStart), txnLockKey(routeEnd))` bracket (which would sort `!txn|lock|!ddb|item|...` outside the bracket and falsely report the drain empty). Mirror cases for SQS (`!txn|lock|!sqs|...` → `!sqs|route|global`), Redis/list, and a plain user key (raw == routing, the trivial case). Negative cases: a same-table lock for a key whose routing falls outside `[routeStart, routeEnd)` is NOT counted; an unrelated-family lock is NOT counted. The same predicate backs §6.1.0's pre-BACKFILL live-lock scan and §3.2a step 0b's post-fence drain — assert both call sites share it.
- `kv/migrator_export_plan_test.go`: `PlanExportBrackets` includes every family whose `routeKey()` maps into the routing namespace. Required cases: DynamoDB table metadata (`!ddb|meta|table|`), DynamoDB generation (`!ddb|meta|gen|`), DynamoDB item/GSI, txn/list/Redis simple or legacy `!redis|...` rows, list base meta/item plus meta-delta/claim rows (`!lst|meta|`, `!lst|itm|`, `!lst|meta|d|`, `!lst|claim|`), Redis wide-column umbrella brackets for hash/set/zset (`!hs|`, `!st|`, `!zs|`) that cover meta, field/member/score, and delta rows, concrete stream brackets (`!stream|meta|`, `!stream|entry|`), SQS/S3, plus red controls where omitting the DynamoDB metadata brackets makes `loadTableSchemaAt` / `loadTableGenerationAt` fail after CUTOVER, omitting list delta/claim brackets loses list length/head deltas or POP claim protection after CUTOVER, omitting Redis wide-column brackets makes `HGETALL` / `SMEMBERS` / `ZRANGE` / `XRANGE` empty or partial after CUTOVER, and adding a list/Redis wide-column bracket without its matching `routeKey()` decoder makes the `RouteKeyFilter` reject or mis-route every row in that bracket. Also assert `RouteKeyFilter(routeStart, nil)` and `RouteKeyFilter(routeStart, []byte{})` both treat the end as unbounded and accept keys in the rightmost route. Disjointness controls: a moving interval beginning at `!redis|` or another known internal prefix must not export that raw row through `familyUser`; it appears only in its explicit family bracket, `!stream|foo` remains a legal user key exported by `familyUser`, concrete `!stream|meta|` / `!stream|entry|` rows appear only in their stream brackets, and reserved `!dist|` / `!migstage|` / `!migwrite|` / `!migfence|` intervals are rejected before planning.
- `distribution/catalog_test.go`: SplitJob codec round-trip + version-1 stability; cross-group `SplitRange` validation rejects any moving interval intersecting reserved migration/control namespaces (`!dist|`, `!dist|migstage|`, `!migstage|`, `!migwrite|`, `!migfence|`) before a SplitJob is created, including `!migstage|ready|<job_id>` records.
- `store/mvcc_store_export_test.go`: raw committed-version semantics — export walks MVCC versions in `(minCommitTS, maxCommitTS]` without the read-visible filter; includes delete tombstones and TTL versions even when `ExpireAt <= maxCommitTS`; preserves `expire_at`; excludes only intent locks; cursor monotonic; `maxScannedBytes` budget bounds a sparse-bracket call (returns a partial/empty chunk + advancing cursor at `done=false` instead of scanning the full prefix). Red control: a reader-visible export loses a DELTA_COPY delete tombstone after BACKFILL copied an older value. Retention-pin control: compact below the active migration pin before a bracket reaches an old tombstone/version and assert export still returns it; removing the pin must reproduce the miss.
- `store/mvcc_store_import_test.go`: idempotency under duplicate `(bracket_id, batch_seq)` (a replayed batch at or below the per-bracket high-water mark is a no-op that returns the stored `acked_cursor`; parallel brackets do not clobber each other's ack record); BACKFILL→DELTA_COPY phase transition clears each bracket cursor/done flag while preserving the monotone per-bracket batch sequence, so DELTA_COPY can rescan exhausted brackets without reusing `(job_id, bracket_id, batch_seq)`; zero-version progress chunk persists `(batch_seq, cursor)` without MVCC writes, `metaLastCommitTS` / HLC advancement, or `max_imported_ts` changes; key_family dispatch; tombstone versions import via `DeleteAt`; non-tombstone TTL versions import via `PutAt(value, commit_ts, expire_at)` and reads after the original expiry hide the value.
- `kv/migrator_hlc_advance_test.go`: non-empty import advances `metaLastCommitTS`, target-local `!migstage|hlc_floor|<job_id>`, physical ceiling, and in-memory `HLC.last`; restart/snapshot restore re-observes the full HLC floor (`ms<<16|logical`) before accepting writes; red control shows physical-ceiling-only restore can emit `ms<<16|0` below an imported version.
- `kv/txn_codec_test.go`: M2-PR0 `txnLock` value round-trip with `commit_ts` plus backward-compat decode of an old lock value without the field (zero → migrator fallback path).
- `kv/txn_prepare_commit_ts_test.go`: M2-PR0 prepare plumbing. Assert the coordinator allocates/fences a non-zero `commit_ts` before PREPARE, the prepare request / txn metadata carry it to each participant, `handlePrepare` writes the same value into the txn lock, the migration write tracker records that value before the prepare is externally visible, and COMMIT reuses it. Red controls: codec-only change leaves zero in `handlePrepare`; allocating a fresh commit timestamp during COMMIT disagrees with the tracker row.
- `kv/fsm_route_fenced_test.go`: FENCE rejection at FSM (table-driven over key families, mirroring `fsm_composed1_test.go`). Include `Op_DEL_PREFIX` range footprints: empty prefix rejects whenever any WriteFenced route exists in the source snapshot; non-empty user prefixes reject on interval intersection; known internal prefixes normalize through the same route-key interval decoder as §7.2.2b; and a red control using only `routeKey(prefix)` allows a prefix outside the moving child to delete keys inside it. The source-group durable fence barrier is part of this test surface: after `ApplyMigrationWriteFence` commits, writes are rejected even if the catalog watcher snapshot is stale, and after snapshot restore / leadership acquisition the persisted `!migfence|<job_id>` entry is loaded before serving writes. The migrator ACK test must advance `fence_ack_cursor` only when each voter has applied that durable source Raft entry; a watcher-only `catalog_version_applied >= fence_catalog_version` heartbeat must not count. Red controls that only wait for `distribution/watcher.go`'s in-memory `catalog_version_applied`, or that restore without loading `!migfence|<job_id>`, can accept a write after restart and must fail.
- `kv/fsm_migration_control_prefix_test.go`: user-plane mutation guard for reserved migration/control prefixes (§3.2 / §7.1). Assert raw `PUT`, `DEL`, `Op_DEL_PREFIX` (including empty prefix), one-phase txn, and prepare requests whose point or range footprint intersects `!dist|`, `!dist|migstage|`, `!migstage|`, `!migwrite|`, or `!migfence|` fail before `handleRawRequest` / `handlePrepare` creates MVCC versions or intents, even when no route is `WriteFenced`. Typed catalog/migration proposals remain allowed. Red controls: empty `DEL_PREFIX` deletes `!dist|job|*`, a user `PUT !migstage|hlc_floor|x` creates a fake floor, and a prepare under `!migfence|` bypasses the gate.
- `kv/fsm_migration_write_tracker_test.go`: source-group `ArmMigrationWriteTracker` records raw point writes, `DEL_PREFIX` range tombstones, one-phase writes, and every prepare whose mutation/locked key routes into the moving range, including moved-secondary locks and same-tick land-and-resolve cases; `Phase_COMMIT` is allowed only while `post_fence_drain_completed=false` and only when the matching pre-existing intent lock is present. New raw point writes, `DEL_PREFIX`, one-phase writes, and prepares after `WriteFenced` are rejected. Red control: waiting for an empty drain while the route remains `Active` can starve under a steady prepare workload.
- `kv/fsm_cutover_read_fence_test.go`: §7.2.2e pending source read fence and §7.2.2 fail-closed ownership lookup. Assert point reads and scans intersecting the moving range with `read_route_version < expected_cutover_version` never touch source MVCC; before the catalog CAS they return `ErrRouteCutoverPending`, after the CAS they return `ErrRouteMoved`; when `max_cutover_version_seen > source_local_version` and the default-group ownership RPC times out/unreachable, both point reads and scan-interval checks return `ErrRouteOwnershipUnknown` and do not read source MVCC; non-intersecting left-half reads continue; `ClearCutoverReadFence` reopens reads only when the CAS did not commit. Recovery cases: crash after `ARMING` before the source proposal re-proposes; crash after the source ACK but before `ARMED` probes the existing source fence and records `ARMED`; abandon/rollback from both states persists `CLEARING` and clears the source fence before returning to DELTA_COPY/FAILED. Internal scan cases must cover `!lst|meta|d|`, `!lst|claim|`, `!hs|`, `!st|`, and `!zs|` raw prefixes: the ownership gate normalizes to route-key intervals before `GetIntersectingRoutes`, and a red control that calls `GetIntersectingRoutes(raw_start, raw_end)` passes stale source ownership and must fail. Placement red controls must call the real read-serving entrypoints (`ShardStore.GetAt`, `ShardStore.ScanAt` / `ReverseScanAt`, `adapter/grpc.go` RawGet, and RawScanAt) with the source watcher paused; wiring the fence only into `kv/fsm.go` while those paths call `Store` directly must serve stale MVCC and fail.
- `kv/fsm_target_staged_readiness_test.go`: §6.4 step 0 target readiness. Assert `ApplyTargetStagedReadiness` is persisted before CUTOVER, survives restart/snapshot restore, and makes target reads/writes for the moving interval fail closed while the target route snapshot lacks matching `staged_visibility_active` / `migration_job_id`; once the descriptor arrives, reads use staged/live merge and writes enforce the HLC floor. Recovery cases mirror the source read fence: crash at `ARMING`, crash after target ACK before `ARMED`, rollback through `CLEARING`, and successful cleanup only after DONE. Red controls: fresh coordinator routes to target before target watcher catches up and target serves live-only empty data, or accepts a low live write before loading the staged floor.
- `kv/fsm_promote_staged_test.go`: target-local `PromotionState` (§6.4). Assert `PromoteStaged` copies a bounded staged batch, physically deletes the exact staged MVCC row (not a `DeleteAt` tombstone), and advances `promote_cursor` in one target-group apply; a replay is idempotent by `(raw_key, commit_ts)`; a leader flap resumes from the target-local cursor; the default-group promotion-complete CAS that clears `staged_visibility_active` is attempted only after target-local `done=true`, and the final DONE history move is attempted only after target/source cleanup ACKs. Include raw staged/live merge cases where staged has `K@10` plus `K@20` tombstone (and a TTL variant), the promoter copies/deletes only the hidden `K@20` row first, and reads/scans at `read_ts >= 20` return not found rather than falling back to staged `K@10`. Red control: tombstoning the staged key instead of physically removing it leaves the staged prefix non-empty or hides older staged rows during partial promotion.
- `kv/fsm_migration_timestamp_floor_test.go`: target-side post-CUTOVER timestamp floor (§6.2.1). With `staged_visibility_active=true` and `!migstage|hlc_floor|<job_id>=T`, coordinator-allocated writes after `Observe(T)` succeed with `commit_ts > T`, but raw / one-phase / prepare paths carrying caller-supplied `commit_ts <= T` are rejected before creating a live MVCC version or intent lock. Red control: accepting the low supplied timestamp hides an acknowledged live write behind a higher staged version during the staged/live merge.
- `kv/migrator_cleanup_test.go`: source cleanup walks the same disjoint bracket plan + `RouteKeyFilter` as export and deletes only versions `<= fence_ts`; red control using a raw `[routeStart, routeEnd)` delete leaves DynamoDB metadata, Redis collection rows, list claims, SQS/S3 rows, or txn records behind. Successful cleanup disarms `!migwrite|<job_id>|*`, clears `!migfence|<job_id>` and the pending source cutover read fence, releases the source retention pin, and after the default promotion-complete witness deletes target-local ack / HLC-floor / readiness / promotion records in bounded batches. Abandon cleanup does the same per-job source/target cleanup before moving the job to `ABANDONED` history. Retryable `FAILED` jobs remain under `!dist|job|<id>` and survive history GC until `RetrySplitJob` or `AbandonSplitJob`.
- `kv/sharded_coordinator_route_fenced_test.go`: coordinator-side rejection + retry-after surfacing. Include `DEL_PREFIX` broadcast preflight cases where one shard's current engine snapshot has a WriteFenced route intersecting the prefix, and assert the coordinator surfaces `ErrRouteWriteFenced` instead of broadcasting a tombstone request that only the source FSM later rejects.
- Property tests (`pgregory.net/rapid`): replay a sequence of (export-chunks, import-acks, leader-flap) and assert (a) all source keys land on target, (b) no duplicates committed at any commit_ts.

### 10.2 Integration

- `kv/integration_split_test.go`: same-group split (M1 regression) + cross-group split happy path.
- Migrator restart mid-phase: cursor recovery, no double-apply.
- Concurrent writes during BACKFILL (fence is not yet up; writes still serve from source, get exported with their commit_ts ≤ snapshot_ts excluded, delta-copied in (delta_floor, fence_ts]).
- **DELTA_COPY tombstone and TTL preservation (§6.1 / §6.2).** BACKFILL copies `K@10`, the source deletes `K@20 <= fence_ts`, and DELTA_COPY exports the delete tombstone so the target hides `K@10` after CUTOVER. A parallel TTL case writes `K@10` with `expire_at=30`, migrates before expiry, then asserts reads at `read_ts < 30` see the value and reads at `read_ts >= 30` do not. Controls: reader-visible export drops the tombstone; import without `expire_at` makes the TTL value non-expiring.
- **Target HLC floor survives restart/snapshot (§6.2.1).** Import a version whose `max_imported_ts = ms<<16|logical` with `logical > 0`, snapshot/restore or restart the target before the first post-CUTOVER write, then assert the restored target re-observes the full floor and its first `Next()` is `> max_imported_ts`. Control: restoring only the physical ceiling emits `ms<<16|0` and fails.
- **Partial-promotion hidden-version merge (§6.4).** After CUTOVER, leave staged `K@10=value`, promote only a newer hidden version (`K@20` tombstone or TTL with `expire_at <= read_ts`) into live, and keep the older staged row unpromoted. Point reads and range scans at `read_ts >= 20` must return not found / omit the key. Control: standard snapshot reads over live plus staged fallback resurrect `K@10`.
- **Redis collection migration (§6.3.1).** Build a list with uncompacted `!lst|meta|d|` rows and live `!lst|claim|` rows, a hash with multiple fields, a set with members, a zset with both member and score-index rows, and a stream with metadata plus entries. After cross-group CUTOVER, `LRANGE`/list length/POP behavior, `HGETALL`, `SMEMBERS`, `ZRANGE`, and `XRANGE` must match the source state. Controls: omitting the `!lst|meta|d|` / `!lst|claim|` / `!hs|` / `!st|` / `!zs|` / concrete `!stream|meta|` and `!stream|entry|` brackets, or keeping the bracket while omitting the matching `routeKey()` decoder, makes the collection read empty or partial or loses POP claim protection and must fail. Also create a plain user key `!stream|foo` and assert it migrates through `familyUser`, not a stream-internal bracket.
- **Post-scan low-ts writes (§6.1.0 fence-before-boundary, codex round-15 P1 + round-20 P1 + round-21 P1).** A raw point request, a raw `DEL_PREFIX` whose prefix starts outside the moving child but matches keys inside it, a one-phase txn, and a prepare each land after BACKFILL opens and before the fence with a caller-supplied / lagging-clock `commit_ts ≤ snapshot_ts`, then commit/apply after the BACKFILL iterator passed their keys. Assert: (a) the source-group migration write tracker records raw point / `DEL_PREFIX` / one-phase `commit_ts` at apply time and prepare `commit_ts` at admission time, even if the prepare lock resolves before the next migrator tick, (b) `delta_floor` is finalised below all four only after step 0b's post-fence drain returns empty, (c) DELTA_COPY's `(delta_floor, fence_ts]` window exports each committed version / tombstone, (d) they are present on the target after CUTOVER. A control case with the finalisation moved *before* the fence, with prepare-only tracking, or with `DEL_PREFIX` tested as a point `routeKey(prefix)` must drop at least one write or resurrect deleted data (red test guarding the fix's ordering and coverage).
- **Moved-secondary, outside-primary multi-shard prepare (§6.1.0 step 2, codex round-16 P1).** A 2PC txn whose primary key routes to a group **outside** the moving range and whose secondary key routes **into** the moving range, prepared with a low caller-supplied `commit_ts ≤ snapshot_ts`, then committed after BACKFILL passed the secondary key. Assert: (a) the running minimum is lowered by the secondary's `commit_ts` via the source-group migration write tracker even when the lock resolves before the next migrator tick, (b) DELTA_COPY's `(delta_floor, fence_ts]` window exports the committed secondary version, (c) it is present on the target after CUTOVER. Control: a build that scopes the running minimum to moved primaries (and relies on the primary-keyed `!txn|cmt|` record, which routes outside) must drop the secondary version — red test for this fix.
- **FENCE drain is non-starving (§3.2a / §7.1).** Run a steady workload that continuously prepares txns in the moving range. Assert `BACKFILL → FENCE` publishes `WriteFenced` before waiting for an empty drain, new prepares are rejected once every voter has applied the fence, existing prepares resolve through the narrow `Phase_COMMIT` / `Phase_ABORT` lane, and the drain completes within one txn-TTL. In the same FENCE window, issue `DEL_PREFIX` with (a) empty prefix and (b) a prefix whose byte interval intersects only the moving right child after its start; both must return `ErrRouteWriteFenced` before `handleDelPrefix` writes tombstones. Controls: the old "wait empty while Active" design never reaches FENCE under the steady workload, and a point-key-only fence lets the prefix delete land on source after `delta_floor` is fixed.
- **Reserved default catalog prefix is immovable and data-plane protected (§3.2).** When the source group is the default group, a cross-group split whose moving right child intersects `!dist|` fails with `ErrReservedRange` before creating a SplitJob. Separately, user-plane raw `PUT` / `DEL_PREFIX ""` / prepare attempts that touch `!dist|`, `!dist|migstage|`, `!migstage|`, `!migwrite|`, or `!migfence|` fail even without an active migration. Controls: allowing the split and then running CLEANUP would delete `!dist|meta|`, `!dist|route|`, or `!dist|job|` records; allowing empty prefix delete would wipe migration-control rows.
- **CUTOVER target-readiness and source-read barrier (§6.4 step 0 / §7.2.2e, codex round-17 P1).** Pause the target catalog watcher after `ApplyTargetStagedReadiness` ACK and let another coordinator refresh against the default group after CUTOVER; target reads/writes for the moved interval must return retry/ownership-unknown until the target route descriptor with matching `staged_visibility_active` is local, never live-only empty data or a low live write. Then pause the source catalog watcher/heartbeat delivery after the source-side `ArmCutoverReadFence` ACK but before the source applies the CUTOVER snapshot; a stale coordinator's point read and cross-boundary scan routed to the source must return `ErrRouteMoved`/retry, never the pre-CUTOVER source value. Also inject default-group ownership RPC timeout/unreachable while `max_cutover_version_seen > source_local_version`; the source must return `ErrRouteOwnershipUnknown` for both point and scan requests without touching source MVCC, and the coordinator must retry through `WatcherRefresh`. Crash controls: kill the default leader after persisting target/source `ARMING` but before each proposal, and again after each ACK but before `ARMED`; recovery must either re-arm/probe and proceed to CUTOVER or persist `CLEARING` and remove both guards before rollback/abandon. Internal scan controls use raw prefixes such as `!lst|meta|d|`, `!lst|claim|`, `!hs|`, `!st|`, and `!zs|`; the passing implementation normalizes them to route-key intervals, while the red implementation that compares raw bounds against catalog routes serves stale collection rows. Control: disabling either barrier, treating timeout as source/target-owned, skipping the `ARMING` witness, or using raw scan bounds for internal ownership checks reproduces stale reads or stranded pending fences.
- **Promotion ownership handoff (§6.4).** Kill the target leader after a `PromoteStaged` apply, and kill the default leader after target-local `PromotionState.done=true` but before the default-group CAS clears `staged_visibility_active`. Assert no staged rows are skipped, range reads keep using the staged/live merge until the CAS lands, and retrying the CAS reaches DONE without data changes.
- Cross-shard txn coordinated through `kv/transaction.go` straddling the split key — must serialize correctly across CUTOVER.

### 10.3 Jepsen

- New `jepsen/elastickv/split_workload.clj` (or extend the existing dynamodb workload):
  - Linearizable register over a key that crosses the moving boundary.
  - Nemeses: leader-kill on source group; partition source↔target mid-DELTA_COPY; abandon mid-FENCE.
  - Pass criterion: `{:valid? true}` per existing Elle setup.
- M4 lands the broader hotspot + nemesis matrix; M2 only adds a deterministic "split happens at workload time T" scenario.

### 10.4 Five-lens self-review (CLAUDE.md)

| Lens | M2-specific risk |
|---|---|
| Data loss | CUTOVER atomicity; ImportRangeVersions idempotency; the BACKFILL/DELTA_COPY commit-ts windows have no gap; raw MVCC export preserves tombstones and TTL expiry headers. |
| Concurrency | Migrator goroutine vs leadership election; FENCE visibility timing on followers; target-local promotion cursor vs default-group visibility CAS; AbandonSplitJob race with phase advance. |
| Performance | Chunk size vs gRPC frame limit; pacing default vs prod throughput; cursor lookup cost per import. |
| Data consistency | Composed-1 alignment at CUTOVER; reader at `≤ fence_ts` on source vs reader at `≥ catalog_version+1` on target; target-local full-HLC floor across restart/snapshot; raw staged/live merge suppresses hidden tombstone/TTL winners; internal-key route mapping, including list delta/claim rows, Redis wide-column prefixes, matching `routeKey()` decoders, route-key-normalized scan ownership checks, and `DEL_PREFIX` range-footprint fencing/tracking. |
| Test coverage | Property tests over chunk-then-restart sequences; FSM unit for FENCE; Redis collection migration controls; Jepsen split + nemesis. |

## 11. Implementation Touchpoints

Phased into reviewable PRs, each lands behind its own doc-or-test gate:

| PR | Scope | Tests |
|---|---|---|
| M2-PR0 | Prepare-time `commit_ts` plumbing: coordinator allocates/fences `commit_ts` before PREPARE; prepare request and txn metadata carry it to every participant; `handlePrepare` writes the same value into each `txnLock`; the migration write tracker records that value at prepare admission; COMMIT reuses it. Keep decoder backward-compat for existing lock values without the field (zero → migrator fallback to direct commit-record lookup / pending drain). | `kv/txn_codec_test.go` round-trip + backward-compat case; `kv/txn_prepare_commit_ts_test.go` coordinator → prepare request/meta → `handlePrepare` → tracker path, plus red controls for codec-only and commit-time allocation |
| M2-PR1 | SplitJob codec + reserved-key layout + catalog read/write helpers | catalog_test |
| M2-PR2 | `proto/distribution.proto` + `proto/internal.proto` regen (`MVCCVersion.expire_at`, bracket/batch identity, scan-budget fields); wire fields plumbed but unused | proto regen; build |
| M2-PR3 | `store/MVCCStore.ExportVersions` raw committed-version export + `ImportVersions` with idempotency, tombstone/`expire_at` preservation, target-local full-HLC floor persistence, no migrator wiring | store unit incl. tombstone/TTL + HLC-floor restore cases + property |
| M2-PR4 | `distribution/migrator.go` state machine + export bracket planner, including list delta/claim and Redis wide-column brackets + matching `routeKey()` decoder coverage — same-group target (no-op data move) to lock the state shape | migrator unit + `migrator_export_plan_test` |
| M2-PR5 | Coordinator + FSM FENCE rejection (`ErrRouteWriteFenced`) with point and `DEL_PREFIX` range-footprint checks + route-faithful txn-lock drain (§3.2a.0a) | fsm + coordinator unit + `migrator_lock_drain_test` |
| M2-PR6 | Cross-group end-to-end: ExportRangeVersions / ImportVersions server-side handlers + migrator BACKFILL/DELTA_COPY + raw-candidate staged/live merge read path + §7.2.2e source-side cutover read-fence arm with route-key-normalized scan ownership checks | integration incl. delete/TTL DELTA_COPY, Redis list/hash/set/zset/stream migration, HLC restart cases + `kv/fsm_cutover_read_fence_test.go` |
| M2-PR7 | Target-local `PromotionState` + background promoter + ordered default-group promotion-complete CAS + source/target cleanup before DONE history move; AbandonSplitJob + CLEANUP GC + Jepsen split workload | `kv/fsm_promote_staged_test.go` raw hidden-version merge + jepsen suite |
| M2-PR8 | Rename `*_proposed_*` → `*_partial_*` after PR1 ships; update parent partial doc M2 status; rename to `*_implemented_*` after PR7 |  |

Each PR follows the five-lens self-review and is gated by its tests + `make lint`.

### 11.1 Rolling-upgrade compatibility

Closes gemini medium on PR #945: an M1 node that has not yet been upgraded does not understand `RouteStateWriteFenced` enforcement on writes, the read fence in §7.2, or the import RPCs. Starting a cross-group split while M1 nodes are still serving is a silent-write-loss hazard.

Layered mitigations:

1. **Capability advertisement.** Each node advertises a `node_capability_bitfield` in its periodic heartbeat to the default group (existing distribution heartbeat — extend by one field). M2-capable nodes set the `cap_migration_v2` bit at boot.
2. **Cluster-readiness gate at the entry RPC.** `SplitRangeRequest` with `target_group_id != source_group_id` is rejected at `adapter/distribution_server.go` with `ErrClusterNotReadyForMigration` unless **every active node** advertises `cap_migration_v2`. Same-group split (M1) remains unconditionally available — its semantics did not change.
3. **In-flight job invalidation on downgrade.** If the default-group leader observes any node losing `cap_migration_v2` mid-migration (rare — would only happen with a botched rollback), the migrator pauses (stays in current phase, no new state advancement) and surfaces `ErrClusterCapabilityRegressed`. Operator must either roll forward or `AbandonSplitJob`.
4. **M2-PR0..PR7 land in this order so the capability bit only opens when the full feature is present (closes claude round-11 P2 on PR #945).** PR0-PR3 are wire/codec/store additions with **no behaviour change for M1** (no SplitJob created without explicit `target_group_id != source_group_id`); PR4 introduces the state machine but no data move; PR5 lands FENCE rejection (`verifyRouteNotFenced` + `ErrRouteWriteFenced`); PR6 lands `ExportRangeVersions` / `ImportRangeVersions` server-side handlers + §6.4 step 2 staged-merge read path + §7.2.2e source-side cutover read-fence arm; PR7 lands the background promoter + `AbandonSplitJob` + CLEANUP drain. **The `cap_migration_v2` bit goes live only in PR7 commit**, when the full end-to-end migration path can complete — not in PR5 (where FENCE is enforced but BACKFILL has no export handler yet, so a SplitJob would create successfully and then fail at BACKFILL, giving operators false assurance). The earlier-rounds intention was "the bit guards against rollout-skew silent execution," and the corrected intent is "the bit guards against operators starting a migration in a half-shipped cluster." If a finer-grained gate is desired later, OQ-18 records the option to split into `cap_fence_v2` (PR5) + `cap_data_move_v2` (PR6/PR7), with `SplitRangeRequest` gated on the conjunction — out of scope for v1.
5. **Test plan**: `distribution/capability_test.go` exercises (a) gate accepts M2-only cluster, (b) gate rejects mixed cluster, (c) gate rejects on heartbeat staleness (no advertisement within 2× heartbeat interval treated as no capability).

This is independent of the existing rolling-upgrade protocol for unrelated subsystems and adds zero overhead for clusters never using cross-group split.

## 12. Open Questions

1. **Job concurrency.** M2 caps at one in-flight migration. Confirm acceptable for first cut; M3 may want concurrent jobs for hotspot bursts.
2. **Pacing knobs.** Default 1 MiB chunk + 5 ms inter-chunk pacing — these are guesses; should we ship a metrics-emitting default and let operators tune via `SplitRangeRequest.options`?
3. **Grace period (RESOLVED — `readFenceGrace = 30 s`).** §7.2.4 sets the default at 30 s with the rationale "comfortably exceeds both lease TTL and watcher tick × 2." All cross-references (§4 CLEANUP row) now match this value. Operators on extreme deployments (very long lease TTLs, or watcher polling intervals beyond the defaults) raise it via the existing `readFenceGrace` knob.
4. **`MVCCVersion.key_family` enum.** Add a strict enum in proto, or stay numeric and document constants in `kv/`? Strict enum costs proto churn for every new internal family; numeric + go-side constant has been the project pattern.
5. **`AbandonSplitJob` after CUTOVER.** Currently rejected. Worth offering a "reverse migration" RPC in M2, or defer to M3+?
6. **Backfill source visibility (RESOLVED — migration write tracker + fence-first drain; BACKFILL snapshot exclusion is intentional).** Two questions inside one OQ — resolved as follows. (a) The `Phase_COMMIT` stranding hazard (a prepared txn whose `Phase_COMMIT` arrives after FENCE rejection — flagged codex round-12 P1) is closed by §3.2a's fence-first drain: `BACKFILL → FENCE` publishes `WriteFenced` before waiting for an empty drain, so new prepares cannot starve the drain after every source voter applies the fence, while existing prepared txns can still resolve through §7.1's narrow `Phase_COMMIT` / `Phase_ABORT` lane until `post_fence_drain_completed=true`. (b) The BACKFILL-snapshot semantic question — *should the snapshot include intent locks or not?* — is closed in the negative: **excluding intent locks from BACKFILL is intentional and correct**. Any txn whose `commit_ts ≤ snapshot_ts` and whose prepare had resolved before the BACKFILL iterator reached its key is captured in BACKFILL as a fully-committed version; every other committed version for the moving range — anything with `commit_ts > snapshot_ts`, **and** any migration-window low-ts write with `commit_ts ≤ snapshot_ts` that committed/applied after BACKFILL passed its key, including raw point requests and `DEL_PREFIX` range tombstones at `r.Ts`, one-phase txns at `TxnMeta.CommitTS`, and prepared txns on the primary or on a secondary key that resolves into the moving range (§6.1.0 step 2's tracker-backed running minimum, keyed per moved key not only the primary and using range footprints for `DEL_PREFIX` — codex round-15 P1 + round-16 P1 + round-18 P1 + round-20 P1 + round-21 P1) — is captured by DELTA_COPY's `(delta_floor, fence_ts]` window, where `delta_floor ≤ snapshot_ts` is finalised only after the fence blocks new writes/prepares. Intent locks themselves are coordinator-managed state and don't need to migrate with the data — after CUTOVER the target's empty intent-lock space for the moved range is correct, because the FENCE drain guarantees no prepare remains unresolved across the boundary.
7. **Should `target_group_id == source_group_id` go through the M2 state machine** (for consistent observability) or short-circuit through the M1 fast-path? Lean: short-circuit, but the SplitJob still gets recorded for audit.

## 13. Acceptance Criteria

1. `SplitRange` with `target_group_id != source_group_id` returns a `job_id` and the migration completes with `phase=DONE`.
2. `ListRoutes` after a successful cross-group split shows two non-overlapping routes whose `raft_group_id` differs.
3. Writes to the moving range during FENCE / DELTA_COPY surface a retryable error to the client and are eventually accepted on the target after CUTOVER.
4. Killing the default-group leader at any point during a migration resumes the job to completion on the new leader without operator action.
5. Jepsen split workload (linearizable register straddling the boundary) passes under leader-kill and partition nemeses.
6. M1 same-group split path remains green (existing tests + manual via `elastickv-split` CLI).

## 14. Lifecycle

This document begins as `*_proposed_*`. Per CLAUDE.md:

- Rename to `*_partial_*` after M2-PR1 lands; track per-PR landing under §11.
- Rename to `*_implemented_*` after M2-PR7 ships and the parent partial doc's M2 row is checked off.

`git mv` is used so history follows.
