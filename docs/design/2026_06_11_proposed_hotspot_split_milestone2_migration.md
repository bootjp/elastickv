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
- `!dist|job|<job_id>` — encoded `SplitJob` record.

`SplitJob` fields:

| Field | Type | Notes |
|---|---|---|
| `job_id` | uint64 | Allocated from `next_job_id`. |
| `source_route_id` | uint64 | Route being split. |
| `split_key` | bytes | Logical (routing-key) boundary. |
| `target_group_id` | uint64 | Where the right child lands; equals source group for same-group split. |
| `phase` | enum | `PLANNED` / `BACKFILL` / `FENCE` / `DELTA_COPY` / `CUTOVER` / `CLEANUP` / `DONE` / `FAILED`. |
| `snapshot_ts` | uint64 | HLC commit-ts pinned at BACKFILL entry. |
| `fence_ts` | uint64 | HLC commit-ts pinned at FENCE → DELTA_COPY entry. |
| `cursor` | bytes | Next-key cursor for BACKFILL / DELTA_COPY resume. |
| `last_error` | string | Most recent transient failure (for operator diagnosis). |
| `started_at`, `updated_at` | int64 | Wall-clock ms; **diagnostic only**, must not be read by any ordering-sensitive logic (CLAUDE.md HLC rule). |

Encoding mirrors `RouteDescriptor` (a single-byte codec version prefix + protobuf body). Reserve a new codec constant `catalogJobCodecVersion = 1`.

### 3.2 RouteState transitions during a job

Same-group split (M1): `Active → (atomic CAS) → Active+Active`.

Cross-group split (M2):

```
source.Active
  ├─ BACKFILL:   right child created with state=MigratingTarget on target group;
  │              source remains Active; writes still flow to source.
  ├─ FENCE:      source.right_child → WriteFenced; writes on the moving range
  │              are rejected with ErrRouteWriteFenced (retryable).
  ├─ DELTA_COPY: same as FENCE; reads still work from source.
  ├─ CUTOVER:    atomic catalog bump — source.right_child removed; target's
  │              MigratingTarget → Active; route version v+1.
  └─ CLEANUP:    grace period; then source GCs the moved key range.
```

The Composed-1 gate already rejects an apply on the wrong group at the observed catalog version; CUTOVER bumps `catalog_version` so any straggler write from an unaware coordinator fails closed at apply time on either side.

## 4. State Machine and Recovery

The migrator is a goroutine on the **default-group leader**. State lives in the catalog (durable); the goroutine is a thin reconciler that picks up wherever the last leader stopped.

Phase transitions are themselves applied through the default Raft group so every node sees the same job state. A transition writes the new phase + cursor + ts to `!dist|job|<id>` via a CAS on the prior phase — concurrent migrators on a leadership flap can't race.

Failure semantics per phase:

| Phase | Crash safety |
|---|---|
| `PLANNED` | Job record durable; no side effects yet. |
| `BACKFILL` | Cursor persisted after each chunk batch (configurable, default every 256 keys); on resume the migrator re-reads cursor and continues. Idempotent ImportRangeVersions (see §6) means re-sending a batch is harmless. |
| `FENCE` | Catalog state is `WriteFenced`; coordinator rejects until phase advances. A leader flap here keeps the fence (state is durable). |
| `DELTA_COPY` | Same as BACKFILL — chunked + cursor-persisted. `fence_ts` is recorded at FENCE entry, so a resumed DELTA_COPY uses the same upper bound. |
| `CUTOVER` | Single atomic catalog write under CAS on `expected_catalog_version`. Either fully visible or fully not — no partial split. |
| `CLEANUP` | After grace period (default 60s, configurable). GC is idempotent (delete-if-version-≤-fence_ts). |
| `FAILED` | Operator-visible terminal state. Job sits until a manual `RetryJob` or `AbandonJob` RPC. Abandoned jobs unblock the source: `WriteFenced` is rolled back to `Active`. |

`PLANNED → BACKFILL` can be rolled back by `AbandonJob` (no data moved yet). After CUTOVER the job is one-way; rollback would require a reverse migration, which is M2-out-of-scope.

## 5. Wire / RPC Changes

### 5.1 `proto/distribution.proto`

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

message ListSplitJobsRequest {}

message ListSplitJobsResponse {
  repeated SplitJob jobs = 1;
}

message AbandonSplitJobRequest {
  uint64 job_id = 1;
  // Operator may abandon only PLANNED / BACKFILL / FENCE / DELTA_COPY.
  // CUTOVER + later is rejected (data is partially landed in target).
}

message AbandonSplitJobResponse {}

service Distribution {
  // ... existing M1 RPCs ...
  rpc GetSplitJob (GetSplitJobRequest) returns (GetSplitJobResponse) {}
  rpc ListSplitJobs (ListSplitJobsRequest) returns (ListSplitJobsResponse) {}
  rpc AbandonSplitJob (AbandonSplitJobRequest) returns (AbandonSplitJobResponse) {}
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
  bytes range_start = 1;
  bytes range_end = 2;
  uint64 max_commit_ts = 3;  // snapshot_ts (BACKFILL) or fence_ts (DELTA_COPY)
  uint64 min_commit_ts = 4;  // 0 for BACKFILL; snapshot_ts for DELTA_COPY
  bytes cursor = 5;          // resume token; empty on first call
  uint32 chunk_bytes = 6;    // soft cap, server may exceed by one row
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
  bytes value = 4;      // omitted when tombstone == true
  // Internal-key family (txn/list/redis) tag, so the importer can
  // dispatch into the matching store helper instead of inferring
  // from the byte prefix.
  uint32 key_family = 5;
}

message ImportRangeVersionsRequest {
  uint64 job_id = 1;
  repeated MVCCVersion versions = 2;
  bytes cursor = 3;  // for ack/idempotency
}

message ImportRangeVersionsResponse {
  bytes acked_cursor = 1;
}
```

Streaming the export keeps the migrator memory-bounded; a single `ImportRangeVersions` RPC per chunk lets the importer ack the cursor (the migrator persists it in the SplitJob on ack).

## 6. MVCC Range Export / Import

### 6.1 Source side (`store/mvcc_store.go`, `store/lsm_store.go`)

New method on `MVCCStore`:

```go
// ExportVersions iterates committed versions in the half-open range
// [start, end) whose commit_ts is in (minCommitTS, maxCommitTS],
// resuming from cursor. Returns a slice of versions (capped roughly
// at chunkBytes) and the next cursor, plus a sentinel when exhausted.
// Visibility rules MUST match the read path's snapshot at maxCommitTS;
// in particular, intent locks (!txn|lock|...) are excluded — those
// belong to in-flight txns the importer must not observe as commits.
ExportVersions(ctx context.Context, start, end []byte, minCommitTS, maxCommitTS uint64,
    cursor []byte, chunkBytes int) ([]MVCCVersion, []byte, bool, error)
```

The function uses the existing snapshot read primitive (the same one Composed-1 already trusts for visibility), so MVCC semantics during export are guaranteed to match a reader at `maxCommitTS`.

### 6.2 Target side

New method:

```go
// ImportVersions writes the given versions idempotently. A second call
// with the same (job_id, cursor) MUST be a no-op past the recorded
// cursor — the importer records last-applied cursor under
// !dist|job|<id>|import_cursor so a network retry doesn't double-write.
ImportVersions(ctx context.Context, jobID uint64, versions []MVCCVersion,
    cursor []byte) ([]byte, error)
```

Idempotency: persist `(job_id, cursor)` after each batch's apply, and on a duplicate request check the persisted cursor.

### 6.3 Internal-key coverage

§9 of the parent doc enumerates the key families. The exporter scans the raw storage range `[start, end)` filtered by **routeKey** equivalence to the moving range, so internal keys (`!txn|...`, `!lst|...`, redis hash/list/etc.) land in the same shard as their logical owner. The importer dispatches by `key_family` into the matching store helper to preserve any per-family invariant (e.g., list head pointer updates).

A simple safety check: every exported key must map back to the moving range under `routeKey()`. The exporter asserts this on every row before adding to the chunk; an assertion failure aborts the job and surfaces in `last_error`. This protects against a future internal-key family being added that the exporter forgot to teach.

## 7. Coordinator / FSM Integration

### 7.1 FENCE write rejection

Coordinator-side:

- `kv/sharded_coordinator.go` consults the engine's per-route state. Routes in `WriteFenced` reject writes with a new sentinel `ErrRouteWriteFenced`, mapped to:
  - gRPC: `codes.Aborted` + status detail `{kind:"route_write_fenced", route_id, retry_after_ms}`.
  - Redis adapter: `MOVED`-style retry (existing path).
  - DynamoDB adapter: throttled error so the SDK retries with backoff.

FSM-side defense in depth:

- `kv/fsm.go` already routes through `verifyComposed1` at apply. Extend `verifyOwnerFromSnapshot` to also reject when the resolved route's state is `WriteFenced` — even if a coordinator with a stale engine forwarded a write, the FSM closes the door. The error wraps `ErrRouteWriteFenced` so M4 retry logic differentiates from `ErrComposed1Violation`.

Reads are **not** fenced: `ShardStore.GetAt` continues to serve from source until CUTOVER. Snapshot reads at any `commit_ts ≤ fence_ts` remain consistent because the source's MVCC history is unchanged through DELTA_COPY.

### 7.2 CUTOVER coherence with Composed-1

The Composed-1 gate compares observed catalog version against current. CUTOVER bumps the version by 1; any in-flight txn pinned at the pre-CUTOVER version that lands on the target sees its observed-version owner ≠ target → ErrComposed1Violation → coordinator retries at the new version → routes to the target's new active route. This is the same flow PR #927 exercised; M2 reuses it, no new gate work.

### 7.3 Engine route lookup invariant

After CUTOVER, the engine's per-route ranges no longer overlap (the right child moved entirely to target's range). `Engine.GetRoute(routeKey(k))` therefore continues to return exactly one route, matching M1's invariant. No `OwnerOf` change is needed beyond what PR #932 already shipped.

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
| Leader loss during BACKFILL | New leader resumes from persisted cursor. | None — job timeline lengthens. |
| Leader loss during FENCE | Catalog state is `WriteFenced` already, so coordinators still reject writes. New migrator advances. | Continued retryable rejection until DELTA_COPY → CUTOVER. |
| Leader loss during CUTOVER | The CAS catalog write either landed (new version visible, job → CLEANUP) or not (job stays in DELTA_COPY, redo). Never partial. | None. |
| Target group temporarily unreachable | Migrator retries with backoff; phase doesn't advance. | None. |
| Source group temporarily unreachable | Same. Reads to source continue to fail with whatever transport error the adapter raises (independent of split). | Existing per-RPC retry. |
| Duplicate ImportRangeVersions RPC | Importer compares request cursor to persisted; skips already-applied prefix; acks. | None. |
| Operator `AbandonSplitJob` during BACKFILL/FENCE/DELTA_COPY | Migrator rolls catalog back to `Active`; job → FAILED with `reason=abandoned`. | Brief retryable rejection clears. |
| Crash between job state writes | Job state is **last-write-wins** on CAS; replaying a phase is safe by construction (idempotent imports, bounded ts windows). | None. |

## 10. Test Strategy

### 10.1 Unit

- `distribution/migrator_test.go`: state machine — exhaustive phase transitions, abandon paths, CAS contention.
- `distribution/catalog_test.go`: SplitJob codec round-trip + version-1 stability.
- `store/mvcc_store_export_test.go`: snapshot semantics — exported versions match a reader at `maxCommitTS`; intent locks excluded; cursor monotonic.
- `store/mvcc_store_import_test.go`: idempotency under duplicate cursor; key_family dispatch.
- `kv/fsm_route_fenced_test.go`: FENCE rejection at FSM (table-driven over key families, mirroring `fsm_composed1_test.go`).
- `kv/sharded_coordinator_route_fenced_test.go`: coordinator-side rejection + retry-after surfacing.
- Property tests (`pgregory.net/rapid`): replay a sequence of (export-chunks, import-acks, leader-flap) and assert (a) all source keys land on target, (b) no duplicates committed at any commit_ts.

### 10.2 Integration

- `kv/integration_split_test.go`: same-group split (M1 regression) + cross-group split happy path.
- Migrator restart mid-phase: cursor recovery, no double-apply.
- Concurrent writes during BACKFILL (fence is not yet up; writes still serve from source, get exported with their commit_ts ≤ snapshot_ts excluded, delta-copied in (snapshot_ts, fence_ts]).
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
| Data loss | CUTOVER atomicity; ImportRangeVersions idempotency; the BACKFILL/DELTA_COPY commit-ts windows have no gap. |
| Concurrency | Migrator goroutine vs leadership election; FENCE visibility timing on followers; AbandonJob race with phase advance. |
| Performance | Chunk size vs gRPC frame limit; pacing default vs prod throughput; cursor lookup cost per import. |
| Data consistency | Composed-1 alignment at CUTOVER; reader at `≤ fence_ts` on source vs reader at `≥ catalog_version+1` on target; internal-key route mapping. |
| Test coverage | Property tests over chunk-then-restart sequences; FSM unit for FENCE; Jepsen split + nemesis. |

## 11. Implementation Touchpoints

Phased into reviewable PRs, each lands behind its own doc-or-test gate:

| PR | Scope | Tests |
|---|---|---|
| M2-PR1 | SplitJob codec + reserved-key layout + catalog read/write helpers | catalog_test |
| M2-PR2 | `proto/distribution.proto` + `proto/internal.proto` regen; wire fields plumbed but unused | proto regen; build |
| M2-PR3 | `store/MVCCStore.ExportVersions` + `ImportVersions` with idempotency, no migrator wiring | store unit + property |
| M2-PR4 | `distribution/migrator.go` state machine — same-group target (no-op data move) to lock the state shape | migrator unit |
| M2-PR5 | Coordinator + FSM FENCE rejection (`ErrRouteWriteFenced`) | fsm + coordinator unit |
| M2-PR6 | Cross-group end-to-end: ExportRangeVersions / ImportRangeVersions server-side handlers + migrator BACKFILL/DELTA_COPY | integration |
| M2-PR7 | AbandonSplitJob + CLEANUP GC + Jepsen split workload | jepsen suite |
| M2-PR8 | Rename `*_proposed_*` → `*_partial_*` after PR1 ships; update parent partial doc M2 status; rename to `*_implemented_*` after PR7 |  |

Each PR follows the five-lens self-review and is gated by its tests + `make lint`.

## 12. Open Questions

1. **Job concurrency.** M2 caps at one in-flight migration. Confirm acceptable for first cut; M3 may want concurrent jobs for hotspot bursts.
2. **Pacing knobs.** Default 1 MiB chunk + 5 ms inter-chunk pacing — these are guesses; should we ship a metrics-emitting default and let operators tune via `SplitRangeRequest.options`?
3. **Grace period.** CLEANUP grace default 60s. Long enough to bound stale-route reads on a lagging follower? Or tie to `hlcPhysicalWindowMs` (3 s) × N?
4. **`MVCCVersion.key_family` enum.** Add a strict enum in proto, or stay numeric and document constants in `kv/`? Strict enum costs proto churn for every new internal family; numeric + go-side constant has been the project pattern.
5. **`AbandonSplitJob` after CUTOVER.** Currently rejected. Worth offering a "reverse migration" RPC in M2, or defer to M3+?
6. **Backfill source visibility.** Backfill reads at `snapshot_ts`; if `snapshot_ts` is younger than any in-flight prepare, the exported state excludes that prepare's intent. Is that semantic correct, or do we want a stricter "wait until all prepares ≤ snapshot_ts are either committed or aborted" gate before BACKFILL entry?
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
