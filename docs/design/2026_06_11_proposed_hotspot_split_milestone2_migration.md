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
| `phase` | enum | `PLANNED` / `BACKFILL` / `FENCE` / `DELTA_COPY` / `CUTOVER` / `CLEANUP` / `DONE` / `FAILED`. |
| `snapshot_ts` | uint64 | HLC commit-ts pinned at BACKFILL entry. |
| `fence_ts` | uint64 | HLC commit-ts pinned at FENCE → DELTA_COPY entry. |
| `cutover_version` | uint64 | Catalog version produced by the CUTOVER write; populated at CUTOVER, used by §7.2 read-fence and §4.2 GC. |
| `cursor` | bytes | Next-key cursor for BACKFILL / DELTA_COPY resume. |
| `last_error` | string | Most recent transient failure (for operator diagnosis). |
| `started_at`, `updated_at`, `terminal_at` | int64 | Wall-clock ms; **diagnostic only**, must not be read by any ordering-sensitive logic (CLAUDE.md HLC rule). `terminal_at` is set when phase enters DONE/FAILED. |

Encoding mirrors `RouteDescriptor` (a single-byte codec version prefix + protobuf body). Reserve a new codec constant `catalogJobCodecVersion = 1`.

#### 3.1.1 Job retention (bounded storage)

Without a retention policy the `!dist|job|*` namespace grows unboundedly across a long-lived cluster (gemini medium on PR #945). M2 ships these bounds:

- **Live cap**: at most `maxLiveJobs = 64` non-terminal SplitJobs in the catalog at any time. `SplitRange` rejects new requests with `ErrTooManyInFlightJobs` once the live count hits the cap; this also throttles a runaway M3 scheduler. The cap is intentionally generous (one per shard group × small factor) — operators raise it via a future runtime config if needed.
- **Audit retention**: terminal jobs (`DONE`, `FAILED`, abandoned) are moved by the migrator from `!dist|job|<id>` to `!dist|jobhist|<terminal_at_ms_be>|<id>` immediately on transition. The history key sorts by terminal time so a single bounded prefix-scan trims oldest entries.
- **History bound**: `maxJobHistory = 1_000` total entries OR retention age `historyTTL = 7d`, whichever is tighter. The migrator runs a GC sweep at most once per minute on the default-group leader; it batch-deletes excess entries with a single Raft proposal.
- **Listing**: `ListSplitJobs` returns live + history (default cap 200 newest), with `since_terminal_at_ms` / `phase` filters; operators page through history via cursor.

Bounded both by count (worst case ≤ `maxLiveJobs + maxJobHistory ≈ 1k` records ≈ low-MB storage) and by time. The cap on live jobs also prevents `ListSplitJobs` enumeration from blowing the response.

### 3.2 RouteState transitions during a job

Same-group split (M1): `Active → (atomic CAS) → Active+Active`.

Cross-group split (M2) — **catalog stays single-source-of-truth, never holds an overlapping route**:

```text
catalog snapshot evolution                migrator action
============================              ====================
[ source.Active(full range) ]
        |
        |  PLANNED → BACKFILL              SplitJob created on default group;
        |                                  no catalog mutation. Target group
        |                                  accepts ImportRangeVersions into a
        |                                  shadow keyspace (not yet routable).
        v
[ source.Active(full range) ]              BACKFILL: chunked export from
                                           source @ snapshot_ts; idempotent
                                           import into target's shadow space.
        |
        |  BACKFILL → FENCE                Atomic catalog write: source's
        v                                  right child split out as a NEW
[ source.Active(left)           ]          RouteDescriptor with state =
[ source.WriteFenced(right)     ]          WriteFenced, raft_group_id =
                                           source. Catalog snapshot remains
                                           non-overlapping. Coordinator/FSM
                                           reject writes on the fenced route.
        |
        |  FENCE → DELTA_COPY              Copy (snapshot_ts, fence_ts] from
        |                                  source to target shadow space.
        v
[ source.Active(left)           ]
[ source.WriteFenced(right)     ]
        |
        |  DELTA_COPY → CUTOVER            Atomic catalog write under
        v                                  CAS(expected_catalog_version):
[ source.Active(left)           ]            (a) source.WriteFenced(right)
[ target.Active(right)          ]                  → removed
                                             (b) target.Active(right) inserted
                                                  with raft_group_id=target
                                           Catalog snapshot still
                                           non-overlapping; version+=1
                                           atomically. `cutover_version`
                                           recorded in the SplitJob.
        |
        |  CUTOVER → CLEANUP               Source group's leader deletes
        v                                  the moved key range from its
[ source.Active(left)           ]          MVCC store after the read-fence
[ target.Active(right)          ]          grace window (§7.2).
        |
        |  CLEANUP → DONE                  Job moved to !dist|jobhist|<...>.
        v
[ source.Active(left)           ]
[ target.Active(right)          ]
```

Key invariant: **catalog snapshots never contain overlapping ranges**. The target shadow keyspace lives outside the catalog (BACKFILL / DELTA_COPY land into private `!dist|migstage|<job_id>|<raw_key>` keys on the target group's MVCC store; CUTOVER promotes the staged data to live keys via a single bulk-rename FSM apply). This keeps `routesFromCatalog` + `validateRouteOrder` (`distribution/engine.go:496-527`) green throughout the migration — addresses codex P1 on overlapping routes (PR #945 review). RouteState `MigratingSource` / `MigratingTarget` are reserved but unused in M2; they remain available for a future merge / multi-stage migration design.

The Composed-1 gate rejects an apply on the wrong group at the observed catalog version; CUTOVER bumps `catalog_version` so any straggler write from an unaware coordinator fails closed at apply time on either side.

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

### 4.2 Job garbage collection

The retention policy from §3.1.1 runs as a CLEANUP-tail sweep on the default-group leader. Concretely:

1. On a phase transition `CUTOVER → CLEANUP`, the migrator records `terminal_at` candidate and waits for the read-fence grace window (§7.2.4) before transitioning `CLEANUP → DONE`.
2. On `CLEANUP → DONE` or `* → FAILED`, the migrator issues one Raft proposal that (a) deletes `!dist|job|<id>`, (b) writes `!dist|jobhist|<terminal_at_ms_be>|<id>` with the final body.
3. Once per minute the leader scans `!dist|jobhist|` for entries older than `historyTTL` (default 7d) OR exceeding `maxJobHistory` (1k) and deletes the oldest excess in one proposal.

GC failure modes are bounded: the sweep is idempotent (delete-if-exists), and a stale leader's proposal lands as a no-op on the new leader's apply because the keys are already gone.

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

The `store` package is generic and **must not** import `kv` or know about routing keys (gemini medium on PR #945). Filtering is injected as a delegate from the caller.

New method on `MVCCStore`:

```go
// KeyFilter is the in-package contract: the caller decides which raw
// keys belong to the slice of MVCC space being exported. nil means
// "accept every key in [start, end)". The decision MUST be deterministic
// and pure — the export streams it on the iteration goroutine.
type KeyFilter func(rawKey []byte) bool

// ExportVersions iterates committed versions in the half-open range
// [start, end) whose commit_ts is in (minCommitTS, maxCommitTS],
// resuming from cursor. Returns a slice of versions (capped roughly
// at chunkBytes) and the next cursor, plus a sentinel when exhausted.
// Visibility rules MUST match the read path's snapshot at maxCommitTS;
// in particular, intent locks (!txn|lock|...) are excluded — those
// belong to in-flight txns the importer must not observe as commits.
// `accept` (nil = no filtering) is the §6.3 decoupling seam.
ExportVersions(ctx context.Context, start, end []byte, minCommitTS, maxCommitTS uint64,
    cursor []byte, chunkBytes int, accept KeyFilter) ([]MVCCVersion, []byte, bool, error)
```

The function uses the existing snapshot read primitive (the same one Composed-1 already trusts for visibility), so MVCC semantics during export are guaranteed to match a reader at `maxCommitTS`. `accept` is consulted before a row is added to the chunk; rejected rows do not advance the chunk-bytes budget so the caller sees deterministic per-call shapes.

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

### 6.3 Internal-key coverage and the routeKey delegate

§9 of the parent doc enumerates the key families. The migrator builds a `KeyFilter` closure in `kv/` (where `routeKey()` lives) and passes it to `store.ExportVersions`:

```go
// kv/migrator_filter.go (sketch)
func RouteKeyFilter(rangeStart, rangeEnd []byte) store.KeyFilter {
    return func(rawKey []byte) bool {
        rKey := routeKey(rawKey)
        // half-open [rangeStart, rangeEnd) in the routing-key namespace.
        if bytes.Compare(rKey, rangeStart) < 0 { return false }
        if rangeEnd != nil && bytes.Compare(rKey, rangeEnd) >= 0 { return false }
        return true
    }
}
```

The closure ensures internal keys (`!txn|...`, `!lst|...`, redis hash/list/etc.) land in the same shard as their logical owner without `store` ever importing `kv`. The importer dispatches by `key_family` into the matching store helper (in a future kv-level `Import` wrapper) to preserve per-family invariants (e.g., list head pointer updates).

A simple safety check: every exported key in the migrator's send buffer must map back to the moving range under `routeKey()`. The migrator asserts this on every row before the gRPC send; an assertion failure aborts the job and surfaces in `last_error`. The assertion guards against a future internal-key family being added that someone forgot to teach `routeKey()`.

## 7. Coordinator / FSM Integration

### 7.1 FENCE write rejection

Coordinator-side:

- `kv/sharded_coordinator.go` consults the engine's per-route state. Routes in `WriteFenced` reject writes with a new sentinel `ErrRouteWriteFenced`, mapped to:
  - gRPC: `codes.Aborted` + status detail `{kind:"route_write_fenced", route_id, retry_after_ms}`.
  - Redis adapter: `MOVED`-style retry (existing path).
  - DynamoDB adapter: throttled error so the SDK retries with backoff.

FSM-side defense in depth:

- `kv/fsm.go` already routes through `verifyComposed1` at apply. Extend `verifyOwnerFromSnapshot` to also reject when the resolved route's state is `WriteFenced` — even if a coordinator with a stale engine forwarded a write, the FSM closes the door. The error wraps `ErrRouteWriteFenced` so M4 retry logic differentiates from `ErrComposed1Violation`.

Reads during FENCE / DELTA_COPY are **not** rejected: `ShardStore.GetAt` continues to serve from source. Snapshot reads at any `commit_ts ≤ fence_ts` remain consistent because the source's MVCC history is unchanged through DELTA_COPY. The post-CUTOVER read-side fence is §7.2.

### 7.2 Post-CUTOVER read fence (closes codex P1)

Codex P1 on PR #945: today the read path resolves the group from the local engine and serves/proxies the read **without carrying an observed catalog version** (`kv/shard_store.go:38-53`, `kv/shard_store.go:1471-1477`), while `verifyComposed1`'s `verifyOwnerFromSnapshot` (`kv/fsm.go:753-773`) only inspects mutations. After CUTOVER, a coordinator or source-group leader that has not applied the new catalog yet would keep routing reads of the moved key to the source — returning the stale pre-cutover value while writes have already landed on the target. M2 must close this read path symmetrically to writes.

#### 7.2.1 Read request carries an observed catalog version

Add `read_route_version uint64` to the read-path message (`pb.ReadRequest`, including the lease-read envelope used by `ShardStore.GetAt` / `.ScanAt`). The coordinator stamps the engine's catalog version into every read at the moment it picks a group. This is the read-side equivalent of `ObservedRouteVersion` on writes.

#### 7.2.2 Source FSM rejects reads after CUTOVER

When a source-group FSM applies a lease read whose `read_route_version` is **strictly less than** the FSM's last-applied catalog version, and the requested key's `routeKey()` maps to a route the FSM no longer owns, the FSM returns `ErrRouteMoved{new_route_version, new_group_id}` instead of the value. The error is mapped to a retryable status (gRPC `codes.FailedPrecondition` + detail) and the coordinator retries after a `RouteEngine` refresh.

Two sub-cases:

- **Coordinator stale**: it routed to source assuming the pre-cutover catalog. Refresh → re-route to target → succeeds.
- **Source FSM stale (read_route_version ≥ FSM version)**: the FSM hasn't applied CUTOVER yet, but the coordinator (and quorum) already have. The FSM treats `read_route_version > local` as a "wait until applied" (bounded by lease TTL) before answering, then re-evaluates ownership; if still not owned, returns `ErrRouteMoved`. This avoids a wedge while keeping the read consistent.

#### 7.2.3 Coordinator-side fallback

If the engine's local catalog is behind the FSM's `new_route_version`, the coordinator does a single best-effort `WatcherRefresh` and retries once. Repeated `ErrRouteMoved` surfaces to the client only on persistent inconsistency (alert-worthy).

#### 7.2.4 Grace window before CLEANUP

CLEANUP deletes the moved range from source's MVCC after a configurable grace window (`readFenceGrace`, default 30 s — chosen to comfortably exceed both lease TTL and watcher tick × 2). During the window:

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

### 11.1 Rolling-upgrade compatibility

Closes gemini medium on PR #945: an M1 node that has not yet been upgraded does not understand `RouteStateWriteFenced` enforcement on writes, the read fence in §7.2, or the import RPCs. Starting a cross-group split while M1 nodes are still serving is a silent-write-loss hazard.

Layered mitigations:

1. **Capability advertisement.** Each node advertises a `node_capability_bitfield` in its periodic heartbeat to the default group (existing distribution heartbeat — extend by one field). M2-capable nodes set the `cap_migration_v2` bit at boot.
2. **Cluster-readiness gate at the entry RPC.** `SplitRangeRequest` with `target_group_id != source_group_id` is rejected at `adapter/distribution_server.go` with `ErrClusterNotReadyForMigration` unless **every active node** advertises `cap_migration_v2`. Same-group split (M1) remains unconditionally available — its semantics did not change.
3. **In-flight job invalidation on downgrade.** If the default-group leader observes any node losing `cap_migration_v2` mid-migration (rare — would only happen with a botched rollback), the migrator pauses (stays in current phase, no new state advancement) and surfaces `ErrClusterCapabilityRegressed`. Operator must either roll forward or `AbandonSplitJob`.
4. **M2-PR1..PR5 land in this order so the capability bit can be set safely**: PR1-PR3 are wire/codec/store additions with **no behaviour change for M1** (no SplitJob created without explicit `target_group_id != source_group_id`); PR4 introduces the state machine but, until PR5 (FENCE rejection) lands, the bit must NOT be advertised. The `cap_migration_v2` bit goes live in PR5 commit so by the time it is observable, all required enforcement is present.
5. **Test plan**: `distribution/capability_test.go` exercises (a) gate accepts M2-only cluster, (b) gate rejects mixed cluster, (c) gate rejects on heartbeat staleness (no advertisement within 2× heartbeat interval treated as no capability).

This is independent of the existing rolling-upgrade protocol for unrelated subsystems and adds zero overhead for clusters never using cross-group split.

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
