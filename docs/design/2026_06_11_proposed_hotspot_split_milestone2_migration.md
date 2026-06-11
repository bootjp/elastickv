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
| `cursor` | bytes | Opaque (raw_key, commit_ts) export position for BACKFILL / DELTA_COPY resume — addresses an MVCC version, not a raw key, so a hot key with many committed versions can be chunked safely across batches (see §6.1.1). |
| `max_imported_ts` | uint64 | Monotone HLC ceiling: the largest `commit_ts` ever included in an ack'd import batch for this job. Used by §6.2.1 to advance the target group's HLC at CUTOVER. |
| `promote_cursor` | bytes | Opaque cursor for the §6.4 incremental staged-to-live promoter. Same encoding as §6.1.1; advances during background promotion after CUTOVER. |
| `promotion_completed_ts` | uint64 | HLC commit-ts of the apply that cleared `staged_visibility_active` and ended CLEANUP. Diagnostic / audit only. |
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
        |  FENCE → DELTA_COPY              Migrator waits for source-side
        |                                  fence ACK (§3.2a) — every source-
        |                                  group voter has applied the
        |                                  WriteFenced route — before picking
        |                                  fence_ts. THEN copies
        |                                  (snapshot_ts, fence_ts] from
        |                                  source to target shadow space.
        v
[ source.Active(left)           ]
[ source.WriteFenced(right)     ]
        |
        |  DELTA_COPY → CUTOVER            Migrator first proposes a final
        v                                  SetPhysicalCeiling(max_imported_ts)
[ source.Active(left)           ]          on the TARGET group so the target
[ source.WriteFenced(right)     ]          HLC ≥ every imported commit_ts
                                           (§6.2.1, monotone primitive).
                                           Then atomic catalog write under
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
        v                                  the moved key range from its
[ source.Active(left)           ]          MVCC store after the read-fence
[ target.Active(right)          ]          grace window (§7.2).
        |
        |  CLEANUP → DONE                  Job moved to !dist|jobhist|<...>.
        v
[ source.Active(left)           ]
[ target.Active(right)          ]
```

Key invariant: **catalog snapshots never contain overlapping ranges**. The target shadow keyspace lives outside the catalog (BACKFILL / DELTA_COPY land into private `!dist|migstage|<job_id>|<raw_key>` keys on the target group's MVCC store; CUTOVER **does not** bulk-rename them — promotion is incremental and runs as a target-side background job after CUTOVER, see §6.2.2). This keeps `routesFromCatalog` + `validateRouteOrder` (`distribution/engine.go:496-527`) green throughout the migration — addresses codex P1 on overlapping routes (PR #945 review). RouteState `MigratingSource` / `MigratingTarget` are reserved but unused in M2; they remain available for a future merge / multi-stage migration design.

The Composed-1 gate rejects an apply on the wrong group at the observed catalog version; CUTOVER bumps `catalog_version` so any straggler write from an unaware coordinator fails closed at apply time on either side.

### 3.2a Source-side fence-apply barrier — fence_ts must follow the source's actual write-reject point (closes codex round-6 P1 line 115)

Codex round-6 P1 on PR #945 (line 115): the FENCE catalog write commits on the default group, but the source FSM only learns about the new `WriteFenced` state through the watcher's asynchronous catalog snapshot apply (`distribution/watcher.go:11`, `:87-98`). If the migrator records `fence_ts = source.LastCommitTS()` and starts DELTA_COPY immediately after the FENCE catalog write, the source leader may still accept writes for the moved keys for one watcher-tick worth of time after the catalog commit — those writes get a `commit_ts > fence_ts`, fall outside DELTA_COPY's `(snapshot_ts, fence_ts]` window, and are silently left on the source at CUTOVER. Result: write loss after CUTOVER hands the routing to the target.

M2 fixes this with a two-step gate on the FENCE → DELTA_COPY transition:

1. **Fence-apply ACK from every source-group voter.** After the FENCE catalog write commits, the migrator does **not** immediately pick `fence_ts`. Instead it polls a per-node `GetCatalogVersion` RPC (or piggybacks on the existing distribution heartbeat — same surface as §11.1's capability bit) against every voter of the source group's Raft membership (`engine.Configuration(ctx)` is already read by the migrator for §3.4 routing). The migrator advances the phase only when **every voter reports `catalog_version_applied >= fence_catalog_version`**, where `fence_catalog_version` is the catalog version of the FENCE write recorded in the SplitJob. This guarantees every node that could be the source-group leader at any future moment has the `WriteFenced` route locally applied, so any write that reaches its FSM apply path after this point is rejected by the FENCE pre-gate (§7.1's `verifyRouteNotFenced`).
2. **`fence_ts` pinned after the ACK.** After step 1 returns, the migrator queries the source group leader for `LastCommitTS()` and pins that as `fence_ts`. Because the WriteFenced route is now applied on every voter, no in-flight write with `commit_ts > fence_ts` can land for the moved range — the apply path rejects them with `ErrRouteWriteFenced` at the FSM gate. DELTA_COPY's `(snapshot_ts, fence_ts]` window therefore captures every committed write whose `commit_ts > snapshot_ts`, completing the BACKFILL+DELTA_COPY semantic.

Timing budget: the migrator's heartbeat-driven loop already runs at one tick per `hlcRenewalInterval` (1s), and `cap_migration_v2` heartbeats carry `catalog_version_applied` already (§11.1 reuses the same field). The fence-apply ACK therefore typically completes in one tick. A non-responsive voter blocks the phase — the migrator logs `last_error = "fence ack pending: nodes [<N1>, <N2>]"` and either (a) the operator intervenes, or (b) Raft removes the unresponsive voter via the existing leader-loss path. **Importantly the migrator does NOT advance phase on a quorum ACK** — every voter must ACK, because any voter could later become the leader and would re-serve writes for the moved range with the pre-FENCE state if its catalog hadn't applied. Quorum is necessary for catalog-write durability; **unanimous voter-ACK is necessary for fence safety**.

Crash safety: the fence-ack-pending state is durable in the SplitJob (`fence_ack_cursor` tracks which voters have ACK'd). A migrator restart resumes from the pending voter list. No data is at risk during this wait — the catalog FENCE state is already durable, so the watcher will catch up on its own; the migrator just doesn't commit DELTA_COPY's `fence_ts` until the wait completes.

CUTOVER is a similar story but is structurally guaranteed by a different mechanism: the catalog write itself bumps `catalog_version`, and the §7.2 read fence's authoritative ownership check guarantees that even a coordinator/source still at the pre-CUTOVER catalog cannot return a stale value. There is no parallel "wait for every voter to apply CUTOVER" gate — readers are the consumers, and §7.2 handles them directly. The fence-apply ACK above is specifically about **writes during DELTA_COPY**, where the data move happens.

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
| `CLEANUP` | After grace period (`readFenceGrace`, default **30 s** per §7.2.4 — comfortably exceeds lease TTL and watcher tick × 2; matches §12 OQ-3 resolution). GC is idempotent (delete-if-version-≤-fence_ts). |
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
// The cursor is opaque (see §6.1.1) — it addresses an MVCC version
// (raw key + commit_ts), not just a raw key, so a single hot key's
// history is allowed to span batches without losing or duplicating
// versions.
// `accept` (nil = no filtering) is the §6.3 decoupling seam.
ExportVersions(ctx context.Context, start, end []byte, minCommitTS, maxCommitTS uint64,
    cursor []byte, chunkBytes int, accept KeyFilter) ([]MVCCVersion, []byte, bool, error)
```

The function uses the existing snapshot read primitive (the same one Composed-1 already trusts for visibility), so MVCC semantics during export are guaranteed to match a reader at `maxCommitTS`. `accept` is consulted before a row is added to the chunk; rejected rows do not advance the chunk-bytes budget so the caller sees deterministic per-call shapes.

#### 6.1.1 Cursor granularity — addresses an MVCC version, not a raw key (closes codex P2)

Codex P2 on PR #945: a "next-key" cursor breaks for hot keys with version histories larger than `chunkBytes` — the migrator would either have to send the whole version chain in one unbounded chunk (blowing the memory bound) or skip/duplicate the tail after a restart. The cursor must therefore address an **MVCC position**, not a raw key.

Concretely, the cursor is opaque on the wire but encodes the tuple `(raw_key, commit_ts)` of the **last emitted MVCC version** — the one that just went out in this chunk's tail (closes codex round-7 P1 line 351 on PR #945). Resume is **exclusive on the last-emitted version**: the next call iterates strictly past `(raw_key, commit_ts)` under the export's `(raw_key ASC, commit_ts DESC)` order (newest-first within a key, matching the MVCC iterator already used by snapshot reads in `store/mvcc_store.go`), so the version the cursor names is **not** re-emitted (avoids duplicates) and the very next version in iteration order **is** emitted (no skip). The earlier wording — "encoded as the exclusive `(raw_key, commit_ts)` of the *next-to-emit* row" — was internally inconsistent: "next-to-emit" + "exclusive resume" reads as "skip the version the cursor names," which would silently drop one MVCC version per chunk boundary for a hot key whose history spans chunks. The "last emitted" + "exclusive resume" form is the only one that preserves both no-duplicate and no-skip simultaneously. Three properties follow:

- **Hot-key safe.** A chunk may contain only versions of a single raw key, and the next chunk picks up the next-older version of the same key without any all-or-nothing constraint on the key's history.
- **Resume-safe across restarts.** Restarting from a persisted opaque cursor lands on the exact same MVCC version as a continuous run — no skipped versions (the cursor is exclusive on the resume side) and no duplicates (the importer's idempotency check, §6.2, dedups any one resume that overlaps an ack window).
- **Importer doesn't care about key boundaries.** Versions arrive in `(raw_key, commit_ts DESC)` order; the importer just applies them. No "this batch is the rest of key K" handshake is needed.

The opaque encoding is `varint(len(raw_key)) || raw_key || uvarint(commit_ts)`; an empty cursor signals "start from the beginning." Future evolution can add fields after `commit_ts` (e.g. a tombstone marker) because the consumer never parses the cursor by hand — only the exporter reads its own encoding.

The store-side type stays `[]byte` and the exporter alone owns the codec, preserving the §6.1 decoupling seam: the migrator hands the cursor back as opaque bytes on every `ExportVersions` call.

### 6.2 Target side

New method:

```go
// ImportVersions writes the given versions idempotently. A second call
// with the same (job_id, cursor) MUST be a no-op past the recorded
// cursor — the importer records last-applied cursor under
// !migstage|cursor|<id> so a network retry doesn't double-write.
// As a side effect, the import atomically advances metaLastCommitTS
// AND the node-local HLC ceiling to at least max(commit_ts) of the
// batch (§6.2.1).
ImportVersions(ctx context.Context, jobID uint64, versions []MVCCVersion,
    cursor []byte) ([]byte, error)
```

Idempotency: persist `(job_id, cursor)` after each batch's apply, and on a duplicate request check the persisted cursor.

#### 6.2.1 Target HLC advancement — preventing post-CUTOVER time travel (closes codex P1)

Codex P1 on PR #945: when the source range contains commit timestamps higher than the target group's current `LastCommitTS` / HLC, an import that only writes versions + cursor leaves the target's clock below the staged data. After CUTOVER the target's first new write at `Next() = max(wall, ceiling)` can therefore receive a `commit_ts` **smaller** than imported values — MVCC visibility then resurrects the pre-cutover value on a snapshot read at the new commit_ts, an unambiguous time-travel hazard.

The fix runs inside `ImportVersions` (and the equivalent CUTOVER bulk-rename apply that promotes the staged keyspace into live keys):

1. **Per-batch advance.** On every apply of an `ImportVersions` batch, the target FSM computes `batchMax = max(versions[i].commit_ts)` and **atomically** (under the same FSM apply lock that mutates the MVCC store):
   - sets `metaLastCommitTS = max(metaLastCommitTS, batchMax)`,
   - calls `hlc.SetPhysicalCeiling(hlcPhysicalMs(batchMax))` — the **physical-ms component only**, not the full HLC `commit_ts` (closes codex round-5 P1 on PR #945 line 367). `SetPhysicalCeiling`'s signature is `int64 ms` (`kv/hlc.go:222`) and is interpreted as Unix milliseconds; passing the full encoded ts `commit_ts = ms<<16 | logical` would advance the ceiling to ~`commit_ts<<16` worth of milliseconds — `2^16 ≈ 65 536` × further in the future than intended — and weaken the lease-ceiling fence until wall time catches up. The conversion is `hlcPhysicalMs(ts) = int64(ts >> 16)` (the same shift `HLC.Next()` uses to extract the physical half, `kv/hlc.go:128-145`), AND
   - calls `hlc.Observe(batchMax)` so the local clock's last-issued value also tracks the high-water mark — keeps `Next()`'s logical-half advancement consistent with what `metaLastCommitTS` now says.

   `SetPhysicalCeiling` is monotone — a lower argument is a no-op — so duplicate / out-of-order batches are safe. `Observe` is similarly monotone.
2. **Job-level monotone witness.** The SplitJob carries `max_imported_ts` (§3.1) — the high-water mark of all ack'd import batches for this job. The migrator updates it whenever it persists an import_cursor, so it survives leader flap.
3. **CUTOVER precondition.** Before the migrator issues the CUTOVER catalog write, it ensures the target group's HLC ceiling is at least `hlcPhysicalMs(max_imported_ts)` by issuing a final `SetPhysicalCeiling(hlcPhysicalMs(max_imported_ts))` + `Observe(max_imported_ts)` proposal on the **target group** (not the default group). The proposal is gated by `cap_migration_v2` (§11.1) and is the **last** target-side write before CUTOVER. If the target HLC is already above this value (e.g. due to per-batch advancement), the `SetPhysicalCeiling` call is the trivial no-op the ceiling already enforces; if a follower flap dropped one of the per-batch advances, this proposal closes the gap.
4. **Post-CUTOVER write-side invariant.** A target-group `Next()` after CUTOVER is bounded below by `metaLastCommitTS` AND by the HLC ceiling (the existing `HLC.Next()` semantics in `kv/hlc.go`), so it is provably strictly greater than every `commit_ts` ever imported under this job. Reads at any post-CUTOVER `Next()` therefore see the imported version as historical (older than the read ts) and see the new write as the most-recent committed version — no resurrection, no time travel.
5. **Same-group split (M1) unaffected.** Same-group split never crosses the FSM apply boundary, so its `Next()` is already bounded by the source's HLC; the new advance path is a no-op when `target_group_id == source_group_id`.

This is the same Raft-agreed monotone-ceiling primitive that PR #927 / Composed-1 already relies on; M2 reuses it instead of inventing a new fence. Test coverage: §10.1 unit gains an explicit `kv/migrator_hlc_advance_test.go` that asserts (a) `metaLastCommitTS` and HLC ceiling are advanced by `max(batch.commit_ts)`, (b) the CUTOVER pre-write proposal closes any gap left by a missing batch, (c) post-CUTOVER first write's `commit_ts > max_imported_ts`. Property test extends the §10.1 export-chunks/import-acks/leader-flap sequence with a `Next()` invocation after CUTOVER and asserts strict monotonicity vs. every imported ts.

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

#### 6.3.1 Internal-family brackets — raw range alone misses internal state (closes codex P1 line 310)

The above closure correctly *rejects* a raw key that does not belong to the moving range, but it cannot *find* internal-family keys that the caller never iterates over in the first place. Codex P1 round-4 on PR #945 (line 310): for a moving user-key interval `[foo, bar)`, an iteration over the raw range `[foo, bar)` never visits `!txn|lock|foo`, `!lst|meta|foo`, `!redis|str|foo`, `!ddb|item|<tbl>|foo`, `!sqs|...`, `!s3|...`, because each of those prefixes sorts **outside** `[foo, bar)` in the raw MVCC key space — the routeKey filter rejects nothing because those bytes were never iterated. Without the fix below, CUTOVER would leave intent locks, list metadata, and adapter-private state behind on the source and the target would serve reads against a half-populated MVCC space — silent data loss.

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
    Start, End []byte  // raw key bounds for ExportVersions
    Family     uint32  // MVCCVersion.key_family tag
}

func PlanExportBrackets(routeStart, routeEnd []byte) []ExportBracket {
    return []ExportBracket{
        // User-key bracket itself, in the routing-key namespace.
        {Start: routeStart, End: routeEnd, Family: familyUser},
        // Each internal-family raw-key band. routeKey() decodes the
        // family-prefixed raw key back to its routing key, so the
        // filter rejects any entry whose routing key falls outside
        // [routeStart, routeEnd).
        {Start: txnLockPrefix(),  End: txnLockPrefixEnd(),  Family: familyTxnLock},
        {Start: txnCmtPrefix(),   End: txnCmtPrefixEnd(),   Family: familyTxnCommit},
        {Start: txnRbPrefix(),    End: txnRbPrefixEnd(),    Family: familyTxnRollback},
        {Start: txnMetaPrefix(),  End: txnMetaPrefixEnd(),  Family: familyTxnMeta},
        {Start: txnIntPrefix(),   End: txnIntPrefixEnd(),   Family: familyTxnInternal},
        {Start: lstMetaPrefix(),  End: lstMetaPrefixEnd(),  Family: familyListMeta},
        {Start: lstItmPrefix(),   End: lstItmPrefixEnd(),   Family: familyListItem},
        {Start: redisPrefix(),    End: redisPrefixEnd(),    Family: familyRedis},
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

`routeKey()` already decodes every internal-family prefix back to the same routing key that the user-key route resolution returns (parent partial doc §9; implementation in `kv/shard_key.go`); the `KeyFilter` therefore correctly accepts an internal-family raw key whose routing key falls inside the moving range and rejects others. So adding the bracket list is the only new machinery needed — the filter stays unchanged.

The migrator runs the brackets **in parallel up to `--migrationExportFanout` (default 4)** within a single phase (BACKFILL or DELTA_COPY): each bracket gets its own opaque cursor (the §6.1.1 codec is extended to key on `(family, raw_key, commit_ts)`), so a bracket can be paused/resumed independently and the §9 resumability matrix applies per bracket. The migrator records per-bracket `cursor` and `done` flags on the SplitJob; the phase advances only when **every bracket** reports `done = true`. Cost is at most `fanout`× the gRPC frame budget but is bounded by the total bracket count (~12 today), not by data volume.

**Bounded scan for sparse / out-of-range brackets (closes codex P2 round-5 line 445).** "Returns its first chunk empty and `done=true` immediately" is only true when the iterator can short-circuit; under the §6.1 contract, `accept` rejections do **not** count against `chunkBytes` (so the caller sees deterministic chunk shapes), which would otherwise let a family-wide raw range scan an arbitrarily large prefix to prove `done=true` for a moving range with no intersection. The fix has two layers:

- **Scan-budget pacing.** Each `ExportVersions` call carries a separate `maxScannedBytes` (default 4 × `chunkBytes`) that **counts both accepted and rejected rows** as iteration cost. When the iterator hits that budget without filling the accepted chunk, it returns whatever it has accepted so far + the next cursor (over the *rejected* tail position, so the next call resumes past the already-rejected window). `done = false`, so the next chunk picks up where the rejection scan left off. Without this, a Redis migration where the SQS family has a cluster-wide 10 GB raw prefix would block on a single `ExportVersions` call for the entire prefix duration.
- **Route-key sub-prefix indexing where the family layout allows it.** Each internal-family bracket's raw key starts with a known fixed prefix (e.g. `!txn|lock|`, `!lst|meta|`) followed by the routing-key bytes (mostly — adapters like SQS encode queue ID first; per-family `EncodeBracketStart(routeStart)` is the per-family hook). When such a hook is available, the bracket's `Start` / `End` are tightened to `(familyPrefix || EncodeBracketStart(routeStart), familyPrefix || EncodeBracketStart(routeEnd))` instead of the family-wide bounds, reducing the scan budget consumption from "full family" to "moving slice within family." Families without an encoder hook (initially `!s3|...`, until the design records its routing-key encoding) keep the family-wide bracket + the scan-budget pacing above; the scan-budget alone is sufficient for correctness, the encoder hook is a per-family optimization that lands in the matching adapter's PR.

These two layers together preserve the §9 resumability matrix per bracket (cursor still advances through both accepted and rejected rows; a leader flap resumes at the persisted scan cursor) and bound the per-chunk wall-clock cost regardless of how sparse the bracket happens to be. The `keys per bracket` distribution AND `rejected_rows per bracket` are §7.3 / §11.x metrics so operators can spot a runaway family scan and prioritize adding an encoder hook for that adapter.

A simple safety check: every exported key in the migrator's send buffer must map back to the moving range under `routeKey()`. The migrator asserts this on every row before the gRPC send; an assertion failure aborts the job and surfaces in `last_error`. The assertion now guards two failure modes: (i) a future internal-family being added that someone forgot to teach `routeKey()`, and (ii) a bracket being added without a matching `routeKey()` decoder. Reuses the same routeKey assertion §6.3 already specifies — no new code surface beyond the bracket list.

### 6.4 Incremental staged-to-live promotion — CUTOVER is constant-time (closes codex P2 on PR #945)

Codex P2 round-3 on PR #945 (line 147): "staging every imported row under `!dist|migstage|...` and then promoting the whole range via a single FSM apply makes CUTOVER proportional to the entire migrated range. For any split larger than the Raft proposal/apply budget, this defeats the chunked BACKFILL/DELTA_COPY design and can block or fail exactly when the catalog must switch atomically." The proposal must therefore avoid a per-key bulk move at cutover. M2 ships an incremental promotion path that keeps CUTOVER a constant-time catalog write while preserving the visibility fence:

1. **CUTOVER itself is one catalog write — no per-key work.** The CUTOVER FSM apply on the default group does **only**:
   - (a) CAS-bump the catalog version,
   - (b) remove the source's `WriteFenced(right)` route,
   - (c) insert the target's `Active(right)` route with `raft_group_id = target`,
   - (d) populate `cutover_version` on the SplitJob and stamp `staged_visibility_active = true` on the new target route.

   No iteration over the migrated key range, no bulk rename, no per-key proposals. The Raft proposal carrying this apply is `O(1)` in the migrated data size — bounded by a handful of catalog descriptors and the SplitJob record.

2. **Staged keyspace participates in MVCC merge — newest commit_ts wins (closes codex P1 line 408).** After CUTOVER the target FSM's read path treats the staged area as **a second source of MVCC versions for the same logical key**, not as an authoritative override. For a read for key `K` at `read_ts` in the moved range with `staged_visibility_active = true`:
   - read the **newest live MVCC version of `K` with `commit_ts ≤ read_ts`** (the standard snapshot read against `!ddb|...K`, `!redis|...K`, the user-key band, etc. — whichever family `K` belongs to);
   - read the **newest staged MVCC version of `K` with `commit_ts ≤ read_ts`** under `!dist|migstage|<job_id>|<K>` (the §6.1.1 staged area, iterated by the same `(raw_key ASC, commit_ts DESC)` order the snapshot iterator already uses);
   - return whichever has the **greater `commit_ts`** (newest wins); if both are absent return "not found".

   This is the standard MVCC visibility rule applied to a second column-family-like source, not a "stage-shadows-live" fallback. The earlier wording — "first look at staged, fall back to live when staged is absent" — was wrong precisely because §6.2.1's HLC ceiling advance guarantees a live write after CUTOVER has `commit_ts > max_imported_ts`, so for any `read_ts ≥ live_write.commit_ts` the live version is the most-recent committed value and a `staged-first-then-live-fallback` would return the older staged value while the staged entry still exists — exactly the visibility regression codex P1 line 408 flagged. The merge form is correct because:
   - For `read_ts < live_write.commit_ts`: live has no committed version ≤ read_ts (or only an older one), staged's imported `commit_ts ≤ max_imported_ts < live_write.commit_ts`, so the merge correctly surfaces whichever is newest at `read_ts` (typically the staged one for `read_ts ∈ (max_imported_ts, live_write.commit_ts)`).
   - For `read_ts ≥ live_write.commit_ts`: the live version's `commit_ts > max_imported_ts ≥ every staged version's commit_ts`, so the merge correctly returns the live value.
   - For never-overwritten keys post-CUTOVER: only the staged version exists ≤ read_ts, the merge returns it.
   - For a key with multiple staged versions (the §6.1.1 hot-key case): the staged iterator returns the newest staged version ≤ read_ts; the live iterator returns the newest live version ≤ read_ts; the merge picks the greater of the two. Correct in every interleaving.

   Writes always land in the live keyspace — the CUTOVER bump already routed writers to the target group via the catalog, and §6.2.1's HLC ceiling advance guarantees their `commit_ts` is strictly greater than every imported `commit_ts`, so a live write shadowing a staged version is the *correct* MVCC visibility AND the merge rule above surfaces it correctly. This gives reads access to the imported data the instant CUTOVER lands, with **no promotion work blocking CUTOVER itself**, and with no staleness window where staged shadows a fresher live write.

   Per-family adapter helpers (Redis list head pointers, DynamoDB GSI shadow rows, etc.) follow the same merge rule on the corresponding internal-family keys — the merge operates on raw keys, so the per-family invariants the `key_family` dispatch (§6.3.1) already enforces still apply.

3. **Background incremental promoter on the target group.** Immediately after CUTOVER the target group's FSM starts a **leader-local promoter goroutine** that walks the `!dist|migstage|<job_id>|*` prefix in cursor-resumable chunks (the same `chunkBytes` / pacing knobs §6.1 and the migrator already use), and for each staged version proposes one `PromoteStaged` Raft entry that:
   - copies the staged key into its live position with the original imported `commit_ts` — **always**, regardless of whether a newer live version at a higher `commit_ts` exists. The MVCC store naturally holds both `K@commit_ts=3` (the staged version being promoted) and `K@commit_ts=10` (a later live write) at different commit timestamps; a snapshot read at any `read_ts ∈ [3, 10)` correctly sees the imported `K@3`, and a read at `read_ts ≥ 10` correctly sees `K@10`. The idempotency guard for this copy is "**no-op if a live version at the same `commit_ts` already exists**" — that means this exact staged version was already promoted in a prior batch before a leader flap re-tried it. It is **not** "no-op when a newer live version exists," which would silently drop the imported version from MVCC history and make snapshot reads at older `read_ts` return wrong results (closes claude round-3 P1, 5-round flag). History pruning belongs to `kv/compactor.go`, not the promoter,
   - deletes the staged key,
   - advances the per-job `promote_cursor` on the SplitJob.
   Each batch is a small bounded proposal, exactly like BACKFILL — there is no single oversized apply.

4. **Promotion is restart-safe.** The promoter is leader-only; a leader flap restarts from `promote_cursor`. Concurrent reads keep using the staged-fallback path (step 2) for whatever has not yet been promoted, so the user-visible state is correct throughout.

5. **CLEANUP → DONE precondition.** The SplitJob does not advance from CLEANUP to DONE until **both** (a) the read-fence grace window (§7.2.4) has elapsed AND (b) the promoter has reported "no staged keys remain" (the prefix scan finds nothing) AND (c) one final apply clears `staged_visibility_active` on the target route. Once cleared, reads no longer pay the staged-fallback cost; the route is indistinguishable from any other Active route. The migrator records the final apply's commit_ts as `promotion_completed_ts` on the SplitJob for audit.

6. **Edge cases.**
   - **Hot key with many versions** (codex P2 on cursor granularity, §6.1.1): the promoter iterates the staged prefix in `(raw_key ASC, commit_ts DESC)` order using the same opaque-cursor codec; the staged-fallback read path treats a key with multiple staged versions identically to the MVCC iterator. No version chain is materialised in one apply.
   - **Concurrent writes during promotion**: a live write at `commit_ts > max_imported_ts` lands ahead of the staged version it shadows. The MVCC store holds **both** `K@commit_ts=staged_ts` (the promoted staged version) **and** `K@commit_ts=live_ts` (the live write) at different commit timestamps — there is no overwrite, only **version coexistence** (closes claude round-8 wording nit). `metaLastCommitTS` advances to track the Raft commit sequence on the live side but does **not** block `PromoteStaged`'s historical insert; phrasing this as "keeps the staged copy from overwriting" was misleading because it could be misread as a conditional gate that skips the staged copy entirely — the exact §6.4-step-3 bug the round-3 fix removed. The §6.4-step-2 merge read at any `read_ts ≥ live_write.commit_ts` returns the live value (newest wins); at any `read_ts ∈ (max_imported_ts, live_write.commit_ts)` returns the imported value (live has no version `≤ read_ts` at that point; staged has one). Both correct.
   - **AbandonSplitJob after CUTOVER** is still rejected (§4); abandoning post-CUTOVER would leave half-promoted state visible.

7. **Per-PR landing.** M2-PR6 ships the staged-fallback read path + `PromoteStaged` apply (mechanics); M2-PR7 ships the background promoter + CLEANUP precondition. PR4-PR5 land the catalog flag and the SplitJob fields needed by PR6/PR7 so the wire is in place before the runtime turns on. The incremental design is therefore a strict superset of the chunked BACKFILL/DELTA_COPY semantics — the same chunk sizing, the same cursor codec, the same leader-flap recovery — applied to the promotion step instead of one bulk apply at CUTOVER.

The result: CUTOVER is a single bounded catalog write, never `O(migrated range)`, and the visibility fence is preserved by the per-route flag plus the staged-fallback. The migrator still gets resumable, bounded apply costs for the actual data move; the catalog switch itself is decoupled from the data volume. This closes codex P2 line 147 on PR #945 without weakening any consistency property already established by §6.2.1 (HLC ceiling) or §7.2 (read fence).

## 7. Coordinator / FSM Integration

### 7.1 FENCE write rejection

Coordinator-side:

- `kv/sharded_coordinator.go` consults the engine's per-route state. Routes in `WriteFenced` reject writes with a new sentinel `ErrRouteWriteFenced`, mapped to:
  - gRPC: `codes.Aborted` + status detail `{kind:"route_write_fenced", route_id, retry_after_ms}`.
  - Redis adapter: `MOVED`-style retry (existing path).
  - DynamoDB adapter: throttled error so the SDK retries with backoff.

FSM-side defense in depth — **must run on an unconditional apply path** (closes codex round-3 P1 on PR #945):

- The naïve placement — extend `verifyOwnerFromSnapshot` so it also rejects routes in `WriteFenced` — does NOT work. `verifyComposed1` short-circuits when `ObservedRouteVersion == 0` (`kv/fsm.go:608-611`), and there are several **intentional zero-version write paths today**: read/write txns with `ReadKeys`, caller-supplied `StartTS`, and resolver-claimed keys all dispatch with `ObservedRouteVersion = 0` (`kv/sharded_coordinator.go:694-754`, M3 sibling §3.5 + auto-pin policy). A stale coordinator in any of those flows would forward a write to the source during FENCE and the FSM defense would never run. The fence must be enforced **before** any version-conditional early return.
- M2 therefore introduces a **separate, unconditional pre-gate** `verifyRouteNotFenced(mutations)` in `kv/fsm.go` that the apply path consults **before** `verifyComposed1`. It scans the mutations exactly as `verifyOwnerFromSnapshot` does — `routeKey(mut.Key)` against the **current** catalog snapshot — and rejects with `ErrRouteWriteFenced` when **any** key resolves to a route whose `RouteState == WriteFenced`. No dependency on `ObservedRouteVersion`, no early return on `0`; the gate runs on every applied write request unconditionally (it has the same `Phase_ABORT` bypass as the Composed-1 gate, for the same lock-release reason in §3.5 of the parent partial doc).
- Concretely, the apply path becomes: `verifyRouteNotFenced` → `verifyComposed1` → existing handlers. `verifyRouteNotFenced` reads the current `RouteSnapshot` once (the same one `verifyComposed1` would have read at the current-version check) and looks up each mutation key's route state. Cost is bounded by the existing per-mutation `OwnerOf` call already done at apply time when ObservedRouteVersion is non-zero; the new path adds it for the zero-version case too. The two gates stay independent so a future change to one cannot regress the other (same isolation rationale as `verifyReadOwner` next to `verifyOwnerFromSnapshot` in §7.2.5).
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
  2. **Cutover-aware ownership query.** When the watermark indicates a possible stale read, the source-group leader makes a single best-effort, deadline-bounded `GetRouteOwnership(routeKey(K), catalog_version=max_cutover_version_seen)` RPC to the default-group leader. The default-group leader answers from its current catalog snapshot. If the answer is "owned by another group" the source returns `ErrRouteMoved{new_route_version=max_cutover_version_seen, new_group_id}` to the coordinator; if owned by the source (no cutover landed on this key after all — the watermark covers an *unrelated* migration) the source returns the value normally.
- The RPC is cached on the source-group leader per `(routeKey, max_cutover_version_seen)` for one second to keep the fence cheap; the cache invalidates the moment `max_cutover_version_seen` advances.

Crucially, this catches **the equal-stale window** the strict-less-than form missed: both coordinator and source are at the pre-CUTOVER version, but the heartbeat watermark on the source is already at the post-CUTOVER version (because the default-group leader propagated it as soon as the catalog write committed), so the source consults the default-group leader and gets the authoritative "no longer yours" verdict before serving a stale value.

Two sub-cases of the legacy framing are preserved:

- **Coordinator stale, source up-to-date**: source's `source_local_version ≥ cutover_version` and source no longer owns the key → unconditional `ErrRouteMoved`. Coordinator refreshes → re-routes to target → succeeds.
- **Source stale, coordinator further ahead (`read_route_version > source_local_version`)**: the FSM blocks on a **condition variable signalled by the apply goroutine** when it advances `appliedIndex`, with a timeout of `min(remaining_lease_TTL, 200 ms)` (the FSM tracks `appliedIndex` natively; the condition variable wraps it with a one-shot notifier consumed by the read-fence path). On the signal the FSM re-evaluates ownership; if `source_local_version` has now caught up to `read_route_version` AND the source still owns the key under the new snapshot, the read proceeds. If `source_local_version` is still behind on timeout, OR if the catch-up reveals the source no longer owns the key, the FSM returns `ErrRouteMoved`. This avoids a wedge while keeping the read consistent (closes claude round-2 spec-mechanism flag, 4-round flag).

#### 7.2.2a Equal-stale window — concrete walkthrough

To make the §7.2.2 fix concrete:

1. `t=0`: catalog at version `v`. Coordinators C1 and C2 both at `v`. Source FSM at `v`. CUTOVER not yet issued.
2. `t=1`: migrator issues CUTOVER on default group. Catalog committed at `v+1`. Default-group leader's outbound heartbeat starts carrying `max_cutover_version_seen = v+1` immediately.
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

The fix is a **scan-interval ownership gate** on the source FSM, parallel to §7.2.2's point-key gate. Like the point-key gate it has **two branches** keyed on the watermark, so it catches both the "coordinator-stale, source-up-to-date" and the "coordinator-and-source-equal-stale" cases (closes claude round-10 P1 on equal-stale scans, PR #945):

- **Branch 1 — source has applied CUTOVER (`source_local_version >= cutover_version`).** The FSM calls `GetIntersectingRoutes(start, end)` against its **own** current catalog snapshot. If **every** intersecting route is owned by this group, the scan proceeds. If **any** intersecting route is owned by a different group, the FSM returns `ErrRouteMoved{new_route_version, new_group_id, sub_range_start, sub_range_end}` for the **first** foreign-owned sub-range encountered (lexicographically smallest).
- **Branch 2 — source has NOT applied CUTOVER but `max_cutover_version_seen > source_local_version` (equal-stale, watcher lag).** The local snapshot still says `source.Active(full: [A, Z))` — a single route, no foreign owner visible — so Branch 1 alone would silently pass a cross-boundary scan. The gate therefore additionally calls `GetIntersectingRoutes([start, end), catalog_version = max_cutover_version_seen)` as a single best-effort, deadline-bounded RPC to the **default-group leader** (mirroring exactly §7.2.2's point-key extension — same watermark trigger, same RPC target, same 1 s cache TTL with cache key `(start, end, max_cutover_version_seen)`). The default-group leader answers from its current catalog snapshot. If any returned route is owned by another group, the source FSM returns `ErrRouteMoved{max_cutover_version_seen, foreign_group_id, sub_range_start, sub_range_end}` for the first foreign-owned sub-range. If every returned route is still source-owned (the elevated watermark covers an *unrelated* migration that did not touch this scan range), the scan proceeds normally.
- **Coordinator path identical in both branches.** The coordinator's existing `WatcherRefresh` + retry path (§7.2.3) re-issues the scan; on retry the fresh engine returns the correct two routes and `routesForScan` splits the scan into `[A, M)` to source and `[M, Z)` to target, both fenced correctly.
- **The §7.2.2 point-key gate still runs** for the scan's start key — the new check is **in addition to**, not in place of, so the watermark / equal-stale logic at the start key continues to apply.

Implementation cost: one additional `GetIntersectingRoutes` call per scan on the FSM. Branch 1 (the common case once watcher catches up) is local-only. Branch 2 adds at most one cached RPC to the default-group leader per `(start, end, max_cutover_version_seen)` per second — bounded by the same cache as §7.2.2's point-key extension. Memory: bounded by the number of routes intersecting the scan (typically 1–2; the rare cross-multi-boundary scan is the same `O(routes_in_range)` the coordinator already pays).

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

Same `t=0..t=2` setup as §7.2.2a (CUTOVER commits at `v+1`, default-group heartbeat carries `max_cutover_version_seen = v+1` immediately, source-group leader has received the heartbeat but its FSM watcher has **not yet** applied `v+1` — `source_local_version` is still `v`):

1. `t=3`: source-group leader's local atomic `max_cutover_version_seen = v+1`, but FSM-local `source_local_version = v`. Source FSM snapshot still says `source.Active(full: [A, Z))`.
2. `t=4`: stale coordinator C1 issues `ScanAt([D, Z), ts=now)` where `D ∈ [A, M)` — scan starts in the non-moved left half and ends in the moved right half. `read_route_version = v`.
3. Coordinator's `routesForScan` against its v-catalog returns `[source.Active(full: [A, Z))]` → entire scan dispatched to source group.
4. Source FSM receives the scan. §7.2.2 **point-key gate** runs: `max_cutover_version_seen (v+1) > source_local_version (v)` → equal-stale watermark fires; FSM calls `GetRouteOwnership(routeKey(D), v+1)` to the default-group leader → answer: "owned by source (left half)". Point-key check passes (the scan's START key is still source-owned).
5. **§7.2.2b scan-interval gate Branch 2 fires** (because watermark is elevated): FSM calls `GetIntersectingRoutes([D, Z), catalog_version = v+1)` to the default-group leader → returns `[source.Active(left: [D, M)), target.Active(right: [M, Z))]`. The second route is owned by target. The FSM returns `ErrRouteMoved{v+1, target_group_id, M, Z}` for the moved sub-range. **The gap §7.2.2c's Branch 1 would have left open is now closed** — without Branch 2, step 5 would have run `GetIntersectingRoutes` against the FSM's own `v` snapshot, returned the single `source.Active(full)` route, and served the entire scan stale.
6. `t=5`: C1 receives `ErrRouteMoved`, runs `WatcherRefresh`, advances to `v+1`, and re-issues the scan as two sub-scans: `[D, M)` to source and `[M, Z)` to target. Both succeed; the merged result is consistent. No stale data for `[M, Z)` was ever returned.

The Branch 2 RPC to the default-group leader is cached for 1 second per `(start, end, max_cutover_version_seen)`, so a burst of cross-boundary scans during the watcher-lag window pays at most one RPC per second across all of them. Once the watcher catches up (`source_local_version >= cutover_version`), Branch 1 takes over and the local snapshot is authoritative — the cached Branch 2 results are no longer consulted.

#### 7.2.3 Coordinator-side fallback

If the engine's local catalog is behind the FSM's `new_route_version`, the coordinator does a single best-effort `WatcherRefresh` and retries once. Repeated `ErrRouteMoved` surfaces to the client only on persistent inconsistency (alert-worthy).

#### 7.2.4 Grace window before CLEANUP

CLEANUP deletes the moved range from source's MVCC after a configurable grace window (`readFenceGrace`, default 30 s — chosen to comfortably exceed both lease TTL and watcher tick × 2).

**Pre-watcher staleness window after CUTOVER (closes claude round-11 medium on PR #945).** The `CatalogWatcher` propagates the CUTOVER catalog write to the source-group leader within one polling interval (`defaultCatalogWatcherInterval = 100 ms`, `distribution/watcher.go`). Within that ~100 ms window the source FSM has neither applied the new catalog nor received the heartbeat-carried `max_cutover_version_seen`, so §7.2.2b Branch 2's watermark check has nothing to fire on. The window is bounded by the watcher polling interval rather than the heartbeat interval (~1 s) because watcher → FSM apply is the faster of the two propagation paths. A coordinator whose watcher fired right at the same tick as CUTOVER could refresh and route reads to the target while the source's watcher hasn't yet applied v+1 — a ~100 ms window during which a cross-boundary read could see source-stale data. This is consistent with the watcher-lag model the read fence operates under elsewhere (lease reads, Composed-1 read path); deployments requiring zero-staleness can replace the polling watcher with a synchronous notification from the default-group leader to source-group leaders on the CUTOVER Raft apply (OQ-17 — "synchronous CUTOVER notification to source group" — out of scope for M2).

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
4. **M2-PR1..PR7 land in this order so the capability bit only opens when the full feature is present (closes claude round-11 P2 on PR #945).** PR1-PR3 are wire/codec/store additions with **no behaviour change for M1** (no SplitJob created without explicit `target_group_id != source_group_id`); PR4 introduces the state machine but no data move; PR5 lands FENCE rejection (`verifyRouteNotFenced` + `ErrRouteWriteFenced`); PR6 lands `ExportRangeVersions` / `ImportRangeVersions` server-side handlers + §6.4 step 2 staged-merge read path; PR7 lands the background promoter + `AbandonSplitJob` + CLEANUP drain. **The `cap_migration_v2` bit goes live only in PR7 commit**, when the full end-to-end migration path can complete — not in PR5 (where FENCE is enforced but BACKFILL has no export handler yet, so a SplitJob would create successfully and then fail at BACKFILL, giving operators false assurance). The earlier-rounds intention was "the bit guards against rollout-skew silent execution," and the corrected intent is "the bit guards against operators starting a migration in a half-shipped cluster." If a finer-grained gate is desired later, OQ-18 records the option to split into `cap_fence_v2` (PR5) + `cap_data_move_v2` (PR6/PR7), with `SplitRangeRequest` gated on the conjunction — out of scope for v1.
5. **Test plan**: `distribution/capability_test.go` exercises (a) gate accepts M2-only cluster, (b) gate rejects mixed cluster, (c) gate rejects on heartbeat staleness (no advertisement within 2× heartbeat interval treated as no capability).

This is independent of the existing rolling-upgrade protocol for unrelated subsystems and adds zero overhead for clusters never using cross-group split.

## 12. Open Questions

1. **Job concurrency.** M2 caps at one in-flight migration. Confirm acceptable for first cut; M3 may want concurrent jobs for hotspot bursts.
2. **Pacing knobs.** Default 1 MiB chunk + 5 ms inter-chunk pacing — these are guesses; should we ship a metrics-emitting default and let operators tune via `SplitRangeRequest.options`?
3. **Grace period (RESOLVED — `readFenceGrace = 30 s`).** §7.2.4 sets the default at 30 s with the rationale "comfortably exceeds both lease TTL and watcher tick × 2." All cross-references (§4 CLEANUP row) now match this value. Operators on extreme deployments (very long lease TTLs, or watcher polling intervals beyond the defaults) raise it via the existing `readFenceGrace` knob.
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
