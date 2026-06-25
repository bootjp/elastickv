# FSM Compaction Design

## Objective

Run MVCC compaction automatically for the FSM store without breaking:

1. Ongoing transactional reads.
2. Read-modify-write flows that depend on a stable read timestamp.
3. Raft apply progress during backlog replay or elections.

## Problem

The current FSM store used by `main.go` and `cmd/server/demo.go` is the
in-memory `mvccStore`.

1. It keeps all MVCC versions in memory.
2. `Compact(ctx, minTS)` exists, but nothing calls it in production startup.
3. The API still supports explicit historical reads (`GetAt`, `ScanAt`), so
   naive compaction can silently change query semantics.

## Design

### 1. Background Compactor

Add a background `kv.FSMCompactor` that runs on a timer instead of the Raft
apply path.

Why:

1. Compaction is potentially heavy.
2. Raft apply must stay simple and deterministic.
3. Compaction should be skipped while a node is still catching up.

### 2. Runtime Eligibility Checks

For each Raft runtime, compaction only runs when:

1. `fsm_pending == 0`
2. `applied_index >= commit_index`
3. Raft state is not `Candidate`

This avoids compacting while the local FSM is still behind or unstable.

### 3. Retention Watermark

The compactor computes a safe `minTS` from:

1. A retention window based on HLC wall time.
2. The oldest actively pinned read timestamp.

Effective rule:

1. Start with `now - retentionWindow` in HLC form.
2. If an older in-flight read exists, clamp the watermark to just before that
   read timestamp.

### 4. Active Timestamp Tracking

Longer read-modify-write flows pin their read timestamp through
`kv.ActiveTimestampTracker`.

The current implementation pins:

1. Redis `MULTI` / `EXEC` transactions.
2. Redis Lua script execution.
3. Distribution `SplitRange`, which performs a catalog read-modify-write cycle.

### 5. Historical Read Contract

The in-memory store now tracks `minRetainedTS`.

1. Reads older than `minRetainedTS` fail with `store.ErrReadTSCompacted`.
2. This makes compaction explicit instead of returning silently incorrect
   historical results.
3. `minRetainedTS` is included in `mvccStore` snapshots so restart/restore keeps
   the same retention boundary.

### 6. Scope of the Current Implementation

Implemented now:

1. Background FSM compactor in `kv/compactor.go`
2. Active timestamp tracker in `kv/active_timestamp_tracker.go`
3. `minRetainedTS` enforcement for `mvccStore`
4. Startup wiring in:
   - `main.go`
   - `cmd/server/demo.go`
5. Redis and distribution server timestamp pinning

Not implemented yet:

1. Pebble FSM compaction logic
2. Dedicated GC for transaction metadata keys such as commit/rollback records
3. Prometheus metrics for compaction runs
4. Full active timestamp pinning across every DynamoDB multi-step flow

## Current Defaults

The compactor currently uses:

1. Interval: `30s`
2. Retention window: `30m`
3. Per-run timeout: `5s`

These are code defaults and can be tuned later if we expose flags or config.

## Tradeoffs

### Pros

1. MVCC history no longer grows forever in the default in-memory FSM path.
2. Ongoing Redis transactions and scripted operations are protected.
3. Historical reads fail explicitly once compacted away.

### Cons

1. Arbitrary old raw timestamp reads are no longer guaranteed forever.
2. DynamoDB paths currently rely mostly on the retention window rather than
   explicit timestamp pinning.
3. Transaction metadata keys still need a dedicated GC pass.

## Follow-Up Work

1. Add compaction metrics and visibility in the monitoring registry.
2. Add transaction-metadata GC for `!txn|cmt|...` and `!txn|rb|...` keys.
3. Extend active timestamp pinning to the remaining long DynamoDB flows.
4. Reuse the same contract when `PebbleStore` becomes the primary FSM store.
