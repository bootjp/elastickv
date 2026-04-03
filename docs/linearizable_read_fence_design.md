# Linearizable Read Fence Design

## Summary

This document describes a repo-local implementation of a ReadIndex-style
linearizable read fence for ElasticKV.

HashiCorp Raft, as currently used by this repository, exposes `VerifyLeader()`
and `Barrier()` but does not expose a public `ReadIndex()` API. Today most read
paths implement linearizability by:

1. checking that the local node is the leader, and
2. calling `VerifyLeader()`, then
3. reading the local FSM.

That is safe enough for many cases, but it leaves a small TOCTOU window between
the successful leader verification and the actual local read. A leader can lose
leadership in that gap and still serve a stale read from its local FSM.

The new design closes that gap without turning reads into writes.

## Goals

- Provide a linearizable local-read fence for Raft-backed read paths.
- Avoid `Barrier()` for normal reads, so reads do not append barrier log
  entries.
- Reuse the existing storage and routing structure of the repository.
- Minimize API churn in adapters and tests.

## Non-Goals

- Add a true `ReadIndex()` implementation to HashiCorp Raft itself.
- Change write-path leader verification.
- Make cross-shard range scans globally snapshot-consistent.
- Fully populate every existing `read_at_index` response field in this change.

## Current Problem

The current single-group and sharded read paths use a pattern similar to:

1. `raft.State() == Leader`
2. `raft.VerifyLeader()`
3. local FSM read

`VerifyLeader()` confirms that a quorum still recognizes the local node as the
leader, but it does not establish an FSM read fence. The leader can be deposed
after verification and before the read reaches the local store.

`Barrier()` is not the right replacement. It would close the correctness gap,
but it does so by appending a barrier log entry and waiting for it to apply.
That makes each read behave like a write from the Raft log's point of view.

## Design

### High-Level Algorithm

For local Raft-backed reads:

1. Verify that the local node is still leader using `VerifyLeader()`.
2. Read the leader's current `commit_index`.
3. Wait until the local FSM has actually applied at least that Raft index.
4. Perform the local store read.

This gives the repository a ReadIndex-style fence:

- the quorum confirmation comes from `VerifyLeader()`
- the read fence comes from `commit_index`
- the local-read safety comes from waiting for real FSM application

### Why FSM Tracking Is Needed

The Raft stats surface `applied_index`, but in HashiCorp Raft that value is
advanced before the FSM goroutine necessarily finishes applying the entry. It is
therefore not strong enough, by itself, to prove that the local store can serve
the read safely.

This change introduces an apply-completion tracker inside `kvFSM`. The tracker
records the highest Raft log index whose `Apply()` call has actually completed.
Linearizable read fences wait on that tracker rather than trusting Raft's
`applied_index` stat alone.

### Components

#### 1. FSM Applied Index Tracker

`kvFSM` gains an applied-index tracker with:

- `AppliedIndex() uint64`
- `WaitForAppliedIndex(ctx, index) error`

Each successful or failed `Apply()` completion records the Raft log index that
has finished processing, then wakes blocked readers.

#### 2. Raft-to-Tracker Registration

The runtime needs a way to find the applied-index tracker for a given
`*raft.Raft`. This design uses a lightweight registry populated when shard
groups are built.

The registry is intentionally internal to the repository. If no tracker is
registered for a Raft instance, the read fence falls back to a conservative
stats-based bootstrap path for compatibility with older tests and helper
setups.

#### 3. Linearizable Read Helper

A new helper performs the local read fence:

1. reject non-leaders
2. call `VerifyLeader()`
3. read `commit_index`
4. wait for FSM apply completion
5. return the read fence index

The helper returns the fence index for observability and future response-field
plumbing, even when the immediate caller only needs the error result.

### Routing Behavior

The new fence is used only for read paths that can serve from local state.

- If the node is not leader, the code still proxies reads to the known leader.
- If the node is leader but the fence fails with `ErrNotLeader`, the code falls
  back to leader proxying.
- If the fence fails for another reason, such as timeout or shutdown, the read
  returns an error instead of silently proxying.

That preserves the existing "proxy on leadership loss" behavior while making
true operational failures visible.

## Scope of This Change

This design is applied to:

- `kv.LeaderRoutedStore`
- `kv.ShardStore`
- Redis read commands that currently verify leadership and then read local state

This design is not applied to:

- write-path leader checks
- health endpoints
- generic "should I reverse proxy this whole request?" leader checks for
  non-read traffic

## Compatibility

No protobuf changes are required for correctness.

The existing `read_at_index` fields remain available for follow-up work, but
this change does not require that every server populate them immediately.

## Alternatives Considered

### 1. Keep `VerifyLeader()` Only

Pros:

- minimal code change

Cons:

- leaves the stale-read TOCTOU window open

Rejected.

### 2. Use `Barrier()` for Every Read

Pros:

- linearizable

Cons:

- appends a barrier log entry per read
- substantially more expensive for read-heavy workloads

Rejected.

### 3. Treat `LeaderLeaseTimeout` as a Read Lease

Pros:

- low latency

Cons:

- upstream Raft uses leader lease for step-down behavior, not as a public
  linearizable-read lease API
- adds clock/lease assumptions that the current code does not otherwise make

Rejected for this change.

### 4. Fork HashiCorp Raft and Add Public `ReadIndex()`

Pros:

- the cleanest long-term abstraction

Cons:

- significantly larger maintenance burden
- expands the patch surface to the Raft dependency itself

Deferred.

## Testing Strategy

- Unit tests for the FSM applied-index tracker.
- Unit tests for the linearizable read helper.
- Store-level tests verifying that local leader reads wait on the fence and that
  follower reads still proxy.
- Adapter tests for Redis commands that previously used `VerifyLeader()`
  directly before local reads.
- Existing package and repository test suites remain the main regression check.

## Follow-Up Work

- Populate `read_at_index` fields for all local gRPC read responses.
- Consider exposing fence metrics such as wait time and timeout count.
- Revisit whether a true Raft `ReadIndex()` fork is worth the ongoing
  dependency cost.
