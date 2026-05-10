# 2026-05-10 — Plumb caller context through kv write & VerifyLeader paths

Status: proposed

## Problem

PR #745 capped `verifyLeaderEngine` (`kv/raft_engine.go`) at 5 s as an
incident hotfix: every caller without an upstream context — `LeaderProxy.Commit/Abort`,
`Coordinate.VerifyLeader`, `ShardedCoordinator.VerifyLeader[ForKey]`, the S3 / SQS / admin
`/healthz/leader` handlers — used `context.Background()` and so blocked
indefinitely whenever a ReadIndex round-trip stalled. Goroutine pile-up
collapsed the leader (the 2026-05-08 incident: 20 K goroutines, 1870 % CPU, OOM).

The 5 s deadline is a defense-in-depth bound. It is not the right answer
for callers that already hold a request context with its own deadline:

- The Redis / DynamoDB / S3 / SQS dispatch path enters via
  `ShardedCoordinator.Dispatch(ctx, …)` and threads `ctx` through
  `dispatchTxn`, but the call lands in `g.Txn.Commit(reqs)` — a
  `Transactional` method whose interface drops `ctx` on the floor.
- `LeaderProxy.Commit` then calls `verifyLeaderEngine(p.engine)` (no
  ctx). The 5 s safety bound applies, but a client whose own deadline
  expired 2 s in still pays the full 5 s.
- The healthz handlers have `r.Context()` but the leader-probe interface
  (`LeaderProbe.IsVerifiedLeader() bool`) drops it. Caddy's per-probe
  budget cannot reach the verify call.

A second smaller hazard lives at `kv/transaction.go:152`:
`proposer.Propose(context.Background(), b)`. Same shape as the original
verifyLeaderEngine bug, just on the propose path instead of the verify
path.

## Goals

1. Pass the caller's `context.Context` end-to-end through the kv write
   path: dispatch → `Transactional.Commit/Abort` → `TransactionManager` /
   `LeaderProxy` → `verifyLeaderEngine` and `proposer.Propose`.
2. Pass the request context through the leader-probe path: HTTP handler →
   `LeaderProbe.IsVerifiedLeader(ctx)` → `Coordinate.VerifyLeader(ctx)` /
   `ShardedCoordinator.VerifyLeader[ForKey](ctx)` → engine.
3. Keep PR #745's 5 s bound on the **no-ctx** call site (`verifyLeaderEngine()`
   with no argument) as defense-in-depth. The bound is invoked when a future
   internal caller is added that genuinely cannot inherit a deadline (lock
   resolver, HLC lease) so the regression cannot recur.

## Non-goals

- Changing the wire-level deadline of any RPC. Existing client deadlines
  are preserved unchanged; this PR only stops dropping them.
- Eliminating `verifyLeaderTimeout`. It stays as the no-ctx fallback's
  bound.

## Surface change

**Interface signatures (kv-internal, no external API):**

```go
// kv/transaction.go
type Transactional interface {
    Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error)
    Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error)
}
```

Implementations updated to take `ctx`:

- `*TransactionManager` — passes ctx into `applyRequests`, which passes
  to `proposer.Propose(ctx, …)`. Replaces the
  `proposer.Propose(context.Background(), …)` at the existing
  `transaction.go:152`.
- `*LeaderProxy` — passes ctx into `verifyLeaderEngineCtx(ctx, …)` and
  into `forwardWithRetry(ctx, …)`. The deadline-budget arithmetic in
  `forwardWithRetry` already respects the parent ctx, so no logic
  change — only the seed parent shifts from `context.Background()` to
  the caller's ctx.
- `*leaseRefreshingTxn` — pure pass-through wrapper.
- `*ShardRouter` — pass-through.

**Caller plumbing:**

- `ShardedCoordinator.dispatchSingleShardTxn` gains a `ctx` parameter;
  the 6 internal callsites of `g.Txn.Commit(...)` plumb ctx in.
- `applyTxnResolution` (`kv/shard_store.go`) gains `ctx`; called from
  `LockResolver.resolveExpiredLock` which already holds a per-cycle
  ctx.

**Verify-leader surface:**

```go
func (c *Coordinate)         VerifyLeader(ctx context.Context) error
func (c *ShardedCoordinator) VerifyLeader(ctx context.Context) error
func (c *ShardedCoordinator) VerifyLeaderForKey(ctx context.Context, key []byte) error
```

The `Coordinator` interface methods themselves take ctx — no
parallel `Ctx`-suffixed variants. The 5 s safety bound is now
internal to `verifyLeaderEngineCtx`: when the caller's ctx has no
deadline (Redis server's long-lived `handlerContext`, background
loops, …), the helper applies `verifyLeaderTimeout` as a wrapper.
Callers with a tighter deadline keep theirs because
`context.WithTimeout` picks the earlier of the two expirations.

**LeaderProbe (`internal/admin/router.go`):**

```go
type LeaderProbe interface {
    IsVerifiedLeader(ctx context.Context) bool
}
```

`main_admin.go` implementation calls `coordinate.VerifyLeader(ctx)`.
Admin `/admin/healthz/leader` handler passes `r.Context()`.

**Adapter healthz (`adapter/s3.go`, `adapter/sqs.go`, `adapter/dynamodb.go`):**

`isVerifiedS3Leader(ctx)` / `isVerifiedSQSLeader(ctx, coordinator)` /
`isVerifiedDynamoLeader(ctx, coordinator)` take ctx, pass it to
`VerifyLeader(ctx)`. HTTP handlers feed `r.Context()`.

## Behaviour

For callers that already had a deadline upstream:

- A Redis client `BLPOP timeout=2s` whose dispatch lands on a slow
  ReadIndex now fails after **2 s** (its own deadline), not 5 s.
- A Caddy active health probe with a 1 s budget likewise fails after
  1 s, not 5 s.

For internal background callers without an upstream deadline:

- LockResolver, HLC lease, etc. continue to hit
  `verifyLeaderEngine()` (the no-arg variant) which still wraps with
  `context.WithTimeout(context.Background(), verifyLeaderTimeout)`.
  PR #745's 5 s bound stays as their safety net.

For misuse cases:

- A future code path that adds a caller without inheriting ctx and
  uses `context.Background()` directly bypasses both the wrapper and
  the 5 s bound; this is the same exposure the ecosystem accepts in
  general (passing Background is a code smell, and the linter flags
  it). The 5 s bound only protects the official no-ctx wrapper.

## Self-review checklist (kept brief; expanded in the PR body)

1. Data loss — no proposal-path change beyond ctx; `Propose(ctx, …)`
   semantics on cancellation match upstream raftengine, which already
   handles `ctx.Err()` as a transient `errProposalCancelled`.
2. Concurrency — ctx is value-passed, not shared mutable state.
3. Performance — no extra round-trip; same number of calls. `WithTimeout`
   in the no-ctx wrapper is unchanged.
4. Data consistency — verify is a freshness check, not a write path;
   shorter deadlines just surface ErrLeaderNotFound earlier.
5. Test coverage — interface change ripples through 3 test stubs
   (`stubTransactional`, `scriptedTransactional`, `fakeTM`); each
   gains a `ctx context.Context` parameter that is currently
   unused but available for future tests asserting cancel propagation.

## Rollout

Single PR, follow-up to merged #745 / #746 / #747. No design-deferred
milestones; all four layers (`Transactional`, `Coordinate.VerifyLeader`,
`LeaderProbe`, healthz handlers) ship together because the value
chains end-to-end.
