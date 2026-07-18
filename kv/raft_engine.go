package kv

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// verifyLeaderTimeout caps how long the no-context verifyLeaderEngine path
// is willing to wait for a ReadIndex round-trip. Without this bound,
// callers that hold context.Background() (LeaderProxy.Commit/Abort,
// Coordinate.VerifyLeader, ShardedCoordinator.VerifyLeader[ForKey], and
// the S3/SQS/admin /healthz/leader handlers) blocked indefinitely whenever
// ReadIndex completion stalled, and a single transient stall accumulated
// callers permanently — Engine.run's Ready loop walks pendingReads O(N)
// per tick, so the queue feeds back on itself once it grows.
//
// 5s matches leaderForwardTimeout: a verify that takes longer than a
// single forward RPC is useless as a freshness check, and the proxy's
// verify-then-forward path stays within its 5s retry budget.
//
// See PR #745 / incident 2026-05-08 for the goroutine-pile production
// failure this prevents.
const verifyLeaderTimeout = 5 * time.Second

func engineForGroup(g *ShardGroup) raftengine.Engine {
	if g == nil {
		return nil
	}
	return g.Engine
}

func isLeaderEngine(engine raftengine.LeaderView) bool {
	return engine != nil && engine.State() == raftengine.StateLeader
}

func leadershipForEngine(engine interface {
	raftengine.LeaderView
	raftengine.StatusReader
}) (bool, uint64) {
	if engine == nil {
		return false, 0
	}
	status := engine.Status()
	return status.State == raftengine.StateLeader, status.Term
}

// isLeaderAcceptingWrites reports whether the engine is currently the leader
// AND not mid leadership-transfer. During transfer, etcd/raft silently drops
// proposals; callers that poll on a timer (HLC lease, lock resolver) should
// pause to avoid log spam and wasted CPU.
func isLeaderAcceptingWrites(engine interface {
	raftengine.LeaderView
	raftengine.StatusReader
}) bool {
	if !isLeaderEngine(engine) {
		return false
	}
	return engine.Status().LeadTransferee == 0
}

func verifyLeaderEngineCtx(ctx context.Context, engine raftengine.LeaderView) error {
	if engine == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	// Defense-in-depth: if the caller's context carries no deadline (the
	// Redis server's long-lived handlerContext, gemini-flagged background
	// loops, or any future caller that passes context.Background), wrap
	// with verifyLeaderTimeout so a stalled ReadIndex still surfaces
	// fail-fast — same bound the no-arg verifyLeaderEngine wrapper has
	// provided since #745. Callers that already set a tighter deadline
	// (Redis dispatch ctx with redisDispatchTimeout, healthz
	// r.Context()) keep theirs: context.WithTimeout picks the earlier of
	// the two expirations.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, verifyLeaderTimeout)
		defer cancel()
	}
	return errors.WithStack(engine.VerifyLeader(ctx))
}

func verifyLeaderEngine(engine raftengine.LeaderView) error {
	ctx, cancel := context.WithTimeout(context.Background(), verifyLeaderTimeout)
	defer cancel()
	return verifyLeaderEngineCtx(ctx, engine)
}

func linearizableReadEngineCtx(ctx context.Context, engine raftengine.LeaderView) (uint64, error) {
	if engine == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	index, err := engine.LinearizableRead(ctx)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return index, nil
}

// leaseReadEngineCtx is the lease-aware sibling of
// linearizableReadEngineCtx. When the engine exposes LeaseProvider
// and the engine-driven lease anchor (LastQuorumAck) is fresh, it
// returns the current AppliedIndex WITHOUT dispatching a new
// read-index request. Only when the lease is unavailable / expired
// does it fall through to the full LinearizableRead round-trip.
//
// Used by ShardStore.GetAt and friends so the per-redis.call read
// path no longer funnels every read through the single raft
// dispatch worker — the fast-path goal PR #560 shipped for the
// top-level Redis GET, now extended to all internal
// storage-read callers.
//
// Safety mirrors Coordinator.LeaseRead (see engineLeaseAckValid):
// the returned AppliedIndex is only served when the local node is
// Leader AND LastQuorumAck is within LeaseDuration of a single
// monoclock.Now() sample (CLOCK_MONOTONIC_RAW).
func leaseReadEngineCtx(ctx context.Context, engine raftengine.LeaderView) (uint64, error) {
	if engine == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	if lp, ok := engine.(raftengine.LeaseProvider); ok {
		if leaseDur := lp.LeaseDuration(); leaseDur > 0 {
			now := monoclock.Now()
			if engineLeaseAckValid(engine.State(), lp.LastQuorumAck(), now, leaseDur) {
				return lp.AppliedIndex(), nil
			}
		}
	}
	index, err := engine.LinearizableRead(ctx)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return index, nil
}

func leaderAddrFromEngine(engine raftengine.LeaderView) string {
	if engine == nil {
		return ""
	}
	return engine.Leader().Address
}
