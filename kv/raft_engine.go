package kv

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

func engineForGroup(g *ShardGroup) raftengine.Engine {
	if g == nil {
		return nil
	}
	return g.Engine
}

func isLeaderEngine(engine raftengine.LeaderView) bool {
	return engine != nil && engine.State() == raftengine.StateLeader
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
	return errors.WithStack(engine.VerifyLeader(ctx))
}

func verifyLeaderEngine(engine raftengine.LeaderView) error {
	return verifyLeaderEngineCtx(context.Background(), engine)
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
// time.Now() sample.
func leaseReadEngineCtx(ctx context.Context, engine raftengine.LeaderView) (uint64, error) {
	if engine == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	if lp, ok := engine.(raftengine.LeaseProvider); ok {
		if leaseDur := lp.LeaseDuration(); leaseDur > 0 {
			now := time.Now()
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
