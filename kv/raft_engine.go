package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
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

func leaderAddrFromEngine(engine raftengine.LeaderView) raft.ServerAddress {
	if engine == nil {
		return ""
	}
	return raft.ServerAddress(engine.Leader().Address)
}
