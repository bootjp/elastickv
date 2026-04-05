package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

func engineFromRaft(r *raft.Raft) raftengine.Engine {
	if r == nil {
		return nil
	}
	return hashicorpraftengine.New(r)
}

func engineForGroup(g *ShardGroup) raftengine.Engine {
	if g == nil {
		return nil
	}
	if g.Engine != nil {
		return g.Engine
	}
	return engineFromRaft(g.Raft)
}

func isLeaderEngine(engine raftengine.LeaderView) bool {
	return engine != nil && engine.State() == raftengine.StateLeader
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
