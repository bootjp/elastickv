package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestShardedCoordinatorVerifyLeader_LeaderReturnsNil(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "shard-leader", NewKvFSM(st))
	t.Cleanup(stop)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Raft: r, Store: st, Txn: NewLeaderProxy(r)},
	}, 1, NewHLC(), NewShardStore(engine, map[uint64]*ShardGroup{
		1: {Raft: r, Store: st, Txn: NewLeaderProxy(r)},
	}))

	require.NoError(t, coord.VerifyLeader())
	require.NoError(t, coord.VerifyLeaderForKey([]byte("b")))
}

func TestShardedCoordinatorVerifyLeader_MissingGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{}, 1, NewHLC(), nil)

	require.ErrorIs(t, coord.VerifyLeader(), ErrLeaderNotFound)
	require.ErrorIs(t, coord.VerifyLeaderForKey([]byte("k")), ErrLeaderNotFound)
}
