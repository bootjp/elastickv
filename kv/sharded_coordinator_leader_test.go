package kv

import (
	"context"
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
	r, stop := newSingleRaft(t, "shard-leader", NewKvFSMWithHLC(st, NewHLC()))
	t.Cleanup(stop)

	re := r
	groups := map[uint64]*ShardGroup{
		1: {Engine: re, Store: st, Txn: NewLeaderProxyWithEngine(re)},
	}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups))

	require.NoError(t, coord.VerifyLeader(context.Background()))
	require.NoError(t, coord.VerifyLeaderForKey(context.Background(), []byte("b")))
}

func TestShardedCoordinatorVerifyLeader_MissingGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{}, 1, NewHLC(), nil)

	require.ErrorIs(t, coord.VerifyLeader(context.Background()), ErrLeaderNotFound)
	require.ErrorIs(t, coord.VerifyLeaderForKey(context.Background(), []byte("k")), ErrLeaderNotFound)
}

// TestShardedCoordinatorLeaseReadAllGroups_FencesEveryLeader asserts that
// LeaseReadAllGroups establishes the lease bound on EVERY owned group, not
// just the default group (codex P1-A): a multi-shard scan visits all routes,
// so a default-group-only fence would leave non-default groups unfenced.
func TestShardedCoordinatorLeaseReadAllGroups_FencesEveryLeader(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "lrag-g1", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)
	s2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "lrag-g2", NewKvFSMWithHLC(s2, NewHLC()))
	t.Cleanup(stop2)

	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
		2: {Engine: r2, Store: s2, Txn: NewLeaderProxyWithEngine(r2)},
	}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups))

	require.NoError(t, coord.LeaseReadAllGroups(context.Background()))
}

// TestShardedCoordinatorLeaseReadAllGroups_FailsClosedOnUnreadableGroup
// asserts LeaseReadAllGroups fails closed when ANY group cannot confirm its
// lease: a partially-fenced multi-shard read is exactly the stale read the
// fence guards against. Group 2 has no engine, so its lease read errors.
func TestShardedCoordinatorLeaseReadAllGroups_FailsClosedOnUnreadableGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "lrag-fail-g1", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)

	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
		2: {Store: store.NewMVCCStore()}, // no Engine: lease read fails closed
	}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups))

	require.ErrorIs(t, coord.LeaseReadAllGroups(context.Background()), ErrLeaderNotFound)
}

// TestShardedCoordinatorLeaseReadAllGroups_EmptyGroups asserts the no-groups
// case fails closed rather than silently confirming freshness on nothing.
func TestShardedCoordinatorLeaseReadAllGroups_EmptyGroups(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{}, 1, NewHLC(), nil)

	require.ErrorIs(t, coord.LeaseReadAllGroups(context.Background()), ErrLeaderNotFound)
}
