package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/stretchr/testify/require"
)

// shardedLeaseEngine is a minimal raftengine.Engine + LeaseProvider used
// by sharded lease tests. It records LinearizableRead invocations and
// the registered leader-loss callback so tests can fire it on demand.
type shardedLeaseEngine struct {
	*fakeLeaseEngine
}

func newShardedLeaseEngine(applied uint64) *shardedLeaseEngine {
	return &shardedLeaseEngine{
		fakeLeaseEngine: &fakeLeaseEngine{
			applied:  applied,
			leaseDur: time.Hour,
		},
	}
}

func mustShardedLeaseCoord(t *testing.T, eng1, eng2 *shardedLeaseEngine) *ShardedCoordinator {
	t.Helper()
	distEngine := distribution.NewEngine()
	// Route a..m -> group 1, m..end -> group 2 so per-key tests can pick
	// a key landing on each shard.
	distEngine.UpdateRoute([]byte("a"), []byte("m"), 1)
	distEngine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 5}},
	}
	g2Txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 12}},
	}
	return NewShardedCoordinator(distEngine, map[uint64]*ShardGroup{
		1: {Engine: eng1, Txn: g1Txn},
		2: {Engine: eng2, Txn: g2Txn},
	}, 1, NewHLC(), nil)
}

func TestShardedCoordinator_LeaseReadForKey_PerShardIsolation(t *testing.T) {
	t.Parallel()

	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	// Pre-extend shard 1's lease only.
	g1 := coord.groups[1]
	g1.lease.extend(time.Now().Add(time.Hour))

	idx, err := coord.LeaseReadForKey(context.Background(), []byte("apple"))
	require.NoError(t, err)
	require.Equal(t, uint64(100), idx)
	require.Equal(t, int32(0), eng1.linearizableCalls.Load(),
		"shard 1 lease is valid; engine 1 should not be called")

	idx, err = coord.LeaseReadForKey(context.Background(), []byte("zebra"))
	require.NoError(t, err)
	require.Equal(t, uint64(200), idx)
	require.Equal(t, int32(1), eng2.linearizableCalls.Load(),
		"shard 2 lease was never extended; engine 2 must take the slow path")

	// After the slow path, shard 2's lease is now valid; engine 1 must
	// remain untouched.
	require.Equal(t, int32(0), eng1.linearizableCalls.Load())
}

func TestShardedCoordinator_LeaseReadForKey_ErrorOnlyInvalidatesShard(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("read-index failed")
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	eng2.linearizableErr = sentinel
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	g1 := coord.groups[1]
	g2 := coord.groups[2]
	g1.lease.extend(time.Now().Add(time.Hour))
	g2.lease.extend(time.Now().Add(time.Hour))
	g2.lease.invalidate() // force shard 2 onto slow path

	_, err := coord.LeaseReadForKey(context.Background(), []byte("zebra"))
	require.ErrorIs(t, err, sentinel)
	require.False(t, g2.lease.valid(time.Now()),
		"shard 2 lease must be invalidated after error")
	require.True(t, g1.lease.valid(time.Now()),
		"shard 1 lease must NOT be touched by shard 2's failure")
}

func TestShardedCoordinator_RegistersPerShardLeaderLossCallback(t *testing.T) {
	t.Parallel()

	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	require.Equal(t, int32(1), eng1.registerLeaderLossCalled.Load(),
		"NewShardedCoordinator must register a callback per shard engine")
	require.Equal(t, int32(1), eng2.registerLeaderLossCalled.Load())

	g1 := coord.groups[1]
	g2 := coord.groups[2]
	g1.lease.extend(time.Now().Add(time.Hour))
	g2.lease.extend(time.Now().Add(time.Hour))

	eng1.fireLeaderLoss()
	require.False(t, g1.lease.valid(time.Now()),
		"shard 1 leader-loss callback must invalidate shard 1's lease")
	require.True(t, g2.lease.valid(time.Now()),
		"shard 2 lease must remain valid; only its own engine's callback affects it")
}
