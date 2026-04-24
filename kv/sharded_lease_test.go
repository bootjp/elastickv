package kv

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/monoclock"
	pb "github.com/bootjp/elastickv/proto"
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
	g1.lease.extend(monoclock.Now().Add(time.Hour), g1.lease.generation())

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
	g1.lease.extend(monoclock.Now().Add(time.Hour), g1.lease.generation())
	g2.lease.extend(monoclock.Now().Add(time.Hour), g2.lease.generation())
	g2.lease.invalidate() // force shard 2 onto slow path

	_, err := coord.LeaseReadForKey(context.Background(), []byte("zebra"))
	require.ErrorIs(t, err, sentinel)
	require.False(t, g2.lease.valid(monoclock.Now()),
		"shard 2 lease must be invalidated after error")
	require.True(t, g1.lease.valid(monoclock.Now()),
		"shard 1 lease must NOT be touched by shard 2's failure")
}

func TestShardedCoordinator_LeaseRefreshingTxn_SkipsWhenCommitIndexZero(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	g1 := coord.groups[1]
	// A response with CommitIndex == 0 signals "no Raft proposal
	// happened" (TransactionManager short-circuits on empty input /
	// no-op abort). Refreshing in that case would be unsound.
	noRaftResp := &TransactionResponse{CommitIndex: 0}
	txn, ok := g1.Txn.(*leaseRefreshingTxn)
	require.True(t, ok, "NewShardedCoordinator wraps Txn in leaseRefreshingTxn")
	txn.inner = &fixedTransactional{response: noRaftResp}

	require.False(t, g1.lease.valid(monoclock.Now()))

	// Commit with empty input returns success with CommitIndex=0.
	_, err := g1.Txn.Commit(nil)
	require.NoError(t, err)
	require.False(t, g1.lease.valid(monoclock.Now()),
		"lease must NOT be refreshed when no Raft commit happened")

	// Same for Abort.
	_, err = g1.Txn.Abort(nil)
	require.NoError(t, err)
	require.False(t, g1.lease.valid(monoclock.Now()))

	// A response with CommitIndex > 0 refreshes the lease.
	realResp := &TransactionResponse{CommitIndex: 42}
	txn.inner = &fixedTransactional{response: realResp}
	_, err = g1.Txn.Commit(nil)
	require.NoError(t, err)
	require.True(t, g1.lease.valid(monoclock.Now()),
		"lease must be refreshed after a real Raft commit")
}

// fixedTransactional is a minimal Transactional whose Commit/Abort
// always return the same response. Used to drive the lease-refresh
// gating tests deterministically.
type fixedTransactional struct {
	response *TransactionResponse
}

func (f *fixedTransactional) Commit(_ []*pb.Request) (*TransactionResponse, error) {
	return f.response, nil
}

func (f *fixedTransactional) Abort(_ []*pb.Request) (*TransactionResponse, error) {
	return f.response, nil
}

// closableTransactional satisfies both Transactional and io.Closer so
// the Close-delegation test can observe whether the wrapper forwards
// Close to the inner value.
type closableTransactional struct {
	fixedTransactional
	closed atomic.Bool
}

func (c *closableTransactional) Close() error {
	c.closed.Store(true)
	return nil
}

func TestLeaseRefreshingTxn_ForwardsClose(t *testing.T) {
	t.Parallel()
	inner := &closableTransactional{
		fixedTransactional: fixedTransactional{response: &TransactionResponse{}},
	}
	wrapper := &leaseRefreshingTxn{inner: inner, g: &ShardGroup{}}

	// ShardStore.closeGroup does a guarded type assertion
	// `if closer, ok := g.Txn.(io.Closer); ok { closer.Close() }`.
	// After wrapping, that `ok` must still be true and the resulting
	// Close must reach the inner Transactional.
	closer, ok := interface{}(wrapper).(io.Closer)
	require.True(t, ok, "leaseRefreshingTxn must implement io.Closer")
	require.NoError(t, closer.Close())
	require.True(t, inner.closed.Load(),
		"Close must delegate to the wrapped Transactional so ShardStore.closeGroup can release its resources")
}

func TestLeaseRefreshingTxn_CloseNoopWhenInnerIsNotCloser(t *testing.T) {
	t.Parallel()
	// fixedTransactional does NOT implement io.Closer. The wrapper's
	// Close must be a safe no-op rather than panicking.
	inner := &fixedTransactional{response: &TransactionResponse{}}
	wrapper := &leaseRefreshingTxn{inner: inner, g: &ShardGroup{}}
	require.NoError(t, wrapper.Close())
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
	g1.lease.extend(monoclock.Now().Add(time.Hour), g1.lease.generation())
	g2.lease.extend(monoclock.Now().Add(time.Hour), g2.lease.generation())

	eng1.fireLeaderLoss()
	require.False(t, g1.lease.valid(monoclock.Now()),
		"shard 1 leader-loss callback must invalidate shard 1's lease")
	require.True(t, g2.lease.valid(monoclock.Now()),
		"shard 2 lease must remain valid; only its own engine's callback affects it")
}
