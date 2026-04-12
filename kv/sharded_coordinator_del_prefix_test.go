package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestHasDelPrefixElem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		elems []*Elem[OP]
		want  bool
	}{
		{
			name:  "nil slice",
			elems: nil,
			want:  false,
		},
		{
			name:  "empty slice",
			elems: []*Elem[OP]{},
			want:  false,
		},
		{
			name: "only Put",
			elems: []*Elem[OP]{
				{Op: Put, Key: []byte("k"), Value: []byte("v")},
			},
			want: false,
		},
		{
			name: "only Del",
			elems: []*Elem[OP]{
				{Op: Del, Key: []byte("k")},
			},
			want: false,
		},
		{
			name: "DelPrefix with nil key",
			elems: []*Elem[OP]{
				{Op: DelPrefix, Key: nil},
			},
			want: true,
		},
		{
			name: "DelPrefix with specific prefix",
			elems: []*Elem[OP]{
				{Op: DelPrefix, Key: []byte("prefix:")},
			},
			want: true,
		},
		{
			name:  "nil element skipped",
			elems: []*Elem[OP]{nil, {Op: DelPrefix}},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, hasDelPrefixElem(tt.elems))
		})
	}
}

// TestShardedCoordinator_DelPrefixBroadcastsToAllGroups verifies that a
// DEL_PREFIX operation is sent to every shard group, not routed to one.
func TestShardedCoordinator_DelPrefixBroadcastsToAllGroups(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 5}},
	}
	g2Txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 12}},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(12), resp.CommitIndex, "should return max commit index")

	// Both groups must have received exactly one request.
	require.Len(t, g1Txn.requests, 1)
	require.Len(t, g2Txn.requests, 1)

	for _, txn := range []*recordingTransactional{g1Txn, g2Txn} {
		req := txn.requests[0]
		require.False(t, req.IsTxn)
		require.Equal(t, pb.Phase_NONE, req.Phase)
		require.NotZero(t, req.Ts)
		require.Len(t, req.Mutations, 1)
		require.Equal(t, pb.Op_DEL_PREFIX, req.Mutations[0].Op)
		require.Empty(t, req.Mutations[0].Key, "nil prefix means all keys")
	}

	// All groups should receive the same timestamp for a single DEL_PREFIX.
	require.Equal(t, g1Txn.requests[0].Ts, g2Txn.requests[0].Ts,
		"same DEL_PREFIX element must use the same timestamp across shards")
}

// TestShardedCoordinator_DelPrefixRejectsTxn verifies that DEL_PREFIX inside
// a transactional group is rejected.
func TestShardedCoordinator_DelPrefixRejectsTxn(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: &recordingTransactional{}},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

// TestShardedCoordinator_DelPrefixRejectsMixed verifies that mixing DEL_PREFIX
// with other operations in the same dispatch is rejected.
func TestShardedCoordinator_DelPrefixRejectsMixed(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: &recordingTransactional{}},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

// TestShardedCoordinator_DelPrefixWithSpecificPrefix verifies broadcasting
// with a non-nil prefix.
func TestShardedCoordinator_DelPrefixWithSpecificPrefix(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{}
	g2Txn := &recordingTransactional{}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	prefix := []byte("dynamo|items|")
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: prefix},
		},
	})
	require.NoError(t, err)

	require.Len(t, g1Txn.requests, 1)
	require.Len(t, g2Txn.requests, 1)

	for _, txn := range []*recordingTransactional{g1Txn, g2Txn} {
		mut := txn.requests[0].Mutations[0]
		require.Equal(t, pb.Op_DEL_PREFIX, mut.Op)
		require.Equal(t, prefix, mut.Key)
	}
}

// TestShardedCoordinator_DelPrefixPartialFailure verifies that an error is
// returned when one of the shard groups fails, while the other still executes.
func TestShardedCoordinator_DelPrefixPartialFailure(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 5}},
	}
	failErr := errors.New("shard2 disk full")
	g2Txn := &recordingTransactional{
		errs: []error{failErr},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
		},
	})
	require.Error(t, err)
	// Both groups should still have been called.
	require.Len(t, g1Txn.requests, 1)
	require.Len(t, g2Txn.requests, 1)
}

// TestShardedCoordinator_DelPrefixSingleShard verifies broadcast works with
// a single shard group.
func TestShardedCoordinator_DelPrefixSingleShard(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 42}},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
	}, 1, NewHLC(), nil)

	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), resp.CommitIndex)
	require.Len(t, g1Txn.requests, 1)
}

// TestShardedCoordinator_DelPrefixIntegration uses real raft instances to
// verify that DEL_PREFIX actually deletes data across multiple shards.
func TestShardedCoordinator_DelPrefixIntegration(t *testing.T) {
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "dp-g1", NewKvFSM(s1))
	t.Cleanup(stop1)

	s2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "dp-g2", NewKvFSM(s2))
	t.Cleanup(stop2)

	e1 := hashicorpraftengine.New(r1)
	e2 := hashicorpraftengine.New(r2)
	groups := map[uint64]*ShardGroup{
		1: {Engine: e1, Store: s1, Txn: NewLeaderProxyWithEngine(e1)},
		2: {Engine: e2, Store: s2, Txn: NewLeaderProxyWithEngine(e2)},
	}

	shardStore := NewShardStore(engine, groups)
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), shardStore)

	// Write keys into both shards.
	_, err := coord.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
		},
	})
	require.NoError(t, err)

	_, err = coord.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)

	// Confirm both keys are readable.
	readTS := shardStore.LastCommitTS()
	v, err := shardStore.GetAt(ctx, []byte("b"), readTS)
	require.NoError(t, err)
	require.Equal(t, "v1", string(v))

	v, err = shardStore.GetAt(ctx, []byte("x"), readTS)
	require.NoError(t, err)
	require.Equal(t, "v2", string(v))

	// Dispatch DEL_PREFIX with nil key (flush all).
	_, err = coord.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
		},
	})
	require.NoError(t, err)

	// Both keys should now be deleted (tombstoned).
	readTS = shardStore.LastCommitTS()
	_, err = shardStore.GetAt(ctx, []byte("b"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "key 'b' should be deleted after DEL_PREFIX")

	_, err = shardStore.GetAt(ctx, []byte("x"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "key 'x' should be deleted after DEL_PREFIX")
}

// TestShardedCoordinator_DelPrefixPreservesTxnKeys verifies that transaction-
// internal keys (!txn|*) are not deleted by DEL_PREFIX.
func TestShardedCoordinator_DelPrefixPreservesTxnKeys(t *testing.T) {
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "dp-txn-g1", NewKvFSM(s1))
	t.Cleanup(stop1)

	groups := map[uint64]*ShardGroup{
		1: {Engine: hashicorpraftengine.New(r1), Store: s1, Txn: NewLeaderProxyWithEngine(hashicorpraftengine.New(r1))},
	}
	shardStore := NewShardStore(engine, groups)
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), shardStore)

	// Write a user key via the coordinator.
	_, err := coord.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("user-key"), Value: []byte("user-val")},
		},
	})
	require.NoError(t, err)

	// Write a txn-internal key directly into the store to simulate leftover
	// transaction metadata. PutAt bypasses raft but lets us verify the FSM's
	// exclude-prefix logic in isolation.
	txnKey := []byte(TxnKeyPrefix + "cmt|simulated")
	err = s1.PutAt(ctx, txnKey, []byte("commit-record"), shardStore.LastCommitTS(), 0)
	require.NoError(t, err)

	readTS := shardStore.LastCommitTS()
	_, err = shardStore.GetAt(ctx, []byte("user-key"), readTS)
	require.NoError(t, err)
	_, err = s1.GetAt(ctx, txnKey, readTS)
	require.NoError(t, err)

	// DEL_PREFIX (flush all) should delete user keys but not txn-internal keys.
	_, err = coord.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: nil},
		},
	})
	require.NoError(t, err)

	readTS = shardStore.LastCommitTS()
	_, err = shardStore.GetAt(ctx, []byte("user-key"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "user key should be deleted")

	// The txn-internal key should still be accessible.
	v, err := s1.GetAt(ctx, txnKey, readTS)
	require.NoError(t, err, "txn-internal key should survive DEL_PREFIX")
	require.Equal(t, "commit-record", string(v))
}
