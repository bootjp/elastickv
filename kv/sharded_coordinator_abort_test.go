package kv

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// failingTransactional is a stub Transactional that always returns an error on Commit.
type failingTransactional struct {
	err error
}

func (f *failingTransactional) Commit([]*pb.Request) (*TransactionResponse, error) {
	return nil, f.err
}

func (f *failingTransactional) Abort([]*pb.Request) (*TransactionResponse, error) {
	return nil, f.err
}

// TestShardedAbortRollback_PrepareFailOnShard2_CleansShard1Locks verifies
// that when a cross-shard transaction's Prepare succeeds on Shard1 but fails
// on Shard2, the coordinator aborts Shard1 and its locks/intents are cleaned up.
func TestShardedAbortRollback_PrepareFailOnShard2_CleansShard1Locks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Route: keys [a, m) -> group 1, keys [m, ...) -> group 2.
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	// Group 1: real raft + real store.
	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "abort-g1", NewKvFSM(s1))
	t.Cleanup(stop1)

	// Group 2: real store (needed for routing) but a Transactional that always fails.
	s2 := store.NewMVCCStore()
	failTxn := &failingTransactional{err: errors.New("simulated shard2 prepare failure")}

	groups := map[uint64]*ShardGroup{
		1: {Engine: hashicorpraftengine.New(r1), Store: s1, Txn: NewLeaderProxyWithEngine(hashicorpraftengine.New(r1))},
		2: {Store: s2, Txn: failTxn},
	}

	shardStore := NewShardStore(engine, groups)
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), shardStore)

	// Dispatch a cross-shard transaction: key "b" -> group 1, key "x" -> group 2.
	// Group IDs are sorted [1, 2], so group 1 is prepared first (succeeds),
	// then group 2 fails, triggering abortPreparedTxn on group 1.
	ops := &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("val-b")},
			{Op: Put, Key: []byte("x"), Value: []byte("val-x")},
		},
	}
	_, err := coord.Dispatch(ctx, ops)
	require.Error(t, err, "dispatch should fail because shard2 prepare fails")

	// Verify that Shard1's locks have been cleaned up by the abort.
	_, err = s1.GetAt(ctx, txnLockKey([]byte("b")), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "lock for key 'b' on shard1 should be deleted after abort")

	// Verify that Shard1's intents have been cleaned up.
	_, err = s1.GetAt(ctx, txnIntentKey([]byte("b")), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "intent for key 'b' on shard1 should be deleted after abort")

	// Verify that no user data was committed on shard1.
	_, err = s1.GetAt(ctx, []byte("b"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "user data for key 'b' should not exist after abort")

	// Shard2 should also have no data (it was never prepared).
	_, err = s2.GetAt(ctx, []byte("x"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "user data for key 'x' should not exist on shard2")
}

func TestAbortPreparedTxn_DoesNotWarnWhenTxnAlreadyCommitted(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	coord := &ShardedCoordinator{
		log: logger,
		groups: map[uint64]*ShardGroup{
			1: {Txn: &failingTransactional{err: errors.WithStack(ErrTxnAlreadyCommitted)}},
		},
	}

	coord.abortPreparedTxn(10, []byte("pk"), []preparedGroup{{
		gid:  1,
		keys: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("pk")}},
	}}, 20)

	require.Empty(t, buf.String())
}
