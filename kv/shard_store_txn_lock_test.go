package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestShardStoreGetAt_ReturnsTxnLockedForPendingLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
	}
	shardStore := NewShardStore(engine, groups)

	startTS := uint64(1)
	key := []byte("k")

	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v")},
		},
	}
	_, err := groups[1].Txn.Commit([]*pb.Request{prepare})
	require.NoError(t, err)

	_, err = shardStore.GetAt(ctx, key, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked, got %v", err)
}

func TestShardStoreGetAt_ResolvesCommittedSecondaryLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	st2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSM(st2))
	defer stop2()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
		2: {Raft: r2, Store: st2, Txn: NewLeaderProxy(r2)},
	}
	shardStore := NewShardStore(engine, groups)

	startTS := uint64(1)
	commitTS := uint64(2)

	primaryKey := []byte("b") // group 1
	secondaryKey := []byte("x")

	prepareMeta := func() *pb.Mutation {
		return &pb.Mutation{
			Op:    pb.Op_PUT,
			Key:   []byte(txnMetaPrefix),
			Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0}),
		}
	}

	preparePrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta(),
			{Op: pb.Op_PUT, Key: primaryKey, Value: []byte("v1")},
		},
	}
	prepareSecondary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta(),
			{Op: pb.Op_PUT, Key: secondaryKey, Value: []byte("v2")},
		},
	}

	_, err := groups[1].Txn.Commit([]*pb.Request{preparePrimary})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{prepareSecondary})
	require.NoError(t, err)

	commitPrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: 0, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: primaryKey},
		},
	}
	_, err = groups[1].Txn.Commit([]*pb.Request{commitPrimary})
	require.NoError(t, err)

	// Reading the secondary key should resolve it based on the primary commit record.
	v, err := shardStore.GetAt(ctx, secondaryKey, commitTS)
	require.NoError(t, err)
	require.Equal(t, "v2", string(v))
}

func TestShardStoreScanAt_ReturnsTxnLockedForPendingLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
	}
	shardStore := NewShardStore(engine, groups)

	key := []byte("k")
	require.NoError(t, st1.PutAt(ctx, key, []byte("old"), 1, 0))

	startTS := uint64(2)
	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v")},
		},
	}
	_, err := groups[1].Txn.Commit([]*pb.Request{prepare})
	require.NoError(t, err)

	_, err = shardStore.ScanAt(ctx, []byte(""), nil, 100, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked, got %v", err)
}

func TestShardStoreScanAt_ReturnsTxnLockedForPendingLockWithoutCommittedValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
	}
	shardStore := NewShardStore(engine, groups)

	key := []byte("k")
	startTS := uint64(1)
	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v")},
		},
	}
	_, err := groups[1].Txn.Commit([]*pb.Request{prepare})
	require.NoError(t, err)

	// User-key range does not include raw !txn|lock|... keys, so lock-only
	// pending writes must still be detected through lock-range scanning.
	_, err = shardStore.ScanAt(ctx, []byte("k"), []byte("l"), 100, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked, got %v", err)
}

func TestShardStoreScanAt_ReturnsTxnLockedWhenPendingLockExceedsUserLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
	}
	shardStore := NewShardStore(engine, groups)

	// A normal committed user key so ScanAt can return data if lock checks miss.
	require.NoError(t, st1.PutAt(ctx, []byte("c"), []byte("visible"), 1, 0))

	committedPrimary := []byte("p")
	committedSecondary := []byte("a")
	committedStartTS := uint64(2)
	committedCommitTS := uint64(3)
	prepareCommitted := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    committedStartTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: committedPrimary, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
			{Op: pb.Op_PUT, Key: committedPrimary, Value: []byte("vp")},
			{Op: pb.Op_PUT, Key: committedSecondary, Value: []byte("va")},
		},
	}
	_, err := groups[1].Txn.Commit([]*pb.Request{prepareCommitted})
	require.NoError(t, err)
	commitCommittedPrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    committedStartTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: committedPrimary, CommitTS: committedCommitTS})},
			{Op: pb.Op_PUT, Key: committedPrimary},
		},
	}
	_, err = groups[1].Txn.Commit([]*pb.Request{commitCommittedPrimary})
	require.NoError(t, err)

	// Create a later pending lock-only write that must block the scan.
	pendingPrimary := []byte("b")
	pendingStartTS := uint64(4)
	preparePending := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    pendingStartTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: pendingPrimary, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
			{Op: pb.Op_PUT, Key: pendingPrimary, Value: []byte("vb")},
		},
	}
	_, err = groups[1].Txn.Commit([]*pb.Request{preparePending})
	require.NoError(t, err)

	// limit=1 should not hide pending locks after one resolved lock.
	_, err = shardStore.ScanAt(ctx, []byte("a"), []byte("z"), 1, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked, got %v", err)
}

func TestShardStoreScanAt_ResolvesCommittedSecondaryLocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	st2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSM(st2))
	defer stop2()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
		2: {Raft: r2, Store: st2, Txn: NewLeaderProxy(r2)},
	}
	shardStore := NewShardStore(engine, groups)

	startTS := uint64(2)
	commitTS := uint64(3)

	primaryKey := []byte("b")
	secondaryKey1 := []byte("x")
	secondaryKey2 := []byte("y")
	require.NoError(t, st2.PutAt(ctx, secondaryKey1, []byte("old2"), 1, 0))
	require.NoError(t, st2.PutAt(ctx, secondaryKey2, []byte("old3"), 1, 0))

	prepareMeta := func() *pb.Mutation {
		return &pb.Mutation{
			Op:    pb.Op_PUT,
			Key:   []byte(txnMetaPrefix),
			Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0}),
		}
	}

	preparePrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta(),
			{Op: pb.Op_PUT, Key: primaryKey, Value: []byte("v1")},
		},
	}
	prepareSecondary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta(),
			{Op: pb.Op_PUT, Key: secondaryKey1, Value: []byte("v2")},
			{Op: pb.Op_PUT, Key: secondaryKey2, Value: []byte("v3")},
		},
	}

	_, err := groups[1].Txn.Commit([]*pb.Request{preparePrimary})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{prepareSecondary})
	require.NoError(t, err)

	commitPrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: 0, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: primaryKey},
		},
	}
	_, err = groups[1].Txn.Commit([]*pb.Request{commitPrimary})
	require.NoError(t, err)

	kvs, err := shardStore.ScanAt(ctx, []byte("w"), nil, 100, commitTS)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	got := map[string]string{}
	for _, kvp := range kvs {
		got[string(kvp.Key)] = string(kvp.Value)
	}
	require.Equal(t, "v2", got[string(secondaryKey1)])
	require.Equal(t, "v3", got[string(secondaryKey2)])
}

func TestShardStoreScanAt_ResolvesCommittedSecondaryLockWithoutCommittedValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	st2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSM(st2))
	defer stop2()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
		2: {Raft: r2, Store: st2, Txn: NewLeaderProxy(r2)},
	}
	shardStore := NewShardStore(engine, groups)

	startTS := uint64(1)
	commitTS := uint64(2)
	primaryKey := []byte("b")
	secondaryKey := []byte("x")

	prepareMeta := func() *pb.Mutation {
		return &pb.Mutation{
			Op:    pb.Op_PUT,
			Key:   []byte(txnMetaPrefix),
			Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0}),
		}
	}

	preparePrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta(),
			{Op: pb.Op_PUT, Key: primaryKey, Value: []byte("v1")},
		},
	}
	prepareSecondary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta(),
			{Op: pb.Op_PUT, Key: secondaryKey, Value: []byte("v2")},
		},
	}
	_, err := groups[1].Txn.Commit([]*pb.Request{preparePrimary})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{prepareSecondary})
	require.NoError(t, err)

	commitPrimary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: 0, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: primaryKey},
		},
	}
	_, err = groups[1].Txn.Commit([]*pb.Request{commitPrimary})
	require.NoError(t, err)

	kvs, err := shardStore.ScanAt(ctx, []byte("x"), []byte("z"), 100, commitTS)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, secondaryKey, kvs[0].Key)
	require.Equal(t, []byte("v2"), kvs[0].Value)
}

func TestShardStoreScanAt_FiltersTxnInternalKeysWithoutRaft(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st1 := store.NewMVCCStore()
	groups := map[uint64]*ShardGroup{
		1: {Store: st1},
	}
	shardStore := NewShardStore(engine, groups)

	userKey := []byte("k")
	require.NoError(t, st1.PutAt(ctx, txnLockKey(userKey), []byte("lock"), 1, 0))
	require.NoError(t, st1.PutAt(ctx, userKey, []byte("v"), 1, 0))

	kvs, err := shardStore.ScanAt(ctx, []byte(""), nil, 100, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, userKey, kvs[0].Key)
	require.Equal(t, []byte("v"), kvs[0].Value)
}
