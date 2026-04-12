package kv

import (
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func setupTwoShardStore(t *testing.T) (*ShardStore, map[uint64]*ShardGroup, func()) {
	t.Helper()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))

	st2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSM(st2))

	e1 := hashicorpraftengine.New(r1)
	e2 := hashicorpraftengine.New(r2)
	groups := map[uint64]*ShardGroup{
		1: {Engine: e1, Store: st1, Txn: NewLeaderProxyWithEngine(e1)},
		2: {Engine: e2, Store: st2, Txn: NewLeaderProxyWithEngine(e2)},
	}
	shardStore := NewShardStore(engine, groups)

	return shardStore, groups, func() {
		stop1()
		stop2()
	}
}

func makePrepareRequest(startTS uint64, key, value, primaryKey []byte) *pb.Request {
	return &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0}),
			},
			{Op: pb.Op_PUT, Key: key, Value: value},
		},
	}
}

func TestLockResolutionForStatus_RolledBackTimestampOverflow(t *testing.T) {
	t.Parallel()

	phase, ts, err := lockResolutionForStatus(
		lockTxnStatus{status: txnStatusRolledBack},
		txnLock{StartTS: math.MaxUint64},
		[]byte("k"),
		0,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked))
	require.Equal(t, pb.Phase_NONE, phase)
	require.Zero(t, ts)
}

func TestLockResolutionForStatus_RolledBackUsesCleanupNowWhenAhead(t *testing.T) {
	t.Parallel()

	phase, ts, err := lockResolutionForStatus(
		lockTxnStatus{status: txnStatusRolledBack},
		txnLock{StartTS: 10},
		[]byte("k"),
		200,
	)
	require.NoError(t, err)
	require.Equal(t, pb.Phase_ABORT, phase)
	require.Equal(t, uint64(200), ts)
}

func TestShardStoreGetAt_ReturnsTxnLockedForPendingLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(st1))
	defer stop1()

	groups := map[uint64]*ShardGroup{
		1: {Engine: hashicorpraftengine.New(r1), Store: st1, Txn: NewLeaderProxyWithEngine(hashicorpraftengine.New(r1))},
	}
	shardStore := NewShardStore(engine, groups)

	startTS := uint64(1)
	key := []byte("k")

	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, key, []byte("v"), key)})
	require.NoError(t, err)

	_, err = shardStore.GetAt(ctx, key, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked, got %v", err)
}

func TestShardStoreGetAt_ReturnsTxnLockedForPendingCrossShardTxn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	shardStore, groups, cleanup := setupTwoShardStore(t)
	defer cleanup()

	startTS := uint64(1)
	primaryKey := []byte("b")
	secondaryKey := []byte("x")

	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, primaryKey, []byte("v1"), primaryKey)})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, secondaryKey, []byte("v2"), primaryKey)})
	require.NoError(t, err)

	_, err = shardStore.GetAt(ctx, primaryKey, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked for primary key, got %v", err)

	_, err = shardStore.GetAt(ctx, secondaryKey, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked for secondary key, got %v", err)
}

func TestShardStoreGetAt_ResolvesCommittedSecondaryLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	shardStore, groups, cleanup := setupTwoShardStore(t)
	defer cleanup()

	startTS := uint64(1)
	commitTS := uint64(2)

	primaryKey := []byte("b") // group 1
	secondaryKey := []byte("x")

	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, primaryKey, []byte("v1"), primaryKey)})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, secondaryKey, []byte("v2"), primaryKey)})
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

func TestShardStoreScanAt_ResolvesCommittedCrossShardTxn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	shardStore, groups, cleanup := setupTwoShardStore(t)
	defer cleanup()

	startTS := uint64(2)
	commitTS := uint64(3)
	primaryKey := []byte("b")
	secondaryKey := []byte("x")

	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, primaryKey, []byte("v1"), primaryKey)})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, secondaryKey, []byte("v2"), primaryKey)})
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

	kvs, err := shardStore.ScanAt(ctx, []byte("a"), []byte("z"), 100, commitTS)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	got := map[string]string{}
	for _, kvp := range kvs {
		got[string(kvp.Key)] = string(kvp.Value)
	}
	require.Equal(t, "v1", got[string(primaryKey)])
	require.Equal(t, "v2", got[string(secondaryKey)])

	_, err = groups[2].Store.GetAt(ctx, txnLockKey(secondaryKey), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
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
		1: {Engine: hashicorpraftengine.New(r1), Store: st1, Txn: NewLeaderProxyWithEngine(hashicorpraftengine.New(r1))},
	}
	shardStore := NewShardStore(engine, groups)

	key := []byte("k")
	require.NoError(t, st1.PutAt(ctx, key, []byte("old"), 1, 0))

	startTS := uint64(2)
	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, key, []byte("v"), key)})
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
		1: {Engine: hashicorpraftengine.New(r1), Store: st1, Txn: NewLeaderProxyWithEngine(hashicorpraftengine.New(r1))},
	}
	shardStore := NewShardStore(engine, groups)

	key := []byte("k")
	startTS := uint64(1)
	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, key, []byte("v"), key)})
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
		1: {Engine: hashicorpraftengine.New(r1), Store: st1, Txn: NewLeaderProxyWithEngine(hashicorpraftengine.New(r1))},
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
	_, err = groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(pendingStartTS, pendingPrimary, []byte("vb"), pendingPrimary)})
	require.NoError(t, err)

	// limit=1 should not hide pending locks after one resolved lock.
	_, err = shardStore.ScanAt(ctx, []byte("a"), []byte("z"), 1, ^uint64(0))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTxnLocked), "expected ErrTxnLocked, got %v", err)
}

func TestShardStoreScanAt_ResolvesCommittedSecondaryLocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	shardStore, groups, cleanup := setupTwoShardStore(t)
	defer cleanup()

	startTS := uint64(2)
	commitTS := uint64(3)

	primaryKey := []byte("b")
	secondaryKey1 := []byte("x")
	secondaryKey2 := []byte("y")
	require.NoError(t, groups[2].Store.PutAt(ctx, secondaryKey1, []byte("old2"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, secondaryKey2, []byte("old3"), 1, 0))

	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, primaryKey, []byte("v1"), primaryKey)})
	require.NoError(t, err)

	prepareMeta := &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0}),
	}
	prepareSecondary := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			prepareMeta,
			{Op: pb.Op_PUT, Key: secondaryKey1, Value: []byte("v2")},
			{Op: pb.Op_PUT, Key: secondaryKey2, Value: []byte("v3")},
		},
	}
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

	shardStore, groups, cleanup := setupTwoShardStore(t)
	defer cleanup()

	startTS := uint64(1)
	commitTS := uint64(2)
	primaryKey := []byte("b")
	secondaryKey := []byte("x")

	_, err := groups[1].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, primaryKey, []byte("v1"), primaryKey)})
	require.NoError(t, err)
	_, err = groups[2].Txn.Commit([]*pb.Request{makePrepareRequest(startTS, secondaryKey, []byte("v2"), primaryKey)})
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
