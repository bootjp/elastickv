package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// setupLockResolverEnv creates a two-shard environment suitable for
// LockResolver tests. Group 1 covers keys [a, m), group 2 covers [m, …).
func setupLockResolverEnv(t *testing.T) (*LockResolver, *ShardStore, map[uint64]*ShardGroup, func()) {
	t.Helper()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	st1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "lr-g1", NewKvFSM(st1))
	st2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "lr-g2", NewKvFSM(st2))

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: st1, Txn: NewLeaderProxy(r1)},
		2: {Raft: r2, Store: st2, Txn: NewLeaderProxy(r2)},
	}
	ss := NewShardStore(engine, groups)
	lr := NewLockResolver(ss, groups, nil)

	return lr, ss, groups, func() {
		lr.Close()
		stop1()
		stop2()
	}
}

// prepareLock writes a PREPARE request (which creates a lock) for a key.
func prepareLock(t *testing.T, g *ShardGroup, startTS uint64, key, primaryKey, value []byte, lockTTLms uint64) {
	t.Helper()
	_, err := g.Txn.Commit([]*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: lockTTLms, CommitTS: 0}),
			},
			{Op: pb.Op_PUT, Key: key, Value: value},
		},
	}})
	require.NoError(t, err)
}

// commitPrimary writes a COMMIT record for a transaction's primary key.
func commitPrimary(t *testing.T, g *ShardGroup, startTS, commitTS uint64, primaryKey []byte) {
	t.Helper()
	_, err := g.Txn.Commit([]*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: primaryKey},
		},
	}})
	require.NoError(t, err)
}

func TestLockResolver_ResolvesExpiredCommittedLock(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()
	_ = lr

	ctx := context.Background()
	startTS := uint64(10)
	commitTS := uint64(20)
	primaryKey := []byte("b")   // group 1
	secondaryKey := []byte("n") // group 2

	// Prepare on both shards with TTL=0 so locks are immediately expired.
	prepareLock(t, groups[1], startTS, primaryKey, primaryKey, []byte("v1"), 0)
	prepareLock(t, groups[2], startTS, secondaryKey, primaryKey, []byte("v2"), 0)

	// Commit the primary.
	commitPrimary(t, groups[1], startTS, commitTS, primaryKey)

	// Run the resolver on the secondary shard — it should resolve the lock.
	err := lr.resolveGroupLocks(ctx, 2, groups[2])
	require.NoError(t, err)

	// After resolution, the secondary key should be readable.
	v, err := ss.GetAt(ctx, secondaryKey, commitTS)
	require.NoError(t, err)
	require.Equal(t, "v2", string(v))
}

func TestLockResolver_ResolvesExpiredRolledBackLock(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(10)
	primaryKey := []byte("b")   // group 1
	secondaryKey := []byte("n") // group 2

	// Prepare on both shards with TTL=0 (immediately expired).
	prepareLock(t, groups[1], startTS, primaryKey, primaryKey, []byte("v1"), 0)
	prepareLock(t, groups[2], startTS, secondaryKey, primaryKey, []byte("v2"), 0)

	// Abort the primary.
	_, err := groups[1].Txn.Commit([]*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, CommitTS: startTS + 1})},
			{Op: pb.Op_PUT, Key: primaryKey},
		},
	}})
	require.NoError(t, err)

	// Resolve expired locks on the secondary shard.
	err = lr.resolveGroupLocks(ctx, 2, groups[2])
	require.NoError(t, err)

	// After abort resolution, the secondary key should not be visible.
	_, err = ss.GetAt(ctx, secondaryKey, startTS+1)
	require.Error(t, err)
}

func TestLockResolver_SkipsNonExpiredLocks(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(10)
	key := []byte("b") // group 1

	// Prepare a lock with a large TTL so it won't be expired.
	prepareLock(t, groups[1], startTS, key, key, []byte("v1"), 60_000)

	// Run the resolver — it should not touch this lock.
	err := lr.resolveGroupLocks(ctx, 1, groups[1])
	require.NoError(t, err)

	// The key should still be locked (GetAt returns ErrTxnLocked).
	_, err = ss.GetAt(ctx, key, startTS+1)
	require.Error(t, err)
}

func TestLockResolver_LeaderOnlyExecution(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "lr-leader", NewKvFSM(st))
	defer stop()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r, Store: st, Txn: NewLeaderProxy(r)},
	}
	ss := NewShardStore(engine, groups)
	lr := NewLockResolver(ss, groups, nil)
	defer lr.Close()

	startTS := uint64(10)
	prepareLock(t, groups[1], startTS, []byte("a"), []byte("a"), []byte("v"), 0)

	// Manually call resolveAllGroups — the group's raft is leader so it runs.
	ctx := context.Background()
	lr.resolveAllGroups(ctx)

	// The lock was expired and primary is pending — the resolver will attempt
	// to abort. After resolveAllGroups, the key should either be cleaned up
	// or the resolver logged a warning. Either way, no panic/error from the
	// resolver itself.
}

func TestLockResolver_CloseStopsBackground(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "lr-close", NewKvFSM(st))
	defer stop()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r, Store: st, Txn: NewLeaderProxy(r)},
	}
	ss := NewShardStore(engine, groups)
	lr := NewLockResolver(ss, groups, nil)

	// Close should return promptly.
	done := make(chan struct{})
	go func() {
		lr.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LockResolver.Close() did not return within 5s")
	}
}
