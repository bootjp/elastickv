package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/pebble/v2"
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
	r1, stop1 := newSingleRaft(t, "lr-g1", NewKvFSMWithHLC(st1, NewHLC()))
	st2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "lr-g2", NewKvFSMWithHLC(st2, NewHLC()))

	e1 := r1
	e2 := r2
	groups := map[uint64]*ShardGroup{
		1: {Engine: e1, Store: st1, Txn: NewLeaderProxyWithEngine(e1)},
		2: {Engine: e2, Store: st2, Txn: NewLeaderProxyWithEngine(e2)},
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
	_, err := g.Txn.Commit(context.Background(), []*pb.Request{{
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
	_, err := g.Txn.Commit(context.Background(), []*pb.Request{{
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

func TestLockResolver_ResolvesCommittedLockWhenPrimaryGroupBackpressured(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(10)
	commitTS := uint64(20)
	primaryKey := []byte("b")   // group 1
	secondaryKey := []byte("n") // group 2

	prepareLock(t, groups[1], startTS, primaryKey, primaryKey, []byte("v1"), 0)
	prepareLock(t, groups[2], startTS, secondaryKey, primaryKey, []byte("v2"), 0)
	commitPrimary(t, groups[1], startTS, commitTS, primaryKey)

	metrics := &pebble.Metrics{}
	metrics.Levels[0].Sublevels = lockResolverMaxL0Sublevels
	groups[1].Store = &lockResolverBackpressureStore{MVCCStore: groups[1].Store, metrics: metrics}

	err := lr.resolveGroupLocks(ctx, 2, groups[2])
	require.NoError(t, err)

	v, err := ss.GetAt(ctx, secondaryKey, commitTS)
	require.NoError(t, err)
	require.Equal(t, "v2", string(v))
}

func TestLockResolver_ResolvesExpiredCommittedPrimaryLock(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(11)
	commitTS := uint64(21)
	primaryKey := []byte("b") // group 1

	prepareLock(t, groups[1], startTS, primaryKey, primaryKey, []byte("v1"), 60_000)
	commitPrimary(t, groups[1], startTS, commitTS, primaryKey)

	// Recreate the leaked-primary-lock state: the commit record exists, but the
	// primary lock is still visible and already expired.
	staleLock := encodeTxnLock(txnLock{
		StartTS:      startTS,
		TTLExpireAt:  1,
		PrimaryKey:   primaryKey,
		IsPrimaryKey: true,
	})
	require.NoError(t, groups[1].Store.PutAt(ctx, txnLockKey(primaryKey), staleLock, commitTS, 0))

	err := lr.resolveGroupLocks(ctx, 1, groups[1])
	require.NoError(t, err)

	_, err = groups[1].Store.GetAt(ctx, txnLockKey(primaryKey), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	v, err := ss.GetAt(ctx, primaryKey, commitTS)
	require.NoError(t, err)
	require.Equal(t, "v1", string(v))
}

func TestLockResolver_ResolvesExpiredRolledBackLock(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(20)
	primaryKey := []byte("b")   // group 1
	secondaryKey := []byte("n") // group 2

	// Prepare on both shards with TTL=0 (immediately expired).
	prepareLock(t, groups[1], startTS, primaryKey, primaryKey, []byte("v1"), 0)
	prepareLock(t, groups[2], startTS, secondaryKey, primaryKey, []byte("v2"), 0)

	// Abort the primary.
	_, err := groups[1].Txn.Commit(context.Background(), []*pb.Request{{
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

func TestLockResolver_SkipsPendingPrimaryAbortWhenPrimaryGroupBackpressured(t *testing.T) {
	t.Parallel()

	lr, _, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(25)
	primaryKey := []byte("b")   // group 1
	secondaryKey := []byte("n") // group 2

	prepareLock(t, groups[1], startTS, primaryKey, primaryKey, []byte("v1"), 0)
	prepareLock(t, groups[2], startTS, secondaryKey, primaryKey, []byte("v2"), 0)

	metrics := &pebble.Metrics{}
	metrics.Levels[0].Sublevels = lockResolverMaxL0Sublevels
	groups[1].Store = &lockResolverBackpressureStore{MVCCStore: groups[1].Store, metrics: metrics}

	err := lr.resolveGroupLocks(ctx, 2, groups[2])
	require.NoError(t, err)

	_, err = groups[2].Store.GetAt(ctx, txnLockKey(secondaryKey), ^uint64(0))
	require.NoError(t, err)
}

func TestLockResolver_SkipsNonExpiredLocks(t *testing.T) {
	t.Parallel()

	lr, ss, groups, cleanup := setupLockResolverEnv(t)
	defer cleanup()

	ctx := context.Background()
	startTS := uint64(30)
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
	r, stop := newSingleRaft(t, "lr-leader", NewKvFSMWithHLC(st, NewHLC()))
	defer stop()

	e := r
	groups := map[uint64]*ShardGroup{
		1: {Engine: e, Store: st, Txn: NewLeaderProxyWithEngine(e)},
	}
	ss := NewShardStore(engine, groups)
	lr := NewLockResolver(ss, groups, nil)
	defer lr.Close()

	startTS := uint64(40)
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
	r, stop := newSingleRaft(t, "lr-close", NewKvFSMWithHLC(st, NewHLC()))
	defer stop()

	e := r
	groups := map[uint64]*ShardGroup{
		1: {Engine: e, Store: st, Txn: NewLeaderProxyWithEngine(e)},
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

func TestLockResolverRaftReadyRequiresSettledLeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine any
		want   bool
	}{
		{name: "nil engine", engine: nil},
		{
			name: "follower",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateFollower,
				status: raftengine.Status{State: raftengine.StateFollower, CommitIndex: 10, AppliedIndex: 10},
			},
		},
		{
			name: "state/status mismatch fails closed",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateLeader,
				status: raftengine.Status{State: raftengine.StateFollower, CommitIndex: 10, AppliedIndex: 10},
			},
		},
		{
			name: "leader transfer in progress",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateLeader,
				status: raftengine.Status{State: raftengine.StateLeader, CommitIndex: 10, AppliedIndex: 10, LeadTransferee: 2},
			},
		},
		{
			name: "pending conf change",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateLeader,
				status: raftengine.Status{State: raftengine.StateLeader, CommitIndex: 10, AppliedIndex: 10, PendingConfChange: true},
			},
		},
		{
			name: "fsm backlog",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateLeader,
				status: raftengine.Status{State: raftengine.StateLeader, CommitIndex: 10, AppliedIndex: 10, FSMPending: 1},
			},
		},
		{
			name: "applied lag",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateLeader,
				status: raftengine.Status{State: raftengine.StateLeader, CommitIndex: 10, AppliedIndex: 9},
			},
		},
		{
			name: "settled leader",
			engine: lockResolverStatusEngine{
				state:  raftengine.StateLeader,
				status: raftengine.Status{State: raftengine.StateLeader, CommitIndex: 10, AppliedIndex: 10},
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine, _ := tc.engine.(interface {
				raftengine.LeaderView
				raftengine.StatusReader
			})
			require.Equal(t, tc.want, lockResolverRaftReady(engine))
		})
	}
}

func TestLockResolverLSMBackpressured(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	tests := []struct {
		name  string
		setup func(*pebble.Metrics)
		want  bool
	}{
		{name: "no pressure", setup: func(*pebble.Metrics) {}, want: false},
		{
			name: "stable wide l0 and debt without active compaction",
			setup: func(m *pebble.Metrics) {
				m.Levels[0].Sublevels = 1
				m.Levels[0].TablesCount = lockResolverMaxL0Files * 2
				m.Compact.EstimatedDebt = lockResolverMaxLSMDebtBytes * 2
			},
			want: false,
		},
		{
			name: "l0 sublevels at threshold",
			setup: func(m *pebble.Metrics) {
				m.Levels[0].Sublevels = lockResolverMaxL0Sublevels
			},
			want: true,
		},
		{
			name: "l0 files at threshold with active compaction",
			setup: func(m *pebble.Metrics) {
				m.Levels[0].TablesCount = lockResolverMaxL0Files
				m.Compact.NumInProgress = 1
			},
			want: true,
		},
		{
			name: "compaction debt at threshold without active compaction",
			setup: func(m *pebble.Metrics) {
				m.Compact.EstimatedDebt = lockResolverMaxLSMDebtBytes
			},
			want: false,
		},
		{
			name: "compaction debt at threshold with active compaction",
			setup: func(m *pebble.Metrics) {
				m.Compact.EstimatedDebt = lockResolverMaxLSMDebtBytes
				m.Compact.NumInProgress = 1
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &pebble.Metrics{}
			tc.setup(metrics)
			st := &lockResolverBackpressureStore{MVCCStore: base, metrics: metrics}
			overloaded, _ := lockResolverLSMBackpressured(st)
			require.Equal(t, tc.want, overloaded)
		})
	}
}

type lockResolverStatusEngine struct {
	state  raftengine.State
	status raftengine.Status
}

func (e lockResolverStatusEngine) State() raftengine.State { return e.state }

func (e lockResolverStatusEngine) Leader() raftengine.LeaderInfo { return e.status.Leader }

func (e lockResolverStatusEngine) VerifyLeader(context.Context) error { return nil }

func (e lockResolverStatusEngine) LinearizableRead(context.Context) (uint64, error) {
	return e.status.AppliedIndex, nil
}

func (e lockResolverStatusEngine) Status() raftengine.Status { return e.status }

type lockResolverBackpressureStore struct {
	store.MVCCStore
	metrics *pebble.Metrics
}

func (s *lockResolverBackpressureStore) Metrics() *pebble.Metrics {
	return s.metrics
}
