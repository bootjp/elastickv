package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func newCommitTSFenceFSM(t *testing.T) (*kvFSM, store.MVCCStore) {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	return fsm, st
}

func rawPut(ts uint64, key, value string) *pb.Request {
	return &pb.Request{Ts: ts, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: []byte(value)}}}
}

func prepareRequest(startTS, commitTS uint64, primary string, keys ...string) *pb.Request {
	muts := []*pb.Mutation{{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte(primary), LockTTLms: defaultTxnLockTTLms, CommitTS: commitTS}),
	}}
	for _, k := range keys {
		muts = append(muts, &pb.Mutation{Op: pb.Op_PUT, Key: []byte(k), Value: []byte("v")})
	}
	return &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: startTS, Mutations: muts}
}

func activateCommitTSFence(t *testing.T, fsm *kvFSM) {
	t.Helper()
	require.Nil(t, fsm.Apply(EncodeCommitTSFenceActivation()))
}

// Two raw writes allocated c1 < c2 apply in the opposite order. A reader
// at the watermark c2 misses c1's key, and c1 then lands below that
// snapshot (G0 on the raw path). The default build, without the fence
// activation, still admits it; this pins the gap the activation closes.
func TestRawApplyOutOfOrderLandsBelowSnapshotWithoutFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fsm, st := newCommitTSFenceFSM(t)

	require.NoError(t, applyFSMRequest(t, fsm, rawPut(20, "a", "x")))
	s := st.LastCommitTS()
	_, err := st.GetAt(ctx, []byte("b"), s)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	require.NoError(t, applyFSMRequest(t, fsm, rawPut(10, "b", "late")))
	got, err := st.GetAt(ctx, []byte("b"), s)
	require.NoError(t, err)
	require.Equal(t, "late", string(got), "without the fence the late raw write lands below the snapshot")
}

// Once the activation has applied, the late raw write is rejected as a
// write conflict and the snapshot at the watermark stays stable.
func TestRawApplyFencedOnceActivated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fsm, st := newCommitTSFenceFSM(t)
	activateCommitTSFence(t, fsm)

	require.NoError(t, applyFSMRequest(t, fsm, rawPut(20, "a", "x")))
	s := st.LastCommitTS()

	err := applyFSMRequest(t, fsm, rawPut(10, "b", "late"))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	_, err = st.GetAt(ctx, []byte("b"), s)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "the fenced raw write must not land below the snapshot")

	// A write above the watermark still applies.
	require.NoError(t, applyFSMRequest(t, fsm, rawPut(s+1, "b", "ok")))
}

// A PREPARE whose settled commit timestamp is at or below the watermark is
// rejected once the fence is active, and installs no lock; above it, the
// PREPARE applies.
func TestPrepareFencedOnceActivated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fsm, st := newCommitTSFenceFSM(t)

	require.NoError(t, applyFSMRequest(t, fsm, rawPut(20, "a", "x")))
	// Default build: the stale PREPARE applies.
	require.NoError(t, applyFSMRequest(t, fsm, prepareRequest(5, 15, "p1", "p1")))

	activateCommitTSFence(t, fsm)
	err := applyFSMRequest(t, fsm, prepareRequest(6, 18, "p2", "p2"))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	_, err = st.GetAt(ctx, txnLockKey([]byte("p2")), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "a fenced PREPARE must not install a lock")

	require.NoError(t, applyFSMRequest(t, fsm, prepareRequest(7, 25, "p3", "p3")))
}

func TestPreparedEvidenceCommitTimestampIsFenced(t *testing.T) {
	t.Parallel()
	fsm, st := newCommitTSFenceFSM(t)
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	activateCommitTSFence(t, fsm)
	require.NoError(t, applyFSMRequest(t, fsm, rawPut(20, "watermark", "v")))

	for _, hint := range []uint64{0, 999} {
		stale := preparedEvidenceRequest(pb.Phase_PREPARE, 18)
		meta, _, err := extractTxnMeta(stale.Mutations)
		require.NoError(t, err)
		meta.CommitTS = hint
		stale.Mutations[0].Value = EncodeTxnMeta(meta)
		err = applyFSMRequest(t, fsm, stale)
		require.ErrorIs(t, err, store.ErrWriteConflict)
		_, err = st.GetAt(t.Context(), txnLockKey([]byte("primary")), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}

	fresh := preparedEvidenceRequest(pb.Phase_PREPARE, 25)
	require.NoError(t, applyFSMRequest(t, fsm, fresh))
}

// The activation persists: a new FSM over the same store fences without
// seeing the entry again, and a replay of the entry is a no-op.
func TestCommitTSFenceActivationPersists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fsm, st := newCommitTSFenceFSM(t)

	enabled, err := CommitTSFenceEnabled(ctx, st)
	require.NoError(t, err)
	require.False(t, enabled)

	activateCommitTSFence(t, fsm)
	activateCommitTSFence(t, fsm)
	enabled, err = CommitTSFenceEnabled(ctx, st)
	require.NoError(t, err)
	require.True(t, enabled)

	reopened, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.True(t, reopened.commitTSFenceEnabled.Load())
	require.NoError(t, applyFSMRequest(t, reopened, rawPut(20, "a", "x")))
	require.ErrorIs(t, applyFSMRequest(t, reopened, rawPut(10, "b", "late")), store.ErrWriteConflict)
}

func TestCommitTSFenceActivationRejectsPayload(t *testing.T) {
	t.Parallel()
	fsm, _ := newCommitTSFenceFSM(t)
	resp := fsm.Apply([]byte{raftEncodeCommitTSFenceActivation, 1})
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrInvalidCommitTSFenceActivation)
	require.False(t, fsm.commitTSFenceEnabled.Load())
}

// A replica that cannot persist the activation halts rather than skip it.
func TestCommitTSFenceActivationHaltsOnPersistFailure(t *testing.T) {
	t.Parallel()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	fsm, ok := NewKvFSMWithHLC(failingRaftApplyStore{MVCCStore: st}, NewHLC()).(*kvFSM)
	require.True(t, ok)

	require.Error(t, haltApplyOf(fsm.Apply(EncodeCommitTSFenceActivation())))
	require.False(t, fsm.commitTSFenceEnabled.Load())
}

// The leader of a group proposes the activation when the coordinator is
// built with WithCommitTSFenceActivation, and stops once it has applied.
func TestShardedCoordinatorActivatesCommitTSFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	r, stop := newSingleRaft(t, "fence", NewKvFSMWithHLC(st, NewHLC()))
	t.Cleanup(stop)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  []distribution.RouteDescriptor{{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive}},
	}))
	groups := map[uint64]*ShardGroup{1: {Engine: r, Store: st, Txn: NewLeaderProxyWithEngine(r)}}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), nil).WithCommitTSFenceActivation()

	coord.activateCommitTSFences(ctx)
	require.Eventually(t, func() bool {
		enabled, err := CommitTSFenceEnabled(ctx, st)
		return err == nil && enabled
	}, 5*time.Second, 10*time.Millisecond)

	coord.activateCommitTSFences(ctx)
	_, done := coord.commitTSFenceActiveGroups.Load(uint64(1))
	require.True(t, done, "an activated group is remembered")
}

// Without WithCommitTSFenceActivation the coordinator proposes nothing.
func TestShardedCoordinatorCommitTSFenceActivationIsOptIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	r, stop := newSingleRaft(t, "nofence", NewKvFSMWithHLC(st, NewHLC()))
	t.Cleanup(stop)

	coord := NewShardedCoordinator(distribution.NewEngine(), map[uint64]*ShardGroup{
		1: {Engine: r, Store: st, Txn: NewLeaderProxyWithEngine(r)},
	}, 1, NewHLC(), nil)
	coord.activateCommitTSFencesAsync(ctx)
	time.Sleep(50 * time.Millisecond)
	enabled, err := CommitTSFenceEnabled(ctx, st)
	require.NoError(t, err)
	require.False(t, enabled)
}

// fencingOnceTransactional rejects the first raw commit as fenced and
// accepts the next, recording the timestamps it saw.
type fencingOnceTransactional struct {
	mu  sync.Mutex
	tss []uint64
}

func (s *fencingOnceTransactional) Commit(_ context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range reqs {
		s.tss = append(s.tss, r.GetTs())
	}
	if len(s.tss) == 1 {
		return nil, store.NewWriteConflictError([]byte("k"))
	}
	return &TransactionResponse{CommitIndex: uint64(len(s.tss))}, nil
}

func (s *fencingOnceTransactional) Abort(context.Context, []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

// A raw write the apply fenced is re-dispatched with a fresh timestamp, and
// the client sees the retry's success.
func TestShardedCoordinatorRetriesFencedRawWrite(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  []distribution.RouteDescriptor{{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive}},
	}))
	txn := &fencingOnceTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {Txn: txn}}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
	})
	require.NoError(t, err)
	require.Len(t, txn.tss, 2)
	require.Greater(t, txn.tss[1], txn.tss[0], "the retry must carry a fresh, higher timestamp")
}
