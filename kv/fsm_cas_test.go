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

func applyDecodedCASBatch(t *testing.T, f *kvFSM, reqs []*pb.Request) any {
	t.Helper()
	return f.applyCASEntry(t.Context(), reqs, false)
}

func casApplyRequest(startTS, commitTS, expectedTS uint64, key, value string) *pb.Request {
	req := onePhaseReq(startTS, commitTS, 0, []byte(key), []byte(value))
	req.Mutations[1].Op = pb.Op_COMPARE_AND_SWAP
	req.Mutations[1].ExpectedCommitTs = expectedTS
	return req
}

func applyCASFrame(t *testing.T, f *kvFSM, index uint64, req *pb.Request) any {
	t.Helper()
	payload, err := marshalRaftCommand([]*pb.Request{req})
	require.NoError(t, err)
	f.SetApplyIndex(index)
	return f.Apply(payload)
}

// The initial rollout excludes migration ranges before the predicate. This
// helper exercises the shared atomic apply path with migration evidence armed.
func applyCASInScope(t *testing.T, f *kvFSM, index uint64, req *pb.Request) any {
	t.Helper()
	f.SetApplyIndex(index)
	return applyDecodedCASBatch(t, f, []*pb.Request{req})
}

func enableCASApplyForTest(t *testing.T, f *kvFSM) {
	t.Helper()
	activateCommitTSFence(t, f)
	f.SetApplyIndex(1)
	require.Nil(t, f.Apply(EncodeMigrationVersionIdentityActivation()))
}

func TestFSMCASABAExpiryAndOrder(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"memory", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			var st store.MVCCStore
			if backend == "pebble" {
				var err error
				st, err = store.NewPebbleStore(t.TempDir())
				require.NoError(t, err)
			} else {
				st = store.NewMVCCStore()
			}
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
			require.True(t, ok)
			enableCASApplyForTest(t, f)

			ctx := t.Context()
			key := []byte("k")
			require.NoError(t, st.PutAt(ctx, key, []byte("A"), 10, 0))
			require.NoError(t, st.PutAt(ctx, key, []byte("B"), 20, 0))
			require.NoError(t, st.PutAt(ctx, key, []byte("A"), 30, 90))

			rejected := applyCASFrame(t, f, 2, casApplyRequest(31, 40, 10, "k", "old token"))
			requireCASRejection(t, rejected, ErrCASMismatch)
			require.NoError(t, st.PutAt(ctx, []byte("unrelated"), []byte("write"), 50, 0))
			rejected = applyCASFrame(t, f, 3, casApplyRequest(31, 45, 30, "k", "old timestamp"))
			requireCASRejection(t, rejected, store.ErrWriteConflict)
			rejected = applyCASFrame(t, f, 4, casApplyRequest(29, 60, 30, "k", "stale snapshot"))
			requireCASRejection(t, rejected, store.ErrWriteConflict)
			require.Nil(t, applyCASFrame(t, f, 5, casApplyRequest(60, 70, 30, "k", "A")))
			version, err := store.GetVersionAt(ctx, st, key, 89)
			require.NoError(t, err)
			require.Equal(t, store.VersionedValue{TS: 70, Value: []byte("A"), ExpireAt: 90}, version)
			rejected = applyCASFrame(t, f, 6, casApplyRequest(89, 90, 70, "k", "expired"))
			requireCASRejection(t, rejected, ErrCASMismatch)
			_, err = st.GetAt(ctx, key, 90)
			require.ErrorIs(t, err, store.ErrKeyNotFound)

			require.NoError(t, st.DeleteAt(ctx, key, 100))
			rejected = applyCASFrame(t, f, 7, casApplyRequest(101, 110, 100, "k", "tombstone"))
			requireCASRejection(t, rejected, ErrCASMismatch)
			require.NoError(t, st.PutAt(ctx, key, []byte("A"), 120, 0))
			rejected = applyCASFrame(t, f, 8, casApplyRequest(121, 130, 70, "k", "before delete"))
			requireCASRejection(t, rejected, ErrCASMismatch)
			rejected = applyCASFrame(t, f, 9, casApplyRequest(131, 135, 120, "missing", "must not create"))
			requireCASRejection(t, rejected, ErrCASMismatch)
			require.Nil(t, applyCASFrame(t, f, 10, casApplyRequest(136, 140, 120, "k", "")))
			version, err = store.GetVersionAt(ctx, st, key, 140)
			require.NoError(t, err)
			require.Equal(t, uint64(140), version.TS)
			require.Empty(t, version.Value)
		})
	}
}

func requireCASRejection(t *testing.T, resp any, want error) {
	t.Helper()
	got, ok := resp.(error)
	require.True(t, ok, "response = %v", resp)
	require.ErrorIs(t, got, want)
}

func TestFSMCASRejectsInvalidApplyShapes(t *testing.T) {
	t.Parallel()
	f, st := newCommitTSFenceFSM(t)
	require.NoError(t, st.PutAt(t.Context(), []byte("k"), []byte("original"), 10, 0))
	resp := applyCASFrame(t, f, 1, casApplyRequest(11, 20, 10, "k", "no fence"))
	_, halted := resp.(interface{ HaltApply() error })
	require.True(t, halted, "frame without the replicated fence returned %v", resp)
	enableCASApplyForTest(t, f)

	raw := casApplyRequest(11, 20, 10, "k", "raw")
	raw.IsTxn = false
	twoPhase := casApplyRequest(11, 20, 10, "k", "two phase")
	twoPhase.Phase = pb.Phase_PREPARE
	multiKey := casApplyRequest(11, 20, 10, "k", "multiple keys")
	multiKey.Mutations = append(multiKey.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte("other")})
	internalKey := casApplyRequest(11, 20, 10, string(txnLockKey([]byte("k"))), "internal")
	for i, invalid := range []*pb.Request{raw, twoPhase, multiKey, internalKey} {
		index := uint64(i + 2)
		f.SetApplyIndex(index)
		requireCASRejection(t, applyDecodedCASBatch(t, f, []*pb.Request{invalid}), ErrInvalidRequest)
		requireCASProgress(t, f, index)
	}
	got, err := st.GetAt(t.Context(), []byte("k"), 20)
	require.NoError(t, err)
	require.Equal(t, "original", string(got))
}

func TestFSMCASUsesStagedHeadAndLocks(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	route := distribution.RouteDescriptor{
		RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
		State: distribution.RouteStateActive, StagedVisibilityActive: true,
		MigrationJobID: 9, MinWriteTSExclusive: 20,
	}
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{route})
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(WrapDistributionEngine(engine), 1)).(*kvFSM)
	require.True(t, ok)
	enableCASApplyForTest(t, f)
	installStagedOwnershipEvidence(t, f, 2, route)

	ctx := t.Context()
	key := []byte("n")
	stagedKey := distribution.MigrationStagedDataKey(9, key)
	require.NoError(t, st.PutAt(ctx, key, []byte("old live"), 10, 0))
	require.NoError(t, st.PutAt(ctx, stagedKey, []byte("newer staged"), 30, 100))
	stale := casApplyRequest(40, 50, 10, "n", "stale")
	stale.ObservedRouteVersion = 2
	requireCASRejection(t, applyCASFrame(t, f, 4, stale), ErrVersionCASUnavailable)
	requireCASProgress(t, f, 4)
	requireCASRejection(t, applyCASInScope(t, f, 5, stale), ErrCASMismatch)

	stagedLockKey := distribution.MigrationStagedDataKey(9, txnLockKey(key))
	require.NoError(t, st.PutAt(ctx, stagedLockKey, encodeTxnLock(txnLock{
		StartTS: 60, CommitTS: 70, PrimaryKey: key, IsPrimaryKey: true,
	}), 60, 0))
	hit := casApplyRequest(61, 75, 30, "n", "new live")
	hit.ObservedRouteVersion = 2
	requireCASRejection(t, applyCASInScope(t, f, 6, hit), ErrTxnLocked)
	require.NoError(t, st.DeleteAt(ctx, stagedLockKey, 65))
	require.Nil(t, applyCASInScope(t, f, 7, hit))
	requireCASValue(t, st, 90, 75, "new live")
}

func TestFSMCASStagedContendersAndOperationalRollback(t *testing.T) {
	t.Parallel()
	f, st := newCommitTSFenceFSM(t)
	observer := &recordingApplyObserver{}
	f.applyObservers = append(f.applyObservers, observer)
	enableCASApplyForTest(t, f)
	require.NoError(t, st.PutAt(t.Context(), []byte("k"), []byte("original"), 10, 0))
	require.NoError(t, st.PutAt(t.Context(), txnLockKey([]byte("corrupt")), []byte("broken lock"), 11, 0))

	winner := casApplyRequest(15, 20, 10, "k", "winner")
	f.SetApplyIndex(2)
	resp := applyDecodedCASBatch(t, f, []*pb.Request{winner, casApplyRequest(25, 30, 10, "corrupt", "fail")})
	halt, ok := resp.(interface{ HaltApply() error })
	require.True(t, ok, "response = %v", resp)
	require.True(t, errors.Is(halt.HaltApply(), ErrRaftEntryApply), "%v", halt.HaltApply())
	require.Empty(t, observer.calls)
	requireCASProgress(t, f, 1)
	original, err := st.GetAt(t.Context(), []byte("k"), 30)
	require.NoError(t, err)
	require.Equal(t, "original", string(original))

	resp = applyDecodedCASBatch(t, f, []*pb.Request{winner,
		casApplyRequest(25, 30, 10, "k", "contender"),
		casApplyRequest(17, 19, 10, "missing", "out of order"),
		casApplyRequest(30, 40, 20, "k", "successor"),
	})
	batch, ok := resp.(*fsmApplyResponse)
	require.True(t, ok, "response = %v", resp)
	require.NoError(t, batch.results[0])
	require.ErrorIs(t, batch.results[1], ErrCASMismatch)
	require.ErrorIs(t, batch.results[2], store.ErrWriteConflict)
	require.NoError(t, batch.results[3])
	require.Equal(t, []recordedApply{{op: pb.Op_PUT, key: "k"}, {op: pb.Op_PUT, key: "k"}}, observer.calls)
	staged, err := store.GetVersionAt(t.Context(), st, []byte("k"), 40)
	require.NoError(t, err)
	require.Equal(t, store.VersionedValue{TS: 40, Value: []byte("successor")}, staged)
}

type pausingCASStore struct {
	store.MVCCStore
	store.RaftControlMetadataStore
	applier store.RaftEntryApplier
	staged  chan struct{}
	resume  chan struct{}
}

func (s pausingCASStore) ApplyRaftEntry(ctx context.Context, index uint64, fn func(context.Context) error) error {
	return s.applier.ApplyRaftEntry(ctx, index, func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			return err
		}
		close(s.staged)
		<-s.resume
		return nil
	})
}

func TestFSMCASSerializesWithConcurrentOrdinaryWriter(t *testing.T) {
	t.Parallel()
	f, st := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, f)
	ctx := t.Context()
	key := []byte("k")
	require.NoError(t, st.PutAt(ctx, key, []byte("original"), 10, 0))
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	meta, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	wrapped := pausingCASStore{
		MVCCStore: st, RaftControlMetadataStore: meta, applier: applier,
		staged: make(chan struct{}), resume: make(chan struct{}),
	}
	f.store = wrapped
	payload, err := marshalRaftCommand([]*pb.Request{casApplyRequest(10, 20, 10, "k", "winner")})
	require.NoError(t, err)
	f.SetApplyIndex(2)
	applied := make(chan any, 1)
	go func() { applied <- f.Apply(payload) }()
	select {
	case <-wrapped.staged:
	case response := <-applied:
		t.Fatalf("CAS finished without staging: %v", response)
	}

	started := make(chan struct{})
	written := make(chan error, 1)
	go func() {
		close(started)
		muts := []*store.KVPairMutation{{Op: store.OpTypePut, Key: key, Value: []byte("competing put")}}
		written <- st.ApplyMutations(ctx, muts, nil, 10, 30)
	}()
	<-started
	close(wrapped.resume)
	require.Nil(t, <-applied)
	require.ErrorIs(t, <-written, store.ErrWriteConflict)
	version, err := store.GetVersionAt(ctx, st, key, 30)
	require.NoError(t, err)
	require.Equal(t, store.VersionedValue{TS: 20, Value: []byte("winner")}, version)
}

func TestFSMCASMigrationTrackerAndWALReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	observer := &recordingApplyObserver{}
	open := func() (*kvFSM, store.MVCCStore) {
		st, err := store.NewPebbleStore(dir)
		require.NoError(t, err)
		f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(nil, 1), WithApplyObserver(observer)).(*kvFSM)
		require.True(t, ok)
		return f, st
	}
	f, st := open()
	enableCASApplyForTest(t, f)
	installSourceOwnershipForTest(t, f, 0)
	require.NoError(t, st.PutAt(t.Context(), []byte("n"), []byte("original"), 20, 100))
	miss := casApplyRequest(30, 40, 10, "n", "must not land")
	miss.ObservedRouteVersion = EncodeObservedRouteVersion(1)
	hit := casApplyRequest(41, 50, 20, "n", "replacement")
	hit.ObservedRouteVersion = miss.ObservedRouteVersion

	requireCASProgress(t, f, 11)
	require.Zero(t, migrationTrackerMinimum(t, f))
	requireCASRejection(t, applyCASInScope(t, f, 12, miss), ErrCASMismatch)
	requireCASProgress(t, f, 12)
	require.Zero(t, migrationTrackerMinimum(t, f))
	require.Empty(t, observer.calls)
	requireCASValue(t, st, 40, 20, "original")

	require.NoError(t, st.Close())
	f, st = open()
	requireCASProgress(t, f, 12)
	require.Zero(t, migrationTrackerMinimum(t, f))
	requireCASValue(t, st, 40, 20, "original")

	require.Nil(t, applyCASInScope(t, f, 12, miss), "a durable rejection must skip predicate evaluation on replay")
	require.Empty(t, observer.calls)
	require.NoError(t, st.PutAt(t.Context(), txnLockKey([]byte("b")), []byte("broken lock"), 22, 0))
	f.SetApplyIndex(13)
	resp := applyDecodedCASBatch(t, f, []*pb.Request{hit, casApplyRequest(51, 60, 20, "b", "fails operationally")})
	_, halted := resp.(interface{ HaltApply() error })
	require.True(t, halted, "response = %v", resp)
	requireCASProgress(t, f, 12)
	require.Zero(t, migrationTrackerMinimum(t, f))
	requireCASValue(t, st, 60, 20, "original")
	require.Empty(t, observer.calls)

	require.Nil(t, applyCASInScope(t, f, 13, hit))
	requireCASProgress(t, f, 13)
	require.Equal(t, uint64(50), migrationTrackerMinimum(t, f))
	requireCASValue(t, st, 60, 50, "replacement")
	require.Len(t, observer.calls, 1)

	require.NoError(t, st.Close())
	f, st = open()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	requireCASProgress(t, f, 13)
	require.Equal(t, uint64(50), migrationTrackerMinimum(t, f))
	requireCASValue(t, st, 60, 50, "replacement")
	require.Nil(t, applyCASInScope(t, f, 13, hit), "success must replay before watermark and CAS checks")
	require.Len(t, observer.calls, 1)
}

func requireCASProgress(t *testing.T, f *kvFSM, want uint64) {
	t.Helper()
	idx, ok, err := f.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, idx)
}

func requireCASValue(t *testing.T, st store.MVCCStore, readTS, wantTS uint64, value string) {
	t.Helper()
	got, err := store.GetVersionAt(t.Context(), st, []byte("n"), readTS)
	require.NoError(t, err)
	require.Equal(t, store.VersionedValue{TS: wantTS, Value: []byte(value), ExpireAt: 100}, got)
}
