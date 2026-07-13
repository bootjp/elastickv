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

type deletePrefixIndexRecordingStore struct {
	store.MVCCStore

	deletePrefixIndexes  []uint64
	deletePrefixPrefixes [][]byte
}

func (s *deletePrefixIndexRecordingStore) DeletePrefixAtRaftAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS, appliedIndex uint64) error {
	s.deletePrefixIndexes = append(s.deletePrefixIndexes, appliedIndex)
	s.deletePrefixPrefixes = append(s.deletePrefixPrefixes, append([]byte(nil), prefix...))
	return s.MVCCStore.DeletePrefixAtRaftAt(ctx, prefix, excludePrefix, commitTS, appliedIndex)
}

func (s *deletePrefixIndexRecordingStore) MigrationTargetReadinessStates(ctx context.Context) ([]store.TargetStagedReadinessState, error) {
	reader, ok := s.MVCCStore.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil, nil
	}
	return reader.MigrationTargetReadinessStates(ctx)
}

func (s *deletePrefixIndexRecordingStore) ApplyTargetStagedReadiness(ctx context.Context, state store.TargetStagedReadinessState) error {
	writer, ok := s.MVCCStore.(store.MigrationTargetReadinessWriter)
	if !ok {
		return store.ErrNotSupported
	}
	return writer.ApplyTargetStagedReadiness(ctx, state)
}

func newWriteFencedFSM(t *testing.T) *kvFSM {
	t.Helper()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 1, State: distribution.RouteStateWriteFenced},
	})
	return newComposed1FSM(t, engine, 1)
}

func newWriteFloorFSM(t *testing.T) *kvFSM {
	t.Helper()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{
			RouteID:             1,
			Start:               []byte("a"),
			End:                 []byte("z"),
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		},
	})
	return newComposed1FSM(t, engine, 1)
}

func newTargetReadinessFSM(t *testing.T, route distribution.RouteDescriptor) *kvFSM {
	t.Helper()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{route})
	fsm := newComposed1FSM(t, engine, route.GroupID)
	writer, ok := fsm.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(context.Background(), store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}))
	return fsm
}

func TestFSMRejectsRawPointWriteOnWriteFencedRoute(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)

	_, getErr := fsm.store.GetAt(context.Background(), []byte("z"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMRejectsRawPointWriteWithoutTargetReadinessProof(t *testing.T) {
	t.Parallel()

	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")}},
	}, 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	_, getErr := fsm.store.GetAt(context.Background(), []byte("b"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMAllowsRawPointWriteWithClearedTargetReadinessProof(t *testing.T) {
	t.Parallel()

	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID:             1,
		Start:               []byte("a"),
		End:                 []byte("z"),
		GroupID:             1,
		State:               distribution.RouteStateActive,
		MinWriteTSExclusive: 100,
	})
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")}},
	}, 101)
	require.NoError(t, err)

	got, getErr := fsm.store.GetAt(context.Background(), []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsTargetReadinessProofFromAnotherGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{
			RouteID:             1,
			Start:               []byte("a"),
			End:                 []byte("z"),
			GroupID:             2,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		},
	})
	fsm := newComposed1FSM(t, engine, 1)
	writer, ok := fsm.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(context.Background(), store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")}},
	}, 101)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestFSMRejectsDelPrefixIntersectingWriteFencedRoute(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("z"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("z")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)

	got, getErr := fsm.store.GetAt(context.Background(), []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsDelPrefixWithoutTargetReadinessProof(t *testing.T) {
	t.Parallel()

	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("b"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("b")}},
	}, 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	got, getErr := fsm.store.GetAt(context.Background(), []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMDelPrefixTombstonesStagedVisibilityRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID:                1,
		Start:                  []byte("a"),
		End:                    []byte("z"),
		GroupID:                1,
		State:                  distribution.RouteStateActive,
		StagedVisibilityActive: true,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
	})
	rawKey := []byte("b")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	require.NoError(t, fsm.store.PutAt(ctx, stagedKey, []byte("staged"), 110, 0))

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: rawKey}},
	}, 120)
	require.NoError(t, err)

	_, err = fsm.store.GetAt(ctx, stagedKey, 130)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestFSMDelPrefixAdvancesApplyIndexOnlyOnLiveDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID:                1,
		Start:                  []byte("a"),
		End:                    []byte("z"),
		GroupID:                1,
		State:                  distribution.RouteStateActive,
		StagedVisibilityActive: true,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
	})
	recording := &deletePrefixIndexRecordingStore{MVCCStore: fsm.store}
	fsm.store = recording
	fsm.SetApplyIndex(55)

	rawKey := []byte("b")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	require.NoError(t, fsm.store.PutAt(ctx, stagedKey, []byte("staged"), 110, 0))

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: rawKey}},
	}, 120)
	require.NoError(t, err)

	require.Equal(t, []uint64{0, 55}, recording.deletePrefixIndexes)
	require.Equal(t, stagedKey, recording.deletePrefixPrefixes[0])
	require.Equal(t, rawKey, recording.deletePrefixPrefixes[1])
}

func TestFSMRejectsFullRangeDelPrefixWhenRouteIsWriteFenced(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("z"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: nil}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)

	got, getErr := fsm.store.GetAt(context.Background(), []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsRawPointWriteBelowRouteFloor(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")}},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	_, getErr := fsm.store.GetAt(context.Background(), []byte("b"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMRejectsDelPrefixBelowRouteFloor(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("b"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("b")}},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	got, getErr := fsm.store.GetAt(context.Background(), []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsBroadInternalDelPrefixWhenRouteIsWriteFenced(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	key := []byte("!redis|string|z")
	require.NoError(t, fsm.store.PutAt(context.Background(), key, []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("!redis|")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)

	got, getErr := fsm.store.GetAt(context.Background(), key, ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsOnePhaseTxnBelowRouteFloor(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	err := fsm.handleTxnRequest(context.Background(), onePhaseReq(10, 100, 0, []byte("b"), []byte("v")), 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	_, getErr := fsm.store.GetAt(context.Background(), []byte("b"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMPrepareUsesCommitTSForRouteFloorWhenPresent(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    50,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), LockTTLms: defaultTxnLockTTLms, CommitTS: 101}),
			},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")},
		},
	}, 50)
	require.NoError(t, err)
}

func TestFSMPrepareWithoutCommitTSUsesStartTSForRouteFloor(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), LockTTLms: defaultTxnLockTTLms}),
			},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")},
		},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)
}

func TestFSMRejectsPrepareOnWriteFencedRouteButAllowsAbort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFencedFSM(t)
	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}
	require.ErrorIs(t, fsm.handleTxnRequest(ctx, prepare, 10), ErrRouteWriteFenced)

	abort := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Ts:    11,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), CommitTS: 11})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}
	err := fsm.handleTxnRequest(ctx, abort, 11)
	require.False(t, errors.Is(err, ErrRouteWriteFenced), "ABORT must keep the narrow cleanup lane open")
}

func TestFSMAbortCleanupBypassesRetainedWriteFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	startTS := uint64(10)
	abortTS := uint64(11)
	primaryKey := []byte("b")
	require.NoError(t, fsm.store.PutAt(ctx, txnLockKey(primaryKey), encodeTxnLock(txnLock{
		StartTS:      startTS,
		PrimaryKey:   primaryKey,
		IsPrimaryKey: true,
	}), startTS, 0))
	require.NoError(t, fsm.store.PutAt(ctx, txnIntentKey(primaryKey), encodeTxnIntent(txnIntent{
		StartTS: startTS,
		Op:      txnIntentOpPut,
		Value:   []byte("v"),
	}), startTS, 0))

	abort := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, CommitTS: abortTS})},
			{Op: pb.Op_PUT, Key: primaryKey},
		},
	}
	require.NoError(t, fsm.handleTxnRequest(ctx, abort, abortTS))

	_, lockErr := fsm.store.GetAt(ctx, txnLockKey(primaryKey), ^uint64(0))
	require.ErrorIs(t, lockErr, store.ErrKeyNotFound)
	_, intentErr := fsm.store.GetAt(ctx, txnIntentKey(primaryKey), ^uint64(0))
	require.ErrorIs(t, intentErr, store.ErrKeyNotFound)
}
