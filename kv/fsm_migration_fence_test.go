package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

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
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
	})
	return newComposed1FSM(t, engine, 1)
}

func newFirstRouteWriteFencedFSM(t *testing.T) *kvFSM {
	t.Helper()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateWriteFenced},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	return newComposed1FSM(t, engine, 1)
}

func s3BucketAuxiliaryFenceRoutes(bucket string, rawGroupID, fencedGroupID uint64) []distribution.RouteDescriptor {
	start := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	end := prefixScanEnd(start)
	return []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: start, GroupID: rawGroupID, State: distribution.RouteStateActive},
		{RouteID: 2, Start: start, End: end, GroupID: fencedGroupID, State: distribution.RouteStateWriteFenced},
		{RouteID: 3, Start: end, End: nil, GroupID: rawGroupID, State: distribution.RouteStateActive},
	}
}

func s3BucketAuxiliarySplitRoutes(bucket string, rawGroupID, ownerGroupID, splitGroupID uint64) []distribution.RouteDescriptor {
	start := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	split := append(append([]byte(nil), start...), 'm')
	end := prefixScanEnd(start)
	return []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: start, GroupID: rawGroupID, State: distribution.RouteStateActive},
		{RouteID: 2, Start: start, End: split, GroupID: ownerGroupID, State: distribution.RouteStateActive},
		{RouteID: 3, Start: split, End: end, GroupID: splitGroupID, State: distribution.RouteStateActive},
		{RouteID: 4, Start: end, End: nil, GroupID: rawGroupID, State: distribution.RouteStateActive},
	}
}

func newS3BucketAuxiliaryWriteFencedFSM(t *testing.T, bucket string) *kvFSM {
	t.Helper()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, s3BucketAuxiliaryFenceRoutes(bucket, 1, 1))
	return newComposed1FSM(t, engine, 1)
}

func TestFSMRejectsCurrentWriteFencedRawPointWrite(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsCurrentWriteFencedEmptyRawPointWrite(t *testing.T) {
	t.Parallel()

	fsm := newFirstRouteWriteFencedFSM(t)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(""), Value: []byte("v")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsObservedWriteFencedRawPointWrite(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		ObservedRouteVersion: 1,
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMWriteFenceBypassAllowsMarkedRawPointWrite(t *testing.T) {
	t.Parallel()

	fsm := newFirstRouteWriteFencedFSM(t)
	key := []byte("!sqs|msg|data|p|partitioned-key")
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		WriteFenceBypassKeys: [][]byte{key},
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}},
	}, 10)
	require.NoError(t, err)

	got, err := fsm.store.GetAt(context.Background(), key, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
}

func TestFSMWriteFenceBypassRejectsRawWriteAtBypassedRouteFloor(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	key := []byte("!sqs|msg|data|p|partitioned-key")
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		ObservedRouteVersion: 1,
		WriteFenceBypassKeys: [][]byte{key},
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)
}

func TestFSMWriteFenceBypassAllowsPinnedTxnOnNonOwningGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateWriteFenced},
	})
	fsm := newComposed1FSM(t, engine, 1)
	key := []byte("z")
	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_PREPARE,
		Ts:                   10,
		ObservedRouteVersion: 1,
		WriteFenceBypassKeys: [][]byte{key},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_DEL, Key: key},
		},
	}, 10)
	require.NoError(t, err)
}

func TestFSMWriteFenceBypassRejectsPinnedTxnAtBypassedRouteFloor(t *testing.T) {
	t.Parallel()

	fsm := newWriteFloorFSM(t)
	key := []byte("!sqs|msg|data|p|partitioned-key")
	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_PREPARE,
		Ts:                   100,
		ObservedRouteVersion: 1,
		WriteFenceBypassKeys: [][]byte{key},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_DEL, Key: key},
		},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)
}

func TestFSMWriteFenceBypassDoesNotAllowDelPrefix(t *testing.T) {
	t.Parallel()

	fsm := newFirstRouteWriteFencedFSM(t)
	prefix := []byte("!sqs|msg|data|p|")
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		WriteFenceBypassKeys: [][]byte{prefix},
		Mutations:            []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: prefix}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMDelPrefixTombstonesStagedVisibilityRowsDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{
			RouteID:                1,
			Start:                  []byte("a"),
			End:                    []byte("z"),
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
		},
	})
	fsm := newComposed1FSM(t, engine, 1)
	dropKey := []byte("b/drop")
	outsideKey := []byte("c/outside")
	stagedDrop := distribution.MigrationStagedDataKey(9, dropKey)
	stagedOutside := distribution.MigrationStagedDataKey(9, outsideKey)
	require.NoError(t, fsm.store.PutAt(ctx, stagedDrop, []byte("drop"), 20, 0))
	require.NoError(t, fsm.store.PutAt(ctx, stagedOutside, []byte("outside"), 20, 0))

	require.NoError(t, fsm.handleDelPrefix(ctx, []byte("b/"), 101))

	_, err := fsm.store.GetAt(ctx, stagedDrop, 150)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	got, err := fsm.store.GetAt(ctx, stagedOutside, 150)
	require.NoError(t, err)
	require.Equal(t, []byte("outside"), got)
}

type recordingPrefixDeleteStore struct {
	store.MVCCStore

	batchCalls   int
	singleCalls  int
	deletes      []store.PrefixDelete
	commitTS     uint64
	appliedIndex uint64
}

func (s *recordingPrefixDeleteStore) DeletePrefixAtRaftAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS, appliedIndex uint64) error {
	s.singleCalls++
	return s.MVCCStore.DeletePrefixAtRaftAt(ctx, prefix, excludePrefix, commitTS, appliedIndex)
}

func (s *recordingPrefixDeleteStore) DeletePrefixesAtRaftAt(ctx context.Context, deletes []store.PrefixDelete, commitTS, appliedIndex uint64) error {
	s.batchCalls++
	s.deletes = clonePrefixDeletes(deletes)
	s.commitTS = commitTS
	s.appliedIndex = appliedIndex
	return s.MVCCStore.DeletePrefixesAtRaftAt(ctx, deletes, commitTS, appliedIndex)
}

func clonePrefixDeletes(deletes []store.PrefixDelete) []store.PrefixDelete {
	out := make([]store.PrefixDelete, len(deletes))
	for i, del := range deletes {
		out[i] = store.PrefixDelete{
			Prefix:        append([]byte(nil), del.Prefix...),
			ExcludePrefix: append([]byte(nil), del.ExcludePrefix...),
		}
	}
	return out
}

func TestFSMDelPrefixBatchesStagedAndRawTombstonesDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{
			RouteID:                1,
			Start:                  []byte("a"),
			End:                    []byte("z"),
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
		},
	})
	fsm := newComposed1FSM(t, engine, 1)
	rec := &recordingPrefixDeleteStore{MVCCStore: fsm.store}
	fsm.store = rec
	fsm.pendingApplyIdx = 1234

	require.NoError(t, fsm.handleDelPrefix(ctx, []byte("b/"), 101))
	require.Equal(t, 1, rec.batchCalls)
	require.Zero(t, rec.singleCalls)
	require.Equal(t, uint64(101), rec.commitTS)
	require.Equal(t, uint64(1234), rec.appliedIndex)
	require.Equal(t, []store.PrefixDelete{
		{Prefix: []byte("b/"), ExcludePrefix: txnCommonPrefix},
		{
			Prefix:        distribution.MigrationStagedDataKey(9, []byte("b/")),
			ExcludePrefix: distribution.MigrationStagedDataKey(9, txnCommonPrefix),
		},
	}, rec.deletes)
}

func TestFSMRejectsCurrentWriteFenceAfterObservedActiveRawPointWrite(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 1, State: distribution.RouteStateWriteFenced},
	})

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		ObservedRouteVersion: 1,
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsCurrentWriteFencedUnpinnedPrepare(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 1, State: distribution.RouteStateWriteFenced},
	})

	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsCurrentWriteFencedS3BucketAuxiliaryPointWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-b"
	fsm := newS3BucketAuxiliaryWriteFencedFSM(t, bucket)

	for _, key := range [][]byte{
		s3keys.BucketMetaKey(bucket),
		s3keys.BucketGenerationKey(bucket),
	} {
		err := fsm.handleRawRequest(ctx, &pb.Request{
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}},
		}, 10)
		require.ErrorIs(t, err, ErrRouteWriteFenced)
	}
}

func TestFSMRejectsObservedWriteFencedS3BucketAuxiliaryPointWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-b"
	fsm := newS3BucketAuxiliaryWriteFencedFSM(t, bucket)

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: s3keys.BucketGenerationKey(bucket), Value: []byte("v")},
		},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMIgnoresRawRouteFenceForS3BucketAuxiliaryWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	key := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	routes := s3BucketAuxiliaryFenceRoutes(bucket, 1, 1)
	routes[1].State = distribution.RouteStateActive
	routes[2].State = distribution.RouteStateWriteFenced
	applyComposed1Snapshot(t, engine, 1, routes)

	rawRoute, ok := engine.GetRoute(routeKey(key))
	require.True(t, ok)
	require.Equal(t, distribution.RouteStateWriteFenced, rawRoute.State)
	auxStart, auxEnd, ok := s3BucketAuxiliaryRouteRange(key)
	require.True(t, ok)
	auxRoutes := engine.GetIntersectingRoutes(auxStart, auxEnd)
	require.NotEmpty(t, auxRoutes)
	require.Equal(t, distribution.RouteStateActive, auxRoutes[0].State)

	fsm := newComposed1FSM(t, engine, 1)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("meta")}},
	}, 100)
	require.NoError(t, err)
}

func TestFSMContinuesWriteFenceValidationAfterS3BucketAuxiliaryWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	auxiliaryKey := s3keys.BucketMetaKey(bucket)
	fsm := newWriteFencedFSM(t)

	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: auxiliaryKey, LockTTLms: defaultTxnLockTTLms}),
			},
			{Op: pb.Op_PUT, Key: auxiliaryKey, Value: []byte("meta")},
		},
	}, 10)
	require.NoError(t, err)

	err = fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    11,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), LockTTLms: defaultTxnLockTTLms}),
			},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}, 11)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMIgnoresNonOwnerS3BucketAuxiliaryFenceForPointWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	key := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	routes := s3BucketAuxiliarySplitRoutes(bucket, 5, 1, 1)
	routes[2].State = distribution.RouteStateWriteFenced
	applyComposed1Snapshot(t, engine, 1, routes)

	auxStart, auxEnd, ok := s3BucketAuxiliaryRouteRange(key)
	require.True(t, ok)
	auxRoutes := engine.GetIntersectingRoutes(auxStart, auxEnd)
	require.Len(t, auxRoutes, 2)
	require.Equal(t, distribution.RouteStateActive, auxRoutes[0].State)
	require.Equal(t, distribution.RouteStateWriteFenced, auxRoutes[1].State)

	fsm := newComposed1FSM(t, engine, 1)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("meta")}},
	}, 100)
	require.NoError(t, err)
}

func TestFSMComposed1UsesS3BucketAuxiliaryRouteOwner(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-b"
	key := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, s3BucketAuxiliaryStagedRoutes(bucket, 3, 4))
	fsm := newComposed1FSM(t, engine, 4)

	err := fsm.verifyComposed1(&pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_PREPARE,
		Ts:                   10,
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("meta")},
		},
	})
	require.NoError(t, err)
}

func TestFSMIgnoresRawRouteFloorForS3BucketAuxiliaryWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	key := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	routes := s3BucketAuxiliaryFenceRoutes(bucket, 1, 1)
	routes[1].State = distribution.RouteStateActive
	routes[2].MinWriteTSExclusive = ^uint64(0)
	applyComposed1Snapshot(t, engine, 1, routes)

	rawRoute, ok := engine.GetRoute(routeKey(key))
	require.True(t, ok)
	require.Equal(t, ^uint64(0), rawRoute.MinWriteTSExclusive)
	auxStart, auxEnd, ok := s3BucketAuxiliaryRouteRange(key)
	require.True(t, ok)
	auxRoutes := engine.GetIntersectingRoutes(auxStart, auxEnd)
	require.NotEmpty(t, auxRoutes)
	require.Zero(t, auxRoutes[0].MinWriteTSExclusive)

	fsm := newComposed1FSM(t, engine, 1)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("meta")}},
	}, 100)
	require.NoError(t, err)
}

func TestFSMIgnoresNonOwnerS3BucketAuxiliaryFloorForPointWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-b"
	key := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	routes := s3BucketAuxiliarySplitRoutes(bucket, 1, 1, 1)
	routes[2].MinWriteTSExclusive = ^uint64(0)
	applyComposed1Snapshot(t, engine, 1, routes)

	auxStart, auxEnd, ok := s3BucketAuxiliaryRouteRange(key)
	require.True(t, ok)
	auxRoutes := engine.GetIntersectingRoutes(auxStart, auxEnd)
	require.Len(t, auxRoutes, 2)
	require.Zero(t, auxRoutes[0].MinWriteTSExclusive)
	require.Equal(t, ^uint64(0), auxRoutes[1].MinWriteTSExclusive)

	fsm := newComposed1FSM(t, engine, 1)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("meta")}},
	}, 100)
	require.NoError(t, err)
}

func TestFSMRejectsCurrentWriteFencedDelPrefix(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("z"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("z")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsObservedWriteFencedDelPrefix(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("z"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		ObservedRouteVersion: 1,
		Mutations:            []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("z")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsCurrentWriteFencedFullRangeDelPrefix(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	require.NoError(t, fsm.store.PutAt(context.Background(), []byte("z"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: nil}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsCurrentWriteFencedBroadInternalDelPrefix(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	key := []byte("!redis|string|z")
	require.NoError(t, fsm.store.PutAt(context.Background(), key, []byte("v"), 1, 0))

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("!redis|")}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsCurrentWriteFencedPrepareButAllowsAbort(t *testing.T) {
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
	require.NotErrorIs(t, err, ErrRouteWriteFenced, "ABORT must keep the narrow cleanup lane open")
}

func TestFSMRejectsObservedWriteFencedPrepareButAllowsAbort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFencedFSM(t)
	prepare := &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_PREPARE,
		Ts:                   10,
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}
	require.ErrorIs(t, fsm.handleTxnRequest(ctx, prepare, 10), ErrRouteWriteFenced)

	abort := &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_ABORT,
		Ts:                   11,
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), CommitTS: 11})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}
	require.NotErrorIs(t, fsm.handleTxnRequest(ctx, abort, 11), ErrRouteWriteFenced)
}

func TestFSMRejectsRawPointWriteAtMigrationTimestampFloorDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	err := fsm.handleRawRequest(ctx, &pb.Request{
		ObservedRouteVersion: 1,
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("replayed")}},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)
	_, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMRejectsDelPrefixAtMigrationTimestampFloorDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	require.NoError(t, fsm.store.PutAt(ctx, []byte("z"), []byte("v"), 10, 0))

	err := fsm.handleRawRequest(ctx, &pb.Request{
		ObservedRouteVersion: 1,
		Mutations:            []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("z")}},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)

	got, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsOnePhaseTxnAtMigrationTimestampFloorDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	req := &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_NONE,
		Ts:                   90,
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), CommitTS: 100})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("low")},
		},
	}
	err := fsm.handleTxnRequest(ctx, req, 100)
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)
	_, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMRejectsPrepareAtMigrationTimestampFloorDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	prepare := &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_PREPARE,
		Ts:                   90,
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}
	require.ErrorIs(t, fsm.handleTxnRequest(ctx, prepare, 90), ErrRouteWriteTimestampTooLow)
}

func TestFSMTimestampFloorUsesObservedSnapshotDuringApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: ^uint64(0)},
	})

	err := fsm.handleRawRequest(ctx, &pb.Request{
		ObservedRouteVersion: 1,
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("proposed-before-floor")}},
	}, 100)
	require.NoError(t, err)

	got, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("proposed-before-floor"), got)
}
