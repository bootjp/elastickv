package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
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

func TestFSMDelPrefixRoutesFilesystemChunkPrefixThroughVirtualRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inode := uint64(22)
	chunkPrefix := fskeys.ChunkPrefix(home, inode)
	chunkRoute := fskeys.ChunkRouteKey(home, inode)
	chunkRouteEnd := prefixScanEnd(chunkRoute)
	chunkRawAll := fskeys.ChunkAllPrefix()
	chunkRawEnd := prefixScanEnd(chunkRawAll)

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: chunkRawAll, GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: chunkRawAll, End: chunkRawEnd, GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 3, Start: chunkRawEnd, End: chunkRoute, GroupID: 1, State: distribution.RouteStateActive},
		{
			RouteID:                4,
			Start:                  chunkRoute,
			End:                    chunkRouteEnd,
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
		},
		{RouteID: 5, Start: chunkRouteEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)
	stagedChunk := distribution.MigrationStagedDataKey(9, fskeys.ChunkKey(home, inode, 0))
	require.NoError(t, fsm.store.PutAt(ctx, stagedChunk, []byte("chunk"), 20, 0))

	require.NoError(t, fsm.handleDelPrefix(ctx, chunkPrefix, 101))

	_, err := fsm.store.GetAt(ctx, stagedChunk, 150)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestFSMRejectsFilesystemChunkDelPrefixAgainstVirtualRouteFence(t *testing.T) {
	t.Parallel()

	home := uint64(11)
	inode := uint64(22)
	chunkRoute := fskeys.ChunkRouteKey(home, inode)
	chunkRouteEnd := prefixScanEnd(chunkRoute)
	chunkRawAll := fskeys.ChunkAllPrefix()
	chunkRawEnd := prefixScanEnd(chunkRawAll)

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: chunkRawAll, GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: chunkRawAll, End: chunkRawEnd, GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 3, Start: chunkRawEnd, End: chunkRoute, GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 4, Start: chunkRoute, End: chunkRouteEnd, GroupID: 1, State: distribution.RouteStateWriteFenced},
		{RouteID: 5, Start: chunkRouteEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: fskeys.ChunkPrefix(home, inode)}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMRejectsFilesystemUsageRoutePrefixWhenAnyOwnerRouteIsWriteFenced(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: fskeys.UsageRouteAllPrefix()}},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
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

func applyTargetReadinessToFSM(t *testing.T, fsm *kvFSM, state store.TargetStagedReadinessState) {
	t.Helper()

	writer, ok := fsm.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(context.Background(), state))
}

func applySourceMigrationControlToFSM(t *testing.T, fsm *kvFSM, readFence bool) {
	t.Helper()
	applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
		JobID:               10,
		RouteStart:          []byte("m"),
		RouteEnd:            []byte("z"),
		MigrationJobID:      10,
		MinWriteTSExclusive: 50,
		Armed:               true,
		SourceWriteFence:    true,
		SourceReadFence:     readFence,
		RetentionPinTS:      40,
	})
}

func newMigrationWriteTrackerFSM(t *testing.T) *kvFSM {
	t.Helper()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	fsm := newComposed1FSM(t, engine, 1)
	applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
		JobID:               11,
		RouteStart:          []byte("m"),
		RouteEnd:            []byte("z"),
		MigrationJobID:      11,
		MinWriteTSExclusive: 1,
		Armed:               true,
		RetentionPinTS:      1,
		TrackWrites:         true,
	})
	return fsm
}

func migrationTrackerMinimum(t *testing.T, fsm *kvFSM) uint64 {
	t.Helper()
	reader, ok := fsm.store.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(context.Background())
	require.NoError(t, err)
	require.Len(t, states, 1)
	return states[0].MinAdmittedTS
}

func TestFSMMigrationWriteTrackerCoversAllAdmissionPaths(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newMigrationWriteTrackerFSM(t)
	require.NoError(t, fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("raw")}},
	}, 80))
	require.Equal(t, uint64(80), migrationTrackerMinimum(t, fsm))

	require.NoError(t, fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("m")}},
	}, 70))
	require.Equal(t, uint64(70), migrationTrackerMinimum(t, fsm))

	require.NoError(t, fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn: true,
		Ts:    50,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("o"), CommitTS: 60})},
			{Op: pb.Op_PUT, Key: []byte("o"), Value: []byte("one-phase")},
		},
	}, 60))
	require.Equal(t, uint64(60), migrationTrackerMinimum(t, fsm))

	require.NoError(t, fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    40,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("p"), CommitTS: 55, LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("p"), Value: []byte("prepared")},
		},
	}, 40))
	require.Equal(t, uint64(55), migrationTrackerMinimum(t, fsm))
}

func TestFSMMigrationWriteTrackerRecordsCommitPreparedBeforeArm(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	fsm := newComposed1FSM(t, engine, 1)
	startTS, commitTS := uint64(40), uint64(60)
	meta := &pb.Mutation{
		Op:  pb.Op_PUT,
		Key: []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{
			PrimaryKey: []byte("n"),
			CommitTS:   commitTS,
			LockTTLms:  defaultTxnLockTTLms,
		}),
	}
	mutation := &pb.Mutation{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("value")}
	require.NoError(t, fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_PREPARE,
		Ts:        startTS,
		Mutations: []*pb.Mutation{meta, mutation},
	}, startTS))

	applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
		JobID:               12,
		RouteStart:          []byte("m"),
		RouteEnd:            []byte("z"),
		MigrationJobID:      12,
		MinWriteTSExclusive: 1,
		Armed:               true,
		RetentionPinTS:      1,
		TrackWrites:         true,
	})
	require.NoError(t, fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_COMMIT,
		Ts:        startTS,
		Mutations: []*pb.Mutation{meta, mutation},
	}, commitTS))
	require.Equal(t, commitTS, migrationTrackerMinimum(t, fsm))
}

func newReadinessReadKeyFSM(t *testing.T) *kvFSM {
	t.Helper()

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)
	applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("m"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})
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

func TestFSMDurableSourceFenceRejectsRawWriteDespiteCatalogBypass(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	applySourceMigrationControlToFSM(t, fsm, false)
	key := []byte("n")
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		WriteFenceBypassKeys: [][]byte{key},
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}},
	}, 60)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMDurableSourceFenceRejectsPrefixWrite(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	applySourceMigrationControlToFSM(t, fsm, false)
	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("m")}},
	}, 60)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
}

func TestFSMDurableSourceFenceRejectsPrefixWriteOnActiveRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("m"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	fsm := newComposed1FSM(t, engine, 1)
	applySourceMigrationControlToFSM(t, fsm, false)
	require.NoError(t, fsm.store.PutAt(ctx, []byte("m/key"), []byte("v"), 1, 0))

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("m")}},
	}, 60)
	require.ErrorIs(t, err, ErrRouteWriteFenced)

	got, getErr := fsm.store.GetAt(ctx, []byte("m/key"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestFSMWriteFenceBypassAllowsPinnedOnePhaseTxnOnNonOwningGroup(t *testing.T) {
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
		Ts:                   10,
		ObservedRouteVersion: 1,
		WriteFenceBypassKeys: [][]byte{key},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v")},
		},
	}, 20)
	require.NoError(t, err)

	got, err := fsm.store.GetAt(context.Background(), key, 20)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
}

func TestFSMDurableSourceFenceRejectsPinnedOnePhaseTxn(t *testing.T) {
	t.Parallel()

	fsm := newWriteFencedFSM(t)
	applySourceMigrationControlToFSM(t, fsm, false)
	key := []byte("n")
	err := fsm.handleTxnRequest(context.Background(), &pb.Request{
		IsTxn:                true,
		Ts:                   50,
		WriteFenceBypassKeys: [][]byte{key},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v")},
		},
	}, 60)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
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

func TestFSMRejectsS3BucketAuxiliaryPointWriteOnWriteFencedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	fsm := newS3BucketAuxiliaryWriteFencedFSM(t, bucket)

	for _, key := range [][]byte{
		s3keys.BucketMetaKey(bucket),
		s3keys.BucketGenerationKey(bucket),
	} {
		err := fsm.handleRawRequest(ctx, &pb.Request{
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}},
		}, 10)
		require.ErrorIs(t, err, ErrRouteWriteFenced)

		_, getErr := fsm.store.GetAt(ctx, key, ^uint64(0))
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestFSMRejectsS3BucketAuxiliaryPointWriteBelowRouteFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{{
		RouteID:             1,
		Start:               routeStart,
		End:                 routeEnd,
		GroupID:             1,
		State:               distribution.RouteStateActive,
		MinWriteTSExclusive: 100,
	}})
	fsm := newComposed1FSM(t, engine, 1)

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: s3keys.BucketMetaKey(bucket), Value: []byte("v")}},
	}, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)
}

func TestFSMIgnoresRawRouteFloorForS3BucketAuxiliaryPointWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	key := s3keys.BucketMetaKey(bucket)
	auxStart, auxEnd, ok := s3BucketAuxiliaryRouteRange(key)
	require.True(t, ok)
	rawStart := []byte(s3keys.BucketMetaPrefix)
	rawEnd := prefixScanEnd(rawStart)
	require.Less(t, bytes.Compare(auxEnd, rawStart), 0)

	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{
		{
			RouteID: 1,
			Start:   auxStart,
			End:     auxEnd,
			GroupID: 1,
			State:   distribution.RouteStateActive,
		},
		{
			RouteID:             2,
			Start:               rawStart,
			End:                 rawEnd,
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		},
	})
	fsm := newComposed1FSM(t, engine, 1)

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}},
	}, 100)
	require.NoError(t, err)
	got, err := fsm.store.GetAt(ctx, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
}

func TestFSMRejectsS3BucketAuxiliaryPointWriteWithoutTargetReadinessProof(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   routeStart,
		End:     routeEnd,
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	fsm := newComposed1FSM(t, engine, 1)
	applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             routeStart,
		RouteEnd:               routeEnd,
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: s3keys.BucketGenerationKey(bucket), Value: []byte("v")}},
	}, 120)
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

func TestFSMOnePhaseDedupChecksTargetReadinessBeforeNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	require.NoError(t, fsm.store.PutAt(ctx, []byte("b"), []byte("landed"), 20, 0))

	err := fsm.handleTxnRequest(ctx, onePhaseReq(30, 40, 20, []byte("b"), []byte("retry")), 40)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	landedAt40, probeErr := fsm.store.CommittedVersionAt(ctx, []byte("b"), 40)
	require.NoError(t, probeErr)
	require.False(t, landedAt40)
}

func TestFSMOnePhaseTxnChecksReadKeysForTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newReadinessReadKeyFSM(t)
	req := onePhaseReq(10, 20, 0, []byte("b"), []byte("v"))
	req.ReadKeys = [][]byte{[]byte("n")}

	err := fsm.handleTxnRequest(ctx, req, 20)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	_, getErr := fsm.store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMPrepareTxnChecksReadKeysForTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newReadinessReadKeyFSM(t)
	req := &pb.Request{
		IsTxn:    true,
		Phase:    pb.Phase_PREPARE,
		Ts:       10,
		ReadKeys: [][]byte{[]byte("n")},
		Mutations: []*pb.Mutation{
			{
				Op:  pb.Op_PUT,
				Key: []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: []byte("b"),
					LockTTLms:  defaultTxnLockTTLms,
				}),
			},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")},
		},
	}

	err := fsm.handleTxnRequest(ctx, req, 10)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	_, getErr := fsm.store.GetAt(ctx, txnLockKey([]byte("b")), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMOnePhaseTxnChecksSourceReadFenceForReadKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{{
		RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive,
	}})
	fsm := newComposed1FSM(t, engine, 1)
	applySourceMigrationControlToFSM(t, fsm, true)
	req := onePhaseReq(10, 20, 0, []byte("b"), []byte("v"))
	req.ReadKeys = [][]byte{[]byte("n")}

	err := fsm.handleTxnRequest(ctx, req, 20)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, getErr := fsm.store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMPrepareTxnSkipsRemotePrimaryForTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, engine, 1)
	applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("m"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})

	err := fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{
				Op:  pb.Op_PUT,
				Key: []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: []byte("b"),
					LockTTLms:  defaultTxnLockTTLms,
				}),
			},
			{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("v")},
		},
	}, 10)
	require.NoError(t, err)

	_, getErr := fsm.store.GetAt(ctx, txnLockKey([]byte("n")), ^uint64(0))
	require.NoError(t, getErr)
}

func TestFSMPrepareTxnChecksStagedVisibilityWriteConflicts(t *testing.T) {
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
	require.NoError(t, fsm.store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged"), 20, 0))

	err := fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{
				Op:  pb.Op_PUT,
				Key: []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: []byte("b"),
					CommitTS:   120,
					LockTTLms:  defaultTxnLockTTLms,
				}),
			},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")},
		},
	}, 10)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	_, getErr := fsm.store.GetAt(ctx, txnLockKey([]byte("b")), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMOnePhaseTxnChecksStagedVisibilityReadConflicts(t *testing.T) {
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
	require.NoError(t, fsm.store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("n")), []byte("staged"), 20, 0))

	req := onePhaseReq(10, 120, 0, []byte("b"), []byte("v"))
	req.ReadKeys = [][]byte{[]byte("n")}

	err := fsm.handleTxnRequest(ctx, req, 120)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	_, getErr := fsm.store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMPrepareUsesCommitTSForRouteFloorWhenPresent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	commitTS := uint64(101)
	primaryKey := []byte("b")
	err := fsm.handleTxnRequest(ctx, &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    50,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: commitTS}),
			},
			{Op: pb.Op_PUT, Key: primaryKey, Value: []byte("v")},
		},
	}, 50)
	require.NoError(t, err)

	rawLock, err := fsm.store.GetAt(ctx, txnLockKey(primaryKey), ^uint64(0))
	require.NoError(t, err)
	lock, err := decodeTxnLock(rawLock)
	require.NoError(t, err)
	require.Equal(t, commitTS, lock.CommitTS)
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

func TestFSMAbortCleanupBypassesTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
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
