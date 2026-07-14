package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
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
	}
}

func TestFSMRejectsObservedWriteFencedS3BucketAuxiliaryPointWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	fsm := newS3BucketAuxiliaryWriteFencedFSM(t, bucket)

	err := fsm.handleRawRequest(context.Background(), &pb.Request{
		ObservedRouteVersion: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: s3keys.BucketGenerationKey(bucket), Value: []byte("v")},
		},
	}, 10)
	require.ErrorIs(t, err, ErrRouteWriteFenced)
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
