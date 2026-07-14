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

func TestFSMAllowsRawPointWriteAtMigrationTimestampFloorDuringReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("replayed")}},
	}, 100)
	require.NoError(t, err)
	got, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("replayed"), got)
}

func TestFSMAllowsDelPrefixAtMigrationTimestampFloorDuringReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	require.NoError(t, fsm.store.PutAt(ctx, []byte("z"), []byte("v"), 10, 0))

	err := fsm.handleRawRequest(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("z")}},
	}, 100)
	require.NoError(t, err)

	_, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.ErrorIs(t, getErr, store.ErrKeyNotFound)
}

func TestFSMAllowsOnePhaseTxnAtMigrationTimestampFloorDuringReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	req := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    90,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), CommitTS: 100})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("low")},
		},
	}
	err := fsm.handleTxnRequest(ctx, req, 100)
	require.NoError(t, err)
	got, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("low"), got)
}

func TestFSMAllowsCommitAtMigrationTimestampFloorDuringReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newWriteFloorFSM(t)
	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    90,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}
	require.NoError(t, fsm.handleTxnRequest(ctx, prepare, 90))

	commit := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    90,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("z"), CommitTS: 100})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("low")},
		},
	}
	err := fsm.handleTxnRequest(ctx, commit, 100)
	require.NoError(t, err)
	got, getErr := fsm.store.GetAt(ctx, []byte("z"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}
