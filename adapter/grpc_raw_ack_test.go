package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// A coordinator with no applied Raft index cannot prove that the mutation
// behind a RawKV success response is visible to a later fenced read.
type noApplyReceiptCoordinator struct{ kv.Coordinator }

func (*noApplyReceiptCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return &kv.CoordinateResponse{}, nil
}

func (*noApplyReceiptCoordinator) LinearizableRead(context.Context) (uint64, error) { return 1, nil }

func (*noApplyReceiptCoordinator) Clock() *kv.HLC { return kv.NewHLC() }

func (*noApplyReceiptCoordinator) IsLeaderForKey([]byte) bool { return true }

func TestRawPutDoesNotAcknowledgeWithoutAppliedIndex(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	key := []byte("jepsen-grpc/raw/79")
	require.NoError(t, st.PutAt(ctx, key, []byte("10"), 10, 0))
	server := NewGRPCServer(st, &noApplyReceiptCoordinator{})

	put, err := server.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: []byte("11")})
	require.Error(t, err, "a write without an applied-index receipt must not be acknowledged")
	require.False(t, put.GetSuccess())

	read, err := server.RawGet(ctx, &pb.RawGetRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, []byte("10"), read.GetValue())
}

func TestRawDeleteDoesNotAcknowledgeWithoutAppliedIndex(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	key := []byte("jepsen-grpc/raw/79")
	require.NoError(t, st.PutAt(ctx, key, []byte("10"), 10, 0))
	server := NewGRPCServer(st, &noApplyReceiptCoordinator{})

	deleted, err := server.RawDelete(ctx, &pb.RawDeleteRequest{Key: key})
	require.Error(t, err, "a delete without an applied-index receipt must not be acknowledged")
	require.False(t, deleted.GetSuccess())

	read, err := server.RawGet(ctx, &pb.RawGetRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, []byte("10"), read.GetValue())
}
