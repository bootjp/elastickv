package adapter

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestProjectPublicGRPCMutationError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		err     error
		code    codes.Code
		message string
		reason  string
	}{
		{"timestamp binding unavailable", kv.ErrTimestampBindingInvalid, codes.FailedPrecondition, "Timestamp binding admission unavailable", publicGRPCNotAdmittedReason},
		{"not admitted", kv.ErrLeaderProxyCircuitOpen, codes.Unavailable, "Route unavailable", publicGRPCNotAdmittedReason},
		{"proven conflict", errors.Wrap(store.ErrWriteConflict, "physical-key"), codes.Aborted, "Operation aborted", publicGRPCAbortedReason},
		{"unknown", kv.ErrTransactionOutcomeUnknown, codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
		{"lost reply", io.EOF, codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
		{"unmarked status", status.Error(codes.Unavailable, "physical-key"), codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
		{"route fence", errors.Wrap(kv.ErrRouteWriteFenced, "physical-key"), codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
		{"lock without decision", kv.ErrTxnLocked, codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
		{"conflict joined with lost reply", errors.Join(store.ErrWriteConflict, io.EOF), codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
		{"conflict joined with route fence", errors.Join(store.ErrWriteConflict, kv.ErrRouteWriteFenced), codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := projectPublicGRPCMutationError(tc.err)
			st := status.Convert(got)
			require.Equal(t, tc.code, st.Code())
			require.Equal(t, tc.message, st.Message())
			require.NotContains(t, got.Error(), "physical-key")
			require.Len(t, st.Details(), 1)
			detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			require.Equal(t, publicGRPCFailureDomain, detail.GetDomain())
			require.Equal(t, tc.reason, detail.GetReason())
			require.Empty(t, detail.GetMetadata())
		})
	}
	require.NoError(t, projectPublicGRPCMutationError(nil))
}

func TestGRPCPublicRawOperationIDRejectedBeforeActivation(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	service := NewGRPCServer(st, &kv.Coordinate{})
	id := append([]byte{1}, make([]byte, 15)...)
	key := []byte("raw-operation-id")
	for _, call := range []func() error{
		func() error {
			_, err := service.RawPut(t.Context(), &pb.RawPutRequest{Key: key, Value: []byte("value"), OperationId: id})
			return err
		},
		func() error {
			_, err := service.RawDelete(t.Context(), &pb.RawDeleteRequest{Key: key, OperationId: id})
			return err
		},
	} {
		err := call()
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Len(t, st.Details(), 1)
		detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		require.Equal(t, publicGRPCNotAdmittedReason, detail.GetReason())
		_, err = service.store.GetAt(t.Context(), key, ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestGRPCPublicMutationsHideUnmarkedDispatchErrors(t *testing.T) {
	t.Parallel()
	service := NewGRPCServer(store.NewMVCCStore(), &availabilityErrorCoordinator{
		err: status.Error(codes.Unavailable, "physical-key: not leader"),
	})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterRawKVServer(grpcServer, service)
	pb.RegisterTransactionalKVServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	key := []byte("client-key")
	for _, call := range []struct {
		name string
		call func() error
	}{
		{"raw put", func() error {
			_, err := pb.NewRawKVClient(conn).RawPut(t.Context(), &pb.RawPutRequest{Key: key})
			return err
		}},
		{"raw delete", func() error {
			_, err := pb.NewRawKVClient(conn).RawDelete(t.Context(), &pb.RawDeleteRequest{Key: key})
			return err
		}},
		{"put", func() error {
			_, err := pb.NewTransactionalKVClient(conn).Put(t.Context(), &pb.PutRequest{Key: key})
			return err
		}},
		{"delete", func() error {
			_, err := pb.NewTransactionalKVClient(conn).Delete(t.Context(), &pb.DeleteRequest{Key: key})
			return err
		}},
	} {
		t.Run(call.name, func(t *testing.T) {
			err := call.call()
			require.Equal(t, codes.Unknown, status.Code(err), "rpc error: %v", err)
			require.False(t, strings.Contains(err.Error(), "physical-key"))
			st := status.Convert(err)
			require.Equal(t, "Operation outcome unknown", st.Message())
			require.Len(t, st.Details(), 1)
			detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			require.Equal(t, publicGRPCFailureDomain, detail.GetDomain())
			require.Equal(t, publicGRPCUnknownReason, detail.GetReason())
		})
	}
}
