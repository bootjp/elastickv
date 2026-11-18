package adapter

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type failingRawGroupWatermarkStore struct {
	store.MVCCStore
	err error
}

func (s *failingRawGroupWatermarkStore) GroupCommittedTimestampFloor(context.Context, uint64) (uint64, error) {
	return 0, s.err
}

func TestRawGroupWatermarkProjectsKeyFreeTypedErrors(t *testing.T) {
	const privateKey = "private-physical-key"
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	backend := &failingRawGroupWatermarkStore{MVCCStore: base}
	service := NewGRPCServer(backend, nil)
	for _, tc := range []struct {
		name  string
		cause error
		code  codes.Code
	}{
		{"leader unavailable", kv.ErrLeaderNotFound, codes.Unavailable},
		{"commit floor unavailable", kv.ErrTSOCommitFloorUnavailable, codes.Unavailable},
		{"leadership lost", raftengine.ErrLeadershipLost, codes.Unavailable},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"storage failure", errors.New("storage failed"), codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend.err = errors.Join(tc.cause, errors.New(privateKey))
			_, err := service.RawLatestCommitTS(t.Context(), &pb.RawLatestCommitTSRequest{GroupId: 7})
			require.Equal(t, tc.code, status.Code(err))
			require.NotContains(t, err.Error(), privateKey)
			require.NotContains(t, status.Convert(err).Message(), privateKey)
		})
	}
}

func TestRawGroupWatermarkErrorSurvivesGRPCWithoutKey(t *testing.T) {
	const privateKey = "private-physical-key"
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	backend := &failingRawGroupWatermarkStore{
		MVCCStore: base,
		err:       errors.Join(kv.ErrTSOCommitFloorUnavailable, errors.New(privateKey)),
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterRawKVServer(server, NewGRPCServer(backend, nil))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = pb.NewRawKVClient(conn).RawLatestCommitTS(t.Context(), &pb.RawLatestCommitTSRequest{GroupId: 7})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotContains(t, err.Error(), privateKey)
}
