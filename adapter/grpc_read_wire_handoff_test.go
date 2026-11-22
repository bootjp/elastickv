package adapter

import (
	"context"
	"net"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type forwardedReadStore struct {
	store.MVCCStore
	client pb.RawKVClient
}

func (s *forwardedReadStore) GetAtWithReadFence(ctx context.Context, key []byte, ts, _, _ uint64) ([]byte, error) {
	response, err := s.client.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts, GroupId: 1, ReadRouteVersion: 1})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return response.GetValue(), nil
}
func TestRawGetRetriesCutoverAcrossGRPCForwarding(t *testing.T) {
	t.Run("cutover", func(t *testing.T) {
		assertRawGetRetriesRouteFailureAfterForwarding(t, kv.ErrRouteCutoverPending)
	})
	t.Run("version lag", func(t *testing.T) {
		shards := kv.NewShardStore(distribution.NewEngine(), nil)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := shards.GetAtWithReadFence(ctx, []byte("n"), 1, 1, 1)
		require.ErrorIs(t, err, kv.ErrReadRouteVersionUnavailable)
		assertRawGetRetriesRouteFailureAfterForwarding(t, err)
	})
}

func assertRawGetRetriesRouteFailureAfterForwarding(t *testing.T, firstError error) {
	t.Helper()
	source := &handoffReadStore{MVCCStore: store.NewMVCCStore(), firstError: firstError}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterRawKVServer(server, NewGRPCServer(source, &stubAdapterCoordinator{}))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///read-handoff", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		connection, err := listener.DialContext(ctx)
		return connection, errors.WithStack(err)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ingress := NewGRPCServer(&forwardedReadStore{MVCCStore: store.NewMVCCStore(), client: pb.NewRawKVClient(conn)}, &stubAdapterCoordinator{})
	response, err := ingress.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte("n"), Ts: 1})
	require.NoError(t, err)
	require.Equal(t, []byte("v"), response.GetValue())
	require.Equal(t, 2, source.attempts)
}
