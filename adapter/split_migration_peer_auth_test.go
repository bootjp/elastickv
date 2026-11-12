package adapter

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSplitMigrationClientNeverSendsTokenOnPlaintext(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterInternalServer(server, &pb.UnimplementedInternalServer{})
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
		require.NoError(t, <-serveErr)
	})
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := newSplitMigrationInternalClient(conn, "migration-secret")
	_, err = client.CleanupMigration(ctx, &pb.CleanupMigrationRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.ErrorContains(t, err, "cannot send secure credentials")
	stream, err := client.ExportRangeVersions(ctx, &pb.ExportRangeVersionsRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.ErrorContains(t, err, "cannot send secure credentials")
	err = conn.Invoke(ctx, pb.Internal_Forward_FullMethodName, &emptypb.Empty{}, &emptypb.Empty{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "ordinary plaintext peer calls must still reach the handler")
}
