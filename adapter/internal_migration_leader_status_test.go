package adapter

import (
	"errors"
	"net"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestMigrationLeaderRejectionRemainsRetryableAcrossGRPC(t *testing.T) {
	backend := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterInternalServer(server, NewInternalWithEngine(nil,
		internalAdminLeaderView(raftengine.StateFollower, 0), nil, nil, WithInternalStore(backend)))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := newSplitMigrationInternalClient(conn, "")

	_, err = client.IssueMigrationTimestamp(t.Context(), &pb.IssueMigrationTimestampRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.True(t, splitJobPhaseErrorRetryable(err))

	stream, err := client.ExportRangeVersions(t.Context(), &pb.ExportRangeVersionsRequest{
		RouteStart: []byte("a"), MaxCommitTs: 1, KeyFamily: 1,
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.True(t, splitJobPhaseErrorRetryable(err))
}

func TestMigrationReadIndexLeadershipLossIsRetryable(t *testing.T) {
	for _, cause := range []error{
		raftengine.ErrNotLeader,
		raftengine.ErrLeadershipLost,
		raftengine.ErrLeadershipTransferInProgress,
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			leader := internalAdminLeaderView(raftengine.StateLeader, 0)
			leader.ReadErr = errors.Join(cause, errors.New("private-physical-key"))
			internal := NewInternalWithEngine(nil, leader, nil, nil)
			_, err := internal.IssueMigrationTimestamp(t.Context(), &pb.IssueMigrationTimestampRequest{})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.NotContains(t, err.Error(), "private-physical-key")
			require.True(t, splitJobPhaseErrorRetryable(err))
		})
	}
}
