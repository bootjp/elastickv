package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSplitMigrationFactoriesPropagateCredentials(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unary, _ := adapter.SplitMigrationTokenAuth("migration-secret", false)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(unary,
		func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			require.Equal(t, []string{"Bearer migration-secret"}, md.Get("authorization"))
			require.Equal(t, []string{"preserved"}, md.Get("trace-id"))
			return &emptypb.Empty{}, nil
		}))
	pb.RegisterInternalServer(server, &pb.UnimplementedInternalServer{})
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
		require.NoError(t, <-serveErr)
	})
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A stale caller credential must not override the configured runner token.
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer stale", "trace-id", "preserved"))
	job := distribution.SplitJob{TargetGroupID: 2}
	resolve := func(uint64) (string, error) { return lis.Addr().String(), nil }
	source, target, err := splitMigrationClientFactory(resolve, &cache, "migration-secret")(ctx, job, 1)
	require.NoError(t, err)
	voters, err := splitMigrationVoterFactory(map[uint64]*kv.ShardGroup{
		2: {Engine: capabilityConfigEngine{cfg: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "voter", Address: lis.Addr().String(), Suffrage: raftMemberSuffrageVoter},
		}}}},
	}, &cache, "migration-secret")(ctx, 2)
	require.NoError(t, err)
	require.Len(t, voters, 1)
	for _, client := range []adapter.SplitMigrationClient{source, target, voters[0].Client} {
		_, err = client.ApplyTargetStagedReadiness(ctx, &pb.TargetStagedReadinessRequest{})
		require.NoError(t, err)
		_, err = client.ImportRangeVersions(ctx, &pb.ImportRangeVersionsRequest{})
		require.NoError(t, err)
		_, err = client.PromoteStagedVersions(ctx, &pb.PromoteStagedVersionsRequest{})
		require.NoError(t, err)
		_, err = client.CleanupMigration(ctx, &pb.CleanupMigrationRequest{})
		require.NoError(t, err)
	}
	promoter, err := splitPromotionClientFactory(func(distribution.SplitJob) (string, error) {
		return lis.Addr().String(), nil
	}, &cache, "migration-secret")(ctx, job)
	require.NoError(t, err)
	_, err = promoter.PromoteStagedVersions(ctx, &pb.PromoteStagedVersionsRequest{})
	require.NoError(t, err)
	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"Bearer stale"}, md.Get("authorization"))
	conn, err := cache.ConnFor(lis.Addr().String())
	require.NoError(t, err)
	_, err = pb.NewInternalClient(conn).CleanupMigration(ctx, &pb.CleanupMigrationRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "runner token must not leak into cached connections")
}
