package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSplitMigrationFactoriesPropagateCredentials(t *testing.T) {
	cert, key := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, key))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	internalutil.ConfigureGRPCForwardToken("migration-secret")
	t.Cleanup(func() { internalutil.ConfigureGRPCForwardToken("") })
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminPath := filepath.Join(t.TempDir(), "admin-token")
	migrationPath := filepath.Join(t.TempDir(), "migration-token")
	require.NoError(t, os.WriteFile(adminPath, []byte("read-only-admin"), 0o600))
	require.NoError(t, os.WriteFile(migrationPath, []byte("migration-secret"), 0o600))
	adminServer, auth, err := configureAdminService(adminPath, false, "", migrationPath, false, adapter.NodeIdentity{}, nil, nil)
	require.NoError(t, err)
	serverOpts := append(raftGRPCServerOptions(auth), grpc.ChainUnaryInterceptor(
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if info.FullMethod == pb.Admin_GetNodeVersion_FullMethodName {
				return handler(ctx, req)
			}
			md, _ := metadata.FromIncomingContext(ctx)
			require.Equal(t, []string{"Bearer migration-secret"}, md.Get("authorization"))
			require.Equal(t, []string{"preserved"}, md.Get("trace-id"))
			return &emptypb.Empty{}, nil
		}))
	server := grpc.NewServer(serverOpts...)
	pb.RegisterInternalServer(server, migrationAuthExportServer{})
	pb.RegisterAdminServer(server, adminServer)
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
		_, err = client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{})
		require.NoError(t, err)
		_, err = client.ProbeMigrationLocks(ctx, &pb.ProbeMigrationLocksRequest{})
		require.NoError(t, err)
		_, err = client.IssueMigrationTimestamp(ctx, &pb.IssueMigrationTimestampRequest{})
		require.NoError(t, err)
		stream, streamErr := client.ExportRangeVersions(ctx, &pb.ExportRangeVersionsRequest{})
		require.NoError(t, streamErr)
		page, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.True(t, page.GetDone())
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
	require.NoError(t, conn.Invoke(ctx, pb.Internal_Forward_FullMethodName, &pb.ForwardRequest{}, &pb.ForwardResponse{}), "normal peer forwarding uses the configured TLS credential")
	clientTLS, err := internalutil.GRPCClientTLSCredentials(cert)
	require.NoError(t, err)
	untrustedConn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(clientTLS))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, untrustedConn.Close()) })
	err = untrustedConn.Invoke(context.Background(), pb.Internal_Forward_FullMethodName, &pb.ForwardRequest{}, &pb.ForwardResponse{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "untrusted clients must not forge catalog or transaction metadata through Forward")
	_, err = pb.NewInternalClient(conn).CleanupMigration(ctx, &pb.CleanupMigrationRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "runner token must not leak into cached connections")
	adminCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer read-only-admin"))
	_, err = pb.NewInternalClient(conn).ApplyTargetStagedReadiness(adminCtx, &pb.TargetStagedReadinessRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "read-only admin token must not authorize migration fences")
	_, err = pb.NewAdminClient(conn).GetNodeVersion(adminCtx, &pb.GetNodeVersionRequest{})
	require.NoError(t, err, "read-only admin operations retain their own credential")
	migrationCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer migration-secret"))
	_, err = pb.NewAdminClient(conn).GetNodeVersion(migrationCtx, &pb.GetNodeVersionRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "migration credential must not gain unrelated Admin access")
}

func TestSplitMigrationOpcodesRequireExplicitAuthentication(t *testing.T) {
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "false")
	}
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, "true")
			_, err := loadNodeGRPCAuth("", false, "", "")
			require.ErrorContains(t, err, "migration opcodes require")
			_, err = loadNodeGRPCAuth("", true, "", "")
			require.NoError(t, err)
		})
	}
}

func TestSplitMigrationTokenMustBeDedicated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	adminPath, peerPath := filepath.Join(dir, "admin"), filepath.Join(dir, "peer")
	require.NoError(t, os.WriteFile(adminPath, []byte("read-only-admin"), 0o600))
	require.NoError(t, os.WriteFile(peerPath, []byte("blob-peer"), 0o600))
	for _, token := range []string{"read-only-admin", "blob-peer", "migration-secret"} {
		path := filepath.Join(t.TempDir(), "migration")
		require.NoError(t, os.WriteFile(path, []byte(token), 0o600))
		cfg, err := loadNodeGRPCAuth(adminPath, false, peerPath, path)
		if token != "migration-secret" {
			require.ErrorContains(t, err, "migration token must differ")
			continue
		}
		require.NoError(t, err)
		require.Equal(t, "migration-secret", cfg.migrationToken)
		_, err = loadNodeGRPCAuth("", true, "", path)
		require.ErrorContains(t, err, "mutually exclusive")
	}
	cfg, err := loadNodeGRPCAuth(adminPath, false, peerPath, "")
	require.NoError(t, err)
	require.Empty(t, cfg.migrationToken, "an Admin token must never implicitly configure migration authority")
}

type migrationAuthExportServer struct {
	pb.UnimplementedInternalServer
}

func (migrationAuthExportServer) ExportRangeVersions(_ *pb.ExportRangeVersionsRequest, stream pb.Internal_ExportRangeVersionsServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer migration-secret" {
		return status.Error(codes.Unauthenticated, "missing stream credential")
	}
	return stream.Send(&pb.ExportRangeVersionsResponse{Done: true})
}

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
