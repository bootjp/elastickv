package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Test fakes also defined in adapter's split-migration peer tests; the
// wiring tests that stay in main need them too.
type splitRangeAuthRPCServer struct {
	pb.UnimplementedDistributionServer
}

func (splitRangeAuthRPCServer) SplitRange(context.Context, *pb.SplitRangeRequest) (*pb.SplitRangeResponse, error) {
	return &pb.SplitRangeResponse{CatalogVersion: 2}, nil
}

func (splitRangeAuthRPCServer) ListRoutes(context.Context, *pb.ListRoutesRequest) (*pb.ListRoutesResponse, error) {
	return &pb.ListRoutesResponse{CatalogVersion: 1}, nil
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

// Capture real server-side AuthInfo from native gRPC, rather than inventing TLS
// flags. The fixture connection stays alive until test cleanup. A client cert
// is intentionally absent: TLS confidentiality is not peer/session authority.
func migrationNativeTLSPeerFixture(t *testing.T) *peer.Peer {
	t.Helper()
	certPath, keyPath := writeSelfSignedCert(t)
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	pem, err := os.ReadFile(certPath)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pem))
	captured := make(chan *peer.Peer, 1)
	observer := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		native, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Internal, "native TLS peer missing")
		}
		captured <- native
		return next(ctx, req)
	}
	address := grpctest.Serve(t, func(server *grpc.Server) {
		healthpb.RegisterHealthServer(server, health.NewServer())
	}, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
	})), grpc.ChainUnaryInterceptor(observer))
	client := grpctest.Dial(t, address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs: roots, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
	})))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	select {
	case native := <-captured:
		info, ok := native.AuthInfo.(credentials.TLSInfo)
		require.True(t, ok)
		require.True(t, info.State.HandshakeComplete)
		require.Equal(t, uint16(tls.VersionTLS12), info.State.Version)
		require.Equal(t, credentials.PrivacyAndIntegrity, info.SecurityLevel)
		require.NotZero(t, info.State.CipherSuite)
		require.Empty(t, info.State.PeerCertificates)
		return native
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

type migrationMembershipNext struct {
	add     func()
	promote func()
	err     error
}

func (n migrationMembershipNext) PreAddMember(context.Context, string, string) error {
	n.add()
	return n.err
}

func (n migrationMembershipNext) PrePromoteMember(context.Context, string, string) error {
	n.promote()
	return n.err
}

type namespaceGateEngine struct {
	raftengine.Engine
	store         store.MVCCStore
	fsm           raftengine.StateMachine
	configuration raftengine.Configuration
	proposals     int
	index         uint64
}

func (e *namespaceGateEngine) ProposeAdmin(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	e.proposals++
	e.index++
	aware, ok := e.fsm.(raftengine.ApplyIndexAware)
	if !ok {
		return nil, kv.ErrMigrationNamespaceProtocol
	}
	aware.SetApplyIndex(e.index)
	return &raftengine.ProposalResult{CommitIndex: e.index, Response: e.fsm.Apply(data)}, nil
}

func (e *namespaceGateEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.configuration, nil
}

func (e *namespaceGateEngine) LinearizableRead(context.Context) (uint64, error) { return e.index, nil }

func (e *namespaceGateEngine) Status() raftengine.Status {
	return raftengine.Status{ConfigurationIndex: 7}
}

func (e *namespaceGateEngine) AppliedConfigurationIndex() (uint64, bool) {
	index, err := kv.MigrationConfigurationIndex(context.Background(), e.store)
	return index, err == nil && index != 0
}

type namespaceHiddenFSM struct{ raftengine.StateMachine }

type ownershipRefreshRaftNode struct {
	group    *kv.ShardGroup
	fsm      kv.FSM
	server   *grpc.Server
	listener net.Listener
}

func ownershipRefreshRootCatalog(t *testing.T) (store.MVCCStore, *distribution.CatalogStore, *pb.MigrationOwnershipProof) {
	t.Helper()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), GroupID: 7, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
	}
	_, err := catalog.Save(t.Context(), 0, routes)
	require.NoError(t, err)
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(routes[1], true)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	proof := &pb.MigrationOwnershipProof{Generation: "native-generation-for-refresh", LocalGroupId: 7, OwnerGroupId: 7,
		CatalogVersion: 1, RouteStart: []byte("m"), RouteId: 2, MinWriteTsExclusive: 100, WriteFloorKnown: true,
		Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER, DescriptorHash: digest[:]}
	// Bootstrap the root store at a later C with the migrated descriptor intact.
	// No SplitJob/history is created: the terminal job may already have been GC'd.
	routes[0].MinWriteTSExclusive = 80
	_, err = catalog.Save(t.Context(), 1, routes)
	require.NoError(t, err)
	return st, catalog, proof
}

func assertOwnershipRootRejectsAnonymous(t *testing.T, ctx context.Context, cache *kv.GRPCConnCache, address string, prior *pb.MigrationOwnershipProof) {
	t.Helper()
	conn, err := cache.ConnFor(address)
	require.NoError(t, err)
	ctx = adapter.MigrationCredentialContext(ctx)
	_, err = pb.NewDistributionClient(conn).AttestMigrationOwnership(ctx, &pb.AttestMigrationOwnershipRequest{JobId: 77, Prior: prior})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = adapter.NewMigrationOwnershipRootGRPCClientFactory(func() (string, error) { return address, nil }, cache, "")(ctx)
	require.Error(t, err, "no anonymous fallback even with a reachable root")
}

type migrationProviderConfigEngine struct{ capabilityConfigEngine }

func (migrationProviderConfigEngine) AppliedConfigurationIndex() (uint64, bool) { return 0, false }

type migrationReadinessConfigurationOverride struct {
	raftengine.Engine
	index     uint64
	supported bool
}

func (e migrationReadinessConfigurationOverride) AppliedConfigurationIndex() (uint64, bool) {
	return e.index, e.supported
}
