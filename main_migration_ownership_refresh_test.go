package main

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraft "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ownershipRefreshRaftNode struct {
	group    *kv.ShardGroup
	server   *grpc.Server
	listener net.Listener
}

func newOwnershipRefreshRaftNode(t *testing.T, id string, groupID uint64, st store.MVCCStore, token string) ownershipRefreshRaftNode {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	factory := etcdraft.NewFactory(etcdraft.FactoryConfig{TickInterval: 20 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10})
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC(), kv.WithRouteHistory(nil, groupID))
	result, err := factory.Create(raftengine.FactoryConfig{LocalID: id, LocalAddress: listener.Addr().String(),
		DataDir: t.TempDir(), Peers: []raftengine.Server{{ID: id, Address: listener.Addr().String(), Suffrage: "voter"}}, Bootstrap: true, StateMachine: fsm})
	require.NoError(t, err)
	auth, _ := adapter.SplitMigrationTokenAuth(token, false, true)
	options := append(internalutil.GRPCServerOptions(), grpc.ChainUnaryInterceptor(auth))
	server := grpc.NewServer(options...)
	if result.RegisterTransport != nil {
		result.RegisterTransport(server)
	}
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		require.NoError(t, result.Engine.Close())
		if result.Close != nil {
			require.NoError(t, result.Close())
		}
	})
	return ownershipRefreshRaftNode{group: &kv.ShardGroup{Store: st, Engine: result.Engine}, server: server, listener: listener}
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
		CatalogVersion: 1, RouteStart: []byte("m"), RouteId: 2, MinWriteTsExclusive: 100,
		Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER, DescriptorHash: digest[:]}
	// Bootstrap the root store at a later C with the migrated descriptor intact.
	// No SplitJob/history is created: the terminal job may already have been GC'd.
	routes[0].MinWriteTSExclusive = 80
	_, err = catalog.Save(t.Context(), 1, routes)
	require.NoError(t, err)
	return st, catalog, proof
}

func TestOwnershipRefreshDifferentRaftLeadersOverAuthenticatedRPC(t *testing.T) {
	cert, key := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, key))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := t.Name()
	rootStore, catalog, prior := ownershipRefreshRootCatalog(t)
	root := newOwnershipRefreshRaftNode(t, "root", 1, rootStore, token)
	data := newOwnershipRefreshRaftNode(t, "data", 7, store.NewMVCCStore(), token)
	rootRoutes := distribution.NewEngine()
	rootRoutes.UpdateRoute(nil, nil, 1)
	rootGroups := map[uint64]*kv.ShardGroup{1: root.group}
	rootShards := kv.NewShardStore(rootRoutes, rootGroups)
	coordinate := kv.NewShardedCoordinator(rootRoutes, rootGroups, 1, kv.NewHLC(), rootShards)
	pb.RegisterDistributionServer(root.server, adapter.NewDistributionServer(rootRoutes, catalog, adapter.WithDistributionCoordinator(coordinate)))
	// This fixture exercises the real FSM; production readiness is separately
	// wired to the public read/proposal hooks, never this test-only callback.
	pb.RegisterInternalServer(data.server, adapter.NewInternalWithEngine(nil, data.group.Engine, nil, nil,
		adapter.WithInternalStore(data.group.Store), adapter.WithInternalMigrationProposer(data.group.Engine),
		adapter.WithInternalMigrationOwnershipReady(func() bool { return true }),
		adapter.WithInternalMigrationCleanupGate(func(context.Context) error { return nil })))
	go func() { _ = root.server.Serve(root.listener) }()
	go func() { _ = data.server.Serve(data.listener) }()
	require.Eventually(t, func() bool {
		return root.group.Engine.State() == raftengine.StateLeader && data.group.Engine.State() == raftengine.StateLeader
	}, 5*time.Second, 10*time.Millisecond)
	require.NotEqual(t, root.group.Engine.Leader().ID, data.group.Engine.Leader().ID)
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	groups := map[uint64]*kv.ShardGroup{7: data.group}
	groupFactory := splitMigrationGroupClientFactory(splitMigrationGroupLeaderResolver(groups), &cache, token)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dataClient, err := groupFactory(ctx, 7)
	require.NoError(t, err)
	index, err := kv.MigrationConfigurationIndex(ctx, data.group.Store)
	require.NoError(t, err)
	_, err = dataClient.CleanupMigration(ctx, &pb.CleanupMigrationRequest{JobId: 77,
		Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR, Ownership: prior,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index})
	require.NoError(t, err)
	mirror := distribution.NewEngine()
	require.NoError(t, mirror.ApplySnapshot(distribution.CatalogSnapshot{Version: 2, Routes: []distribution.RouteDescriptor{{RouteID: 99, GroupID: 99}}}))
	rootFactory := migrationOwnershipRootClientFactory(migrationOwnershipRootLeaderResolver(&raftGroupRuntime{engine: root.group.Engine}), &cache, token)
	consumer := adapter.NewDistributionServer(mirror, nil,
		adapter.WithSplitJobRunnerReadinessGate(func(context.Context) error { return nil }),
		adapter.WithSplitMigrationGroupClientFactory(groupFactory),
		adapter.WithSplitMigrationVoterFactory(splitMigrationVoterFactory(groups, &cache, token)),
		adapter.WithMigrationOwnershipRefresh(groups, rootFactory))
	// A caller's unrelated credential cannot override the configured peer token.
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer unrelated"))
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(ctx))
	writer, ok := data.group.Store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := writer.MigrationServingState(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, view.Ownership[0].CatalogVersion)
	require.Equal(t, prior.GetGeneration(), view.Ownership[0].Generation)
	require.EqualValues(t, 1, view.Ownership[0].Receipts[0].CatalogVersion)
	assertOwnershipRootRejectsAnonymous(t, ctx, &cache, root.listener.Addr().String(), prior)
}

func assertOwnershipRootRejectsAnonymous(t *testing.T, ctx context.Context, cache *kv.GRPCConnCache, address string, prior *pb.MigrationOwnershipProof) {
	t.Helper()
	conn, err := cache.ConnFor(address)
	require.NoError(t, err)
	ctx = migrationCredentialContext(ctx)
	_, err = pb.NewDistributionClient(conn).AttestMigrationOwnership(ctx, &pb.AttestMigrationOwnershipRequest{JobId: 77, Prior: prior})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = migrationOwnershipRootClientFactory(func() (string, error) { return address, nil }, cache, "")(ctx)
	require.Error(t, err, "no anonymous fallback even with a reachable root")
}
