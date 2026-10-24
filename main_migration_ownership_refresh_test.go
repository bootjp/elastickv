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
	fsm      kv.FSM
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
	options := raftGRPCServerOptions(adminGRPCInterceptors{unary: []grpc.UnaryServerInterceptor{auth}})
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
	return ownershipRefreshRaftNode{group: &kv.ShardGroup{Store: st, Engine: result.Engine}, fsm: fsm, server: server, listener: listener}
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

func TestOwnershipRefreshDifferentRaftLeadersRespectExplicitClosedGate(t *testing.T) {
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
	groups := map[uint64]*kv.ShardGroup{7: data.group}
	dataRoutes := distribution.NewEngine()
	dataRoutes.UpdateRoute(nil, nil, 7)
	dataShards := kv.NewShardStore(dataRoutes, groups)
	dataTx := newInternalMigrationTransaction(data.group.Engine, nil, data.group.Store, 7)
	t.Cleanup(dataTx.Close)
	data.group.Txn = dataTx
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	groupFactory := splitMigrationGroupClientFactory(splitMigrationGroupLeaderResolver(groups), &cache, token)
	rootFactory := migrationOwnershipRootClientFactory(migrationOwnershipRootLeaderResolver(&raftGroupRuntime{engine: root.group.Engine}), &cache, token)
	mirror := distribution.NewEngine()
	require.NoError(t, mirror.ApplySnapshot(distribution.CatalogSnapshot{Version: 2, Routes: []distribution.RouteDescriptor{{RouteID: 99, GroupID: 99}}}))
	closedGate := func(context.Context) error {
		return status.Error(codes.FailedPrecondition, "synthetic migration gate closed")
	}
	consumer := adapter.NewDistributionServer(mirror, nil,
		adapter.WithSplitJobRunnerReadinessGate(closedGate),
		adapter.WithSplitMigrationGroupClientFactory(groupFactory),
		adapter.WithSplitMigrationVoterFactory(splitMigrationVoterFactory(groups, &cache, token)),
		adapter.WithMigrationOwnershipRefresh(groups, rootFactory),
		migrationPartitionRuntimeOption(dataShards, groups, &cache, token, true))
	require.True(t, migrationOwnershipRuntimeHooksInstalled(dataTx, dataShards, consumer, 7))
	pb.RegisterInternalServer(data.server, adapter.NewInternalWithEngine(nil, data.group.Engine, nil, nil,
		adapter.WithInternalStore(data.group.Store), adapter.WithInternalMigrationProposer(data.group.Engine),
		adapter.WithInternalMigrationOwnershipReady(func() bool { return closedGate(t.Context()) == nil })))
	go func() { _ = root.server.Serve(root.listener) }()
	go func() { _ = data.server.Serve(data.listener) }()
	require.Eventually(t, func() bool {
		return root.group.Engine.State() == raftengine.StateLeader && data.group.Engine.State() == raftengine.StateLeader
	}, 5*time.Second, 10*time.Millisecond)
	require.NotEqual(t, root.group.Engine.Leader().ID, data.group.Engine.Leader().ID)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dataClient, err := groupFactory(ctx, 7)
	require.NoError(t, err)
	capability, err := dataClient.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 77,
		Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY})
	require.NoError(t, err)
	require.False(t, capability.GetReady())
	index, err := kv.MigrationConfigurationIndex(ctx, data.group.Store)
	require.NoError(t, err)
	_, err = dataClient.CleanupMigration(ctx, &pb.CleanupMigrationRequest{JobId: 77,
		Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR, Ownership: prior,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	// A caller's unrelated credential cannot override the configured peer token.
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer unrelated"))
	require.ErrorContains(t, consumer.RunMigrationOwnershipRefreshOnce(ctx), "synthetic migration gate closed")
	rootClient, err := rootFactory(ctx)
	require.NoError(t, err)
	attestation, err := rootClient.AttestMigrationOwnership(ctx, &pb.AttestMigrationOwnershipRequest{JobId: 77, Prior: prior})
	require.NoError(t, err)
	require.EqualValues(t, 2, attestation.GetOwnershipProtocol())
	require.EqualValues(t, 2, attestation.GetOwnership().GetCatalogVersion())
	require.True(t, attestation.GetOwnership().GetWriteFloorKnown())
	writer, ok := data.group.Store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := writer.MigrationServingState(ctx)
	require.NoError(t, err)
	require.Empty(t, view.Ownership, "explicit closed gate must prevent all data-side ownership writes")
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
