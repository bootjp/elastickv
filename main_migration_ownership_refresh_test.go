package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/noderuntime"
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

func newOwnershipRefreshRaftNode(t *testing.T, id string, groupID uint64, st store.MVCCStore, token string, ready adapter.MigrationReadinessGate) ownershipRefreshRaftNode {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	factory := etcdraft.NewFactory(etcdraft.FactoryConfig{TickInterval: 20 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10})
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC(), kv.WithRouteHistory(nil, groupID))
	result, err := factory.Create(raftengine.FactoryConfig{LocalID: id, LocalAddress: listener.Addr().String(),
		DataDir: t.TempDir(), Peers: []raftengine.Server{{ID: id, Address: listener.Addr().String(), Suffrage: "voter"}}, Bootstrap: true, StateMachine: fsm})
	require.NoError(t, err)
	auth, _ := adapter.SplitMigrationTokenAuth(token, false, true)
	options := raftGRPCServerOptions(adminGRPCInterceptors{unary: []grpc.UnaryServerInterceptor{auth}, migrationReady: ready})
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

func TestOwnershipRefreshDifferentRaftLeadersRespectExplicitClosedGate(t *testing.T) {
	// Not parallel: internalutil.ConfigureGRPCTLS replaces the process-global
	// TLS config that every gRPC server and dialer in the process reads.
	cert, key := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, key))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := t.Name()
	rootStore, catalog, prior := ownershipRefreshRootCatalog(t)
	root := newOwnershipRefreshRaftNode(t, "root", 1, rootStore, token, nodeAuthFlags{}.splitMigrationLocalReadinessGate)
	data := newOwnershipRefreshRaftNode(t, "data", 7, store.NewMVCCStore(), token, nodeAuthFlags{}.splitMigrationLocalReadinessGate)
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
	dataTx := adapter.NewInternalMigrationTransaction(data.group.Engine, nil, data.group.Store, 7)
	t.Cleanup(dataTx.Close)
	data.group.Txn = dataTx
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	groupFactory := adapter.NewSplitMigrationGroupGRPCClientFactory(adapter.SplitMigrationGroupLeaderResolver(groups), &cache, token)
	rootFactory := adapter.NewMigrationOwnershipRootGRPCClientFactory(migrationOwnershipRootLeaderResolver(&raftGroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Engine: root.group.Engine}}), &cache, token)
	mirror := distribution.NewEngine()
	require.NoError(t, mirror.ApplySnapshot(distribution.CatalogSnapshot{Version: 2, Routes: []distribution.RouteDescriptor{{RouteID: 99, GroupID: 99}}}))
	closedGate := func(context.Context) error {
		return status.Error(codes.FailedPrecondition, "synthetic migration gate closed")
	}
	consumer := adapter.NewDistributionServer(mirror, nil,
		adapter.WithSplitJobRunnerReadinessGate(closedGate),
		adapter.WithSplitMigrationGroupClientFactory(groupFactory),
		adapter.WithSplitMigrationVoterFactory(adapter.NewSplitMigrationGRPCVoterFactory(groups, &cache, token)),
		adapter.WithMigrationOwnershipRefresh(groups, rootFactory),
		adapter.MigrationPartitionRuntimeOption(dataShards, groups, &cache, token, true))
	require.True(t, adapter.MigrationOwnershipRuntimeHooksInstalled(dataTx, dataShards, consumer, 7))
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
