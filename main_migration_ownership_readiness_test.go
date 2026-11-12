package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// enableMigrationRuntimeFlags turns on the migration opcodes for the test
// and returns node auth with peer-auth enforcement complete.
func enableMigrationRuntimeFlags(t *testing.T) nodeauth.Flags {
	t.Helper()
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "true")
	}
	return nodeauth.Flags{SplitMigrationPeerAuthEnforced: true}
}

func TestMigrationOwnershipProviderWiringRequiresAppliedConfiguration(t *testing.T) {
	migrationAuth := enableMigrationRuntimeFlags(t)
	st := store.NewMVCCStore()
	engine := migrationProviderConfigEngine{}
	tx := adapter.NewInternalMigrationTransaction(engine, nil, st, 7)
	t.Cleanup(tx.Close)
	groups := map[uint64]*kv.ShardGroup{7: {Engine: engine, Store: st, Txn: tx}}
	routes := distribution.NewEngine()
	shards := kv.NewShardStore(routes, groups)
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	server := adapter.NewDistributionServer(routes, nil,
		adapter.WithSplitMigrationGroupClientFactory(adapter.NewSplitMigrationGroupGRPCClientFactory(adapter.SplitMigrationGroupLeaderResolver(groups), &cache, t.Name())),
		adapter.WithSplitMigrationVoterFactory(adapter.NewSplitMigrationGRPCVoterFactory(groups, &cache, t.Name())),
		migrationOwnershipRefreshOption(groups, &raftGroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Engine: engine}}, &cache, t.Name(), true),
		adapter.MigrationPartitionRuntimeOption(shards, groups, &cache, t.Name(), true))
	require.True(t, kv.MigrationRequestAdmissionReady(tx))
	require.True(t, shards.MigrationReadAdmissionReady(7))
	require.True(t, server.MigrationOwnershipRefreshConfigured(7))
	require.True(t, adapter.MigrationOwnershipRuntimeHooksInstalled(tx, shards, server, 7))
	require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate), "installed hooks alone do not prove an applied configuration")
	internal := adapter.NewInternalWithEngine(tx, engine, nil, nil, adapter.WithInternalStore(st),
		adapter.WithInternalMigrationOwnershipReady(func() bool {
			return adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate)
		}))
	resp, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 7,
		Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY})
	require.NoError(t, err)
	require.False(t, resp.GetReady())
	legacy := kv.NewTransactionWithProposer(engine)
	t.Cleanup(legacy.Close)
	require.False(t, adapter.MigrationOwnershipRuntimeHooksInstalled(legacy, shards, server, 7))
	require.False(t, adapter.MigrationOwnershipRuntimeHooksInstalled(tx, nil, server, 7))
	require.False(t, adapter.MigrationOwnershipRuntimeHooksInstalled(tx, shards, nil, 7))
	zero := adapter.NewInternalMigrationTransaction(engine, nil, st, 0)
	t.Cleanup(zero.Close)
	require.False(t, kv.MigrationRequestAdmissionReady(zero))
}

func TestMigrationDisabledFlagsCloseLegacyCapabilityAndRunners(t *testing.T) {
	migrationAuth := nodeauth.Flags{SplitMigrationPeerAuthEnforced: true}
	t.Setenv(adapter.MigrationImportOpcodeEnv, "false")
	require.ErrorContains(t, migrationAuth.SplitMigrationLocalReadinessGate(t.Context()), adapter.MigrationImportOpcodeEnv)
	server := adapter.NewDistributionServer(distribution.NewEngine(), nil, adapter.WithSplitJobRunnerReady(),
		adapter.WithSplitJobRunnerReadinessGate(migrationAuth.SplitMigrationLocalReadinessGate))
	resp, err := server.GetSplitMigrationCapability(t.Context(), &pb.GetSplitMigrationCapabilityRequest{})
	require.NoError(t, err)
	require.False(t, resp.GetMigrationCapable())
	require.NotContains(t, resp.GetCapabilities(), adapter.SplitMigrationCapabilityV2)
	for _, start := range []func(context.Context, *errgroup.Group, *adapter.DistributionServer, adapter.MigrationReadinessGate){startDistributionSplitJobRunner, adapter.StartMigrationOwnershipRefresh} {
		assertMigrationCoverageRunnerNotStarted(t, start, server, migrationAuth.SplitMigrationLocalReadinessGate)
	}
}

func assertMigrationCoverageRunnerNotStarted(t *testing.T, start func(context.Context, *errgroup.Group, *adapter.DistributionServer, adapter.MigrationReadinessGate), server *adapter.DistributionServer, ready adapter.MigrationReadinessGate) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	group := new(errgroup.Group)
	start(ctx, group, server, ready)
	done := make(chan error, 1)
	go func() { done <- group.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("disabled migration flags must not start a migration runner")
	}
}

func TestMigrationCoverageRPCGateLeavesOrdinaryAndResolutionPathsAvailable(t *testing.T) {
	var migrationAuth nodeauth.Flags
	t.Setenv(adapter.MigrationImportOpcodeEnv, "false")
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName, pb.Distribution_AbandonSplitJob_FullMethodName, pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName, pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName, pb.Internal_CleanupMigration_FullMethodName, pb.Internal_IssueMigrationTimestamp_FullMethodName,
	} {
		_, err := adapter.MigrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			t.Fatal("disabled migration reached its handler")
			return nil, nil
		}, migrationAuth.SplitMigrationLocalReadinessGate)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), method)
	}
	for _, method := range []string{pb.Internal_Forward_FullMethodName, pb.Internal_ForwardAdminProposal_FullMethodName,
		pb.Distribution_SplitRange_FullMethodName, pb.Distribution_ListRoutes_FullMethodName, pb.Distribution_AttestMigrationOwnership_FullMethodName,
		pb.RawKV_RawPut_FullMethodName} {
		called := false
		_, err := adapter.MigrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			called = true
			return nil, nil
		}, migrationAuth.SplitMigrationLocalReadinessGate)
		require.NoError(t, err)
		require.True(t, called, method)
	}
}

func TestMigrationCoverageRPCGateUsesLocalFlags(t *testing.T) {
	migrationAuth := enableMigrationRuntimeFlags(t)
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName, pb.Distribution_AbandonSplitJob_FullMethodName, pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName, pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName, pb.Internal_CleanupMigration_FullMethodName, pb.Internal_IssueMigrationTimestamp_FullMethodName,
	} {
		called := false
		_, err := adapter.MigrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			called = true
			return nil, nil
		}, migrationAuth.SplitMigrationLocalReadinessGate)
		require.NoError(t, err, method)
		require.True(t, called, method)
	}
}

func TestMigrationOwnershipRuntimeRequiresInstalledHooksFlagsAndAppliedConfiguration(t *testing.T) {
	migrationAuth := enableMigrationRuntimeFlags(t)
	st := store.NewMVCCStore()
	node := newOwnershipRefreshRaftNode(t, "readiness", 7, st, t.Name(), migrationAuth.SplitMigrationLocalReadinessGate)
	engine := node.group.Engine
	tx := adapter.NewInternalMigrationTransaction(engine, nil, st, 7)
	t.Cleanup(tx.Close)
	node.group.Txn = tx
	groups := map[uint64]*kv.ShardGroup{7: node.group}
	routes := distribution.NewEngine()
	shards := kv.NewShardStore(routes, groups)
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	server := adapter.NewDistributionServer(routes, nil,
		adapter.WithSplitMigrationGroupClientFactory(adapter.NewSplitMigrationGroupGRPCClientFactory(adapter.SplitMigrationGroupLeaderResolver(groups), &cache, t.Name())),
		adapter.WithSplitMigrationVoterFactory(adapter.NewSplitMigrationGRPCVoterFactory(groups, &cache, t.Name())),
		migrationOwnershipRefreshOption(groups, &raftGroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Engine: engine}}, &cache, t.Name(), true),
		adapter.MigrationPartitionRuntimeOption(shards, groups, &cache, t.Name(), true))
	reader, ok := engine.(raftengine.AppliedConfigurationReader)
	require.True(t, ok)
	index, supported := reader.AppliedConfigurationIndex()
	require.True(t, supported)
	require.NotZero(t, index)
	require.True(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate))
	internal := adapter.NewInternalWithEngine(tx, engine, nil, nil, adapter.WithInternalStore(st),
		adapter.WithInternalMigrationOwnershipReady(func() bool {
			return adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate)
		}))
	probe, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1,
		Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index})
	require.NoError(t, err)
	require.True(t, probe.GetReady())
	require.Equal(t, index, probe.GetAppliedConfigurationIndex())
	require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, nil, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate))
	require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, nil, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate))
	require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 0, engine, migrationAuth.SplitMigrationLocalReadinessGate))
	for _, missing := range []raftengine.Engine{nil, capabilityConfigEngine{}, migrationProviderConfigEngine{},
		migrationReadinessConfigurationOverride{Engine: engine, supported: true},
		migrationReadinessConfigurationOverride{Engine: engine, index: index}} {
		require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, missing, migrationAuth.SplitMigrationLocalReadinessGate))
	}
	legacy := kv.NewTransactionWithProposer(engine)
	t.Cleanup(legacy.Close)
	require.False(t, adapter.MigrationOwnershipRuntimeReady(legacy, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate))
	assertMigrationRuntimeFlagsRequired(t, tx, shards, server, engine, &migrationAuth)
}

func assertMigrationRuntimeFlagsRequired(t *testing.T, tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, engine raftengine.Engine, migrationAuth *nodeauth.Flags) {
	t.Helper()
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, "false")
			require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate))
			_, err := adapter.MigrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: pb.Internal_ImportRangeVersions_FullMethodName},
				func(context.Context, any) (any, error) {
					t.Fatal("disabled flags reached mutation handler")
					return nil, nil
				}, migrationAuth.SplitMigrationLocalReadinessGate)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
	migrationAuth.SplitMigrationPeerAuthEnforced = false
	require.False(t, adapter.MigrationOwnershipRuntimeReady(tx, shards, server, 7, engine, migrationAuth.SplitMigrationLocalReadinessGate))
	migrationAuth.SplitMigrationPeerAuthEnforced = true
}

func TestMigrationCoverageEnabledFlagsRetainRPCAuthenticationAndNamespaceGuards(t *testing.T) {
	migrationAuth := enableMigrationRuntimeFlags(t)
	st := store.NewMVCCStore()
	node := newOwnershipRefreshRaftNode(t, "namespace-guard", 7, st, t.Name(), migrationAuth.SplitMigrationLocalReadinessGate)
	fsm := node.fsm
	engine := &namespaceGateEngine{Engine: node.group.Engine, store: st, fsm: fsm}
	internal := adapter.NewInternalWithEngine(nil, engine, nil, nil, adapter.WithInternalStore(st),
		adapter.WithInternalMigrationProposer(engine), adapter.WithInternalMigrationImportGate(func(context.Context) error { return nil }),
		adapter.WithMigrationNamespaceAdmissionReady(func() bool {
			return kv.MigrationNamespaceInstalledVersion(st, fsm) == kv.MigrationNamespaceVersion
		}))
	request := &pb.ImportRangeVersionsRequest{JobId: 7, BracketId: 1, BatchSeq: 1,
		Versions: []*pb.MVCCVersion{{Key: []byte("k"), Value: []byte("value"), CommitTs: 30}}}
	auth, _ := adapter.SplitMigrationTokenAuth(t.Name(), false, true)
	info := &grpc.UnaryServerInfo{FullMethod: pb.Internal_ImportRangeVersions_FullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return adapter.MigrationCoverageUnaryGate(ctx, req, info, func(ctx context.Context, _ any) (any, error) {
			return internal.ImportRangeVersions(ctx, request)
		}, migrationAuth.SplitMigrationLocalReadinessGate)
	}
	_, err := auth(t.Context(), request, info, handler)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	tlsContext := peer.NewContext(t.Context(), migrationNativeTLSPeerFixture(t))
	_, err = auth(tlsContext, request, info, handler)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "TLS alone cannot authorize migration")
	wrongToken := metadata.NewIncomingContext(tlsContext, metadata.Pairs("authorization", "Bearer unrelated"))
	_, err = auth(wrongToken, request, info, handler)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "unrelated credentials cannot authorize migration")
	ctx := metadata.NewIncomingContext(tlsContext, metadata.Pairs("authorization", "Bearer "+t.Name()))
	_, err = auth(ctx, request, info, handler)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "namespace admission protocol")
	require.Zero(t, engine.proposals, "enabled flags and valid credentials cannot bypass durable namespace activation")
}
