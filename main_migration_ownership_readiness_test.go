package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
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

func enableMigrationRuntimeFlags(t *testing.T) {
	t.Helper()
	oldEnforced, oldNoAuth := *splitMigrationPeerAuthEnforced, *adminInsecureNoAuth
	*splitMigrationPeerAuthEnforced, *adminInsecureNoAuth = true, false
	t.Cleanup(func() {
		*splitMigrationPeerAuthEnforced, *adminInsecureNoAuth = oldEnforced, oldNoAuth
	})
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "true")
	}
}

func TestMigrationOwnershipProviderWiringRequiresAppliedConfiguration(t *testing.T) {
	enableMigrationRuntimeFlags(t)
	st := store.NewMVCCStore()
	engine := migrationProviderConfigEngine{}
	tx := newInternalMigrationTransaction(engine, nil, st, 7)
	t.Cleanup(tx.Close)
	groups := map[uint64]*kv.ShardGroup{7: {Engine: engine, Store: st, Txn: tx}}
	routes := distribution.NewEngine()
	shards := kv.NewShardStore(routes, groups)
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	server := adapter.NewDistributionServer(routes, nil,
		adapter.WithSplitMigrationGroupClientFactory(splitMigrationGroupClientFactory(splitMigrationGroupLeaderResolver(groups), &cache, t.Name())),
		adapter.WithSplitMigrationVoterFactory(splitMigrationVoterFactory(groups, &cache, t.Name())),
		migrationOwnershipRefreshOption(groups, &raftGroupRuntime{engine: engine}, &cache, t.Name(), true),
		migrationPartitionRuntimeOption(shards, groups, &cache, t.Name(), true))
	require.True(t, kv.MigrationRequestAdmissionReady(tx))
	require.True(t, shards.MigrationReadAdmissionReady(7))
	require.True(t, server.MigrationOwnershipRefreshConfigured(7))
	require.True(t, migrationOwnershipRuntimeHooksInstalled(tx, shards, server, 7))
	require.False(t, migrationOwnershipRuntimeReady(tx, shards, server, 7, engine), "installed hooks alone do not prove an applied configuration")
	internal := adapter.NewInternalWithEngine(tx, engine, nil, nil, adapter.WithInternalStore(st),
		adapter.WithInternalMigrationOwnershipReady(func() bool { return migrationOwnershipRuntimeReady(tx, shards, server, 7, engine) }))
	resp, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 7,
		Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY})
	require.NoError(t, err)
	require.False(t, resp.GetReady())
	legacy := kv.NewTransactionWithProposer(engine)
	t.Cleanup(legacy.Close)
	require.False(t, migrationOwnershipRuntimeHooksInstalled(legacy, shards, server, 7))
	require.False(t, migrationOwnershipRuntimeHooksInstalled(tx, nil, server, 7))
	require.False(t, migrationOwnershipRuntimeHooksInstalled(tx, shards, nil, 7))
	zero := newInternalMigrationTransaction(engine, nil, st, 0)
	t.Cleanup(zero.Close)
	require.False(t, kv.MigrationRequestAdmissionReady(zero))
}

func TestMigrationDisabledFlagsCloseLegacyCapabilityAndRunners(t *testing.T) {
	old := *splitMigrationPeerAuthEnforced
	*splitMigrationPeerAuthEnforced = true
	t.Cleanup(func() { *splitMigrationPeerAuthEnforced = old })
	t.Setenv(adapter.MigrationImportOpcodeEnv, "false")
	require.ErrorContains(t, splitMigrationLocalReadinessGate(t.Context()), adapter.MigrationImportOpcodeEnv)
	server := adapter.NewDistributionServer(distribution.NewEngine(), nil, adapter.WithSplitJobRunnerReady(),
		adapter.WithSplitJobRunnerReadinessGate(splitMigrationLocalReadinessGate))
	resp, err := server.GetSplitMigrationCapability(t.Context(), &pb.GetSplitMigrationCapabilityRequest{})
	require.NoError(t, err)
	require.False(t, resp.GetMigrationCapable())
	require.NotContains(t, resp.GetCapabilities(), adapter.SplitMigrationCapabilityV2)
	for _, start := range []func(context.Context, *errgroup.Group, *adapter.DistributionServer){startDistributionSplitJobRunner, startMigrationOwnershipRefresh} {
		assertMigrationCoverageRunnerNotStarted(t, start, server)
	}
}

func assertMigrationCoverageRunnerNotStarted(t *testing.T, start func(context.Context, *errgroup.Group, *adapter.DistributionServer), server *adapter.DistributionServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	group := new(errgroup.Group)
	start(ctx, group, server)
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
	t.Setenv(adapter.MigrationImportOpcodeEnv, "false")
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName, pb.Distribution_AbandonSplitJob_FullMethodName, pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName, pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName, pb.Internal_CleanupMigration_FullMethodName, pb.Internal_IssueMigrationTimestamp_FullMethodName,
	} {
		_, err := migrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			t.Fatal("disabled migration reached its handler")
			return nil, nil
		})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), method)
	}
	for _, method := range []string{pb.Internal_Forward_FullMethodName, pb.Internal_ForwardAdminProposal_FullMethodName,
		pb.Distribution_SplitRange_FullMethodName, pb.Distribution_ListRoutes_FullMethodName, pb.Distribution_AttestMigrationOwnership_FullMethodName,
		pb.RawKV_RawPut_FullMethodName} {
		called := false
		_, err := migrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			called = true
			return nil, nil
		})
		require.NoError(t, err)
		require.True(t, called, method)
	}
}

func TestMigrationCoverageRPCGateUsesLocalFlags(t *testing.T) {
	enableMigrationRuntimeFlags(t)
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName, pb.Distribution_AbandonSplitJob_FullMethodName, pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName, pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName, pb.Internal_CleanupMigration_FullMethodName, pb.Internal_IssueMigrationTimestamp_FullMethodName,
	} {
		called := false
		_, err := migrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			called = true
			return nil, nil
		})
		require.NoError(t, err, method)
		require.True(t, called, method)
	}
}

func TestMigrationOwnershipRuntimeRequiresInstalledHooksFlagsAndAppliedConfiguration(t *testing.T) {
	enableMigrationRuntimeFlags(t)
	st := store.NewMVCCStore()
	node := newOwnershipRefreshRaftNode(t, "readiness", 7, st, t.Name())
	engine := node.group.Engine
	tx := newInternalMigrationTransaction(engine, nil, st, 7)
	t.Cleanup(tx.Close)
	node.group.Txn = tx
	groups := map[uint64]*kv.ShardGroup{7: node.group}
	routes := distribution.NewEngine()
	shards := kv.NewShardStore(routes, groups)
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	server := adapter.NewDistributionServer(routes, nil,
		adapter.WithSplitMigrationGroupClientFactory(splitMigrationGroupClientFactory(splitMigrationGroupLeaderResolver(groups), &cache, t.Name())),
		adapter.WithSplitMigrationVoterFactory(splitMigrationVoterFactory(groups, &cache, t.Name())),
		migrationOwnershipRefreshOption(groups, &raftGroupRuntime{engine: engine}, &cache, t.Name(), true),
		migrationPartitionRuntimeOption(shards, groups, &cache, t.Name(), true))
	reader, ok := engine.(raftengine.AppliedConfigurationReader)
	require.True(t, ok)
	index, supported := reader.AppliedConfigurationIndex()
	require.True(t, supported)
	require.NotZero(t, index)
	require.True(t, migrationOwnershipRuntimeReady(tx, shards, server, 7, engine))
	internal := adapter.NewInternalWithEngine(tx, engine, nil, nil, adapter.WithInternalStore(st),
		adapter.WithInternalMigrationOwnershipReady(func() bool { return migrationOwnershipRuntimeReady(tx, shards, server, 7, engine) }))
	probe, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1,
		Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index})
	require.NoError(t, err)
	require.True(t, probe.GetReady())
	require.Equal(t, index, probe.GetAppliedConfigurationIndex())
	require.False(t, migrationOwnershipRuntimeReady(tx, nil, server, 7, engine))
	require.False(t, migrationOwnershipRuntimeReady(tx, shards, nil, 7, engine))
	require.False(t, migrationOwnershipRuntimeReady(tx, shards, server, 0, engine))
	for _, missing := range []raftengine.Engine{nil, capabilityConfigEngine{}, migrationProviderConfigEngine{},
		migrationReadinessConfigurationOverride{Engine: engine, supported: true},
		migrationReadinessConfigurationOverride{Engine: engine, index: index}} {
		require.False(t, migrationOwnershipRuntimeReady(tx, shards, server, 7, missing))
	}
	legacy := kv.NewTransactionWithProposer(engine)
	t.Cleanup(legacy.Close)
	require.False(t, migrationOwnershipRuntimeReady(legacy, shards, server, 7, engine))
	assertMigrationRuntimeFlagsRequired(t, tx, shards, server, engine)
}

func assertMigrationRuntimeFlagsRequired(t *testing.T, tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, engine raftengine.Engine) {
	t.Helper()
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, "false")
			require.False(t, migrationOwnershipRuntimeReady(tx, shards, server, 7, engine))
			_, err := migrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: pb.Internal_ImportRangeVersions_FullMethodName},
				func(context.Context, any) (any, error) {
					t.Fatal("disabled flags reached mutation handler")
					return nil, nil
				})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
	*splitMigrationPeerAuthEnforced = false
	require.False(t, migrationOwnershipRuntimeReady(tx, shards, server, 7, engine))
	*splitMigrationPeerAuthEnforced = true
}

func TestMigrationCoverageEnabledFlagsRetainRPCAuthenticationAndNamespaceGuards(t *testing.T) {
	enableMigrationRuntimeFlags(t)
	st := store.NewMVCCStore()
	node := newOwnershipRefreshRaftNode(t, "namespace-guard", 7, st, t.Name())
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
		return migrationCoverageUnaryGate(ctx, req, info, func(ctx context.Context, _ any) (any, error) {
			return internal.ImportRangeVersions(ctx, request)
		})
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
