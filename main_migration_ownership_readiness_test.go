package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMigrationOwnershipProviderWiringDoesNotAuthorizeIncompleteCoverage(t *testing.T) {
	st := store.NewMVCCStore()
	engine := capabilityConfigEngine{}
	tx := newInternalMigrationTransaction(engine, nil, st, 7)
	t.Cleanup(tx.Close)
	groups := map[uint64]*kv.ShardGroup{7: {Engine: engine, Store: st}}
	routes := distribution.NewEngine()
	shards := kv.NewShardStore(routes, groups)
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	server := adapter.NewDistributionServer(routes, nil,
		adapter.WithSplitMigrationGroupClientFactory(splitMigrationGroupClientFactory(splitMigrationGroupLeaderResolver(groups), &cache, t.Name())),
		adapter.WithSplitMigrationVoterFactory(splitMigrationVoterFactory(groups, &cache, t.Name())),
		migrationOwnershipRefreshOption(groups, &raftGroupRuntime{engine: engine}, &cache, t.Name(), true))
	require.True(t, kv.MigrationRequestAdmissionReady(tx))
	require.True(t, shards.MigrationReadAdmissionReady(7))
	require.True(t, server.MigrationOwnershipRefreshConfigured(7))
	require.True(t, migrationOwnershipRuntimeHooksInstalled(tx, shards, server, 7))
	require.False(t, migrationOwnershipRuntimeReady(tx, shards, server, 7), "installed hooks are not full geometry/whole-runtime acceptance")
	internal := adapter.NewInternalWithEngine(tx, engine, nil, nil, adapter.WithInternalStore(st),
		adapter.WithInternalMigrationOwnershipReady(func() bool { return migrationOwnershipRuntimeReady(tx, shards, server, 7) }))
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

func TestMigrationIncompleteCoverageClosesLegacyCapabilityAndRunners(t *testing.T) {
	old := *splitMigrationPeerAuthEnforced
	*splitMigrationPeerAuthEnforced = true
	t.Cleanup(func() { *splitMigrationPeerAuthEnforced = old })
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "true")
	}
	require.ErrorContains(t, splitMigrationLocalReadinessGate(t.Context()), "protocol not enabled")
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
		t.Fatal("incomplete coverage must not start a migration runner")
	}
}

func TestMigrationCoverageRPCGateLeavesOrdinaryAndResolutionPathsAvailable(t *testing.T) {
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName, pb.Distribution_AbandonSplitJob_FullMethodName, pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName, pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName, pb.Internal_CleanupMigration_FullMethodName, pb.Internal_IssueMigrationTimestamp_FullMethodName,
	} {
		_, err := migrationCoverageUnaryGate(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			t.Fatal("incomplete migration reached its handler")
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
