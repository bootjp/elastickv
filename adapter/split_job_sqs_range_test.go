package adapter

import (
	"context"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDistributionServerStartSplitMigrationRejectsResolverOwnedSQSRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	require.Equal(t, []byte("!sqs|route|global"), kv.RouteKey([]byte("!sqs|msg|data|p|orders|3")))
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st)
	snapshot, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1, Start: []byte("!sqs|route|"), End: []byte("!sqs|route|z"), GroupID: 7,
	}})
	require.NoError(t, err)
	coordinator := newDistributionCoordinatorStub(st, true)
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(coordinator),
		WithDistributionKnownRaftGroups(7, 9),
		WithSplitMigrationCapabilityGate(func(context.Context) error { return nil }),
	)
	_, err = server.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		RouteId: 1, SplitKey: []byte("!sqs|route|global"), TargetGroupId: 9, ExpectedCatalogVersion: snapshot.Version,
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "resolver-owned SQS")
	require.Zero(t, coordinator.dispatchCalls)
	jobs, err := catalog.ListSplitJobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestSplitJobSQSOverlapDoesNotBlockRecovery(t *testing.T) {
	t.Parallel()
	server := NewDistributionServer(distribution.NewEngine(), nil)
	for _, phase := range []distribution.SplitJobPhase{distribution.SplitJobPhaseAbandoning, distribution.SplitJobPhaseCleanup} {
		job := distribution.SplitJob{Phase: phase, SplitKey: []byte("!sqs|route|global")}
		require.NoError(t, server.verifySplitJobMigrationRange(context.Background(), job))
	}
}

func TestSplitJobAbandonsLegacySQSOverlapWithoutLosingData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, sourceStore := store.NewMVCCStore(), store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	global, end := []byte("!sqs|route|global"), []byte("!sqs|route|z")
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 2, ParentRouteID: 1, Start: []byte("!sqs|route|"), End: global, GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: global, End: end, GroupID: 7, State: distribution.RouteStateWriteFenced},
	})
	require.NoError(t, err)
	job := splitJobForSourceGroupTest(t)
	job.SplitKey, job.Phase, job.AbandonFromPhase = global, distribution.SplitJobPhaseAbandoning, distribution.SplitJobPhaseFence
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	key := []byte("!sqs|msg|data|p|orders|3")
	require.NoError(t, sourceStore.PutAt(ctx, key, []byte("retained"), 1, 0))
	writer, ok := sourceStore.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
		JobID: job.JobID, MigrationJobID: job.JobID, RouteStart: global, RouteEnd: end,
		Armed: true, SourceWriteFence: true, MinWriteTSExclusive: 1, RetentionPinTS: 1,
	}))
	cleaner, ok := sourceStore.(store.MigrationCleaner)
	require.True(t, ok)
	source, target := &abandonSourceClient{cleaner: cleaner}, &splitMigrationClientStub{}
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(st, true)),
		WithSplitMigrationGroupClientFactory(func(context.Context, uint64) (SplitMigrationClient, error) { return source, nil }),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, target, nil
		}),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) { return target, nil }),
	)
	for range 8 {
		require.NoError(t, server.RunSplitJobRunnerOnce(ctx))
	}
	completed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseAbandoned, completed.Phase)
	sharded := kv.NewShardStore(server.engine, map[uint64]*kv.ShardGroup{7: {Store: sourceStore}})
	value, err := sharded.GetGroupAt(ctx, 7, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("retained"), value)
	txn := kv.NewTransactionWithProposer(&applyingMigrationProposer{fsm: kv.NewKvFSMWithHLC(sourceStore, kv.NewHLC(), kv.WithRouteHistory(kv.WrapDistributionEngine(server.engine), 7))})
	t.Cleanup(txn.Close)
	_, err = txn.Commit(ctx, []*pb.Request{{Ts: 100, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("available")}}}})
	require.NoError(t, err)
	value, err = sharded.GetGroupAt(ctx, 7, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("available"), value)
}

func TestSplitJobRunnerRejectsPersistedResolverOwnedSQSRange(t *testing.T) {
	t.Parallel()
	for _, phase := range []distribution.SplitJobPhase{
		distribution.SplitJobPhasePlanned, distribution.SplitJobPhaseBackfill, distribution.SplitJobPhaseFence,
		distribution.SplitJobPhaseDeltaCopy, distribution.SplitJobPhaseCutover,
	} {
		t.Run(strconv.Itoa(int(phase)), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(st)
			_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
				RouteID: 1, Start: []byte("!sqs|route|"), End: []byte("!sqs|route|z"), GroupID: 7,
			}})
			require.NoError(t, err)
			job := splitJobForSourceGroupTest(t)
			job.SplitKey, job.Phase = []byte("!sqs|route|global"), phase
			server := NewDistributionServer(distribution.NewEngine(), catalog,
				WithDistributionCoordinator(newDistributionCoordinatorStub(st, true)),
				WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
					t.Fatal("unsupported catalog interval must be rejected before migration RPCs")
					return nil, nil, nil
				}),
			)
			require.ErrorContains(t, server.runSplitJobPhase(ctx, job), "resolver-owned SQS")
		})
	}
}
