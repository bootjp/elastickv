package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fenceDrainCommitClient struct {
	*splitRuntimeClient
	drained bool
}

func (s *fenceDrainCommitClient) ProbeMigrationLocks(ctx context.Context, req *pb.ProbeMigrationLocksRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationLocksResponse, error) {
	if !s.drained {
		if _, err := s.internal.transactionManager.Commit(ctx, []*pb.Request{fenceLateCommitRequest(pb.Phase_COMMIT)}); err != nil {
			return nil, err
		}
		s.drained = true
	}
	return s.splitRuntimeClient.ProbeMigrationLocks(ctx, req, opts...)
}

func fenceLateCommitRequest(phase pb.Phase) *pb.Request {
	return &pb.Request{Ts: 4, IsTxn: true, Phase: phase, ObservedRouteVersion: kv.EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("o"), CommitTS: 5, LockTTLms: 60000})},
		{Op: pb.Op_PUT, Key: []byte("o"), Value: []byte("late-commit")},
	}}
}

func TestSplitJobFenceRefreshesMinimumAfterDrainingLocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
	require.NoError(t, err)
	job := splitJobForSourceGroupTest(t)
	job.SnapshotTS = 40
	job.Phase = distribution.SplitJobPhaseFence
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	server, source, target := newSplitRuntimeRunner(t, st, catalog, job)
	// This transaction predates tracking, so only its later COMMIT may lower
	// the first tracked minimum after the source write fence is installed.
	_, err = source.internal.transactionManager.Commit(ctx, []*pb.Request{fenceLateCommitRequest(pb.Phase_PREPARE)})
	require.NoError(t, err)
	_, complete, err := server.armSplitSourceOwnership(ctx, job, job.SourceGroupID, source)
	require.NoError(t, err)
	require.True(t, complete)
	_, err = source.internal.transactionManager.Commit(ctx, []*pb.Request{{Ts: 50, ObservedRouteVersion: kv.EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("p"), Value: []byte("first-tracked")},
	}}})
	require.NoError(t, err)
	reader, ok := source.internal.store.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, uint64(50), states[0].MinAdmittedTS)
	client := &fenceDrainCommitClient{splitRuntimeClient: source}
	server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		return client, target, nil
	}
	server.splitMigrationGroupFactory = func(context.Context, uint64) (SplitMigrationClient, error) { return client, nil }
	for range len(job.BracketProgress) + 3 {
		require.NoError(t, server.finalizeSplitJobFence(ctx, job))
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		if job.Phase == distribution.SplitJobPhaseDeltaCopy {
			break
		}
	}
	persisted, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, client.drained)
	require.Equal(t, distribution.SplitJobPhaseDeltaCopy, persisted.Phase)
	require.True(t, persisted.PostFenceDrainCompleted)
	require.Equal(t, uint64(5), persisted.SnapshotMinAdmittedTS)
	require.Equal(t, uint64(4), persisted.DeltaFloor, "the late commit must be included in the durable delta-copy window")
	value, err := source.internal.store.GetAt(ctx, []byte("o"), 5)
	require.NoError(t, err)
	require.Equal(t, []byte("late-commit"), value)
	requireRuntimeTransferGuard(t, source)
}

func requireRuntimeTransferGuard(t *testing.T, source *splitRuntimeClient) {
	t.Helper()
	require.NoError(t, kv.RequireMigrationNamespaceAdmission(t.Context(), source.internal.store))
	var guard *pb.TargetStagedReadinessRequest
	for _, control := range source.controls {
		if control.GetTransferSizeGuardVersion() == 1 {
			guard = control
		}
	}
	require.NotNil(t, guard, "the real source must apply a guard, not just advertise support")
	require.True(t, guard.GetRequireConfigurationIndex())
	require.Equal(t, uint64(9), guard.GetExpectedConfigurationIndex())
	probe := probeMigrationControlRequest(guard)
	probe.RequireConfigurationIndex, probe.ExpectedConfigurationIndex = true, guard.GetExpectedConfigurationIndex()
	probe.TransferSizeGuardVersion = guard.GetTransferSizeGuardVersion()
	ack, err := source.ProbeMigrationState(t.Context(), probe)
	require.NoError(t, err)
	require.True(t, ack.GetReady(), "the real FSM/store must ACK the persisted guard control")
	require.Equal(t, guard.GetExpectedConfigurationIndex(), ack.GetAppliedConfigurationIndex())
}
