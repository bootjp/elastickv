package adapter

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitJobPhaseErrorClassification(t *testing.T) {
	t.Parallel()
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, store.ErrWriteConflict, errSplitMigrationVoterBarrierIncomplete,
		kv.ErrLeaderNotFound, ErrNotLeader, raftengine.ErrNotLeader, raftengine.ErrLeadershipLost, raftengine.ErrLeadershipTransferInProgress,
		status.Error(codes.Unknown, "not leader"), status.Error(codes.Unknown, "leader not found"),
		status.Error(codes.Unavailable, "leader"), status.Error(codes.Aborted, "conflict"), status.Error(codes.ResourceExhausted, "busy")} {
		require.True(t, splitJobPhaseErrorRetryable(errors.WithStack(err)))
	}
	for _, err := range []error{errors.New("invalid durable cursor"), status.Error(codes.Unauthenticated, "token"),
		status.Error(codes.PermissionDenied, "denied"), status.Error(codes.FailedPrecondition, "witness"), status.Error(codes.DataLoss, "corrupt")} {
		require.False(t, splitJobPhaseErrorRetryable(errors.WithStack(err)))
	}
}

func TestSplitJobLeaderChurnNeverConsumesFailureBudget(t *testing.T) {
	t.Parallel()
	ctx, server, catalog, job := failedCleanupFixture(t)
	for _, leaderErr := range []error{kv.ErrLeaderNotFound, ErrNotLeader, status.Error(codes.Unknown, "not leader"), status.Error(codes.Unknown, "leader not found")} {
		server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return nil, nil, leaderErr
		}
		for range maxSplitJobPhaseFailures + 1 {
			require.Error(t, server.RunSplitJobRunnerOnce(ctx))
		}
	}
	current, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, distribution.SplitJobsEquivalent(job, current))
	require.Empty(t, server.splitJobFailures)
}

func TestSplitJobPersistentFailureIsDurableAndRetryable(t *testing.T) {
	t.Parallel()
	ctx, server, catalog, job := failedCleanupFixture(t)
	for range maxSplitJobPhaseFailures {
		require.ErrorContains(t, server.RunSplitJobRunnerOnce(ctx), "invalid split cleanup cursor")
	}
	failed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseFailed, failed.Phase)
	require.Equal(t, distribution.SplitJobPhaseCleanup, failed.RetryPhase)
	require.Contains(t, failed.LastError, "invalid split cleanup cursor")
	require.Equal(t, job.SourceRetentionPinTS, failed.SourceRetentionPinTS)
	require.Equal(t, job.FenceTS, failed.FenceTS)
	require.Equal(t, job.Cursor, failed.Cursor)
	require.NoError(t, server.RunSplitJobRunnerOnce(ctx), "FAILED jobs must stop automatic phase attempts")
	// Losing the in-memory failure budget must not resume a durable FAILED job.
	server.splitJobFailures = nil
	require.NoError(t, server.RunSplitJobRunnerOnce(ctx))
	_, err = server.RetrySplitJob(ctx, &pb.RetrySplitJobRequest{JobId: job.JobID})
	require.NoError(t, err)
	retried, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseCleanup, retried.Phase)
	require.Empty(t, retried.LastError)
	require.Equal(t, job.Cursor, retried.Cursor)
}

func TestSplitJobTransientFailureResetsPersistentFailureBudget(t *testing.T) {
	t.Parallel()
	ctx, server, catalog, job := failedCleanupFixture(t)
	for range maxSplitJobPhaseFailures - 1 {
		require.Error(t, server.RunSplitJobRunnerOnce(ctx))
	}
	original := server.splitMigrationClientFactory
	server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		return nil, nil, status.Error(codes.Unavailable, "leader changing")
	}
	require.Error(t, server.RunSplitJobRunnerOnce(ctx))
	server.splitMigrationClientFactory = original
	for range maxSplitJobPhaseFailures - 1 {
		require.Error(t, server.RunSplitJobRunnerOnce(ctx))
	}
	current, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseCleanup, current.Phase)
}

func TestSplitJobStaleFailureDoesNotOverwriteConcurrentProgress(t *testing.T) {
	t.Parallel()
	ctx, server, catalog, job := failedCleanupFixture(t)
	for range maxSplitJobPhaseFailures - 1 {
		require.Error(t, server.RunSplitJobRunnerOnce(ctx))
	}
	// Fresh route/read-drain ACKs are legitimate durable progress too. Race
	// against the current record, not the pre-ACK fixture that CAS must reject.
	observed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	job = observed
	progressed := distribution.CloneSplitJob(job)
	progressed.Cursor = distribution.CloneBytes(splitSourceCleanupDoneCursor)
	progressed.UpdatedAtMs++
	original := server.splitMigrationClientFactory
	var advance sync.Once
	server.splitMigrationClientFactory = func(ctx context.Context, current distribution.SplitJob, group uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		advance.Do(func() { require.NoError(t, catalog.SaveSplitJob(ctx, job, progressed)) })
		return original(ctx, current, group)
	}
	require.ErrorContains(t, server.RunSplitJobRunnerOnce(ctx), "invalid split cleanup cursor")
	current, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, distribution.SplitJobsEquivalent(progressed, current))
}

func failedCleanupFixture(t *testing.T) (context.Context, *DistributionServer, *distribution.CatalogStore, distribution.SplitJob) {
	t.Helper()
	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	routes := promotionCompleteDistributionRoutes()
	routes[1].StagedVisibilityActive, routes[1].MigrationJobID = false, 0
	_, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	job.TargetPromotionDone = true
	job.SourceRetentionPinTS = 1
	job.TargetClearedDescriptorAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	job.SourceCutoverAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	job.SourceReadDrainCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	job.Cursor = []byte("invalid")
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	client := &splitMigrationClientStub{}
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(&ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(st, true)}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return client, client, nil
		}),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) { return client, nil }),
	)
	return ctx, server, catalog, job
}
