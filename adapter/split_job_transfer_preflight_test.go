package adapter

import (
	"context"
	"io"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type transferSourceClient struct {
	splitMigrationClientStub
	exportFn func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error)
}

func (c *transferSourceClient) ApplyTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest, opts ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	cloned := proto.CloneOf(req)
	if len(c.controls) != 0 {
		current := c.controls[len(c.controls)-1]
		cloned.TransferSizeGuardVersion = max(cloned.GetTransferSizeGuardVersion(), current.GetTransferSizeGuardVersion())
		cloned.SourceWriteFence = cloned.GetSourceWriteFence() || current.GetSourceWriteFence()
		cloned.SourceReadFence = cloned.GetSourceReadFence() || current.GetSourceReadFence()
		cloned.MinWriteTsExclusive = max(cloned.GetMinWriteTsExclusive(), current.GetMinWriteTsExclusive())
	}
	if cloned.GetTransferSizeGuardVersion() != 0 {
		c.events = append(c.events, "guard")
	}
	return c.splitMigrationClientStub.ApplyTargetStagedReadiness(ctx, cloned, opts...)
}

func (c *transferSourceClient) ProbeMigrationLocks(ctx context.Context, req *pb.ProbeMigrationLocksRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationLocksResponse, error) {
	c.events = append(c.events, "locks")
	return c.splitMigrationClientStub.ProbeMigrationLocks(ctx, req, opts...)
}

func (c *transferSourceClient) ExportRangeVersions(ctx context.Context, req *pb.ExportRangeVersionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	if c.exportFn != nil {
		return c.exportFn(ctx, req)
	}
	return c.splitMigrationClientStub.ExportRangeVersions(ctx, req, opts...)
}

func transferPreflightFixture(t *testing.T) (context.Context, *DistributionServer, *distribution.CatalogStore, distribution.SplitJob, *transferSourceClient) {
	t.Helper()
	ctx, server, catalog, job, _, target := importReplayFixture(t)
	next := distribution.CloneSplitJob(job)
	next.Phase = distribution.SplitJobPhaseFence
	next.SnapshotTS = 10
	require.NoError(t, catalog.SaveSplitJob(ctx, job, next))
	source := &transferSourceClient{}
	server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		return source, target, nil
	}
	installTransferPhaseFixture(t, server, next, source)
	return ctx, server, catalog, next, source
}

func loadTransferJob(t *testing.T, ctx context.Context, catalog *distribution.CatalogStore, id uint64) distribution.SplitJob {
	t.Helper()
	job, found, err := catalog.SplitJob(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	return job
}

func TestTransferPreflightPersistsEveryFamilyBeforeFence(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	// Old copy cursors, even marked done, do not attest a fresh full-history scan.
	for i := range job.BracketProgress {
		job.BracketProgress[i].Done = true
	}
	require.NoError(t, server.updateSplitJobViaCoordinator(ctx, job.JobID, func(distribution.SplitJob) (distribution.SplitJob, error) { return job, nil }))
	plan, err := distribution.PlanExportBrackets(job.SplitKey, []byte("z"))
	require.NoError(t, err)
	for range len(plan) + 2 {
		require.NoError(t, server.finalizeSplitJobFence(ctx, job))
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		raw, encodeErr := distribution.EncodeSplitJob(job)
		require.NoError(t, encodeErr)
		job, err = distribution.DecodeSplitJob(raw)
		require.NoError(t, err)
		snapshot, snapshotErr := catalog.Snapshot(ctx)
		require.NoError(t, snapshotErr)
		require.Len(t, snapshot.Routes, 1, "no catalog fence before every preflight bracket finishes")
	}
	require.True(t, job.TransferPreflightDone)
	require.Len(t, source.exports, len(plan))
	for index, req := range source.exports {
		require.Equal(t, plan[index].Family, req.GetKeyFamily())
		require.Equal(t, uint64(0), req.GetMinCommitTs(), "include retained historical versions")
		require.Equal(t, job.TransferPreflightTS, req.GetMaxCommitTs())
		require.Equal(t, uint32(defaultSplitMigrationChunkBytes), req.GetChunkBytes())
	}
	require.NotEmpty(t, source.controls)
	for _, control := range source.controls {
		require.False(t, control.GetSourceWriteFence(), "preflight must not install a source write fence")
		require.Equal(t, uint32(1), control.GetTransferSizeGuardVersion())
	}
	require.NoError(t, server.finalizeSplitJobFence(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseDeltaCopy, job.Phase)
}

func TestTransferPreflightRequiresExplicitDoneAndProgress(t *testing.T) {
	for _, response := range []*pb.ExportRangeVersionsResponse{nil, {}, {NextCursor: []byte("resume")}} {
		ctx, server, catalog, job, source := transferPreflightFixture(t)
		require.NoError(t, server.finalizeSplitJobFence(ctx, job))
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
			var responses []*pb.ExportRangeVersionsResponse
			if response != nil {
				responses = append(responses, response)
			}
			return &splitMigrationStreamStub{responses: responses}, nil
		}
		err := server.finalizeSplitJobFence(ctx, job)
		switch {
		case response == nil:
			require.ErrorIs(t, err, io.EOF)
		case len(response.GetNextCursor()) == 0:
			require.ErrorContains(t, err, "did not progress")
		default:
			require.NoError(t, err)
		}
		current := loadTransferJob(t, ctx, catalog, job.JobID)
		require.False(t, current.TransferPreflightDone)
		require.False(t, current.TransferPreflightProgress[0].Done)
	}
}

func TestTransferPreflightCannotTrustIncompleteVersionedEvidence(t *testing.T) {
	job := splitJobForSourceGroupTest(t)
	plan, err := distribution.PlanExportBrackets(job.SplitKey, []byte("z"))
	require.NoError(t, err)
	job.TransferPreflightVersion = splitTransferPreflightVersion
	job.TransferPreflightTS = 100
	job.TransferPreflightProgress = distribution.SplitJobBracketProgressForPlan(plan, distribution.SplitJobExportPhaseBackfill)
	require.NoError(t, validateSplitTransferPreflight(job, plan))
	missing := distribution.CloneSplitJob(job)
	missing.TransferPreflightProgress = missing.TransferPreflightProgress[1:]
	require.Error(t, validateSplitTransferPreflight(missing, plan))
	unknown := distribution.CloneSplitJob(job)
	unknown.TransferPreflightVersion++
	require.Error(t, validateSplitTransferPreflight(unknown, plan))
	unproven := distribution.CloneSplitJob(job)
	unproven.TransferPreflightDone = true
	require.Error(t, validateSplitTransferPreflight(unproven, plan))
}

func TestTransferOversizeAutoAbandonRetainsPhaseCursorAndHistory(t *testing.T) {
	for _, phase := range []distribution.SplitJobPhase{distribution.SplitJobPhasePlanned, distribution.SplitJobPhaseBackfill, distribution.SplitJobPhaseFence, distribution.SplitJobPhaseDeltaCopy} {
		ctx, server, catalog, job, _ := transferPreflightFixture(t)
		job.Phase = phase
		job.Cursor = []byte("saved-copy-cursor")
		require.NoError(t, server.updateSplitJobViaCoordinator(ctx, job.JobID, func(distribution.SplitJob) (distribution.SplitJob, error) { return job, nil }))
		cause := errors.Mark(status.Error(codes.ResourceExhausted, "typed source oversize"), errMigrationOversizedSourceVersion)
		require.NoError(t, server.abandonOversizedSplitTransfer(ctx, job, cause))
		abandoning := loadTransferJob(t, ctx, catalog, job.JobID)
		require.Equal(t, distribution.SplitJobPhaseAbandoning, abandoning.Phase)
		require.Equal(t, phase, abandoning.AbandonFromPhase)
		require.Equal(t, job.Cursor, abandoning.Cursor)
		require.Contains(t, abandoning.LastError, "typed source oversize")
		failed, err := distribution.FailSplitJobState(abandoning, "cleanup denied", 20)
		require.NoError(t, err)
		require.Equal(t, abandoning.LastError, failed.LastError)
		retried, err := distribution.RetrySplitJobState(failed, 21)
		require.NoError(t, err)
		require.Equal(t, abandoning.LastError, retried.LastError)
		require.NoError(t, server.finishSplitJobHistory(ctx, abandoning, distribution.SplitJobPhaseAbandoned))
		history, found, err := catalog.SplitJob(ctx, job.JobID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, distribution.SplitJobPhaseAbandoned, history.Phase)
		require.Equal(t, abandoning.LastError, history.LastError)
		require.Equal(t, job.Cursor, history.Cursor)
	}
}

func TestTransferAutoAbandonCannotCrossCutoverOrCanceledCAS(t *testing.T) {
	ctx, server, catalog, job, _ := transferPreflightFixture(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	cause := errors.Mark(status.Error(codes.ResourceExhausted, "oversize"), errMigrationOversizedSourceVersion)
	require.Error(t, server.abandonOversizedSplitTransfer(canceled, job, cause))
	require.Equal(t, job.Phase, loadTransferJob(t, ctx, catalog, job.JobID).Phase)
	for _, phase := range []distribution.SplitJobPhase{distribution.SplitJobPhaseCutover, distribution.SplitJobPhaseCleanup, distribution.SplitJobPhaseDone} {
		require.NoError(t, server.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
			current.Phase = phase
			return current, nil
		}))
		require.NoError(t, server.abandonOversizedSplitTransfer(ctx, job, cause))
		require.Equal(t, phase, loadTransferJob(t, ctx, catalog, job.JobID).Phase)
	}
}

func TestTransferStaleFenceCannotRunAfterAbandon(t *testing.T) {
	ctx, server, catalog, job, _ := transferPreflightFixture(t)
	require.NoError(t, server.abandonOversizedSplitTransfer(ctx, job, errors.Mark(errors.New("oversize"), errMigrationOversizedSourceVersion)))
	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	_, _, err = server.ensureSplitJobCatalogFence(ctx, job, snapshot, snapshot.Routes[0])
	require.ErrorIs(t, err, store.ErrWriteConflict)
	snapshot, err = catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Routes, 1)
}

func TestTransferFenceTransactionDependsOnLiveJob(t *testing.T) {
	ctx, server, catalog, job, _ := transferPreflightFixture(t)
	coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	coordinator.beforeApply = func(ctx context.Context, st store.MVCCStore) error {
		next, err := distribution.BeginSplitJobAbandon(job, 900)
		if err != nil {
			return err
		}
		encoded, err := distribution.EncodeSplitJob(next)
		if err != nil {
			return err
		}
		return st.PutAt(ctx, distribution.CatalogSplitJobKey(job.JobID), encoded, coordinator.lastCommitTS+1, 0)
	}
	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	_, _, err = server.ensureSplitJobCatalogFence(ctx, job, snapshot, snapshot.Routes[0])
	require.Error(t, err)
	require.Contains(t, coordinator.lastReadKeys, distribution.CatalogSplitJobKey(job.JobID))
	snapshot, err = catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Routes, 1, "ABANDON won the transaction; no stale fence can be installed")
}

func TestTransferPreflightFindsHistoricalOversizeBeforeAnyFence(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	data := store.NewMVCCStore()
	t.Cleanup(func() { _ = data.Close() })
	require.NoError(t, data.PutAt(ctx, []byte("n"), make([]byte, store.MigrationPagePayloadBudget+(1<<19)), 5, 0))
	require.NoError(t, data.PutAt(ctx, []byte("n"), []byte("small latest"), 9, 0))
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(data))
	source.exportFn = func(ctx context.Context, req *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		capture := &captureExportRangeVersionsStream{ctx: ctx}
		if err := internal.ExportRangeVersions(req, capture); err != nil {
			return nil, err
		}
		return &splitMigrationStreamStub{responses: capture.responses}, nil
	}
	for range len(job.BracketProgress) + 3 {
		err := server.runSplitJobAttempt(ctx, job)
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		if job.Phase == distribution.SplitJobPhaseAbandoning {
			require.True(t, errors.Is(err, errMigrationOversizedSourceVersion))
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, distribution.SplitJobPhaseAbandoning, job.Phase)
	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Routes, 1)
	latest, err := data.GetAt(ctx, []byte("n"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("small latest"), latest)
}

func TestTransferPreflightRaceAfterCompletionAutoAbandonsDelta(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	data := store.NewMVCCStore()
	t.Cleanup(func() { _ = data.Close() })
	require.NoError(t, data.PutAt(ctx, []byte("n"), []byte("small"), 5, 0))
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(data))
	source.exportFn = func(ctx context.Context, req *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		capture := &captureExportRangeVersionsStream{ctx: ctx}
		if err := internal.ExportRangeVersions(req, capture); err != nil {
			return nil, err
		}
		return &splitMigrationStreamStub{responses: capture.responses}, nil
	}
	for range len(job.BracketProgress) + 3 {
		require.NoError(t, server.finalizeSplitJobFence(ctx, job))
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		if job.TransferPreflightDone {
			break
		}
	}
	require.True(t, job.TransferPreflightDone)
	require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
	// Inject an old authenticated prepared resolution after preflight. The guard
	// must not impose a retroactive limit; delta export still compensates it.
	require.NoError(t, data.PutAt(ctx, []byte("n"), make([]byte, store.MigrationPagePayloadBudget+(1<<19)), 150, 0))
	source.nextTS = 200
	require.NoError(t, server.finalizeSplitJobFence(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseDeltaCopy, job.Phase)
	err := server.runSplitJobAttempt(ctx, job)
	require.True(t, errors.Is(err, errMigrationOversizedSourceVersion))
	abandoning := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseAbandoning, abandoning.Phase)
	require.Equal(t, distribution.SplitJobPhaseDeltaCopy, abandoning.AbandonFromPhase)
	require.False(t, abandoning.TargetPromotionDone)
}
