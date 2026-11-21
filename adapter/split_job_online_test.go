package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOnlineSplitReplaysWritesAfterBaseAndDuringPreflight(t *testing.T) {
	t.Parallel()
	runtime := newResplitAcceptanceRuntime(t)
	afterBase, preflight := false, false
	job := runtime.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
		if job.Phase == distribution.SplitJobPhaseBackfill && len(job.BracketProgress) != 0 && job.BracketProgress[0].Done && !afterBase {
			require.Equal(t, uint32(store.OnlineMigrationVersion), job.OnlineCaptureVersion)
			require.NoError(t, runtime.put(t, 1, "n", "after-base-low-ts", 20))
			require.NoError(t, runtime.put(t, 1, "x", "new-after-base", 21))
			require.NoError(t, runtime.put(t, 1, "b", "unmoved", 1000))
			afterBase = true
		}
		if job.Phase == distribution.SplitJobPhaseFence && !job.TransferPreflightDone && !preflight {
			require.NoError(t, runtime.put(t, 1, "n", "during-preflight", 30))
			preflight = true
		}
	})
	require.True(t, afterBase)
	require.True(t, preflight)
	require.Equal(t, uint32(store.OnlineMigrationVersion), job.OnlineCaptureVersion)
	require.GreaterOrEqual(t, job.OnlineSequence, uint64(3))
	require.Equal(t, job.OnlineSequence, job.OnlineSealSequence)
	runtime.read(t, "n", "during-preflight")
	runtime.read(t, "x", "new-after-base")
	runtime.read(t, "b", "unmoved")
	source, err := store.ReadOnlineMigrationSession(t.Context(), runtime.groups[1].Store, job.JobID)
	require.NoError(t, err)
	require.True(t, source.Sealed)
	require.True(t, source.Closed)
	_, err = store.ReadOnlineMigrationFrame(t.Context(), runtime.groups[1].Store, source, 1)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	target, err := store.ReadOnlineMigrationTarget(t.Context(), runtime.groups[2].Store, job.JobID)
	require.NoError(t, err)
	require.True(t, target.Ready)
	require.True(t, target.Closed)
}

func TestOnlineSplitConfigurationChangesAfterSeal(t *testing.T) {
	t.Parallel()
	for _, group := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "source", 2: "target"}[group], func(t *testing.T) {
			runtime := newResplitAcceptanceRuntime(t)
			changed := false
			job := runtime.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
				if job.Phase != distribution.SplitJobPhaseCutover || changed {
					return
				}
				proposer, ok := runtime.clients[group].internal.migrationProposer.(*ownershipRuntimeProposer)
				require.True(t, ok)
				leader, ok := runtime.clients[group].internal.leader.(ownershipRuntimeLeader)
				require.True(t, ok)
				proposer.index++
				require.NoError(t, leader.configuration.RecordAppliedConfiguration(proposer.index))
				changed = true
			})
			require.True(t, changed)
			require.Equal(t, distribution.SplitJobPhaseDone, job.Phase)
			require.NoError(t, runtime.put(t, 2, "n", "after-config-recovery", job.FenceTS+100))
			runtime.read(t, "n", "after-config-recovery")
		})
	}
}

// A writer enters before every runner tick. Requiring an additional idle tick
// after checkpointing the final frame would prevent fencing indefinitely.
func TestOnlineSplitCatchesUpWithWritesOnEveryFenceTick(t *testing.T) {
	t.Parallel()
	runtime := newResplitAcceptanceRuntime(t)
	writes := uint64(0)
	job := runtime.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
		if job.Phase == distribution.SplitJobPhaseFence && job.TransferPreflightDone {
			writes++
			require.NoError(t, runtime.put(t, 1, "n", "continuous", 100+writes))
		}
	})
	require.Positive(t, writes)
	require.Equal(t, distribution.SplitJobPhaseDone, job.Phase)
	require.GreaterOrEqual(t, job.OnlineSealSequence, writes)
	runtime.read(t, "n", "continuous")
}

type lostOnlineTrackerResponse struct {
	*resplitAcceptanceClient
	lost bool
}

func (c *lostOnlineTrackerResponse) ApplyTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest, opts ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	response, err := c.resplitAcceptanceClient.ApplyTargetStagedReadiness(ctx, req, opts...)
	if err == nil && req.GetOnlineCaptureVersion() != 0 && !c.lost {
		c.lost = true
		return nil, status.Error(codes.Unavailable, "lost tracker response")
	}
	return response, err
}

func TestOnlineSplitLostInitialTrackerResponsePreservesGeneration(t *testing.T) {
	t.Parallel()
	r := newResplitAcceptanceRuntime(t)
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	started, err := r.client.StartSplitMigration(t.Context(), &pb.StartSplitMigrationRequest{ExpectedCatalogVersion: snapshot.Version, RouteId: 1, TargetGroupId: 2, SplitKey: []byte("m")})
	require.NoError(t, err)
	job, found, err := r.server.catalog.SplitJob(t.Context(), started.JobId)
	require.NoError(t, err)
	require.True(t, found)
	job, err = r.server.prepareSplitJobWorkingSource(t.Context(), job)
	require.NoError(t, err)
	source := &lostOnlineTrackerResponse{resplitAcceptanceClient: r.clients[1]}
	_, complete, err := r.server.armSplitSourceOwnership(t.Context(), job, 1, source)
	require.Error(t, err)
	require.False(t, complete)
	require.True(t, source.lost)
	before, err := store.ReadOnlineMigrationSession(t.Context(), r.groups[1].Store, job.JobID)
	require.NoError(t, err)
	_, complete, err = r.server.armSplitSourceOwnership(t.Context(), job, 1, source)
	require.NoError(t, err)
	require.True(t, complete)
	after, err := store.ReadOnlineMigrationSession(t.Context(), r.groups[1].Store, job.JobID)
	require.NoError(t, err)
	require.Equal(t, before.Identity, after.Identity)
	require.Equal(t, before.StartIndex, after.StartIndex)
}

type writeAfterOnlineImport struct {
	SplitMigrationClient
	write func()
}

func (c writeAfterOnlineImport) ImportRangeVersions(ctx context.Context, req *pb.ImportRangeVersionsRequest, opts ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	response, err := c.SplitMigrationClient.ImportRangeVersions(ctx, req, opts...)
	if err == nil && len(req.GetOnlineFrame()) != 0 {
		c.write()
	}
	return response, err
}

func TestOnlineSplitFencesWithWriterBetweenEveryFrameAckAndExport(t *testing.T) {
	t.Parallel()
	for _, delay := range []time.Duration{0, preflightTimePerTick + time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) { onlineSplitWriterBetweenFrames(t, delay) })
	}
}

func onlineSplitWriterBetweenFrames(t *testing.T, delay time.Duration) {
	t.Helper()
	r := newResplitAcceptanceRuntime(t)
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	started, err := r.client.StartSplitMigration(t.Context(), &pb.StartSplitMigrationRequest{ExpectedCatalogVersion: snapshot.Version, RouteId: 1, TargetGroupId: 2, SplitKey: []byte("m")})
	require.NoError(t, err)
	var job distribution.SplitJob
	seeded := false
	for range 200 {
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		var found bool
		job, found, err = r.server.catalog.SplitJob(t.Context(), started.JobId)
		require.NoError(t, err)
		require.True(t, found)
		if job.Phase == distribution.SplitJobPhaseFence && !job.TransferPreflightDone && !seeded {
			require.NoError(t, r.put(t, 1, "n", "seed-one", 100))
			require.NoError(t, r.put(t, 1, "n", "seed-two", 101))
			seeded = true
		}
		if job.Phase == distribution.SplitJobPhaseFence && job.TransferPreflightDone {
			break
		}
	}
	require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
	require.True(t, job.TransferPreflightDone)
	require.True(t, seeded)
	writes := uint64(0)
	original := r.server.splitMigrationClientFactory
	r.server.splitMigrationClientFactory = func(ctx context.Context, job distribution.SplitJob, group uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		source, target, err := original(ctx, job, group)
		if job.Phase == distribution.SplitJobPhaseFence {
			target = writeAfterOnlineImport{SplitMigrationClient: target, write: func() {
				writes++
				require.NoError(t, r.put(t, 1, "n", "between-frames", 200+writes))
				time.Sleep(delay)
			}}
		}
		return source, target, err
	}
	job = advanceOnlineSplitBeyondFence(t, r, started.JobId)
	require.Positive(t, writes)
	require.NotEqual(t, distribution.SplitJobPhaseFence, job.Phase, "ACK of the exported head must permit fencing despite the newly appended frame")
	for range 200 {
		if job.Phase == distribution.SplitJobPhaseDone {
			break
		}
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		job, _, err = r.server.catalog.SplitJob(t.Context(), started.JobId)
		require.NoError(t, err)
	}
	require.Equal(t, distribution.SplitJobPhaseDone, job.Phase)
	r.read(t, "n", "between-frames")
}

func advanceOnlineSplitBeyondFence(t *testing.T, r *resplitAcceptanceRuntime, jobID uint64) distribution.SplitJob {
	t.Helper()
	var job distribution.SplitJob
	var err error
	for range 5 {
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		job, _, err = r.server.catalog.SplitJob(t.Context(), jobID)
		require.NoError(t, err)
		if job.Phase != distribution.SplitJobPhaseFence {
			break
		}
	}
	return job
}
