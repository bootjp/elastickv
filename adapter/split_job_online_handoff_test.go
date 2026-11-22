package adapter

import (
	"context"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestOnlineSplitDefersHandoffForUnimportedTail(t *testing.T) {
	for _, tc := range []struct {
		name      string
		frames    int
		valueSize int
	}{{"frame budget", 9, 8}, {"byte budget", 4, 128 << 10}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newResplitAcceptanceRuntime(t)
			wroteTail, checkedRefresh := false, false
			value := strings.Repeat("x", tc.valueSize)
			job := r.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
				if checkedRefresh || !job.TransferPreflightDone {
					return
				}
				require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
				goal, err := store.DecodeOnlineMigrationManifest(job.OnlineManifest)
				require.NoError(t, err)
				if !wroteTail {
					require.Zero(t, goal.Sequence)
					for i := range tc.frames {
						require.NoError(t, r.put(t, 1, "n", value, 100+uint64(i)))
					}
					wroteTail = true
					return
				}

				// First runner attempt after the burst must checkpoint a fresh
				// goal without blocking the next write on the moving range.
				_, route, err := r.server.splitJobSourceRoute(t.Context(), job)
				require.NoError(t, err)
				require.Equal(t, distribution.RouteStateActive, route.State)
				require.Zero(t, job.OnlineSequence)
				require.Equal(t, uint64(tc.frames), goal.Sequence)
				require.NoError(t, r.put(t, 1, "x", "after-goal-refresh", 200))
				checkedRefresh = true
			})
			require.True(t, checkedRefresh)
			require.Equal(t, distribution.SplitJobPhaseDone, job.Phase)
			require.Equal(t, uint64(tc.frames+1), job.OnlineSealSequence)
			require.Equal(t, job.OnlineSealSequence, job.OnlineSequence)
			r.read(t, "n", value)
			r.read(t, "x", "after-goal-refresh")
		})
	}
}

func TestOnlineSplitGoalRefreshRacingCatalogFence(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	injected, checked := false, false
	job := r.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
		if checked || !job.TransferPreflightDone {
			return
		}
		require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
		goal, err := store.DecodeOnlineMigrationManifest(job.OnlineManifest)
		require.NoError(t, err)
		require.Zero(t, goal.Sequence)
		snapshot, route, err := r.server.splitJobSourceRoute(t.Context(), job)
		require.NoError(t, err)
		if injected {
			require.Equal(t, distribution.RouteStateWriteFenced, route.State)
			checked = true
			return
		}
		require.Equal(t, distribution.RouteStateActive, route.State)
		for i := range onlineSplitHandoffMaxFrames + 1 {
			require.NoError(t, r.put(t, 1, "n", "pending-at-fence", 100+uint64(i)))
		}
		original := r.server.splitMigrationGroupFactory
		source := &onlineSealTimingClient{resplitAcceptanceClient: r.clients[1]}
		source.afterExport = func(*pb.ExportRangeVersionsRequest) {
			r.server.splitMigrationGroupFactory = original
			_, _, err := r.server.ensureSplitJobCatalogFence(t.Context(), job, snapshot, route)
			require.NoError(t, err)
			injected = true
		}
		r.server.splitMigrationGroupFactory = func(ctx context.Context, id uint64) (SplitMigrationClient, error) {
			if id == job.SourceGroupID {
				return source, nil
			}
			return original(ctx, id)
		}
	})
	require.True(t, checked)
	require.Equal(t, uint64(onlineSplitHandoffMaxFrames+1), job.OnlineSealSequence)
	r.read(t, "n", "pending-at-fence")
}

func TestOnlineSplitLegacyFenceRejectsRefreshedGoal(t *testing.T) {
	t.Parallel()
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	require.Zero(t, job.WorkingSourceRouteID)
	_, route, err := server.splitJobSourceRoute(ctx, job)
	require.NoError(t, err)
	head := store.OnlineMigrationSession{
		Version: store.OnlineMigrationVersion, RouteStart: job.SplitKey, RouteEnd: route.End,
		Identity:   store.MigrationCaptureIdentity{SourceGroupID: route.GroupID, JobID: job.JobID, Generation: 1, Index: 1, Lineage: [32]byte{1}},
		StartIndex: 1, LastIndex: 1, ConfigurationIndex: 1,
	}
	oldJob := distribution.CloneSplitJob(job)
	oldJob.SourceGroupID = route.GroupID
	oldJob.OnlineCaptureVersion = head.Version
	oldJob.OnlineManifest, err = store.EncodeOnlineMigrationManifest(head)
	require.NoError(t, err)
	require.NoError(t, catalog.SaveSplitJob(ctx, job, oldJob))
	source.exportFn = func(ctx context.Context, req *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		if req.GetOnlineJobId() == 0 {
			return source.splitMigrationClientStub.ExportRangeVersions(ctx, req)
		}
		manifest, err := store.EncodeOnlineMigrationManifest(head)
		require.NoError(t, err)
		return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{OnlineManifest: manifest, Done: head.Sequence == 0}}}, nil
	}
	for range len(oldJob.BracketProgress) + 2 {
		if oldJob.TransferPreflightDone {
			break
		}
		_, err := server.preflightSplitTransferPages(ctx, oldJob, preflightPagesPerTick)
		require.NoError(t, err)
		oldJob = loadTransferJob(t, ctx, catalog, job.JobID)
	}
	require.True(t, oldJob.TransferPreflightDone)

	// A admits the old goal. Before A takes its fence snapshot, B observes
	// over-budget lag and commits a new goal while the route is still active.
	admitted, err := server.admitOnlineSplitHandoff(ctx, oldJob)
	require.NoError(t, err)
	require.True(t, admitted)
	head.Sequence = onlineSplitHandoffMaxFrames + 1
	head.LastIndex += head.Sequence
	head.Bytes = 4096
	admitted, err = server.admitOnlineSplitHandoff(ctx, oldJob)
	require.NoError(t, err)
	require.False(t, admitted)

	snapshot, route, err := server.splitJobSourceRoute(ctx, oldJob)
	require.NoError(t, err)
	current, found, err := catalog.SplitJobAt(ctx, job.JobID, snapshot.ReadTS)
	require.NoError(t, err)
	require.True(t, found)
	newGoal, err := store.DecodeOnlineMigrationManifest(current.OnlineManifest)
	require.NoError(t, err)
	require.Equal(t, head.Sequence, newGoal.Sequence)
	require.Equal(t, distribution.SplitJobPhaseFence, current.Phase)
	require.Equal(t, distribution.RouteStateActive, route.State)
	_, _, err = server.ensureSplitJobCatalogFence(ctx, oldJob, snapshot, route)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	after, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, snapshot.Version, after.Version)
	require.Equal(t, snapshot.Routes, after.Routes)
	require.True(t, distribution.SplitJobsEquivalent(current, loadTransferJob(t, ctx, catalog, job.JobID)))
}
