package adapter

import (
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSplitJobCapabilityRegressionUsesCASedCleanupState(t *testing.T) {
	t.Parallel()
	for _, alreadyMarked := range []bool{false, true} {
		t.Run(map[bool]string{false: "new regression", true: "existing regression"}[alreadyMarked], func(t *testing.T) {
			t.Parallel()
			ctx, server, catalog, stale := failedCleanupFixture(t)
			stale.CapabilityRegressed = alreadyMarked
			current := distribution.CloneSplitJob(stale)
			current.SourceRetentionPinTS = math.MaxUint64
			current.UpdatedAtMs++
			expected, found, err := catalog.SplitJob(ctx, stale.JobID)
			require.NoError(t, err)
			require.True(t, found)
			require.NoError(t, catalog.SaveSplitJob(ctx, expected, current))
			server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
				t.Fatal("completed source/target cleanup must not rearm guards from stale input")
				return nil, nil, nil
			}
			ready, err := server.handleSplitJobCapabilityRegression(ctx, stale, errors.New("voter unavailable"))
			require.NoError(t, err)
			require.False(t, ready)
			marked, found, err := catalog.SplitJob(ctx, stale.JobID)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, marked.CapabilityRegressed)
			require.Equal(t, uint64(math.MaxUint64), marked.SourceRetentionPinTS)
		})
	}
}

func TestSplitJobHistoryPageSurvivesDeletedAnchor(t *testing.T) {
	t.Parallel()
	jobs := make([]distribution.SplitJob, 0, 205)
	for id := uint64(1); id <= 205; id++ {
		job := sampleDistributionSplitJob(id)
		job.Phase, job.TerminalAtMs = distribution.SplitJobPhaseDone, 1000
		jobs = append(jobs, job)
	}
	first, err := splitJobListPage(jobs, &pb.ListSplitJobsRequest{}, "")
	require.NoError(t, err)
	require.NotEmpty(t, first.NextPageCursor)
	// The page ended at job 6; GC removes that anchor and another older row.
	remaining := make([]distribution.SplitJob, 0, len(jobs))
	for _, job := range jobs {
		if job.JobID != 6 && job.JobID != 4 {
			remaining = append(remaining, job)
		}
	}
	second, err := splitJobListPage(remaining, &pb.ListSplitJobsRequest{PageCursor: first.NextPageCursor}, "")
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 3, 2, 1}, splitJobIDs(second.Jobs))
	last := sampleDistributionSplitJob(1)
	last.TerminalAtMs = 1000
	end, err := splitJobListPage(remaining[1:], &pb.ListSplitJobsRequest{PageCursor: encodeSplitJobListCursor(last)}, "")
	require.NoError(t, err)
	require.Empty(t, end.Jobs)
}
