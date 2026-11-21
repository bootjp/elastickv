package adapter

import (
	"context"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
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
