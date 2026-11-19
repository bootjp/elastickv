package adapter

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSplitPreflightBatchThroughRunner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, server, catalog, job, source := transferPreflightFixture(t)
		server.splitPromotionClientFactory = func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			t.Fatal("promotion during preflight")
			return nil, nil
		}
		// The first family needs two pages. The others can finish with an
		// empty page, which must still count against the per-tick limit.
		var families []uint32
		source.exportFn = func(_ context.Context, req *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
			families = append(families, req.GetKeyFamily())
			resp := &pb.ExportRangeVersionsResponse{Done: true}
			switch len(families) {
			case 1:
				resp = &pb.ExportRangeVersionsResponse{NextCursor: []byte("cursor")}
			case 2:
				require.Equal(t, families[0], req.GetKeyFamily())
				require.Equal(t, []byte("cursor"), req.GetCursor())
				persisted := loadTransferJob(t, ctx, catalog, job.JobID)
				require.Equal(t, req.GetCursor(), persisted.TransferPreflightProgress[0].Cursor)
			}
			return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{resp}}, nil
		}
		start := time.Now()
		for !job.TransferPreflightDone {
			require.LessOrEqual(t, time.Since(start), 5*time.Second)
			before := len(families)
			require.NoError(t, server.RunSplitJobRunnerOnce(ctx))
			require.LessOrEqual(t, len(families)-before, preflightPagesPerTick)
			job = loadTransferJob(t, ctx, catalog, job.JobID)
			require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
			require.Zero(t, job.FenceTS)
			if !job.TransferPreflightDone {
				time.Sleep(defaultSplitJobRunnerInterval)
			}
		}
		require.Len(t, families, len(job.TransferPreflightProgress)+1)
		for i, progress := range job.TransferPreflightProgress {
			require.True(t, progress.Done)
			require.Equal(t, progress.Family, families[i+1])
		}
		require.NoError(t, server.RunSplitJobRunnerOnce(ctx))
		require.Equal(t, distribution.SplitJobPhaseDeltaCopy, loadTransferJob(t, ctx, catalog, job.JobID).Phase)
	})
}

func TestSplitPreflightBatchResumesAfterBudget(t *testing.T) {
	for _, budget := range []string{"time", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, server, catalog, job, source := transferPreflightFixture(t)
				require.NoError(t, server.runSplitJobPhase(ctx, job))
				job = loadTransferJob(t, ctx, catalog, job.JobID)
				exports := 0
				source.exportFn = func(_ context.Context, req *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
					exports++
					resp := &pb.ExportRangeVersionsResponse{NextCursor: []byte("page-two")}
					if exports == 1 {
						if budget == "time" {
							time.Sleep(preflightTimePerTick)
						} else {
							resp.Versions = []*pb.MVCCVersion{{Key: []byte("n"), Value: make([]byte, preflightBytesPerTick)}}
						}
					} else {
						require.Equal(t, []byte("page-two"), req.GetCursor())
						return nil, context.DeadlineExceeded
					}
					return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{resp}}, nil
				}
				require.NoError(t, server.runSplitJobPhase(ctx, job))
				require.Equal(t, 1, exports)
				saved := loadTransferJob(t, ctx, catalog, job.JobID)
				require.Equal(t, []byte("page-two"), saved.TransferPreflightProgress[0].Cursor)
				require.ErrorIs(t, server.runSplitJobPhase(ctx, saved), context.DeadlineExceeded)
				require.Equal(t, 2, exports)
				require.True(t, distribution.SplitJobsEquivalent(saved, loadTransferJob(t, ctx, catalog, job.JobID)))
			})
		})
	}
}

func TestSplitPreflightBatchYieldsOnChangedJobOrLeader(t *testing.T) {
	for _, change := range []string{"during-export", "after-checkpoint", "leader-loss"} {
		t.Run(change, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, server, catalog, job, source := transferPreflightFixture(t)
				require.NoError(t, server.runSplitJobPhase(ctx, job))
				job = loadTransferJob(t, ctx, catalog, job.JobID)
				coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
				require.True(t, ok)
				changeJob := func() {
					current := loadTransferJob(t, ctx, catalog, job.JobID)
					next := distribution.CloneSplitJob(current)
					next.LastError = "concurrent update"
					require.NoError(t, catalog.SaveSplitJob(ctx, current, next))
				}
				if change != "during-export" {
					coordinator.afterDispatch = func(context.Context, store.MVCCStore, uint64) error {
						if change == "leader-loss" {
							coordinator.leader = false
						} else {
							changeJob()
						}
						return nil
					}
				}
				exports := 0
				source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
					exports++
					if change == "during-export" {
						changeJob()
					}
					return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
				}
				require.NoError(t, server.runSplitJobPhase(ctx, job))
				require.Equal(t, 1, exports)
				saved := loadTransferJob(t, ctx, catalog, job.JobID)
				require.Equal(t, change != "during-export", saved.TransferPreflightProgress[0].Done)
				require.False(t, saved.TransferPreflightProgress[1].Done)
				if change != "leader-loss" {
					require.Equal(t, "concurrent update", saved.LastError)
				}
			})
		})
	}
}

func TestSplitPreflightBatchChecksConfigurationBetweenPages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, server, catalog, job, source := transferPreflightFixture(t)
		require.NoError(t, server.runSplitJobPhase(ctx, job))
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
		require.True(t, ok)
		coordinator.afterDispatch = func(context.Context, store.MVCCStore, uint64) error {
			members := server.splitApplyMemberFactory
			server.splitApplyMemberFactory = func(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
				snapshot, err := members(ctx, groupID)
				snapshot.ConfigurationIndex++
				return snapshot, err
			}
			return nil
		}
		require.ErrorContains(t, server.runSplitJobPhase(ctx, job), "configuration changed")
		require.Len(t, source.exports, 1)
		saved := loadTransferJob(t, ctx, catalog, job.JobID)
		require.True(t, saved.TransferPreflightProgress[0].Done)
		require.False(t, saved.TransferPreflightProgress[1].Done)
	})
}
