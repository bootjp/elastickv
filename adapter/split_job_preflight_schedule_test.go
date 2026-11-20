package adapter

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestPreflightRunnerReschedulesDurablePagesWithinFiveSeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, server, catalog, job, source := transferPreflightFixture(t)
		server.splitPromotionClientFactory = func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			t.Fatal("promotion during preflight")
			return nil, nil
		}
		exports := 0
		source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
			exports++
			time.Sleep(40 * time.Millisecond)
			return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
		}
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		finished := make(chan error, 1)
		start := time.Now()
		go func() { finished <- server.RunSplitJobRunner(ctx) }()
		for ctx.Err() == nil && !job.TransferPreflightDone {
			time.Sleep(5 * time.Millisecond)
			job = loadTransferJob(t, parent, catalog, job.JobID)
		}
		cancel()
		require.NoError(t, <-finished)
		require.Positive(t, exports, "exercise the configured runner")
		require.True(t, job.TransferPreflightDone, "preflight exceeded five seconds with bounded page I/O")
		require.Less(t, time.Since(start), 5*time.Second)
		require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
		require.Zero(t, job.FenceTS, "completion must yield before the fence")
	})
}

func TestPreflightContinuationRequiresIncompleteDurableProgress(t *testing.T) {
	for _, change := range []string{"unchanged", "initialization", "checkpoint", "complete", "phase", "timestamp"} {
		t.Run(change, func(t *testing.T) {
			ctx, server, catalog, original, _ := transferPreflightFixture(t)
			require.NoError(t, server.runSplitJobPhase(ctx, original))
			previous := loadTransferJob(t, ctx, catalog, original.JobID)
			next := distribution.CloneSplitJob(previous)
			expected := change == "initialization" || change == "checkpoint"
			switch change {
			case "initialization":
				previous = original
			case "checkpoint":
				next.TransferPreflightProgress[0].Done = true
			case "complete":
				next.TransferPreflightDone = true
			case "phase":
				next.Phase = distribution.SplitJobPhaseDeltaCopy
			case "timestamp":
				next.TransferPreflightTS++
			}
			if change != "unchanged" && change != "initialization" {
				require.NoError(t, catalog.SaveSplitJob(ctx, previous, next))
			}
			fast, err := server.splitJobPreflightContinuation(ctx, previous)
			require.NoError(t, err)
			require.Equal(t, expected, fast)
		})
	}
}

func TestPreflightRunnerWaitKeepsOrdinaryBackoff(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(map[bool]string{false: "stalled", true: "progress"}[fast], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ticker := time.NewTicker(defaultSplitJobRunnerInterval)
				defer ticker.Stop()
				start := time.Now()
				require.True(t, waitSplitJobRunner(t.Context(), ticker.C, fast))
				expected := defaultSplitJobRunnerInterval
				if fast {
					expected = splitPreflightContinuationDelay
				}
				require.Equal(t, expected, time.Since(start))
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.False(t, waitSplitJobRunner(ctx, nil, false))
			})
		})
	}
}

func TestPreflightReadsBackAlreadyAppliedGuardWithoutReproposal(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	require.NotEmpty(t, source.controls)
	applied := len(source.controls)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	require.Equal(t, applied, len(source.controls), "matching all-member guard ACKs must avoid another Raft proposal")
	saved := loadTransferJob(t, ctx, catalog, job.JobID)
	require.True(t, saved.TransferPreflightProgress[0].Done)
}

func TestPreflightGuardReadbackKeepsAllMemberBarrier(t *testing.T) {
	for _, mode := range []string{"installed", "missing", "unreachable", "configuration-change"} {
		t.Run(mode, func(t *testing.T) {
			f := newTransferAdmissionFixture(t)
			peer, ok := f.snapshot.Members[1].Client.(*transferAdmissionClient)
			require.True(t, ok)
			original := peer.probe
			peer.probe = func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				resp, err := original(req)
				if req.GetKind() != pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED {
					return resp, err
				}
				switch mode {
				case "missing":
					resp.Ready = len(f.applied) > 0
				case "unreachable":
					return nil, errors.New("learner unavailable")
				case "configuration-change":
					f.snapshot.ConfigurationIndex++
				}
				return resp, err
			}
			witness, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof, splitTransferAdmissionOptions{readExistingControl: true})
			switch mode {
			case "installed":
				require.NoError(t, err)
				require.NotNil(t, witness)
				require.Empty(t, f.applied)
			case "missing":
				require.NoError(t, err)
				require.NotNil(t, witness)
				require.Len(t, f.applied, 1)
			default:
				require.Error(t, err)
				require.Empty(t, f.applied)
			}
		})
	}
}

func TestPreflightCheckpointCommitsOnlyValidatedBatch(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "read-error"}[failure], func(t *testing.T) {
			ctx, server, catalog, job, source := transferPreflightFixture(t)
			require.NoError(t, server.runSplitJobPhase(ctx, job))
			job = loadTransferJob(t, ctx, catalog, job.JobID)
			calls := 0
			source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
				calls++
				if calls == 2 {
					current := loadTransferJob(t, ctx, catalog, job.JobID)
					require.True(t, distribution.SplitJobsEquivalent(job, current), "tentative first page must not be published")
					if failure {
						return nil, context.DeadlineExceeded
					}
				}
				return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
			}
			err := server.runSplitJobPhase(ctx, job)
			current := loadTransferJob(t, ctx, catalog, job.JobID)
			if failure {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.True(t, distribution.SplitJobsEquivalent(job, current))
			} else {
				require.NoError(t, err)
				require.True(t, current.TransferPreflightProgress[0].Done)
				require.True(t, current.TransferPreflightProgress[1].Done)
			}
			expectedCalls := preflightPagesPerTick
			if failure {
				expectedCalls = 2
			}
			require.Equal(t, expectedCalls, calls)
		})
	}
}

func TestPreflightFinalExportCannotCommitAcrossCatalogChange(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	calls := 0
	source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		calls++
		if calls == 2 {
			snapshot, err := catalog.Snapshot(ctx)
			require.NoError(t, err)
			_, err = catalog.Save(ctx, snapshot.Version, snapshot.Routes)
			require.NoError(t, err)
		}
		return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
	}
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	require.True(t, distribution.SplitJobsEquivalent(job, loadTransferJob(t, ctx, catalog, job.JobID)), "catalog-only update invalidates tentative pages")
}

func TestPreflightCheckpointReadsJobVersionAndRoute(t *testing.T) {
	ctx, server, _, job, _ := transferPreflightFixture(t)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	require.Contains(t, coordinator.lastReadKeys, distribution.CatalogSplitJobKey(job.JobID))
	require.Contains(t, coordinator.lastReadKeys, distribution.CatalogVersionKey())
	require.Contains(t, coordinator.lastReadKeys, distribution.CatalogRouteKey(sourceLookupRouteID(job)))
}

func TestPreflightCheckpointOCCRejectsLateCatalogChange(t *testing.T) {
	ctx, server, catalog, job, _ := transferPreflightFixture(t)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	coordinator.beforeApply = func(context.Context, store.MVCCStore) error {
		coordinator.beforeApply = nil
		snapshot, err := catalog.Snapshot(ctx)
		require.NoError(t, err)
		_, err = catalog.Save(ctx, snapshot.Version, snapshot.Routes)
		return err
	}
	require.Error(t, server.runSplitJobPhase(ctx, job))
	require.True(t, distribution.SplitJobsEquivalent(job, loadTransferJob(t, ctx, catalog, job.JobID)))
}

func TestPreflightCheckpointResolvesLostResponseFromDurableProgress(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	coordinator.afterDispatch = func(context.Context, store.MVCCStore, uint64) error { return context.DeadlineExceeded }
	source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true, Versions: []*pb.MVCCVersion{{Key: []byte("t"), Value: []byte("v"), CommitTs: 1}}}}}, nil
	}
	require.Error(t, server.runSplitJobPhase(ctx, job))
	saved := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, uint64(1), saved.TransferPreflightProgress[0].AcceptedRows)
	require.True(t, saved.TransferPreflightProgress[1].Done)
	coordinator.afterDispatch = nil
	first := true
	source.exportFn = func(_ context.Context, req *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		if first {
			require.Equal(t, saved.TransferPreflightProgress[preflightPagesPerTick].Family, req.GetKeyFamily())
			first = false
		}
		return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
	}
	require.NoError(t, server.runSplitJobPhase(ctx, saved))
	resumed := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, uint64(1), resumed.TransferPreflightProgress[0].AcceptedRows, "never replay committed row accounting")
}

func TestPreflightOversizedCannotAbandonNewerCheckpoint(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	require.NoError(t, server.runSplitJobPhase(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	calls := 0
	source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		calls++
		if calls == 2 {
			newer := distribution.CloneSplitJob(job)
			newer.LastError = "newer runner diagnostic"
			require.NoError(t, catalog.SaveSplitJob(ctx, job, newer))
			return nil, migrationOversizedVersionError(&pb.MVCCVersion{Key: []byte("t"), CommitTs: 1, Value: make([]byte, store.MigrationPagePayloadBudget+1)})
		}
		return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
	}
	err := server.runSplitJobAttempt(ctx, job)
	require.True(t, errors.Is(err, errMigrationOversizedSourceVersion))
	current := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, job.Phase, current.Phase)
	require.Equal(t, "newer runner diagnostic", current.LastError)
	require.False(t, current.TransferPreflightProgress[0].Done)
}
