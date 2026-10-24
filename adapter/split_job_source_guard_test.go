package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type sourceGuardProbeClient struct {
	splitMigrationClientStub
	server *Internal
}

func (s *sourceGuardProbeClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	return s.server.ProbeMigrationState(ctx, req)
}

func TestSplitJobSourceRecoverySurvivesCatalogCompaction(t *testing.T) {
	t.Parallel()
	for _, ambiguous := range []bool{false, true} {
		t.Run(map[bool]string{false: "original source", true: "ambiguous guards"}[ambiguous], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			catalogStore := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(catalogStore)
			initial, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
			require.NoError(t, err)
			job := splitJobForSourceGroupTest(t)
			job.SourceGroupID = 0
			job.Phase = distribution.SplitJobPhaseCleanup
			job.SnapshotTS = catalog.LatestCommitTS()
			job.FenceTS = job.SnapshotTS
			require.NoError(t, catalog.CreateSplitJob(ctx, job))
			_, err = catalog.Save(ctx, initial.Version, []distribution.RouteDescriptor{
				{RouteID: 4, ParentRouteID: 2, Start: []byte("a"), End: []byte("f"), GroupID: 4},
				{RouteID: 5, ParentRouteID: 2, Start: []byte("f"), End: []byte("m"), GroupID: 4},
				{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
			})
			require.NoError(t, err)
			require.NoError(t, catalogStore.Compact(ctx, catalog.LatestCommitTS()))
			_, err = catalog.SnapshotAt(ctx, job.SnapshotTS)
			require.ErrorIs(t, err, store.ErrReadTSCompacted)
			clients := make(map[uint64]*sourceGuardProbeClient)
			for _, groupID := range []uint64{4, 7, 9} {
				st := store.NewMVCCStore()
				state := store.TargetStagedReadinessState{
					JobID: job.JobID, MigrationJobID: job.JobID, RouteStart: job.SplitKey, RouteEnd: []byte("z"),
					Armed: true, MinWriteTSExclusive: 1, RetentionPinTS: 1, TrackWrites: groupID == 7 || (groupID == 4 && ambiguous),
				}
				writer, ok := st.(store.MigrationTargetReadinessWriter)
				require.True(t, ok)
				require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, state))
				clients[groupID] = &sourceGuardProbeClient{server: NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))}
			}
			server := NewDistributionServer(distribution.NewEngine(), catalog,
				WithDistributionCoordinator(newDistributionCoordinatorStub(catalogStore, true)),
				WithDistributionKnownRaftGroups(4, 7, 9),
				WithSplitMigrationClientFactory(func(_ context.Context, _ distribution.SplitJob, groupID uint64) (SplitMigrationClient, SplitMigrationClient, error) {
					return clients[groupID], clients[9], nil
				}))
			_, err = server.backfillSplitJobSourceGroup(ctx, job)
			if ambiguous {
				require.ErrorContains(t, err, "multiple groups")
			} else {
				require.NoError(t, err)
			}
			persisted, found, loadErr := catalog.SplitJob(ctx, job.JobID)
			require.NoError(t, loadErr)
			require.True(t, found)
			if ambiguous {
				require.Zero(t, persisted.SourceGroupID)
			} else {
				require.Equal(t, uint64(7), persisted.SourceGroupID, "source metadata outlives catalog MVCC ancestry")
			}
		})
	}
}

func TestInternalSourceGuardProbeValidatesDurableIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		ready bool
	}{
		{"source tracker", true}, {"source fence", true}, {"target only", false}, {"wrong job", false},
		{"wrong migration", false}, {"wrong start", false}, {"wrong end", false}, {"disarmed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := store.NewMVCCStore()
			state := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, RouteStart: []byte("m"), RouteEnd: []byte("z"), Armed: true, MinWriteTSExclusive: 1, RetentionPinTS: 1, TrackWrites: true}
			switch tc.name {
			case "source tracker":
			case "source fence":
				state.TrackWrites = false
				state.SourceWriteFence = true
			case "target only":
				state.TrackWrites = false
			case "wrong job":
				state.JobID = 8
			case "wrong migration":
				state.MigrationJobID = 8
			case "wrong start":
				state.RouteStart = []byte("n")
			case "wrong end":
				state.RouteEnd = []byte("zz")
			case "disarmed":
				state.Armed = false
			}
			writer, ok := st.(store.MigrationTargetReadinessWriter)
			require.True(t, ok)
			require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, state))
			leader := &recordingInternalLeader{}
			server := NewInternalWithEngine(nil, leader, nil, nil, WithInternalStore(st))
			resp, err := server.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 7, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_GUARD_PRESENT, RouteStart: []byte("m"), RouteEnd: []byte("z")})
			require.NoError(t, err)
			require.Equal(t, tc.ready, resp.GetReady())
			require.Equal(t, 1, leader.linearizableReadCalls)
		})
	}
}
