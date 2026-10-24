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
)

type blockedSourceOwnershipProbe struct {
	*ownershipRuntimeClient
	blockAt   int
	probes    int
	blocked   context.Context
	onBlocked func()
}

func (c *blockedSourceOwnershipProbe) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED {
		c.probes++
		if c.probes == c.blockAt {
			c.blocked = ctx
			if c.onBlocked != nil {
				c.onBlocked()
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	return c.ownershipRuntimeClient.ProbeMigrationState(ctx, req, opts...)
}

func sourceOwnershipTimeoutFixture(t *testing.T, blockAt int) (*DistributionServer, distribution.SplitJob, *blockedSourceOwnershipProbe) {
	t.Helper()
	root := store.NewMVCCStore()
	t.Cleanup(func() { _ = root.Close() })
	catalog := distribution.NewCatalogStore(root, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
	require.NoError(t, err)
	job := sampleDistributionSplitJob(11)
	job.Phase, job.SourceRouteID, job.SourceGroupID, job.TargetGroupID, job.SplitKey = distribution.SplitJobPhasePlanned, 1, 7, 9, []byte("m")
	require.NoError(t, catalog.CreateSplitJob(t.Context(), job))
	local := store.NewMVCCStore()
	t.Cleanup(func() { _ = local.Close() })
	runtime, _ := newOwnershipRuntimeClient(t, local, 7)
	source := &blockedSourceOwnershipProbe{ownershipRuntimeClient: runtime, blockAt: blockAt}
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(&ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(root, true)}),
		WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) {
			return []SplitMigrationVoter{{ID: "source", Address: "source:1", Client: source}}, nil
		}))
	return server, job, source
}

func sourceOwnershipTimeoutView(t *testing.T, source *blockedSourceOwnershipProbe) store.MigrationServingState {
	t.Helper()
	reader, ok := source.internal.store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	return view
}

func TestSplitSourceOwnershipBoundsStalledProbes(t *testing.T) {
	t.Parallel()
	for index, name := range []string{"prior", "ack"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, job, source := sourceOwnershipTimeoutFixture(t, index+1)
			ctx, cancel := context.WithTimeout(t.Context(), 3*defaultSplitMigrationAttemptTimeout)
			defer cancel()
			cursor, complete, err := server.armSplitSourceOwnership(ctx, job, 7, source)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err(), "a stalled RPC must not consume the runner's parent lifetime")
			require.True(t, splitJobPhaseErrorRetryable(err))
			require.False(t, complete)
			require.Nil(t, cursor)
			deadline, bounded := source.blocked.Deadline()
			require.True(t, bounded)
			require.True(t, time.Now().After(deadline))
			before := sourceOwnershipTimeoutView(t, source)
			require.Len(t, before.Ownership, index)
			require.Len(t, before.Readiness, index)
			saved, found, err := server.catalog.SplitJob(t.Context(), job.JobID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, distribution.SplitJobPhasePlanned, saved.Phase)
			source.blockAt = 0
			_, complete, err = server.armSplitSourceOwnership(ctx, job, 7, source)
			require.NoError(t, err)
			require.True(t, complete, "the next attempt must rediscover installed ownership")
			after := sourceOwnershipTimeoutView(t, source)
			require.Len(t, after.Ownership, 1)
			require.Len(t, after.Readiness, 1)
			require.True(t, store.MigrationOwnershipEvidenceValid(after.Ownership[0]))
			if index == 1 {
				require.True(t, before.Readiness[0].Armed)
				require.True(t, before.Readiness[0].TrackWrites)
				require.Equal(t, initialMigrationRetentionPinTS, before.Readiness[0].RetentionPinTS)
				require.Equal(t, before.Ownership[0].SourceAppliedIndex, after.Ownership[0].SourceAppliedIndex)
				require.Equal(t, before.Ownership[0].Receipts, after.Ownership[0].Receipts)
				require.Equal(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, source.cleanups[len(source.cleanups)-1].GetMode())
			}
		})
	}
}

func TestSplitSourceOwnershipBlockedProbesHonorParentCancellation(t *testing.T) {
	t.Parallel()
	for index, name := range []string{"prior", "ack"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, job, source := sourceOwnershipTimeoutFixture(t, index+1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source.onBlocked = cancel
			cursor, complete, err := server.armSplitSourceOwnership(ctx, job, 7, source)
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, splitJobPhaseErrorRetryable(err))
			require.False(t, complete)
			require.Nil(t, cursor)
			require.ErrorIs(t, source.blocked.Err(), context.Canceled)
		})
	}
}
