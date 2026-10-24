package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type pendingTransferControlClient struct {
	SplitMigrationClient
	pending    bool
	fencedOnly bool
	probes     int
}

func (c *pendingTransferControlClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	response, err := c.SplitMigrationClient.ProbeMigrationState(ctx, req, opts...)
	if err == nil && response != nil && c.pending && (!c.fencedOnly || req.GetSourceWriteFence()) &&
		req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED && req.GetTransferSizeGuardVersion() != 0 {
		c.probes++
		response.Ready = false
	}
	return response, err
}

func TestSplitTransferPendingLearnerACKDoesNotExhaustRunnerFailureBudget(t *testing.T) {
	server, catalog, job, source := newTransferRuntimePhaseFixture(t)
	learner := installPendingTransferLearner(server, job, source)
	for range maxSplitJobPhaseFailures + 1 {
		err := server.RunSplitJobRunnerOnce(t.Context())
		require.ErrorIs(t, err, errSplitMigrationVoterBarrierIncomplete)
		current := loadTransferJob(t, t.Context(), catalog, job.JobID)
		require.Equal(t, distribution.SplitJobPhasePlanned, current.Phase)
		require.Zero(t, current.SnapshotTS)
		require.Empty(t, server.splitJobFailures)
	}
	require.Equal(t, maxSplitJobPhaseFailures+1, learner.probes)
	learner.pending = false
	require.NoError(t, server.RunSplitJobRunnerOnce(t.Context()))
	current := loadTransferJob(t, t.Context(), catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseBackfill, current.Phase)
	require.NotZero(t, current.SnapshotTS)
}

func installPendingTransferLearner(server *DistributionServer, job distribution.SplitJob, source *splitRuntimeClient) *pendingTransferControlClient {
	factory := server.splitApplyMemberFactory
	learner := &pendingTransferControlClient{SplitMigrationClient: &transferPartitionProtocolPeer{splitRuntimeClient: source}, pending: true}
	server.splitApplyMemberFactory = func(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
		snapshot, err := factory(ctx, groupID)
		if groupID == job.SourceGroupID {
			snapshot.Members = append(snapshot.Members, SplitMigrationVoter{ID: "learner", Address: "learner:1", Client: learner})
		}
		return snapshot, err
	}
	return learner
}

func TestSplitTransferPendingLearnerACKKeepsFencedJobRunnable(t *testing.T) {
	server, catalog, job, source := newTransferRuntimePhaseFixture(t)
	learner := installPendingTransferLearner(server, job, source)
	learner.fencedOnly = true
	for range len(job.BracketProgress)*2 + 3 {
		job = loadTransferJob(t, t.Context(), catalog, job.JobID)
		if job.Phase == distribution.SplitJobPhaseFence {
			break
		}
		require.NoError(t, server.RunSplitJobRunnerOnce(t.Context()))
	}
	require.Equal(t, distribution.SplitJobPhaseFence, job.Phase)
	for range len(job.BracketProgress)*2 + 3 {
		err := server.RunSplitJobRunnerOnce(t.Context())
		if learner.probes > 0 {
			require.ErrorIs(t, err, errSplitMigrationVoterBarrierIncomplete)
			break
		}
		require.NoError(t, err)
	}
	require.Positive(t, learner.probes, "the full preflight must finish before the fenced guard is tested")
	for range maxSplitJobPhaseFailures + 1 {
		err := server.RunSplitJobRunnerOnce(t.Context())
		require.ErrorIs(t, err, errSplitMigrationVoterBarrierIncomplete)
		current := loadTransferJob(t, t.Context(), catalog, job.JobID)
		require.Equal(t, distribution.SplitJobPhaseFence, current.Phase)
		_, route, err := server.splitJobSourceRoute(t.Context(), current)
		require.NoError(t, err)
		require.Equal(t, distribution.RouteStateWriteFenced, route.State)
		require.Empty(t, server.splitJobFailures)
	}
	require.GreaterOrEqual(t, learner.probes, maxSplitJobPhaseFailures+2)
	learner.pending = false
	require.NoError(t, server.RunSplitJobRunnerOnce(t.Context()))
	current := loadTransferJob(t, t.Context(), catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseDeltaCopy, current.Phase)
	require.NotZero(t, current.FenceTS)
}
