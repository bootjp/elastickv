package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type pendingNamespaceACKClient struct {
	SplitMigrationClient
	pending bool
	probes  int
}

func (c *pendingNamespaceACKClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	response, err := c.SplitMigrationClient.ProbeMigrationState(ctx, req, opts...)
	if err == nil && response != nil && c.pending && req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION {
		c.probes++
		response.Ready = false
	}
	return response, err
}

func TestNamespaceAdmissionPendingACKDoesNotExhaustRunnerFailureBudget(t *testing.T) {
	runtime := newResplitAcceptanceRuntime(t)
	server, catalog := runtime.server, runtime.server.catalog
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	started, err := runtime.client.StartSplitMigration(t.Context(), &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: snapshot.Version, RouteId: 1, TargetGroupId: 2, SplitKey: []byte("m")})
	require.NoError(t, err)
	job := loadTransferJob(t, t.Context(), catalog, started.GetJobId())
	factory := server.splitApplyMemberFactory
	var pending *pendingNamespaceACKClient
	server.splitApplyMemberFactory = func(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
		snapshot, err := factory(ctx, groupID)
		if groupID == job.SourceGroupID {
			if pending == nil {
				pending = &pendingNamespaceACKClient{SplitMigrationClient: snapshot.Members[0].Client, pending: true}
			}
			snapshot.Members[0].Client = pending
		}
		return snapshot, err
	}
	for range maxSplitJobPhaseFailures + 1 {
		err := server.RunSplitJobRunnerOnce(t.Context())
		require.ErrorIs(t, err, errSplitMigrationVoterBarrierIncomplete)
		current := loadTransferJob(t, t.Context(), catalog, job.JobID)
		require.Equal(t, distribution.SplitJobPhasePlanned, current.Phase)
		require.Zero(t, current.WorkingSourceRouteID, "pending namespace ACK must precede geometry effects")
		require.Empty(t, server.splitJobFailures)
	}
	require.Positive(t, pending.probes)
	pending.pending = false
	require.NoError(t, server.RunSplitJobRunnerOnce(t.Context()))
	current := loadTransferJob(t, t.Context(), catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseBackfill, current.Phase)
	require.NotZero(t, current.SnapshotTS)
}
