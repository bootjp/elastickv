package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type splitRecoveryOwnerFailure struct {
	MigrationOwnershipAttestor
	catalog *distribution.CatalogStore
	version uint64
	blocked bool
	calls   int
}

func (c *splitRecoveryOwnerFailure) AttestMigrationOwnership(ctx context.Context, request *pb.AttestMigrationOwnershipRequest, options ...grpc.CallOption) (*pb.AttestMigrationOwnershipResponse, error) {
	id := request.GetPrior().GetLocalGroupId()
	if id == 0 {
		partitions, legacy, err := decodeMigrationPartitionPriors(request)
		if err != nil {
			return nil, err
		}
		prior, _ := migrationPartitionAttestationPrior(partitions, legacy)
		id = prior.LocalGroupID
	}
	snapshot, err := c.catalog.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if id == 2 && snapshot.Version > c.version && c.blocked {
		c.calls++
		return nil, status.Error(codes.Unavailable, "injected post-commit disjoint owner outage")
	}
	return c.MigrationOwnershipAttestor.AttestMigrationOwnership(ctx, request, options...)
}

func TestSplitWorkingSourceRecoveryRetriesDisjointOwnerBarrier(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	require.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
	before, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	started, err := r.client.StartSplitMigration(t.Context(), &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: before.Version, RouteId: r.ownership(t, 3)[0].RouteID,
		TargetGroupId: 1, SplitKey: []byte("x")})
	require.NoError(t, err)
	job, exists, err := r.server.catalog.SplitJob(t.Context(), started.GetJobId())
	require.NoError(t, err)
	require.True(t, exists)
	failure := &splitRecoveryOwnerFailure{MigrationOwnershipAttestor: r.client, catalog: r.server.catalog, version: before.Version, blocked: true}
	r.server.ownershipRefresh.root = func(context.Context) (MigrationOwnershipAttestor, error) { return failure, nil }
	_, err = r.server.ensureSplitWorkingSource(t.Context(), job)
	require.Error(t, err)
	require.Positive(t, failure.calls, "the failure must occur after root geometry commits; error: %v", err)
	stored, exists, err := r.server.catalog.SplitJob(t.Context(), job.JobID)
	require.NoError(t, err)
	require.True(t, exists)
	require.NotZero(t, stored.WorkingSourceRouteID)
	committed, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Greater(t, committed.Version, before.Version)
	next, err := r.server.catalog.NextRouteIDAt(t.Context(), committed.ReadTS)
	require.NoError(t, err)
	_, err = r.server.ensureSplitWorkingSource(t.Context(), stored)
	require.Error(t, err, "a persisted route ID is not a completed owner-ACK barrier")
	failure.blocked = false
	resumed, err := r.server.ensureSplitWorkingSource(t.Context(), stored)
	require.NoError(t, err)
	require.Equal(t, stored.WorkingSourceRouteID, resumed.WorkingSourceRouteID)
	after, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, committed.Version, after.Version)
	require.Equal(t, committed.Routes, after.Routes)
	afterNext, err := r.server.catalog.NextRouteIDAt(t.Context(), after.ReadTS)
	require.NoError(t, err)
	require.Equal(t, next, afterNext)
	require.NoError(t, r.put(t, 2, "n", "recovered-disjoint-owner", 1<<40))
	r.read(t, "n", "recovered-disjoint-owner")
}
