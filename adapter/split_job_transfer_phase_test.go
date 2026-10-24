package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestSplitTransferPhaseMissingFactoryBlocksTimestampAndExport(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	server.splitApplyMemberFactory = nil
	_, err := server.preflightSplitJobTransfer(ctx, job)
	require.Error(t, err)
	require.Empty(t, source.events)
	require.Empty(t, source.exports)
	current := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Zero(t, current.TransferPreflightTS)
	job.Phase, job.SnapshotTS = distribution.SplitJobPhaseBackfill, 10
	err = server.copySplitJobPhase(ctx, job, distribution.SplitJobExportPhaseBackfill, 0, 10)
	require.Error(t, err)
	require.Empty(t, source.exports)
}

func installTransferPhaseFixture(t *testing.T, server *DistributionServer, job distribution.SplitJob, source *transferSourceClient) {
	t.Helper()
	snapshot, route, err := server.splitJobSourceRoute(t.Context(), job)
	require.NoError(t, err)
	proof, err := splitSourceOwnershipProof(snapshot, job, route.GroupID, "root")
	require.NoError(t, err)
	proof.SourceAppliedIndex = 21
	source.probeFn = func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		resp := &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 1, OwnershipProtocol: 2,
			OwnershipPartitionProtocol: 1, TransferSizeGuardVersion: 1, OwnershipGeneration: "root"}
		if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED {
			resp.Ownership = proto.CloneOf(proof)
		}
		if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED {
			resp.Ready = len(source.controls) != 0 && transferFixtureControlMatches(source.controls[len(source.controls)-1], req)
		}
		return resp, nil
	}
	server.splitApplyMemberFactory = func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error) {
		return SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 1, Members: []SplitMigrationVoter{{ID: "source", Address: "source:1", Client: source}}}, nil
	}
}

func transferFixtureControlMatches(control *pb.TargetStagedReadinessRequest, req *pb.ProbeMigrationStateRequest) bool {
	expected := probeMigrationControlRequest(control)
	expected.RequireConfigurationIndex, expected.ExpectedConfigurationIndex = req.GetRequireConfigurationIndex(), req.GetExpectedConfigurationIndex()
	if req.GetTransferSizeGuardVersion() != 0 {
		expected.TransferSizeGuardVersion = control.GetTransferSizeGuardVersion()
	}
	return proto.Equal(expected, req)
}

type transferTimestampChangeClient struct {
	SplitMigrationClient
	onTimestamp func()
}

func (c *transferTimestampChangeClient) IssueMigrationTimestamp(context.Context, *pb.IssueMigrationTimestampRequest, ...grpc.CallOption) (*pb.IssueMigrationTimestampResponse, error) {
	c.onTimestamp()
	return &pb.IssueMigrationTimestampResponse{Timestamp: 100, LastCommitTs: 99}, nil
}

func TestSplitTransferPhaseConfigurationChangeAfterSnapshotDoesNotAdvance(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	witness, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.NoError(t, err)
	source := &transferTimestampChangeClient{SplitMigrationClient: f.source, onTimestamp: func() { f.snapshot.ConfigurationIndex++ }}
	// No catalog is needed: revalidation must reject before attempting a write.
	err = f.server.openSplitJobBackfill(t.Context(), f.job, source, witness)
	require.ErrorContains(t, err, "configuration changed")
}

func TestSplitTransferPhasePreflightGuardPrecedesTimestamp(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	_, err := server.preflightSplitJobTransfer(ctx, job)
	require.NoError(t, err)
	require.Equal(t, "guard", source.events[0])
	require.Equal(t, "timestamp", source.events[len(source.events)-1])
	require.Positive(t, loadTransferJob(t, ctx, catalog, job.JobID).TransferPreflightTS)
}

func newTransferRuntimePhaseFixture(t *testing.T) (*DistributionServer, *distribution.CatalogStore, distribution.SplitJob, *splitRuntimeClient) {
	t.Helper()
	root := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	catalog := distribution.NewCatalogStore(root, distribution.WithCatalogRouteDescriptorV2Writes(true))
	snapshot, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1}})
	require.NoError(t, err)
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{JobID: 1, SourceRouteID: 1, SplitKey: []byte("m"), TargetGroupID: 2}, snapshot.Routes[0], 1000)
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(t.Context(), job))
	server, source, target := newSplitRuntimeRunner(t, root, catalog, job)
	// The base has only partition interfaces; its implementation is integrated
	// by the parent worker. Stub just that independent static capability here.
	// Ownership, actual configuration, guard application and guard ACK remain
	// backed by the real source/target stores and FSMs in this focused fixture.
	sourcePeer := &transferPartitionProtocolPeer{splitRuntimeClient: source}
	targetPeer := &transferPartitionProtocolPeer{splitRuntimeClient: target}
	server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		return sourcePeer, targetPeer, nil
	}
	server.splitMigrationGroupFactory = func(context.Context, uint64) (SplitMigrationClient, error) { return sourcePeer, nil }
	server.splitMigrationVoterFactory = func(_ context.Context, groupID uint64) ([]SplitMigrationVoter, error) {
		var peer SplitMigrationClient = targetPeer
		if groupID == job.SourceGroupID {
			peer = sourcePeer
		}
		return []SplitMigrationVoter{{ID: "voter", Address: "voter:1", Client: peer}}, nil
	}
	server.splitApplyMemberFactory = func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error) {
		return SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 9, Members: []SplitMigrationVoter{{ID: "voter", Address: "voter:1", Client: sourcePeer}}}, nil
	}
	return server, catalog, job, source
}

type transferPartitionProtocolPeer struct{ *splitRuntimeClient }

func (p *transferPartitionProtocolPeer) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	resp, err := p.splitRuntimeClient.ProbeMigrationState(ctx, req, opts...)
	if resp != nil {
		resp.OwnershipPartitionProtocol = MigrationOwnershipPartitionProtocolVersion
	}
	return resp, err
}

func TestSplitTransferPhaseBeginGuardsBeforeLocksAndSnapshot(t *testing.T) {
	server, catalog, job, source := newTransferRuntimePhaseFixture(t)
	require.NoError(t, server.beginSplitJobBackfill(t.Context(), job))
	require.Equal(t, []string{"control", "guard", "locks", "timestamp"}, source.events)
	current := loadTransferJob(t, t.Context(), catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseBackfill, current.Phase)
	require.Equal(t, uint32(1), source.controls[1].GetTransferSizeGuardVersion())
	require.True(t, source.controls[1].GetRequireConfigurationIndex())
	require.Equal(t, uint64(9), source.controls[1].GetExpectedConfigurationIndex())
}

func TestSplitTransferPhaseBeginOldLearnerBlocksLocksAndSnapshot(t *testing.T) {
	server, catalog, job, source := newTransferRuntimePhaseFixture(t)
	factory := server.splitApplyMemberFactory
	server.splitApplyMemberFactory = func(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
		snapshot, err := factory(ctx, groupID)
		snapshot.Members = append(snapshot.Members, SplitMigrationVoter{ID: "learner", Address: "learner:1", Client: &splitMigrationClientStub{}})
		return snapshot, err
	}
	require.Error(t, server.beginSplitJobBackfill(t.Context(), job))
	require.Equal(t, []string{"control"}, source.events)
	require.Len(t, source.controls, 1, "plain ownership tracker is not transfer admission evidence")
	require.Zero(t, source.controls[0].GetTransferSizeGuardVersion())
	require.Equal(t, distribution.SplitJobPhasePlanned, loadTransferJob(t, t.Context(), catalog, job.JobID).Phase)
}

func TestSplitTransferPhaseCopyConfigurationChangeAfterGuardBlocksExport(t *testing.T) {
	ctx, server, _, job, source := transferPreflightFixture(t)
	job.Phase, job.SnapshotTS = distribution.SplitJobPhaseBackfill, 10
	clients := server.splitMigrationClientFactory
	members := server.splitApplyMemberFactory
	changed, calls := false, 0
	server.splitMigrationClientFactory = func(ctx context.Context, job distribution.SplitJob, groupID uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		calls++
		if calls > 1 {
			changed = true
		}
		return clients(ctx, job, groupID)
	}
	server.splitApplyMemberFactory = func(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
		snapshot, err := members(ctx, groupID)
		if changed {
			snapshot.ConfigurationIndex++
		}
		return snapshot, err
	}
	err := server.copySplitJobPhase(ctx, job, distribution.SplitJobExportPhaseBackfill, 0, 10)
	require.ErrorContains(t, err, "configuration changed")
	require.NotEmpty(t, source.controls, "guard was persisted before membership changed")
	require.Equal(t, uint32(1), source.controls[0].GetTransferSizeGuardVersion())
	require.Empty(t, source.exports)
}

func TestSplitTransferPhaseWitnessPreservesInitialOwnershipACKEpoch(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	// A bound ACK at 40 cannot be relabelled to the observed configuration 41.
	ack := encodeSplitVoterAckCursor([]splitVoterAck{{id: "voter", address: "voter:9000", acked: true, configurationIndex: 40}})
	_, err := f.server.ensureSplitJobTransferControl(context.Background(), f.job, 2, f.source, f.control, ack)
	require.Error(t, err)
	require.Empty(t, f.applied)
}
