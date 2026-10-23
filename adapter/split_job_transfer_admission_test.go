package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type transferAdmissionClient struct {
	SplitMigrationClient
	probe func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error)
	apply func(*pb.TargetStagedReadinessRequest) (*pb.TargetStagedReadinessResponse, error)
}

func (c *transferAdmissionClient) ProbeMigrationState(_ context.Context, req *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	return c.probe(req)
}

func (c *transferAdmissionClient) ApplyTargetStagedReadiness(_ context.Context, req *pb.TargetStagedReadinessRequest, _ ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	return c.apply(req)
}

type transferAdmissionFixture struct {
	server   *DistributionServer
	job      distribution.SplitJob
	control  *pb.TargetStagedReadinessRequest
	proof    *pb.MigrationOwnershipProof
	snapshot SplitMigrationApplyMemberSnapshot
	source   *transferAdmissionClient
	probes   []string
	applied  []*pb.TargetStagedReadinessRequest
}

func newTransferAdmissionFixture(t *testing.T) *transferAdmissionFixture {
	t.Helper()
	f := &transferAdmissionFixture{
		job: distribution.SplitJob{JobID: 11, SourceRouteID: 3, SplitKey: []byte("m")},
		control: &pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11, Armed: true, TrackWrites: true,
			RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 7, RetentionPinTs: 1,
			RequireConfigurationIndex: true, ExpectedConfigurationIndex: 41},
		proof: &pb.MigrationOwnershipProof{Generation: "root", LocalGroupId: 2, OwnerGroupId: 2, RouteId: 3,
			RouteStart: []byte("a"), RouteEnd: []byte("z"), SourceTrackerVersion: 7, SourceAppliedIndex: 9,
			Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER},
	}
	f.snapshot.ConfigurationIndex = 41
	for _, id := range []string{"voter", "learner"} {
		client := &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
			f.probes = append(f.probes, id)
			require.True(t, req.GetRequireConfigurationIndex())
			require.Equal(t, uint64(41), req.GetExpectedConfigurationIndex())
			if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED {
				require.Equal(t, uint32(1), req.GetTransferSizeGuardVersion(), "field 18 must bind persisted guard ACK")
				require.Equal(t, f.control.GetSourceWriteFence(), req.GetSourceWriteFence())
			}
			return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41,
				OwnershipProtocol: 2, OwnershipPartitionProtocol: 1, TransferSizeGuardVersion: 1,
				Ownership: proto.CloneOf(f.proof)}, nil
		}}
		f.snapshot.Members = append(f.snapshot.Members, SplitMigrationVoter{ID: id, Address: id + ":9000", Client: client})
	}
	f.source = &transferAdmissionClient{apply: func(req *pb.TargetStagedReadinessRequest) (*pb.TargetStagedReadinessResponse, error) {
		require.Contains(t, f.probes, "learner", "all members must attest before activation")
		require.Equal(t, uint32(1), req.GetTransferSizeGuardVersion())
		f.applied = append(f.applied, proto.CloneOf(req))
		return &pb.TargetStagedReadinessResponse{}, nil
	}}
	f.server = NewDistributionServer(nil, nil, WithSplitMigrationApplyMemberSnapshotFactory(func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error) {
		return f.snapshot, nil
	}))
	return f
}

func TestSplitTransferAdmissionAllMembersAndRevalidation(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	witness, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.NoError(t, err)
	require.Len(t, f.applied, 1)
	require.Zero(t, f.control.GetTransferSizeGuardVersion(), "caller controls must not be mutated")
	require.NoError(t, f.server.revalidateSplitTransferAdmission(t.Context(), witness))
	f.snapshot.ConfigurationIndex++
	require.Error(t, f.server.revalidateSplitTransferAdmission(t.Context(), witness))
	require.Len(t, f.applied, 1, "revalidation must never silently reapply at a new epoch")
}

func TestSplitTransferAdmissionFailsClosedBeforeApply(t *testing.T) {
	cases := map[string]func(*transferAdmissionFixture){
		"missing factory": func(f *transferAdmissionFixture) { f.server = NewDistributionServer(nil, nil) },
		"empty":           func(f *transferAdmissionFixture) { f.snapshot.Members = nil },
		"zero epoch":      func(f *transferAdmissionFixture) { f.snapshot.ConfigurationIndex = 0 },
		"changed epoch":   func(f *transferAdmissionFixture) { f.snapshot.ConfigurationIndex++ },
		"unbound control": func(f *transferAdmissionFixture) { f.control.RequireConfigurationIndex = false },
		"missing proof":   func(f *transferAdmissionFixture) { f.proof = nil },
		"duplicate":       func(f *transferAdmissionFixture) { f.snapshot.Members[1] = f.snapshot.Members[0] },
		"missing client":  func(f *transferAdmissionFixture) { f.snapshot.Members[1].Client = nil },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newTransferAdmissionFixture(t)
			change(f)
			_, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
			require.Error(t, err)
			require.Empty(t, f.applied)
		})
	}
}

func TestSplitTransferAdmissionRejectsMemberEvidence(t *testing.T) {
	cases := map[string]func(*pb.ProbeMigrationStateResponse){
		"old size":      func(r *pb.ProbeMigrationStateResponse) { r.TransferSizeGuardVersion = 0 },
		"old partition": func(r *pb.ProbeMigrationStateResponse) { r.OwnershipPartitionProtocol = 0 },
		"wrong config":  func(r *pb.ProbeMigrationStateResponse) { r.AppliedConfigurationIndex++ },
		"not ready":     func(r *pb.ProbeMigrationStateResponse) { r.Ready = false },
		"old proof":     func(r *pb.ProbeMigrationStateResponse) { r.Ownership.SourceAppliedIndex++ },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newTransferAdmissionFixture(t)
			f.snapshot.Members[1].Client = &transferAdmissionClient{probe: func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				resp := &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, OwnershipProtocol: 2,
					OwnershipPartitionProtocol: 1, TransferSizeGuardVersion: 1, Ownership: proto.CloneOf(f.proof)}
				change(resp)
				return resp, nil
			}}
			_, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
			require.Error(t, err)
			require.Empty(t, f.applied)
		})
	}
}

func TestSplitTransferAdmissionRejectsNilResponseAndInvalidGroup(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	_, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 0, f.source, f.control, f.proof)
	require.Error(t, err)
	f.snapshot.Members[1].Client = &transferAdmissionClient{probe: func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) { return nil, nil }}
	_, err = f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.Error(t, err)
	require.Empty(t, f.applied)
}

func TestSplitTransferAdmissionRejectsMissingPersistedLearnerACK(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	learner, ok := f.snapshot.Members[1].Client.(*transferAdmissionClient)
	require.True(t, ok)
	probe := learner.probe
	learner.probe = func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		resp, err := probe(req)
		if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED {
			resp.Ready = false
		}
		return resp, err
	}
	witness, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.Error(t, err)
	require.Nil(t, witness)
	require.Len(t, f.applied, 1, "proposal success alone is not persisted all-member evidence")
}

func TestSplitTransferAdmissionMembershipChangesBeforeApply(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	calls := 0
	WithSplitMigrationApplyMemberSnapshotFactory(func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error) {
		calls++
		if calls > 1 {
			f.snapshot.Members = f.snapshot.Members[:1]
		}
		return f.snapshot, nil
	})(f.server)
	_, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.Error(t, err)
	require.Empty(t, f.applied)
}

func TestSplitTransferAdmissionRevalidationUsesFreshMembers(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	witness, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.NoError(t, err)
	f.snapshot.Members[1].Client = &transferAdmissionClient{probe: func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		return &pb.ProbeMigrationStateResponse{}, nil
	}}
	require.Error(t, f.server.revalidateSplitTransferAdmission(t.Context(), witness), "new factory clients must be probed, not cached ACKs")
}

func TestSplitTransferAdmissionChangedControlsNeedNewWitness(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	first, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.NoError(t, err)
	f.control.SourceWriteFence, f.control.SourceReadFence = true, true
	f.control.RetentionPinTs++
	second, err := f.server.ensureSplitTransferAdmission(t.Context(), f.job, 2, f.source, f.control, f.proof)
	require.NoError(t, err)
	require.False(t, first.control.GetSourceWriteFence())
	require.True(t, second.control.GetSourceWriteFence())
	require.NotEqual(t, first.control.GetRetentionPinTs(), second.control.GetRetentionPinTs())
	require.Len(t, f.applied, 2)
	// Mutating caller-owned proof/control must not rewrite the first witness.
	f.proof.SourceAppliedIndex++
	require.Equal(t, uint64(9), first.ownershipProof.GetSourceAppliedIndex())
}

func TestSplitTransferAdmissionRejectsCanceledContext(t *testing.T) {
	f := newTransferAdmissionFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := f.server.ensureSplitTransferAdmission(ctx, f.job, 2, f.source, f.control, f.proof)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.applied)
	require.Error(t, f.server.revalidateSplitTransferAdmission(t.Context(), nil))
}
