package adapter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestActiveNamespaceUsesFreshAllMemberReadback(t *testing.T) {
	var namespaceProbes, totalProbes atomic.Int32
	members := SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 41}
	for _, id := range []string{"voter", "learner"} {
		client := &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
			totalProbes.Add(1)
			if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION {
				namespaceProbes.Add(1)
			}
			return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion, OwnershipPartitionProtocol: MigrationOwnershipPartitionProtocolVersion}, nil
		}}
		members.Members = append(members.Members, SplitMigrationVoter{ID: id, Address: id + ":1", Client: client})
	}
	server := &DistributionServer{splitApplyMemberFactory: func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error) { return members, nil }, splitMigrationGroupFactory: func(context.Context, uint64) (SplitMigrationClient, error) { return members.Members[0].Client, nil }}
	require.NoError(t, server.ensureMigrationNamespaces(t.Context(), []uint64{1}))
	require.Equal(t, int32(2), totalProbes.Load(), "fresh namespace ACKs must also prove installed capabilities without another RPC")
	require.Equal(t, int32(2), namespaceProbes.Load(), "one current activation ACK per apply member must suffice without replaying negotiation")
	require.NoError(t, server.ensureMigrationNamespaces(t.Context(), []uint64{1}))
	require.Equal(t, int32(4), totalProbes.Load())
	require.Equal(t, int32(4), namespaceProbes.Load(), "ACKs are re-read, never cached across attempts")
}

func TestActiveNamespaceReadbackRejectsMissingLearnerProof(t *testing.T) {
	for _, mode := range []string{"pending", "unsupported", "configuration", "transport"} {
		t.Run(mode, func(t *testing.T) {
			members := SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 41}
			for _, id := range []string{"voter", "learner"} {
				client := &transferAdmissionClient{probe: func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
					response := &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion, OwnershipPartitionProtocol: MigrationOwnershipPartitionProtocolVersion}
					if id == "learner" {
						switch mode {
						case "pending":
							response.Ready = false
						case "unsupported":
							response.NamespaceAdmissionVersion = 0
						case "configuration":
							response.AppliedConfigurationIndex++
						case "transport":
							return nil, context.DeadlineExceeded
						}
					}
					return response, nil
				}}
				members.Members = append(members.Members, SplitMigrationVoter{ID: id, Address: id + ":1", Client: client})
			}
			active, err := readActiveSplitNamespace(t.Context(), splitNamespaceGroup{members: members})
			require.False(t, active)
			if mode == "pending" {
				require.NoError(t, err, "only missing activation may take the normal activation path")
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestActiveNamespaceReadbackRechecksAuthoritativeConfiguration(t *testing.T) {
	var changed atomic.Bool
	members := SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 41}
	for _, id := range []string{"voter", "learner"} {
		client := &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
			if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION && id == "learner" {
				changed.Store(true)
			}
			return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion, OwnershipPartitionProtocol: MigrationOwnershipPartitionProtocolVersion}, nil
		}}
		members.Members = append(members.Members, SplitMigrationVoter{ID: id, Address: id + ":1", Client: client})
	}
	server := &DistributionServer{splitApplyMemberFactory: func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error) {
		result := members
		if changed.Load() {
			result.ConfigurationIndex++
		}
		return result, nil
	}, splitMigrationGroupFactory: func(context.Context, uint64) (SplitMigrationClient, error) { return members.Members[0].Client, nil }}
	require.Error(t, server.ensureMigrationNamespaces(t.Context(), []uint64{1}), "ACKs at the previous configuration cannot authorize progress")
}

func TestNamespaceReadbackFallsBackForLegacyCapabilities(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-supported", true: "old-learner"}[unsupported], func(t *testing.T) {
			var capabilities atomic.Int32
			members := SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 41}
			for _, id := range []string{"voter", "learner"} {
				client := &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
					response := &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion}
					if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY {
						capabilities.Add(1)
						if !unsupported || id != "learner" {
							response.OwnershipPartitionProtocol = MigrationOwnershipPartitionProtocolVersion
						}
					}
					return response, nil
				}}
				members.Members = append(members.Members, SplitMigrationVoter{ID: id, Address: id + ":1", Client: client})
			}
			active, err := readActiveSplitNamespace(t.Context(), splitNamespaceGroup{members: members})
			if unsupported {
				require.Error(t, err)
				require.False(t, active)
			} else {
				require.NoError(t, err)
				require.True(t, active)
			}
			require.Equal(t, int32(2), capabilities.Load(), "missing namespace capability fields require explicit all-member proof")
		})
	}
}

func TestNamespaceReadbackReportsInstalledPartitionSupport(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	internal := h.clients[0].internal
	response, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION})
	require.NoError(t, err)
	require.Equal(t, MigrationOwnershipPartitionProtocolVersion, response.GetOwnershipPartitionProtocol())
	internal.migrationPartitionProtocolReady = func() bool { return false }
	response, err = internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION})
	require.NoError(t, err)
	require.Zero(t, response.GetOwnershipPartitionProtocol())
}

type activationOrderClient struct {
	*transferAdmissionClient
	activations *atomic.Int32
}

func (c *activationOrderClient) CleanupMigration(context.Context, *pb.CleanupMigrationRequest, ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	c.activations.Add(1)
	return &pb.CleanupMigrationResponse{}, nil
}

func TestNamespaceProvesEveryGroupBeforeActivation(t *testing.T) {
	for _, mode := range []string{"unsupported-target", "changed-target"} {
		t.Run(mode, func(t *testing.T) {
			server, activations := namespaceActivationOrderFixture(mode)
			require.Error(t, server.ensureMigrationNamespaces(t.Context(), []uint64{1, 2}))
			require.Zero(t, activations.Load(), "a source activation must not precede target capability/configuration validation")
		})
	}
}

func namespaceActivationOrderFixture(mode string) (*DistributionServer, *atomic.Int32) {
	var changed atomic.Bool
	activations := &atomic.Int32{}
	clients := make(map[uint64]*activationOrderClient)
	for _, groupID := range []uint64{1, 2} {
		clients[groupID] = &activationOrderClient{activations: activations, transferAdmissionClient: &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
			response := &pb.ProbeMigrationStateResponse{Ready: groupID == 2, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion, OwnershipPartitionProtocol: MigrationOwnershipPartitionProtocolVersion}
			if groupID == 2 {
				if mode == "unsupported-target" {
					response.OwnershipPartitionProtocol = 0
				}
				if mode == "changed-target" && req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION {
					changed.Store(true)
				}
			}
			return response, nil
		}}}
	}
	server := &DistributionServer{splitMigrationGroupFactory: func(_ context.Context, id uint64) (SplitMigrationClient, error) { return clients[id], nil }, splitApplyMemberFactory: func(_ context.Context, id uint64) (SplitMigrationApplyMemberSnapshot, error) {
		cfg := uint64(41)
		if id == 2 && changed.Load() {
			cfg++
		}
		name := map[uint64]string{1: "source", 2: "target"}[id]
		return SplitMigrationApplyMemberSnapshot{ConfigurationIndex: cfg, Members: []SplitMigrationVoter{{ID: name, Address: name + ":1", Client: clients[id]}}}, nil
	}}
	return server, activations
}

func TestLegacyNamespaceReadbackDiscardsACKBeforeCapability(t *testing.T) {
	for _, mode := range []string{"pending", "configuration", "transport"} {
		t.Run(mode, func(t *testing.T) {
			var namespaces atomic.Int32
			client := &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				response := &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion}
				if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY {
					response.OwnershipPartitionProtocol = MigrationOwnershipPartitionProtocolVersion
				} else if namespaces.Add(1) > 1 {
					switch mode {
					case "pending":
						response.Ready = false
					case "configuration":
						response.AppliedConfigurationIndex++
					case "transport":
						return nil, context.DeadlineExceeded
					}
				}
				return response, nil
			}}
			group := splitNamespaceGroup{members: SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 41, Members: []SplitMigrationVoter{{ID: "learner", Address: "learner:1", Client: client}}}}
			active, err := readActiveSplitNamespace(t.Context(), group)
			require.False(t, active, "the initial old-server ACK cannot survive the capability fallback")
			if mode == "pending" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, int32(2), namespaces.Load())
		})
	}
}
