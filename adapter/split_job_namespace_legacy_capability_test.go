package adapter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestLegacyNamespaceFinalCapabilitiesBeforeAnyActivation(t *testing.T) {
	for _, field := range []string{"guard", "partition", "guard-from-start"} {
		for _, value := range []uint32{0, 2} {
			t.Run(field+map[uint32]string{0: "-missing", 2: "-unknown"}[value], func(t *testing.T) {
				activations := &atomic.Int32{}
				members := map[uint64]SplitMigrationApplyMemberSnapshot{}
				for _, groupID := range []uint64{1, 2} {
					snapshot := SplitMigrationApplyMemberSnapshot{ConfigurationIndex: 41}
					ids := []string{"source"}
					if groupID == 2 {
						ids = []string{"modern", "legacy"}
					}
					for _, id := range ids {
						peer := legacyCapabilityChangePeer(groupID, id, field, value, activations)
						snapshot.Members = append(snapshot.Members, SplitMigrationVoter{ID: id, Address: id + ":1", Client: peer})
					}
					members[groupID] = snapshot
				}
				server := &DistributionServer{splitApplyMemberFactory: func(_ context.Context, id uint64) (SplitMigrationApplyMemberSnapshot, error) { return members[id], nil }, splitMigrationGroupFactory: func(_ context.Context, id uint64) (SplitMigrationClient, error) {
					return members[id].Members[0].Client, nil
				}}
				require.Error(t, server.ensureMigrationNamespaces(t.Context(), []uint64{1, 2}))
				require.Zero(t, activations.Load(), "the inactive source must not activate before the final target capability check")
			})
		}
	}
}

func legacyCapabilityChangePeer(groupID uint64, id, field string, value uint32, activations *atomic.Int32) *activationOrderClient {
	var namespaces atomic.Int32
	return &activationOrderClient{activations: activations, transferAdmissionClient: &transferAdmissionClient{probe: func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		response := &pb.ProbeMigrationStateResponse{Ready: groupID == 2, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion, OwnershipPartitionProtocol: MigrationOwnershipPartitionProtocolVersion}
		if req.GetKind() != pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION {
			return response, nil
		}
		round := namespaces.Add(1)
		if id == "legacy" {
			response.OwnershipPartitionProtocol = 0
		}
		if id == "modern" && (round > 1 || field == "guard-from-start") {
			if field != "partition" {
				response.TransferSizeGuardVersion = value
			} else {
				response.OwnershipPartitionProtocol = value
			}
		}
		return response, nil
	}}}
}
