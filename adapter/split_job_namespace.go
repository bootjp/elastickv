package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type splitNamespaceGroup struct {
	id      uint64
	client  SplitMigrationClient
	members SplitMigrationApplyMemberSnapshot
	active  bool
}

func splitJobTransferNamespaceRequired(phase distribution.SplitJobPhase) bool {
	switch phase {
	case distribution.SplitJobPhaseBackfill, distribution.SplitJobPhaseFence, distribution.SplitJobPhaseDeltaCopy, distribution.SplitJobPhaseCutover:
		return true
	case distribution.SplitJobPhaseNone, distribution.SplitJobPhasePlanned, distribution.SplitJobPhaseCleanup,
		distribution.SplitJobPhaseDone, distribution.SplitJobPhaseFailed, distribution.SplitJobPhaseAbandoning, distribution.SplitJobPhaseAbandoned:
		return false
	}
	return false
}

// Probe both sides before any activation or catalog geometry write. Static
// support is not a guard ACK; MODE8 subsequently proves durable activation.
func (s *DistributionServer) ensureSplitJobNamespaceAdmission(ctx context.Context, job distribution.SplitJob) error {
	return s.ensureMigrationNamespaces(ctx, []uint64{job.SourceGroupID, job.TargetGroupID})
}

func (s *DistributionServer) ensureMigrationNamespaces(ctx context.Context, ids []uint64) error {
	if s.splitMigrationGroupFactory == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	groups := make([]splitNamespaceGroup, 0, len(ids))
	for _, id := range ids {
		group, err := s.splitNamespaceGroup(ctx, id)
		if err != nil {
			return err
		}
		groups = append(groups, group)
	}
	// Prove capabilities on BOTH groups before any activation side effect.
	for index, group := range groups {
		active, err := readActiveSplitNamespace(ctx, group)
		if err != nil {
			return err
		}
		groups[index].active = active
	}
	for _, group := range groups {
		if !group.active {
			if err := s.revalidateSplitNamespaceGroups(ctx, groups); err != nil {
				return err
			}
			if err := s.activateCapturedSplitNamespace(ctx, group); err != nil {
				return err
			}
		}
	}
	return s.revalidateSplitNamespaceGroups(ctx, groups)
}

func (s *DistributionServer) splitNamespaceGroup(ctx context.Context, id uint64) (splitNamespaceGroup, error) {
	members, err := s.splitTransferApplyMembers(ctx, id)
	if err != nil {
		return splitNamespaceGroup{}, err
	}
	client, err := s.splitMigrationGroupFactory(ctx, id)
	if err != nil {
		return splitNamespaceGroup{}, errors.WithStack(err)
	}
	if client == nil {
		return splitNamespaceGroup{}, errors.WithStack(store.ErrNotSupported)
	}
	return splitNamespaceGroup{id: id, client: client, members: members}, nil
}

func verifySplitNamespaceMembers(ctx context.Context, snapshot SplitMigrationApplyMemberSnapshot) error {
	for _, member := range snapshot.Members {
		attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationVoterProbeTimeout)
		response, err := member.Client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{JobId: 1,
			Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY,
			RequireConfigurationIndex: true, ExpectedConfigurationIndex: snapshot.ConfigurationIndex})
		cancel()
		if err != nil {
			return errors.WithStack(err)
		}
		if response.GetTransferSizeGuardVersion() != store.MigrationTransferSizeGuardVersion ||
			response.GetOwnershipPartitionProtocol() != MigrationOwnershipPartitionProtocolVersion ||
			response.GetAppliedConfigurationIndex() != snapshot.ConfigurationIndex {
			return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
	}
	return nil
}

// The apply-member inventory comes from an authoritative group snapshot.
// Fresh activation ACKs at that exact configuration need no negotiation or
// activation replay. The caller revalidates every captured inventory afterward.
func readActiveSplitNamespace(ctx context.Context, group splitNamespaceGroup) (bool, error) {
	peers := make([]NamespaceAdmissionPeer, 0, len(group.members.Members))
	for _, member := range group.members.Members {
		peers = append(peers, NamespaceAdmissionPeer{MemberID: raftengine.DeriveNodeID(member.ID), Address: member.Address, Client: member.Client})
	}
	responses, err := probeNamespacePeers(ctx, peers, group.members.ConfigurationIndex, false)
	if err != nil {
		return false, err
	}
	capabilitiesReported := true
	for _, response := range responses {
		reported, err := namespaceCapabilitiesReported(response)
		if err != nil {
			return false, err
		}
		capabilitiesReported = capabilitiesReported && reported
	}
	// Older sources omit installed partition support from namespace responses.
	// Re-probe it explicitly rather than inferring support from namespace v1.
	if !capabilitiesReported {
		responses, err = readLegacySplitNamespace(ctx, group, peers, responses)
		if err != nil {
			return false, err
		}
	}
	return splitNamespaceResponsesActive(responses), nil
}

func splitNamespaceResponsesActive(responses []*pb.ProbeMigrationStateResponse) bool {
	for _, response := range responses {
		if !response.GetReady() {
			return false
		}
	}
	return true
}

func (s *DistributionServer) revalidateSplitNamespaceGroups(ctx context.Context, groups []splitNamespaceGroup) error {
	for _, group := range groups {
		if _, err := s.revalidateSplitTransferMembers(ctx, &splitTransferAdmissionWitness{groupID: group.id, snapshot: group.members}); err != nil {
			return err
		}
	}
	return nil
}

// Activation remains bound to the authoritative inventory captured before all
// groups were probed. A changed configuration cannot silently replace it.
func (s *DistributionServer) activateCapturedSplitNamespace(ctx context.Context, group splitNamespaceGroup) error {
	snapshot := func(ctx context.Context) (NamespaceAdmissionSnapshot, error) {
		members, err := s.revalidateSplitTransferMembers(ctx, &splitTransferAdmissionWitness{groupID: group.id, snapshot: group.members})
		if err != nil {
			return NamespaceAdmissionSnapshot{}, err
		}
		state := NamespaceAdmissionSnapshot{ConfigurationIndex: members.ConfigurationIndex}
		for _, member := range members.Members {
			state.Members = append(state.Members, NamespaceAdmissionPeer{MemberID: raftengine.DeriveNodeID(member.ID), Address: member.Address, Client: member.Client})
		}
		return state, nil
	}
	activate := func(ctx context.Context, command kv.NamespaceAdmissionCommand) error {
		request, err := kv.NewNamespaceAdmissionRequest(1, command)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = cleanupSplitMigration(ctx, group.client, request)
		return errors.WithStack(err)
	}
	return EnsureMigrationNamespaceAdmissionWithSnapshot(ctx, snapshot, activate, nil)
}

func namespaceCapabilitiesReported(response *pb.ProbeMigrationStateResponse) (bool, error) {
	guard, partition := response.GetTransferSizeGuardVersion(), response.GetOwnershipPartitionProtocol()
	if guard != store.MigrationTransferSizeGuardVersion || (partition != 0 && partition != MigrationOwnershipPartitionProtocolVersion) {
		return false, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return guard == store.MigrationTransferSizeGuardVersion && partition == MigrationOwnershipPartitionProtocolVersion, nil
}

// Capability readback must precede the ACK used from an old endpoint. Also
// reject support lost by a modern peer while another peer needs fallback.
func readLegacySplitNamespace(ctx context.Context, group splitNamespaceGroup, peers []NamespaceAdmissionPeer, before []*pb.ProbeMigrationStateResponse) ([]*pb.ProbeMigrationStateResponse, error) {
	if err := verifySplitNamespaceMembers(ctx, group.members); err != nil {
		return nil, err
	}
	after, err := probeNamespacePeers(ctx, peers, group.members.ConfigurationIndex, false)
	if err != nil {
		return nil, err
	}
	if err := validateSplitNamespaceCapabilityReadback(before, after); err != nil {
		return nil, err
	}
	return after, nil
}

func validateSplitNamespaceCapabilityReadback(before, after []*pb.ProbeMigrationStateResponse) error {
	if len(before) != len(after) {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	for index, response := range after {
		if _, err := namespaceCapabilitiesReported(response); err != nil {
			return err
		}
		if !splitNamespaceCapabilitiesRetained(before[index], response) {
			return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
	}
	return nil
}

func splitNamespaceCapabilitiesRetained(before, after *pb.ProbeMigrationStateResponse) bool {
	return (before.GetTransferSizeGuardVersion() == 0 || before.GetTransferSizeGuardVersion() == after.GetTransferSizeGuardVersion()) &&
		(before.GetOwnershipPartitionProtocol() == 0 || before.GetOwnershipPartitionProtocol() == after.GetOwnershipPartitionProtocol())
}
