package adapter

import (
	"bytes"
	"context"
	"slices"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// This witness is local and immutable, not catalog evidence. Recreate it when
// controls change, and revalidate it before advancing a phase. Never reuse an
// ownership ACK by rebinding its control to a newly observed configuration.
type splitTransferAdmissionWitness struct {
	groupID        uint64
	snapshot       SplitMigrationApplyMemberSnapshot
	control        *pb.TargetStagedReadinessRequest
	ownershipProof *pb.MigrationOwnershipProof
}

// The caller has already ACKed the full same-owner partition at the control's
// configuration. This must finish before preflight timestamps/catalog writes,
// history export, or source write fencing. No leader-only fallback is allowed.
func (s *DistributionServer) ensureSplitTransferAdmission(ctx context.Context, job distribution.SplitJob, groupID uint64, source SplitMigrationClient, control *pb.TargetStagedReadinessRequest, ownershipProof *pb.MigrationOwnershipProof) (*splitTransferAdmissionWitness, error) {
	if err := validateSplitTransferAdmission(job, groupID, source, control, ownershipProof); err != nil {
		return nil, err
	}
	snapshot, err := s.splitTransferApplyMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if snapshot.ConfigurationIndex != control.GetExpectedConfigurationIndex() {
		return nil, errors.New("transfer guard configuration differs from ownership ACK")
	}
	witness := &splitTransferAdmissionWitness{groupID: groupID, snapshot: snapshot,
		control: proto.CloneOf(control), ownershipProof: proto.CloneOf(ownershipProof)}
	witness.control.TransferSizeGuardVersion = store.MigrationTransferSizeGuardVersion
	if err := probeSplitTransferOwnership(ctx, witness, snapshot.Members); err != nil {
		return nil, err
	}
	if _, err := s.revalidateSplitTransferMembers(ctx, witness); err != nil {
		return nil, err
	}
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	if _, err := source.ApplyTargetStagedReadiness(attempt, proto.CloneOf(witness.control)); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := s.revalidateSplitTransferAdmission(ctx, witness); err != nil {
		return nil, err
	}
	return witness, nil
}

func validateSplitTransferAdmission(job distribution.SplitJob, groupID uint64, source SplitMigrationClient, control *pb.TargetStagedReadinessRequest, proof *pb.MigrationOwnershipProof) error {
	if source == nil || !validSplitTransferControl(job, groupID, control) {
		return errors.New("transfer guard requires configuration-bound armed source controls")
	}
	if !validSplitTransferOwner(groupID, proof) ||
		bytes.Compare(proof.GetRouteStart(), control.GetRouteStart()) > 0 ||
		!splitTransferEndContains(proof.GetRouteEnd(), control.GetRouteEnd()) {
		return errors.New("transfer guard requires the ACKed same-owner source proof")
	}
	return nil
}

func validSplitTransferControl(job distribution.SplitJob, groupID uint64, control *pb.TargetStagedReadinessRequest) bool {
	if control == nil {
		return false
	}
	return job.JobID != 0 && groupID != 0 && control.GetJobId() == job.JobID &&
		control.GetMigrationJobId() == job.JobID && len(job.SplitKey) != 0 && bytes.Equal(control.GetRouteStart(), job.SplitKey) &&
		validTransferSizeGuardActivation(transferGuardControl(control)) &&
		(control.GetTransferSizeGuardVersion() == 0 || control.GetTransferSizeGuardVersion() == store.MigrationTransferSizeGuardVersion)
}

func validSplitTransferOwner(groupID uint64, proof *pb.MigrationOwnershipProof) bool {
	return proof != nil && proof.GetGeneration() != "" && proof.GetSourceAppliedIndex() != 0 &&
		proof.GetSourceTrackerVersion() != 0 && proof.GetLocalGroupId() == groupID && proof.GetOwnerGroupId() == groupID &&
		!proof.GetStaged() && proof.GetRole() == pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER
}

func transferGuardControl(control *pb.TargetStagedReadinessRequest) *pb.TargetStagedReadinessRequest {
	guarded := proto.CloneOf(control)
	guarded.TransferSizeGuardVersion = store.MigrationTransferSizeGuardVersion
	return guarded
}

func splitTransferEndContains(outer, inner []byte) bool {
	return len(outer) == 0 || (len(inner) != 0 && bytes.Compare(outer, inner) >= 0)
}

func (s *DistributionServer) splitTransferApplyMembers(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return SplitMigrationApplyMemberSnapshot{}, errors.WithStack(err)
	}
	if s.splitApplyMemberFactory == nil {
		return SplitMigrationApplyMemberSnapshot{}, errors.New("transfer guard all-member snapshot factory is not configured")
	}
	snapshot, err := s.splitApplyMemberFactory(ctx, groupID)
	if err != nil {
		return SplitMigrationApplyMemberSnapshot{}, errors.WithStack(err)
	}
	if snapshot.ConfigurationIndex == 0 || len(snapshot.Members) == 0 {
		return SplitMigrationApplyMemberSnapshot{}, errors.New("transfer guard requires a nonempty applied configuration")
	}
	snapshot.Members = slices.Clone(snapshot.Members)
	if err := validateSplitMigrationVoters(snapshot.Members); err != nil {
		return SplitMigrationApplyMemberSnapshot{}, err
	}
	addresses := make(map[string]struct{}, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if _, duplicate := addresses[member.Address]; duplicate {
			return SplitMigrationApplyMemberSnapshot{}, errors.New("transfer guard member address is duplicated")
		}
		addresses[member.Address] = struct{}{}
	}
	return snapshot, nil
}

func (s *DistributionServer) revalidateSplitTransferMembers(ctx context.Context, witness *splitTransferAdmissionWitness) (SplitMigrationApplyMemberSnapshot, error) {
	snapshot, err := s.splitTransferApplyMembers(ctx, witness.groupID)
	if err != nil {
		return SplitMigrationApplyMemberSnapshot{}, err
	}
	if snapshot.ConfigurationIndex != witness.snapshot.ConfigurationIndex || !slices.EqualFunc(snapshot.Members, witness.snapshot.Members,
		func(a, b SplitMigrationVoter) bool { return a.ID == b.ID && a.Address == b.Address }) {
		return SplitMigrationApplyMemberSnapshot{}, errors.New("transfer guard apply-member configuration changed")
	}
	return snapshot, nil
}

func probeSplitTransferMember(ctx context.Context, member SplitMigrationVoter, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationVoterProbeTimeout)
	defer cancel()
	resp, err := member.Client.ProbeMigrationState(attempt, proto.CloneOf(req))
	if err != nil {
		return nil, errors.Wrapf(err, "transfer guard member %s", member.ID)
	}
	if !resp.GetReady() || resp.GetAppliedConfigurationIndex() != req.GetExpectedConfigurationIndex() ||
		resp.GetTransferSizeGuardVersion() != store.MigrationTransferSizeGuardVersion {
		return nil, errors.Errorf("transfer guard member %s lacks applied configuration or size admission ACK", member.ID)
	}
	return resp, nil
}

func probeSplitTransferOwnership(ctx context.Context, witness *splitTransferAdmissionWitness, members []SplitMigrationVoter) error {
	req := &pb.ProbeMigrationStateRequest{JobId: witness.control.GetJobId(),
		Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: witness.snapshot.ConfigurationIndex,
		Ownership: witness.ownershipProof, TransferSizeGuardVersion: store.MigrationTransferSizeGuardVersion}
	for _, member := range members {
		resp, err := probeSplitTransferMember(ctx, member, req)
		if err != nil {
			return err
		}
		if resp.GetOwnershipProtocol() != migrationOwnershipProtocol ||
			resp.GetOwnershipPartitionProtocol() != MigrationOwnershipPartitionProtocolVersion ||
			resp.GetOwnershipCleared() || !proto.Equal(resp.GetOwnership(), witness.ownershipProof) {
			return errors.Errorf("transfer guard member %s lacks current partition ownership ACK", member.ID)
		}
	}
	return nil
}

func (s *DistributionServer) revalidateSplitTransferAdmission(ctx context.Context, witness *splitTransferAdmissionWitness) error {
	if witness == nil || witness.control == nil || witness.ownershipProof == nil {
		return errors.New("transfer guard admission witness is missing")
	}
	snapshot, err := s.revalidateSplitTransferMembers(ctx, witness)
	if err != nil {
		return err
	}
	if err := probeSplitTransferOwnership(ctx, witness, snapshot.Members); err != nil {
		return err
	}
	req := probeMigrationControlRequest(witness.control)
	req.RequireConfigurationIndex = true
	req.ExpectedConfigurationIndex = witness.snapshot.ConfigurationIndex
	req.TransferSizeGuardVersion = store.MigrationTransferSizeGuardVersion
	for _, member := range snapshot.Members {
		if _, err := probeSplitTransferMember(ctx, member, req); err != nil {
			return err
		}
	}
	_, err = s.revalidateSplitTransferMembers(ctx, witness)
	return err
}
