package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type splitNamespaceGroup struct {
	id      uint64
	client  SplitMigrationClient
	members SplitMigrationApplyMemberSnapshot
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
	for _, group := range groups {
		if err := EnsureMigrationNamespaceAdmission(ctx, group.client, group.members.Members); err != nil {
			return err
		}
	}
	for _, group := range groups {
		_, err := s.revalidateSplitTransferMembers(ctx, &splitTransferAdmissionWitness{groupID: group.id, snapshot: group.members})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DistributionServer) splitNamespaceGroup(ctx context.Context, id uint64) (splitNamespaceGroup, error) {
	members, err := s.splitTransferApplyMembers(ctx, id)
	if err != nil {
		return splitNamespaceGroup{}, err
	}
	if err := verifySplitNamespaceMembers(ctx, members); err != nil {
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
