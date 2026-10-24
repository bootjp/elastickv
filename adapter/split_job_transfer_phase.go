package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Namespace activation and the full same-owner partition ACK precede the
// caller's source ownership acquisition. Initial arm ACKs bind C explicitly;
// resumed ticks collect a fresh exact all-member ownership ACK, never just
// stamp a newly observed configuration onto a previously saved proof.
func (s *DistributionServer) ensureSplitJobTransferControl(ctx context.Context, job distribution.SplitJob, groupID uint64, source SplitMigrationClient, control *pb.TargetStagedReadinessRequest, ownershipACK []byte) (*splitTransferAdmissionWitness, error) {
	if source == nil || control == nil {
		return nil, errors.New("transfer phase source or control is missing")
	}
	snapshot, err := s.splitTransferApplyMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}
	configuration, err := splitTransferPhaseConfiguration(snapshot.ConfigurationIndex, control, ownershipACK)
	if err != nil {
		return nil, err
	}
	state, err := probeSplitSourceOwnershipState(ctx, source, job.JobID, configuration)
	if err != nil {
		return nil, err
	}
	if !validSplitTransferPhaseOwnership(state, groupID, configuration) {
		return nil, errors.New("transfer phase requires acquired source ownership at the ACK configuration")
	}
	proof := proto.CloneOf(state.GetOwnership())
	bound := proto.CloneOf(control)
	// This temporary witness only collects the fresh ownership ACK. It is not
	// returned or used as control evidence until ensure completes guard ACKs.
	ack := &splitTransferAdmissionWitness{groupID: groupID, snapshot: snapshot, control: bound, ownershipProof: proof}
	if err := probeSplitTransferOwnership(ctx, ack, snapshot.Members); err != nil {
		return nil, err
	}
	bound.RequireConfigurationIndex, bound.ExpectedConfigurationIndex = true, configuration
	if bound.ExpectedCutoverVersion == 0 {
		bound.ExpectedCutoverVersion = proof.GetSourceTrackerVersion()
	}
	bound.MinWriteTsExclusive = max(bound.GetMinWriteTsExclusive(), proof.GetMinWriteTsExclusive())
	return s.ensureSplitTransferAdmission(ctx, job, groupID, source, bound, proof)
}

func splitTransferPhaseConfiguration(configuration uint64, control *pb.TargetStagedReadinessRequest, ownershipACK []byte) (uint64, error) {
	if control.GetRequireConfigurationIndex() && control.GetExpectedConfigurationIndex() != configuration {
		return 0, errors.New("transfer phase cannot rebind an existing control configuration")
	}
	if len(ownershipACK) == 0 {
		return configuration, nil
	}
	index, err := splitCleanupCursorConfiguration(ownershipACK)
	if err != nil {
		return 0, err
	}
	if index != configuration {
		return 0, errors.New("transfer phase ownership ACK configuration changed")
	}
	return configuration, nil
}

func validSplitTransferPhaseOwnership(state *pb.ProbeMigrationStateResponse, groupID, configuration uint64) bool {
	return state.GetReady() && state.GetAppliedConfigurationIndex() == configuration && !state.GetOwnershipCleared() &&
		validSplitTransferOwner(groupID, state.GetOwnership())
}

func (s *DistributionServer) ensureSplitJobTransferPhase(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, ownershipACK []byte) (*splitTransferAdmissionWitness, error) {
	snapshot, route, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return nil, err
	}
	if source == nil {
		source, err = s.splitJobSourceClient(ctx, job, route.GroupID)
		if err != nil {
			return nil, err
		}
	}
	control, err := splitTransferPhaseControl(job, route, snapshot.Version)
	if err != nil {
		return nil, err
	}
	return s.ensureSplitJobTransferControl(ctx, job, route.GroupID, source, control, ownershipACK)
}

func splitTransferPhaseControl(job distribution.SplitJob, route distribution.RouteDescriptor, catalogVersion uint64) (*pb.TargetStagedReadinessRequest, error) {
	// A zero expected version is filled from the freshly ACKed source tracker,
	// not predicted from the root catalog. Full-history preflight must not itself
	// introduce a source write fence before all history has been checked.
	control := splitMigrationControlRequest(job, route.End, 0, initialMigrationRetentionPinTS, false, false, true, initialMigrationRetentionPinTS)
	switch job.Phase {
	case distribution.SplitJobPhasePlanned, distribution.SplitJobPhaseBackfill:
		return control, nil
	case distribution.SplitJobPhaseFence:
		if route.State != distribution.RouteStateWriteFenced {
			return control, nil
		}
		control.ExpectedCutoverVersion = catalogVersion
	case distribution.SplitJobPhaseDeltaCopy:
		if job.FenceCatalogVersion == 0 {
			return nil, errors.New("delta transfer requires the committed fence catalog version")
		}
		control.ExpectedCutoverVersion = job.FenceCatalogVersion
	case distribution.SplitJobPhaseNone, distribution.SplitJobPhaseCutover, distribution.SplitJobPhaseCleanup,
		distribution.SplitJobPhaseDone, distribution.SplitJobPhaseFailed, distribution.SplitJobPhaseAbandoning, distribution.SplitJobPhaseAbandoned:
		return nil, errors.New("transfer admission is not a copy or preflight phase")
	default:
		return nil, errors.New("unknown transfer admission phase")
	}
	control.SourceWriteFence, control.MinWriteTsExclusive = true, job.SnapshotTS
	return control, nil
}
