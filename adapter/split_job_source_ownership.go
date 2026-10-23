package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// The tracker precedes its certificate in the same data log. Creation is
// refused between these entries; authenticated old resolutions still drain.
func (s *DistributionServer) armSplitSourceOwnership(ctx context.Context, job distribution.SplitJob, groupID uint64, source SplitMigrationClient) ([]byte, bool, error) {
	configuration, generation, err := s.splitOwnershipConfiguration(ctx, job.JobID, groupID, source)
	if err != nil {
		return nil, false, err
	}
	snapshot, err := s.loadCommittedMigrationCatalog(ctx)
	if err != nil {
		return nil, false, err
	}
	proof, err := splitSourceOwnershipProof(snapshot, job, groupID, generation)
	if err != nil {
		return nil, false, err
	}
	prior, err := probeSplitSourceOwnershipState(ctx, source, job.JobID, configuration)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	mode, err := splitSourceOwnershipOperation(proof, prior)
	if err != nil {
		return nil, false, err
	}
	minimum := max(initialMigrationRetentionPinTS, proof.GetMinWriteTsExclusive())
	if _, err := applySplitMigrationControl(ctx, source, job, proof.GetRouteEnd(), proof.GetSourceTrackerVersion(), minimum,
		false, false, true, initialMigrationRetentionPinTS); err != nil {
		return nil, false, err
	}
	acks, complete, err := s.syncSplitMigrationVoterBarrier(ctx, groupID, job.FenceAckCursor, source, &pb.ProbeMigrationStateRequest{
		JobId: job.JobID, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		RouteStart: job.SplitKey, RouteEnd: proof.GetRouteEnd(), ExpectedCatalogVersion: proof.GetSourceTrackerVersion(),
		MigrationJobId: job.JobID, MinWriteTsExclusive: minimum, TrackWrites: true, RetentionPinTs: initialMigrationRetentionPinTS,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: configuration})
	if err != nil || !complete {
		return acks, false, err
	}
	if _, err := cleanupSplitMigration(ctx, source, &pb.CleanupMigrationRequest{JobId: job.JobID, Mode: mode,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: configuration, Ownership: proof}); err != nil {
		return nil, false, err
	}
	return s.ackSplitSourceOwnership(ctx, job, groupID, configuration, source, proof)
}

func splitSourceOwnershipProof(snapshot distribution.CatalogSnapshot, job distribution.SplitJob, groupID uint64, generation string) (*pb.MigrationOwnershipProof, error) {
	route, found := findRouteByID(snapshot.Routes, job.SourceRouteID)
	if !found || route.GroupID != groupID || route.State != distribution.RouteStateActive || route.StagedVisibilityActive ||
		!splitOwnershipRouteContains(route, job.SplitKey) || snapshot.Version == 0 {
		return nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	digest := sha256.Sum256(encoded)
	return &pb.MigrationOwnershipProof{Generation: generation, LocalGroupId: groupID, OwnerGroupId: groupID,
		CatalogVersion: snapshot.Version, RouteId: route.RouteID, ParentRouteId: route.ParentRouteID,
		RouteStart: bytes.Clone(job.SplitKey), RouteEnd: bytes.Clone(route.End), MinWriteTsExclusive: route.MinWriteTSExclusive,
		WriteFloorKnown: true, SourceTrackerVersion: snapshot.Version, Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER,
		DescriptorHash: digest[:]}, nil
}

func splitSourceOwnershipOperation(proof *pb.MigrationOwnershipProof, prior *pb.ProbeMigrationStateResponse) (pb.MigrationCleanupMode, error) {
	mode := pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE
	if prior.GetOwnershipProtocol() != migrationOwnershipProtocol {
		return mode, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	old := prior.GetOwnership()
	if old == nil {
		return mode, nil
	}
	if !prior.GetReady() || old.GetSourceAppliedIndex() == 0 || old.GetGeneration() != proof.GetGeneration() ||
		!bytes.Equal(old.GetDescriptorHash(), proof.GetDescriptorHash()) {
		return mode, errors.New("source tracker requires unchanged committed ownership")
	}
	proof.SourceTrackerVersion, proof.SourceAppliedIndex = old.GetSourceTrackerVersion(), old.GetSourceAppliedIndex()
	return pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, nil
}

func (s *DistributionServer) ackSplitSourceOwnership(ctx context.Context, job distribution.SplitJob, groupID, configuration uint64, source SplitMigrationClient, proof *pb.MigrationOwnershipProof) ([]byte, bool, error) {
	resp, err := probeSplitSourceOwnershipState(ctx, source, job.JobID, configuration)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if !resp.GetReady() || resp.GetOwnership().GetSourceAppliedIndex() == 0 {
		return nil, false, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	expected := proto.CloneOf(proof)
	expected.SourceAppliedIndex = resp.GetOwnership().GetSourceAppliedIndex()
	if !proto.Equal(expected, resp.GetOwnership()) {
		return nil, false, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return s.syncSplitMigrationVoterBarrier(ctx, groupID, nil, source, &pb.ProbeMigrationStateRequest{JobId: job.JobID,
		Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED, Ownership: expected,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: configuration})
}

func probeSplitSourceOwnershipState(ctx context.Context, source SplitMigrationClient, jobID, configuration uint64) (*pb.ProbeMigrationStateResponse, error) {
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	resp, err := source.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{JobId: jobID,
		Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: configuration})
	return resp, errors.WithStack(err)
}
