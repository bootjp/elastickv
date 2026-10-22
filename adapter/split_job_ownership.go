package adapter

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

type catalogAppliedReader interface {
	LinearizableReadForKey(context.Context, []byte) (uint64, error)
}

// AttestUnchangedMigrationOwnership is the root-leader producer for a fresh
// replicated attestation. It does not propose to the data group: the consumer
// must bind the resulting REFRESH to that group's current all-voter epoch.
// No watcher, version increment, or same-owner guess substitutes for this read.
func (s *DistributionServer) AttestUnchangedMigrationOwnership(ctx context.Context, prior *pb.MigrationOwnershipProof) (*pb.MigrationOwnershipProof, error) {
	if prior == nil || len(prior.GetDescriptorHash()) != sha256.Size || prior.GetGeneration() == "" {
		return nil, errors.New("migration ownership attestation requires durable prior proof")
	}
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	snapshot, err := s.loadCommittedMigrationCatalog(attempt)
	if err != nil {
		return nil, err
	}
	if snapshot.Version < prior.GetCatalogVersion() {
		return nil, errors.New("committed catalog precedes durable ownership proof")
	}
	route, found := findRouteByID(snapshot.Routes, prior.GetRouteId())
	if !found {
		return nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], prior.GetDescriptorHash()) || route.GroupID != prior.GetOwnerGroupId() {
		return nil, errors.New("committed ownership descriptor changed; reacquisition required")
	}
	refreshed := proto.CloneOf(prior)
	refreshed.CatalogVersion = snapshot.Version
	return refreshed, nil
}

func (s *DistributionServer) loadCommittedMigrationCatalog(ctx context.Context) (distribution.CatalogSnapshot, error) {
	if err := s.verifyCatalogLeader(ctx); err != nil {
		return distribution.CatalogSnapshot{}, err
	}
	reader, ok := s.coordinator.(catalogAppliedReader)
	if !ok {
		return distribution.CatalogSnapshot{}, errors.New("migration ownership requires root leader applied-read support")
	}
	if _, err := reader.LinearizableReadForKey(ctx, distribution.CatalogVersionKey()); err != nil {
		return distribution.CatalogSnapshot{}, errors.WithStack(err)
	}
	return s.loadCatalogSnapshot(ctx)
}

func splitOwnershipProof(snapshot distribution.CatalogSnapshot, job distribution.SplitJob, localGroupID uint64, generation string) (*pb.MigrationOwnershipProof, error) {
	for _, route := range snapshot.Routes {
		if !splitOwnershipRouteContains(route, job.SplitKey) {
			continue
		}
		if route.State != distribution.RouteStateActive || route.GroupID == 0 {
			return nil, errors.New("migration ownership descriptor is not active")
		}
		if !splitOwnershipDescriptorMatchesJob(route, job) {
			return nil, errors.New("committed descriptor does not attest this migration job")
		}
		encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		digest := sha256.Sum256(encoded)
		role := pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER
		if route.GroupID == localGroupID {
			role = pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER
		}
		return &pb.MigrationOwnershipProof{Generation: generation, LocalGroupId: localGroupID, OwnerGroupId: route.GroupID,
			CatalogVersion: snapshot.Version, RouteStart: bytes.Clone(job.SplitKey), RouteEnd: bytes.Clone(route.End),
			RouteId: route.RouteID, ParentRouteId: route.ParentRouteID, MinWriteTsExclusive: route.MinWriteTSExclusive,
			Staged: localGroupID == route.GroupID && route.StagedVisibilityActive, Role: role, DescriptorHash: digest[:]}, nil
	}
	return nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
}

func splitOwnershipRouteContains(route distribution.RouteDescriptor, key []byte) bool {
	return bytes.Compare(route.Start, key) <= 0 && (len(route.End) == 0 || bytes.Compare(key, route.End) < 0)
}

func splitOwnershipDescriptorMatchesJob(route distribution.RouteDescriptor, job distribution.SplitJob) bool {
	if job.Phase == distribution.SplitJobPhaseCleanup {
		return validSplitJobCleanupWitness(job, ^uint64(0)) && splitJobCleanupTargetAttests(route, job)
	}
	if job.Phase != distribution.SplitJobPhaseAbandoning || route.GroupID == job.TargetGroupID || route.StagedVisibilityActive {
		return false
	}
	return (route.RouteID == job.SourceRouteID || route.ParentRouteID == job.SourceRouteID) &&
		route.MinWriteTSExclusive >= max(initialMigrationRetentionPinTS, job.SnapshotTS, job.FenceTS)
}

func (s *DistributionServer) splitOwnershipConfiguration(ctx context.Context, jobID, groupID uint64, fallback SplitMigrationClient) (uint64, string, error) {
	if s.splitMigrationVoterFactory == nil {
		return 0, "", errors.Wrap(errSplitMigrationVoterBarrierIncomplete, "ownership requires explicit voter discovery")
	}
	voters, err := s.splitMigrationVoters(ctx, groupID, fallback)
	if err != nil {
		return 0, "", err
	}
	var index uint64
	var generation string
	for n, voter := range voters {
		resp, err := probeSplitOwnershipCapability(ctx, voter, jobID)
		if err != nil {
			return 0, "", err
		}
		if n == 0 {
			index, generation = resp.GetAppliedConfigurationIndex(), resp.GetOwnershipGeneration()
		}
		if index != resp.GetAppliedConfigurationIndex() || generation != resp.GetOwnershipGeneration() {
			return 0, "", errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
	}
	if generation == "" {
		// The first native control establishes the namespace in this group's
		// replicated ownership record. A lost response rediscovers that record;
		// a copied configuration index is never used as the namespace.
		const ownershipGenerationBytes = 16
		nonce := make([]byte, ownershipGenerationBytes)
		if _, err := rand.Read(nonce); err != nil {
			return 0, "", errors.WithStack(err)
		}
		generation = hex.EncodeToString(nonce)
	}
	return index, generation, nil
}

func probeSplitOwnershipCapability(ctx context.Context, voter SplitMigrationVoter, jobID uint64) (*pb.ProbeMigrationStateResponse, error) {
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationVoterProbeTimeout)
	defer cancel()
	resp, err := voter.Client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{
		JobId: jobID, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY, RequireConfigurationIndex: true,
	})
	if err != nil || !resp.GetReady() || resp.GetOwnershipProtocol() != migrationOwnershipProtocol || resp.GetAppliedConfigurationIndex() == 0 {
		return nil, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return resp, nil
}

func (s *DistributionServer) applySplitOwnership(ctx context.Context, job distribution.SplitJob, groupID uint64, client SplitMigrationClient, mode pb.MigrationCleanupMode, expectedConfiguration ...uint64) error {
	index, generation, err := s.splitOwnershipConfiguration(ctx, job.JobID, groupID, client)
	if err != nil {
		return err
	}
	if len(expectedConfiguration) > 0 && expectedConfiguration[0] != index {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	snapshot, err := s.loadCommittedMigrationCatalog(ctx)
	if err != nil {
		return err
	}
	proof, err := splitOwnershipProof(snapshot, job, groupID, generation)
	if err != nil {
		return err
	}
	mode, err = splitOwnershipOperation(ctx, client, job.JobID, index, mode)
	if err != nil {
		return err
	}
	if _, err := cleanupSplitMigration(ctx, client, &pb.CleanupMigrationRequest{JobId: job.JobID, Mode: mode,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index, Ownership: proof}); err != nil {
		return err
	}
	_, ready, err := s.syncSplitMigrationVoterBarrier(ctx, groupID, nil, client, &pb.ProbeMigrationStateRequest{
		JobId: job.JobID, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index, Ownership: proof,
	})
	if err != nil {
		return err
	}
	if !ready {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return nil
}

func splitOwnershipOperation(ctx context.Context, client SplitMigrationClient, jobID, index uint64, requested pb.MigrationCleanupMode) (pb.MigrationCleanupMode, error) {
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	resp, err := client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{
		JobId: jobID, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index,
	})
	if err != nil {
		return requested, errors.WithStack(err)
	}
	if resp.GetOwnershipProtocol() != migrationOwnershipProtocol || resp.GetAppliedConfigurationIndex() != index {
		return requested, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	if resp.GetReady() && resp.GetOwnership() != nil && (requested == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_ACQUIRE || resp.GetOwnershipCleared()) {
		return pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, nil
	}
	return requested, nil
}

func (s *DistributionServer) prepareSplitOwnershipRollback(ctx context.Context, job distribution.SplitJob) error {
	snapshot, err := s.loadCommittedMigrationCatalog(ctx)
	if err != nil {
		return err
	}
	groupID, end, ok := splitJobSourceRouteState(snapshot.Routes, job)
	if !ok {
		return errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	source, err := s.splitJobSourceClient(ctx, job, groupID)
	if err != nil {
		return err
	}
	if _, _, err := s.splitOwnershipConfiguration(ctx, job.JobID, groupID, source); err != nil {
		return err
	}
	_, err = s.splitAbandonedSourceConfiguration(ctx, job, source, end)
	return err
}

func splitOwnershipRollbackRoute(routes []distribution.RouteDescriptor, job distribution.SplitJob) *distribution.RouteDescriptor {
	for _, route := range routes {
		if route.GroupID == job.TargetGroupID || route.StagedVisibilityActive {
			continue
		}
		if route.RouteID == job.SourceRouteID || (route.ParentRouteID == job.SourceRouteID && bytes.Equal(route.Start, job.SplitKey)) {
			copy := route
			return &copy
		}
	}
	return nil
}

func (s *DistributionServer) verifySplitOwnershipRuntimes(ctx context.Context, job distribution.SplitJob, sourceGroupID uint64, source SplitMigrationClient) error {
	if _, _, err := s.splitOwnershipConfiguration(ctx, job.JobID, sourceGroupID, source); err != nil {
		return err
	}
	_, target, err := s.splitMigrationClientFactory(ctx, job, sourceGroupID)
	if err != nil {
		return errors.WithStack(err)
	}
	_, _, err = s.splitOwnershipConfiguration(ctx, job.JobID, job.TargetGroupID, target)
	return err
}
