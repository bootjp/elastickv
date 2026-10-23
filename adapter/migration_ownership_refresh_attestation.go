package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// loadCommittedMigrationPartitionCatalog pins a leader-issued timestamp only
// after a root applied-read barrier. Root and data leadership are independent.
func (s *DistributionServer) loadCommittedMigrationPartitionCatalog(ctx context.Context) (distribution.CatalogSnapshot, func(), error) {
	if err := s.migrationPartitionRootBarrier(ctx); err != nil {
		return distribution.CatalogSnapshot{}, nil, errors.WithStack(err)
	}
	readTS, err := s.issueMigrationPartitionReadTS(ctx)
	if err != nil {
		return distribution.CatalogSnapshot{}, nil, errors.WithStack(err)
	}
	pin := s.pinReadTS(readTS)
	release := func() { releaseReadPin(pin) }
	snapshot, err := s.loadCatalogSnapshotAt(ctx, readTS)
	if err == nil {
		err = s.verifyCatalogLeader(ctx)
	}
	if err != nil {
		release()
		return distribution.CatalogSnapshot{}, nil, errors.WithStack(err)
	}
	return snapshot, release, nil
}

func (s *DistributionServer) attestUnchangedOwnershipAtCommittedRoot(ctx context.Context, prior *pb.MigrationOwnershipProof) (*pb.MigrationOwnershipProof, error) {
	snapshot, release, err := s.loadCommittedMigrationPartitionCatalog(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if snapshot.Version < prior.GetCatalogVersion() {
		return nil, errors.WithStack(store.ErrMigrationOwnership)
	}
	route, found := findRouteByID(snapshot.Routes, prior.GetRouteId())
	if !found {
		return nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	hash, err := distribution.OwnershipPartitionDescriptorHash(route)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if route.GroupID != prior.GetOwnerGroupId() || !bytes.Equal(hash, prior.GetDescriptorHash()) {
		return nil, errors.New("committed ownership descriptor changed; reacquisition required")
	}
	proof := proto.CloneOf(prior)
	proof.CatalogVersion = snapshot.Version
	return proof, nil
}

func (s *DistributionServer) migrationPartitionRootBarrier(ctx context.Context) error {
	if err := s.requireReadReady(); err != nil {
		return errors.WithStack(err)
	}
	if err := s.verifyCatalogLeader(ctx); err != nil {
		return errors.WithStack(err)
	}
	reader, ok := s.coordinator.(catalogAppliedReader)
	if !ok || s.catalog == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	_, err := reader.LinearizableReadForKey(ctx, distribution.CatalogVersionKey())
	return errors.WithStack(err)
}

func (s *DistributionServer) issueMigrationPartitionReadTS(ctx context.Context) (uint64, error) {
	_, allocator := kv.TimestampAllocatorThrough(s.coordinator)
	if !allocator && s.coordinator.Clock() == nil {
		return 0, errors.WithStack(store.ErrNotSupported)
	}
	issued, err := kv.NextTimestampAfterThrough(ctx, s.coordinator, s.catalog.LatestCommitTS(), "partition attestation: issue root read timestamp")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	ts, err := kv.BeginReadTimestampThrough(ctx, s.coordinator, issued, "partition attestation: pinned root read")
	return ts.Timestamp(), errors.WithStack(err)
}

func (s *DistributionServer) attestMigrationPartition(ctx context.Context, req *pb.AttestMigrationOwnershipRequest) (*pb.AttestMigrationOwnershipResponse, error) {
	partitions, legacy, err := decodeMigrationPartitionPriors(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	prior, configuration := migrationPartitionAttestationPrior(partitions, legacy)
	snapshot, release, err := s.loadCommittedMigrationPartitionCatalog(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer release()
	if snapshot.Version < prior.CatalogVersion {
		return nil, errors.WithStack(store.ErrMigrationOwnership)
	}
	raw, unchanged, err := s.migrationPartitionManifestForPriors(ctx, snapshot, prior, partitions, legacy)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	control, err := kv.BuildMigrationPartitionControl(partitions, legacy, raw, unchanged, snapshot.Version, configuration)
	if err != nil {
		if errors.Is(err, store.ErrNotSupported) {
			return nil, errors.WithStack(status.Error(codes.Unimplemented, "unchanged legacy ownership uses per-job attestation"))
		}
		return nil, errors.WithStack(err)
	}
	encoded, err := kv.EncodeMigrationPartitionControl(control)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.AttestMigrationOwnershipResponse{OwnershipProtocol: migrationOwnershipProtocol, PartitionTransition: encoded}, nil
}

func decodeMigrationPartitionPriors(req *pb.AttestMigrationOwnershipRequest) ([]store.MigrationOwnershipPartition, []store.MigrationOwnershipState, error) {
	if req.GetPrior() != nil {
		return nil, nil, errors.WithStack(kv.ErrInvalidRequest)
	}
	var partitions []store.MigrationOwnershipPartition
	var legacy []store.MigrationOwnershipState
	if len(req.GetPriorPartition()) != 0 {
		if err := kv.DecodeMigrationPartitionJSON(req.GetPriorPartition(), &partitions); err != nil {
			return nil, nil, errors.WithStack(err)
		}
	}
	if len(req.GetPriorOwnershipState()) != 0 {
		if err := kv.DecodeMigrationPartitionJSON(req.GetPriorOwnershipState(), &legacy); err != nil {
			return nil, nil, errors.WithStack(err)
		}
	}
	if len(req.GetPriorPartition())+len(req.GetPriorOwnershipState()) > kv.MaxMigrationPartitionControlBytes || len(partitions)+len(legacy) == 0 {
		return nil, nil, errors.WithStack(kv.ErrInvalidRequest)
	}
	return partitions, legacy, nil
}

func migrationPartitionAttestationPrior(partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) (store.MigrationOwnershipPartition, uint64) {
	var prior store.MigrationOwnershipPartition
	var configuration uint64
	for _, p := range partitions {
		if prior.CatalogVersion < p.CatalogVersion {
			prior = p
		}
		configuration = max(configuration, p.ConfigurationIndex)
	}
	for _, state := range legacy {
		p := kv.MigrationPartitionFromLegacy(state)
		if prior.CatalogVersion < p.CatalogVersion {
			prior = p
		}
		configuration = max(configuration, p.ConfigurationIndex)
	}
	return prior, configuration
}

func (s *DistributionServer) migrationPartitionManifestAt(ctx context.Context, snapshot distribution.CatalogSnapshot, prior store.MigrationOwnershipPartition) ([]byte, *distribution.RouteDescriptor, error) {
	manifest, found, err := s.catalog.OwnershipPartitionAt(ctx, prior.RouteID, prior.DescriptorHash, snapshot.ReadTS)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if found {
		raw, err := distribution.EncodeOwnershipPartitionManifest(manifest)
		return raw, nil, errors.WithStack(err)
	}
	route, exists := findRouteByID(snapshot.Routes, prior.RouteID)
	if !exists {
		return nil, nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	hash, hashErr := distribution.OwnershipPartitionDescriptorHash(route)
	if hashErr != nil {
		return nil, nil, errors.WithStack(hashErr)
	}
	if !bytes.Equal(hash, prior.DescriptorHash) {
		return nil, nil, errors.WithStack(errMigrationPartitionDescriptorChanged)
	}
	return nil, &route, nil
}
