package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrMigrationNegativePartitionDeferred never means a new receipt or serving
// authority. Foreground callers remain pending; background retains the denial.
var ErrMigrationNegativePartitionDeferred = errors.New("negative partition retained; no committed geometry to install")

var errMigrationPartitionDescriptorChanged = errors.New("committed partition descriptor changed; no manifest")

func (s *DistributionServer) migrationPartitionManifestForPriors(ctx context.Context, snapshot distribution.CatalogSnapshot, prior store.MigrationOwnershipPartition, partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) ([]byte, *distribution.RouteDescriptor, error) {
	raw, unchanged, exactErr := s.migrationPartitionManifestAt(ctx, snapshot, prior)
	if exactErr == nil {
		return raw, unchanged, nil
	}
	if !negativePartitionLookupMissing(exactErr) || !kv.MigrationPartitionNegativePriors(partitions, legacy) {
		return nil, nil, exactErr
	}
	manifest, found, err := s.catalog.OwnershipPartitionNegativeAt(ctx, prior.RouteID, prior.ParentRouteID, prior.RouteStart, prior.RouteEnd, snapshot.ReadTS)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if found {
		if !kv.MigrationPartitionNegativePriorsMatchParent(partitions, legacy, manifest.Parent) {
			return nil, nil, errors.WithStack(store.ErrMigrationOwnership)
		}
		encoded, err := distribution.EncodeOwnershipPartitionManifest(manifest)
		return encoded, nil, errors.WithStack(err)
	}
	return negativePartitionGeometryUnchanged(snapshot, prior, partitions, legacy, exactErr)
}

func negativePartitionLookupMissing(err error) bool {
	return errors.Is(err, distribution.ErrMigrationSourceRouteChanged) || errors.Is(err, errMigrationPartitionDescriptorChanged)
}

func negativePartitionGeometryUnchanged(snapshot distribution.CatalogSnapshot, prior store.MigrationOwnershipPartition, partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState, exactErr error) ([]byte, *distribution.RouteDescriptor, error) {
	route, found := findRouteByID(snapshot.Routes, prior.RouteID)
	if !found || !kv.MigrationPartitionNegativePriorsMatchParent(partitions, legacy, route) {
		return nil, nil, exactErr
	}
	return nil, nil, errors.WithStack(status.Error(codes.NotFound, "negative ownership geometry unchanged; no committed partition manifest"))
}

func deferredMigrationNegativePartition(err error, partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) bool {
	return status.Code(err) == codes.NotFound && kv.MigrationPartitionNegativePriors(partitions, legacy)
}

func migrationPartitionBackgroundResult(err error, background bool) error {
	if background && errors.Is(err, ErrMigrationNegativePartitionDeferred) {
		return nil
	}
	return err
}
