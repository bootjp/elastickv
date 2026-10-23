package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const splitPartitionControlReserve = 4 << 10

// Check the full downstream envelope before publishing geometry. The manifest
// alone is smaller than the control, which repeats bounds in priors and tiles.
func (s *DistributionServer) validateSplitPartitionBudget(ctx context.Context, parent distribution.RouteDescriptor, manifest []byte, version uint64) error {
	if s.ownershipRefresh == nil {
		return nil
	}
	group := s.ownershipRefresh.groups[parent.GroupID]
	if group == nil || group.Store == nil {
		return nil
	}
	reader, ok := group.Store.(store.MigrationServingAdmissionReader)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	view, err := reader.MigrationServingAdmissionState(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	partitions, legacy := splitPartitionBudgetPriors(view, parent)
	if len(partitions)+len(legacy) == 0 {
		return nil
	}
	configuration, err := kv.MigrationConfigurationIndex(ctx, group.Store)
	if err != nil {
		return errors.WithStack(err)
	}
	control, err := kv.BuildMigrationPartitionControl(partitions, legacy, manifest, nil, version, configuration)
	if err != nil {
		return errors.WithStack(err)
	}
	encoded, err := kv.EncodeMigrationPartitionControl(control)
	if err != nil {
		return errors.WithStack(err)
	}
	// Leave room for epoch/index growth before the actual Raft proposal. This
	// dry run is never used as a root attestation or an apply authorization.
	if len(encoded) > kv.MaxMigrationPartitionControlBytes-splitPartitionControlReserve {
		return errors.Wrap(kv.ErrInvalidRequest, "split ownership control exceeds transfer budget")
	}
	return nil
}

func splitPartitionBudgetPriors(view store.MigrationServingState, parent distribution.RouteDescriptor) ([]store.MigrationOwnershipPartition, []store.MigrationOwnershipState) {
	var partitions []store.MigrationOwnershipPartition
	var legacy []store.MigrationOwnershipState
	for _, candidate := range migrationPartitionCandidates(view, view.Partitions) {
		proof := candidate.geometry()
		if !routeRangeIntersects(proof.RouteStart, proof.RouteEnd, parent.Start, parent.End) {
			continue
		}
		if candidate.partition != nil {
			partitions = append(partitions, *candidate.partition)
		} else {
			legacy = append(legacy, *candidate.legacy)
		}
	}
	return partitions, legacy
}
