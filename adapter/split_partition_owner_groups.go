package adapter

import (
	"context"
	"maps"
	"slices"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (s *DistributionServer) splitRangeOwnerGroups(ctx context.Context, snapshot distribution.CatalogSnapshot, parent distribution.RouteDescriptor, parentActive bool) ([]uint64, error) {
	for _, route := range snapshot.Routes {
		if s.ownershipRefresh.groups[route.GroupID] == nil {
			return nil, errors.WithStack(store.ErrNotSupported)
		}
	}
	selected := make(map[uint64]bool)
	if parentActive {
		selected[parent.GroupID] = true
	}
	for id, group := range s.ownershipRefresh.groups {
		if id == 0 {
			continue
		}
		active, err := s.splitRangeOwnerGroupActive(ctx, id, group)
		if err != nil {
			return nil, err
		}
		if active {
			selected[id] = true
		}
	}
	return slices.Sorted(maps.Keys(selected)), nil
}

func (s *DistributionServer) splitRangeOwnerGroupActive(ctx context.Context, id uint64, group *kv.ShardGroup) (bool, error) {
	if group == nil || group.Store == nil {
		return false, errors.WithStack(store.ErrNotSupported)
	}
	reader, ok := group.Store.(store.MigrationServingAdmissionReader)
	if !ok {
		return false, errors.WithStack(store.ErrNotSupported)
	}
	view, err := reader.MigrationServingAdmissionState(ctx)
	if err != nil {
		return false, errors.WithStack(err)
	}
	// Known historical NONOWNER closures remain DENY while offline. Empty or
	// potentially positive local views still need a current data-group fence.
	if splitPartitionKnownNegativeOnly(view) {
		return false, nil
	}
	view, err = s.splitPartitionAdmissionView(ctx, id, group)
	if err != nil {
		return false, err
	}
	return splitPartitionHasServingOwner(view), nil
}

func splitPartitionKnownNegativeOnly(view store.MigrationServingState) bool {
	if len(view.Readiness) != 0 || len(view.Ownership)+len(view.Partitions.Partitions) == 0 {
		return false
	}
	for _, proof := range view.Ownership {
		if proof.Role != store.MigrationNonOwner || !store.MigrationOwnershipEvidenceValid(proof) {
			return false
		}
	}
	for _, proof := range view.Partitions.Partitions {
		if proof.Role != store.MigrationNonOwner || !store.MigrationOwnershipPartitionEvidenceValid(view.Partitions, proof) {
			return false
		}
	}
	return true
}
