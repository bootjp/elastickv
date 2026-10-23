package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (s *DistributionServer) prepareSplitRangePartitions(ctx context.Context, snapshot distribution.CatalogSnapshot, parentID uint64) ([]uint64, error) {
	if s.ownershipRefresh == nil {
		return nil, nil
	}
	parent, found := findRouteByID(snapshot.Routes, parentID)
	if !found {
		return nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	active, err := s.splitRangeGroupHasOwnership(ctx, parent, s.ownershipRefresh.groups[parent.GroupID])
	if err != nil {
		return nil, err
	}
	groups, err := s.splitRangeOwnerGroups(ctx, snapshot, parent, active)
	if err != nil || len(groups) == 0 {
		return nil, err
	}
	if s.splitJobRunnerReadinessGate == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	if err := s.splitJobRunnerReadinessGate(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureMigrationNamespaces(ctx, groups); err != nil {
		return nil, err
	}
	return groups, s.reconcileSplitRangePartitions(ctx, groups)
}

func (s *DistributionServer) splitRangeGroupHasOwnership(ctx context.Context, parent distribution.RouteDescriptor, group *kv.ShardGroup) (bool, error) {
	view, err := s.splitPartitionAdmissionView(ctx, parent.GroupID, group)
	if err != nil {
		return false, errors.WithStack(err)
	}
	active := splitRangeOwnershipIntersects(view, parent)
	if active && !s.MigrationPartitionRuntimeConfigured(parent.GroupID) {
		return false, errors.WithStack(store.ErrNotSupported)
	}
	return active, nil
}

func splitRangeOwnershipIntersects(view store.MigrationServingState, parent distribution.RouteDescriptor) bool {
	for _, proof := range view.Ownership {
		if routeRangeIntersects(proof.RouteStart, proof.RouteEnd, parent.Start, parent.End) {
			return true
		}
	}
	for _, partition := range view.Partitions.Partitions {
		if routeRangeIntersects(partition.RouteStart, partition.RouteEnd, parent.Start, parent.End) {
			return true
		}
	}
	return false
}

func (s *DistributionServer) reconcileSplitRangePartitions(ctx context.Context, groups []uint64) error {
	for _, groupID := range groups {
		if err := s.reconcileSplitServingOwners(ctx, groupID); err != nil {
			return err
		}
	}
	return nil
}
