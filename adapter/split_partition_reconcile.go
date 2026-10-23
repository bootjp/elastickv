package adapter

import (
	"context"
	"slices"
)

func (s *DistributionServer) reconcileSplitRangePartitions(ctx context.Context) error {
	if s.ownershipRefresh == nil {
		return nil
	}
	groups := make([]uint64, 0, len(s.ownershipRefresh.groups))
	for groupID := range s.ownershipRefresh.groups {
		groups = append(groups, groupID)
	}
	slices.Sort(groups)
	for _, groupID := range groups {
		if s.MigrationPartitionRuntimeConfigured(groupID) {
			if err := s.reconcileMigrationPartitions(ctx, groupID); err != nil {
				return err
			}
		}
	}
	return nil
}
