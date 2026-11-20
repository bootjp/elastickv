package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Migration creation requires a tracked routing version even for raw writes.
// Keep ordinary raw writes unpinned and preserve any caller-supplied version.
// This only supplies the observation; proposal and apply still prove ownership.
func (c *ShardedCoordinator) pinRawMigrationVersion(ctx context.Context, groupID uint64, request *pb.Request, observed uint64) error {
	if request.ObservedRouteVersion != 0 {
		return nil
	}
	group := c.groups[groupID]
	if group == nil || group.Store == nil {
		return nil
	}
	present, err := rawMigrationMetadataPresent(ctx, group.Store)
	if err != nil || !present {
		return err
	}
	view, err := readMigrationServingState(ctx, group.Store)
	if err != nil {
		return err
	}
	ranges, err := migrationServingFootprint(request)
	if err != nil {
		return err
	}
	if !migrationApplyIntersects(view, ranges) {
		return nil
	}
	if observed == 0 {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	request.ObservedRouteVersion = observed
	return nil
}

func rawMigrationMetadataPresent(ctx context.Context, st store.MVCCStore) (bool, error) {
	reader, ok := st.(store.MigrationServingSummaryReader)
	if !ok {
		return true, nil
	}
	summary, err := reader.MigrationServingSummary(ctx)
	return summary.Present, errors.WithStack(err)
}
