package kv

import (
	"context"
	"maps"
	"slices"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

type admittedBroadcastParticipant struct {
	group    *ShardGroup
	requests []*pb.Request
}

// Route-local preflight is insufficient for a full-prefix broadcast. Finish
// every participant's whole-request admission before calling any Commit.
// Apply-time admission still repeats these checks; this is not distributed
// atomic commit against concurrent state changes or transport failures.
func (c *ShardedCoordinator) admitBroadcastRequests(ctx context.Context, requests []*pb.Request) ([]admittedBroadcastParticipant, error) {
	groups, err := c.allShardGroups()
	if err != nil {
		return nil, err
	}
	// allShardGroups uses these same immutable, ordered configuration IDs.
	ids := c.allShardGroupIDs
	if !c.allShardGroupsConfigured {
		ids = slices.Sorted(maps.Keys(c.groups))
	}
	if len(ids) != len(groups) {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	migration, err := broadcastIntersectsMigration(ctx, groups, requests)
	if err != nil {
		return nil, err
	}
	participants := make([]admittedBroadcastParticipant, 0, len(groups))
	for i, group := range groups {
		if c.groups[ids[i]] != group || group.Txn == nil {
			return nil, errors.WithStack(ErrLeaderNotFound)
		}
		owned, err := c.admitBroadcastParticipant(ctx, group, ids[i], requests, migration)
		if err != nil {
			return nil, err
		}
		participants = append(participants, admittedBroadcastParticipant{group: group, requests: owned})
	}
	return participants, nil
}

func (c *ShardedCoordinator) admitBroadcastParticipant(ctx context.Context, group *ShardGroup, groupID uint64, requests []*pb.Request, migration bool) ([]*pb.Request, error) {
	if migration {
		if _, supported := group.Store.(store.MigrationOwnershipStore); !supported {
			return nil, errors.WithStack(store.ErrNotSupported)
		}
	}
	owned := make([]*pb.Request, len(requests))
	for i, request := range requests {
		owned[i] = proto.CloneOf(request)
		if len(request.TimestampClaim) != 0 {
			owned[i].TimestampClaimGroupId = groupID
		}
	}
	if err := StampTimestampAdmission(ctx, c.tsAllocator, group.Store, owned, groupID); err != nil {
		return nil, err
	}
	// Evidence-free legacy groups without a store keep their existing behavior.
	// They cannot participate in a migration-bearing broadcast above.
	if group.Store != nil {
		for _, request := range owned {
			if err := CheckMigrationRequest(ctx, group.Store, groupID, request); err != nil {
				return nil, err
			}
		}
	}
	return owned, nil
}

func broadcastIntersectsMigration(ctx context.Context, groups []*ShardGroup, requests []*pb.Request) (bool, error) {
	var ranges []migrationApplyRange
	for _, request := range requests {
		footprint, err := migrationServingFootprint(request)
		if err != nil {
			return false, err
		}
		ranges = append(ranges, footprint...)
	}
	for _, group := range groups {
		view, err := readMigrationServingState(ctx, group.Store)
		if err != nil {
			return false, err
		}
		if migrationApplyIntersects(view, ranges) {
			return true, nil
		}
	}
	return false, nil
}
