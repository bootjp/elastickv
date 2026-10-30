package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func WithCoordinateTimestampClaimGroup(groupID uint64) CoordinatorOption {
	return func(c *Coordinate) { c.claimGroupID = groupID }
}

func RequestsUseTimestampClaims(requests []*pb.Request) bool {
	for _, request := range requests {
		if request != nil && (request.TimestampIntent != nil || len(request.TimestampClaim) != 0) {
			return true
		}
	}
	return false
}

func rawClaimRequestForGroup(group *OperationGroup[OP], groupIDs ...uint64) *pb.Request {
	request := &pb.Request{TimestampIntent: group.TimestampIntent, ObservedRouteVersion: group.ObservedRouteVersion}
	if len(groupIDs) != 0 {
		request.TimestampClaimGroupId = groupIDs[0]
	}
	for _, elem := range group.Elems {
		request.Mutations = append(request.Mutations, elemToMutation(elem))
	}
	return request
}

func prepareRawGroupTimestampIntent(allocator TimestampAllocator, group *OperationGroup[OP], groupID uint64) error {
	if group.IsTxn || !TimestampClaimsRequired(allocator) {
		return nil
	}
	request := rawClaimRequestForGroup(group, groupID)
	if err := EnsureRequestTimestampIntent(request); err != nil {
		return err
	}
	group.TimestampIntent = request.TimestampIntent
	return nil
}

// Only an unstamped creation may ask the owning leader to issue a claim.
// A supplied timestamp without its exact proof is never restamped or accepted
// through the old uint64 range validator.
func StampRequestTimestampClaim(ctx context.Context, allocator TimestampAllocator, request *pb.Request) error {
	if request == nil {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if len(request.TimestampClaim) != 0 {
		return ValidateRequestTimestampClaim(ctx, allocator, request)
	}
	timestamp, err := unsettableClaimPersistenceTimestamp(request)
	if err != nil {
		return err
	}
	if timestamp != 0 {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return IssueRequestTimestampClaim(ctx, allocator, request)
}

func unsettableClaimPersistenceTimestamp(request *pb.Request) (uint64, error) {
	if !request.IsTxn {
		return request.Ts, nil
	}
	meta, _, err := extractTxnMeta(request.Mutations)
	if err != nil {
		return 0, err
	}
	if request.Phase == pb.Phase_PREPARE {
		return meta.PrepareCommitTS, nil
	}
	return meta.CommitTS, nil
}

func validateRequestTimestampClaims(ctx context.Context, allocator TimestampAllocator, st store.MVCCStore, reqs []*pb.Request, expectedGroupID uint64) error {
	for _, request := range reqs {
		if request == nil {
			continue
		}
		if !requestUsesTimestampClaim(allocator, request) {
			continue
		}
		if err := validateReceivedTimestampClaim(ctx, allocator, st, request, expectedGroupID); err != nil {
			return err
		}
	}
	return nil
}

func validateReceivedTimestampClaim(ctx context.Context, allocator TimestampAllocator, st store.MVCCStore, request *pb.Request, expectedGroupID uint64) error {
	if expectedGroupID == 0 || request.TimestampClaimGroupId != expectedGroupID {
		return errors.Wrap(ErrTSOClaimInvalid, "claim target differs from receiving group")
	}
	if request.IsTxn && (request.Phase == pb.Phase_COMMIT || request.Phase == pb.Phase_ABORT) {
		return ValidateClaimedTxnResolution(ctx, allocator, st, request)
	}
	return ValidateRequestTimestampClaim(ctx, allocator, request)
}
