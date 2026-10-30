package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

func (c *ShardedCoordinator) dispatchClaimedPrefixBroadcast(ctx context.Context, group *OperationGroup[OP]) (*CoordinateResponse, error) {
	if group.IsTxn {
		return nil, errors.Wrap(ErrInvalidRequest, "DEL_PREFIX not supported in transactions")
	}
	if err := validateDelPrefixOnly(group.Elems); err != nil {
		return nil, err
	}
	if err := c.rejectWriteFencedDelPrefixes(group.Elems); err != nil {
		return nil, err
	}
	requests, err := c.claimedPrefixRequests(ctx, group)
	if err != nil {
		return nil, err
	}
	if err := c.rejectWriteTimestampFloorDelPrefixes(group.Elems, group.CommitTS); err != nil {
		return nil, err
	}
	if err := c.rejectDelPrefixesWithoutTargetReadinessProof(ctx, group.Elems, group.CommitTS); err != nil {
		return nil, err
	}
	for _, request := range requests {
		if err := c.ensureMutationWriteAllowed(request.Mutations[0], group.CommitTS); err != nil {
			return nil, err
		}
	}
	return c.broadcastToAllGroups(ctx, requests)
}

func (c *ShardedCoordinator) claimedPrefixRequests(ctx context.Context, group *OperationGroup[OP]) ([]*pb.Request, error) {
	ids, err := c.claimedBroadcastIDs()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 || len(group.Elems) == 0 {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	participants := make([]*pb.Request, 0, len(ids))
	for _, id := range ids {
		participants = append(participants, rawClaimRequestForGroup(group, id))
	}
	if _, err := bindClaimToParticipantRequests(ctx, c.tsAllocator, participants, group); err != nil {
		return nil, err
	}
	requests := make([]*pb.Request, 0, len(group.Elems))
	for index, mutation := range participants[0].Mutations {
		request := proto.CloneOf(participants[0])
		request.Mutations = []*pb.Mutation{proto.CloneOf(mutation)}
		request.TimestampClaimPrefixIndices = []uint32{uint32(index)} // #nosec G115 -- protobuf payload size bounds mutation count.
		requests = append(requests, request)
	}
	return requests, nil
}
