package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

func (c *Coordinate) buildStampedRedirectRequests(ctx context.Context, group *OperationGroup[OP]) ([]*pb.Request, error) {
	requests, err := c.buildRedirectRequests(group)
	if err != nil {
		return nil, err
	}
	if TimestampClaimsRequired(c.tsAllocator) {
		if err := c.stampRetainedOperationClaim(ctx, group, requests[0]); err != nil {
			return nil, err
		}
	}
	return requests, nil
}

func (c *Coordinate) buildClaimRedirectRequests(group *OperationGroup[OP]) ([]*pb.Request, error) {
	if group.IsTxn {
		request, err := c.onePhaseClaimRequest(group)
		if err != nil {
			return nil, err
		}
		return []*pb.Request{request}, nil
	}
	request := rawClaimRequestForGroup(group, c.claimGroupID)
	request.TimestampClaim, request.Ts = group.TimestampClaim, group.CommitTS
	if err := restoreRetainedClaimTimestamp(request, group.CommitTS); err != nil {
		return nil, err
	}
	if err := EnsureRequestTimestampIntent(request); err != nil {
		return nil, err
	}
	return []*pb.Request{request}, nil
}

func (c *Coordinate) prepareClaimDispatch(ctx context.Context, group *OperationGroup[OP]) error {
	if err := validateOperationGroup(group); err != nil {
		return err
	}
	if !TimestampClaimsRequired(c.tsAllocator) {
		return nil
	}
	if !group.IsTxn {
		return prepareRawGroupTimestampIntent(c.tsAllocator, group, c.claimGroupID)
	}
	if group.StartTS == 0 {
		timestamp, err := c.nextStartTS(ctx)
		if err != nil {
			return err
		}
		group.StartTS, group.CommitTS = timestamp, 0
	}
	request, err := c.onePhaseClaimRequest(group)
	if err != nil {
		return err
	}
	if err := EnsureRequestTimestampIntent(request); err != nil {
		return err
	}
	group.TimestampIntent = request.TimestampIntent
	return nil
}

func (c *Coordinate) onePhaseClaimRequest(group *OperationGroup[OP]) (*pb.Request, error) {
	primary := transactionPrimaryKey(group.Elems, group.PrimaryKey)
	if len(primary) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	request := onePhaseTxnRequestWithPrevCommit(group.StartTS, group.CommitTS, group.PrevCommitTS, primary, group.Elems, group.ReadKeys, group.ObservedRouteVersion, nil)
	request.TimestampIntent, request.TimestampClaim, request.TimestampClaimGroupId = group.TimestampIntent, group.TimestampClaim, c.claimGroupID
	return request, nil
}

func (c *Coordinate) dispatchSelectedTxn(ctx context.Context, group *OperationGroup[OP]) (*CoordinateResponse, error) {
	if !TimestampClaimsRequired(c.tsAllocator) {
		return c.dispatchTxn(ctx, group.Elems, group.ReadKeys, group.StartTS, group.CommitTS, group.PrevCommitTS, group.ObservedRouteVersion, group.KeyVizLabel, group.PrimaryKey)
	}
	request, err := c.onePhaseClaimRequest(group)
	if err != nil {
		return nil, err
	}
	if err := c.stampRetainedOperationClaim(ctx, group, request); err != nil {
		return nil, err
	}
	commitTS, err := requestCommitTS(request)
	if err != nil {
		return nil, err
	}
	group.TimestampClaim, group.TimestampIntent, group.CommitTS = request.TimestampClaim, request.TimestampIntent, commitTS
	response, err := c.transactionManager.Commit(ctx, []*pb.Request{request})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CoordinateResponse{CommitIndex: response.CommitIndex, CommitTS: commitTS}, nil
}

func (c *Coordinate) stampRetainedOperationClaim(ctx context.Context, group *OperationGroup[OP], request *pb.Request) error {
	if err := restoreRetainedClaimTimestamp(request, group.CommitTS); err != nil {
		return err
	}
	if err := StampRequestTimestampClaim(ctx, c.tsAllocator, request); err != nil {
		return err
	}
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	if err != nil {
		return err
	}
	group.TimestampIntent, group.TimestampClaim, group.CommitTS = request.TimestampIntent, request.TimestampClaim, claim.Proof.Record.Timestamp
	return nil
}
