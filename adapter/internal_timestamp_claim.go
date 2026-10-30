package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

func WithInternalTimestampClaimGroup(groupID uint64) InternalOption {
	return func(i *Internal) { i.timestampClaimGroupID = groupID }
}

func (i *Internal) stampClaimedRawTimestamp(ctx context.Context, request *pb.Request) error {
	if i.timestampClaimGroupID == 0 || request.TimestampClaimGroupId != i.timestampClaimGroupID || request.TimestampIntent == nil || len(request.TimestampClaim) == 0 {
		return errors.WithStack(kv.ErrTSOClaimInvalid)
	}
	if err := kv.ValidateRequestTimestampClaim(ctx, i.tsAllocator, request); err != nil {
		return errors.Wrap(err, "stamp forwarded raw claim")
	}
	return i.ensureRawWriteAllowed(request)
}

func (i *Internal) claimPrepareApplyVersion() uint32 {
	if i.kvForwardRejected {
		return kv.TSOClaimProtocolVersion
	}
	if _, ok := i.store.(kv.PreparedClaimMutationApplier); ok {
		return kv.TSOClaimProtocolVersion
	}
	return 0
}

func (i *Internal) stampClaimedTxnTimestamps(ctx context.Context, requests []*pb.Request) (uint64, error) {
	var common uint64
	for _, request := range requests {
		if request == nil || request.TimestampIntent == nil || i.timestampClaimGroupID == 0 || request.TimestampClaimGroupId != i.timestampClaimGroupID {
			return 0, errors.WithStack(kv.ErrTSOClaimInvalid)
		}
		if err := i.stampClaimedTxnRequest(ctx, request); err != nil {
			return 0, errors.WithStack(err)
		}
		claim, err := kv.DecodeRequestTimestampClaim(request.TimestampClaim)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if common != 0 && common != claim.Proof.Record.Timestamp {
			return 0, errors.WithStack(kv.ErrTSOClaimInvalid)
		}
		common = claim.Proof.Record.Timestamp
	}
	return common, nil
}

func (i *Internal) stampClaimedTxnRequest(ctx context.Context, request *pb.Request) error {
	if request.Phase == pb.Phase_COMMIT || request.Phase == pb.Phase_ABORT {
		return errors.WithStack(kv.ValidateClaimedTxnResolution(ctx, i.tsAllocator, i.store, request))
	}
	return errors.WithStack(kv.ValidateRequestTimestampClaim(ctx, i.tsAllocator, request))
}

func internalClaimProtocolVersion(requests []*pb.Request) uint32 {
	for _, request := range requests {
		if len(request.GetTimestampClaim()) != 0 {
			return kv.TSOClaimProtocolVersion
		}
	}
	return 0
}
