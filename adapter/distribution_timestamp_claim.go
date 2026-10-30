package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *DistributionServer) getConcreteTimestampClaim(ctx context.Context, request *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	if request.Count != 0 || request.MinTimestamp != 0 || request.ActivateCutover || request.ActivatePhaseD {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "claim requests cannot carry reservation or activation flags"))
	}
	intent, err := kv.DecodeSignedTimestampClaim(request.ClaimRequest)
	if err != nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, err.Error()))
	}
	issuer, ok := s.timestampAllocator.(kv.TSOClaimIssuer)
	if !ok {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "concrete timestamp claims unavailable"))
	}
	var record kv.TSOClaimRecord
	if request.ClaimLookupOnly {
		record, err = issuer.LookupTimestampClaim(ctx, intent)
	} else {
		record, err = issuer.ClaimTimestamp(ctx, intent)
	}
	if err != nil {
		return nil, claimTimestampRPCError(err)
	}
	raw, err := kv.EncodeTimestampClaimRecord(record)
	if err != nil {
		return nil, timestampRPCError(err)
	}
	return &pb.GetTimestampResponse{Timestamp: record.Timestamp, TimestampClaim: raw,
		ClaimProtocolVersion: kv.TSOClaimProtocolVersion, CommittedByDedicatedTso: true, Count: 1, PhaseDActive: true}, nil
}

func (s *DistributionServer) validateConcreteTimestampClaim(ctx context.Context, request *pb.ValidateTimestampRequest) (*pb.ValidateTimestampResponse, error) {
	if request.Timestamp != 0 || request.StartTimestamp != 0 || request.LegacyResolution {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "claim validation cannot carry legacy validation fields"))
	}
	if err := kv.ValidateRequestTimestampClaim(ctx, s.timestampAllocator, request.ClaimedRequest); err != nil {
		return nil, claimTimestampRPCError(err)
	}
	return &pb.ValidateTimestampResponse{Valid: true, PhaseDActive: true, ClaimProtocolVersion: kv.TSOClaimProtocolVersion}, nil
}

func claimTimestampRPCError(err error) error {
	switch {
	case errors.Is(err, kv.ErrTSOClaimUnknown):
		return errors.WithStack(status.Error(codes.NotFound, err.Error()))
	case errors.Is(err, kv.ErrTSOClaimInvalid), errors.Is(err, kv.ErrTSOClaimConflict):
		return errors.WithStack(status.Error(codes.InvalidArgument, err.Error()))
	case errors.Is(err, kv.ErrTSOClaimAuthorityRequired):
		return errors.WithStack(status.Error(codes.PermissionDenied, err.Error()))
	case errors.Is(err, kv.ErrTSOClaimCapacity):
		return errors.WithStack(status.Error(codes.ResourceExhausted, err.Error()))
	default:
		return timestampRPCError(err)
	}
}
