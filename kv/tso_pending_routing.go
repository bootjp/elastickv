package kv

import (
	"context"
	stderrors "errors"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateLegacyResolutionThrough(ctx context.Context, allocator any, start, commit uint64) error {
	validator, ok := allocator.(DurableLegacyResolutionValidator)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return errors.WithStack(validator.ValidateLegacyResolutionTimestamps(ctx, start, commit))
}

func (b *BatchAllocator) ValidateLegacyResolutionTimestamps(ctx context.Context, start, commit uint64) error {
	return validateLegacyResolutionThrough(ctx, b.tso, start, commit)
}

func (d *DynamicTimestampAllocator) ValidateLegacyResolutionTimestamps(ctx context.Context, start, commit uint64) error {
	return validateLegacyResolutionThrough(ctx, d.currentTimestampAllocator(), start, commit)
}

func (a *ShadowTimestampAllocator) ValidateLegacyResolutionTimestamps(ctx context.Context, start, commit uint64) error {
	return validateLegacyResolutionThrough(ctx, a.shadow, start, commit)
}

func (a *LeaderRoutedTSOAllocator) ValidateLegacyResolutionTimestamps(ctx context.Context, start, commit uint64) error {
	if start == 0 || commit <= start {
		return errors.WithStack(ErrTSOTimestampInvalid)
	}
	ctx, cancel := context.WithTimeout(nonNilTSOContext(ctx), a.retryBudget)
	defer cancel()
	for {
		started := time.Now()
		path, err := a.tryLegacyResolutionValidation(ctx, start, commit)
		a.observeRequest("validate_legacy_resolution", path, err, time.Since(started))
		if err == nil || errors.Is(err, ErrTSOPhaseDInactive) || errors.Is(err, ErrTSOProtocolUnsupported) || !isTransientTSORouteError(err) {
			return err
		}
		if waitErr := waitTSORouteRetry(ctx, a.retryInterval); waitErr != nil {
			return errors.Wrap(stderrors.Join(waitErr, err), "tso legacy resolution validation")
		}
	}
}

func (a *LeaderRoutedTSOAllocator) tryLegacyResolutionValidation(ctx context.Context, start, commit uint64) (string, error) {
	if a.local.IsLeader() {
		return tsoRequestPathLocal, validateLegacyResolutionThrough(ctx, a.local, start, commit)
	}
	addr := leaderAddrFromEngine(a.leader)
	if addr == "" {
		return "remote", errors.WithStack(ErrLeaderNotFound)
	}
	return "remote", a.requestRemoteLegacyResolution(ctx, addr, start, commit)
}

func (a *LeaderRoutedTSOAllocator) requestRemoteLegacyResolution(ctx context.Context, addr string, start, commit uint64) error {
	conn, err := a.connCache.ConnFor(addr)
	if err != nil {
		return errors.Wrap(err, "tso dial legacy resolution leader")
	}
	resp, err := pb.NewDistributionClient(conn).ValidateTimestamp(ctx, &pb.ValidateTimestampRequest{
		Timestamp: commit, StartTimestamp: start, LegacyResolution: true,
	})
	if err != nil {
		return tsoValidationRPCError(err)
	}
	if !resp.GetValid() || !resp.GetLegacyResolutionValidated() {
		return errors.Wrap(ErrTSOProtocolUnsupported, "leader did not acknowledge legacy resolution validation")
	}
	return nil
}

func tsoValidationRPCError(err error) error {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok &&
			info.GetDomain() == TSOValidationErrorDomain && info.GetReason() == TSOPhaseDInactiveReason {
			return errors.Wrap(stderrors.Join(err, ErrTSOPhaseDInactive), "tso phase D is inactive at leader")
		}
	}
	if status.Code(err) == codes.OutOfRange {
		return errors.Wrap(stderrors.Join(err, ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD), "tso validate at leader")
	}
	if status.Code(err) == codes.InvalidArgument {
		return errors.Wrap(stderrors.Join(err, ErrTSOTimestampInvalid), "tso validate at leader")
	}
	return errors.Wrap(err, "tso validate at leader")
}
