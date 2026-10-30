package kv

import (
	"context"
	stderrors "errors"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (a *LeaderRoutedTSOAllocator) TimestampClaimIdentity() *TSOClaimIdentity {
	return timestampClaimIdentity(a.local)
}

func (a *LeaderRoutedTSOAllocator) ClaimTimestamp(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	return a.routeTimestampClaim(ctx, s, false)
}

func (a *LeaderRoutedTSOAllocator) LookupTimestampClaim(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	return a.routeTimestampClaim(ctx, s, true)
}

func (a *LeaderRoutedTSOAllocator) routeTimestampClaim(ctx context.Context, s TSOSignedClaimIntent, lookup bool) (TSOClaimRecord, error) {
	if err := s.verify(); err != nil {
		return TSOClaimRecord{}, err
	}
	ctx, cancel := context.WithTimeout(nonNilTSOContext(ctx), a.retryBudget)
	defer cancel()
	for {
		r, err := a.tryTimestampClaim(ctx, s, lookup)
		if err == nil {
			return r, nil
		}
		if errors.Is(err, ErrTSOProtocolUnsupported) || !isTransientTSORouteError(err) {
			return TSOClaimRecord{}, err
		}
		if waitErr := waitTSORouteRetry(ctx, a.retryInterval); waitErr != nil {
			return TSOClaimRecord{}, errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err, waitErr))
		}
	}
}

func (a *LeaderRoutedTSOAllocator) tryTimestampClaim(ctx context.Context, s TSOSignedClaimIntent, lookup bool) (TSOClaimRecord, error) {
	if a.local.IsLeader() {
		return a.localTimestampClaim(ctx, s, lookup)
	}
	addr := leaderAddrFromEngine(a.leader)
	if addr == "" {
		return TSOClaimRecord{}, errors.WithStack(ErrLeaderNotFound)
	}
	conn, err := a.connCache.ConnFor(addr)
	if err != nil {
		return TSOClaimRecord{}, errors.Wrap(err, "dial concrete claim leader")
	}
	raw, err := EncodeSignedTimestampClaim(s)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	started := time.Now()
	response, err := pb.NewDistributionClient(conn).GetTimestamp(ctx, &pb.GetTimestampRequest{ClaimRequest: raw, ClaimLookupOnly: lookup})
	a.observeRequest("claim", "remote", err, time.Since(started))
	if err != nil {
		return TSOClaimRecord{}, remoteTimestampClaimError(err)
	}
	return validateRoutedTimestampClaim(s, response)
}

func (a *LeaderRoutedTSOAllocator) localTimestampClaim(ctx context.Context, s TSOSignedClaimIntent, lookup bool) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(a.local)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	if lookup {
		record, err := issuer.LookupTimestampClaim(ctx, s)
		return record, errors.Wrap(err, "lookup local routed timestamp claim")
	}
	record, err := issuer.ClaimTimestamp(ctx, s)
	return record, errors.Wrap(err, "issue local routed timestamp claim")
}

func remoteTimestampClaimError(err error) error {
	if status.Code(err) == codes.NotFound {
		return errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err))
	}
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Canceled {
		return errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err))
	}
	return errors.WithStack(err)
}

func validateRoutedTimestampClaim(s TSOSignedClaimIntent, response *pb.GetTimestampResponse) (TSOClaimRecord, error) {
	if response.GetClaimProtocolVersion() != TSOClaimProtocolVersion || len(response.GetTimestampClaim()) == 0 {
		return TSOClaimRecord{}, errors.Wrap(ErrTSOProtocolUnsupported, "leader did not acknowledge concrete claim protocol")
	}
	record, err := DecodeTimestampClaimRecord(response.GetTimestampClaim())
	if err != nil {
		return TSOClaimRecord{}, err
	}
	if !s.matchesRecord(record) || response.Timestamp != record.Timestamp {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	return record, nil
}
