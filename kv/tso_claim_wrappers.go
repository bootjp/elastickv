package kv

import (
	"context"

	"github.com/cockroachdb/errors"
)

func claimIssuerThrough(value any) (TSOClaimIssuer, error) {
	issuer, ok := value.(TSOClaimIssuer)
	if !ok {
		return nil, errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return issuer, nil
}

func (b *BatchAllocator) TimestampClaimIdentity() *TSOClaimIdentity { return b.claimIdentity }

func (b *BatchAllocator) ClaimTimestamp(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(b.tso)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.ClaimTimestamp(ctx, s)
	return record, errors.Wrap(err, "claim through timestamp allocator")
}

func (b *BatchAllocator) LookupTimestampClaim(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(b.tso)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.LookupTimestampClaim(ctx, s)
	return record, errors.Wrap(err, "lookup through timestamp allocator")
}

func (d *DynamicTimestampAllocator) TimestampClaimIdentity() *TSOClaimIdentity {
	return timestampClaimIdentity(d.currentTimestampAllocator())
}

func (d *DynamicTimestampAllocator) ClaimTimestamp(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(d.currentTimestampAllocator())
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.ClaimTimestamp(ctx, s)
	return record, errors.Wrap(err, "claim through dynamic timestamp allocator")
}

func (d *DynamicTimestampAllocator) LookupTimestampClaim(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(d.currentTimestampAllocator())
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.LookupTimestampClaim(ctx, s)
	return record, errors.Wrap(err, "lookup through dynamic timestamp allocator")
}

func (a *ShadowTimestampAllocator) TimestampClaimIdentity() *TSOClaimIdentity {
	if a.cutover == nil || !a.cutover.CutoverActive() {
		return nil
	}
	return timestampClaimIdentity(a.shadow)
}

func (a *ShadowTimestampAllocator) ClaimTimestamp(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(a.shadow)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.ClaimTimestamp(ctx, s)
	return record, errors.Wrap(err, "claim through shadow timestamp allocator")
}

func (a *ShadowTimestampAllocator) LookupTimestampClaim(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	issuer, err := claimIssuerThrough(a.shadow)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.LookupTimestampClaim(ctx, s)
	return record, errors.Wrap(err, "lookup through shadow timestamp allocator")
}

func (f CoordinatorForwarder) TimestampClaimIdentity() *TSOClaimIdentity {
	allocator, _ := f.innerTimestampAllocator()
	return timestampClaimIdentity(allocator)
}

func (f CoordinatorForwarder) ClaimTimestamp(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	allocator, _ := f.innerTimestampAllocator()
	issuer, err := claimIssuerThrough(allocator)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.ClaimTimestamp(ctx, s)
	return record, errors.Wrap(err, "claim through coordinator timestamp allocator")
}

func (f CoordinatorForwarder) LookupTimestampClaim(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	allocator, _ := f.innerTimestampAllocator()
	issuer, err := claimIssuerThrough(allocator)
	if err != nil {
		return TSOClaimRecord{}, err
	}
	record, err := issuer.LookupTimestampClaim(ctx, s)
	return record, errors.Wrap(err, "lookup through coordinator timestamp allocator")
}
