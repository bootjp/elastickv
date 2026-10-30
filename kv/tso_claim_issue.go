package kv

import (
	"context"
	"crypto/rand"
	"crypto/sha256"

	"github.com/cockroachdb/errors"
)

var ErrTSOClaimAuthorityRequired = errors.New("tso: authenticated claim issuance authority is required")

func WithTSOClaimAuthority(authority TSOClaimAuthority) RaftTSOAllocatorOption {
	return func(a *RaftTSOAllocator) { a.claimAuthority = authority }
}

func WithRaftTSOClaimIdentity(identity *TSOClaimIdentity) RaftTSOAllocatorOption {
	return func(a *RaftTSOAllocator) { a.claimIdentity = identity }
}

func (a *RaftTSOAllocator) TimestampClaimIdentity() *TSOClaimIdentity { return a.claimIdentity }

func (a *RaftTSOAllocator) authorizeSignedClaim(ctx context.Context, s TSOSignedClaimIntent) error {
	if err := s.verify(); err != nil {
		return err
	}
	if a.claimAuthority == nil {
		return errors.WithStack(ErrTSOClaimAuthorityRequired)
	}
	return errors.Wrap(a.claimAuthority.AuthorizeTimestampClaim(ctx, s.PublicKey), "authorize timestamp claim signer")
}

func (a *RaftTSOAllocator) LookupTimestampClaim(ctx context.Context, s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	ctx, cancel := context.WithTimeout(nonNilTSOContext(ctx), tsoClaimCommitBudget)
	defer cancel()
	if err := s.verify(); err != nil {
		return TSOClaimRecord{}, err
	}
	if a.claimAuthority == nil {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimAuthorityRequired)
	}
	if err := a.claimAuthority.AuthorizeTimestampClaimLookup(ctx); err != nil {
		return TSOClaimRecord{}, errors.WithStack(err)
	}
	if err := a.lockClaimAllocator(ctx); err != nil {
		return TSOClaimRecord{}, err
	}
	defer a.mu.Unlock()
	if _, err := a.verifiedLeader(ctx); err != nil {
		return TSOClaimRecord{}, err
	}
	return a.state.recordForSignedClaim(s)
}

func (a *RaftTSOAllocator) selectIssuanceGrant(ctx context.Context, s TSOSignedClaimIntent, term uint64, staged *tsoClaimLedger) (TSOClaimGrant, error) {
	if g, ok := a.state.openIssuanceGrant(s, term, staged); ok {
		return g, nil
	}
	g, engine, err := a.prepareClaimGrant(ctx, s.Binding.Owner, int(s.GrantSize), s.Minimum)
	if err != nil {
		return TSOClaimGrant{}, err
	}
	payload, err := marshalTSOClaimGrant(g)
	if err != nil {
		return TSOClaimGrant{}, err
	}
	if err := proposeTSOClaim(ctx, a.group.Proposer(), payload); err != nil {
		return TSOClaimGrant{}, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, false); err != nil {
		return TSOClaimGrant{}, err
	}
	a.initializedTerm = term
	return g, nil
}

func (f *TSOStateMachine) openIssuanceGrant(s TSOSignedClaimIntent, term uint64, staged *tsoClaimLedger) (TSOClaimGrant, bool) {
	f.claimMu.RLock()
	defer f.claimMu.RUnlock()
	var selected TSOClaimGrant
	if f.claims == nil {
		return selected, false
	}
	for _, retained := range f.claims.Grants {
		g := retained.Grant
		if retained.Closed || g.Term != term || g.Owner != s.Binding.Owner {
			continue
		}
		if max(s.Minimum, f.claims.Cursors[g.ID], staged.Cursors[g.ID]) >= g.end() {
			continue
		}
		if g.Base > selected.Base {
			selected = g
		}
	}
	return selected, selected.ID != (TSOClaimID{})
}

func issueTSOObservation(ctx context.Context, issuer TSOClaimIssuer, identity *TSOClaimIdentity, size int, minimum uint64) (uint64, error) {
	var operation TSOOperationID
	if _, err := rand.Read(operation[:]); err != nil {
		return 0, errors.Wrap(err, "create observation operation identity")
	}
	binding := TSOClaimBinding{Owner: identity.owner(), Operation: operation,
		Intent: TSOIntentDigest(sha256.Sum256([]byte("elastickv/read-observation/v1"))), Role: TSOClaimObservation}
	s, _, err := identity.signClaim(binding, minimum, size)
	if err != nil {
		return 0, err
	}
	r, err := issuer.ClaimTimestamp(ctx, s)
	if err != nil {
		return 0, errors.Wrap(err, "issue read observation claim")
	}
	if !s.matchesRecord(r) {
		return 0, errors.WithStack(ErrTSOClaimInvalid)
	}
	return r.Timestamp, nil
}
