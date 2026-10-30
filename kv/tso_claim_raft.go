package kv

import (
	"context"
	"crypto/rand"
	stderrors "errors"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// These entry points deliberately remain package-private. Production rolling
// activation, authenticated owner derivation and operation-intent propagation
// are not installed yet. The old uint64 API is NOT a fallback for this path.
func (a *RaftTSOAllocator) reserveClaimGrant(ctx context.Context, owner TSOClaimOwner, count int, minimum uint64) (TSOClaimGrant, error) {
	var empty TSOClaimGrant
	if owner == (TSOClaimOwner{}) {
		return empty, errors.WithStack(ErrTSOClaimInvalid)
	}
	if err := validateTSOMinimumWindow(minimum, count); err != nil {
		return empty, err
	}
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	g, engine, err := a.prepareClaimGrant(ctx, owner, count, minimum)
	if err != nil {
		return empty, err
	}
	payload, err := marshalTSOClaimGrant(g)
	if err != nil {
		return empty, err
	}
	if err := proposeTSOClaim(ctx, a.group.Proposer(), payload); err != nil {
		return empty, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, g.Term, false); err != nil {
		return empty, err
	}
	a.state.claimMu.RLock()
	defer a.state.claimMu.RUnlock()
	if a.state.claims == nil || a.state.claims.Grants[g.ID].Grant != g {
		return empty, errors.Wrap(ErrTSOClaimUnknown, "grant committed without applied record")
	}
	a.initializedTerm = g.Term
	return g, nil
}

func (a *RaftTSOAllocator) prepareClaimGrant(ctx context.Context, owner TSOClaimOwner, count int, minimum uint64) (TSOClaimGrant, raftengine.Engine, error) {
	var empty TSOClaimGrant
	engine, err := a.verifiedLeader(ctx)
	if err != nil {
		return empty, nil, err
	}
	term := engine.Status().Term
	if !a.state.PhaseDActive() || term == 0 {
		return empty, nil, errors.WithStack(ErrTSOPhaseDInactive)
	}
	floor, _, err := a.prepareLeaderTermReservation(ctx, engine, term, false, false)
	if err != nil {
		return empty, nil, err
	}
	base, _, err := a.reservePreparedLocalWindow(count, minimum, floor, false)
	if err != nil {
		return empty, nil, err
	}
	g := TSOClaimGrant{Owner: owner, Term: term, Base: base, Count: uint32(count)} // #nosec G115 -- count was validated against MaxTSOBatchSize.
	if _, err := rand.Read(g.ID[:]); err != nil {
		return empty, nil, errors.Wrap(err, "create TSO claim grant ID")
	}
	return g, engine, nil
}

// commitClaimBatch never allocates or rebinds a candidate. After any unknown
// result, retry EXACTLY these records or reconcile their stable credentials.
// A lost successful response does not turn an absent local cache into authority.
func (a *RaftTSOAllocator) commitClaimBatch(ctx context.Context, term uint64, records []TSOClaimRecord) error {
	payload, err := marshalTSOClaimBatch(term, records)
	if err != nil {
		return err
	}
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	engine, err := a.verifiedLeader(ctx)
	if err != nil {
		return err
	}
	if engine.Status().Term != term {
		return errors.WithStack(ErrTSOClaimClosed)
	}
	if err := proposeTSOClaim(ctx, a.group.Proposer(), payload); err != nil {
		return err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, false); err != nil {
		return errors.Wrap(stderrors.Join(ErrTSOClaimUnknown, err), "claim committed across leadership change")
	}
	a.state.claimMu.RLock()
	defer a.state.claimMu.RUnlock()
	for _, record := range records {
		if a.state.claims == nil || a.state.claims.Claims[record.ID] != record {
			return errors.Wrap(ErrTSOClaimUnknown, "claim committed without matching applied record")
		}
	}
	return nil
}

func proposeTSOClaim(ctx context.Context, proposer raftengine.Proposer, payload []byte) error {
	result, err := proposer.Propose(ctx, payload)
	if err != nil {
		return errors.Wrap(stderrors.Join(ErrTSOClaimUnknown, err), "propose concrete TSO claim")
	}
	if result != nil {
		if responseErr, ok := result.Response.(error); ok {
			return errors.Wrap(responseErr, "apply concrete TSO claim")
		}
	}
	return nil
}

func (a *RaftTSOAllocator) reconcileClaim(ctx context.Context, credential TSOClaimCredential, binding TSOClaimBinding) (TSOClaimProof, error) {
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.verifiedLeader(ctx); err != nil {
		return TSOClaimProof{}, err
	}
	return a.state.lookupClaim(credential, binding)
}

func (a *RaftTSOAllocator) validateTimestampClaim(ctx context.Context, proof TSOClaimProof, expected TSOClaimBinding, phase TSOClaimPhase) error {
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.verifiedLeader(ctx); err != nil {
		return err
	}
	return a.state.validateClaim(proof, expected, phase)
}
