package kv

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/cockroachdb/errors"
)

type tsoLiveClaimResult struct {
	record TSOClaimRecord
	err    error
}
type tsoLiveClaimCall struct {
	ctx    context.Context
	signed TSOSignedClaimIntent
	done   chan tsoLiveClaimResult
}

// Every call retains its authenticated context. Authorization is never copied
// from the first request into the rest of a combined proposal.
func (a *RaftTSOAllocator) ClaimTimestamp(ctx context.Context, signed TSOSignedClaimIntent) (TSOClaimRecord, error) {
	ctx, cancel := context.WithTimeout(nonNilTSOContext(ctx), tsoClaimCommitBudget)
	defer cancel()
	if err := signed.verify(); err != nil {
		return TSOClaimRecord{}, err
	}
	if a.claimAuthority == nil {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimAuthorityRequired)
	}
	if err := a.claimAuthority.AuthorizeTimestampClaimLookup(ctx); err != nil {
		return TSOClaimRecord{}, errors.WithStack(err)
	}
	call := &tsoLiveClaimCall{ctx: ctx, signed: signed, done: make(chan tsoLiveClaimResult, 1)}
	a.claimQueueMu.Lock()
	if len(a.claimQueue) >= tsoClaimQueueCapacity {
		a.claimQueueMu.Unlock()
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimCapacity)
	}
	a.claimQueue = append(a.claimQueue, call)
	if !a.claimQueueFlushing {
		a.claimQueueFlushing = true
		go a.drainLiveClaims()
	}
	a.claimQueueMu.Unlock()
	select {
	case result := <-call.done:
		return result.record, result.err
	case <-ctx.Done():
		return TSOClaimRecord{}, errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, ctx.Err()))
	}
}

func (a *RaftTSOAllocator) drainLiveClaims() {
	for {
		timer := time.NewTimer(tsoClaimCollectDelay)
		<-timer.C
		a.claimQueueMu.Lock()
		n := min(len(a.claimQueue), MaxTSOClaimsPerBatch)
		batch := append([]*tsoLiveClaimCall(nil), a.claimQueue[:n]...)
		a.claimQueue = a.claimQueue[n:]
		a.claimQueueMu.Unlock()
		a.flushLiveClaims(batch)
		a.claimQueueMu.Lock()
		if len(a.claimQueue) == 0 {
			a.claimQueue = nil
			a.claimQueueFlushing = false
			a.claimQueueMu.Unlock()
			return
		}
		a.claimQueueMu.Unlock()
	}
}

func (a *RaftTSOAllocator) flushLiveClaims(batch []*tsoLiveClaimCall) {
	ctx, cancel := context.WithTimeout(context.Background(), tsoClaimCommitBudget)
	defer cancel()
	active := a.authorizeLiveClaims(ctx, batch)
	if len(active) == 0 {
		return
	}
	err := a.lockClaimAllocator(ctx)
	if err == nil {
		err = a.commitLiveClaims(ctx, active)
		a.mu.Unlock()
	}
	for _, call := range active {
		if contextErr := call.ctx.Err(); contextErr != nil {
			call.done <- tsoLiveClaimResult{err: errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, contextErr))}
			continue
		}
		if err != nil {
			call.done <- tsoLiveClaimResult{err: err}
			continue
		}
		record, lookupErr := a.state.recordForSignedClaim(call.signed)
		call.done <- tsoLiveClaimResult{record: record, err: lookupErr}
	}
}

// A concurrent reservation/lookup must not turn the bounded flush into an
// unbounded mutex wait. The fast path does not allocate a timer.
func (a *RaftTSOAllocator) lockClaimAllocator(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err))
	}
	if a.mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, ctx.Err()))
		case <-ticker.C:
			if a.mu.TryLock() {
				return nil
			}
		}
	}
}

func (a *RaftTSOAllocator) authorizeLiveClaims(ctx context.Context, batch []*tsoLiveClaimCall) []*tsoLiveClaimCall {
	active := make([]*tsoLiveClaimCall, 0, len(batch))
	deadline, _ := ctx.Deadline()
	for _, call := range batch {
		if err := call.ctx.Err(); err != nil {
			call.done <- tsoLiveClaimResult{err: errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err))}
			continue
		}
		_, lookupErr := a.state.recordForSignedClaim(call.signed)
		if lookupErr == nil {
			active = append(active, call)
			continue
		}
		if !errors.Is(lookupErr, ErrTSOClaimUnknown) {
			call.done <- tsoLiveClaimResult{err: lookupErr}
			continue
		}
		authCtx, cancel := context.WithDeadline(call.ctx, deadline)
		err := a.authorizeSignedClaim(authCtx, call.signed)
		cancel()
		if err != nil {
			call.done <- tsoLiveClaimResult{err: err}
			continue
		}
		active = append(active, call)
	}
	return active
}

func (a *RaftTSOAllocator) commitLiveClaims(ctx context.Context, calls []*tsoLiveClaimCall) error {
	engine, err := a.verifiedLeader(ctx)
	if err != nil {
		return err
	}
	term := engine.Status().Term
	selected, err := a.planLiveClaims(ctx, term, calls)
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		return nil
	}
	payload, err := marshalTSOClaimIntents(term, selected)
	if err != nil {
		return err
	}
	if err := proposeTSOClaim(ctx, a.group.Proposer(), payload); err != nil {
		return err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, false); err != nil {
		return errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err))
	}
	return nil
}

func (a *RaftTSOAllocator) planLiveClaims(ctx context.Context, term uint64, calls []*tsoLiveClaimCall) ([]tsoSelectedClaimIntent, error) {
	staged := newTSOClaimLedger()
	selected := make([]tsoSelectedClaimIntent, 0, len(calls))
	for _, call := range calls {
		s := call.signed
		_, err := a.state.recordForSignedClaim(s)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrTSOClaimUnknown) {
			return nil, err
		}
		if prior, exists := staged.Claims[s.ID]; exists {
			if !s.matchesRecord(prior) {
				return nil, errors.WithStack(ErrTSOClaimConflict)
			}
			continue
		}
		g, err := a.selectIssuanceGrant(ctx, s, term, staged)
		if err != nil {
			return nil, err
		}
		intent := tsoSelectedClaimIntent{Grant: g.ID, Intent: s}
		a.state.claimMu.RLock()
		record, err := a.state.claims.selectClaim(intent, staged)
		a.state.claimMu.RUnlock()
		if err != nil {
			return nil, err
		}
		staged.insertRecord(record)
		selected = append(selected, intent)
	}
	return selected, nil
}
