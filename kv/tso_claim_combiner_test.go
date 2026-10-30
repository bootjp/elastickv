package kv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatedTSOClaimCommitter struct {
	issuer  tsoClaimCommitter
	entered chan []TSOClaimRecord
	release chan struct{}
}

func (g *gatedTSOClaimCommitter) commitClaimBatch(ctx context.Context, term uint64, records []TSOClaimRecord) error {
	select {
	case g.entered <- append([]TSOClaimRecord(nil), records...):
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-g.release:
		return g.issuer.commitClaimBatch(ctx, term, records)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestTSOClaimCombinerWaitsForAppliedSingleton(t *testing.T) {
	alloc, state := newSlotClaimTSOFixture(t)
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	gate := &gatedTSOClaimCommitter{issuer: alloc, entered: make(chan []TSOClaimRecord, 1), release: make(chan struct{})}
	combiner := newTSOClaimCombiner(t.Context(), gate)
	defer combiner.close()
	done := make(chan error, 1)
	go func() {
		_, err := combiner.claim(t.Context(), g.Term, p)
		done <- err
	}()
	select {
	case batch := <-gate.entered:
		require.Len(t, batch, 1, "low load must not wait for a full batch")
	case <-time.After(time.Second):
		t.Fatal("singleton collection did not finish")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before consensus/apply: %v", err)
	default:
	}
	require.ErrorIs(t, state.validateClaim(p, p.Record.Binding, TSOClaimPrimary), ErrTSOClaimUnknown)
	close(gate.release)
	require.NoError(t, <-done)
	require.NoError(t, state.validateClaim(p, p.Record.Binding, TSOClaimPrimary))
}

func TestTSOClaimCombinerQueueIsBoundedAndShutdownCancelsProposal(t *testing.T) {
	alloc, _ := newSlotClaimTSOFixture(t)
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	gate := &gatedTSOClaimCommitter{issuer: alloc, entered: make(chan []TSOClaimRecord, 1), release: make(chan struct{})}
	combiner := newTSOClaimCombiner(t.Context(), gate)
	done := make(chan error, 1)
	go func() {
		_, err := combiner.claim(t.Context(), g.Term, p)
		done <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		combiner.close()
		t.Fatal("proposal not started")
	}
	for range tsoClaimQueueCapacity {
		combiner.queue <- &tsoClaimSubmission{ctx: t.Context(), term: g.Term, proof: p, result: make(chan error, 1)}
	}
	require.Equal(t, tsoClaimQueueCapacity, len(combiner.queue))
	_, err = combiner.claim(t.Context(), g.Term, p)
	require.ErrorIs(t, err, ErrTSOClaimCapacity)
	combiner.close()
	require.Error(t, <-done)
}
