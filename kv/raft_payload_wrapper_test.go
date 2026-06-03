package kv

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
)

// fakeProposer records every Propose / ProposeAdmin call so the
// wrapper tests can inspect both (a) what bytes the engine would
// have seen, and (b) which method routed there — so a test that
// expects wrappedProposer.ProposeAdmin to bypass the wrap layer
// can assert against adminCalls / adminLast independently of
// (Propose) calls / last.
type fakeProposer struct {
	calls      atomic.Int32
	last       []byte
	adminCalls atomic.Int32
	adminLast  []byte
	resp       *raftengine.ProposalResult
	err        error
}

func (p *fakeProposer) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.calls.Add(1)
	cp := make([]byte, len(data))
	copy(cp, data)
	p.last = cp
	if p.err != nil {
		return nil, p.err
	}
	if p.resp == nil {
		return &raftengine.ProposalResult{CommitIndex: 1}, nil
	}
	return p.resp, nil
}

func (p *fakeProposer) ProposeAdmin(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.adminCalls.Add(1)
	cp := make([]byte, len(data))
	copy(cp, data)
	p.adminLast = cp
	if p.err != nil {
		return nil, p.err
	}
	if p.resp == nil {
		return &raftengine.ProposalResult{CommitIndex: 1}, nil
	}
	return p.resp, nil
}

func TestApplyRaftPayloadWrap_NilIsPassThrough(t *testing.T) {
	t.Parallel()
	got, err := applyRaftPayloadWrap(nil, []byte("hello"))
	if err != nil {
		t.Fatalf("applyRaftPayloadWrap: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("nil wrapper mutated payload: got %q", got)
	}
}

func TestApplyRaftPayloadWrap_PropagatesError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("wrap-side fail")
	wrap := func([]byte) ([]byte, error) { return nil, sentinel }
	_, err := applyRaftPayloadWrap(wrap, []byte("x"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel, got %v", err)
	}
}

// TestWrappedProposer_ProposeAdminBypassesWrap pins the §6.3
// invariant the Stage 6E-2b ProposeAdmin path must enforce: even
// when a wrap closure is configured, ProposeAdmin payloads MUST
// reach the inner proposer verbatim and MUST take the inner's
// ProposeAdmin path (not Propose). The cutover entry itself sits
// at `index == cutover` and the apply hook's strict-`>` dispatch
// will treat it as cleartext; if wrappedProposer wrapped it
// anyway, the apply side would either (a) fail to find the
// cutover marker because its body is encrypted, or (b) attempt
// to unwrap it against a key that hasn't been activated yet —
// either way bricking the cutover.
func TestWrappedProposer_ProposeAdminBypassesWrap(t *testing.T) {
	t.Parallel()
	var wrapCalls atomic.Int32
	wrap := func(p []byte) ([]byte, error) {
		wrapCalls.Add(1)
		out := make([]byte, len(p)+1)
		out[0] = 'W'
		copy(out[1:], p)
		return out, nil
	}
	inner := &fakeProposer{}
	wp := newWrappedProposer(inner, wrap)
	plain := []byte("cutover-marker")
	if _, err := wp.ProposeAdmin(context.Background(), plain); err != nil {
		t.Fatalf("ProposeAdmin: %v", err)
	}
	if got := wrapCalls.Load(); got != 0 {
		t.Fatalf("wrap closure ran %d times under ProposeAdmin; want 0 — admin path must bypass wrap", got)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Fatalf("inner.Propose called %d times under ProposeAdmin; want 0 — admin path must route through inner.ProposeAdmin", got)
	}
	if got := inner.adminCalls.Load(); got != 1 {
		t.Fatalf("inner.ProposeAdmin call count = %d, want 1", got)
	}
	if !bytes.Equal(inner.adminLast, plain) {
		t.Fatalf("inner.ProposeAdmin saw %q, want %q (cleartext verbatim)", inner.adminLast, plain)
	}
}

func TestNewWrappedProposer_NilWrapperReturnsInnerVerbatim(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	got := newWrappedProposer(inner, nil)
	// Stage 3 default: identical pointer; no allocation.
	if got != raftengine.Proposer(inner) {
		t.Fatal("nil wrapper: newWrappedProposer should return the inner proposer verbatim")
	}
}

func TestWrappedProposer_InvokesWrapperOncePerCall(t *testing.T) {
	t.Parallel()
	var wrapCalls atomic.Int32
	wrap := func(p []byte) ([]byte, error) {
		wrapCalls.Add(1)
		out := make([]byte, len(p)+1)
		out[0] = 'W'
		copy(out[1:], p)
		return out, nil
	}
	inner := &fakeProposer{}
	wp := newWrappedProposer(inner, wrap)
	if _, err := wp.Propose(context.Background(), []byte("payload")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got := wrapCalls.Load(); got != 1 {
		t.Fatalf("wrapper call count = %d, want 1", got)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("inner.Propose call count = %d, want 1", got)
	}
	want := append([]byte{'W'}, []byte("payload")...)
	if !bytes.Equal(inner.last, want) {
		t.Fatalf("inner saw %q, want %q (wrapper output)", inner.last, want)
	}
}

func TestWrappedProposer_PropagatesWrapperError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("wrapper denied")
	inner := &fakeProposer{}
	wp := newWrappedProposer(inner, func([]byte) ([]byte, error) { return nil, sentinel })
	_, err := wp.Propose(context.Background(), []byte("x"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Fatalf("inner.Propose called %d times despite wrap fail", got)
	}
}

// TestWrappedProposer_RoundTripWithRealCipher exercises the seam end-
// to-end: wrap with a real raft envelope, the inner proposer
// observes the encrypted bytes, and a hand-rolled Unwrap recovers
// the original plaintext (the engine-side hook from
// internal/raftengine/etcd is unit-tested separately; this test
// proves the coordinator's wrap output is shape-compatible with
// what the engine expects).
func TestWrappedProposer_RoundTripWithRealCipher(t *testing.T) {
	t.Parallel()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	const kid uint32 = 0x42
	if err := ks.Set(kid, dek); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	wrap := func(p []byte) ([]byte, error) {
		nonce := make([]byte, encryption.NonceSize)
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		return encryption.WrapRaftPayload(c, kid, nonce, p)
	}
	inner := &fakeProposer{}
	wp := newWrappedProposer(inner, wrap)

	plaintext := []byte("op=put key=k1 v=secret")
	if _, err := wp.Propose(context.Background(), plaintext); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	got, err := encryption.UnwrapRaftPayload(c, inner.last)
	if err != nil {
		t.Fatalf("UnwrapRaftPayload: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q, want %q", got, plaintext)
	}
}
