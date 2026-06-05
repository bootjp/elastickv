package kv

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

// TestWrappedProposer_ProposeAdminAppliesWrap pins the invariant
// codex P1 round-1 surfaced: admin entries that land at
// `index > raftEnvelopeCutoverIndex` (any post-cutover
// BootstrapEncryption / RotateDEK / RegisterEncryptionWriter
// committed during normal operation) MUST be wrapped, else the
// §6.3 strict-`>` apply hook will try to AEAD-decrypt cleartext
// bytes and halt apply on integrity failure.
//
// ProposeAdmin's only divergence from Propose is the future §7.1
// quiescence-barrier exemption Stage 6E-2d installs on Propose
// only — wrap behaviour is identical between the two paths. The
// lone admin entry that must remain cleartext is the
// EnableRaftEnvelope cutover marker itself (at index == cutover),
// and that one is handled by routing it through the raw engine
// reference instead of wrappedProposer, NOT by a method-level
// wrap bypass.
//
// This test pins the corrected behaviour. The earlier round-1
// shape (wrap-bypass on ProposeAdmin) is gone; an attempt to
// reintroduce it must visibly fail here.
func TestWrappedProposer_ProposeAdminAppliesWrap(t *testing.T) {
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
	plain := []byte("post-cutover-admin")
	if _, err := wp.ProposeAdmin(context.Background(), plain); err != nil {
		t.Fatalf("ProposeAdmin: %v", err)
	}
	if got := wrapCalls.Load(); got != 1 {
		t.Fatalf("wrap closure ran %d times under ProposeAdmin; want 1 — admin path must apply wrap so post-cutover admin entries survive strict-> unwrap", got)
	}
	if got := inner.adminCalls.Load(); got != 1 {
		t.Fatalf("inner.ProposeAdmin call count = %d, want 1 — admin path must still route through inner.ProposeAdmin so 6E-2d's barrier exemption survives", got)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Fatalf("inner.Propose called %d times under ProposeAdmin; want 0 — admin path must not silently fall back to the non-exempt method", got)
	}
	want := append([]byte{'W'}, plain...)
	if !bytes.Equal(inner.adminLast, want) {
		t.Fatalf("inner.ProposeAdmin saw %q, want %q (wrapper output)", inner.adminLast, want)
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

// TestDynamicWrappedProposer_NilPointerReturnsInnerVerbatim pins the
// defensive degradation in newDynamicWrappedProposer: if a caller
// accidentally passes a nil *atomic.Pointer, the wrapper falls back
// to the inner proposer rather than NPE on the first Propose call.
// This matches newWrappedProposer(inner, nil)'s shape and keeps a
// misconfigured wiring from crashing the engine loop.
func TestDynamicWrappedProposer_NilPointerReturnsInnerVerbatim(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	got := newDynamicWrappedProposer(inner, nil)
	if got != raftengine.Proposer(inner) {
		t.Fatal("nil pointer: newDynamicWrappedProposer should return the inner proposer verbatim, not a wrapper")
	}
}

// TestDynamicWrappedProposer_NilStoredIsPassThrough pins the
// Stage 6E-2c "wrap inactive" default: when the atomic pointer
// holds nil (no wrap closure installed), payloads reach the inner
// proposer verbatim and routing distinguishes Propose vs
// ProposeAdmin correctly.
func TestDynamicWrappedProposer_NilStoredIsPassThrough(t *testing.T) {
	t.Parallel()
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	inner := &fakeProposer{}
	wp := newDynamicWrappedProposer(inner, &wrapPtr)

	if _, err := wp.Propose(context.Background(), []byte("plain")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("inner.Propose calls = %d, want 1", got)
	}
	if !bytes.Equal(inner.last, []byte("plain")) {
		t.Fatalf("inner.Propose saw %q, want %q (verbatim)", inner.last, []byte("plain"))
	}

	if _, err := wp.ProposeAdmin(context.Background(), []byte("admin")); err != nil {
		t.Fatalf("ProposeAdmin: %v", err)
	}
	if got := inner.adminCalls.Load(); got != 1 {
		t.Fatalf("inner.ProposeAdmin calls = %d, want 1", got)
	}
	if !bytes.Equal(inner.adminLast, []byte("admin")) {
		t.Fatalf("inner.ProposeAdmin saw %q, want %q (verbatim)", inner.adminLast, []byte("admin"))
	}
}

// TestDynamicWrappedProposer_LoadsCurrentWrapEveryCall pins the
// Stage 6E-2c hot-swap contract: the atomic pointer is loaded on
// every Propose / ProposeAdmin call, so a publish via Store between
// two proposals is observed by the second without rebuilding the
// proposer.
func TestDynamicWrappedProposer_LoadsCurrentWrapEveryCall(t *testing.T) {
	t.Parallel()
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	inner := &fakeProposer{}
	wp := newDynamicWrappedProposer(inner, &wrapPtr)

	// First call: no wrap installed, payload passes through verbatim.
	if _, err := wp.Propose(context.Background(), []byte("first")); err != nil {
		t.Fatalf("Propose 1: %v", err)
	}
	if !bytes.Equal(inner.last, []byte("first")) {
		t.Fatalf("first call: inner saw %q, want %q", inner.last, "first")
	}

	// Install a wrap mid-flight; the next call must see it.
	var wrap RaftPayloadWrapper = func(p []byte) ([]byte, error) {
		out := make([]byte, len(p)+1)
		out[0] = 'W'
		copy(out[1:], p)
		return out, nil
	}
	wrapPtr.Store(&wrap)

	if _, err := wp.Propose(context.Background(), []byte("second")); err != nil {
		t.Fatalf("Propose 2: %v", err)
	}
	want := append([]byte{'W'}, []byte("second")...)
	if !bytes.Equal(inner.last, want) {
		t.Fatalf("second call: inner saw %q, want %q (wrapped)", inner.last, want)
	}

	// Clear the wrap; the next call must see it gone.
	wrapPtr.Store(nil)

	if _, err := wp.Propose(context.Background(), []byte("third")); err != nil {
		t.Fatalf("Propose 3: %v", err)
	}
	if !bytes.Equal(inner.last, []byte("third")) {
		t.Fatalf("third call (post-clear): inner saw %q, want %q (verbatim)", inner.last, "third")
	}
}

// TestShardGroup_SetRaftPayloadWrap_RoundTrip pins the
// Stage 6E-2c hot-swap surface on ShardGroup. Set with a non-nil
// closure publishes it; Set with nil clears; the round-trip via
// RaftPayloadWrap returns identity.
func TestShardGroup_SetRaftPayloadWrap_RoundTrip(t *testing.T) {
	t.Parallel()
	g := &ShardGroup{}
	if got := g.RaftPayloadWrap(); got != nil {
		t.Fatalf("default: RaftPayloadWrap = %v, want nil", got)
	}
	var called atomic.Int32
	var wrap RaftPayloadWrapper = func(p []byte) ([]byte, error) {
		called.Add(1)
		return p, nil
	}
	g.SetRaftPayloadWrap(wrap)
	got := g.RaftPayloadWrap()
	if got == nil {
		t.Fatal("after Set: RaftPayloadWrap returned nil")
	}
	if _, err := got([]byte("x")); err != nil {
		t.Fatalf("invoking stored wrap: %v", err)
	}
	if got := called.Load(); got != 1 {
		t.Fatalf("stored wrap call count = %d, want 1", got)
	}
	g.SetRaftPayloadWrap(nil)
	if got := g.RaftPayloadWrap(); got != nil {
		t.Fatalf("after Set(nil): RaftPayloadWrap = %v, want nil", got)
	}
}

// TestShardGroup_BarrierForwarders_DegradedFallback pins the
// 6E-2d ShardGroup forwarders' bare-engine fallback contract: when
// g.proposer is nil (struct-literal test fixture), the forwarders
// MUST degrade to immediate-success rather than panic or block —
// otherwise a unit test that constructs ShardGroup directly would
// deadlock the EnableRaftEnvelope handler on its WaitDrained.
// Begin returns a pre-closed channel; WaitDrained returns nil;
// End is a no-op. The semantics match what the EnableRaftEnvelope
// state machine expects from a barrier-capable controller in the
// no-op case.
func TestShardGroup_BarrierForwarders_DegradedFallback(t *testing.T) {
	t.Parallel()
	g := &ShardGroup{}

	ch := g.BeginCutoverBarrier()
	select {
	case <-ch:
		// expected: pre-closed
	default:
		t.Fatal("BeginCutoverBarrier without proposer: channel must be pre-closed for degraded fallback")
	}

	if err := g.WaitInflightDrained(context.Background()); err != nil {
		t.Fatalf("WaitInflightDrained without proposer: want nil, got %v", err)
	}

	// Idempotent / no-panic.
	g.EndCutoverBarrier()
}

// TestShardGroup_BarrierForwarders_DelegatesToProposer pins the
// production-path: when ShardGroup is constructed via
// NewLeaderProxyForShardGroup, the forwarders MUST drive the
// underlying *dynamicWrappedProposer's barrier state so a
// subsequent Propose call observes ErrEnvelopeCutoverInProgress.
// A regression that left the forwarders no-op for production
// ShardGroups would silently disable the §7.1 barrier — exactly
// the bug class that this stage exists to prevent.
func TestShardGroup_BarrierForwarders_DelegatesToProposer(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	g := &ShardGroup{Engine: &recordingEngineForShardGroup{inner: inner}}
	_ = NewLeaderProxyForShardGroup(g)

	// Sanity: pre-barrier Propose succeeds.
	if _, err := g.Proposer().Propose(context.Background(), []byte("pre")); err != nil {
		t.Fatalf("pre-barrier Propose: %v", err)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("pre-barrier inner.Propose calls = %d, want 1", got)
	}

	// Open barrier via ShardGroup forwarder.
	g.BeginCutoverBarrier()
	t.Cleanup(g.EndCutoverBarrier)

	// Post-Begin Propose is rejected at the wrap layer (engine NOT reached).
	_, err := g.Proposer().Propose(context.Background(), []byte("blocked"))
	if !errors.Is(err, raftengine.ErrEnvelopeCutoverInProgress) {
		t.Fatalf("Propose under ShardGroup barrier: want ErrEnvelopeCutoverInProgress, got %v", err)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Errorf("inner.Propose calls = %d after barrier; want still 1 — barrier did not gate", got)
	}
}

// recordingEngineForShardGroup satisfies the raftengine.Engine
// surface that NewLeaderProxyForShardGroup expects, forwarding
// Propose/ProposeAdmin to an embedded fakeProposer so the
// barrier-forwarder test can inspect what the engine saw. The
// nil-embedded shape is sufficient because the test path only
// exercises Propose/ProposeAdmin; everything else NPEs if invoked,
// which is the fail-fast we want for a misuse.
type recordingEngineForShardGroup struct {
	raftengine.Engine
	inner *fakeProposer
}

func (e *recordingEngineForShardGroup) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.inner.Propose(ctx, data)
}

func (e *recordingEngineForShardGroup) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.inner.ProposeAdmin(ctx, data)
}

// TestDynamicWrappedProposer_ProposeAdminAppliesCurrentWrap is the
// admin-path mirror of LoadsCurrentWrapEveryCall: ProposeAdmin
// must also see hot-swapped wrap closures so post-cutover admin
// entries (RotateDEK, RegisterEncryptionWriter) committed at
// `index > raftEnvelopeCutoverIndex` carry the AEAD envelope the
// §6.3 strict-`>` apply hook expects. The cutover marker itself
// bypasses this proposer at the call site (raw engine reference),
// not at this layer.
func TestDynamicWrappedProposer_ProposeAdminAppliesCurrentWrap(t *testing.T) {
	t.Parallel()
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	var wrap RaftPayloadWrapper = func(p []byte) ([]byte, error) {
		out := make([]byte, len(p)+1)
		out[0] = 'A'
		copy(out[1:], p)
		return out, nil
	}
	wrapPtr.Store(&wrap)

	inner := &fakeProposer{}
	wp := newDynamicWrappedProposer(inner, &wrapPtr)

	if _, err := wp.ProposeAdmin(context.Background(), []byte("admin-payload")); err != nil {
		t.Fatalf("ProposeAdmin: %v", err)
	}
	if got := inner.adminCalls.Load(); got != 1 {
		t.Fatalf("inner.ProposeAdmin calls = %d, want 1", got)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Fatalf("inner.Propose calls = %d, want 0 — admin path must NOT downgrade to non-exempt method", got)
	}
	want := append([]byte{'A'}, []byte("admin-payload")...)
	if !bytes.Equal(inner.adminLast, want) {
		t.Fatalf("inner.ProposeAdmin saw %q, want %q (wrapped)", inner.adminLast, want)
	}
}

// TestDynamicWrappedProposer_BarrierBlocksPropose pins the §7.1
// step-1 contract: once BeginCutoverBarrier runs, fresh Propose
// calls fail with raftengine.ErrEnvelopeCutoverInProgress and the
// payload never reaches the inner engine. A regression that lets
// the barrier no-op would silently let a user proposal land at
// index > cutover_index during the cutover window — exactly the
// case the §7.1 barrier exists to prevent.
func TestDynamicWrappedProposer_BarrierBlocksPropose(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	wp, ok := newDynamicWrappedProposer(inner, &wrapPtr).(*dynamicWrappedProposer)
	if !ok {
		t.Fatal("newDynamicWrappedProposer should return *dynamicWrappedProposer when wrapPtr is non-nil")
	}
	wp.BeginCutoverBarrier()
	t.Cleanup(wp.EndCutoverBarrier)

	_, err := wp.Propose(context.Background(), []byte("user-write"))
	if !errors.Is(err, raftengine.ErrEnvelopeCutoverInProgress) {
		t.Fatalf("Propose under barrier: want ErrEnvelopeCutoverInProgress, got %v", err)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Errorf("inner.Propose called %d times under barrier; gate is broken — payload could have reached the engine", got)
	}
}

// TestDynamicWrappedProposer_BarrierAllowsProposeAdmin pins the
// design's barrier-exemption: ProposeAdmin MUST remain admissible
// across the barrier so the EnableRaftEnvelope handler can propose
// the cutover marker through its own barrier (without this exempt,
// the handler would deadlock on its own cutover proposal). A
// regression that gated ProposeAdmin on barrierOpen would brick
// the cutover RPC; this test catches that immediately.
func TestDynamicWrappedProposer_BarrierAllowsProposeAdmin(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	wp, ok := newDynamicWrappedProposer(inner, &wrapPtr).(*dynamicWrappedProposer)
	if !ok {
		t.Fatal("newDynamicWrappedProposer should return *dynamicWrappedProposer when wrapPtr is non-nil")
	}
	wp.BeginCutoverBarrier()
	t.Cleanup(wp.EndCutoverBarrier)

	if _, err := wp.ProposeAdmin(context.Background(), []byte("cutover-marker")); err != nil {
		t.Fatalf("ProposeAdmin under barrier: %v (must be barrier-exempt)", err)
	}
	if got := inner.adminCalls.Load(); got != 1 {
		t.Errorf("inner.ProposeAdmin calls = %d, want 1 under barrier (admin path must remain admissible)", got)
	}
}

// TestDynamicWrappedProposer_BarrierEndReopens pins step-6 of the
// quiescence sequence: after EndCutoverBarrier, fresh Propose
// calls succeed again (the cutover is complete, user writes resume).
// A regression that left barrierOpen=true after End would brick the
// cluster on the post-cutover write path — every coordinator would
// see ErrEnvelopeCutoverInProgress forever.
func TestDynamicWrappedProposer_BarrierEndReopens(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	wp, ok := newDynamicWrappedProposer(inner, &wrapPtr).(*dynamicWrappedProposer)
	if !ok {
		t.Fatal("newDynamicWrappedProposer should return *dynamicWrappedProposer when wrapPtr is non-nil")
	}
	wp.BeginCutoverBarrier()
	wp.EndCutoverBarrier()

	if _, err := wp.Propose(context.Background(), []byte("post-cutover")); err != nil {
		t.Fatalf("Propose after End: %v (user writes must resume)", err)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Errorf("inner.Propose calls = %d, want 1 after End", got)
	}
}

// TestDynamicWrappedProposer_WaitDrainedNoBarrier pins the degraded
// fast-path: WaitInflightDrained called WITHOUT a preceding
// BeginCutoverBarrier returns nil immediately rather than blocking
// forever. A handler that mis-sequences the calls (or a test stub
// that skips Begin) should not deadlock — it should observe an
// immediately-drained state because there is no in-flight gate to
// drain past.
func TestDynamicWrappedProposer_WaitDrainedNoBarrier(t *testing.T) {
	t.Parallel()
	inner := &fakeProposer{}
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	wp, ok := newDynamicWrappedProposer(inner, &wrapPtr).(*dynamicWrappedProposer)
	if !ok {
		t.Fatal("newDynamicWrappedProposer should return *dynamicWrappedProposer when wrapPtr is non-nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := wp.WaitInflightDrained(ctx); err != nil {
		t.Fatalf("WaitInflightDrained without prior Begin must return nil, got %v", err)
	}
}

// TestDynamicWrappedProposer_BarrierDrainsInflight pins the §7.1
// step-2 contract: BeginCutoverBarrier opens the gate but
// WaitInflightDrained must NOT return until the in-flight
// proposals that started before the gate finished. Without this,
// the handler would propose the cutover entry while a previously-
// accepted Propose is still mid-flight inside engine.Propose,
// risking it landing at index > cutover_index after the FSM apply
// of the cutover entry — which the §6.3 hook would then try to
// unwrap as ciphertext and HaltApply the cluster.
//
// Construction: start a Propose that blocks inside the inner
// proposer via a hand-rolled gate, open the barrier, assert
// WaitInflightDrained blocks, then release the inner gate and
// assert WaitInflightDrained returns.
func TestDynamicWrappedProposer_BarrierDrainsInflight(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	entered := make(chan struct{})
	inner := &blockingFakeProposer{enter: entered, release: release}
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	wp, ok := newDynamicWrappedProposer(inner, &wrapPtr).(*dynamicWrappedProposer)
	if !ok {
		t.Fatal("newDynamicWrappedProposer should return *dynamicWrappedProposer when wrapPtr is non-nil")
	}

	// In-flight Propose: blocks inside inner.Propose
	proposeDone := make(chan error, 1)
	go func() {
		_, err := wp.Propose(context.Background(), []byte("inflight-write"))
		proposeDone <- err
	}()
	<-entered // inner.Propose reached

	// Open the barrier while one Propose is in-flight.
	wp.BeginCutoverBarrier()
	t.Cleanup(wp.EndCutoverBarrier)

	// WaitInflightDrained must NOT return — drainSig isn't closed
	// because inflightUser > 0.
	drained := make(chan error, 1)
	go func() {
		drained <- wp.WaitInflightDrained(context.Background())
	}()

	select {
	case err := <-drained:
		t.Fatalf("WaitInflightDrained returned (%v) while inflight Propose is still in flight; barrier drain semantics broken", err)
	case <-makeShortTimer():
		// expected: still blocking
	}

	// Release inner.Propose; in-flight drains; drainSig closes.
	close(release)
	if err := <-proposeDone; err != nil {
		t.Fatalf("inflight Propose: %v", err)
	}

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("WaitInflightDrained after drain: %v", err)
		}
	case <-makeLongTimer():
		t.Fatal("WaitInflightDrained did not return within budget after in-flight finished")
	}
}

// blockingFakeProposer signals on enter when Propose is invoked
// and blocks until release is closed. Used by the
// barrier-drains-inflight test to deterministically hold a
// Propose call past BeginCutoverBarrier.
type blockingFakeProposer struct {
	enter   chan struct{}
	release chan struct{}
}

func (p *blockingFakeProposer) Propose(_ context.Context, _ []byte) (*raftengine.ProposalResult, error) {
	close(p.enter)
	<-p.release
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func (p *blockingFakeProposer) ProposeAdmin(_ context.Context, _ []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

// TestDynamicWrappedProposer_WaitDrainedRespectsCtx pins the
// composability requirement: a context cancellation MUST short-
// circuit WaitInflightDrained so a misbehaving in-flight proposal
// can't deadlock the EnableRaftEnvelope handler past its gRPC
// deadline. Without ctx-respect the handler would block forever,
// holding the cutoverSem and the barrier — denying every retry of
// the same cutover RPC and blocking user writes indefinitely.
func TestDynamicWrappedProposer_WaitDrainedRespectsCtx(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{})
	inner := &blockingFakeProposer{enter: entered, release: release}
	var wrapPtr atomic.Pointer[RaftPayloadWrapper]
	wp, ok := newDynamicWrappedProposer(inner, &wrapPtr).(*dynamicWrappedProposer)
	if !ok {
		t.Fatal("newDynamicWrappedProposer should return *dynamicWrappedProposer when wrapPtr is non-nil")
	}

	go func() {
		_, _ = wp.Propose(context.Background(), []byte("inflight"))
	}()
	<-entered

	wp.BeginCutoverBarrier()
	t.Cleanup(wp.EndCutoverBarrier)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := wp.WaitInflightDrained(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitInflightDrained: want wrapped DeadlineExceeded, got %v", err)
	}
}

// makeShortTimer returns a channel that fires after a few
// milliseconds — long enough for the in-flight goroutine to make
// progress past the barrier check if the drain semantic were
// broken, short enough to keep the test fast.
func makeShortTimer() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		<-time.After(20 * time.Millisecond)
		close(ch)
	}()
	return ch
}

// makeLongTimer returns a channel that fires after a longer
// budget for the post-release drain to complete. Picks a window
// well within the test deadline so a 50ms CI hiccup doesn't flake.
func makeLongTimer() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		<-time.After(2 * time.Second)
		close(ch)
	}()
	return ch
}
