package kv

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// RaftPayloadWrapper transforms an FSM payload into a §4.2 raft
// envelope just before submission to the engine. The Stage 3 default
// (when no wrapper is installed on a coordinator) is identity —
// payloads pass through unchanged. Stage 6's cluster-flag pipeline
// installs an active wrapper, sourced from the sidecar's currently-
// active raft DEK and a writer-registry-backed nonce factory.
//
// Implementations MUST be safe to call concurrently from many
// goroutines: the coordinator may invoke this on every concurrent
// proposal. Encryption-state transitions (Phase 1 → Phase 2 cutover)
// publish a fresh closure via atomic.Pointer so the wrapper
// observes one consistent (cipher, key_id, nonce_factory) tuple per
// call.
type RaftPayloadWrapper func(payload []byte) ([]byte, error)

// applyRaftPayloadWrap is a coordinator-internal helper that runs
// the configured wrapper, or returns the payload verbatim when no
// wrapper is installed. Centralised so every coordinator call site
// gates payload bytes through the same path — a future audit can
// grep for engine.Propose / proposer.Propose and verify each goes
// through this helper or has an explicit "intentionally cleartext"
// reason.
func applyRaftPayloadWrap(wrap RaftPayloadWrapper, payload []byte) ([]byte, error) {
	if wrap == nil {
		return payload, nil
	}
	wrapped, err := wrap(payload)
	if err != nil {
		return nil, errors.Wrap(err, "kv: raft payload wrap")
	}
	return wrapped, nil
}

// wrappedProposer adapts a raftengine.Proposer so every Propose call
// transparently runs the configured RaftPayloadWrapper. Used by
// transaction.go's applyRequests path and by future code that needs
// to share a single Proposer between callers some of whom wrap and
// some of whom do not — the wrapping decision lives with the
// constructed proposer, not the call site.
//
// When wrap is nil the wrappedProposer is functionally identical to
// the inner proposer; this keeps the Stage 3 default a no-op.
type wrappedProposer struct {
	inner raftengine.Proposer
	wrap  RaftPayloadWrapper
}

// newWrappedProposer returns the inner proposer untouched when the
// wrapper is nil, so the cipher-disabled path stays a single
// pointer assignment.
func newWrappedProposer(inner raftengine.Proposer, wrap RaftPayloadWrapper) raftengine.Proposer {
	if wrap == nil {
		return inner
	}
	return &wrappedProposer{inner: inner, wrap: wrap}
}

func (p *wrappedProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	wrapped, err := applyRaftPayloadWrap(p.wrap, data)
	if err != nil {
		return nil, err
	}
	res, err := p.inner.Propose(ctx, wrapped)
	if err != nil {
		return nil, errors.Wrap(err, "kv: wrapped propose")
	}
	return res, nil
}

// ProposeAdmin applies the configured RaftPayloadWrapper before
// forwarding the payload to the inner ProposeAdmin path. The wrap
// layer is NOT a barrier-exemption concern: it exists so that
// every entry landing at `index > raftEnvelopeCutoverIndex` carries
// an AEAD envelope the §6.3 strict-`>` apply hook can unwrap.
// Admin entries (BootstrapEncryption, RotateDEK,
// RegisterEncryptionWriter, etc.) committed after the cutover are
// no different from user data in that regard — a cleartext admin
// entry above cutover would halt the apply loop on unwrap-failure,
// not be silently passed through.
//
// The lone exception is the EnableRaftEnvelope cutover marker
// itself, which sits exactly at `index == cutover` and must remain
// cleartext for strict-`>` dispatch to leave it alone. That marker
// is proposed via a raw engine reference (adapter/encryption_admin.go
// holds the engine directly as s.proposer), not via the
// wrappedProposer — so this wrap path is never on its way.
//
// ProposeAdmin's only divergence from Propose lives in Stage
// 6E-2d's §7.1 quiescence-barrier check, which is installed on
// Propose only.
func (p *wrappedProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	wrapped, err := applyRaftPayloadWrap(p.wrap, data)
	if err != nil {
		return nil, err
	}
	res, err := p.inner.ProposeAdmin(ctx, wrapped)
	if err != nil {
		return nil, errors.Wrap(err, "kv: wrapped propose-admin")
	}
	return res, nil
}

// dynamicWrappedProposer is the Stage 6E-2c sibling of wrappedProposer
// for the case where the wrap closure must be swappable at runtime.
// On every Propose / ProposeAdmin call it loads an atomic.Pointer to
// the currently-active RaftPayloadWrapper; a nil pointer means wrap
// is inactive (payload passes through verbatim).
//
// Why a separate type (vs adding a setter to wrappedProposer): the
// existing wrappedProposer captures wrap at construction time and is
// used by static call sites that never need to change wrap state.
// The Stage 6E-2 cutover model needs runtime swap so the
// EnableRaftEnvelope admin handler (Stage 6E-2d) can publish the
// active wrap closure the instant the cutover entry commits, without
// rebuilding the TransactionManager or stalling in-flight proposals.
// Splitting the two keeps the static-wrap fast path branch-free and
// keeps the dynamic path's atomic.Pointer.Load() cost confined to
// the call sites that need it.
//
// The pointer is required to be non-nil; pass an atomic.Pointer that
// stores nil to express "wrap inactive". This is so the call site
// (typically a ShardGroup) owns the storage and the proposer just
// reads.
//
// §7.1 quiescence-barrier state (Stage 6E-2d) lives here too — see
// the BeginCutoverBarrier / WaitInflightDrained / EndCutoverBarrier
// trio below for the 6-step state machine's mechanics. The barrier
// gates only the user-Propose path; ProposeAdmin is exempt by
// interface contract so the EnableRaftEnvelope handler can propose
// the cutover marker itself across its own barrier.
type dynamicWrappedProposer struct {
	inner   raftengine.Proposer
	wrapPtr *atomic.Pointer[RaftPayloadWrapper]

	// barrierMu guards barrierOpen, inflightUser, and drainSig. Held
	// only briefly on Propose entry/exit (the engine.Propose call
	// itself runs without the mutex), so contention is bounded by
	// the per-Propose acquire/release pair. The 6E-2d handler holds
	// it longer on the BeginCutoverBarrier / EndCutoverBarrier
	// transitions, but those are once per cutover (~ms-scale).
	//
	// Why a mutex rather than three atomics: the barrier-open check
	// and the in-flight counter increment MUST be observed as one
	// atomic transition, or a Propose that read barrierOpen=false
	// just before the cutover handler stores true could increment
	// the counter AFTER BeginCutoverBarrier observed it as 0,
	// leaving WaitInflightDrained returning success while a fresh
	// user proposal slips through to engine.Propose. The mutex is
	// the simplest way to make Propose's (read-barrier, inc-counter)
	// pair atomic with the handler's (open-barrier, sample-counter)
	// pair on the other side.
	barrierMu    sync.Mutex
	barrierOpen  bool
	inflightUser int64
	// drainSig is freshly allocated each BeginCutoverBarrier call
	// and closed when inflightUser drops to 0 after the barrier has
	// opened (either at BeginCutoverBarrier time if no Propose is
	// in flight, or at the last proposeExit that hits 0). nil
	// outside a barrier cycle so WaitInflightDrained called without
	// an active barrier degrades gracefully to immediate success.
	drainSig chan struct{}
}

// newDynamicWrappedProposer wires a proposer that consults wrapPtr
// on every Propose / ProposeAdmin. wrapPtr.Load() == nil keeps the
// path as a pure pass-through to inner; a non-nil value applies
// wrap before forwarding.
//
// Contract: inner MUST be non-nil. A nil inner is a caller bug
// (the construction site is responsible for handing in a valid
// proposer) and the constructor deliberately does not silently
// degrade — passing nil to Propose / ProposeAdmin would NPE at
// the first call site, which is the same fail-fast shape as the
// sibling newWrappedProposer(nil, wrap) and matches CLAUDE.md's
// "don't validate for scenarios that can't happen" policy at
// internal boundaries.
//
// wrapPtr MAY be nil — that path returns inner verbatim, mirroring
// newWrappedProposer(inner, nil)'s degraded shape so a wiring site
// that accidentally drops the pointer downgrades to a no-wrap
// proposer rather than crashing the engine loop on first use. The
// caller owns the storage lifetime of the pointer when non-nil.
func newDynamicWrappedProposer(inner raftengine.Proposer, wrapPtr *atomic.Pointer[RaftPayloadWrapper]) raftengine.Proposer {
	if wrapPtr == nil {
		// Defensive degradation: passing a nil pointer would NPE
		// on the first Propose. Treat as static no-wrap so callers
		// that pass nil by mistake see the Stage 3 default rather
		// than crashing the engine loop. This matches
		// newWrappedProposer(inner, nil) returning inner verbatim.
		return inner
	}
	return &dynamicWrappedProposer{inner: inner, wrapPtr: wrapPtr}
}

func (p *dynamicWrappedProposer) currentWrap() RaftPayloadWrapper {
	if loaded := p.wrapPtr.Load(); loaded != nil {
		return *loaded
	}
	return nil
}

// Propose runs the §7.1 quiescence-barrier gate, then forwards the
// (optionally wrapped) payload to the inner Propose. The gate
// rejects with ErrEnvelopeCutoverInProgress while the barrier is
// open — the 6E-2d EnableRaftEnvelope handler holds the barrier
// open for the few-ms it takes to commit the cutover entry and
// publish the active wrap closure. Callers should treat the error
// as a transient back-off (retry on a new leader-issued ts).
//
// The barrierMu + inflight-counter pair makes (read-barrier,
// inc-counter) atomic with the handler's (open-barrier,
// sample-counter); see the type comment for the race that motivates
// it.
func (p *dynamicWrappedProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if err := p.beginUserPropose(); err != nil {
		return nil, err
	}
	defer p.endUserPropose()

	wrapped, err := applyRaftPayloadWrap(p.currentWrap(), data)
	if err != nil {
		return nil, err
	}
	res, err := p.inner.Propose(ctx, wrapped)
	if err != nil {
		return nil, errors.Wrap(err, "kv: dynamic wrapped propose")
	}
	return res, nil
}

// beginUserPropose increments the in-flight counter under
// barrierMu, returning ErrEnvelopeCutoverInProgress if the barrier
// is open. Returning the error without incrementing is intentional:
// a rejected Propose must not count toward in-flight (the handler
// would otherwise wait on a fictional in-flight that will never
// drain). Pair with endUserPropose.
func (p *dynamicWrappedProposer) beginUserPropose() error {
	p.barrierMu.Lock()
	defer p.barrierMu.Unlock()
	if p.barrierOpen {
		return raftengine.ErrEnvelopeCutoverInProgress
	}
	p.inflightUser++
	return nil
}

// endUserPropose pairs with beginUserPropose: decrement the
// in-flight counter under barrierMu and, if the barrier is open
// and we just dropped to 0, close drainSig so the handler's
// WaitInflightDrained unblocks. Idempotent close via the
// select/default pattern so concurrent EndCutoverBarrier doesn't
// race a late proposeExit on a freshly nil-ed drainSig (the nil
// check covers EndCutoverBarrier having already torn down the
// channel; the closed-recv guard covers the BeginCutoverBarrier
// path that closed an empty-inflight channel at open time).
func (p *dynamicWrappedProposer) endUserPropose() {
	p.barrierMu.Lock()
	defer p.barrierMu.Unlock()
	p.inflightUser--
	if p.barrierOpen && p.inflightUser == 0 && p.drainSig != nil {
		select {
		case <-p.drainSig:
			// already closed at BeginCutoverBarrier-with-no-inflight
			// or by a sibling exit; treat as success.
		default:
			close(p.drainSig)
		}
	}
}

// BeginCutoverBarrier opens the §7.1 step-1 quiescence barrier.
// After return, every fresh dynamicWrappedProposer.Propose call
// fails with ErrEnvelopeCutoverInProgress until EndCutoverBarrier
// runs. ProposeAdmin is unaffected (the cutover marker proposes
// through it).
//
// HAZARDS — per-leader scope of the barrier (codex P1 round-2 and
// round-3 on PR933): the barrier is an in-memory data structure
// owned by THIS leader's dynamicWrappedProposer. It does not
// coordinate across the cluster, and it does not survive leadership
// transfer. Two related future-state failure modes follow from
// that scope and MUST be closed by 6E-2e before 6E-2f flips the
// gate; 6E-2d ships them inert by leaving raftEnvelopeWrapEnabled
// false so production never opens the cutover window.
//
//	(a) Wrap-gap admin RPCs (codex P1 #1 round-2):
//	    Other admin RPCs that route through ProposeAdmin (RotateDEK,
//	    RegisterEncryptionWriter) are barrier-exempt and currently
//	    reach the engine via the raw-engine s.proposer in
//	    adapter/encryption_admin.go. Between the cutover marker's
//	    commit and the handler's InstallWrap call, an admin RPC
//	    that lands at `index > raftEnvelopeCutoverIndex` would be
//	    cleartext, and the §6.3 strict-`>` apply hook on every
//	    follower would treat it as a wrapped envelope and halt
//	    apply cluster-wide.
//
//	    Remediation options for 6E-2e:
//	      Option A (preferred): route RotateDEK /
//	                            RegisterEncryptionWriter through the
//	                            wrap-aware proposer so post-cutover
//	                            admin entries are wrapped. The
//	                            cutover marker itself remains on a
//	                            separate raw-engine reference held
//	                            by the EnableRaftEnvelope handler.
//	      Option B: extend cutoverSem to serialize RotateDEK and
//	                RegisterEncryptionWriter against the
//	                EnableRaftEnvelope handler so no admin RPC can
//	                race the barrier window.
//	    See main_encryption_registration.go's call-site comment for
//	    the 7c §3.1 wiring that Option A would extend.
//
//	(b) Leader failover mid-cutover (codex P1 round-3):
//	    If leadership transfers from L1 to L2 between the cutover
//	    marker's commit and L1's InstallWrap call, L2 has its own
//	    barrierOpen=false and a nil wrap pointer. L2 admits a fresh
//	    user proposal through Propose without wrapping; it lands at
//	    `index > raftEnvelopeCutoverIndex` in cleartext, and once
//	    L2 (or any follower) applies the cutover marker the §6.3
//	    strict-`>` hook treats every subsequent cleartext proposal
//	    as a wrapped envelope and halts.
//
//	    Remediation options for 6E-2e:
//	      Option A (preferred): auto-install the wrap on every
//	                            replica's FSM-apply of the cutover
//	                            marker so L2's
//	                            dynamicWrappedProposer publishes the
//	                            same wrap closure independently of
//	                            leadership state. The handler's
//	                            InstallWrap call then becomes a
//	                            redundant convenience (matches the
//	                            state every follower will reach via
//	                            the apply path).
//	      Option B: make dynamicWrappedProposer.Propose consult the
//	                sidecar's RaftEnvelopeCutoverIndex on every call
//	                and refuse when the wrap pointer is nil but the
//	                sidecar already reflects a cutover. Trades a
//	                sidecar load per propose for a closed gap.
//
// Both hazards (a) and (b) share a single shape: post-cutover
// cleartext entries land in Raft at indexes that the §6.3 apply
// hook treats as wrapped. The gate (`raftEnvelopeWrapEnabled =
// false`) is the only thing keeping either from triggering today.
//
// Idempotent against double-Begin: a second call freshens drainSig
// and leaves barrierOpen true. CALLER SAFETY: a goroutine that was
// blocked on a prior cycle's drainSig from WaitInflightDrained is
// orphaned by the freshen (it never observes the close because the
// channel reference was discarded). This is safe in practice
// because the EncryptionAdminServer serializes EnableRaftEnvelope
// calls via cutoverSem, so only one handler goroutine ever drives
// the BeginCutoverBarrier → WaitInflightDrained → EndCutoverBarrier
// sequence at a time. A future caller that drives the barrier
// outside that semaphore MUST not rely on Begin's idempotency to
// rescue an orphaned WaitInflightDrained — explicit End/Begin
// ordering on a single goroutine is the only correct usage.
//
// Returns the channel that closes when in-flight drains to 0 so
// callers MAY block on it directly; the recommended pattern is to
// use WaitInflightDrained which composes context cancellation.
func (p *dynamicWrappedProposer) BeginCutoverBarrier() <-chan struct{} {
	p.barrierMu.Lock()
	defer p.barrierMu.Unlock()
	p.drainSig = make(chan struct{})
	p.barrierOpen = true
	if p.inflightUser == 0 {
		// Fast path: no in-flight, drain is already complete.
		close(p.drainSig)
	}
	return p.drainSig
}

// WaitInflightDrained blocks until the in-flight Propose counter
// drops to 0 after BeginCutoverBarrier was called, or ctx fires.
// Returns nil on drain, an error wrapping ctx.Err() with a
// domain-only prefix on cancellation (the ShardGroup forwarder
// adds the package prefix so operator logs don't carry a
// redundant "kv: ... kv: ..." chain, claude r2 finding B), and
// nil also if no barrier is currently open (degraded fast-path so
// out-of-sequence calls don't deadlock callers).
func (p *dynamicWrappedProposer) WaitInflightDrained(ctx context.Context) error {
	p.barrierMu.Lock()
	ch := p.drainSig
	p.barrierMu.Unlock()
	if ch == nil {
		// No active barrier — drain is trivially complete.
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "drain await canceled")
	}
}

// EndCutoverBarrier closes the §7.1 step-6 barrier. After return,
// fresh Propose calls succeed again. Idempotent against a
// no-barrier-active state. Callers MUST call this once for each
// BeginCutoverBarrier (the EnableRaftEnvelope handler uses defer).
func (p *dynamicWrappedProposer) EndCutoverBarrier() {
	p.barrierMu.Lock()
	defer p.barrierMu.Unlock()
	p.barrierOpen = false
	// Drop the drainSig reference so a stale WaitInflightDrained
	// caller that reads the channel after EndCutoverBarrier sees
	// nil (immediate-success degraded path) rather than blocking
	// on a closed channel from a previous cycle. Whether the
	// channel was closed or not at this point depends on whether
	// in-flight drained; both shapes are acceptable transient
	// states because no new BeginCutoverBarrier has run yet to
	// allocate a fresh channel.
	p.drainSig = nil
}

// ProposeAdmin mirrors Propose's wrap-applies semantics. See
// wrappedProposer.ProposeAdmin for the design rationale (the wrap
// layer is NOT a barrier exemption; the EnableRaftEnvelope cutover
// marker bypasses wrap at the call site, not the method level).
func (p *dynamicWrappedProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	wrapped, err := applyRaftPayloadWrap(p.currentWrap(), data)
	if err != nil {
		return nil, err
	}
	res, err := p.inner.ProposeAdmin(ctx, wrapped)
	if err != nil {
		return nil, errors.Wrap(err, "kv: dynamic wrapped propose-admin")
	}
	return res, nil
}
