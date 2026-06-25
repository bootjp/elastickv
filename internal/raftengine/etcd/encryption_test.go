package etcd

import (
	"crypto/rand"
	"io"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// fakeStateMachine records every Apply call so the encryption tests
// can assert (a) what bytes the FSM saw, and (b) whether Apply was
// even reached on the unwrap-failure path.
type fakeStateMachine struct {
	calls atomic.Int32
	last  []byte
}

func (f *fakeStateMachine) Apply(data []byte) any {
	f.calls.Add(1)
	cp := make([]byte, len(data))
	copy(cp, data)
	f.last = cp
	return nil
}

func (f *fakeStateMachine) Snapshot() (Snapshot, error) { return nil, nil }
func (f *fakeStateMachine) Restore(_ io.Reader) error   { return nil }

// raftCipherFixture wires a Cipher with a single DEK at testKeyID.
// The Stage-3 unit tests don't need to model purpose enforcement;
// the Cipher itself does not check purpose, so a single keystore
// with one key is enough to exercise the apply hook's gate logic.
func raftCipherFixture(t *testing.T) (*encryption.Cipher, uint32) {
	t.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read DEK: %v", err)
	}
	const kid uint32 = 0xDEADBEEF
	if err := ks.Set(kid, dek); err != nil {
		t.Fatalf("Set DEK: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	return c, kid
}

func newRaftNonce(t *testing.T) []byte {
	t.Helper()
	n := make([]byte, encryption.NonceSize)
	if _, err := rand.Read(n); err != nil {
		t.Fatalf("rand.Read nonce: %v", err)
	}
	return n
}

func newTestEngine(fsm StateMachine, cipher *encryption.Cipher, cutover RaftCutoverIndex) *Engine {
	return &Engine{
		fsm:              fsm,
		raftCipher:       cipher,
		raftCutoverIndex: orInertCutover(cutover),
	}
}

func envelopeEntry(t *testing.T, c *encryption.Cipher, kid uint32, index uint64, plaintext []byte) raftpb.Entry {
	t.Helper()
	wrapped, err := encryption.WrapRaftPayload(c, kid, newRaftNonce(t), plaintext)
	if err != nil {
		t.Fatalf("WrapRaftPayload: %v", err)
	}
	return raftpb.Entry{Index: index, Type: raftpb.EntryNormal, Data: encodeProposalEnvelope(7, wrapped)}
}

// TestErrEnvelopeCutoverInProgress_DistinctFromUnwrapFailure pins
// the §7.1 barrier's typed error as a separate sentinel from
// ErrRaftUnwrapFailed. The two have very different operator
// responses: cutover-in-progress is a retryable transient (the
// barrier completes in drain-time, then the next attempt
// succeeds under the new wrapped regime), while unwrap-failed is
// process-fatal (sidecar / Raft-log divergence or KEK custody
// problem). A caller that confuses them would either drop
// retryable cutover-window proposals on the floor or paper over
// a fatal data-integrity error with a retry loop. errors.Is
// distinguishes them; this test pins that distinction so future
// refactors of the error chain do not silently merge the two.
func TestErrEnvelopeCutoverInProgress_DistinctFromUnwrapFailure(t *testing.T) {
	t.Parallel()
	if errors.Is(ErrEnvelopeCutoverInProgress, ErrRaftUnwrapFailed) {
		t.Error("ErrEnvelopeCutoverInProgress matches ErrRaftUnwrapFailed; the §7.1 barrier sentinel must be a distinct error chain")
	}
	if errors.Is(ErrRaftUnwrapFailed, ErrEnvelopeCutoverInProgress) {
		t.Error("ErrRaftUnwrapFailed matches ErrEnvelopeCutoverInProgress; the process-fatal sentinel must not be misclassified as retryable")
	}
	// Pin the chain contract that Stage 6E-2b callers depend
	// on: an errors.Wrap-wrapped sentinel must still match via
	// errors.Is. A downstream caller that wraps the sentinel
	// for diagnostic context would otherwise silently lose its
	// retryable classification and fall through to a
	// process-fatal error path.
	wrapped := errors.Wrap(ErrEnvelopeCutoverInProgress, "cutover barrier open")
	if !errors.Is(wrapped, ErrEnvelopeCutoverInProgress) {
		t.Error("ErrEnvelopeCutoverInProgress lost through errors.Wrap; callers using errors.Is on a wrapped error must still match the sentinel")
	}
}

// TestApplyNormalEntry_CutoverActive_NoCipher_FailsClosed locks down
// the misconfig case: when the cluster has crossed the raft envelope
// cutover (an above-cutover entry arrives) but raftCipher is nil —
// a sidecar/init race or operator wiring mistake — the engine MUST
// refuse to apply, NOT silently hand wrapped envelope bytes to
// fsm.Apply. The latter would permanently diverge this node from
// peers that DID unwrap and apply the cleartext.
func TestApplyNormalEntry_CutoverActive_NoCipher_FailsClosed(t *testing.T) {
	t.Parallel()
	const cutover uint64 = 100
	fsm := &fakeStateMachine{}
	// cipher == nil simulates the misconfig: cutover is set, no
	// cipher wired.
	e := newTestEngine(fsm, nil, func() uint64 { return cutover })

	// Any payload above the cutover index hits the fail-closed branch
	// regardless of whether it's a real envelope or cleartext, because
	// without a cipher we cannot tell them apart.
	entry := raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: cutover + 1,
		Data:  encodeProposalEnvelope(99, []byte("would-be wrapped payload")),
	}
	_, err := e.applyNormalEntry(entry, false)
	if !errors.Is(err, ErrRaftUnwrapFailed) {
		t.Fatalf("expected ErrRaftUnwrapFailed for cutover-active+no-cipher misconfig, got %v", err)
	}
	if got := fsm.calls.Load(); got != 0 {
		t.Fatalf("fsm.Apply called %d times despite refused apply", got)
	}

	// Below-cutover entries still pass through unchanged in this
	// configuration — the misconfig detection fires only when an
	// above-cutover entry actually arrives. (Pre-cutover entries
	// were written before encryption was activated and remain
	// legitimately cleartext.)
	belowCutoverEntry := raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: cutover,
		Data:  encodeProposalEnvelope(11, []byte("legacy cleartext")),
	}
	if _, err := e.applyNormalEntry(belowCutoverEntry, false); err != nil {
		t.Fatalf("below-cutover should pass through, got %v", err)
	}
	if got := fsm.calls.Load(); got != 1 {
		t.Fatalf("fsm.Apply call count after below-cutover = %d, want 1", got)
	}
}

// TestApplyNormalEntry_NoCipher_PassThrough confirms Stage-3 wiring
// preserves Stage-0/2 behaviour: with raftCipher == nil AND no
// cutover (the OpenConfig defaults), the apply hook is inert and the
// FSM sees the proposal envelope's inner payload byte-for-byte.
func TestApplyNormalEntry_NoCipher_PassThrough(t *testing.T) {
	t.Parallel()
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, nil, nil)
	plain := []byte("op=put key=k1 v=hello")
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Data: encodeProposalEnvelope(42, plain)}
	if _, err := e.applyNormalEntry(entry, false); err != nil {
		t.Fatalf("applyNormalEntry: %v", err)
	}
	if got := fsm.calls.Load(); got != 1 {
		t.Fatalf("fsm.Apply call count = %d, want 1", got)
	}
	if string(fsm.last) != string(plain) {
		t.Fatalf("FSM saw %q, want %q", fsm.last, plain)
	}
}

// TestApplyNormalEntry_BelowCutover_PassThrough confirms entries
// whose index is at or below the cutover are NOT unwrapped — they
// carry cleartext payloads (the legacy / pre-Phase-2 regime).
// Strict greater-than: index == cutover is the enable-flag entry
// itself and stays cleartext.
func TestApplyNormalEntry_BelowCutover_PassThrough(t *testing.T) {
	t.Parallel()
	c, _ := raftCipherFixture(t)
	cutover := uint64(100)
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })

	cleartextPayload := []byte("legacy cleartext")
	for _, idx := range []uint64{1, 50, cutover - 1, cutover} {
		fsm.calls.Store(0)
		entry := raftpb.Entry{
			Type:  raftpb.EntryNormal,
			Index: idx,
			Data:  encodeProposalEnvelope(11, cleartextPayload),
		}
		if _, err := e.applyNormalEntry(entry, false); err != nil {
			t.Fatalf("idx=%d: applyNormalEntry: %v", idx, err)
		}
		if got := fsm.calls.Load(); got != 1 {
			t.Fatalf("idx=%d: fsm.Apply call count = %d, want 1", idx, got)
		}
		if string(fsm.last) != string(cleartextPayload) {
			t.Fatalf("idx=%d: FSM saw %q, want %q", idx, fsm.last, cleartextPayload)
		}
	}
}

// TestApplyNormalEntry_AboveCutover_Unwraps confirms entries whose
// index is strictly greater than the cutover go through the §4.2
// raft envelope Unwrap and the FSM observes the cleartext payload.
func TestApplyNormalEntry_AboveCutover_Unwraps(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	cutover := uint64(100)
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })

	for _, idx := range []uint64{cutover + 1, cutover + 100, cutover + 1_000_000} {
		fsm.calls.Store(0)
		plaintext := []byte("op=put key=k1 v=secret")
		entry := envelopeEntry(t, c, kid, idx, plaintext)
		if _, err := e.applyNormalEntry(entry, false); err != nil {
			t.Fatalf("idx=%d: applyNormalEntry: %v", idx, err)
		}
		if got := fsm.calls.Load(); got != 1 {
			t.Fatalf("idx=%d: fsm.Apply call count = %d, want 1", idx, got)
		}
		if string(fsm.last) != string(plaintext) {
			t.Fatalf("idx=%d: FSM saw %q, want %q", idx, fsm.last, plaintext)
		}
	}
}

// TestApplyNormalEntry_UnwrapFailure_Halts is the integrity test:
// when an above-cutover entry's envelope fails GCM verification
// (DEK retired, tampered bytes, missing key), applyNormalEntry
// returns ErrRaftUnwrapFailed and the FSM is NOT called. The
// caller (applyCommitted) is responsible for not advancing
// setApplied; that's covered by TestApplyCommitted_UnwrapFailure_DoesNotAdvanceApplied.
func TestApplyNormalEntry_UnwrapFailure_Halts(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	cutover := uint64(100)
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })

	entry := envelopeEntry(t, c, kid, cutover+1, []byte("payload"))
	// Tamper the GCM tag (last byte of the wrapped envelope, before
	// the proposal-envelope wrapper). encodeProposalEnvelope uses
	// `[0x01][8B id][envelope...]` so the last byte is the tag's
	// last byte.
	entry.Data[len(entry.Data)-1] ^= 0xff

	_, err := e.applyNormalEntry(entry, false)
	if !errors.Is(err, ErrRaftUnwrapFailed) {
		t.Fatalf("expected ErrRaftUnwrapFailed, got %v", err)
	}
	if got := fsm.calls.Load(); got != 0 {
		t.Fatalf("fsm.Apply was called %d times despite unwrap failure", got)
	}
}

// TestApplyCommitted_UnwrapFailure_DoesNotAdvanceApplied locks down
// the design §6.3 invariant: an unwrap failure halts the apply
// loop WITHOUT advancing setApplied. The next restart must replay
// the same entry, not skip it. A regression here would let a
// divergent FSM survive across restarts — exactly the safety
// property the integrity tag was added to detect.
func TestApplyCommitted_UnwrapFailure_DoesNotAdvanceApplied(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	cutover := uint64(100)
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })
	const startApplied uint64 = 99
	e.applied = startApplied
	e.appliedIndex.Store(startApplied)

	good := envelopeEntry(t, c, kid, cutover+1, []byte("ok"))
	bad := envelopeEntry(t, c, kid, cutover+2, []byte("tampered"))
	bad.Data[len(bad.Data)-1] ^= 0xff
	// A third entry that we expect NOT to be processed (it sits
	// after the failing one, so applyCommitted must stop at bad).
	never := envelopeEntry(t, c, kid, cutover+3, []byte("never"))

	err := e.applyCommitted([]raftpb.Entry{good, bad, never})
	if !errors.Is(err, ErrRaftUnwrapFailed) {
		t.Fatalf("applyCommitted: expected ErrRaftUnwrapFailed, got %v", err)
	}
	// good was applied, so applied advanced to good.Index.
	if e.applied != cutover+1 {
		t.Fatalf("applied = %d, want %d (good entry advanced, bad did not)", e.applied, cutover+1)
	}
	if got := e.appliedIndex.Load(); got != cutover+1 {
		t.Fatalf("appliedIndex = %d, want %d", got, cutover+1)
	}
	// good was applied (1 call), bad halted, never untouched.
	if got := fsm.calls.Load(); got != 1 {
		t.Fatalf("fsm.Apply count = %d, want 1 (only good entry)", got)
	}
}

// TestApplyNormalEntry_BoundaryCutover exercises the strict
// greater-than gate at exactly index = cutover and cutover + 1.
// The cutover index is itself the §7.1 enable-raft-envelope flag
// entry and is NOT raft-DEK-wrapped; only entries strictly greater
// must Unwrap.
func TestApplyNormalEntry_BoundaryCutover(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	const cutover uint64 = 100
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })

	// cutover itself: cleartext payload — must NOT be unwrapped.
	cleartext := []byte("enable-raft-envelope flag")
	atCutover := raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: cutover,
		Data:  encodeProposalEnvelope(13, cleartext),
	}
	if _, err := e.applyNormalEntry(atCutover, false); err != nil {
		t.Fatalf("at-cutover: %v", err)
	}
	if string(fsm.last) != string(cleartext) {
		t.Fatalf("at-cutover: FSM saw %q, want %q", fsm.last, cleartext)
	}

	// cutover+1: wrapped payload — MUST be unwrapped.
	above := envelopeEntry(t, c, kid, cutover+1, []byte("first encrypted"))
	if _, err := e.applyNormalEntry(above, false); err != nil {
		t.Fatalf("above-cutover: %v", err)
	}
	if string(fsm.last) != "first encrypted" {
		t.Fatalf("above-cutover: FSM saw %q, want %q", fsm.last, "first encrypted")
	}
}

// TestApplyNormalEntry_ProposalIDStillResolvable confirms the
// design's load-bearing invariant from §6.3: the engine pre-apply
// hook unwraps the *inner* fsm payload, NOT entry.Data itself, so
// decodeProposalEnvelope(entry.Data) continues to recover the
// proposal id even after unwrap. An earlier draft of the design
// proposed unwrapping entry.Data directly, which would have
// destroyed the leading 0x01 proposalEnvelopeVersion byte and
// timed out every coordinator write.
func TestApplyNormalEntry_ProposalIDStillResolvable(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	const cutover uint64 = 100
	fsm := &fakeStateMachine{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })

	const wantID uint64 = 31337
	wrapped, err := encryption.WrapRaftPayload(c, kid, newRaftNonce(t), []byte("payload"))
	if err != nil {
		t.Fatalf("WrapRaftPayload: %v", err)
	}
	data := encodeProposalEnvelope(wantID, wrapped)
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: cutover + 1, Data: data}
	if _, err := e.applyNormalEntry(entry, false); err != nil {
		t.Fatalf("applyNormalEntry: %v", err)
	}
	gotID, _, ok := decodeProposalEnvelope(entry.Data)
	if !ok {
		t.Fatal("decodeProposalEnvelope(entry.Data) failed after applyNormalEntry — entry.Data was mutated")
	}
	if gotID != wantID {
		t.Fatalf("proposal id = %d, want %d", gotID, wantID)
	}
}

// TestApplyNormalEntry_NoCutoverDefault confirms an OpenConfig
// without a cutover callback installs the inert default
// (^uint64(0)). With raftCipher set but cutover = MaxUint64, no
// entry index is greater than the cutover, so the unwrap path
// stays inert.
func TestApplyNormalEntry_NoCutoverDefault(t *testing.T) {
	t.Parallel()
	c, _ := raftCipherFixture(t)
	fsm := &fakeStateMachine{}
	// nil cutover → Open's orInertCutover(nil) → inertRaftCutoverIndex
	e := newTestEngine(fsm, c, nil)

	cleartext := []byte("legacy cleartext")
	for _, idx := range []uint64{1, 1 << 20, 1 << 40, ^uint64(0) - 1} {
		entry := raftpb.Entry{
			Type:  raftpb.EntryNormal,
			Index: idx,
			Data:  encodeProposalEnvelope(7, cleartext),
		}
		if _, err := e.applyNormalEntry(entry, false); err != nil {
			t.Fatalf("idx=%d: %v", idx, err)
		}
		if string(fsm.last) != string(cleartext) {
			t.Fatalf("idx=%d: FSM saw %q, want %q", idx, fsm.last, cleartext)
		}
	}
}

// haltStateMachine is a fakeStateMachine that returns a response
// implementing the HaltApply interface — used by
// TestApplyCommitted_HaltApply_DoesNotAdvanceApplied to confirm the
// engine's seam for §6.3 encryption-apply fatal halts works
// independently from the raft envelope unwrap hook.
type haltStateMachine struct {
	calls atomic.Int32
	err   error
}

type haltResponse struct{ err error }

func (h *haltResponse) HaltApply() error { return h.err }

func (s *haltStateMachine) Apply([]byte) any {
	s.calls.Add(1)
	if s.err != nil {
		return &haltResponse{err: s.err}
	}
	return nil
}
func (s *haltStateMachine) Snapshot() (Snapshot, error) { return nil, nil }
func (s *haltStateMachine) Restore(_ io.Reader) error   { return nil }

// TestApplyCommitted_HaltApply_DoesNotAdvanceApplied locks down the
// HaltApply seam: when the FSM returns a value implementing
// `HaltApply() error` and that method returns non-nil, applyCommitted
// surfaces the error and does NOT advance setApplied. Mirrors
// TestApplyCommitted_UnwrapFailure_DoesNotAdvanceApplied, but for
// the encryption-FSM-apply fatal path that arrives via the FSM's
// return value rather than the engine's pre-apply hook.
func TestApplyCommitted_HaltApply_DoesNotAdvanceApplied(t *testing.T) {
	t.Parallel()
	fsm := &haltStateMachine{err: ErrEncryptionApply}
	e := newTestEngine(fsm, nil, nil)
	const startApplied uint64 = 99
	e.applied = startApplied
	e.appliedIndex.Store(startApplied)

	good := raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: 100,
		Data:  encodeProposalEnvelope(7, []byte("payload")),
	}
	err := e.applyCommitted([]raftpb.Entry{good})
	if !errors.Is(err, ErrEncryptionApply) {
		t.Fatalf("expected ErrEncryptionApply, got %v", err)
	}
	// fsm.Apply was reached (1 call), but setApplied did NOT
	// advance because HaltApply().err was non-nil.
	if got := fsm.calls.Load(); got != 1 {
		t.Fatalf("fsm.Apply call count = %d, want 1", got)
	}
	if e.applied != startApplied {
		t.Fatalf("applied advanced to %d despite halt; want %d", e.applied, startApplied)
	}
	if got := e.appliedIndex.Load(); got != startApplied {
		t.Fatalf("appliedIndex advanced to %d despite halt; want %d", got, startApplied)
	}
}

// TestApplyCommitted_HaltApply_NilContinues confirms that a HaltApply
// implementer whose `HaltApply()` returns nil does NOT halt — the
// apply loop advances normally. This is the no-op case where the
// encryption FSM ran a registration/bootstrap/rotation successfully.
func TestApplyCommitted_HaltApply_NilContinues(t *testing.T) {
	t.Parallel()
	fsm := &haltStateMachine{err: nil}
	e := newTestEngine(fsm, nil, nil)
	good := raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: 100,
		Data:  encodeProposalEnvelope(7, []byte("payload")),
	}
	if err := e.applyCommitted([]raftpb.Entry{good}); err != nil {
		t.Fatalf("applyCommitted: %v", err)
	}
	if e.applied != 100 {
		t.Fatalf("applied = %d, want 100 (nil HaltApply must advance)", e.applied)
	}
}

// TestUnwrapRaftPayload_Helper sanity-checks that the engine-internal
// unwrapRaftPayload helper marks all encryption errors with
// ErrRaftUnwrapFailed (so callers can errors.Is-match without
// caring which underlying code class fired).
func TestUnwrapRaftPayload_Helper(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	wrapped, err := encryption.WrapRaftPayload(c, kid, newRaftNonce(t), []byte("payload"))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	// Tag tamper.
	tampered := append([]byte(nil), wrapped...)
	tampered[len(tampered)-1] ^= 0xff
	_, err = unwrapRaftPayload(c, tampered)
	if !errors.Is(err, ErrRaftUnwrapFailed) {
		t.Fatalf("expected ErrRaftUnwrapFailed via Mark, got %v", err)
	}
	if !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected nested ErrIntegrity preserved, got %v", err)
	}
}
