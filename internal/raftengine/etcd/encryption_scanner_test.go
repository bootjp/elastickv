package etcd

import (
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// newEntry constructs a normal raftpb.Entry at the supplied index
// whose Data is the production-shape proposal envelope:
// [version=0x01][8-byte proposal id][payload]. The payload is a
// one-byte buffer containing the supplied opcode. Wrapping via
// encodeProposalEnvelope is critical — without it the entry's
// Data[0] would be the opcode directly, which is NOT what the
// engine ever writes; tests against bare-byte Data passed
// trivially because Data[0] happened to match the opcode,
// masking the envelope-strip bug fixed in the scanner per
// gemini CRITICAL on PR #783.
func newEntry(index uint64, opcode byte) raftpb.Entry {
	return raftpb.Entry{
		Type:  entryTypePtr(raftpb.EntryNormal),
		Term:  uint64Ptr(1),
		Index: uint64Ptr(index),
		Data:  encodeProposalEnvelope(index, []byte{opcode}),
	}
}

// scannerWithEntries builds an encryptionScanner backed by a fresh
// MemoryStorage containing the supplied entries. The storage is
// snapshot-bumped to index 0 (no snapshot) so the entries are
// readable from index 1 onward — the same baseline a freshly
// opened engine sees.
func scannerWithEntries(t *testing.T, entries ...raftpb.Entry) *encryptionScanner {
	t.Helper()
	storage := etcdraft.NewMemoryStorage()
	if len(entries) > 0 {
		if err := storage.Append(entryPointers(entries)); err != nil {
			t.Fatalf("storage.Append: %v", err)
		}
	}
	return &encryptionScanner{storage: storage}
}

// TestEncryptionScanner_EmptyRange verifies the no-op fast path:
// startExclusive >= endInclusive must short-circuit to
// (false, nil) without touching the storage backend.
func TestEncryptionScanner_EmptyRange(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start uint64
		end   uint64
	}{
		{"equal", 5, 5},
		{"start_ahead", 10, 5},
		{"both_zero", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := scannerWithEntries(t) // empty storage
			hit, err := s.HasEncryptionRelevantEntryInRange(tc.start, tc.end)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hit {
				t.Fatalf("empty range must report no hit")
			}
		})
	}
}

// TestEncryptionScanner_NoRelevantEntries pins the
// "behind but harmless" path: the gap exists and entries are
// readable, but none carry a §5.5 sidecar-mutating opcode.
//
// We use OpRegistration (0x03) deliberately because that opcode
// was the codex P2 false-positive on PR #782 — pinning the
// scanner against it ensures the predicate's exclusion is wired
// through end-to-end.
func TestEncryptionScanner_NoRelevantEntries(t *testing.T) {
	entries := []raftpb.Entry{
		newEntry(1, 0x00),                   // non-encryption opcode
		newEntry(2, 0x01),                   // non-encryption opcode
		newEntry(3, fsmwire.OpRegistration), // 0x03 — IN the OpEncryption range
		newEntry(4, 0x02),                   // non-encryption opcode
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 4)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if hit {
		t.Fatal("range with only non-sidecar-mutating opcodes must not be classified as relevant — registration (0x03) is in OpEncryption range but is NOT sidecar-mutating")
	}
}

// TestEncryptionScanner_BootstrapHit verifies the fire path:
// at least one OpBootstrap entry in the range triggers the
// predicate.
func TestEncryptionScanner_BootstrapHit(t *testing.T) {
	entries := []raftpb.Entry{
		newEntry(1, 0x00),
		newEntry(2, fsmwire.OpBootstrap),
		newEntry(3, 0x00),
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 3)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !hit {
		t.Fatal("range containing OpBootstrap must be classified as relevant")
	}
}

// TestEncryptionScanner_RotationHit verifies OpRotation triggers
// the predicate just like OpBootstrap. Uses a small range starting
// from index 1 to keep within MemoryStorage's sequential-index
// requirement.
func TestEncryptionScanner_RotationHit(t *testing.T) {
	entries := []raftpb.Entry{
		newEntry(1, 0x00),
		newEntry(2, fsmwire.OpRotation),
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(1, 2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !hit {
		t.Fatal("range containing OpRotation must be classified as relevant")
	}
}

// TestEncryptionScanner_NonNormalEntriesSkipped verifies that
// EntryConfChange / EntryConfChangeV2 entries are skipped even
// if their Data happens to start with a byte in the encryption
// range. Config changes never carry FSM payloads, so they must
// not falsely trigger the predicate.
func TestEncryptionScanner_NonNormalEntriesSkipped(t *testing.T) {
	entries := []raftpb.Entry{
		{Type: entryTypePtr(raftpb.EntryConfChange), Term: uint64Ptr(1), Index: uint64Ptr(1), Data: encodeProposalEnvelope(1, []byte{fsmwire.OpBootstrap})},
		{Type: entryTypePtr(raftpb.EntryConfChangeV2), Term: uint64Ptr(1), Index: uint64Ptr(2), Data: encodeProposalEnvelope(2, []byte{fsmwire.OpRotation})},
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if hit {
		t.Fatal("conf-change entries must NOT be classified as encryption-relevant even if their envelope-stripped payload byte 0 is in the OpEncryption range")
	}
}

// TestEncryptionScanner_EmptyDataSkipped verifies that
// zero-length normal entries (etcd-raft uses these for leadership
// bumps after election) and malformed (sub-envelope-length)
// entries are skipped. decodeProposalEnvelope returns ok=false
// for both — the predicate must short-circuit cleanly without
// nil-deref or out-of-range panic.
func TestEncryptionScanner_EmptyDataSkipped(t *testing.T) {
	entries := []raftpb.Entry{
		{Type: entryTypePtr(raftpb.EntryNormal), Term: uint64Ptr(1), Index: uint64Ptr(1), Data: nil},
		{Type: entryTypePtr(raftpb.EntryNormal), Term: uint64Ptr(1), Index: uint64Ptr(2), Data: []byte{}},
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if hit {
		t.Fatal("zero-length normal entries (leadership bumps) must NOT be classified as encryption-relevant")
	}
}

// TestEncryptionScanner_EnvelopeStripping_RawBytesIgnored pins
// gemini CRITICAL on PR #783: an entry whose Data[0] coincidentally
// matches an encryption opcode but is NOT wrapped in a proposal
// envelope (envelope version != 0x01 or length < 9) MUST be
// rejected by decodeProposalEnvelope, not silently classified
// as a hit. Otherwise the scanner would have returned true for
// every entry whose raw byte 0 happens to land in [0x04, 0x07] —
// the exact false-positive class the envelope strip prevents.
func TestEncryptionScanner_EnvelopeStripping_RawBytesIgnored(t *testing.T) {
	entries := []raftpb.Entry{
		// Bare opcode byte, no envelope. decodeProposalEnvelope
		// returns ok=false because len(data) < envelopeHeaderSize.
		{Type: entryTypePtr(raftpb.EntryNormal), Term: uint64Ptr(1), Index: uint64Ptr(1), Data: []byte{fsmwire.OpBootstrap}},
		// Wrong version byte but full length. decodeProposalEnvelope
		// returns ok=false because data[0] != proposalEnvelopeVersion.
		{Type: entryTypePtr(raftpb.EntryNormal), Term: uint64Ptr(1), Index: uint64Ptr(2), Data: []byte{0x99, 0, 0, 0, 0, 0, 0, 0, 0, fsmwire.OpBootstrap}},
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if hit {
		t.Fatal("entries without a valid proposal envelope must NOT be classified as encryption-relevant — would re-introduce the pre-fix false-positive class")
	}
}

// TestEncryptionScanner_CompactedRange propagates the storage
// error when the requested range is below the snapshot horizon.
// The error MUST NOT be silently classified as "no hit" — the
// scanner cannot inspect the gap and must fail loudly so the
// guard surfaces it as a scanner-error refusal (distinct from
// ErrSidecarBehindRaftLog gap-coverage refusal).
func TestEncryptionScanner_CompactedRange(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	// Establish a snapshot at index 10 — entries below 10 become
	// inaccessible via Entries().
	snap := raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: uint64Ptr(10), Term: uint64Ptr(1)},
	}
	if err := storage.ApplySnapshot(&snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	s := &encryptionScanner{storage: storage}
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 5)
	if err == nil {
		t.Fatal("expected compacted-range error, got nil")
	}
	if hit {
		t.Errorf("compacted range must NOT report hit=true; got hit=%v err=%v", hit, err)
	}
	// Enforce the wrapped sentinel: the caller (GuardSidecarBehindRaftLog)
	// distinguishes scanner errors from gap-coverage refusals AND
	// downstream callers may want to errors.Is on ErrCompacted
	// specifically to suggest `encryption resync-sidecar` in the
	// recovery instructions.
	if !errors.Is(err, etcdraft.ErrCompacted) {
		t.Fatalf("compacted-range error MUST be errors.Is(err, etcdraft.ErrCompacted); got %v", err)
	}
}

// TestEncryptionScanner_NilStorageError verifies the defensive
// nil-storage check. A zero-value encryptionScanner (or one
// constructed before the engine is opened) must fail closed
// rather than nil-deref. The range is non-empty so the
// empty-range short-circuit doesn't fire first.
func TestEncryptionScanner_NilStorageError(t *testing.T) {
	var s encryptionScanner
	_, err := s.HasEncryptionRelevantEntryInRange(0, 5)
	if err == nil {
		t.Fatal("nil-storage scanner must return an error on a non-empty range")
	}
}

// TestEncryptionScanner_NilStorageEmptyRange pins the
// EncryptionRelevantScanner contract: empty range MUST return
// (false, nil) UNCONDITIONALLY — including on a nil-storage
// scanner. The guard order in HasEncryptionRelevantEntryInRange
// is empty-range-first, then nil-storage, so this test pins the
// ordering against future regressions where someone reorders the
// guards and reintroduces the contract violation claude flagged
// on PR #783 r3.
func TestEncryptionScanner_NilStorageEmptyRange(t *testing.T) {
	var s encryptionScanner
	hit, err := s.HasEncryptionRelevantEntryInRange(5, 5)
	if err != nil {
		t.Fatalf("nil-storage scanner with empty range MUST return (false, nil) per contract; got err=%v", err)
	}
	if hit {
		t.Fatalf("empty range must report no hit; got hit=%v", hit)
	}
}

// TestEngineEncryptionScanner_NilReceiver pins the codex P2
// finding on PR #783: calling Engine.EncryptionScanner() on a
// nil *Engine MUST return a usable scanner that surfaces the
// nil-storage error rather than panicking. Production paths that
// forward a maybe-nil engine pointer during startup/refusal
// triage rely on this defensive return.
func TestEngineEncryptionScanner_NilReceiver(t *testing.T) {
	var e *Engine
	scanner := e.EncryptionScanner()
	if scanner == nil {
		t.Fatal("Engine.EncryptionScanner() on nil receiver must return a non-nil scanner")
	}
	_, err := scanner.HasEncryptionRelevantEntryInRange(0, 5)
	if err == nil {
		t.Fatal("nil-receiver scanner must surface a storage error, not silently succeed")
	}
}

// TestEncryptionScanner_EndInclusiveMaxUint64Refused pins the
// coderabbit minor: endInclusive == math.MaxUint64 would cause
// endInclusive+1 to wrap to 0, making the half-open Entries()
// range degenerate. The scanner detects this and refuses with
// an explicit error rather than silently scanning nothing.
func TestEncryptionScanner_EndInclusiveMaxUint64Refused(t *testing.T) {
	s := scannerWithEntries(t) // empty storage; should still hit the overflow guard before any read
	hit, err := s.HasEncryptionRelevantEntryInRange(0, ^uint64(0))
	if err == nil {
		t.Fatal("endInclusive == math.MaxUint64 must refuse with an explicit error to avoid uint64 wraparound")
	}
	if hit {
		t.Errorf("overflow refusal must NOT report hit=true; got hit=%v err=%v", hit, err)
	}
}

// The "no-progress" fail-closed branch in the scanner is
// guarded by a defensive `return false, error` when
// storage.Entries returns an empty batch with no error. Real
// MemoryStorage never returns this combination (it always
// returns ErrCompacted or ErrUnavailable), so exercising the
// branch via the public Storage API isn't possible without a
// custom mock. The branch's correctness is verified by code
// inspection (any zero-length non-error batch returns an
// errors.Newf-wrapped error before the loop can advance) and
// by the compacted-range test which exercises the same
// fail-closed contract via a real ErrCompacted path.
