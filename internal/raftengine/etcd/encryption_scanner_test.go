package etcd

import (
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// newEntry constructs a normal raftpb.Entry at the supplied index
// with a one-byte payload that begins with the supplied opcode.
// The §5.5 wire format has the opcode at data[0], so this single
// byte is enough to exercise the scanner's predicate.
func newEntry(index uint64, opcode byte) raftpb.Entry {
	return raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Term:  1,
		Index: index,
		Data:  []byte{opcode},
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
		if err := storage.Append(entries); err != nil {
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
		{Type: raftpb.EntryConfChange, Term: 1, Index: 1, Data: []byte{fsmwire.OpBootstrap}},
		{Type: raftpb.EntryConfChangeV2, Term: 1, Index: 2, Data: []byte{fsmwire.OpRotation}},
	}
	s := scannerWithEntries(t, entries...)
	hit, err := s.HasEncryptionRelevantEntryInRange(0, 2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if hit {
		t.Fatal("conf-change entries must NOT be classified as encryption-relevant even if their Data byte 0 is in the OpEncryption range")
	}
}

// TestEncryptionScanner_EmptyDataSkipped verifies that
// zero-length normal entries (etcd-raft uses these for leadership
// bumps after election) are skipped. The opcode-byte read would
// be an index-out-of-range panic if we didn't gate on data length.
func TestEncryptionScanner_EmptyDataSkipped(t *testing.T) {
	entries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Term: 1, Index: 1, Data: nil},
		{Type: raftpb.EntryNormal, Term: 1, Index: 2, Data: []byte{}},
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
		Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 1},
	}
	if err := storage.ApplySnapshot(snap); err != nil {
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
	if !errors.Is(err, etcdraft.ErrCompacted) {
		t.Logf("note: wrapped error %v — caller surfaces this as a scanner-error refusal", err)
	}
}

// TestEncryptionScanner_NilStorageError verifies the defensive
// nil-storage check. A zero-value encryptionScanner (or one
// constructed before the engine is opened) must fail closed
// rather than nil-deref.
func TestEncryptionScanner_NilStorageError(t *testing.T) {
	var s encryptionScanner
	_, err := s.HasEncryptionRelevantEntryInRange(0, 5)
	if err == nil {
		t.Fatal("nil-storage scanner must return an error")
	}
}
