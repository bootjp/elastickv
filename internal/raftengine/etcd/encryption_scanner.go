package etcd

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// scanMaxBytes caps the byte size of a single Entries() fetch from
// MemoryStorage during a gap scan. The §9.1 ErrSidecarBehindRaftLog
// guard runs once per process start, so a comfortable margin over
// any expected gap (typical: 1-N entries from a partial-write
// crash) keeps the scan to a single Entries() call without the
// per-iteration ceremony of a pagination loop. If a real gap ever
// exceeds this size the iteration falls back to additional calls
// (see scanEntries() below) so the cap is not a correctness
// boundary, only a per-call memory cap.
const scanMaxBytes uint64 = 64 << 20 // 64 MiB

// encryptionScanner implements encryption.EncryptionRelevantScanner
// against the engine's in-memory raft storage. It is constructed
// once per Engine and embedded into the engine struct; callers get
// access to it via Engine.EncryptionScanner().
//
// The scan walks raftpb.Entry rows in (startExclusive, endInclusive]
// via etcdraft.MemoryStorage.Entries(lo, hi, maxSize), filters out
// non-normal entries (conf-change, empty leadership bump) since the
// §5.5 IsEncryptionRelevantOpcode predicate only applies to normal
// FSM entries, and reports whether any survives the predicate.
//
// Compacted-out-of-MemoryStorage ranges return raft's
// ErrCompacted (or ErrUnavailable) which the scanner wraps and
// propagates. GuardSidecarBehindRaftLog routes that as a scanner
// error (NOT as ErrSidecarBehindRaftLog), so the operator sees a
// distinct refusal: "the gap exists but is below the snapshot
// horizon; run encryption resync-sidecar to advance past the
// compacted entries". A sidecar that far behind is itself the
// operator's primary problem; the scanner just reports honestly.
type encryptionScanner struct {
	storage *etcdraft.MemoryStorage
}

// HasEncryptionRelevantEntryInRange satisfies the
// encryption.EncryptionRelevantScanner contract: returns true iff
// at least one raftpb.Entry with index in (startExclusive,
// endInclusive] is a §5.5 sidecar-mutating opcode per
// encryption.IsEncryptionRelevantOpcode.
//
// Empty range (startExclusive >= endInclusive) returns (false, nil)
// per the interface contract. The encryption.GuardSidecarBehindRaftLog
// caller precomputes the non-empty-gap branch and only calls scan
// when there's a range to inspect, but the defensive guard stays.
func (s *encryptionScanner) HasEncryptionRelevantEntryInRange(startExclusive, endInclusive uint64) (bool, error) {
	if s.storage == nil {
		return false, errors.New("encryption scanner: storage is nil (engine not opened?)")
	}
	if startExclusive >= endInclusive {
		return false, nil
	}
	// MemoryStorage.Entries(lo, hi, ...) returns indices [lo, hi).
	// We want (startExclusive, endInclusive] = [startExclusive+1,
	// endInclusive+1).
	lo := startExclusive + 1
	hi := endInclusive + 1
	cursor := lo
	for cursor < hi {
		batch, err := s.storage.Entries(cursor, hi, scanMaxBytes)
		if err != nil {
			return false, errors.Wrapf(err,
				"encryption scanner: read raft entries [%d, %d) from MemoryStorage",
				cursor, hi)
		}
		if len(batch) == 0 {
			// Defensive: MemoryStorage.Entries returning no entries
			// without an error means we have made no progress. Bail
			// out rather than spin.
			break
		}
		for _, ent := range batch {
			if isEncryptionRelevantEntry(ent) {
				return true, nil
			}
		}
		// Continue from one past the last returned index.
		cursor = batch[len(batch)-1].Index + 1
	}
	return false, nil
}

// isEncryptionRelevantEntry centralises the per-Entry predicate.
// Returns false for:
//
//   - EntryConfChange / EntryConfChangeV2 — never carry encryption
//     FSM payloads.
//   - Empty Data — etcd-raft uses a zero-length normal entry to
//     announce a new leader; not a real FSM op.
//
// Returns IsEncryptionRelevantOpcode(Data[0]) otherwise. The
// opcode is the LEADING byte per the §5.5 wire format (see
// kv/fsm_encryption.go and the encoders in fsmwire).
func isEncryptionRelevantEntry(ent raftpb.Entry) bool {
	if ent.Type != raftpb.EntryNormal {
		return false
	}
	if len(ent.Data) == 0 {
		return false
	}
	return encryption.IsEncryptionRelevantOpcode(ent.Data[0])
}

// EncryptionScanner returns the engine's
// encryption.EncryptionRelevantScanner implementation. The
// returned value is safe to call after Open() has populated
// MemoryStorage from the on-disk WAL + snapshot, and only after
// — calling it before Open() returns a nil-storage error.
//
// Used by main.go's startup-guard phase (Stage 6C-2d) to invoke
// encryption.GuardSidecarBehindRaftLog against the engine's
// applied index and the sidecar's raft_applied_index.
func (e *Engine) EncryptionScanner() encryption.EncryptionRelevantScanner {
	return &encryptionScanner{storage: e.storage}
}
