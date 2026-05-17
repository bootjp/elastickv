package etcd

import (
	"math"

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
// against the engine's in-memory raft storage. It wraps the
// engine's *MemoryStorage; a fresh value is constructed per
// Engine.EncryptionScanner() call (cheap — no internal state).
// Callers get access to it via Engine.EncryptionScanner().
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
	// endInclusive+1). Guard against uint64 wraparound at
	// MaxUint64 (otherwise hi = 0 and the loop never runs).
	if endInclusive == math.MaxUint64 {
		return false, errors.New("encryption scanner: endInclusive == math.MaxUint64 cannot be expressed as half-open Entries() range")
	}
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
			// Fail closed: an empty batch with cursor < hi means
			// MemoryStorage didn't advance even though the requested
			// range is non-empty. Treating this as "no hit" would
			// silently classify an unscanned range as harmless, so
			// we surface it as a scanner error. The caller routes
			// scanner errors as a refusal distinct from
			// ErrSidecarBehindRaftLog.
			return false, errors.WithStack(errors.Newf(
				"encryption scanner: Entries(cursor=%d, hi=%d) returned no entries and no error (unscanned gap)",
				cursor, hi))
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
//   - Entries whose Data does not decode as a valid proposal
//     envelope (zero-length leadership bumps, malformed entries).
//   - Envelopes wrapping an empty FSM payload.
//
// For valid envelopes, returns IsEncryptionRelevantOpcode(payload[0])
// where payload is the post-envelope-strip FSM byte sequence. The
// engine wraps every EntryNormal in a 9-byte proposal envelope
// (1 byte version + 8 byte proposal ID) via encodeProposalEnvelope
// at propose time, so the raw Data[0] is always the envelope
// version 0x01 — NEVER the FSM opcode. A scanner that misreads
// the layout would return false for every entry in a real Raft
// log, silently defeating the §9.1 ErrSidecarBehindRaftLog guard.
func isEncryptionRelevantEntry(ent raftpb.Entry) bool {
	if ent.Type != raftpb.EntryNormal {
		return false
	}
	_, payload, ok := decodeProposalEnvelope(ent.Data)
	if !ok || len(payload) == 0 {
		return false
	}
	return encryption.IsEncryptionRelevantOpcode(payload[0])
}

// EncryptionScanner returns the engine's
// encryption.EncryptionRelevantScanner implementation. The
// returned value is safe to call after Open() has populated
// MemoryStorage from the on-disk WAL + snapshot, and only after
// — calling it before Open() returns a nil-storage error from
// HasEncryptionRelevantEntryInRange.
//
// Used by main.go's startup-guard phase (Stage 6C-2d) to invoke
// encryption.GuardSidecarBehindRaftLog against the engine's
// applied index and the sidecar's raft_applied_index.
//
// Nil-receiver-safe: a nil *Engine returns a zero-value
// encryptionScanner whose HasEncryptionRelevantEntryInRange will
// surface the nil-storage error. This matches the rest of the
// package's defensive nil-receiver posture (Status() / AppliedIndex()
// return zero values on nil receivers) and lets call sites in
// the lifecycle that may forward a maybe-nil engine pointer
// avoid a panic during startup/refusal triage.
func (e *Engine) EncryptionScanner() encryption.EncryptionRelevantScanner {
	if e == nil {
		return &encryptionScanner{}
	}
	return &encryptionScanner{storage: e.storage}
}
