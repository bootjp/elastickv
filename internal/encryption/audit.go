package encryption

import (
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	pkgerrors "github.com/cockroachdb/errors"
)

// IsEncryptionRelevantOpcode reports whether the supplied FSM
// opcode byte is one of the §5.5 encryption-relevant opcodes:
// 0x03 OpRegistration, 0x04 OpBootstrap, 0x05 OpRotation, and
// the reserved 0x06 / 0x07 slots in the fsmwire OpEncryption
// range.
//
// IMPORTANT — wire-format contract: the opcode byte is `data[0]`
// of the FSM entry payload (the LEADING opcode tag), NOT the
// byte after a wire-version prefix. See kv/fsm_encryption.go
// which dispatches via `wireBytes[0]` and the EncodeBootstrap /
// EncodeRotation / EncodeRegistration helpers in fsmwire/wire.go
// which produce `[opcode, version=0x01, ...payload]`. A scanner
// that misreads the layout and inspects payload bytes (e.g., a
// rotation sub-tag at position 1+) would return false negatives,
// silently letting GuardSidecarBehindRaftLog miss encryption-
// relevant gaps and start with a stale sidecar.
//
// The predicate is the §9.1 ErrSidecarBehindRaftLog guard's
// gap-coverage check: an unapplied entry in the sidecar/engine
// gap matters iff its `data[0]` is in this range. Non-encryption-
// relevant entries (writes, transactions, control-plane RPCs)
// in the gap do not affect the encryption sidecar and are safe
// to ignore.
//
// Defined here (rather than in fsmwire) so the encryption
// package owns its semantic-level predicate; the fsmwire
// package owns the wire-level constants (OpEncryptionMin /
// OpEncryptionMax) that this function reads.
func IsEncryptionRelevantOpcode(opcode byte) bool {
	return opcode >= fsmwire.OpEncryptionMin && opcode <= fsmwire.OpEncryptionMax
}

// EncryptionRelevantScanner is the cross-package contract that
// GuardSidecarBehindRaftLog uses to inspect a Raft entry-index
// range for encryption-relevant opcodes WITHOUT importing the
// raftengine into the encryption package.
//
// The raftengine implements this in a follow-up PR (Stage 6C-2c)
// against its WAL + applied-snapshot state, exposing only the
// predicate result. Shipping the encryption-side primitive here
// without the engine-side implementation is intentional: it lets
// Stage 6D-and-later RPC handlers reuse the same predicate
// (`IsEncryptionRelevantOpcode`) without depending on raftengine
// having shipped its scanner first.
//
// HasEncryptionRelevantEntryInRange returns true iff at least
// one Raft entry with index in [startExclusive+1, endInclusive]
// carries a §5.5-relevant opcode. The startExclusive parameter
// is the sidecar's last-applied index (which has already been
// reflected in the sidecar), so the entry AT that index is NOT
// in the gap; the entries that follow it are.
//
// Implementations must handle the empty-range case
// (startExclusive >= endInclusive) by returning (false, nil)
// — the guard precomputes the gap-non-empty branch and only
// calls scan when there's actually a range to inspect, but
// defensive implementations are safer in the face of caller bugs.
type EncryptionRelevantScanner interface {
	HasEncryptionRelevantEntryInRange(startExclusive, endInclusive uint64) (bool, error)
}

// GuardSidecarBehindRaftLog implements the §9.1
// ErrSidecarBehindRaftLog refusal logic as a pure function: it
// reads no I/O and depends on nothing beyond the supplied indices
// and the caller-provided scanner.
//
// The contract:
//
//  1. If sidecarAppliedIdx >= engineAppliedIdx, the sidecar is
//     caught up (or ahead, which would itself be a bug — but
//     this guard's scope is "behind", not "ahead"). Return nil.
//  2. If the gap is non-empty, ask the scanner whether any entry
//     in (sidecarAppliedIdx, engineAppliedIdx] is encryption-
//     relevant. If yes, fire ErrSidecarBehindRaftLog. If no, the
//     gap is harmless and we return nil.
//  3. Propagate any scanner I/O error wrapped with context but
//     NOT marked as ErrSidecarBehindRaftLog — scanner failure is
//     a different operator problem than the gap-coverage refusal.
//
// The function does not consult flags or encryption state; it is
// the caller's responsibility to skip the call when
// --encryption-enabled is off (the gap is irrelevant in that
// case) or when the engine hasn't yet been opened (no applied
// index to compare against).
func GuardSidecarBehindRaftLog(sidecarAppliedIdx, engineAppliedIdx uint64, scanner EncryptionRelevantScanner) error {
	if sidecarAppliedIdx >= engineAppliedIdx {
		return nil
	}
	if scanner == nil {
		// A nil scanner cannot inspect the gap, so we cannot
		// safely advance past it. Fail closed.
		return pkgerrors.Wrapf(ErrSidecarBehindRaftLog,
			"sidecar_applied_index=%d engine_applied_index=%d (no scanner provided to inspect gap)",
			sidecarAppliedIdx, engineAppliedIdx)
	}
	hit, err := scanner.HasEncryptionRelevantEntryInRange(sidecarAppliedIdx, engineAppliedIdx)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"encryption: scan raft entries (%d, %d] for encryption-relevant opcodes",
			sidecarAppliedIdx, engineAppliedIdx)
	}
	if !hit {
		return nil
	}
	return pkgerrors.Wrapf(ErrSidecarBehindRaftLog,
		"sidecar_applied_index=%d engine_applied_index=%d gap_covers_encryption_relevant_entry",
		sidecarAppliedIdx, engineAppliedIdx)
}
