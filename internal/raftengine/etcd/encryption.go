package etcd

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

// ErrRaftUnwrapFailed is returned by applyNormalEntry when the
// pre-apply hook cannot unwrap a §4.2 raft envelope (GCM tag
// mismatch, missing DEK in the local keystore, malformed
// envelope, or active tampering).
//
// Per design §6.3 this is a process-fatal event, NOT a recoverable
// Apply error. applyCommitted propagates the error up to runLoop,
// which exits via the engine's existing fatal-error path. The
// failing entry's index is NOT advanced through setApplied — the
// next restart must replay the same entry, not skip it. Silently
// skipping would let the local FSM diverge from peers that DID
// successfully unwrap and apply, breaking the consistency
// invariant the integrity tag was added to detect.
//
// Operator response: investigate sidecar / Raft-log divergence
// (§5.5 of the encryption design doc) or KEK custody (§9.3); a
// supervised restart with a corrected sidecar is the only safe
// recovery path.
var ErrRaftUnwrapFailed = errors.New("raftengine/etcd: raft envelope unwrap failed; halting apply")

// RaftCutoverIndex returns the §7.1 Phase 2 cutover Raft index.
// Entries with index strictly greater than the returned value carry
// raft-envelope-wrapped fsm payloads; entries at or below the
// cutover are cleartext. The returned value is read on every
// applyNormalEntry, so implementations should be lock-free
// (atomic.Uint64.Load) — the engine does not synchronize the read.
//
// The Stage 3 default (when OpenConfig.RaftCutoverIndex is nil) is
// `^uint64(0)` (no entry's index is greater) so the unwrap path is
// inert until Stage 6 wires the sidecar's
// raft_envelope_cutover_index in.
type RaftCutoverIndex func() uint64

// inertRaftCutoverIndex is the OpenConfig-default returned when no
// cutover function is supplied: every entry index is treated as
// "below cutover" and the pre-apply hook is a no-op.
func inertRaftCutoverIndex() uint64 {
	return ^uint64(0)
}

// orInertCutover returns the supplied callback if non-nil, otherwise
// the inert default. Letting Engine.raftCutoverIndex be a real
// closure avoids a nil-check in the apply hot path.
func orInertCutover(fn RaftCutoverIndex) RaftCutoverIndex {
	if fn == nil {
		return inertRaftCutoverIndex
	}
	return fn
}

// unwrapRaftPayload runs the §4.2 raft envelope Unwrap when both a
// cipher is wired AND entry.Index > cutover(). Returns the
// cleartext payload on success, or wraps any decrypt failure with
// ErrRaftUnwrapFailed for the caller to recognise via errors.Is.
//
// Extracted so applyNormalEntry stays a one-liner and the unit
// tests can exercise the cutover gate + error mapping without
// constructing a full Engine.
func unwrapRaftPayload(cipher *encryption.Cipher, payload []byte) ([]byte, error) {
	plain, err := encryption.UnwrapRaftPayload(cipher, payload)
	if err != nil {
		// Mark wraps the encryption-package error with
		// ErrRaftUnwrapFailed so the apply loop's errors.Is check
		// distinguishes envelope-unwrap from other Apply paths;
		// the underlying ErrIntegrity / ErrUnknownKeyID stays
		// available for diagnostic logs via errors.Is.
		return nil, errors.Wrap(errors.Mark(err, ErrRaftUnwrapFailed), "raftengine/etcd: raft envelope unwrap")
	}
	return plain, nil
}
