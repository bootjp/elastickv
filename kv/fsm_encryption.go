package kv

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/cockroachdb/errors"
)

// EncryptionApplier owns the side-effects an encryption FSM entry
// must persist on apply: keystore mutation, sidecar update, and
// writer-registry insert. Stage 4 ships the dispatch seam and
// HaltApply propagation; Stage 5/6/7 will provide a concrete
// implementation that
//
//   - KEK-unwraps the wrapped DEK and calls Keystore.Set
//   - mutates the local sidecar (Active.{Storage,Raft}, keys map,
//     raft_envelope_cutover_index) via the §5.1 crash-durable
//     WriteSidecar protocol
//   - inserts writer-registry rows under the §4.1
//     `!encryption|writers|<dek_id>|<uint16(node_id)>` Pebble key
//
// The separation lets Stage 4 land the byte-tag dispatch + halt
// machinery without depending on the Stage 7 writer-registry
// storage layer or the Stage 5 admin RPC plumbing.
//
// All three methods may return an error wrapped with
// encryption.ErrEncryptionApply to halt the apply loop. The kvFSM
// dispatcher converts any non-nil return into a haltApplyResponse
// so internal/raftengine/etcd's HaltApply seam recognises it.
type EncryptionApplier interface {
	ApplyRegistration(p fsmwire.RegistrationPayload) error
	ApplyBootstrap(p fsmwire.BootstrapPayload) error
	ApplyRotation(p fsmwire.RotationPayload) error
}

// haltApplyResponse satisfies the HaltApply interface that
// internal/raftengine/etcd.applyNormalCommitted inspects. Any
// non-nil err returned by an encryption FSM handler is packed in
// this type, which the engine recognises as "halt the apply loop
// without advancing setApplied" — the §6.3 fail-closed contract.
type haltApplyResponse struct {
	err error
}

// HaltApply returns the wrapped error so the engine's
// applyNormalCommitted halt path fires. Returning nil here would
// be a no-op halt; callers always pass a non-nil err.
func (h *haltApplyResponse) HaltApply() error {
	return h.err
}

// applyEncryption is the kvFSM dispatcher for opcodes 0x03 / 0x04 /
// 0x05. data is the payload BYTE FOLLOWING the opcode tag (the
// caller in Apply has already stripped data[0]).
//
// Without an EncryptionApplier wired (Stage 4 default in tests and
// production until Stage 5/6/7), every encryption opcode returns
// ErrEncryptionApply via the HaltApply seam — this is the
// fail-closed default: a malformed or premature encryption
// proposal halts the apply loop rather than silently advancing
// setApplied.
func (f *kvFSM) applyEncryption(opcode byte, data []byte) any {
	if f.encryption == nil {
		return haltErr(errors.Wrapf(encryption.ErrEncryptionApply,
			"encryption opcode %#x arrived but no EncryptionApplier wired", opcode))
	}
	switch opcode {
	case fsmwire.OpRegistration:
		return f.applyRegistration(data)
	case fsmwire.OpBootstrap:
		return f.applyBootstrap(data)
	case fsmwire.OpRotation:
		return f.applyRotation(data)
	default:
		return haltErr(errors.Wrapf(encryption.ErrEncryptionApply,
			"unknown encryption opcode %#x", opcode))
	}
}

// applyRegistration decodes a 0x03 payload and dispatches to the
// EncryptionApplier. Errors are wrapped with ErrEncryptionApply so
// the engine's HaltApply seam halts the apply loop.
func (f *kvFSM) applyRegistration(data []byte) any {
	p, err := fsmwire.DecodeRegistration(data)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, encryption.ErrEncryptionApply),
			"kv/fsm: decode registration"))
	}
	if err := f.encryption.ApplyRegistration(p); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, encryption.ErrEncryptionApply),
			"kv/fsm: apply registration"))
	}
	return nil
}

// applyBootstrap decodes a 0x04 payload and dispatches.
func (f *kvFSM) applyBootstrap(data []byte) any {
	p, err := fsmwire.DecodeBootstrap(data)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, encryption.ErrEncryptionApply),
			"kv/fsm: decode bootstrap"))
	}
	if err := f.encryption.ApplyBootstrap(p); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, encryption.ErrEncryptionApply),
			"kv/fsm: apply bootstrap"))
	}
	return nil
}

// applyRotation decodes a 0x05 payload and dispatches.
func (f *kvFSM) applyRotation(data []byte) any {
	p, err := fsmwire.DecodeRotation(data)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, encryption.ErrEncryptionApply),
			"kv/fsm: decode rotation"))
	}
	if err := f.encryption.ApplyRotation(p); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, encryption.ErrEncryptionApply),
			"kv/fsm: apply rotation"))
	}
	return nil
}

// haltErr is a tiny constructor so the dispatch table above stays
// terse. Always returns a non-nil *haltApplyResponse — never use
// this with err == nil (the resulting response would still satisfy
// the HaltApply interface but its HaltApply() returning nil would
// be a no-op halt, which masks intent).
func haltErr(err error) *haltApplyResponse {
	return &haltApplyResponse{err: err}
}
