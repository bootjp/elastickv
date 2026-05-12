// Package fsmwire defines the §6.3 / §11 binary wire format for the
// FSM-internal encryption Raft entry types (opcodes 0x03 / 0x04 /
// 0x05). The encoding is hand-rolled binary with a leading version
// byte rather than proto3 because:
//
//   - Each replica's apply path must reproduce the bytes exactly to
//     keep state machine determinism. proto3's lenient unknown-field
//     handling would let a future leader-built field silently drop
//     on a stale follower, leaving registry rows or active-pointer
//     entries missing on that follower — exactly the silent
//     divergence the encryption tag was added to detect.
//
//   - The schema is small (~30 lines of encode/decode per opcode),
//     so the maintenance cost of hand-rolled binary is negligible
//     while keeping every state-machine-affecting bit explicit.
//
//   - It matches the in-house style: internal/encryption/envelope.go,
//     raft_envelope.go, and kv/raft_payload_wrapper.go all hand-roll
//     binary with a leading version byte.
//
// Version byte. Every payload starts with WireVersionV1 (0x01).
// Future schema changes bump the version and the decoder fails
// closed (ErrFSMWireVersion) — operators upgrade clusters fully
// before flipping flags that produce the new format. There is no
// "skip unknown fields" path because skipping a field on the apply
// path is the divergence we are guarding against.
package fsmwire

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

// Opcode tags consumed by kv/fsm.go's Apply dispatch. Reserved
// per design §11.3:
//
//	0x00 raftEncodeSingle    (kv legacy)
//	0x01 raftEncodeBatch     (kv legacy)
//	0x02 raftEncodeHLCLease  (kv HLC ceiling)
//	0x03 OpRegistration      (writer-registry register, §4.1)
//	0x04 OpBootstrap         (initial DEK install, §5.6)
//	0x05 OpRotation          (DEK rotation / cluster flag flip, §5.2 + §7.1)
const (
	OpRegistration byte = 0x03
	OpBootstrap    byte = 0x04
	OpRotation     byte = 0x05
)

// WireVersionV1 is the current FSM-wire version. Carried as the
// second byte of every payload (after the opcode tag, which is
// stripped before fsmwire sees the payload).
const WireVersionV1 byte = 0x01

// Errors returned by fsmwire's encoders/decoders. All three are
// terminal in the apply path — kv/fsm.go marks them with
// ErrEncryptionApply so internal/raftengine/etcd halts the apply
// loop instead of silently advancing setApplied.
var (
	// ErrFSMWireMalformed indicates the payload bytes do not
	// match the fixed-shape decoder for the supplied opcode.
	// Length mismatch, missing length-prefixed body, or trailing
	// garbage all surface here.
	ErrFSMWireMalformed = errors.New("fsmwire: payload is malformed")

	// ErrFSMWireVersion indicates the payload version byte is
	// not WireVersionV1. The decoder fails closed rather than
	// trying to interpret an unknown version under the v1
	// layout.
	ErrFSMWireVersion = errors.New("fsmwire: unknown wire version")

	// ErrFSMWireSubtag indicates the rotation opcode (0x05)
	// carries an unknown sub-tag. Reserved for forward-compat:
	// future opcodes that share the rotation tag (rewrap-deks,
	// retire-dek, enable-storage-envelope, enable-raft-envelope)
	// will allocate distinct sub-tags.
	ErrFSMWireSubtag = errors.New("fsmwire: unknown rotation sub-tag")
)

// Purpose is the §5.1 sidecar purpose tag. The Cipher itself does
// not enforce purpose — that contract is maintained by the sidecar
// loader and the FSM apply handlers in kv/fsm_encryption.go. The
// fsmwire codec carries the purpose alongside each DEK so the
// applier can install it under the correct sidecar slot.
type Purpose byte

const (
	// PurposeStorage marks the storage-layer DEK (§4.1 envelopes).
	PurposeStorage Purpose = 1
	// PurposeRaft marks the raft-layer DEK (§4.2 envelopes).
	PurposeRaft Purpose = 2
)

// Sub-tags used by OpRotation. Encoded as the first byte after the
// version byte.
const (
	// RotateSubRotateDEK is a fresh DEK install + active-pointer
	// update. Ships in Stage 4. See §5.2.
	RotateSubRotateDEK byte = 0x01

	// Reserved for later stages; declared here so the byte space
	// is contiguous and a future commit cannot silently re-use
	// 0x02..0x0F without an audit:
	//
	//   0x02 — rewrap-deks       (Stage 9, §5.4 / §5.5)
	//   0x03 — retire-dek        (Stage 9, §5.4)
	//   0x04 — enable-storage-envelope (Stage 6, §7.1 Phase 1)
	//   0x05 — enable-raft-envelope    (Stage 6, §7.1 Phase 2)
)

// OpEncryptionMin / OpEncryptionMax delimit the FSM-internal opcode
// range RESERVED for the encryption subsystem. kv/fsm.go's Apply
// dispatcher routes EVERY byte in the closed range
// [OpEncryptionMin, OpEncryptionMax] through applyEncryption, which
// fails closed via ErrEncryptionApply for any byte that is not yet
// implemented (Stage 4 implements only 0x03/0x04/0x05; 0x06/0x07 are
// reserved for later stages). The widened range is the codex P1 fix
// for PR748: previously only the three implemented opcodes were
// routed, so a future leader emitting 0x06 against a stale follower
// would fall through to the legacy proto3 decoder rather than halting
// the apply loop — the same divergence shape the §6.3 fail-closed
// halt was added to prevent.
//
// Upper bound 0x07 (NOT 0x0F): proto3 wire tags for field 1 occupy
// 0x08..0x0D (field 1 with wire types varint/fixed64/length-delim/
// start-group/end-group/fixed32). Routing those through the
// encryption dispatcher would short-circuit the legacy proto3
// fallback in `decodeLegacyRaftRequest` for any RaftCommand/Request
// payload whose first encoded field is field 1 (e.g. `Request.is_txn`
// = true → first bytes `0x08 0x01`). Bytes 0x03..0x07 are SAFE
// because they encode either field 0 (proto3 disallows field 0) or
// reserved/invalid wire types (0x06/0x07 = wire types 6/7 which
// proto3 marks reserved), so no valid proto3 marshal output starts
// with them. Future encryption opcodes 0x08+ would need a different
// dispatch shape (e.g. a 2-byte sentinel) before they could be
// routed safely.
const (
	OpEncryptionMin byte = 0x03
	OpEncryptionMax byte = 0x07
)

// RegistrationPayload is the OpRegistration body: a single
// (dek_id, full_node_id, local_epoch) triple inserted by FSM apply
// into the §4.1 writer registry. The kv/fsm_encryption.go handler
// decides between case 1 (insert), 2 (re-register / monotonic
// bump), 3 (rollback → ErrLocalEpochRollback) and 4 (collision →
// ErrNodeIDCollision).
type RegistrationPayload struct {
	DEKID      uint32
	FullNodeID uint64
	LocalEpoch uint16
}

// BootstrapPayload is the OpBootstrap body: §5.6 step 1a's "install
// the initial wrapped DEK pair plus a batch of writer-registry
// rows for every member that passed the capability pre-check".
//
// The wrapped DEKs are KEK-output blobs the leader produced; the
// FSM apply layer installs them into the local keystore via
// Keystore.Set after the KEK Unwrap.
type BootstrapPayload struct {
	StorageDEKID   uint32
	WrappedStorage []byte
	RaftDEKID      uint32
	WrappedRaft    []byte
	BatchRegistry  []RegistrationPayload
}

// RotationPayload is the OpRotation body. SubTag selects between
// rotate-dek / rewrap / enable-flag variants; Stage 4 ships only
// RotateSubRotateDEK.
//
// For RotateSubRotateDEK the payload is:
//   - DEKID: the new key id being installed
//   - Purpose: which sidecar slot to update (PurposeStorage / PurposeRaft)
//   - Wrapped: the KEK-wrapped DEK bytes
//   - ProposerRegistration: the proposing node's writer-registry
//     row, inserted in the same FSM apply transaction so the
//     proposer's first encrypted write under the new DEK is
//     covered by the §4.1 case 2 epoch monotonicity check.
type RotationPayload struct {
	SubTag               byte
	DEKID                uint32
	Purpose              Purpose
	Wrapped              []byte
	ProposerRegistration RegistrationPayload
}

// Field-size constants. Named so the encoder/decoder don't sprinkle
// 1/2/4/8 magic numbers and so the layout pin tests have a single
// source of truth to drift-check against.
const (
	versionByteSize   = 1
	subTagSize        = 1
	purposeSize       = 1
	dekIDSize         = 4                                           // uint32 BE
	localEpochSize    = 2                                           // uint16 BE
	fullNodeIDSize    = 8                                           // uint64 BE
	lengthPrefixSize  = 4                                           // uint32 BE length prefix
	batchCountPrefix  = 4                                           // uint32 BE batch entry count
	registrationSize  = dekIDSize + fullNodeIDSize + localEpochSize // 14
	regPayloadEncoded = versionByteSize + registrationSize          // 15
)

// maxBootstrapBatchCount bounds the number of RegistrationPayload
// entries `readRegistrationBatch` will allocate from a single
// bootstrap payload. The wire-bytes check (count * registrationSize
// ≤ remaining) is necessary but NOT sufficient: each
// RegistrationPayload occupies 24 bytes in Go (uint32+uint64+uint16
// + alignment padding) versus 14 bytes packed on the wire, so a
// crafted bootstrap entry that fits the wire-bounds check could
// still drive a >70% larger heap allocation. With a sufficiently
// large raft entry the in-memory cost would multiply into the
// gigabytes — codex P1 round-3 finding for PR748.
//
// The cap of 16384 is ~8x the largest realistic cluster (raft
// deployments are typically tens of voters; the largest documented
// production raft clusters are in the low thousands of nodes). A
// bootstrap covers every member × 2 DEK purposes (storage + raft),
// so for an 8000-node cluster the legitimate count would be 16000
// — still under the cap. The post-cap allocation ceiling is
// 16384 * 24 = 384 KiB, which is bounded regardless of any future
// struct-padding changes.
const maxBootstrapBatchCount = 1 << 14

// EncodeRegistration serialises p as the OpRegistration payload
// (without the leading 0x03 opcode tag — that is added by the
// kv/fsm.go dispatch layer).
//
// Wire layout:
//
//	[ver 1] [dek_id 4 BE] [full_node_id 8 BE] [local_epoch 2 BE]
func EncodeRegistration(p RegistrationPayload) []byte {
	out := make([]byte, regPayloadEncoded)
	out[0] = WireVersionV1
	off := versionByteSize
	binary.BigEndian.PutUint32(out[off:off+dekIDSize], p.DEKID)
	off += dekIDSize
	binary.BigEndian.PutUint64(out[off:off+fullNodeIDSize], p.FullNodeID)
	off += fullNodeIDSize
	binary.BigEndian.PutUint16(out[off:off+localEpochSize], p.LocalEpoch)
	return out
}

// DecodeRegistration reverses EncodeRegistration. Fails closed on
// length mismatch or unknown version byte.
func DecodeRegistration(raw []byte) (RegistrationPayload, error) {
	if len(raw) != regPayloadEncoded {
		return RegistrationPayload{}, errors.Wrapf(ErrFSMWireMalformed,
			"registration: got %d bytes, want %d", len(raw), regPayloadEncoded)
	}
	if raw[0] != WireVersionV1 {
		return RegistrationPayload{}, errors.Wrapf(ErrFSMWireVersion,
			"registration: version=%#x", raw[0])
	}
	off := versionByteSize
	dekID := binary.BigEndian.Uint32(raw[off : off+dekIDSize])
	off += dekIDSize
	fullNodeID := binary.BigEndian.Uint64(raw[off : off+fullNodeIDSize])
	off += fullNodeIDSize
	localEpoch := binary.BigEndian.Uint16(raw[off : off+localEpochSize])
	return RegistrationPayload{
		DEKID:      dekID,
		FullNodeID: fullNodeID,
		LocalEpoch: localEpoch,
	}, nil
}

// EncodeBootstrap serialises p as the OpBootstrap payload (no
// leading opcode tag).
//
// Wire layout:
//
//	[ver 1]
//	[storage_dek_id 4] [storage_wrapped_len 4] [storage_wrapped ...]
//	[raft_dek_id 4]    [raft_wrapped_len 4]    [raft_wrapped ...]
//	[batch_count 4]    [registration_payload]*batch_count
//
// The two wrapped-DEK length prefixes are independent; the
// concrete length is whatever the configured KEK produced (the
// reference FileWrapper produces 60 bytes per 32-byte DEK).
func EncodeBootstrap(p BootstrapPayload) []byte {
	size := versionByteSize +
		dekIDSize + lengthPrefixSize + len(p.WrappedStorage) +
		dekIDSize + lengthPrefixSize + len(p.WrappedRaft) +
		batchCountPrefix + len(p.BatchRegistry)*registrationSize
	out := make([]byte, 0, size)
	out = append(out, WireVersionV1)
	out = appendU32(out, p.StorageDEKID)
	out = appendLenU32Bytes(out, p.WrappedStorage)
	out = appendU32(out, p.RaftDEKID)
	out = appendLenU32Bytes(out, p.WrappedRaft)
	out = appendU32(out, safeU32(len(p.BatchRegistry)))
	for _, reg := range p.BatchRegistry {
		out = appendU32(out, reg.DEKID)
		out = appendU64(out, reg.FullNodeID)
		out = appendU16(out, reg.LocalEpoch)
	}
	return out
}

// DecodeBootstrap reverses EncodeBootstrap. Fails closed on any
// length-prefix overrun or version mismatch.
func DecodeBootstrap(raw []byte) (BootstrapPayload, error) {
	r := &reader{src: raw}
	if err := readWireVersion(r, "bootstrap"); err != nil {
		return BootstrapPayload{}, err
	}
	storageID, storageWrapped, err := readDEKAndWrapped(r, "bootstrap.storage")
	if err != nil {
		return BootstrapPayload{}, err
	}
	raftID, raftWrapped, err := readDEKAndWrapped(r, "bootstrap.raft")
	if err != nil {
		return BootstrapPayload{}, err
	}
	regs, err := readRegistrationBatch(r)
	if err != nil {
		return BootstrapPayload{}, err
	}
	if r.remaining() != 0 {
		return BootstrapPayload{}, errors.Wrapf(ErrFSMWireMalformed,
			"bootstrap: %d trailing bytes", r.remaining())
	}
	return BootstrapPayload{
		StorageDEKID:   storageID,
		WrappedStorage: storageWrapped,
		RaftDEKID:      raftID,
		WrappedRaft:    raftWrapped,
		BatchRegistry:  regs,
	}, nil
}

// readWireVersion consumes the leading version byte and validates
// it. Used by both DecodeBootstrap and DecodeRotation so the
// version-check error surface stays uniform.
func readWireVersion(r *reader, opTag string) error {
	ver, ok := r.readByte()
	if !ok {
		return errors.Wrapf(ErrFSMWireMalformed, "%s: missing version byte", opTag)
	}
	if ver != WireVersionV1 {
		return errors.Wrapf(ErrFSMWireVersion, "%s: version=%#x", opTag, ver)
	}
	return nil
}

// readDEKAndWrapped reads a (dek_id, wrapped_dek) pair. Bootstrap
// reads two; rotation reads one.
func readDEKAndWrapped(r *reader, opTag string) (uint32, []byte, error) {
	dekID, ok := r.readU32()
	if !ok {
		return 0, nil, errors.Wrapf(ErrFSMWireMalformed, "%s: dek_id truncated", opTag)
	}
	wrapped, ok := r.readLenU32Bytes()
	if !ok {
		return 0, nil, errors.Wrapf(ErrFSMWireMalformed, "%s: wrapped truncated", opTag)
	}
	return dekID, wrapped, nil
}

// readRegistrationBatch reads a uint32 count followed by that many
// registration rows. Used by BootstrapPayload's batch-registry
// section.
//
// DoS guard: the count must fit in the bytes the reader has left.
// Without this guard, a malformed bootstrap payload that names
// `count = 0xffffffff` triggers `make([]RegistrationPayload, count)`
// (4G × 14 bytes ≈ 56 GiB) before the loop even tries to read the
// first row, killing the process via OOM. Since Raft entries are
// external input on the apply path, we fail closed with
// ErrFSMWireMalformed instead.
func readRegistrationBatch(r *reader) ([]RegistrationPayload, error) {
	count, ok := r.readU32()
	if !ok {
		return nil, errors.Wrap(ErrFSMWireMalformed, "bootstrap: batch count truncated")
	}
	// Hard cap on count BEFORE the wire-bytes check. The
	// wire-bytes check alone would let a count of (2^32-1) /
	// registrationSize ≈ 306M pass for a sufficiently large raft
	// entry, which `make([]RegistrationPayload, count)` would
	// turn into a multi-GiB heap allocation due to the struct's
	// alignment padding (24B in-memory vs 14B on the wire). The
	// cap bounds the post-make allocation at maxBootstrapBatchCount
	// * sizeof(RegistrationPayload) = 16384 * 24 = 384 KiB,
	// independent of how many bytes the raft entry happens to
	// carry. See the constant comment for the cluster-size
	// rationale; this is the codex P1 round-3 fix for PR748.
	if count > maxBootstrapBatchCount {
		return nil, errors.Wrapf(ErrFSMWireMalformed,
			"bootstrap: batch count %d exceeds cap %d", count, maxBootstrapBatchCount)
	}
	rem := r.remaining()
	if rem < 0 {
		return nil, errors.Wrap(ErrFSMWireMalformed, "bootstrap: negative remaining bytes")
	}
	// Compare in uint64 so neither side truncates: a 64-bit reader
	// with > 2^32 remaining bytes (theoretical) and a count just
	// under 2^32 must still produce a correct check. The
	// multiplication cannot overflow uint64 because
	// count * registrationSize <= (2^32-1) * 14 < 2^64.
	need := uint64(count) * uint64(registrationSize)
	if uint64(rem) < need {
		return nil, errors.Wrapf(ErrFSMWireMalformed,
			"bootstrap: batch count %d × %d > remaining %d", count, registrationSize, rem)
	}
	regs := make([]RegistrationPayload, count)
	for i := uint32(0); i < count; i++ {
		reg, err := readRegistrationRow(r, i)
		if err != nil {
			return nil, err
		}
		regs[i] = reg
	}
	return regs, nil
}

// readRegistrationRow reads one (dek_id, full_node_id, local_epoch)
// triple. Used by readRegistrationBatch and DecodeRotation's
// proposer-registration field.
func readRegistrationRow(r *reader, i uint32) (RegistrationPayload, error) {
	dekID, ok1 := r.readU32()
	fullNodeID, ok2 := r.readU64()
	localEpoch, ok3 := r.readU16()
	if !ok1 || !ok2 || !ok3 {
		return RegistrationPayload{}, errors.Wrapf(ErrFSMWireMalformed,
			"registration #%d truncated", i)
	}
	return RegistrationPayload{
		DEKID:      dekID,
		FullNodeID: fullNodeID,
		LocalEpoch: localEpoch,
	}, nil
}

// EncodeRotation serialises p as the OpRotation payload (no leading
// opcode tag).
//
// Wire layout (RotateSubRotateDEK):
//
//	[ver 1] [subtag 1] [dek_id 4] [purpose 1]
//	[wrapped_len 4] [wrapped ...]
//	[proposer_dek_id 4] [proposer_full_node_id 8] [proposer_local_epoch 2]
func EncodeRotation(p RotationPayload) []byte {
	size := versionByteSize + subTagSize + dekIDSize + purposeSize +
		lengthPrefixSize + len(p.Wrapped) + registrationSize
	out := make([]byte, 0, size)
	out = append(out, WireVersionV1, p.SubTag)
	out = appendU32(out, p.DEKID)
	out = append(out, byte(p.Purpose))
	out = appendLenU32Bytes(out, p.Wrapped)
	out = appendU32(out, p.ProposerRegistration.DEKID)
	out = appendU64(out, p.ProposerRegistration.FullNodeID)
	out = appendU16(out, p.ProposerRegistration.LocalEpoch)
	return out
}

// DecodeRotation reverses EncodeRotation. Fails closed on length
// prefix overrun, version mismatch, or unknown sub-tag.
func DecodeRotation(raw []byte) (RotationPayload, error) {
	r := &reader{src: raw}
	if err := readWireVersion(r, "rotation"); err != nil {
		return RotationPayload{}, err
	}
	sub, err := readRotationSubTag(r)
	if err != nil {
		return RotationPayload{}, err
	}
	dekID, purpose, wrapped, err := readRotationBody(r)
	if err != nil {
		return RotationPayload{}, err
	}
	proposerReg, err := readRegistrationRow(r, 0)
	if err != nil {
		return RotationPayload{}, errors.Wrap(err, "rotation: proposer registration")
	}
	if r.remaining() != 0 {
		return RotationPayload{}, errors.Wrapf(ErrFSMWireMalformed, "rotation: %d trailing bytes", r.remaining())
	}
	return RotationPayload{
		SubTag:               sub,
		DEKID:                dekID,
		Purpose:              purpose,
		Wrapped:              wrapped,
		ProposerRegistration: proposerReg,
	}, nil
}

// readRotationSubTag consumes the 1-byte rotation sub-tag and
// validates it against the known set. Stage 4 ships only
// RotateSubRotateDEK; later stages (rewrap, retire, enable-flag)
// will whitelist new values.
func readRotationSubTag(r *reader) (byte, error) {
	sub, ok := r.readByte()
	if !ok {
		return 0, errors.Wrap(ErrFSMWireMalformed, "rotation: missing sub-tag")
	}
	if sub != RotateSubRotateDEK {
		return 0, errors.Wrapf(ErrFSMWireSubtag, "rotation: subtag=%#x", sub)
	}
	return sub, nil
}

// readRotationBody decodes the (dek_id, purpose, wrapped) triple
// that follows the rotation sub-tag.
//
// Purpose is validated against the §5.1 enum at decode time so an
// unknown byte (e.g. 0xFF) fails closed with ErrFSMWireMalformed
// instead of being passed to the applier as an out-of-enum value.
// Without this check a malformed entry could advance setApplied if
// the applier did not independently re-validate purpose, weakening
// the §6.3 fail-closed contract. This is the codex P2 fix for
// PR748.
func readRotationBody(r *reader) (uint32, Purpose, []byte, error) {
	dekID, ok := r.readU32()
	if !ok {
		return 0, 0, nil, errors.Wrap(ErrFSMWireMalformed, "rotation: dek_id truncated")
	}
	purpose, ok := r.readByte()
	if !ok {
		return 0, 0, nil, errors.Wrap(ErrFSMWireMalformed, "rotation: purpose truncated")
	}
	switch Purpose(purpose) {
	case PurposeStorage, PurposeRaft:
		// known, fall through
	default:
		return 0, 0, nil, errors.Wrapf(ErrFSMWireMalformed,
			"rotation: unknown purpose=%#x (want %#x storage / %#x raft)",
			purpose, byte(PurposeStorage), byte(PurposeRaft))
	}
	wrapped, ok := r.readLenU32Bytes()
	if !ok {
		return 0, 0, nil, errors.Wrap(ErrFSMWireMalformed, "rotation: wrapped truncated")
	}
	return dekID, Purpose(purpose), wrapped, nil
}

// reader is a tiny binary cursor used by the Decode helpers above.
// Centralised so the per-field length checks share one set of
// "out-of-bounds returns ok=false" guards.
type reader struct {
	src []byte
	off int
}

func (r *reader) remaining() int { return len(r.src) - r.off }

func (r *reader) readByte() (byte, bool) {
	if r.remaining() < 1 {
		return 0, false
	}
	b := r.src[r.off]
	r.off++
	return b, true
}

func (r *reader) readU16() (uint16, bool) {
	if r.remaining() < localEpochSize {
		return 0, false
	}
	v := binary.BigEndian.Uint16(r.src[r.off : r.off+localEpochSize])
	r.off += localEpochSize
	return v, true
}

func (r *reader) readU32() (uint32, bool) {
	if r.remaining() < dekIDSize {
		return 0, false
	}
	v := binary.BigEndian.Uint32(r.src[r.off : r.off+dekIDSize])
	r.off += dekIDSize
	return v, true
}

func (r *reader) readU64() (uint64, bool) {
	if r.remaining() < fullNodeIDSize {
		return 0, false
	}
	v := binary.BigEndian.Uint64(r.src[r.off : r.off+fullNodeIDSize])
	r.off += fullNodeIDSize
	return v, true
}

func (r *reader) readLenU32Bytes() ([]byte, bool) {
	n, ok := r.readU32()
	if !ok {
		return nil, false
	}
	// Compare in uint64 so a 64-bit reader with >2^32 remaining
	// bytes (theoretical) is not silently truncated by uint32(rem)
	// — a regression where rem = 4.3 GiB would shrink to ~300 MiB
	// and falsely report "insufficient space". The rem<0 guard is
	// belt-and-suspenders for any future change to remaining()
	// that introduces a signed-cast hazard.
	rem := r.remaining()
	if rem < 0 || uint64(rem) < uint64(n) { //nolint:gosec // rem >= 0 enforced by the rem < 0 branch
		return nil, false
	}
	end := r.off + int(n) //nolint:gosec // n bounded by rem above; rem fits int
	out := make([]byte, n)
	copy(out, r.src[r.off:end])
	r.off = end
	return out, true
}

func appendU16(dst []byte, v uint16) []byte {
	return binary.BigEndian.AppendUint16(dst, v)
}

func appendU32(dst []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, v)
}

func appendU64(dst []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, v)
}

// appendLenU32Bytes appends a uint32 BE length prefix followed by
// data. Length must fit a uint32; safeU32 panics if it does not,
// because exceeding 4 GiB in a single fsmwire payload is a
// programmer error (a Raft entry that large could not be replicated
// regardless of encryption framing).
func appendLenU32Bytes(dst []byte, data []byte) []byte {
	dst = appendU32(dst, safeU32(len(data)))
	return append(dst, data...)
}

// safeU32 widens a non-negative int into uint32, panicking on
// overflow. The fsmwire encoders are leader-only paths where the
// caller controls every input length (wrapped DEK, batch count) at
// hundred-byte / hundred-entry scale; a uint32 overflow indicates a
// caller bug, not adversarial input, so panic is appropriate.
//
// Width-agnostic bound check: convert n to uint64 (safe for any
// non-negative int) and compare against math.MaxUint32. An
// earlier draft used `int(^uint32(0))` as the bound, which fails
// to compile on GOARCH=386 because the constant 4,294,967,295
// overflows the 32-bit signed int — the cross-compile build would
// break before any runtime check fired.
func safeU32(n int) uint32 {
	if n < 0 || uint64(n) > math.MaxUint32 { //nolint:gosec // n < 0 short-circuits the uint64 cast
		panic(errors.Newf("fsmwire: length %d does not fit in uint32", n))
	}
	return uint32(n) //nolint:gosec // bounds checked above
}
