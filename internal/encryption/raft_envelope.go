package encryption

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// RaftAADPurpose is the literal byte 'R' (0x52) that prefixes the
// raft-envelope AAD per design §4.2. It distinguishes a raft envelope
// from a storage envelope: a storage-layer ciphertext replayed into
// the raft layer (or the reverse) fails GCM verification because the
// AAD prefix does not match.
const RaftAADPurpose byte = 'R' // 0x52

// raftAADSize is the length of the raft-envelope AAD:
//
//	purpose(1) ‖ envelope_version(1) ‖ key_id(4)
//
// No pebble_key — the raft envelope is location-independent. The
// engine identifies the entry by raftpb.Entry.Index, which the
// pre-apply hook uses to gate the Unwrap (§6.3).
const raftAADSize = 1 + versionBytes + keyIDBytes // 6

// BuildRaftAAD composes the §4.2 raft-envelope AAD: a single-byte
// purpose tag ('R'), the envelope version, and the 4-byte big-endian
// key_id. Exposed for tests; production callers go through
// WrapRaftPayload / UnwrapRaftPayload.
func BuildRaftAAD(version byte, keyID uint32) []byte {
	aad := make([]byte, raftAADSize)
	aad[0] = RaftAADPurpose
	aad[1] = version
	binary.BigEndian.PutUint32(aad[2:2+keyIDBytes], keyID)
	return aad
}

// WrapRaftPayload wraps payload in a §4.2 raft envelope under the DEK
// identified by keyID, using the supplied 12-byte nonce. The cipher
// must already hold the keyID under the "raft" purpose (the keystore
// itself does not enforce purpose — that contract is maintained by the
// sidecar loader).
//
// The flag byte is fixed at 0x00; raft proposals do not carry the
// Snappy compression bit (the apply path is latency-sensitive and
// proposals are small / high-entropy).
//
// Nonce uniqueness is the caller's responsibility: re-using a
// (keyID, nonce) pair under the same DEK is a catastrophic AES-GCM
// failure (key-recovery + plaintext XOR). The §4.2 deterministic
// nonce construction (`node_id ‖ local_epoch ‖ write_count`)
// guarantees uniqueness by construction; do not substitute a
// different scheme without an equivalent uniqueness proof.
func WrapRaftPayload(c *Cipher, keyID uint32, nonce, payload []byte) ([]byte, error) {
	if c == nil {
		return nil, errors.WithStack(ErrNilKeystore)
	}
	if len(nonce) != NonceSize {
		return nil, errors.Wrapf(ErrBadNonceSize, "got %d bytes, want %d", len(nonce), NonceSize)
	}
	const envelopeFlag byte = 0
	aad := BuildRaftAAD(EnvelopeVersionV1, keyID)
	body, err := c.Encrypt(payload, aad, keyID, nonce)
	if err != nil {
		return nil, errors.Wrap(err, "encryption: raft envelope encrypt")
	}
	var nonceArr [NonceSize]byte
	copy(nonceArr[:], nonce)
	env := Envelope{
		Version: EnvelopeVersionV1,
		Flag:    envelopeFlag,
		KeyID:   keyID,
		Nonce:   nonceArr,
		Body:    body,
	}
	encoded, err := env.Encode()
	if err != nil {
		return nil, errors.Wrap(err, "encryption: raft envelope encode")
	}
	return encoded, nil
}

// UnwrapRaftPayload reverses WrapRaftPayload. Decodes the envelope,
// rebuilds the AAD identically, and calls Decrypt. The same `*Cipher`
// instance used at wrap time must hold the embedded keyID (or one of
// its rotated successors) for unwrap to succeed.
//
// Surfaces typed errors callers can disambiguate via errors.Is:
//
//   - ErrEnvelopeShort: encoded shorter than HeaderSize+TagSize
//   - ErrEnvelopeVersion: unknown version byte
//   - ErrEnvelopeFlag: a storage-only or unknown flag bit is present
//   - ErrUnknownKeyID: DEK is not loaded (retired or sidecar missing)
//   - ErrIntegrity: GCM tag mismatch (tampered envelope, wrong DEK,
//     or layer confusion with a storage envelope)
//
// A storage envelope fed to UnwrapRaftPayload fails with
// ErrIntegrity because the storage AAD prefix
// ('envelope_version ‖ flag ‖ key_id ‖ value_header(9B) ‖ pebble_key')
// does not start with the raft-purpose byte 'R'.
func UnwrapRaftPayload(c *Cipher, encoded []byte) ([]byte, error) {
	if c == nil {
		return nil, errors.WithStack(ErrNilKeystore)
	}
	env, err := DecodeEnvelope(encoded)
	if err != nil {
		return nil, errors.Wrap(err, "encryption: raft envelope decode")
	}
	if env.Flag != 0 {
		return nil, errors.Wrapf(ErrEnvelopeFlag,
			"encryption: raft envelope flag must be 0x00, got 0x%02x", env.Flag)
	}
	aad := BuildRaftAAD(env.Version, env.KeyID)
	plain, err := c.Decrypt(env.Body, aad, env.KeyID, env.Nonce[:])
	if err != nil {
		return nil, errors.Wrap(err, "encryption: raft envelope decrypt")
	}
	return plain, nil
}
