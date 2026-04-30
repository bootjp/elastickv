package encryption

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// Public constants for the §4.1 wire format.
const (
	// EnvelopeVersionV1 is the current envelope format version. §11.3
	// reserves 0x02..0x0F for future authenticated formats. The current
	// build only understands 0x01; ANY other version byte (including the
	// 0x02..0x0F reserved range) causes DecodeEnvelope to return
	// ErrEnvelopeVersion. Future decoders that know how to handle the
	// reserved range will widen this check.
	EnvelopeVersionV1 byte = 0x01

	// FlagCompressed (bit 0) is set when ciphertext encrypts a Snappy-
	// compressed plaintext (§6.4). The flag participates in the AAD so a
	// post-hoc bit-flip is rejected by GCM verification.
	FlagCompressed byte = 1 << 0

	// KeySize is the AES-256 key length in bytes.
	KeySize = 32

	// NonceSize is the AES-GCM standard nonce size in bytes.
	NonceSize = 12

	// TagSize is the AES-GCM authentication tag size in bytes.
	TagSize = 16

	// versionBytes is the size of the envelope version field.
	versionBytes = 1
	// flagBytes is the size of the envelope flag field.
	flagBytes = 1
	// keyIDBytes is the size of the envelope key_id field (uint32 BE).
	keyIDBytes = 4

	// HeaderAADSize covers version + flag + key_id (the bytes that
	// participate in storage AAD, distinct from the full envelope header
	// which also carries nonce). Exposed as the input length of
	// HeaderAADBytes.
	HeaderAADSize = versionBytes + flagBytes + keyIDBytes // 6

	// HeaderSize covers version + flag + key_id + nonce, in that order.
	HeaderSize = HeaderAADSize + NonceSize // 18

	// EnvelopeOverhead is the per-value byte overhead introduced by the
	// envelope: HeaderSize + TagSize.
	EnvelopeOverhead = HeaderSize + TagSize // 34

	// ReservedKeyID is the cluster-wide "not bootstrapped" sentinel
	// (§5.1). Implementations MUST refuse to install or look up this
	// key_id.
	ReservedKeyID uint32 = 0

	// versionOffset / flagOffset / keyIDOffset / nonceOffset are the
	// byte offsets of each header field. Exposed as private constants so
	// envelope.go and cipher.go agree without duplicated magic numbers.
	versionOffset = 0
	flagOffset    = 1
	keyIDOffset   = 2
	nonceOffset   = 6
)

// Envelope is the parsed form of the §4.1 wire format.
type Envelope struct {
	Version byte
	Flag    byte
	KeyID   uint32
	Nonce   [NonceSize]byte
	// Body is the concatenation of ciphertext and the GCM tag, as produced
	// by AEAD.Seal. Length is plaintext_len + TagSize.
	Body []byte
}

// Encode serialises the envelope into a single byte slice using the §4.1
// wire format. The returned slice is freshly allocated.
//
// Encode validates the envelope at build time so a programmer error
// (uninitialised Version, truncated Body) fails here with a clear
// stack trace, rather than surfacing later as a confusing
// DecodeEnvelope or Cipher.Decrypt failure on the read side. Returns:
//   - ErrEnvelopeVersion if Version is not EnvelopeVersionV1.
//   - ErrEnvelopeShort   if Body is shorter than TagSize (every valid
//     body must contain at least the GCM tag).
func (e *Envelope) Encode() ([]byte, error) {
	if e.Version != EnvelopeVersionV1 {
		return nil, errors.Wrapf(ErrEnvelopeVersion,
			"encode: got 0x%02x, want 0x%02x", e.Version, EnvelopeVersionV1)
	}
	if len(e.Body) < TagSize {
		return nil, errors.Wrapf(ErrEnvelopeShort,
			"encode: body %d bytes, want >= %d", len(e.Body), TagSize)
	}
	out := make([]byte, HeaderSize+len(e.Body))
	out[versionOffset] = e.Version
	out[flagOffset] = e.Flag
	binary.BigEndian.PutUint32(out[keyIDOffset:keyIDOffset+keyIDBytes], e.KeyID)
	copy(out[nonceOffset:nonceOffset+NonceSize], e.Nonce[:])
	copy(out[HeaderSize:], e.Body)
	return out, nil
}

// DecodeEnvelope parses an envelope. It does NOT verify the GCM tag —
// authentication happens at Cipher.Decrypt time once the AAD is known.
//
// DecodeEnvelope copies Body so the returned Envelope does not alias src.
func DecodeEnvelope(src []byte) (*Envelope, error) {
	if len(src) < HeaderSize+TagSize {
		return nil, errors.Wrapf(ErrEnvelopeShort,
			"got %d bytes, want >= %d", len(src), HeaderSize+TagSize)
	}
	if src[versionOffset] != EnvelopeVersionV1 {
		return nil, errors.Wrapf(ErrEnvelopeVersion,
			"got 0x%02x, want 0x%02x", src[versionOffset], EnvelopeVersionV1)
	}
	e := &Envelope{
		Version: src[versionOffset],
		Flag:    src[flagOffset],
		KeyID:   binary.BigEndian.Uint32(src[keyIDOffset : keyIDOffset+4]),
	}
	copy(e.Nonce[:], src[nonceOffset:nonceOffset+NonceSize])
	body := make([]byte, len(src)-HeaderSize)
	copy(body, src[HeaderSize:])
	e.Body = body
	return e, nil
}

// HeaderAADBytes returns the first 6 bytes of the envelope header
// (version, flag, key_id) in their on-disk order. These bytes participate
// in the §4.1 storage-layer AAD (storage AAD = HeaderAADBytes ‖ pebble_key)
// and in the §4.2 raft-layer AAD's middle slice (raft AAD =
// "R" ‖ version ‖ key_id, computed by raft-layer callers in a later
// stage).
//
// Allocates HeaderAADSize bytes. Hot-path callers should prefer
// AppendHeaderAADBytes to reuse a buffer.
func HeaderAADBytes(version, flag byte, keyID uint32) []byte {
	return AppendHeaderAADBytes(make([]byte, 0, HeaderAADSize), version, flag, keyID)
}

// AppendHeaderAADBytes appends the same 6-byte header prefix (version,
// flag, key_id) onto dst and returns the extended slice. Allocation-free
// when dst already has HeaderAADSize spare capacity, which lets storage
// callers in later stages write the AAD directly into a pooled buffer
// alongside the per-record context (e.g., pebble_key) without an
// intermediate make().
func AppendHeaderAADBytes(dst []byte, version, flag byte, keyID uint32) []byte {
	var hdr [HeaderAADSize]byte
	hdr[0] = version
	hdr[1] = flag
	binary.BigEndian.PutUint32(hdr[2:], keyID)
	return append(dst, hdr[:]...)
}
