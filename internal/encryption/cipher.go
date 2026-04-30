package encryption

import (
	"crypto/cipher"

	"github.com/cockroachdb/errors"
)

// Cipher is the AES-256-GCM primitive over a Keystore.
//
// Cipher does NOT compose AAD — callers in store/ (§4.1 AAD) and
// internal/raftengine/etcd/ (§4.2 AAD) supply the full AAD bytes. This
// keeps the cipher narrow and lets each layer choose the right AAD
// without baking storage/raft assumptions into the foundation package.
//
// AES key expansion and GCM initialization happen once per DEK at
// Keystore.Set time; the hot path only needs an atomic.Pointer load
// and a Seal/Open call.
type Cipher struct {
	keystore *Keystore
}

// NewCipher returns a Cipher backed by ks.
//
// Returns ErrNilKeystore if ks is nil. Catching this at construction
// time turns a wiring mistake into a typed error during process
// startup or DEK rotation, rather than a nil-deref panic on the first
// Encrypt/Decrypt — important for the dynamic dependency-wiring paths
// where the encryption stack may be re-initialised after a sidecar
// resync (§5.5) or rotation (§5.2).
func NewCipher(ks *Keystore) (*Cipher, error) {
	if ks == nil {
		return nil, errors.WithStack(ErrNilKeystore)
	}
	return &Cipher{keystore: ks}, nil
}

// Encrypt produces (ciphertext ‖ tag) for plaintext under the DEK
// identified by keyID and the supplied nonce. aad is treated verbatim.
//
// Constraints:
//   - keyID must not be ReservedKeyID; otherwise ErrReservedKeyID.
//   - nonce must be NonceSize bytes; otherwise ErrBadNonceSize.
//   - keyID must be present in the Keystore; otherwise ErrUnknownKeyID.
//
// CRITICAL: callers MUST NOT reuse the same (keyID, nonce) pair with
// any two distinct plaintexts. Nonce reuse under AES-GCM is
// catastrophic: it leaks the XOR of the two plaintexts and enables
// authentication-key recovery. The §4.1 nonce construction
// (node_id ‖ local_epoch ‖ write_count) is designed to make reuse
// impossible by construction; do not bypass it with crypto/rand or
// external counters without re-deriving the same uniqueness proof.
//
// The returned slice has length len(plaintext) + TagSize. It is
// freshly allocated; the caller may retain it indefinitely.
func (c *Cipher) Encrypt(plaintext, aad []byte, keyID uint32, nonce []byte) ([]byte, error) {
	aead, err := c.aeadFor(keyID, nonce)
	if err != nil {
		return nil, err
	}
	return aead.Seal(nil, nonce, plaintext, aad), nil
}

// Decrypt verifies and decrypts (ciphertext ‖ tag) using the DEK
// identified by keyID, the supplied nonce, and the same aad bytes that
// were passed to Encrypt.
//
// On GCM tag mismatch, Decrypt returns an error wrapping ErrIntegrity.
// Per §4.1, callers MUST treat this as a typed read error and never
// silently zero or retry. The original Open error is attached as a
// secondary cause for diagnostic logging.
func (c *Cipher) Decrypt(ciphertextAndTag, aad []byte, keyID uint32, nonce []byte) ([]byte, error) {
	aead, err := c.aeadFor(keyID, nonce)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertextAndTag, aad)
	if err != nil {
		// Wrap ErrIntegrity (the typed error callers match via errors.Is)
		// and attach the original GCM Open error as a secondary cause for
		// diagnostic logging. Per §4.1, callers MUST treat this as a
		// typed read error and never silently zero or retry.
		return nil, errors.Wrap(
			errors.WithSecondaryError(ErrIntegrity, err),
			"encryption: aead.Open",
		)
	}
	return plaintext, nil
}

// aeadFor validates keyID and nonce length, then returns the
// pre-initialized AEAD from the keystore. The hot path here is a single
// atomic.Pointer load + a map lookup; AES key expansion happened once
// at Keystore.Set time.
func (c *Cipher) aeadFor(keyID uint32, nonce []byte) (cipher.AEAD, error) {
	if keyID == ReservedKeyID {
		return nil, errors.WithStack(ErrReservedKeyID)
	}
	if len(nonce) != NonceSize {
		return nil, errors.Wrapf(ErrBadNonceSize, "got %d bytes, want %d", len(nonce), NonceSize)
	}
	aead, ok := c.keystore.AEAD(keyID)
	if !ok {
		return nil, errors.Wrapf(ErrUnknownKeyID, "key_id=%d", keyID)
	}
	return aead, nil
}
