package encryption

import (
	"crypto/aes"
	"crypto/cipher"

	"github.com/cockroachdb/errors"
)

// Cipher is the AES-256-GCM primitive over a Keystore.
//
// Cipher does NOT compose AAD — callers in store/ (§4.1 AAD) and
// internal/raftengine/etcd/ (§4.2 AAD) supply the full AAD bytes. This
// keeps the cipher narrow and lets each layer choose the right AAD
// without baking storage/raft assumptions into the foundation package.
type Cipher struct {
	keystore *Keystore
}

// NewCipher returns a Cipher backed by ks.
func NewCipher(ks *Keystore) *Cipher {
	return &Cipher{keystore: ks}
}

// Encrypt produces (ciphertext ‖ tag) for plaintext under the DEK
// identified by keyID and the supplied nonce. aad is treated verbatim.
//
// Constraints:
//   - keyID must not be ReservedKeyID; otherwise ErrReservedKeyID.
//   - nonce must be NonceSize bytes; otherwise ErrBadNonceSize.
//   - keyID must be present in the Keystore; otherwise ErrUnknownKeyID.
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

// aeadFor returns a per-call AEAD bound to the DEK looked up from the
// keystore, after validating keyID and nonce length.
func (c *Cipher) aeadFor(keyID uint32, nonce []byte) (cipher.AEAD, error) {
	if keyID == ReservedKeyID {
		return nil, errors.WithStack(ErrReservedKeyID)
	}
	if len(nonce) != NonceSize {
		return nil, errors.Wrapf(ErrBadNonceSize, "got %d bytes, want %d", len(nonce), NonceSize)
	}
	dek, ok := c.keystore.Get(keyID)
	if !ok {
		return nil, errors.Wrapf(ErrUnknownKeyID, "key_id=%d", keyID)
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, errors.Wrap(err, "encryption: aes.NewCipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "encryption: cipher.NewGCM")
	}
	return aead, nil
}
