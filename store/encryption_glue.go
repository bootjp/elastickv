package store

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

// ErrEncryptedReadIntegrity wraps encryption.ErrIntegrity for storage-layer
// callers (Get / scan / iterator). Per design §4.1, callers MUST treat this
// as a typed read error and never silently zero the value or skip the row.
//
// Callers can disambiguate it from any other read error with errors.Is.
var ErrEncryptedReadIntegrity = errors.New("store: encrypted value failed integrity check (GCM tag mismatch); refusing to surface plaintext")

// NonceFactory produces unique 12-byte AES-GCM nonces for the storage
// envelope (§4.1). The factory is responsible for the cluster-wide
// uniqueness invariant across `(node_id, local_epoch, write_count)` —
// the storage layer just calls Next() and uses what comes back.
//
// Stage 7 of the encryption rollout will replace the in-tree
// reference implementation (deterministicCounterNonce, defined in the
// _test.go helper) with a writer-registry-backed factory that
// guarantees uniqueness across voters, learners, and historical
// replicas. The interface stays the same; only the construction
// changes. Implementations MUST NOT return the same nonce twice
// under the same DEK — AES-GCM nonce reuse is catastrophic
// (see encryption.Cipher doc).
type NonceFactory interface {
	Next() ([encryption.NonceSize]byte, error)
}

// ActiveStorageKeyID reports the currently-active storage DEK
// identifier. The bool is false when no storage DEK is active (i.e.
// the cluster has not run Phase 1 of the §7.1 rollout yet) — in that
// case the storage layer writes cleartext as if no cipher were
// configured. Stage 5/6 wires this from the sidecar's Active.Storage
// slot; Stage 2 takes it as a closure so test code can flip it
// independently.
type ActiveStorageKeyID func() (uint32, bool)

// WithEncryption configures the pebble-backed store to wrap every
// committed value in the §4.1 storage envelope.
//
// All three arguments must be non-nil. activeKeyID is called on
// every Put — when it returns ok=false the store writes cleartext
// (encryption_state = 0b00) even though a cipher is wired, matching
// the §7.1 Phase 0 / Phase 1 split where capability is provisioned
// before activation. Reads that observe encryption_state = 0b01
// always go through the cipher regardless of activeKeyID, so a
// cluster mid-cutover stays readable.
//
// Calling WithEncryption with any nil argument is a no-op (the
// store stays in legacy cleartext-only mode). This keeps the
// option backwards-compatible with every existing NewPebbleStore
// caller and keeps the Stage 2 wiring trivially reversible.
func WithEncryption(cipher *encryption.Cipher, nf NonceFactory, activeKeyID ActiveStorageKeyID) PebbleStoreOption {
	return func(s *pebbleStore) {
		if cipher == nil || nf == nil || activeKeyID == nil {
			return
		}
		s.cipher = cipher
		s.nonceFactory = nf
		s.activeStorageKeyID = activeKeyID
	}
}

// encryptForKey wraps plaintext in the §4.1 storage envelope when an
// encryption key is active for the storage purpose. Returns
// (plaintext, encStateCleartext, nil) when encryption is disabled or
// no DEK is currently active so the cipher=nil fast path stays a
// single branch. AAD binds the ciphertext to the envelope header AND
// the encoded Pebble key, defeating cut-and-paste / version
// substitution per §4.1 case 2/3.
func (s *pebbleStore) encryptForKey(pebbleKey, plaintext []byte) ([]byte, byte, error) {
	if s.cipher == nil || s.activeStorageKeyID == nil {
		return plaintext, encStateCleartext, nil
	}
	keyID, ok := s.activeStorageKeyID()
	if !ok {
		return plaintext, encStateCleartext, nil
	}
	nonceArr, err := s.nonceFactory.Next()
	if err != nil {
		return nil, 0, errors.Wrap(err, "store: nonce factory")
	}
	nonce := nonceArr[:]
	// flag = 0: Snappy compression deferred to Stage 9 per design §4.1.
	const envelopeFlag byte = 0
	aad := encryption.AppendHeaderAADBytes(nil,
		encryption.EnvelopeVersionV1, envelopeFlag, keyID)
	aad = append(aad, pebbleKey...)
	ciphertextAndTag, err := s.cipher.Encrypt(plaintext, aad, keyID, nonce)
	if err != nil {
		return nil, 0, errors.Wrap(err, "store: encrypt value")
	}
	env := encryption.Envelope{
		Version: encryption.EnvelopeVersionV1,
		Flag:    envelopeFlag,
		KeyID:   keyID,
		Nonce:   nonceArr,
		Body:    ciphertextAndTag,
	}
	encoded, err := env.Encode()
	if err != nil {
		return nil, 0, errors.Wrap(err, "store: encode envelope")
	}
	return encoded, encStateEncrypted, nil
}

// decryptForKey is the read-side counterpart. encState=0 returns the
// body verbatim; encState=1 decodes the envelope, recomputes the AAD
// against the supplied pebbleKey, and unwraps via the cipher. A GCM
// tag mismatch surfaces as ErrEncryptedReadIntegrity — callers MUST
// NOT silently translate this into "key not found" or "empty value"
// because that would let a disk attacker who flipped a tag bit
// silently corrupt reads.
//
// Reserved encryption_state values are rejected upstream in
// decodeValue, so this function only sees the two valid states.
func (s *pebbleStore) decryptForKey(pebbleKey []byte, encState byte, body []byte) ([]byte, error) {
	if encState == encStateCleartext {
		return body, nil
	}
	if s.cipher == nil {
		return nil, errors.New("store: encrypted value present but no cipher configured")
	}
	env, err := encryption.DecodeEnvelope(body)
	if err != nil {
		return nil, errors.Wrap(err, "store: decode envelope")
	}
	aad := encryption.AppendHeaderAADBytes(nil, env.Version, env.Flag, env.KeyID)
	aad = append(aad, pebbleKey...)
	plain, err := s.cipher.Decrypt(env.Body, aad, env.KeyID, env.Nonce[:])
	if err != nil {
		if errors.Is(err, encryption.ErrIntegrity) {
			return nil, errors.Wrap(
				errors.WithSecondaryError(ErrEncryptedReadIntegrity, err),
				"store: decrypt value")
		}
		return nil, errors.Wrap(err, "store: decrypt value")
	}
	return plain, nil
}
