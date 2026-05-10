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
// reference implementation (CounterNonceFactory) with a
// writer-registry-backed factory that guarantees uniqueness across
// voters, learners, and historical replicas. The interface stays
// the same; only the construction changes. Implementations MUST
// NOT return the same nonce twice under the same DEK — AES-GCM
// nonce reuse is catastrophic (see encryption.Cipher doc).
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
// single branch.
//
// AAD binds the ciphertext to:
//
//   - the envelope header (envelope_version, flag, key_id),
//   - the encoded Pebble key (defeats cut-and-paste / version
//     substitution per §4.1 case 2/3),
//   - the on-disk value-header bytes (tombstone bit,
//     encryption_state, expireAt). Without binding the value-header,
//     a disk attacker could flip the tombstone bit or lower expireAt
//     to force GetAt/scan into a silent ErrKeyNotFound/expired
//     branch BEFORE any AEAD verification runs.
//
// The expireAt argument is the value the caller will write into the
// resulting storage entry; tombstone is hard-coded false because the
// encrypt path is never invoked for tombstone writes (deletes carry
// no plaintext and are emitted as cleartext by the store
// already).
func (s *pebbleStore) encryptForKey(pebbleKey, plaintext []byte, expireAt uint64) ([]byte, byte, error) {
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
	var hdr [valueHeaderSize]byte
	writeValueHeaderBytes(hdr[:], false /*tombstone*/, expireAt, encStateEncrypted)
	aad := buildStorageAAD(encryption.EnvelopeVersionV1, envelopeFlag, keyID, hdr[:], pebbleKey)
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

// decryptForKey is the read-side counterpart of encryptForKey.
// encState=cleartext returns the body verbatim after the
// envelope-rebadge guard below; encState=encrypted decodes the
// envelope, reconstructs the AAD over (header + value-header +
// pebble key), and unwraps via the cipher.
//
// A GCM tag mismatch surfaces as ErrEncryptedReadIntegrity. Callers
// MUST NOT silently translate this into "key not found" or "empty
// value" because that would let a disk attacker who flipped a tag
// bit (or any AAD-bound header field) silently corrupt reads.
//
// Reserved encryption_state values are rejected upstream in
// decodeValue, so this function only sees the two valid states.
//
// sv is the storedValue freshly decoded from the on-disk bytes; its
// Tombstone, ExpireAt, and EncState are reproduced into the AAD so
// any flip on disk fails GCM verification. Callers MUST run
// tombstone / expireAt visibility checks AFTER decrypt succeeds —
// the values they observe pre-decrypt are not yet authenticated.
func (s *pebbleStore) decryptForKey(pebbleKey []byte, sv storedValue, body []byte) ([]byte, error) {
	if sv.EncState == encStateCleartext {
		if err := s.rejectRebadgedEnvelope(pebbleKey, sv, body); err != nil {
			return nil, err
		}
		return body, nil
	}
	if s.cipher == nil {
		return nil, errors.New("store: encrypted value present but no cipher configured")
	}
	env, err := encryption.DecodeEnvelope(body)
	if err != nil {
		return nil, errors.Wrap(err, "store: decode envelope")
	}
	var hdr [valueHeaderSize]byte
	writeValueHeaderBytes(hdr[:], sv.Tombstone, sv.ExpireAt, sv.EncState)
	aad := buildStorageAAD(env.Version, env.Flag, env.KeyID, hdr[:], pebbleKey)
	plain, err := s.cipher.Decrypt(env.Body, aad, env.KeyID, env.Nonce[:])
	if err != nil {
		if errors.Is(err, encryption.ErrIntegrity) {
			return nil, errors.Wrap(
				errors.WithSecondaryError(ErrEncryptedReadIntegrity, err),
				"store: decrypt value")
		}
		return nil, errors.Wrap(err, "store: decrypt value")
	}
	// AES-GCM Open returns a nil dst slice for an empty plaintext;
	// upstream callers (notably ExistsAt) distinguish "key absent"
	// from "key present with empty value" via val != nil. Normalize
	// to a non-nil zero-length slice so an empty stored value
	// continues to satisfy ExistsAt → true.
	if plain == nil {
		plain = []byte{}
	}
	return plain, nil
}

// rejectRebadgedEnvelope is the cleartext-branch guard for the §4.1
// encryption-state rebadge attack. The on-disk encryption_state bit
// is not itself authenticated, so a disk attacker who flips it from
// 0b01 to 0b00 leaves the original envelope bytes in place and tells
// the read path to skip decryption. Without a guard the caller
// would silently receive raw envelope bytes as "plaintext".
//
// The guard runs an AEAD trial decrypt under each loaded DEK:
// reconstruct the AAD that the encrypt path would have produced, then
// call cipher.Decrypt over the body's ciphertext+tag region. If the
// GCM tag verifies the bytes are unambiguously a real envelope —
// only the DEK holder can produce a tag that survives this check, so
// legitimate cleartext has a 2⁻¹²⁸ false-positive probability. On
// any other outcome (parse failure, unknown key, tag mismatch) the
// row is treated as legitimate cleartext.
//
// AAD reconstruction substitutes the values the encrypt path always
// uses, instead of trusting the on-disk header bytes the attacker
// could have flipped:
//
//   - envelope_version = EnvelopeVersionV1 (the encrypt path's
//     fixed value; trusting on-disk would let a corrupted version
//     byte force the body through DecodeEnvelope's error path)
//   - flag             = 0 (Snappy compression is deferred; the
//     encrypt path's fixed value)
//   - tombstone        = false (the encrypt path never wraps
//     tombstones, so any on-disk tombstone bit on an encrypted
//     entry is necessarily attacker-supplied)
//
// The remaining AAD inputs come from disk and we enumerate plausible
// candidates:
//
//   - key_id: every loaded DEK (the attacker can rewrite the on-disk
//     byte, so we substitute candidates rather than trust env.KeyID)
//   - expireAt: {on-disk value, 0} (covers both no-flip and the
//     common "no-TTL write whose expireAt was rewritten by the
//     attacker" case)
//
// The body is sliced at fixed offsets rather than going through
// DecodeEnvelope so a corrupted version or flag byte cannot force
// the parse to fail and short-circuit the guard.
//
// Residual gap. The encryption_state bit cannot itself be AAD-bound
// because the AAD reconstruction depends on it for dispatch. An
// attacker who flips encState=0b01→0b00 and ALSO corrupts a byte the
// trial cannot reproduce from canonical inputs — specifically
// body[HeaderSize:] (ciphertext / tag) or expireAt when the original
// was a non-zero value the attacker also rewrote — falls through to
// the cleartext branch. The user receives garbage bytes (NOT the
// original plaintext: the attacker does not hold the DEK), so the
// gap is in integrity observability, not confidentiality. Stage 8
// closes this by moving encryption_state into authenticated MVCC
// metadata.
//
// No-op when the store has no cipher wired (legacy single-mode
// deployments have no rebadge attack surface) or when the body is
// too short to be an envelope.
func (s *pebbleStore) rejectRebadgedEnvelope(pebbleKey []byte, sv storedValue, body []byte) error {
	if s.cipher == nil {
		return nil
	}
	if len(body) < encryption.EnvelopeOverhead {
		return nil
	}
	nonce := body[encryption.HeaderAADSize:encryption.HeaderSize]
	ct := body[encryption.HeaderSize:]
	candidateExpireAts := []uint64{sv.ExpireAt}
	if sv.ExpireAt != 0 {
		candidateExpireAts = append(candidateExpireAts, 0)
	}
	for _, kid := range s.cipher.LoadedKeyIDs() {
		for _, candidateExpire := range candidateExpireAts {
			var hdr [valueHeaderSize]byte
			writeValueHeaderBytes(hdr[:], false /*canonical*/, candidateExpire, encStateEncrypted)
			aad := buildStorageAAD(encryption.EnvelopeVersionV1, 0 /*flag canonical*/, kid, hdr[:], pebbleKey)
			if _, err := s.cipher.Decrypt(ct, aad, kid, nonce); err == nil {
				return errors.Wrap(ErrEncryptedReadIntegrity,
					"store: cleartext-labelled value verifies as a relabeled envelope under a loaded DEK")
			}
		}
	}
	// No (DEK, candidate-expireAt) combination tag-matches. The body
	// is legitimate cleartext, an envelope under a retired DEK, or
	// fell into the documented residual gap above.
	return nil
}

// buildStorageAAD composes the §4.1 storage-envelope AAD with a
// single allocation. Layout:
//
//	envelope_version ‖ flag ‖ key_id ‖ value_header(9B) ‖ pebble_key
//
// Pre-sizing avoids re-allocation across the two appends below
// (AppendHeaderAADBytes + the value-header / pebble-key append).
// The value-header inclusion is what binds tombstone, encryption_state,
// and expireAt into the AAD so an on-disk flip of those fields fails
// GCM verification on read.
func buildStorageAAD(version, flag byte, keyID uint32, header, pebbleKey []byte) []byte {
	aad := make([]byte, 0, encryption.HeaderAADSize+len(header)+len(pebbleKey))
	aad = encryption.AppendHeaderAADBytes(aad, version, flag, keyID)
	aad = append(aad, header...)
	aad = append(aad, pebbleKey...)
	return aad
}
