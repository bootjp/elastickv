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
//     branch BEFORE any AEAD verification runs (PR742 codex P1).
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

// decryptForKey is the read-side counterpart. encState=0 returns the
// body verbatim (with the envelope-shaped fingerprint check below);
// encState=1 decodes the envelope, recomputes the AAD (header +
// value-header + pebble key), and unwraps via the cipher. A GCM tag
// mismatch surfaces as ErrEncryptedReadIntegrity — callers MUST NOT
// silently translate this into "key not found" or "empty value"
// because that would let a disk attacker who flipped a tag bit (or
// any AAD-bound header field) silently corrupt reads.
//
// Reserved encryption_state values are rejected upstream in
// decodeValue, so this function only sees the two valid states.
//
// Cleartext-rebadge guard (PR742 codex P1, round-3). A disk
// attacker who flips encryption_state from 0b01 to 0b00 leaves the
// envelope bytes in place but tells the read path to skip
// decryption — so the caller would silently receive raw envelope
// bytes as "plaintext". When a cipher is wired, we therefore run
// the body through DecodeEnvelope on the cleartext branch too: if
// it parses as a well-formed envelope AND its key_id is loaded in
// the keystore (i.e. the bytes are almost certainly a relabeled
// envelope rather than a coincidence), we reject as
// ErrEncryptedReadIntegrity. False positives on legitimate
// cleartext are bounded by the joint probability of envelope-shape
// match + a 32-bit key_id collision with a loaded DEK.
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
	// continues to satisfy ExistsAt → true (PR742 codex P1 round-4).
	if plain == nil {
		plain = []byte{}
	}
	return plain, nil
}

// rejectRebadgedEnvelope is the cleartext-branch guard for the §4.1
// encState rebadge attack. The on-disk encryption_state bit is not
// itself authenticated (PR742 codex P1 family — Stage 8 plans to
// move it into authenticated MVCC metadata), so a disk attacker who
// flips it from 0b01 to 0b00 leaves the original envelope bytes in
// place and tells the read path to skip decryption. Without a
// guard, the caller would silently receive raw envelope bytes as
// "plaintext".
//
// The detection runs an actual AEAD trial decrypt: rebuild the
// AAD that the envelope WOULD have used had it been written under
// encState=0b01, then call cipher.Decrypt over the parsed envelope
// body. If the GCM tag verifies the bytes are unambiguously a
// real envelope — only the holder of the DEK can produce a tag
// that survives this check, so legitimate cleartext data has a
// 2⁻¹²⁸ false-positive probability (negligible). On any other
// outcome (parse failure, unknown key, tag mismatch) we treat the
// row as legitimate cleartext.
//
// PR742 review history:
//   - round-3: "envelope-shape AND key_id loaded" — bypassable via
//     key_id rewrite to an unloaded value (codex round-4).
//   - round-5: "envelope-shape only" — false-positives on
//     legitimate binary cleartext that happens to start with 0x01
//     and parse the length/version/flag/nonce checks (codex
//     round-6).
//   - round-7 (this change): tag verification under reconstructed
//     encrypted-state AAD — no false positives, catches the
//     primary key_id-unchanged rebadge. The kid-rewrite-to-loaded-
//     other-DEK variant still falls through (AAD mismatch on the
//     foreign DEK), but the user observes ciphertext garbage that
//     application code rejects downstream rather than plaintext —
//     deferred to Stage 8.
//
// No-op when the store has no cipher wired (legacy single-mode
// deployments cannot have rebadge attacks) or when the body is too
// short to be an envelope.
func (s *pebbleStore) rejectRebadgedEnvelope(pebbleKey []byte, sv storedValue, body []byte) error {
	if s.cipher == nil {
		return nil
	}
	if len(body) < encryption.EnvelopeOverhead {
		return nil
	}
	env, err := encryption.DecodeEnvelope(body)
	if err != nil {
		// Body does not parse as an envelope; treat as legitimate
		// cleartext.
		return nil //nolint:nilerr // intentional: parse failure means "not an envelope"
	}
	// Reconstruct the AAD as if the row carried encState=encrypted
	// and trial-decrypt under each loaded DEK + candidate-header
	// pair.
	//
	// Header reconstruction. The encrypt path always writes
	// tombstone=false (deletes never carry plaintext, so the encrypt
	// helper is never reached on a tombstone). Trusting the on-disk
	// tombstone bit here would let an attacker who flipped
	// encryption_state AND tombstone slip through the trial: the AAD
	// would carry tombstone=true while the encrypt-time AAD used
	// tombstone=false (PR742 codex P1 round-7). We canonicalise to
	// false here regardless of what's on disk.
	//
	// expireAt is harder: the encrypt-time value is whatever the
	// caller supplied, and we have nothing on disk to compare
	// against beyond the (potentially tampered) sv.ExpireAt. Try
	// the on-disk value (covers the encState-only flip case, which
	// keeps the original expireAt intact) and also expireAt=0
	// (covers no-TTL writes whose expireAt was rewritten by the
	// attacker — by far the common case). The residual
	// "encState flip + expireAt rewritten when original was non-zero"
	// is a known limitation that Stage 8's authenticated MVCC
	// metadata bit closes deterministically.
	//
	// key_id reconstruction. The encrypt-time AAD included the
	// *original* envelope key_id; attackers can rewrite that field
	// on disk, so we substitute each candidate kid into the AAD's
	// HeaderAAD section rather than trusting env.KeyID.
	candidateExpireAts := []uint64{sv.ExpireAt}
	if sv.ExpireAt != 0 {
		candidateExpireAts = append(candidateExpireAts, 0)
	}
	for _, kid := range s.cipher.LoadedKeyIDs() {
		for _, candidateExpire := range candidateExpireAts {
			var hdr [valueHeaderSize]byte
			writeValueHeaderBytes(hdr[:], false /*canonical*/, candidateExpire, encStateEncrypted)
			aad := buildStorageAAD(env.Version, env.Flag, kid, hdr[:], pebbleKey)
			if _, err := s.cipher.Decrypt(env.Body, aad, kid, env.Nonce[:]); err == nil {
				return errors.Wrap(ErrEncryptedReadIntegrity,
					"store: cleartext-labelled value verifies as a relabeled envelope under a loaded DEK")
			}
		}
	}
	// No (DEK, candidate-expireAt) combination produces a tag match.
	// Body is either legitimate cleartext that happens to look
	// envelope-shaped, an envelope under a retired DEK, an envelope
	// whose original expireAt was non-zero AND has been rewritten
	// (Stage 8 closes this), or a partially-tampered envelope. The
	// first case is the legitimate-mixed-mode outcome the round-6
	// codex finding required us to preserve.
	return nil
}

// buildStorageAAD composes the §4.1 storage-envelope AAD with a
// single allocation. Layout:
//
//	envelope_version ‖ flag ‖ key_id ‖ value_header(9B) ‖ pebble_key
//
// Pre-sizing avoids the double-allocation gemini medium flagged on
// PR742 round-1 (AppendHeaderAADBytes alloc + the subsequent
// append). The value-header inclusion was added in round-2 in
// response to the codex P1 finding that flipping tombstone / expireAt
// would otherwise bypass GCM verification.
func buildStorageAAD(version, flag byte, keyID uint32, header, pebbleKey []byte) []byte {
	aad := make([]byte, 0, encryption.HeaderAADSize+len(header)+len(pebbleKey))
	aad = encryption.AppendHeaderAADBytes(aad, version, flag, keyID)
	aad = append(aad, header...)
	aad = append(aad, pebbleKey...)
	return aad
}
