package store

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/golang/snappy"
)

// ErrUnsupportedStoreForWriterRegistry is returned by
// WriterRegistryFor when the supplied MVCCStore is not backed by
// the in-tree Pebble implementation. Test fakes and alternate
// backends are detected at construction time so the failure mode
// is "binary refuses to start" rather than "FSM apply panics
// inside Raft loop".
var ErrUnsupportedStoreForWriterRegistry = errors.New("store: WriterRegistryFor requires a Pebble-backed MVCCStore")

// pebbleWriterRegistry adapts the Pebble-backed MVCCStore to the
// encryption.WriterRegistryStore interface used by the §6.3
// EncryptionApplier. The §4.1 writer-registry rows live under the
// `!encryption|writers|...` Pebble prefix — outside the MVCC
// namespace — so reads and writes bypass the MVCC layer entirely
// and hit the underlying *pebble.DB directly.
//
// Both Get and Set hold dbMu for read (RLock) only: the apply
// path that calls into the Applier is already serialised by
// kv/fsm.go's applyMu, so the registry row read-modify-write is
// safe without an exclusive Pebble-level lock. dbMu's RLock is
// the minimum required to protect against the Pebble DB pointer
// being swapped during snapshot restore.
//
// Set uses pebble.Sync so the row is durable before the FSM apply
// returns — without that, a crash between the FSM dispatch
// returning success and Pebble flushing the WAL would leave the
// Raft log claiming the entry applied but the registry row
// missing on the next restart. That would violate §4.1 case 2's
// monotonic-epoch invariant.
type pebbleWriterRegistry struct {
	s *pebbleStore
}

// GetRegistryRow looks up the value at key under the
// `!encryption|writers|...` namespace. A missing row returns
// (nil, false, nil) — NOT an error. Any other Pebble error
// (corruption, I/O fault) returns (nil, false, err) so the
// applier halts apply rather than silently treating the row as
// missing.
func (p *pebbleWriterRegistry) GetRegistryRow(key []byte) ([]byte, bool, error) {
	p.s.dbMu.RLock()
	defer p.s.dbMu.RUnlock()
	val, closer, err := p.s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.Wrap(err, "pebble: get writer registry row")
	}
	out := make([]byte, len(val))
	copy(out, val)
	if cerr := closer.Close(); cerr != nil {
		return nil, false, errors.Wrap(cerr, "pebble: close writer registry row reader")
	}
	return out, true, nil
}

// SetRegistryRow durably writes value at key. Idempotent at the
// (key, value) tuple level: writing the same bytes twice is
// observationally identical to writing them once. pebble.Sync
// blocks until the WAL is flushed to disk so the row survives a
// crash that lands after the FSM apply returns.
func (p *pebbleWriterRegistry) SetRegistryRow(key, value []byte) error {
	p.s.dbMu.RLock()
	defer p.s.dbMu.RUnlock()
	if err := p.s.db.Set(key, value, pebble.Sync); err != nil {
		return errors.Wrap(err, "pebble: set writer registry row")
	}
	return nil
}

// WriterRegistryFor returns an encryption.WriterRegistryStore
// backed by the Pebble DB underlying the supplied MVCCStore.
// Returns ErrUnsupportedStoreForWriterRegistry if the store is
// not a Pebble-backed MVCCStore — callers should treat this as a
// startup-fatal misconfiguration (the only in-tree non-Pebble
// MVCCStore is the in-memory test fake, which has no encryption
// requirements).
func WriterRegistryFor(s MVCCStore) (encryption.WriterRegistryStore, error) {
	ps, ok := s.(*pebbleStore)
	// A typed-nil (*pebbleStore)(nil) passes the assertion with
	// ok=true but ps==nil; the adapter would then nil-deref on
	// first call. Reject both shapes at construction so the
	// failure mode is "binary refuses to start" rather than
	// "FSM apply panics deep in Raft loop".
	if !ok || ps == nil {
		return nil, errors.WithStack(ErrUnsupportedStoreForWriterRegistry)
	}
	return &pebbleWriterRegistry{s: ps}, nil
}

// ErrEncryptedReadIntegrity wraps encryption.ErrIntegrity for storage-layer
// callers (Get / scan / iterator). Per design §4.1, callers MUST treat this
// as a typed read error and never silently zero the value or skip the row.
//
// Callers can disambiguate it from any other read error with errors.Is.
var ErrEncryptedReadIntegrity = errors.New("store: encrypted value failed integrity check (GCM tag mismatch); refusing to surface plaintext")

// ErrEncryptedReadCompression is returned when an authenticated envelope is
// marked as Snappy-compressed but its decrypted body is not a valid Snappy
// stream. Disk corruption normally fails GCM first; reaching this sentinel
// means a writer produced a malformed authenticated payload, so reads fail
// closed instead of surfacing the compressed bytes as user data.
var ErrEncryptedReadCompression = errors.New("store: authenticated encrypted value contains an invalid Snappy payload")

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

// StorageEnvelopeActive reports whether the §7.1 Phase 1 cutover has
// fired on this node (the Stage 6D-4 sidecar field of the same name,
// surfaced to the storage layer via WithStorageEnvelopeGate). A
// return value of false forces every Put to write cleartext even
// when a cipher + active DEK are wired; true allows the existing
// activeStorageKeyID-driven envelope emit to proceed.
//
// The contract intentionally separates "encryption is provisioned"
// (ActiveStorageKeyID has a DEK to use) from "encryption is
// activated for new writes" (the cutover has fired). The two
// signals diverge during the §7.1 Phase 0 → Phase 1 window: every
// node in the cluster has the DEK installed, but the cluster has
// not yet quiesced the cutover Raft entry that flips the bit on
// every replica simultaneously.
//
// Reads ignore this gate — once an on-disk version's
// encryption_state bit is 0b01, the read path always invokes the
// cipher to unwrap regardless of the gate's current value. A
// cluster that flipped from active back to inactive (not a path
// the design supports) would still decrypt old envelopes correctly.
type StorageEnvelopeActive func() bool

// StorageRegistered reports whether this process load's §4.1 writer
// registration has committed for the currently-active storage DEK
// (Stage 7a-2). It gates only the DIRECT write path: when it returns
// false while the envelope would otherwise encrypt, encryptForKey
// refuses to emit a nonce and returns ErrWriterNotRegistered so the
// self-originated caller (catalog bootstrap Save, admin snapshot,
// migration) retries until registration lands rather than emitting an
// envelope ahead of its writer-registry row.
//
// The FSM-apply path never consults it — see WithStorageRegistrationGate.
type StorageRegistered func() bool

// ErrWriterNotRegistered is returned by the direct write path when the
// §7.1 storage envelope is active but this process load has not yet
// confirmed its §4.1 writer registration for the active storage DEK
// (Stage 7a-2). It is a transient, retryable condition: the caller
// should retry once registration commits (the barrier closes). Callers
// disambiguate it with errors.Is so a fail-closed pre-registration
// write is never mistaken for a permanent storage fault.
var ErrWriterNotRegistered = errors.New("store: storage envelope active but writer not yet registered; refusing to emit nonce before registration")

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

// WithStorageEnvelopeGate wires the Stage 6D-5 §6.2 cutover gate
// in front of the existing envelope-emit path. After this option,
// encryptForKey writes cleartext when active() returns false even
// if a cipher and active DEK are present — exactly the §7.1
// Phase 0 / Phase 1 split described in the parent encryption
// design doc.
//
// Passing nil is a no-op: the store stays in the pre-6D-5
// "encrypt whenever activeKeyID returns a DEK" posture used by
// existing test fixtures and by production deployments that have
// not yet run the EnableStorageEnvelope RPC (lands in 6D-6).
// Operators must thread this option in alongside WithEncryption
// to actually opt into the cutover semantics.
//
// The gate is consulted on every Put. Reads never consult it —
// on-disk versions with `encryption_state == 0b01` always go
// through the cipher regardless of the gate, so a cluster mid-
// cutover (some versions cleartext, others encrypted) stays
// readable.
func WithStorageEnvelopeGate(active StorageEnvelopeActive) PebbleStoreOption {
	return func(s *pebbleStore) {
		if active == nil {
			return
		}
		s.storageEnvelopeActive = active
	}
}

// WithStorageRegistrationGate wires the Stage 7a-2 §4.1 registration
// gate in front of the envelope-emit path on the DIRECT write path
// only. After this option, when the envelope would encrypt
// (cipher + active DEK + StorageEnvelopeActive all true) but
// registered() reports false, the direct path (PutAt / ExpireAt /
// ApplyMutations) returns ErrWriterNotRegistered instead of emitting a
// nonce. The FSM-apply path (ApplyMutationsRaft) passes
// gateRegistration=false into encryptForKey and is never gated — see
// design §1: replicated apply must stay deterministic and may run
// before this node's own registration entry commits, so fail-closing
// it would halt the apply loop (and deadlock a node whose storage
// entry is ordered before its registration entry).
//
// Passing nil is a no-op: the store keeps the pre-7a-2 posture where
// the direct path emits envelopes as soon as the cutover gate is open,
// used by existing fixtures and by deployments that have not wired the
// writer registry. Operators thread cache.Registered in via main.go.
func WithStorageRegistrationGate(registered StorageRegistered) PebbleStoreOption {
	return func(s *pebbleStore) {
		if registered == nil {
			return
		}
		s.storageRegistered = registered
	}
}

// resolveEnvelopeEmit folds the cipher-wired / active-DEK / §6.2
// cutover-gate checks and the Stage 7a-2 §4.1 registration gate into a
// single decision for encryptForKey. It returns (keyID, true, nil) when
// the value should be wrapped, (0, false, nil) when it should pass
// through cleartext, and (0, false, ErrWriterNotRegistered) when the
// direct path would encrypt but this load's writer registration has not
// yet committed.
//
// The cutover gate (storageEnvelopeActive) stays unconsulted when not
// wired so pre-6D-6 fixtures keep working; the registration gate
// (storageRegistered) likewise no-ops when not wired, preserving the
// pre-7a-2 posture. gateRegistration is false on the FSM-apply path so
// replicated apply is never blocked (design §1).
func (s *pebbleStore) resolveEnvelopeEmit(gateRegistration bool) (keyID uint32, encrypt bool, err error) {
	if s.cipher == nil || s.activeStorageKeyID == nil {
		return 0, false, nil
	}
	id, ok := s.activeStorageKeyID()
	if !ok {
		return 0, false, nil
	}
	// Stage 6D-5 §6.2 cutover gate. When the gate is wired AND reports
	// false (`sidecar.StorageEnvelopeActive == false`), fall through to
	// cleartext even though a DEK is active. This is the only
	// correctness-critical site for the §7.1 Phase 0 → Phase 1 split: a
	// pre-cutover Put that emitted an envelope would advance the
	// writer-registry nonce counter before every replica had agreed the
	// cutover applied, opening the cross-replica nonce-divergence race
	// the §6.4 atomicity contract is built to close.
	if s.storageEnvelopeActive != nil && !s.storageEnvelopeActive() {
		return 0, false, nil
	}
	// Stage 7a-2 §4.1 registration gate. Reaching here means we WILL
	// emit an envelope (cipher + active DEK + cutover all confirmed). On
	// the direct path, refuse to emit the nonce until this load's writer
	// registration has committed — otherwise a self-originated write
	// (catalog bootstrap Save) could advance the nonce counter before the
	// writer-registry row exists, the exact pre-registration emission
	// 7a-2 closes. The FSM-apply path passes gateRegistration=false and
	// is never blocked here (design §1).
	if gateRegistration && s.storageRegistered != nil && !s.storageRegistered() {
		return 0, false, errors.WithStack(ErrWriterNotRegistered)
	}
	return id, true, nil
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
//
// gateRegistration selects the Stage 7a-2 §4.1 registration enforcement
// (design §1): true on the DIRECT write path (PutAt / ExpireAt /
// ApplyMutations), where a self-originated encrypted write must not
// emit a nonce before this load's writer registration commits; false
// on the FSM-apply path (ApplyMutationsRaft), which must stay
// deterministic and may legitimately encrypt during the pre-registration
// window. When true and the registration gate is wired and reports
// false, returns ErrWriterNotRegistered without emitting a nonce.
//
// The encrypt-vs-cleartext decision (and the registration gate) lives
// in resolveEnvelopeEmit so this function stays within the cyclop
// budget.
func (s *pebbleStore) encryptForKey(pebbleKey, plaintext []byte, expireAt uint64, gateRegistration bool) ([]byte, byte, error) {
	keyID, encrypt, err := s.resolveEnvelopeEmit(gateRegistration)
	if err != nil {
		return nil, 0, err
	}
	if !encrypt {
		return plaintext, encStateCleartext, nil
	}
	nonceArr, err := s.nonceFactory.Next()
	if err != nil {
		return nil, 0, errors.Wrap(err, "store: nonce factory")
	}
	nonce := nonceArr[:]
	payload, envelopeFlag := compressForEncryption(plaintext)
	var hdr [valueHeaderSize]byte
	writeValueHeaderBytes(hdr[:], false /*tombstone*/, expireAt, encStateEncrypted)
	aad := buildStorageAAD(encryption.EnvelopeVersionV1, envelopeFlag, keyID, hdr[:], pebbleKey)
	ciphertextAndTag, err := s.cipher.Encrypt(payload, aad, keyID, nonce)
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
	if env.Flag&encryption.FlagCompressed != 0 {
		plain, err = snappy.Decode(nil, plain)
		if err != nil {
			return nil, errors.Wrap(
				errors.WithSecondaryError(ErrEncryptedReadCompression, err),
				"store: decompress encrypted value")
		}
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

// compressForEncryption applies §6.4's compress-then-encrypt policy. Snappy
// output is used only when it is strictly smaller than the original bytes;
// already-compressed and small payloads stay uncompressed so encryption never
// increases CPU and storage cost for a larger intermediate representation.
func compressForEncryption(plaintext []byte) ([]byte, byte) {
	compressed := snappy.Encode(nil, plaintext)
	if len(compressed) >= len(plaintext) {
		return plaintext, 0
	}
	return compressed, encryption.FlagCompressed
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
//   - flag: both supported values (uncompressed and Snappy-compressed),
//     because the attacker can rewrite the on-disk flag byte
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
			for _, candidateFlag := range []byte{0, encryption.FlagCompressed} {
				var hdr [valueHeaderSize]byte
				writeValueHeaderBytes(hdr[:], false /*canonical*/, candidateExpire, encStateEncrypted)
				aad := buildStorageAAD(encryption.EnvelopeVersionV1, candidateFlag, kid, hdr[:], pebbleKey)
				if _, err := s.cipher.Decrypt(ct, aad, kid, nonce); err == nil {
					return errors.Wrap(ErrEncryptedReadIntegrity,
						"store: cleartext-labelled value verifies as a relabeled envelope under a loaded DEK")
				}
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
