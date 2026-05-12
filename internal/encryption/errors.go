package encryption

import "github.com/cockroachdb/errors"

var (
	// ErrUnknownKeyID is returned when a wrap/unwrap call references a key_id
	// that is not present in the Keystore. Surfaces as `unknown_key_id` on
	// the §9.2 elastickv_encryption_decrypt_failures_total counter.
	ErrUnknownKeyID = errors.New("encryption: unknown key_id")

	// ErrReservedKeyID is returned when a caller tries to install or use
	// key_id 0; that value is reserved cluster-wide as the
	// "not bootstrapped" sentinel per §5.1.
	ErrReservedKeyID = errors.New("encryption: key_id 0 is reserved as the not-bootstrapped sentinel")

	// ErrBadNonceSize indicates the nonce passed to Encrypt/Decrypt was not
	// exactly NonceSize bytes.
	ErrBadNonceSize = errors.New("encryption: nonce size invalid")

	// ErrBadKeySize indicates the DEK passed to Keystore.Set was not exactly
	// KeySize bytes (AES-256 requires 32).
	ErrBadKeySize = errors.New("encryption: DEK size invalid")

	// ErrIntegrity indicates a GCM tag mismatch on Decrypt — i.e., the
	// ciphertext was tampered with, the AAD does not match the one used at
	// Encrypt, or the wrong DEK is loaded. Per §4.1, callers MUST treat this
	// as a typed read error and never silently zero or retry.
	ErrIntegrity = errors.New("encryption: integrity check failed (GCM tag mismatch)")

	// ErrEnvelopeShort indicates DecodeEnvelope received fewer bytes than the
	// minimum envelope size (HeaderSize + TagSize).
	ErrEnvelopeShort = errors.New("encryption: envelope shorter than header+tag")

	// ErrEnvelopeVersion indicates DecodeEnvelope saw a version byte the
	// current build does not know how to parse. Reserved values per §11.3.
	ErrEnvelopeVersion = errors.New("encryption: unknown envelope version")

	// ErrNilKeystore indicates NewCipher was called with a nil Keystore.
	// Surfaced at construction time so a wiring mistake is caught
	// before the first Encrypt/Decrypt would otherwise nil-deref panic.
	ErrNilKeystore = errors.New("encryption: keystore is nil")

	// ErrKeyConflict indicates Keystore.Set was called with a keyID
	// already loaded under DIFFERENT key material. Replacing live key
	// bytes for an in-use key_id would render every envelope already
	// persisted under that id undecryptable, so Set fails closed
	// rather than silently overwriting. Set with the SAME bytes is
	// idempotent (returns nil) and does not raise this error.
	ErrKeyConflict = errors.New("encryption: key_id already loaded with different key material")

	// ErrUnsupportedFilesystem indicates the parent directory of the
	// sidecar cannot guarantee crash-durability of os.Rename via
	// fsync (typical on NFS, some FUSE mounts). Per §5.1 the
	// encryption package refuses to start in that situation rather
	// than silently degrading the durability guarantee. WriteSidecar
	// wraps any fsync-on-directory failure with this sentinel so the
	// Stage 5+ startup integration can errors.Is-match it.
	ErrUnsupportedFilesystem = errors.New("encryption: filesystem does not support durable directory sync (NFS, some FUSE mounts are unsupported)")

	// ErrSidecarActiveKeyMissing indicates the Sidecar has a non-zero
	// Active.{Storage,Raft} key_id that does not appear in the Keys
	// map. The two halves are written together by every rotation /
	// bootstrap path; an Active id without a corresponding wrapped
	// DEK is malformed input.
	ErrSidecarActiveKeyMissing = errors.New("encryption: sidecar active key_id has no entry in keys map")

	// ErrSidecarActivePurposeMismatch indicates the Sidecar has a
	// non-zero Active.{Storage,Raft} key_id pointing to a Keys entry
	// whose Purpose does not match the slot. e.g., active.storage=7
	// but Keys["7"].purpose == "raft". Crossed pointers would route
	// the wrong DEK into a purpose-specific encryption path after
	// restart or rotation, so the reader fails closed.
	ErrSidecarActivePurposeMismatch = errors.New("encryption: sidecar active key_id references a key with mismatched purpose")

	// ErrEncryptionApply is the §6.3 / §11.3 fatal-apply sentinel
	// surfaced by encryption FSM handlers (kv/fsm_encryption.go)
	// when one of the opcodes (0x03 registration, 0x04 bootstrap,
	// 0x05 rotation) cannot be applied — malformed payload,
	// KEK-unwrap failure, local-epoch rollback, sidecar write
	// failure, etc.
	//
	// The FSM packs this error in a haltApplyResponse value;
	// internal/raftengine/etcd's applyNormalCommitted recognises
	// the HaltApply interface, returns the error, and runLoop's
	// fatal-error path takes the process down without advancing
	// setApplied — the next restart must replay the entry.
	//
	// Defined here (and not in internal/raftengine/etcd) so
	// kv/fsm_encryption.go can errors.Mark its handler outputs
	// without importing the engine package, which would close
	// the kv ↔ engine cycle (engine_test imports kv as a fake
	// FSM).
	ErrEncryptionApply = errors.New("encryption: FSM apply failed; halting apply (see design §6.3)")
)
