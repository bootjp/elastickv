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
)
