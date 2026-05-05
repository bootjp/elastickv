package kek

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"os"
	"runtime"

	"github.com/cockroachdb/errors"
)

// ErrInsecureKEKFile is returned by NewFileWrapper when the KEK file
// permission bits permit group or other access. Loading such a file
// would silently weaken the at-rest encryption boundary on a
// multi-user host (any local user could read the master key bytes),
// so the wrapper fails closed rather than warning. Owner-only modes
// (0o400 / 0o600) are accepted; anything with bits in 0o077 is not.
var ErrInsecureKEKFile = errors.New("kek: file is group/world-accessible; require owner-only mode")

const (
	// fileKEKSize is the on-disk KEK length: 32 bytes for AES-256.
	fileKEKSize = 32

	// fileNonceSize is the AES-GCM nonce length the FileWrapper produces.
	// Same as the package-level encryption.NonceSize constant; redeclared
	// here to keep this file dependency-free of the parent package.
	fileNonceSize = 12

	// fileTagSize is the AES-GCM tag length the FileWrapper produces.
	fileTagSize = 16
)

// FileWrapper wraps DEKs using AES-256-GCM under a KEK read from a file
// at construction time.
//
// Suitable for tests, single-host clusters, and deployments that store
// the KEK on a sealed tmpfs volume. Production deployments should
// prefer a KMS-backed Wrapper (Stage 9: aws_kms.go, gcp_kms.go,
// vault.go); see §5.1 for the recommended provider ordering.
type FileWrapper struct {
	aead cipher.AEAD
	path string
}

// NewFileWrapper reads a KEK from path. The file must contain exactly
// 32 bytes (an AES-256 key). Any other length returns an error rather
// than silently padding or truncating.
//
// On unix, the file's permission bits MUST be owner-only (no group or
// other access bits set, i.e. mode & 0o077 == 0). A misconfigured
// 0o644 or 0o666 KEK file would let any local user read the master
// key on a multi-user host, defeating the entire at-rest encryption
// boundary — NewFileWrapper fails closed with ErrInsecureKEKFile
// rather than logging a warning. Windows has a fundamentally
// different permission model and is not gated.
func NewFileWrapper(path string) (*FileWrapper, error) {
	if err := checkSecureKEKMode(path); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path) //nolint:gosec // path comes from operator config; mode pre-checked above
	if err != nil {
		return nil, errors.Wrapf(err, "kek: read %q", path)
	}
	if len(raw) != fileKEKSize {
		return nil, errors.Errorf("kek: file %q is %d bytes, want exactly %d",
			path, len(raw), fileKEKSize)
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, errors.Wrap(err, "kek: aes.NewCipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "kek: cipher.NewGCM")
	}
	return &FileWrapper{aead: aead, path: path}, nil
}

// checkSecureKEKMode rejects a KEK file whose permission bits permit
// group or other access. Skipped on Windows, where the unix mode
// model does not apply.
func checkSecureKEKMode(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	st, err := os.Stat(path)
	if err != nil {
		return errors.Wrapf(err, "kek: stat %q", path)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		return errors.Wrapf(ErrInsecureKEKFile, "%q has mode %#o", path, perm)
	}
	return nil
}

// Wrap returns AES-GCM(KEK, dek) prefixed by a freshly-drawn random
// nonce. Output layout:
//
//	[nonce 12 bytes] [ciphertext 32 bytes] [tag 16 bytes]
//
// Total wrapped size: 60 bytes for a 32-byte DEK.
func (w *FileWrapper) Wrap(dek []byte) ([]byte, error) {
	if len(dek) != fileKEKSize {
		return nil, errors.Errorf("kek: dek is %d bytes, want %d", len(dek), fileKEKSize)
	}
	nonce := make([]byte, fileNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, errors.Wrap(err, "kek: random nonce")
	}
	out := make([]byte, 0, fileNonceSize+fileKEKSize+fileTagSize)
	out = append(out, nonce...)
	// AAD is intentionally nil. Key-ID binding at the KEK layer (so a
	// wrapped DEK from one key_id cannot be replayed under another) is
	// deferred to Stage 9, when the KMS-backed providers add their own
	// AAD scheme. Adding AAD here in isolation would silently break
	// every persisted wrapped-DEK blob.
	out = w.aead.Seal(out, nonce, dek, nil)
	return out, nil
}

// Unwrap reverses Wrap. It returns ErrIntegrity-equivalent errors via
// the AEAD library (the parent encryption package's ErrIntegrity is the
// caller's responsibility to wrap, since this package must stay
// dependency-free of the parent).
//
// The post-Open length check that earlier drafts had was unreachable —
// the strict-length input check above guarantees Open returns exactly
// fileKEKSize bytes on success — and has been removed.
func (w *FileWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if len(wrapped) != fileNonceSize+fileKEKSize+fileTagSize {
		return nil, errors.Errorf("kek: wrapped DEK is %d bytes, want %d",
			len(wrapped), fileNonceSize+fileKEKSize+fileTagSize)
	}
	nonce := wrapped[:fileNonceSize]
	ct := wrapped[fileNonceSize:]
	plain, err := w.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, errors.Wrap(err, "kek: AES-GCM Open")
	}
	return plain, nil
}

// Name returns the provider id plus the file path so log lines and the
// EncryptionAdmin status RPC let an operator distinguish multiple
// configured KEKs without grepping config.
func (w *FileWrapper) Name() string { return "file:" + w.path }
