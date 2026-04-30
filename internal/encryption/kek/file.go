package kek

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"os"

	"github.com/cockroachdb/errors"
)

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
func NewFileWrapper(path string) (*FileWrapper, error) {
	raw, err := os.ReadFile(path)
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
	out = w.aead.Seal(out, nonce, dek, nil)
	return out, nil
}

// Unwrap reverses Wrap. It returns ErrIntegrity-equivalent errors via
// the AEAD library (the parent encryption package's ErrIntegrity is the
// caller's responsibility to wrap, since this package must stay
// dependency-free of the parent).
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
	if len(plain) != fileKEKSize {
		return nil, errors.Errorf("kek: unwrapped DEK is %d bytes, want %d",
			len(plain), fileKEKSize)
	}
	return plain, nil
}

// Name implements Wrapper.
func (*FileWrapper) Name() string { return "file" }
