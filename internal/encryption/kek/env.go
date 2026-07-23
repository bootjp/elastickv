package kek

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"os"

	"github.com/cockroachdb/errors"
)

// EnvVar is the test/CI-only environment variable accepted as a static KEK
// source. Production deployments should use a remote KMS provider.
const EnvVar = "ELASTICKV_KEK_BASE64"

var ErrNilEnvWrapper = errors.New("kek: EnvWrapper is nil or uninitialised; construct with NewEnvWrapper")

// EnvWrapper wraps DEKs with a process-local AES-256-GCM key loaded from
// EnvVar. NewEnvWrapper removes EnvVar immediately after decoding it so the
// raw KEK is not retained in the process environment.
type EnvWrapper struct {
	aead cipher.AEAD
}

// NewEnvWrapper reads a standard-base64 encoded 32-byte KEK. EnvVar is unset
// after the decode attempt on both success and failure paths.
func NewEnvWrapper() (*EnvWrapper, error) {
	encoded, ok := os.LookupEnv(EnvVar)
	if !ok {
		return nil, errors.Errorf("kek: %s is not set", EnvVar)
	}
	raw, decodeErr := base64.StdEncoding.DecodeString(encoded)
	unsetErr := os.Unsetenv(EnvVar)
	if decodeErr != nil {
		return nil, errors.Wrapf(decodeErr, "kek: decode %s", EnvVar)
	}
	defer clear(raw)
	if unsetErr != nil {
		return nil, errors.Wrapf(unsetErr, "kek: unset %s", EnvVar)
	}
	if err := validateDEK(raw); err != nil {
		return nil, errors.Wrapf(err, "kek: %s", EnvVar)
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, errors.Wrap(err, "kek: env aes.NewCipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "kek: env cipher.NewGCM")
	}
	return &EnvWrapper{aead: aead}, nil
}

// Wrap returns nonce || AES-GCM(KEK, DEK), matching FileWrapper's local
// provider format.
func (w *EnvWrapper) Wrap(dek []byte) ([]byte, error) {
	if w == nil || w.aead == nil {
		return nil, errors.WithStack(ErrNilEnvWrapper)
	}
	if err := validateDEK(dek); err != nil {
		return nil, err
	}
	nonce := make([]byte, fileNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, errors.Wrap(err, "kek: env random nonce")
	}
	out := make([]byte, 0, fileNonceSize+fileKEKSize+fileTagSize)
	out = append(out, nonce...)
	return w.aead.Seal(out, nonce, dek, nil), nil
}

// Unwrap authenticates and decrypts an EnvWrapper payload.
func (w *EnvWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if w == nil || w.aead == nil {
		return nil, errors.WithStack(ErrNilEnvWrapper)
	}
	if len(wrapped) != fileNonceSize+fileKEKSize+fileTagSize {
		return nil, errors.Errorf("kek: env wrapped DEK is %d bytes, want %d",
			len(wrapped), fileNonceSize+fileKEKSize+fileTagSize)
	}
	plain, err := w.aead.Open(nil, wrapped[:fileNonceSize], wrapped[fileNonceSize:], nil)
	if err != nil {
		return nil, errors.Wrap(err, "kek: env AES-GCM Open")
	}
	return plain, nil
}

// Name identifies the environment-backed provider without exposing key bytes.
func (*EnvWrapper) Name() string { return "env" }
