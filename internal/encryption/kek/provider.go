package kek

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"time"

	"github.com/cockroachdb/errors"
)

const providerRequestTimeout = 30 * time.Second

var (
	// ErrInvalidDEKLength rejects non-AES-256 data keys at every provider
	// boundary, including malformed provider responses.
	ErrInvalidDEKLength = errors.New("kek: DEK must be exactly 32 bytes")
	// ErrInvalidProviderResponse rejects an empty or integrity-invalid response
	// before it can be persisted in the encryption sidecar.
	ErrInvalidProviderResponse = errors.New("kek: invalid provider response")
	// ErrKEKPreflightFailed prevents mutators from opening when the configured
	// provider cannot complete a real wrap/unwrap round trip.
	ErrKEKPreflightFailed = errors.New("kek: provider preflight failed")
)

// VerifyWrapper proves credentials, provider reachability, encrypt/decrypt
// permissions, and key binding before any encryption mutator can commit a
// wrapped DEK. Provider constructors alone only validate local configuration.
func VerifyWrapper(wrapper Wrapper) error {
	if wrapper == nil {
		return errors.Wrap(ErrKEKPreflightFailed, "wrapper is nil")
	}
	dek := make([]byte, fileKEKSize)
	defer clear(dek)
	if _, err := rand.Read(dek); err != nil {
		return errors.Wrapf(ErrKEKPreflightFailed, "generate probe DEK: %v", err)
	}
	wrapped, err := wrapper.Wrap(dek)
	if err != nil {
		return errors.Wrapf(ErrKEKPreflightFailed, "wrap with %s: %v", wrapper.Name(), err)
	}
	defer clear(wrapped)
	plain, err := wrapper.Unwrap(wrapped)
	if err != nil {
		return errors.Wrapf(ErrKEKPreflightFailed, "unwrap with %s: %v", wrapper.Name(), err)
	}
	defer clear(plain)
	if len(plain) != len(dek) || subtle.ConstantTimeCompare(plain, dek) != 1 {
		return errors.Wrapf(ErrKEKPreflightFailed, "%s returned a different DEK", wrapper.Name())
	}
	return nil
}

func validateDEK(dek []byte) error {
	if len(dek) != fileKEKSize {
		return errors.Wrapf(ErrInvalidDEKLength, "got %d bytes", len(dek))
	}
	return nil
}

func requestContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = providerRequestTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}
