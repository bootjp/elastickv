package kek

import (
	"context"
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
)

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
