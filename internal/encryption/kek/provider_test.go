package kek

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type preflightWrapper struct {
	wrapErr   error
	unwrapErr error
	mismatch  bool
}

func (w *preflightWrapper) Wrap(dek []byte) ([]byte, error) {
	if w.wrapErr != nil {
		return nil, w.wrapErr
	}
	return append([]byte(nil), dek...), nil
}

func (w *preflightWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if w.unwrapErr != nil {
		return nil, w.unwrapErr
	}
	plain := append([]byte(nil), wrapped...)
	if w.mismatch {
		plain[0] ^= 0xFF
	}
	return plain, nil
}

func (*preflightWrapper) Name() string { return "test" }

func TestVerifyWrapper(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		wrapper Wrapper
		wantErr bool
	}{
		{name: "round trip", wrapper: &preflightWrapper{}},
		{name: "nil wrapper", wrapper: nil, wantErr: true},
		{name: "wrap failure", wrapper: &preflightWrapper{wrapErr: errors.New("denied")}, wantErr: true},
		{name: "unwrap failure", wrapper: &preflightWrapper{unwrapErr: errors.New("denied")}, wantErr: true},
		{name: "mismatched plaintext", wrapper: &preflightWrapper{mismatch: true}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyWrapper(tc.wrapper)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrKEKPreflightFailed)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestVerifyWrapperUsesRandomDEKLength(t *testing.T) {
	t.Parallel()
	wrapper := &capturingPreflightWrapper{}
	require.NoError(t, VerifyWrapper(wrapper))
	require.Len(t, wrapper.seen, fileKEKSize)
	require.False(t, bytes.Equal(wrapper.seen, make([]byte, fileKEKSize)))
}

type capturingPreflightWrapper struct {
	seen []byte
}

func (w *capturingPreflightWrapper) Wrap(dek []byte) ([]byte, error) {
	w.seen = append([]byte(nil), dek...)
	return append([]byte(nil), dek...), nil
}

func (*capturingPreflightWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	return append([]byte(nil), wrapped...), nil
}

func (*capturingPreflightWrapper) Name() string { return "capture" }
