package kek

import (
	"bytes"
	"encoding/base64"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestEnvWrapperRoundTripAndUnset(t *testing.T) {
	key := bytes.Repeat([]byte{0x41}, fileKEKSize)
	t.Setenv(EnvVar, base64.StdEncoding.EncodeToString(key))
	wrapper, err := NewEnvWrapper()
	require.NoError(t, err)
	_, stillSet := os.LookupEnv(EnvVar)
	require.False(t, stillSet)
	require.Equal(t, "env", wrapper.Name())

	dek := bytes.Repeat([]byte{0xA5}, fileKEKSize)
	wrapped, err := wrapper.Wrap(dek)
	require.NoError(t, err)
	require.NotEqual(t, dek, wrapped)
	plain, err := wrapper.Unwrap(wrapped)
	require.NoError(t, err)
	require.Equal(t, dek, plain)

	wrapped[len(wrapped)-1] ^= 0x01
	_, err = wrapper.Unwrap(wrapped)
	require.Error(t, err)
}

func TestEnvWrapperInvalidInputStillUnsets(t *testing.T) {
	t.Setenv(EnvVar, "not-base64")
	_, err := NewEnvWrapper()
	require.Error(t, err)
	_, stillSet := os.LookupEnv(EnvVar)
	require.False(t, stillSet)
}

func TestEnvWrapperRejectsInvalidDEKLength(t *testing.T) {
	t.Setenv(EnvVar, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, fileKEKSize)))
	wrapper, err := NewEnvWrapper()
	require.NoError(t, err)
	_, err = wrapper.Wrap([]byte("short"))
	require.ErrorIs(t, err, ErrInvalidDEKLength)
	var nilWrapper *EnvWrapper
	_, err = nilWrapper.Wrap(bytes.Repeat([]byte{1}, fileKEKSize))
	require.True(t, errors.Is(err, ErrNilEnvWrapper))
}
