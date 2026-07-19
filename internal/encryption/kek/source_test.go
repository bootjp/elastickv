package kek

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewWrapperFromSourcesNoSource(t *testing.T) {
	t.Setenv(EnvVar, "")
	require.NoError(t, os.Unsetenv(EnvVar))
	wrapper, err := NewWrapperFromSources(context.Background(), "", "")
	require.NoError(t, err)
	require.Nil(t, wrapper)
}

func TestNewWrapperFromSourcesRejectsAmbiguity(t *testing.T) {
	t.Setenv(EnvVar, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, fileKEKSize)))
	_, err := NewWrapperFromSources(context.Background(), "/tmp/kek", "")
	require.ErrorIs(t, err, ErrMultipleKEKSources)
	_, stillSet := os.LookupEnv(EnvVar)
	require.False(t, stillSet)
}

func TestNewWrapperFromSourcesFileAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kek.bin")
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{2}, fileKEKSize), 0o600))

	wrapper, err := NewWrapperFromSources(context.Background(), path, "")
	require.NoError(t, err)
	require.Contains(t, wrapper.Name(), "file:")

	t.Setenv(EnvVar, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{3}, fileKEKSize)))
	wrapper, err = NewWrapperFromSources(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, "env", wrapper.Name())
	_, stillSet := os.LookupEnv(EnvVar)
	require.False(t, stillSet)
}

func TestNewURIWrapperRejectsInvalidTargetsWithoutNetwork(t *testing.T) {
	for _, uri := range []string{
		"unknown://key",
		"aws-kms://not-an-arn",
		"gcp-kms://projects/incomplete",
		"vault-transit://transit",
	} {
		t.Run(uri, func(t *testing.T) {
			_, err := newURIWrapper(context.Background(), uri)
			require.ErrorIsf(t, err, ErrInvalidKEKURI, "uri=%q", uri)
		})
	}
}
