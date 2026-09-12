package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/stretchr/testify/require"
)

func writeTestKEK(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "kek")
	kekBytes := make([]byte, encryption.KeySize)
	for i := range kekBytes {
		kekBytes[i] = byte(i + 1)
	}
	require.NoError(t, os.WriteFile(path, kekBytes, 0o600))
	return path
}

// The wrapped DEK must round-trip through the same wrapper the server uses, or
// bootstrap would be handed bytes the node cannot unwrap -- and the Jepsen suite
// would fail at bootstrap instead of running encrypted.
func TestWrapFreshDEKProducesAnUnwrappableDEK(t *testing.T) {
	t.Parallel()

	path := writeTestKEK(t)
	encoded, err := wrapFreshDEK(path)
	require.NoError(t, err)

	raw, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)

	wrapper, err := kek.NewFileWrapper(path)
	require.NoError(t, err)
	dek, err := wrapper.Unwrap(raw)
	require.NoError(t, err)
	require.Len(t, dek, encryption.KeySize,
		"bootstrap rejects a DEK that is not AES-256")
}

// Two invocations must not produce the same DEK: the harness calls this once for
// the storage DEK and once for the raft DEK, and bootstrap requires them to
// differ.
func TestWrapFreshDEKGeneratesADistinctDEKEachCall(t *testing.T) {
	t.Parallel()

	path := writeTestKEK(t)
	wrapper, err := kek.NewFileWrapper(path)
	require.NoError(t, err)

	seen := make(map[string]struct{}, 8)
	for range 8 {
		encoded, err := wrapFreshDEK(path)
		require.NoError(t, err)
		raw, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(t, err)
		dek, err := wrapper.Unwrap(raw)
		require.NoError(t, err)
		_, dup := seen[string(dek)]
		require.False(t, dup, "each call must generate a fresh DEK")
		seen[string(dek)] = struct{}{}
	}
}

func TestRunRequiresAKEKFile(t *testing.T) {
	t.Parallel()

	require.Error(t, run(nil, os.Stdout))
}
