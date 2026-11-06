package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func offlineEncryptionFlags() []string {
	return []string{"--encryption-sidecar", "/offline/keys.json", "--encryption-kek-file", "/secrets/kek"}
}

func TestBootstrapOfflineEncryptionCLIStrictInputs(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"inspect", "prepare", "resume"} {
		args := baselineCLIArgs()
		args[0] = command
		r, err := parseBootstrapBaselineCLI(append(args, offlineEncryptionFlags()...))
		require.NoError(t, err)
		require.Equal(t, "/offline/keys.json", r.Encryption.Sidecar)
	}
	for _, extra := range [][]string{
		{"--encryption-sidecar", "/offline/keys.json"},
		{"--encryption-kek-file", "/secrets/kek"},
		{"--encryption-sidecar", "relative", "--encryption-kek-file", "/secrets/kek"},
		append(offlineEncryptionFlags(), "--encryption-kek-file", "/secrets/kek"),
		append(offlineEncryptionFlags(), "--encryption-kek-uri", "aws-kms://key"),
		{"--encryption-sidecar", "", "--encryption-kek-file", "/secrets/kek"},
	} {
		_, err := parseBootstrapBaselineCLI(append(baselineCLIArgs(), extra...))
		require.Error(t, err)
	}
	for _, command := range []string{"manifest", "certify"} {
		var err error
		if command == "manifest" {
			_, err = parseBootstrapObservationCommand(append(observationSyntaxArgs(t.TempDir(), command), offlineEncryptionFlags()...))
		} else {
			_, err = parseBootstrapArtifactCommand(append(bootstrapArtifactSyntaxArgs(command, t.TempDir()), offlineEncryptionFlags()...))
		}
		require.Error(t, err, "artifact-only commands cannot open encryption sources")
	}
	_, err := parseBootstrapObservationCommand(append(observationSyntaxArgs(t.TempDir(), "probe"), offlineEncryptionFlags()...))
	require.NoError(t, err)
	_, err = parseBootstrapArtifactCommand(append(bootstrapArtifactSyntaxArgs("attest", t.TempDir()), offlineEncryptionFlags()...))
	require.NoError(t, err)
}

func TestBootstrapOfflineEncryptionCLILoadIsExplicitAndReadOnly(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, path, _ := offlineEncryptedStore(t, 1, a)
	cfg := bootstrapOfflineEncryptionCLI{Sidecar: path, KEKFile: a.file}
	before := offlineDataFiles(t, root)
	enc, err := cfg.load(t.Context(), 1, 7, root)
	require.NoError(t, err)
	_, err = bootstrapOfflineDataStoreProbe(t.Context(), root, enc)
	require.NoError(t, err)
	require.Equal(t, before, offlineDataFiles(t, root))
	_, err = cfg.load(t.Context(), 1, 0, root)
	require.Error(t, err, "encrypted scalar TSO format not supported by the data decoder")
	empty, err := (bootstrapOfflineEncryptionCLI{}).load(t.Context(), 1, 7, root)
	require.NoError(t, err)
	require.Nil(t, empty)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = bootstrapOfflineDataStoreProbe(ctx, root, enc)
	require.ErrorIs(t, err, context.Canceled)
	_, err = bootstrapOfflineDataPreparation(kv.PreparedBootstrapActivation{}, nil)
	require.Error(t, err, "an explicit nil crypto context cannot mean plaintext")
	require.Error(t, bootstrapOfflineSecretOutsideSource(root, filepath.Join(root, bootstrapOfflineDataPath, "LOCK")))
}

func TestBootstrapOfflineEncryptionCLIRejectsHardlinkedSecret(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, path, _ := offlineEncryptedStore(t, 1, a)
	require.NoError(t, os.Link(a.file, filepath.Join(root, "external-secret-alias")))
	before := offlineDataFiles(t, root)
	_, err := (bootstrapOfflineEncryptionCLI{Sidecar: path, KEKFile: a.file}).load(t.Context(), 1, 7, root)
	require.Error(t, err)
	require.Equal(t, before, offlineDataFiles(t, root))
}
