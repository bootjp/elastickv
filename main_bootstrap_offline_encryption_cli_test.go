package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
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
		r, err := clusterbootstrap.ParseBootstrapBaselineCLI(append(args, offlineEncryptionFlags()...))
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
		_, err := clusterbootstrap.ParseBootstrapBaselineCLI(append(baselineCLIArgs(), extra...))
		require.Error(t, err)
	}
	for _, command := range []string{"manifest", "certify"} {
		var err error
		if command == "manifest" {
			_, err = clusterbootstrap.ParseBootstrapObservationCommand(append(observationSyntaxArgs(t.TempDir(), command), offlineEncryptionFlags()...))
		} else {
			_, err = clusterbootstrap.ParseBootstrapArtifactCommand(append(bootstrapArtifactSyntaxArgs(command, t.TempDir()), offlineEncryptionFlags()...))
		}
		require.Error(t, err, "artifact-only commands cannot open encryption sources")
	}
	_, err := clusterbootstrap.ParseBootstrapObservationCommand(append(observationSyntaxArgs(t.TempDir(), "probe"), offlineEncryptionFlags()...))
	require.NoError(t, err)
	_, err = clusterbootstrap.ParseBootstrapArtifactCommand(append(bootstrapArtifactSyntaxArgs("attest", t.TempDir()), offlineEncryptionFlags()...))
	require.NoError(t, err)
}

func TestBootstrapOfflineEncryptionCLILoadIsExplicitAndReadOnly(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, path, _ := offlineEncryptedStore(t, 1, a)
	cfg := clusterbootstrap.BootstrapOfflineEncryptionCLI{Sidecar: path, KEKFile: a.file}
	before := offlineDataFiles(t, root)
	enc, err := cfg.Load(t.Context(), 1, 7, root)
	require.NoError(t, err)
	_, err = clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), root, enc)
	require.NoError(t, err)
	require.Equal(t, before, offlineDataFiles(t, root))
	_, err = cfg.Load(t.Context(), 1, 0, root)
	require.Error(t, err, "encrypted scalar TSO format not supported by the data decoder")
	empty, err := (clusterbootstrap.BootstrapOfflineEncryptionCLI{}).Load(t.Context(), 1, 7, root)
	require.NoError(t, err)
	require.Nil(t, empty)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = clusterbootstrap.BootstrapOfflineDataStoreProbe(ctx, root, enc)
	require.ErrorIs(t, err, context.Canceled)
	_, err = clusterbootstrap.BootstrapOfflineDataPreparation(kv.PreparedBootstrapActivation{}, nil)
	require.Error(t, err, "an explicit nil crypto context cannot mean plaintext")
	require.Error(t, clusterbootstrap.BootstrapOfflineSecretOutsideSource(root, filepath.Join(root, clusterbootstrap.BootstrapOfflineDataPath, "LOCK")))
}

func TestBootstrapOfflineEncryptionCLIRejectsHardlinkedSecret(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, path, _ := offlineEncryptedStore(t, 1, a)
	require.NoError(t, os.Link(a.file, filepath.Join(root, "external-secret-alias")))
	before := offlineDataFiles(t, root)
	_, err := (clusterbootstrap.BootstrapOfflineEncryptionCLI{Sidecar: path, KEKFile: a.file}).Load(t.Context(), 1, 7, root)
	require.Error(t, err)
	require.Equal(t, before, offlineDataFiles(t, root))
}
