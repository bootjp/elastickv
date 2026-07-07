package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/stretchr/testify/require"
)

func TestArchiveCLICompressesAndExtractsDump(t *testing.T) {
	root := writeCLIDumpFixture(t)
	archivePath := filepath.Join(t.TempDir(), "dump.tar.zst")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	code, err := run([]string{"pack", "--input", root, "--output", archivePath}, logger)
	require.NoError(t, err)
	require.Equal(t, exitSuccess, code)

	out := filepath.Join(t.TempDir(), "out")
	code, err = run([]string{"unpack", "--input", archivePath, "--output", out}, logger)
	require.NoError(t, err)
	require.Equal(t, exitSuccess, code)
	require.NoError(t, backup.VerifyChecksums(out))
	require.FileExists(t, filepath.Join(out, "redis", "db_0", "strings", "key.bin"))
}

func TestArchiveCLIRejectsCorruptChecksums(t *testing.T) {
	root := writeCLIDumpFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(root, "redis", "db_0", "strings", "key.bin"), []byte("tampered"), 0o600))

	code, err := run([]string{
		"pack",
		"--input", root,
		"--output", filepath.Join(t.TempDir(), "dump.tar"),
		"--compression", "none",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, backup.ErrChecksumMismatch)
	require.Equal(t, exitDataErr, code)
}

func writeCLIDumpFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "redis", "db_0", "strings"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "redis", "db_0", "strings", "key.bin"), []byte("value"), 0o600))
	m := backup.NewPhase0SnapshotManifest(time.Unix(0, 0))
	m.ElastickvVersion = "test"
	m.ClusterID = "cluster-a"
	m.LastCommitTS = 1
	m.Source = &backup.Source{FSMPath: "source.fsm"}
	m.Adapters = &backup.Adapters{Redis: &backup.Adapter{}}
	f, err := os.Create(filepath.Join(root, "MANIFEST.json"))
	require.NoError(t, err)
	require.NoError(t, backup.WriteManifest(f, m))
	require.NoError(t, f.Close())
	require.NoError(t, backup.WriteChecksums(root))
	return root
}
