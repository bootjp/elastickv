package etcd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupStaleSnapshotSpools(t *testing.T) {
	dir := t.TempDir()

	// Create several orphaned spool files matching the pattern.
	for i := 0; i < 5; i++ {
		f, err := os.CreateTemp(dir, snapshotSpoolPattern)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	// Create an unrelated file that must not be removed.
	unrelated := filepath.Join(dir, "keep-me.txt")
	require.NoError(t, os.WriteFile(unrelated, []byte("data"), 0o644))

	matches, err := filepath.Glob(filepath.Join(dir, snapshotSpoolPattern))
	require.NoError(t, err)
	require.Len(t, matches, 5)

	require.NoError(t, cleanupStaleSnapshotSpools(dir))

	// All spool files should be gone.
	matches, err = filepath.Glob(filepath.Join(dir, snapshotSpoolPattern))
	require.NoError(t, err)
	require.Empty(t, matches)

	// Unrelated file should still exist.
	_, err = os.Stat(unrelated)
	require.NoError(t, err)
}

func TestCleanupStaleSnapshotSpoolsEmptyDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, cleanupStaleSnapshotSpools(dir))
}
