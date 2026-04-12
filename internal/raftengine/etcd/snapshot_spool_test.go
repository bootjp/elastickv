package etcd

import (
	"fmt"
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
	require.NoError(t, os.WriteFile(unrelated, []byte("data"), 0o600))

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

func TestCleanupStaleSnapshotSpoolsNonExistentDir(t *testing.T) {
	require.NoError(t, cleanupStaleSnapshotSpools(filepath.Join(t.TempDir(), "no-such-dir")))
}

// createSnapFile creates a fake .snap file with the etcd naming convention.
func createSnapFile(t *testing.T, dir string, term, index uint64) string {
	t.Helper()
	name := fmt.Sprintf("%016x-%016x.snap", term, index)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("fake"), 0o600))
	return path
}

func TestPurgeOldSnapFiles(t *testing.T) {
	dir := t.TempDir()

	// Create 6 snap files at increasing indices.
	for i := uint64(1); i <= 6; i++ {
		createSnapFile(t, dir, 1, i*10000)
	}

	// Create a non-snap file that must be preserved.
	other := filepath.Join(dir, "db.tmp.12345")
	require.NoError(t, os.WriteFile(other, []byte("x"), 0o600))

	require.NoError(t, purgeOldSnapFiles(dir, defaultMaxSnapFiles))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var snaps []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".snap" {
			snaps = append(snaps, e.Name())
		}
	}

	// Only the newest 3 should remain.
	require.Len(t, snaps, 3)
	require.Equal(t, fmt.Sprintf("%016x-%016x.snap", 1, uint64(40000)), snaps[0])
	require.Equal(t, fmt.Sprintf("%016x-%016x.snap", 1, uint64(50000)), snaps[1])
	require.Equal(t, fmt.Sprintf("%016x-%016x.snap", 1, uint64(60000)), snaps[2])

	// Non-snap file preserved.
	_, err = os.Stat(other)
	require.NoError(t, err)
}

func TestPurgeOldSnapFilesUnderLimit(t *testing.T) {
	dir := t.TempDir()

	// Only 2 files — under the limit of 3, nothing should be removed.
	createSnapFile(t, dir, 1, 1000)
	createSnapFile(t, dir, 1, 2000)

	require.NoError(t, purgeOldSnapFiles(dir, defaultMaxSnapFiles))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestPurgeOldSnapFilesEmptyDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, purgeOldSnapFiles(dir, defaultMaxSnapFiles))
}
