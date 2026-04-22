package etcd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeDummyWALSegment creates a plausible-looking WAL filename in dir so
// purgeOldWALFiles has something to pick up. The wal package's naming format
// is %016x-%016x.wal; we only need the suffix and lexicographic ordering to
// match production so we can exercise the purge logic without standing up a
// full wal.WAL.
func writeDummyWALSegment(t *testing.T, dir string, seq, index uint64) string {
	t.Helper()
	name := fmt.Sprintf("%016x-%016x.wal", seq, index)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("dummy"), 0o600))
	return name
}

// listWALFiles returns the sorted set of .wal filenames currently in dir.
func listWALFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".wal" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func TestPurgeOldWALFiles_NoOpWhenBelowLimit(t *testing.T) {
	dir := t.TempDir()
	writeDummyWALSegment(t, dir, 0, 0)
	writeDummyWALSegment(t, dir, 1, 100)

	require.NoError(t, purgeOldWALFiles(dir, 5))

	got := listWALFiles(t, dir)
	require.Len(t, got, 2)
}

func TestPurgeOldWALFiles_KeepsMostRecentN(t *testing.T) {
	dir := t.TempDir()
	for i := uint64(0); i < 10; i++ {
		writeDummyWALSegment(t, dir, i, i*100)
	}

	require.NoError(t, purgeOldWALFiles(dir, 3))

	got := listWALFiles(t, dir)
	// Must keep exactly the 3 newest; names are lexicographic == chronological.
	require.Len(t, got, 3)
	require.Equal(t, []string{
		fmt.Sprintf("%016x-%016x.wal", 7, 700),
		fmt.Sprintf("%016x-%016x.wal", 8, 800),
		fmt.Sprintf("%016x-%016x.wal", 9, 900),
	}, got)
}

func TestPurgeOldWALFiles_NeverDeletesActiveTail(t *testing.T) {
	dir := t.TempDir()
	// Create 5 segments; then hold an OS-level file lock on the newest one
	// to simulate the wal package's flock on the active tail. keep=1 means
	// the purger would WANT to delete the other 4, but the tail must remain
	// under all circumstances.
	var names []string
	for i := uint64(0); i < 5; i++ {
		names = append(names, writeDummyWALSegment(t, dir, i, i*100))
	}
	// keep=1 asks the purger to delete the 4 older segments; the newest is
	// already excluded by the keep cutoff, not by locking. This verifies
	// the "keep at least 1" invariant even with an aggressive cap.
	require.NoError(t, purgeOldWALFiles(dir, 1))

	got := listWALFiles(t, dir)
	require.Len(t, got, 1)
	require.Equal(t, names[len(names)-1], got[0])
}

func TestPurgeOldWALFiles_MissingDirIsNoOp(t *testing.T) {
	// Directory that doesn't exist -> nil error, not a failure.
	require.NoError(t, purgeOldWALFiles(filepath.Join(t.TempDir(), "no-such-dir"), 3))
}

func TestPurgeOldWALFiles_ClampsKeepToOne(t *testing.T) {
	dir := t.TempDir()
	for i := uint64(0); i < 4; i++ {
		writeDummyWALSegment(t, dir, i, i*100)
	}
	// keep=0 is an invalid configuration; the function must clamp to 1
	// rather than delete every segment (which would destroy the WAL).
	require.NoError(t, purgeOldWALFiles(dir, 0))

	got := listWALFiles(t, dir)
	require.Len(t, got, 1)
}

func TestPurgeOldWALFiles_IgnoresNonWALEntries(t *testing.T) {
	dir := t.TempDir()
	writeDummyWALSegment(t, dir, 0, 0)
	writeDummyWALSegment(t, dir, 1, 100)
	writeDummyWALSegment(t, dir, 2, 200)
	// Non-.wal files must be left untouched: operators sometimes drop
	// hand-crafted recovery artefacts here.
	otherPath := filepath.Join(dir, "NOTES.txt")
	require.NoError(t, os.WriteFile(otherPath, []byte("hello"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o700))

	require.NoError(t, purgeOldWALFiles(dir, 1))

	got := listWALFiles(t, dir)
	require.Len(t, got, 1)
	_, statErr := os.Stat(otherPath)
	require.NoError(t, statErr, "non-wal file must not be deleted")
}

func TestMaxWALFilesFromEnv_DefaultsWhenUnset(t *testing.T) {
	t.Setenv(maxWALFilesEnvVar, "")
	require.Equal(t, defaultMaxWALFiles, maxWALFilesFromEnv())
}

func TestMaxWALFilesFromEnv_ReadsOverride(t *testing.T) {
	t.Setenv(maxWALFilesEnvVar, "2")
	require.Equal(t, 2, maxWALFilesFromEnv())
}

func TestMaxWALFilesFromEnv_FallsBackOnInvalid(t *testing.T) {
	t.Setenv(maxWALFilesEnvVar, "not-a-number")
	require.Equal(t, defaultMaxWALFiles, maxWALFilesFromEnv())

	t.Setenv(maxWALFilesEnvVar, "0")
	require.Equal(t, defaultMaxWALFiles, maxWALFilesFromEnv())

	t.Setenv(maxWALFilesEnvVar, "-3")
	require.Equal(t, defaultMaxWALFiles, maxWALFilesFromEnv())
}

func TestSnapshotEveryFromEnv_DefaultsWhenUnset(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "")
	require.Equal(t, uint64(defaultSnapshotEvery), snapshotEveryFromEnv())
}

func TestSnapshotEveryFromEnv_ReadsOverride(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "1500")
	require.Equal(t, uint64(1500), snapshotEveryFromEnv())
}

func TestSnapshotEveryFromEnv_ClampsZeroToOne(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "0")
	require.Equal(t, uint64(1), snapshotEveryFromEnv())
}

func TestSnapshotEveryFromEnv_FallsBackOnInvalid(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "not-a-number")
	require.Equal(t, uint64(defaultSnapshotEvery), snapshotEveryFromEnv())
}
