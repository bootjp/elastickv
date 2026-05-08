package etcd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSnapshotSpool_DefaultCapAcceptsRealisticFSM pins the regression behind
// the 2026-05-08 incident: with the prior 1 GiB hardcoded cap, any real-world
// FSM (production observed 1.35 GiB) failed mid-stream with
// errSnapshotPayloadTooLarge, breaking the gRPC snapshot stream and locking
// the leader/follower into a retransmit loop. The default cap must accept at
// least 1.5 GiB without env override.
func TestSnapshotSpool_DefaultCapAcceptsRealisticFSM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: writes 1.5 GiB to a temp file")
	}
	dir := t.TempDir()
	spool, err := newSnapshotSpool(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = spool.Close() })

	// 1.5 GiB exceeds the legacy 1 GiB ceiling and matches realistic
	// production FSM sizes within the same order of magnitude.
	const target = int64(1536) << 20 // 1.5 GiB
	const chunk = 8 << 20            // 8 MiB writes mirror the gRPC snapshot chunk size order
	buf := bytes.Repeat([]byte{0xAB}, chunk)

	var written int64
	for written < target {
		toWrite := chunk
		if remaining := target - written; remaining < int64(chunk) {
			toWrite = int(remaining)
		}
		n, err := spool.Write(buf[:toWrite])
		require.NoError(t, err, "write at offset %d unexpectedly failed", written)
		require.Equal(t, toWrite, n)
		written += int64(n)
	}
	require.Equal(t, target, spool.size)

	// Round-trip through the materialization path (the io.ReadAll →
	// io.ReadFull refactor) to lock down behaviour at 1.5 GiB. The
	// returned slice MUST match s.size exactly: a short read here would
	// indicate the pre-allocation drifted out of sync with what Write
	// actually persisted to the spool file.
	got, err := spool.Bytes()
	require.NoError(t, err)
	require.Equal(t, int(target), len(got), "Bytes() returned %d, want %d", len(got), target)
	// Spot-check first/last bytes match the 0xAB fill; full byte-equality
	// would double the test's memory cost without adding signal.
	require.Equal(t, byte(0xAB), got[0])
	require.Equal(t, byte(0xAB), got[len(got)-1])
}

// TestSnapshotSpool_OverrideViaEnv confirms the env knob actually moves the
// cap. Tests deliberately *lower* it (cheap to write past) instead of
// raising — the upper-bound test above already proves a generous cap works.
func TestSnapshotSpool_OverrideViaEnv(t *testing.T) {
	const spoolCap = int64(4096)
	t.Setenv(maxSnapshotPayloadBytesEnvVar, strconv.FormatInt(spoolCap, 10))

	spool, err := newSnapshotSpool(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = spool.Close() })

	require.Equal(t, spoolCap, spool.maxSize)

	// Write up to the cap — succeeds.
	_, err = spool.Write(bytes.Repeat([]byte{0x01}, int(spoolCap)))
	require.NoError(t, err)

	// One byte past — fails with the documented sentinel so callers can
	// errors.Is against errSnapshotPayloadTooLarge for telemetry.
	_, err = spool.Write([]byte{0x02})
	require.Error(t, err)
	require.True(t, errors.Is(err, errSnapshotPayloadTooLarge), "got %v", err)
}

// TestSnapshotSpool_OverrideInvalidFallsBack pins the resolver's
// fail-soft behaviour: a malformed env value must NOT zero the cap (which
// would make every receive fail) — it falls back to the default.
func TestSnapshotSpool_OverrideInvalidFallsBack(t *testing.T) {
	t.Setenv(maxSnapshotPayloadBytesEnvVar, "not-a-number")
	spool, err := newSnapshotSpool(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = spool.Close() })
	require.Equal(t, defaultMaxSnapshotPayloadBytes, spool.maxSize)
}

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
// term is always 1 in the test suite.
func createSnapFile(t *testing.T, dir string, index uint64) {
	t.Helper()
	const term = uint64(1)
	name := fmt.Sprintf("%016x-%016x.snap", term, index)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("fake"), 0o600))
}

func TestPurgeOldSnapFiles(t *testing.T) {
	snapDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	// Create 6 snap files at increasing indices.
	for i := uint64(1); i <= 6; i++ {
		createSnapFile(t, snapDir, i*10000)
	}

	// Create a non-snap file that must be preserved.
	other := filepath.Join(snapDir, "db.tmp.12345")
	require.NoError(t, os.WriteFile(other, []byte("x"), 0o600))

	require.NoError(t, purgeOldSnapshotFiles(snapDir, fsmSnapDir))

	entries, err := os.ReadDir(snapDir)
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
	snapDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	// Only 2 files — under the limit of 3, nothing should be removed.
	createSnapFile(t, snapDir, 1000)
	createSnapFile(t, snapDir, 2000)

	require.NoError(t, purgeOldSnapshotFiles(snapDir, fsmSnapDir))

	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestPurgeOldSnapFilesEmptyDir(t *testing.T) {
	snapDir := t.TempDir()
	fsmSnapDir := t.TempDir()
	require.NoError(t, purgeOldSnapshotFiles(snapDir, fsmSnapDir))
}
