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
	t.Setenv(snapshotSpoolMinFreeBytesEnvVar, "0")
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
	t.Setenv(snapshotSpoolMinFreeBytesEnvVar, "0")

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

func TestSnapshotSpoolDefaultReserveTracksPayloadCap(t *testing.T) {
	const spoolCap = int64(4096)
	t.Setenv(maxSnapshotPayloadBytesEnvVar, strconv.FormatInt(spoolCap, 10))

	spool, err := newReceiveSnapshotSpool(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = spool.Close() })

	require.Equal(t, spoolCap, spool.maxSize)
	require.Equal(t, spoolCap, spool.minFreeBytes)
}

func TestSnapshotSpoolMaterializeDoesNotReserveDiskHeadroom(t *testing.T) {
	t.Setenv(snapshotSpoolMinFreeBytesEnvVar, "1024")
	originalAvailable := snapshotSpoolAvailableBytes
	snapshotSpoolAvailableBytes = func(string) (int64, error) {
		return 0, nil
	}
	t.Cleanup(func() { snapshotSpoolAvailableBytes = originalAvailable })

	spool, err := newSnapshotSpool(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = spool.Close() })

	n, err := spool.Write([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, int64(1), spool.size)
}

func TestSnapshotSpoolRejectsWhenReserveWouldBeConsumed(t *testing.T) {
	t.Setenv(snapshotSpoolMinFreeBytesEnvVar, "1024")
	originalAvailable := snapshotSpoolAvailableBytes
	snapshotSpoolAvailableBytes = func(string) (int64, error) {
		return 1024, nil
	}
	t.Cleanup(func() { snapshotSpoolAvailableBytes = originalAvailable })

	spool, err := newReceiveSnapshotSpool(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = spool.Close() })

	n, err := spool.Write([]byte("x"))
	require.Zero(t, n)
	require.Error(t, err)
	require.True(t, errors.Is(err, errSnapshotSpoolDiskHeadroom), "got %v", err)
	require.Zero(t, spool.size)

	info, statErr := spool.file.Stat()
	require.NoError(t, statErr)
	require.Zero(t, info.Size(), "headroom rejection must happen before writing bytes")
}

// TestFinalizeAsFSMFile_PostFinalizeCloseIsNoop pins the gemini-medium
// review on PR #747: after a successful FinalizeAsFSMFile, the deferred
// caller-side spool.Close() must NOT attempt to remove the renamed file
// (which would surface a misleading os.ErrNotExist that buries any real
// error the caller is reporting). State clearing inside FinalizeAsFSMFile
// at each successful step makes the post-success Close() a true no-op.
func TestFinalizeAsFSMFile_PostFinalizeCloseIsNoop(t *testing.T) {
	const (
		index uint64 = 99
		crc   uint32 = 0x42424242
	)
	spoolDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	spool, err := newSnapshotSpool(spoolDir)
	require.NoError(t, err)

	payload := []byte("payload-bytes-for-finalize-test")
	_, err = spool.Write(payload)
	require.NoError(t, err)

	require.NoError(t, spool.FinalizeAsFSMFile(fsmSnapDir, index, crc))

	// Spool dir is empty (file moved, not deleted, not orphaned).
	spoolEntries, err := os.ReadDir(spoolDir)
	require.NoError(t, err)
	require.Empty(t, spoolEntries)

	// .fsm file exists at canonical path with payload + 4-byte CRC footer.
	finalPath := fsmSnapPath(fsmSnapDir, index)
	got, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	require.Equal(t, len(payload)+4, len(got))
	require.Equal(t, payload, got[:len(payload)])

	// Critical: post-Finalize Close() must be a clean no-op, not surface
	// os.ErrNotExist from trying to remove the original spool path.
	require.NoError(t, spool.Close(), "Close after successful Finalize must not error")

	// Idempotent: a second Close (from a buggy / over-cautious caller)
	// must also be a no-op.
	require.NoError(t, spool.Close())

	// Renamed file is still on disk — Close did not nuke it.
	_, err = os.Stat(finalPath)
	require.NoError(t, err)
}

// TestFinalizeAsFSMFile_RenameFailureCleansUpSpool pins the partial-failure
// path: if os.Rename fails (here simulated by removing the spool dir), the
// already-closed spool file lives at its original path. The deferred
// caller-side Close() should still remove that orphan so we don't leak
// disk on the unhappy path.
func TestFinalizeAsFSMFile_RenameFailureCleansUpSpool(t *testing.T) {
	spoolDir := t.TempDir()
	// Point fsmSnapDir at a path under a directory we'll make unwritable
	// AFTER the spool file is created. The file gets sync+close'd but the
	// rename fails because the destination dir cannot be created.
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o500)) // r-x: MkdirAll under it fails
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })
	fsmSnapDir := filepath.Join(parent, "no-such-subdir")

	spool, err := newSnapshotSpool(spoolDir)
	require.NoError(t, err)
	_, err = spool.Write([]byte("payload"))
	require.NoError(t, err)
	spoolPath := spool.path

	err = spool.FinalizeAsFSMFile(fsmSnapDir, 7, 0xdeadbeef)
	require.Error(t, err, "Finalize must report the MkdirAll/Rename failure")

	// Spool file is still at its original path (rename never happened).
	_, statErr := os.Stat(spoolPath)
	require.NoError(t, statErr, "spool file should still exist after a pre-rename failure")

	// Caller's deferred Close() now removes the orphan.
	require.NoError(t, spool.Close())
	_, statErr = os.Stat(spoolPath)
	require.True(t, os.IsNotExist(statErr), "Close must remove the orphaned spool file")
}

// TestFinalizeAsFSMFile_SyncDirFailureCloseIsNoop pins the partial-failure
// path documented in FinalizeAsFSMFile's comment block: when os.Rename has
// already succeeded but the subsequent dir-fsync fails, the spool's state
// clearing must have advanced past s.path so a deferred caller-side
// spool.Close() returns nil instead of surfacing os.ErrNotExist from
// trying to delete the now-renamed file. Without the snapshotSyncDir
// injection seam there is no portable way to reproduce a syncDir failure
// (the kernel happily fsyncs an empty tmp dir).
func TestFinalizeAsFSMFile_SyncDirFailureCloseIsNoop(t *testing.T) {
	const (
		index uint64 = 17
		crc   uint32 = 0xcafebabe
	)
	spoolDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	original := snapshotSyncDir
	syncErr := errors.New("simulated syncDir failure")
	snapshotSyncDir = func(string) error { return syncErr }
	t.Cleanup(func() { snapshotSyncDir = original })

	spool, err := newSnapshotSpool(spoolDir)
	require.NoError(t, err)

	payload := []byte("payload-bytes-for-syncdir-failure-test")
	_, err = spool.Write(payload)
	require.NoError(t, err)

	err = spool.FinalizeAsFSMFile(fsmSnapDir, index, crc)
	require.Error(t, err)
	require.ErrorIs(t, err, syncErr, "Finalize must surface the syncDir error")

	// Crucial: rename already happened, so the .fsm file is at its
	// canonical path. A future read MUST find it; the syncDir failure
	// is durability-only, not a logical-state regression.
	finalPath := fsmSnapPath(fsmSnapDir, index)
	_, statErr := os.Stat(finalPath)
	require.NoError(t, statErr, ".fsm file should be at canonical path despite syncDir failure")

	// Spool dir is empty (file was renamed out, not deleted, not
	// orphaned at the spool location).
	spoolEntries, err := os.ReadDir(spoolDir)
	require.NoError(t, err)
	require.Empty(t, spoolEntries)

	// THE point: deferred Close() must be a clean no-op despite the
	// upstream error. Pre-fix, Close() would have tried os.Remove on
	// the original spool path, returned os.ErrNotExist, and the slog
	// warning in receiveSnapshotStream would bury the real syncDir
	// signal under a misleading not-exist log line.
	require.NoError(t, spool.Close(), "Close after syncDir failure must not surface os.ErrNotExist")

	// .fsm file is still on disk after Close.
	_, statErr = os.Stat(finalPath)
	require.NoError(t, statErr)
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
