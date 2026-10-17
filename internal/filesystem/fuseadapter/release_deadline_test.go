package fuseadapter

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// shortenAbandonedRelease sets the two parts of the window an abandoned
// mount's release runs in for the duration of a test.
func shortenAbandonedRelease(t *testing.T, unmount, detach time.Duration) {
	t.Helper()
	oldUnmount, oldDetach := abandonedUnmountTimeout, abandonedDetachTimeout
	abandonedUnmountTimeout, abandonedDetachTimeout = unmount, detach
	t.Cleanup(func() { abandonedUnmountTimeout, abandonedDetachTimeout = oldUnmount, oldDetach })
}

// TestReleaseAfterReadinessSharesTheUnmountDeadline: the wait for the
// readiness check and the plain unmount that follows it share one deadline,
// so a readiness check that returns late leaves the unmount only what is
// left of it rather than a fresh bound of its own.
func TestReleaseAfterReadinessSharesTheUnmountDeadline(t *testing.T) {
	shortenAbandonedRelease(t, 2*time.Second, 2*time.Second)
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	marker := writeRecordingMountHelper(t)
	ready := make(chan error, 1)
	time.AfterFunc(1600*time.Millisecond, func() { ready <- nil })
	hung := make(chan struct{})
	t.Cleanup(func() { close(hung) })

	start := time.Now()
	releaseAfterReadiness(ready, func() error {
		<-hung
		return nil
	}, mountPoint, fuseTypeFor(filesystemName))
	elapsed := time.Since(start)
	require.FileExists(t, marker, "the hung unmount must be followed by the lazy detach")
	require.Less(t, elapsed, 3*time.Second, "the unmount got a fresh bound after the readiness wait")
}

// TestReleaseAfterReadinessBoundsTheLazyDetach: the lazy detach runs by the
// end of the same window, so a detach helper that hangs cannot stretch the
// release past it.
func TestReleaseAfterReadinessBoundsTheLazyDetach(t *testing.T) {
	shortenAbandonedRelease(t, 100*time.Millisecond, 2*time.Second)
	helperDir := t.TempDir()
	helper := filepath.Join(helperDir, "fusermount3")
	require.NoError(t, os.WriteFile(helper, []byte("#!/bin/sh\nexec /bin/sleep 30\n"), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	onlyMountHelpersIn(t, helperDir)
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))

	unmounted := make(chan struct{}, 1)
	start := time.Now()
	releaseAfterReadiness(make(chan error), func() error {
		unmounted <- struct{}{}
		return nil
	}, mountPoint, fuseTypeFor(filesystemName))
	require.Less(t, time.Since(start), 3*time.Second, "the lazy detach ran past the release window")
	require.Empty(t, unmounted, "the unmount must be skipped while the readiness check is stuck")
}
