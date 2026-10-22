package fuseadapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDetachFUSEMountBoundsHelperLookup stands in a helper lookup that never
// returns, as exec.LookPath does when a PATH directory sits on a wedged FUSE
// or network filesystem. The lookup must be bounded like the helper itself,
// so the kernel fallback is still attempted inside the caller's budget (here
// on a plain directory, so it fails with its own error, not the deadline's).
func TestDetachFUSEMountBoundsHelperLookup(t *testing.T) {
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	old := lookupMountHelperFn
	lookupMountHelperFn = func(string) (string, error) {
		<-block
		return "", nil
	}
	t.Cleanup(func() { lookupMountHelperFn = old })

	budget := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	method, err := detachFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Empty(t, method)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the hung lookups must not pre-empt the kernel fallback")
	require.ErrorContains(t, err, lazyUnmountMethod)
	require.Less(t, elapsed, budget, "the fallback must run inside the caller's budget")
}

// TestDetachFUSEMountRechecksAfterHelperLookup covers our mount going away
// while the first helper is still being looked up, which exposes a foreign
// FUSE mount at the path. fusermount3 -u -z acts on whatever FUSE
// filesystem the mount point names, so the mounts table must be checked
// again after every lookup, right before the helper runs.
func TestDetachFUSEMountRechecksAfterHelperLookup(t *testing.T) {
	mountPoint := t.TempDir()
	table := useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	helperDir := t.TempDir()
	marker := filepath.Join(helperDir, "helper-ran")
	helper := filepath.Join(helperDir, "fusermount3")
	require.NoError(t, os.WriteFile(helper, []byte("#!/bin/sh\n: > "+marker+"\nexit 0\n"), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	old := lookupMountHelperFn
	lookupMountHelperFn = func(string) (string, error) {
		// The plain unmount finishes while the lookup runs and a foreign
		// FUSE filesystem is now on top at the path.
		// It runs on the lookup goroutine, so it reports with Errorf.
		if err := os.WriteFile(table, []byte(mountTableLine("sshfs#host:/", mountPoint, "fuse.sshfs")), 0o600); err != nil {
			t.Errorf("rewrite mounts table: %v", err)
		}
		return helper, nil
	}
	t.Cleanup(func() { lookupMountHelperFn = old })

	method, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
	require.ErrorIs(t, err, errNoFUSEMount)
	require.Empty(t, method)
	require.NotContains(t, err.Error(), lazyUnmountMethod)
	require.NoFileExists(t, marker, "no helper may run against the exposed foreign mount")
}
