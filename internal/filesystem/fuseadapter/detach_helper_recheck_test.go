package fuseadapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDetachFUSEMountRechecksBeforeSecondHelper covers fusermount3 detaching
// our mount and still reporting a failure (a descendant holding its output
// pipe past WaitDelay, for example), which exposes a foreign FUSE mount at
// the path. The legacy fusermount helper accepts any FUSE filesystem, so it
// must not run once the mounts table no longer shows our mount on top.
func TestDetachFUSEMountRechecksBeforeSecondHelper(t *testing.T) {
	mountPoint := t.TempDir()
	table := useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	helperDir := t.TempDir()
	replacement := filepath.Join(helperDir, "table.after")
	require.NoError(t, os.WriteFile(replacement, []byte(mountTableLine("sshfs#host:/", mountPoint, "fuse.sshfs")), 0o600))
	first := "#!/bin/sh\n/bin/cp " + replacement + " " + table + "\nexit 1\n"
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "fusermount3"), []byte(first), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	marker := filepath.Join(helperDir, "second-ran")
	second := "#!/bin/sh\n/bin/touch " + marker + "\nexit 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "fusermount"), []byte(second), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	onlyMountHelpersIn(t, helperDir)

	method, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
	require.ErrorIs(t, err, errNoFUSEMount)
	require.ErrorContains(t, err, "fusermount3 -u -z", "the first helper's failure stays in the chain")
	require.NotContains(t, err.Error(), lazyUnmountMethod)
	require.Empty(t, method)
	require.NoFileExists(t, marker, "the second helper must not run against the exposed foreign mount")
}
