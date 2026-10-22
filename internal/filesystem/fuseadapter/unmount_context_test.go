package fuseadapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrdinaryUnmountWithoutMountTableUsesBoundedHelper(t *testing.T) {
	helperDir := t.TempDir()
	marker := filepath.Join(helperDir, "called")
	t.Setenv("ELASTICKV_ORDINARY_MARKER", marker)
	script := "#!/bin/sh\nprintf '%s' \"$*\" > \"$ELASTICKV_ORDINARY_MARKER\"\n"
	//nolint:gosec // Executable helper fixture, isolated in the test's temporary directory.
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "umount"), []byte(script), 0o700))
	onlyMountHelpersIn(t, helperDir)
	table := useFakeDetachMountTable(t, "")
	require.NoError(t, os.Remove(table))
	mountPoint := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, unmountFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName)))
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, mountPoint, string(data), "ordinary umount receives no force/lazy flags")
	_, err = detachFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	require.Error(t, err, "lazy detach must still refuse an unverifiable mount")
}

func TestAbandonedReleaseReapsOrdinaryHelperBeforeDetach(t *testing.T) {
	shortenAbandonedRelease(t, 2*time.Second, 3*time.Second)
	helperDir := t.TempDir()
	detached, late := filepath.Join(helperDir, "detached"), filepath.Join(helperDir, "late")
	t.Setenv("ELASTICKV_DETACHED_MARKER", detached)
	t.Setenv("ELASTICKV_LATE_MARKER", late)
	script := "#!/bin/sh\nif [ \"$2\" = '-z' ]; then : > \"$ELASTICKV_DETACHED_MARKER\"; exit 0; fi\n( /bin/sleep 1; : > \"$ELASTICKV_LATE_MARKER\" ) &\nexec /bin/sleep 5\n"
	//nolint:gosec // Executable helper fixture, isolated in the test's temporary directory.
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "fusermount3"), []byte(script), 0o700))
	onlyMountHelpersIn(t, helperDir)
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	releaseAbandoned(func(ctx context.Context) error {
		return unmountFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	}, mountPoint, fuseTypeFor(filesystemName))
	require.FileExists(t, detached)
	time.Sleep(1100 * time.Millisecond)
	require.NoFileExists(t, late, "ordinary helper descendants must not survive the lazy detach")
}
