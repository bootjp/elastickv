//go:build linux

package fuseadapter

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDetachFUSEMountLeavesForeignMountAlone mounts a tmpfs where a FUSE
// mount might have been and runs the detach against it with the real mounts
// table: the kernel fallback must refuse, and the tmpfs must still be
// mounted afterwards. This is the shutdown case Codex raised: go-fuse's
// Unmount can remove the FUSE mount and then block on its event loop, so the
// path-based lazy detach that follows would otherwise MNT_DETACH whatever
// filesystem the mount point sits on, such as the root of a data volume.
func TestDetachFUSEMountLeavesForeignMountAlone(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounting a tmpfs needs root")
	}
	mountPoint := t.TempDir()
	if err := syscall.Mount("tmpfs", mountPoint, "tmpfs", 0, ""); err != nil {
		t.Skipf("cannot mount a tmpfs here: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(mountPoint, syscall.MNT_DETACH) })
	require.NoError(t, os.WriteFile(filepath.Join(mountPoint, "keep"), []byte("x"), 0o600))
	onlyMountHelpersIn(t, t.TempDir()) // no fusermount helpers: straight to the fallback decision

	method, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
	require.ErrorIs(t, err, errNoFUSEMount)
	require.Empty(t, method)

	top, found, err := topmostMount(context.Background(), mountTablePath, mountPoint)
	require.NoError(t, err)
	require.True(t, found, "the tmpfs must still be mounted")
	require.Equal(t, "tmpfs", top.fsType)
	require.FileExists(t, filepath.Join(mountPoint, "keep"))
}
