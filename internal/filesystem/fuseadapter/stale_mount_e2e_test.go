package fuseadapter_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/cockroachdb/errors"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

const (
	// staleMountHelperEnv names the mount point a helper copy of this test
	// binary mounts and then holds until it is killed.
	staleMountHelperEnv = "ELASTICKV_FUSE_STALE_MOUNT_HELPER"
	staleMountHelperOK  = "mounted"
	// staleMountHelperLifetime bounds how long an orphaned helper keeps its
	// mount if the parent test dies before killing it.
	staleMountHelperLifetime = 2 * time.Minute
	staleMountWait           = 30 * time.Second
	staleMountPoll           = 50 * time.Millisecond
)

// TestHelperProcessHoldsMount is the entry point of the helper process used
// by the stale-mount tests. It is a no-op in the normal test run and only
// does work when staleMountHelperEnv is set: it mounts an in-memory
// filesystem at that path, reports readiness on stdout, and blocks until the
// parent kills it, which leaves a dangling FUSE mount behind.
func TestHelperProcessHoldsMount(t *testing.T) {
	mountPoint := os.Getenv(staleMountHelperEnv)
	if mountPoint == "" {
		t.Skip("helper entry point; only runs in the spawned helper process")
	}
	svc, err := fstest.NewMemoryService(filesystem.WithChunkSize(e2eChunkSize))
	require.NoError(t, err)
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid()) //nolint:gosec // test process ids fit uint32.
	require.NoError(t, svc.InitializeRoot(t.Context(), e2eRootMode, uid, gid))
	adapter := fuseadapter.New(svc, []byte("stale-helper"))
	server, err := fuseadapter.Mount(mountPoint, adapter, &fuse.MountOptions{DirectMount: os.Geteuid() == 0})
	require.NoError(t, err)
	go server.Serve()
	require.NoError(t, server.WaitMount())
	fmt.Println(staleMountHelperOK)
	// Hold the mount; the parent SIGKILLs us. Exit without unmounting if the
	// parent never does, so a lost parent cannot leak the helper forever.
	time.Sleep(staleMountHelperLifetime)
	os.Exit(1)
}

// spawnDanglingMount mounts a FUSE filesystem from a helper process at a
// fresh directory, kills the helper the way a crashed or SIGKILLed server
// dies, and returns the path once the kernel reports the mount as dead
// (ENOTCONN, "Transport endpoint is not connected").
func spawnDanglingMount(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFail(t, "no /dev/fuse: %v", err)
	}
	mountPoint := t.TempDir()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperProcessHoldsMount$", "-test.count=1") //nolint:gosec // re-executes this test binary.
	cmd.Env = append(os.Environ(), staleMountHelperEnv+"="+mountPoint)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		// Whatever the test left behind, never leak a dangling mount into
		// the next test or the host.
		_ = syscall.Unmount(mountPoint, syscall.MNT_DETACH)
	})

	ready := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		for {
			line, err := reader.ReadString('\n')
			if strings.TrimSpace(line) == staleMountHelperOK {
				ready <- nil
				return
			}
			if err != nil {
				ready <- errors.Wrapf(err, "helper exited before mounting: %s", stderr.String())
				return
			}
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			skipOrFail(t, "FUSE mount unavailable in helper: %v", err)
		}
	case <-time.After(staleMountWait):
		t.Fatalf("helper did not report its mount in time; stderr: %s", stderr.String())
	}

	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	require.Eventually(t, func() bool {
		var st syscall.Stat_t
		return errors.Is(syscall.Stat(mountPoint, &st), syscall.ENOTCONN)
	}, staleMountWait, staleMountPoll, "killed helper did not leave a dangling mount")
	return mountPoint
}

// TestMountDetachesStaleMountLeftByKilledServer reproduces the restart
// failure from the operations guide: a server killed without unmounting
// leaves `Transport endpoint is not connected` at the mount point and the
// next Mount used to fail. Mount now detaches the dead mount and serves.
func TestMountDetachesStaleMountLeftByKilledServer(t *testing.T) {
	mountPoint := spawnDanglingMount(t)

	mountMemoryFilesystemAt(t, mountPoint)

	path := filepath.Join(mountPoint, "after-restart")
	require.NoError(t, os.WriteFile(path, []byte("recovered"), 0o600))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("recovered"), got)
}

// TestKernelMountUnmountLeavesPlainDirectory guards the Mount-time detach
// against false positives: a mount point that was unmounted cleanly holds no
// FUSE entry, so remounting there must not try to detach anything and the
// directory must be usable as an ordinary directory in between.
func TestKernelMountUnmountLeavesPlainDirectory(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFail(t, "no /dev/fuse: %v", err)
	}
	mountPoint := t.TempDir()
	mount := mountMemoryFilesystemServer(t, mountPoint)
	require.NoError(t, os.WriteFile(filepath.Join(mountPoint, "probe"), []byte("x"), 0o600))
	require.NoError(t, mount.unmount())
	select {
	case <-mount.serveDone:
	case <-time.After(staleMountWait):
		t.Fatal("FUSE server did not stop after unmount")
	}

	entries, err := os.ReadDir(mountPoint)
	require.NoError(t, err)
	require.Empty(t, entries)

	mountMemoryFilesystemAt(t, mountPoint)
	entries, err = os.ReadDir(mountPoint)
	require.NoError(t, err)
	require.Empty(t, entries, "fresh in-memory filesystem starts empty")
}
