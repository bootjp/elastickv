//go:build linux

package fuseadapter

import (
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

// TestReleaseAbandonedMountReturns mounts for real and hands the server to
// releaseAbandonedMount the way newServerContext does after a cancellation.
// go-fuse's Unmount waits for an event loop that only Serve runs, so a
// release that skipped Serve would never return; the mount point must also
// be a plain directory again afterwards.
func TestReleaseAbandonedMountReturns(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
			t.Fatalf("no /dev/fuse: %v", err)
		}
		t.Skipf("no /dev/fuse: %v", err)
	}
	svc, err := fstest.NewMemoryService()
	require.NoError(t, err)
	adapter := New(svc, []byte("abandoned"))
	t.Cleanup(adapter.Close)
	mountPoint := t.TempDir()
	opts := defaultMountOptions(&fuse.MountOptions{DirectMount: os.Geteuid() == 0})
	server, err := fuse.NewServer(NewRawFileSystem(adapter), mountPoint, opts)
	if err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
			t.Fatalf("FUSE mount unavailable: %v", err)
		}
		t.Skipf("FUSE mount unavailable: %v", err)
	}
	results := make(chan newServerResult, 1)
	results <- newServerResult{server: server}

	released := make(chan struct{})
	go func() {
		releaseAbandonedMount(results, mountPoint, fuseTypeFor(opts.Name))
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(15 * time.Second):
		t.Fatal("releaseAbandonedMount did not return; Unmount is waiting for a serve loop that never ran")
	}

	entries, err := readMountTable(mountTablePath)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, mountPoint, entry.mountPoint, "abandoned mount is still attached")
	}
}

// TestMountContextTypeFollowsMountName mounts for real with an FsName that
// differs from Name. go-fuse records the mount as "fuse."+Name (the
// subtype; FsName only fills the source column), so the type the server
// remembers for its stale-mount check and lazy detach must be built from
// Name: built from FsName it would never match the server's own mount.
func TestMountContextTypeFollowsMountName(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
			t.Fatalf("no /dev/fuse: %v", err)
		}
		t.Skipf("no /dev/fuse: %v", err)
	}
	svc, err := fstest.NewMemoryService()
	require.NoError(t, err)
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid()) //nolint:gosec // test process ids fit uint32.
	require.NoError(t, svc.InitializeRoot(t.Context(), 0o755, uid, gid))
	mountPoint := t.TempDir()
	server, err := MountContext(t.Context(), mountPoint, New(svc, []byte("subtype")), &fuse.MountOptions{
		DirectMount: os.Geteuid() == 0,
		FsName:      "elastickv-source",
		Name:        "elastickv-subtype",
	})
	if err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
			t.Fatalf("FUSE mount unavailable: %v", err)
		}
		t.Skipf("FUSE mount unavailable: %v", err)
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	t.Cleanup(func() {
		require.NoError(t, server.Unmount())
		<-serveDone
	})
	require.NoError(t, server.WaitMount())

	top, found, err := topmostMount(t.Context(), mountTablePath, mountPoint)
	require.NoError(t, err)
	require.True(t, found, "the mount must be listed")
	require.Equal(t, "fuse.elastickv-subtype", top.fsType, "go-fuse records the subtype as the type")
	require.Equal(t, top.fsType, server.fsType, "the server must remember the type the kernel lists")
}
