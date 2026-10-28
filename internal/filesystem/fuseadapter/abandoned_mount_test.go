//go:build linux

package fuseadapter

import (
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

// TestReleaseAbandonedMountReturns mounts for real and hands the server to
// releaseAbandonedMount the way newServerContext does after a cancellation.
// go-fuse's Unmount waits for an event loop that only Serve runs, so a
// release that skipped Serve would never return; the mount point must also
// be a plain directory again afterwards, and a request in flight on the
// late mount must have been cancelled.
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
	fs := NewRawFileSystem(adapter)
	server, err := fuse.NewServer(fs, mountPoint, opts)
	if err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
			t.Fatalf("FUSE mount unavailable: %v", err)
		}
		t.Skipf("FUSE mount unavailable: %v", err)
	}
	results := make(chan newServerResult, 1)
	results <- newServerResult{server: server}
	// A request a client sent to the late mount before it is released.
	inFlight, done := fs.requestContext(nil)
	defer done()

	released := make(chan struct{})
	go func() {
		releaseAbandonedMount(results, fs, mountPoint, fuseTypeFor(opts.Name))
		close(released)
	}()
	testutil.Closed(t, released, 15*time.Second, "releaseAbandonedMount did not return; Unmount is waiting for a serve loop that never ran")
	// The release cancels the requests in flight, so a handler blocked in
	// the backend returns before the stores behind it are closed.
	require.Error(t, inFlight.Err(), "the release must cancel the requests in flight on the late mount")

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
