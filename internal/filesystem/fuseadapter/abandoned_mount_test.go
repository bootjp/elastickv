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
		releaseAbandonedMount(results, mountPoint)
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
