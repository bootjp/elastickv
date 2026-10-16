//go:build linux

package fuseadapter

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

// getAttrBlockingCore passes everything through to the wrapped backend
// except GetAttr, which once armed blocks until release or until its request
// context ends, the way a backend read does while no leader is available.
type getAttrBlockingCore struct {
	Core
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *getAttrBlockingCore) GetAttr(ctx context.Context, inode uint64) (filesystem.Stat, error) {
	if c.armed.Load() {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-c.release:
		case <-ctx.Done():
			return filesystem.Stat{}, ctx.Err() //nolint:wrapcheck // test double reports the cancellation as is.
		}
	}
	return c.Core.GetAttr(ctx, inode) //nolint:wrapcheck // pass-through test double.
}

// TestWaitMountStopsWhenCancelledWhileReadinessBlocks mounts for real and
// blocks the backend getattr that go-fuse's readiness check (the poll hack)
// triggers through the kernel's permission check on the root. Cancelling the
// MountContext context must end WaitMount, cancel the in-flight backend
// request so the kernel gets its answer (the caller of a request already
// sent to the server waits for it uninterruptibly, and here that caller is
// this very process), release the mount within its bound, and let Unmount
// and the serve loop return, so a SIGTERM during that window still reaches
// the bounded shutdown and the WAL sync.
func TestWaitMountStopsWhenCancelledWhileReadinessBlocks(t *testing.T) {
	old := abandonedUnmountTimeout
	abandonedUnmountTimeout = 200 * time.Millisecond
	t.Cleanup(func() { abandonedUnmountTimeout = old })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := mountWithBlockedReadiness(t, ctx)

	cancel()
	select {
	case err := <-m.waitDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("WaitMount did not return after its context was cancelled")
	}
	unmountDone := make(chan error, 1)
	go func() { unmountDone <- m.server.Unmount() }()
	select {
	case err := <-unmountDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Unmount blocked on the mount WaitMount had already released")
	}
	select {
	case <-m.serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the mount was released")
	}
	top, found, err := topmostMount(context.Background(), mountTablePath, m.mountPoint)
	require.NoError(t, err)
	require.False(t, found && top.fsType == m.server.fsType, "the released mount is still attached")
}

// blockedReadinessMount is a kernel mount whose WaitMount is stuck in the
// backend getattr of go-fuse's readiness check.
type blockedReadinessMount struct {
	server     *Server
	mountPoint string
	serveDone  <-chan struct{}
	waitDone   <-chan error
}

// mountWithBlockedReadiness mounts under ctx, arms the getattr block, starts
// Serve and WaitMount, and returns once the readiness check is blocked in
// the backend with WaitMount still waiting.
func mountWithBlockedReadiness(t *testing.T, ctx context.Context) blockedReadinessMount {
	t.Helper()
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
	// The mount point is created before the release is registered so that
	// the release runs first at cleanup: removing the directory stats it,
	// and that getattr must not block.
	mountPoint := t.TempDir()
	core := &getAttrBlockingCore{Core: svc, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(core.release) })
	server, err := MountContext(ctx, mountPoint, New(core, []byte("readiness")), &fuse.MountOptions{DirectMount: os.Geteuid() == 0})
	if err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
			t.Fatalf("FUSE mount unavailable: %v", err)
		}
		t.Skipf("FUSE mount unavailable: %v", err)
	}
	core.armed.Store(true)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	waitDone := make(chan error, 1)
	go func() { waitDone <- server.WaitMount() }()

	select {
	case <-core.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the readiness check never reached the backend getattr")
	}
	select {
	case err := <-waitDone:
		t.Fatalf("WaitMount returned (%v) while the backend getattr was blocked", err)
	case <-time.After(100 * time.Millisecond):
	}
	return blockedReadinessMount{server: server, mountPoint: mountPoint, serveDone: serveDone, waitDone: waitDone}
}
