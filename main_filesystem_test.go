package main

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type testFilesystemLeaseReaper struct {
	cancel context.CancelFunc
	calls  int
	limits []int
	err    error
}

func (r *testFilesystemLeaseReaper) ReapExpiredOpenHandleLeases(
	_ context.Context,
	limit int,
) (filesystem.LeaseReapStats, error) {
	r.calls++
	r.limits = append(r.limits, limit)
	if r.calls == 2 {
		r.cancel()
	}
	if r.err != nil {
		return filesystem.LeaseReapStats{}, r.err
	}
	return filesystem.LeaseReapStats{ExpiredRefs: 1}, nil
}

func TestRunFilesystemLeaseReaperRunsPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reaper := &testFilesystemLeaseReaper{cancel: cancel}

	require.NoError(t, runFilesystemLeaseReaper(ctx, reaper, time.Millisecond))
	require.Equal(t, 2, reaper.calls)
	require.Equal(t, []int{0, 0}, reaper.limits)
}

func TestFilesystemStartConfigFromFlags(t *testing.T) {
	oldMount, oldClientID, oldRaftID := *filesystemMount, *filesystemClientID, *raftId
	oldMode, oldUID, oldGID := *filesystemRootMode, *filesystemRootUID, *filesystemRootGID
	t.Cleanup(func() {
		*filesystemMount, *filesystemClientID, *raftId = oldMount, oldClientID, oldRaftID
		*filesystemRootMode, *filesystemRootUID, *filesystemRootGID = oldMode, oldUID, oldGID
	})

	tests := []struct {
		name     string
		mount    string
		clientID string
		raftID   string
		mode     uint
		uid      uint
		gid      uint
		want     filesystemStartConfig
		wantErr  string
	}{
		{
			name:  "disabled when mount point is blank",
			mount: "   ",
			mode:  0o7777 + 1,
			want:  filesystemStartConfig{},
		},
		{
			name:   "client id defaults to raft id",
			mount:  " /mnt/fs ",
			raftID: " n1 ",
			mode:   0o750,
			uid:    1000,
			gid:    1001,
			want:   filesystemStartConfig{mountPoint: "/mnt/fs", clientID: "n1", rootMode: 0o750, rootUID: 1000, rootGID: 1001},
		},
		{
			name:     "explicit client id wins",
			mount:    "/mnt/fs",
			clientID: "mount-a",
			raftID:   "n1",
			mode:     0o755,
			want:     filesystemStartConfig{mountPoint: "/mnt/fs", clientID: "mount-a", rootMode: 0o755},
		},
		{
			name:    "client id required",
			mount:   "/mnt/fs",
			mode:    0o755,
			wantErr: "client ID is required",
		},
		{
			name:    "root mode out of range",
			mount:   "/mnt/fs",
			raftID:  "n1",
			mode:    0o7777 + 1,
			wantErr: "out of range",
		},
		{
			name:    "root uid out of range",
			mount:   "/mnt/fs",
			raftID:  "n1",
			mode:    0o755,
			uid:     math.MaxUint32 + 1,
			wantErr: "out of range",
		},
		{
			name:    "root gid out of range",
			mount:   "/mnt/fs",
			raftID:  "n1",
			mode:    0o755,
			gid:     math.MaxUint32 + 1,
			wantErr: "out of range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			*filesystemMount, *filesystemClientID, *raftId = tt.mount, tt.clientID, tt.raftID
			*filesystemRootMode, *filesystemRootUID, *filesystemRootGID = tt.mode, tt.uid, tt.gid

			got, err := filesystemStartConfigFromFlags()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, filesystemStartConfig{}, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStartFilesystemIfEnabledIsNoopWithoutMountPoint(t *testing.T) {
	oldMount := *filesystemMount
	t.Cleanup(func() { *filesystemMount = oldMount })
	*filesystemMount = ""

	require.NoError(t, startFilesystemIfEnabled(context.Background(), nil, nil, nil, nil, nil))
}

func TestStartFilesystemIfEnabledRequiresLifecycle(t *testing.T) {
	oldMount, oldRaftID := *filesystemMount, *raftId
	t.Cleanup(func() { *filesystemMount, *raftId = oldMount, oldRaftID })
	*filesystemMount = t.TempDir()
	*raftId = "n1"

	err := startFilesystemIfEnabled(context.Background(), nil, nil, nil, nil, nil)
	require.ErrorContains(t, err, "filesystem lifecycle is required")
}

func newFilesystemTestService(t *testing.T) *filesystem.Service {
	t.Helper()
	svc, err := fstest.NewMemoryService()
	require.NoError(t, err)
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid()) //nolint:gosec // test process ids fit uint32.
	require.NoError(t, svc.InitializeRoot(context.Background(), defaultFilesystemRootMode, uid, gid))
	return svc
}

func TestMountFilesystemServiceFailsOnMissingMountPoint(t *testing.T) {
	svc := newFilesystemTestService(t)
	config := filesystemStartConfig{
		mountPoint: filepath.Join(t.TempDir(), "missing"),
		clientID:   "mount-a",
	}

	server, serveDone, err := mountFilesystemService(context.Background(), svc, config)
	require.ErrorContains(t, err, "mount filesystem frontend")
	require.Nil(t, server)
	require.Nil(t, serveDone)
}

// TestMountFilesystemServiceAbortsOnCancelledContext pins that a shutdown
// requested while the filesystem is still starting stops the pre-mount work
// instead of letting it run to completion (or to the five-minute recovery
// timeout) while the signal handler swallows repeat signals.
func TestMountFilesystemServiceAbortsOnCancelledContext(t *testing.T) {
	svc := newFilesystemTestService(t)
	config := filesystemStartConfig{mountPoint: t.TempDir(), clientID: "mount-cancel"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server, serveDone, err := mountFilesystemService(ctx, svc, config)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "mount filesystem frontend")
	require.Nil(t, server)
	require.Nil(t, serveDone)
}

// skipOrFailWithoutFuse skips when the FUSE prerequisites are missing unless
// ELASTICKV_REQUIRE_FUSE is set, in which case missing coverage is a failure.
func skipOrFailWithoutFuse(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("ELASTICKV_REQUIRE_FUSE") != "" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

func TestMountFilesystemServiceServesKernelMount(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFailWithoutFuse(t, "no /dev/fuse: %v", err)
	}
	svc := newFilesystemTestService(t)
	config := filesystemStartConfig{mountPoint: t.TempDir(), clientID: "mount-a", mountOptions: testFUSEMountOptions()}

	server, serveDone, err := mountFilesystemService(context.Background(), svc, config)
	if err != nil {
		skipOrFailWithoutFuse(t, "FUSE mount unavailable: %v", err)
	}
	require.NotNil(t, server)
	unmount := sync.OnceValue(server.Unmount)
	t.Cleanup(func() {
		if err := unmount(); err != nil {
			t.Errorf("unmount: %v", err)
		}
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
			t.Error("serve loop did not stop after unmount")
		}
	})

	path := filepath.Join(config.mountPoint, "probe")
	require.NoError(t, os.WriteFile(path, []byte("probe"), 0o600))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("probe"), got)

	require.NoError(t, unmount())
	select {
	case <-serveDone:
	case <-time.After(10 * time.Second):
		t.Fatal("serve loop did not stop after unmount")
	}
}

func TestInstallFilesystemServerLifecycleUnmountsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	cleanup := &internalutil.CleanupStack{}
	server := &fuseadapter.Server{}
	serveDone := make(chan struct{})

	installFilesystemServerLifecycle(egCtx, eg, cleanup, server, serveDone, "/mnt/fs")

	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		t.Fatalf("lifecycle returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// Both ctx.Done() and serveDone are ready once the serve loop exits
	// after cancellation; the lifecycle must report a clean shutdown
	// whichever branch the select picks.
	cancel()
	close(serveDone)
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle did not stop after cancellation")
	}
	cleanup.Run()
}

// fakeFilesystemMountServer stands in for *fuseadapter.Server so the unmount
// failure paths of the lifecycle can be driven without a kernel mount.
type fakeFilesystemMountServer struct {
	unmountErr error
	// unmountBlock, when set, makes Unmount block until it is closed, like
	// go-fuse waiting for an event loop stuck in a backend request.
	unmountBlock <-chan struct{}
	lazyErr      error
	onLazy       func()
	lazyCalls    int
}

func (f *fakeFilesystemMountServer) Unmount() error {
	if f.unmountBlock != nil {
		<-f.unmountBlock
	}
	return f.unmountErr
}

func (f *fakeFilesystemMountServer) UnmountLazy(context.Context) error {
	f.lazyCalls++
	if f.onLazy != nil {
		f.onLazy()
	}
	return f.lazyErr
}

func shortenFilesystemServeStopTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := filesystemServeStopTimeout
	filesystemServeStopTimeout = d
	t.Cleanup(func() { filesystemServeStopTimeout = old })
}

// A plain unmount that fails (EBUSY: a client holds a file open) must fall
// back to a lazy detach; the serve loop then ends when the kernel releases
// the connection, here simulated by the fake closing serveDone.
func TestInstallFilesystemServerLifecycleDetachesLazilyWhenUnmountFails(t *testing.T) {
	shortenFilesystemServeStopTimeout(t, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	cleanup := &internalutil.CleanupStack{}
	serveDone := make(chan struct{})
	server := &fakeFilesystemMountServer{unmountErr: errors.New("umount: device or resource busy")}
	server.onLazy = func() { close(serveDone) }

	installFilesystemServerLifecycle(egCtx, eg, cleanup, server, serveDone, "/mnt/fs")
	cancel()

	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle did not stop after the lazy detach released the serve loop")
	}
	require.Equal(t, 1, server.lazyCalls)
	cleanup.Run()
}

// When even the lazy detach cannot end the serve loop (handles stay open),
// the shutdown must proceed after the bounded wait instead of hanging until
// SIGKILL.
func TestInstallFilesystemServerLifecycleBoundsWaitWhenServeLoopStalls(t *testing.T) {
	shortenFilesystemServeStopTimeout(t, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	cleanup := &internalutil.CleanupStack{}
	serveDone := make(chan struct{}) // never closed
	server := &fakeFilesystemMountServer{
		unmountErr: errors.New("umount: device or resource busy"),
		lazyErr:    errors.New("fusermount: not permitted"),
	}

	installFilesystemServerLifecycle(egCtx, eg, cleanup, server, serveDone, "/mnt/fs")
	cancel()

	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle hung on a serve loop that never stops")
	}
	require.Equal(t, 1, server.lazyCalls)
	cleanup.Run()
}

// go-fuse's Unmount waits for its event loops; one stuck in a backend
// request never returns. The shutdown must not wait on it past the budget.
func TestInstallFilesystemServerLifecycleBoundsHungUnmount(t *testing.T) {
	shortenFilesystemServeStopTimeout(t, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	cleanup := &internalutil.CleanupStack{}
	serveDone := make(chan struct{}) // never closed
	block := make(chan struct{})
	release := sync.OnceFunc(func() { close(block) })
	t.Cleanup(release)
	server := &fakeFilesystemMountServer{unmountBlock: block}

	installFilesystemServerLifecycle(egCtx, eg, cleanup, server, serveDone, "/mnt/fs")
	cancel()

	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle hung on an Unmount that never returns")
	}
	require.Equal(t, 1, server.lazyCalls)
	// cleanup.Run calls the once-wrapped unmount, which waits for the first,
	// still-blocked call; release it first.
	release()
	cleanup.Run()
}

func TestInstallFilesystemServerLifecycleTreatsServeExitAfterCancelAsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	cleanup := &internalutil.CleanupStack{}
	serveDone := make(chan struct{})

	// Cancel and close before the lifecycle goroutine can run, so its select
	// sees both cases ready from the start.
	cancel()
	close(serveDone)
	installFilesystemServerLifecycle(egCtx, eg, cleanup, &fuseadapter.Server{}, serveDone, "/mnt/fs")

	require.NoError(t, eg.Wait())
	cleanup.Run()
}

func TestInstallFilesystemServerLifecycleFailsWhenServeStops(t *testing.T) {
	eg, egCtx := errgroup.WithContext(context.Background())
	cleanup := &internalutil.CleanupStack{}
	serveDone := make(chan struct{})

	installFilesystemServerLifecycle(egCtx, eg, cleanup, &fuseadapter.Server{}, serveDone, "/mnt/fs")
	close(serveDone)

	err := eg.Wait()
	require.ErrorContains(t, err, "filesystem FUSE server stopped unexpectedly")
	cleanup.Run()
}

func TestStartFilesystemPlacementCollectorStopsOnCancel(t *testing.T) {
	svc := newFilesystemTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)

	startFilesystemPlacementCollector(egCtx, eg, svc, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	cancel()

	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("placement collector did not stop after cancellation")
	}
}

func TestFilesystemBackgroundLoopsSkipWhenDisabled(t *testing.T) {
	svc := newFilesystemTestService(t)
	eg := &errgroup.Group{}

	startFilesystemPlacementCollector(context.Background(), nil, svc, time.Second)
	startFilesystemPlacementCollector(context.Background(), eg, nil, time.Second)
	startFilesystemPlacementCollector(context.Background(), eg, svc, 0)
	startFilesystemLeaseReaper(context.Background(), nil, svc, time.Second)
	startFilesystemLeaseReaper(context.Background(), eg, nil, time.Second)
	startFilesystemLeaseReaper(context.Background(), eg, svc, -time.Second)

	require.NoError(t, eg.Wait())
}

func TestStartFilesystemLeaseReaperStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	reaper := &testFilesystemLeaseReaper{cancel: cancel}

	startFilesystemLeaseReaper(egCtx, eg, reaper, time.Millisecond)

	require.NoError(t, eg.Wait())
	require.GreaterOrEqual(t, reaper.calls, 2)
}

func TestRunFilesystemLeaseReaperLogsErrorsAndContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reaper := &testFilesystemLeaseReaper{cancel: cancel, err: errors.New("boom")}

	require.NoError(t, runFilesystemLeaseReaper(ctx, reaper, time.Millisecond))
	require.Equal(t, 2, reaper.calls)
}
