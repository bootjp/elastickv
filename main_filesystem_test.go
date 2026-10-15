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

	server, serveDone, err := mountFilesystemService(svc, config)
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

	server, serveDone, err := mountFilesystemService(svc, config)
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
