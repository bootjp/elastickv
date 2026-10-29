package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const shutdownTestTimeout = 15 * time.Second

// resetSignalShutdown clears the process-wide sentinel so tests that raise a
// signal start from the same state main does.
func resetSignalShutdown(t *testing.T) {
	t.Helper()
	signalShutdown.Store(false)
	t.Cleanup(func() { signalShutdown.Store(false) })
}

// testFUSEMountOptions mounts directly when the test runs as root, which
// lets the kernel-mount tests run in containers without fusermount3.
func testFUSEMountOptions() *fuse.MountOptions {
	return &fuse.MountOptions{DirectMount: os.Geteuid() == 0}
}

func TestInstallShutdownSignalHandlerCancelsOnSignal(t *testing.T) {
	resetSignalShutdown(t)
	ctx, cancel := context.WithCancel(context.Background())
	stop := installShutdownSignalHandler(cancel, syscall.SIGTERM)
	t.Cleanup(stop)

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	testutil.Closed(t, ctx.Done(), shutdownTestTimeout, "SIGTERM did not cancel the root context")
	require.True(t, signalShutdown.Load())

	// A repeat signal while the shutdown is in flight must stay with the
	// handler: if the registration had been released, the default
	// disposition would kill this test binary.
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	time.Sleep(50 * time.Millisecond)
	stop()
	stop() // idempotent
}

func TestInstallShutdownSignalHandlerStopReleasesWithoutSignal(t *testing.T) {
	resetSignalShutdown(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stop := installShutdownSignalHandler(cancel, syscall.SIGUSR2)

	stop()
	require.NoError(t, ctx.Err())
	require.False(t, signalShutdown.Load())
}

func TestIgnoreCanceledAfterSignal(t *testing.T) {
	resetSignalShutdown(t)
	wrappedCancel := errors.Wrap(context.Canceled, "failed to serve")
	other := errors.New("listener exploded")
	// What waitErrgroupAfterStartupFailure produces when a signal cancels
	// startup: the startup error joined with the drain error.
	joinedOnlyCanceled := errors.Wrap(errors.Join(
		errors.Wrap(context.Canceled, "wait for raft join"),
		errors.Wrap(context.Canceled, "shutdown failed after startup error"),
	), "startup failed")
	joinedWithFailure := errors.Wrap(errors.Join(
		errors.Wrap(context.Canceled, "wait for raft join"),
		errors.Wrap(other, "shutdown failed after startup error"),
	), "startup failed")
	tests := []struct {
		name     string
		signaled bool
		err      error
		wantNil  bool
	}{
		{name: "nil error", signaled: true, err: nil, wantNil: true},
		{name: "canceled after signal", signaled: true, err: context.Canceled, wantNil: true},
		{name: "wrapped canceled after signal", signaled: true, err: wrappedCancel, wantNil: true},
		{name: "joined cancellations after signal", signaled: true, err: joinedOnlyCanceled, wantNil: true},
		{name: "joined cancellation and failure after signal", signaled: true, err: joinedWithFailure, wantNil: false},
		{name: "other error after signal", signaled: true, err: other, wantNil: false},
		{name: "canceled without signal", signaled: false, err: context.Canceled, wantNil: false},
		{name: "other error without signal", signaled: false, err: other, wantNil: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signalShutdown.Store(tt.signaled)
			got := ignoreCanceledAfterSignal(tt.err)
			if tt.wantNil {
				require.NoError(t, got)
				return
			}
			require.ErrorIs(t, got, tt.err)
		})
	}
	// errors.Is alone would have accepted the mixed join; pin the reason the
	// helper exists.
	require.True(t, errors.Is(joinedWithFailure, context.Canceled))
	require.False(t, onlyContextCanceled(joinedWithFailure))
	require.True(t, onlyContextCanceled(joinedOnlyCanceled))
}

// TestShutdownSignalWithOpenFileDetachesLazily: SIGTERM while a client still
// holds a file open on the mount. The plain unmount fails with EBUSY, so the
// lifecycle must detach lazily and finish the shutdown within the bounded
// wait instead of hanging until SIGKILL; once the handle closes the kernel
// releases the connection and the serve loop ends, leaving a bare directory.
func TestShutdownSignalWithOpenFileDetachesLazily(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFailWithoutFuse(t, "no /dev/fuse: %v", err)
	}
	resetSignalShutdown(t)
	shortenFilesystemServeStopTimeout(t, 2*time.Second)
	svc := newFilesystemTestService(t)
	config := filesystemStartConfig{
		mountPoint:   t.TempDir(),
		clientID:     "mount-busy",
		mountOptions: testFUSEMountOptions(),
	}
	server, serveDone, err := mountFilesystemService(context.Background(), svc, config)
	if err != nil {
		skipOrFailWithoutFuse(t, "FUSE mount unavailable: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cleanup := &internalutil.CleanupStack{}
	t.Cleanup(cleanup.Run)
	cleanup.Add(installShutdownSignalHandler(cancel, syscall.SIGTERM))
	cleanup.Add(cancel)
	eg, egCtx := errgroup.WithContext(ctx)
	installFilesystemServerLifecycle(egCtx, eg, cleanup, server, serveDone, config.mountPoint)

	held, err := os.OpenFile(filepath.Join(config.mountPoint, "held"), os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })
	_, err = held.Write([]byte("held"))
	require.NoError(t, err)

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(shutdownTestTimeout):
		t.Fatal("shutdown hung on a busy FUSE mount")
	}

	// The signal may interrupt FUSE release after the lazy detach. The serve
	// loop below is the shutdown assertion; an interrupted close is expected.
	if err := held.Close(); err != nil {
		require.ErrorIs(t, err, syscall.EINTR)
	}
	select {
	case <-serveDone:
	case <-time.After(shutdownTestTimeout):
		t.Fatal("FUSE serve loop did not stop after the last handle closed")
	}
	entries, err := os.ReadDir(config.mountPoint)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestShutdownSignalUnmountsFilesystem drives the production wiring end to
// end: the signal handler cancels the root context, the errgroup-derived
// context fires, and the filesystem lifecycle unmounts the kernel mount.
func TestShutdownSignalUnmountsFilesystem(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFailWithoutFuse(t, "no /dev/fuse: %v", err)
	}
	resetSignalShutdown(t)
	svc := newFilesystemTestService(t)
	config := filesystemStartConfig{
		mountPoint:   t.TempDir(),
		clientID:     "mount-signal",
		mountOptions: testFUSEMountOptions(),
	}
	server, serveDone, err := mountFilesystemService(context.Background(), svc, config)
	if err != nil {
		skipOrFailWithoutFuse(t, "FUSE mount unavailable: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cleanup := &internalutil.CleanupStack{}
	t.Cleanup(cleanup.Run)
	cleanup.Add(installShutdownSignalHandler(cancel, syscall.SIGTERM))
	cleanup.Add(cancel)
	eg, egCtx := errgroup.WithContext(ctx)
	installFilesystemServerLifecycle(egCtx, eg, cleanup, server, serveDone, config.mountPoint)

	probe := filepath.Join(config.mountPoint, "probe")
	require.NoError(t, os.WriteFile(probe, []byte("probe"), 0o600))

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(shutdownTestTimeout):
		t.Fatal("filesystem lifecycle did not stop after SIGTERM")
	}
	require.True(t, signalShutdown.Load())
	select {
	case <-serveDone:
	case <-time.After(shutdownTestTimeout):
		t.Fatal("FUSE serve loop did not stop after SIGTERM")
	}

	// The mount is gone: the path is the bare temp directory again, so the
	// probe written through the mount is no longer visible and stat works.
	require.NoError(t, ignoreCanceledAfterSignal(egCtx.Err()))
	entries, err := os.ReadDir(config.mountPoint)
	require.NoError(t, err)
	require.Empty(t, entries)
}
