package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestShutdownHandoffStartupSignalCancelsBeforeArming(t *testing.T) {
	resetSignalShutdown(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	handoff := newShutdownHandoff(cancel)
	stop := installShutdownSignalHandler(handoff.requestShutdown, syscall.SIGUSR2)
	t.Cleanup(stop)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGUSR2))
	testutil.Closed(t, ctx.Done(), shutdownTestTimeout, "a signal during startup must cancel immediately")
	eg, runCtx := errgroup.WithContext(ctx)
	handoff.arm(runCtx, eg, nil, nil)
	require.NoError(t, eg.Wait(), "arming after startup cancellation must not wait for another signal")
	require.True(t, signalShutdown.Load())
}

func TestShutdownHandoffSignalWaitsForTransferAndIgnoresRepeats(t *testing.T) {
	resetSignalShutdown(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	started := make(chan struct{})
	released := make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	t.Cleanup(release)
	engine := shutdownTestEngine(func(transferCtx context.Context, _ []raftengine.TransferTarget, _ uint64) error {
		close(started)
		select {
		case <-released:
			return nil
		case <-transferCtx.Done():
			return transferCtx.Err()
		}
	})
	handoff := newShutdownHandoff(cancel)
	stop := installShutdownSignalHandler(handoff.requestShutdown, syscall.SIGUSR2)
	t.Cleanup(stop)
	eg, runCtx := errgroup.WithContext(ctx)
	handoff.arm(runCtx, eg, []*raftGroupRuntime{{spec: groupSpec{id: 1}, engine: engine}}, nil)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGUSR2))
	testutil.Closed(t, started, shutdownTestTimeout, "the signal did not start the leadership handoff")
	require.NoError(t, ctx.Err(), "root cancellation must wait until the handoff finishes")
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGUSR2))
	testutil.NoRecv(t, ctx.Done(), shutdownTransferRetryDelay, "a repeated signal canceled the in-flight handoff")
	release()
	done := make(chan error, 1)
	go func() { done <- eg.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(shutdownTestTimeout):
		t.Fatal("shutdown did not finish after the handoff")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.True(t, signalShutdown.Load())
}

func TestShutdownHandoffDeadlineSurvivesBlockedLogging(t *testing.T) {
	handler := &blockingLogHandler{release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(handler.release) })
	old := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() {
		release()
		slog.SetDefault(old)
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error { return nil })
	requests := make(chan struct{})
	close(requests)
	done := make(chan struct{})
	go func() {
		shutdownAfterRequest(ctx, cancel, []*raftGroupRuntime{{engine: engine}}, requests, shutdownTransferRetryDelay, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTestTimeout):
		t.Fatal("a blocked handoff log held back shutdown past its deadline")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
