package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
)

// blockingLogHandler blocks every record until release is closed, the way
// a write to a backpressured stderr pipe or a synchronous log shipper does.
type blockingLogHandler struct {
	release chan struct{}
}

func (h *blockingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *blockingLogHandler) Handle(context.Context, slog.Record) error {
	<-h.release
	return nil
}

func (h *blockingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *blockingLogHandler) WithGroup(string) slog.Handler { return h }

// The first signal must start the shutdown before the handler logs it: a
// log write that blocks would otherwise hold the cancellation back, and the
// signals that follow are swallowed on purpose.
func TestInstallShutdownSignalHandlerCancelsBeforeLogging(t *testing.T) {
	resetSignalShutdown(t)
	handler := &blockingLogHandler{release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(handler.release) })
	old := slog.Default()
	slog.SetDefault(slog.New(handler))
	ctx, cancel := context.WithCancel(context.Background())
	stop := installShutdownSignalHandler(cancel, syscall.SIGUSR2)
	t.Cleanup(func() {
		release()
		stop()
		slog.SetDefault(old)
	})

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGUSR2))
	testutil.Closed(t, ctx.Done(), shutdownTestTimeout, "a blocked log write held back the shutdown")
}

// The cleanup that stops the handler must not wait forever on a goroutine
// stuck in that log write: it runs at the end of the shutdown, after the
// WAL sync, and a hang there would keep the process from exiting.
func TestInstallShutdownSignalHandlerStopDoesNotWaitOnBlockedLog(t *testing.T) {
	resetSignalShutdown(t)
	handler := &blockingLogHandler{release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(handler.release) })
	old := slog.Default()
	slog.SetDefault(slog.New(handler))
	ctx, cancel := context.WithCancel(context.Background())
	stop := installShutdownSignalHandler(cancel, syscall.SIGUSR2)
	t.Cleanup(func() {
		release()
		slog.SetDefault(old)
	})

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGUSR2))
	testutil.Closed(t, ctx.Done(), shutdownTestTimeout, "the signal did not cancel the shutdown")
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	testutil.Closed(t, stopped, shutdownTestTimeout, "stopping the handler waited on the goroutine stuck in the log write")
}
