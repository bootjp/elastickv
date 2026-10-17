package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

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
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTestTimeout):
		t.Fatal("a blocked log write held back the shutdown")
	}
}
