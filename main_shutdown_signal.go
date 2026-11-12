package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

// shutdownSignals are the signals that start a graceful stop. Without a
// handler Go's default disposition ends the process immediately, which
// skips the CleanupStack: the WAL is not synced and a FUSE mount stays
// attached to a dead server (`Transport endpoint is not connected`).
var shutdownSignals = []os.Signal{syscall.SIGTERM, syscall.SIGINT}

// shutdownSignalHandlerJoinTimeout bounds how long stopping the signal
// handler waits for its goroutine, which a blocked log write can hold.
const shutdownSignalHandlerJoinTimeout = time.Second

// signalShutdown is set by the signal handler once a shutdown signal has
// requested a stop, so main can tell a requested stop from a
// failure when the errgroup surfaces context.Canceled while unwinding.
var signalShutdown atomic.Bool

// grpcGracefulStopTimeout bounds how long a gRPC server's GracefulStop may
// wait for in-flight RPCs during shutdown. Long-lived streams (catalog
// watches, the Raft transport's SendStream) only end when their client goes
// away, so without a bound a SIGTERM would wait on them for as long as a
// peer stays connected while repeat signals are swallowed. After the bound,
// Stop closes the remaining streams. A variable so tests can shorten it.
var grpcGracefulStopTimeout = 10 * time.Second

// stopGRPCServerBounded drains srv for at most grpcGracefulStopTimeout and
// then forces Stop, which also makes the pending GracefulStop return.
func stopGRPCServerBounded(ctx context.Context, srv *grpc.Server, name string) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(grpcGracefulStopTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		slog.WarnContext(ctx, "gRPC graceful stop did not finish in time, closing remaining streams",
			"server", name,
			"timeout", grpcGracefulStopTimeout,
		)
		srv.Stop()
		<-done
	}
}

// installShutdownSignalHandler routes SIGTERM/SIGINT (or the given signals)
// into the supplied shutdown callback. Cleanup follows the memory watchdog's
// path: the errgroup drains, the FUSE mount is unmounted, and the
// CleanupStack syncs the WAL. Startup passes an immediate cancellation;
// after startup, the callback requests a leadership handoff first. Later signals
// are logged and ignored so a second SIGTERM from a supervisor cannot
// cut the shutdown short (SIGKILL still can, which no handler prevents).
// The returned function releases the registration and must be called once
// the cleanup that needs protection has run.
func installShutdownSignalHandler(cancel context.CancelFunc, signals ...os.Signal) func() {
	if len(signals) == 0 {
		signals = shutdownSignals
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, signals...)
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case sig := <-sigCh:
				if signalShutdown.CompareAndSwap(false, true) {
					// Request shutdown before logging: a log write that blocks (a
					// backpressured stderr, a synchronous shipper) must not
					// hold the shutdown back, and later signals are ignored.
					cancel()
					slog.Info("shutdown signal received, stopping", "signal", sig.String())
					continue
				}
				slog.Warn("shutdown already in progress, ignoring repeated signal", "signal", sig.String())
			case <-stopCh:
				return
			}
		}
	}()
	return sync.OnceFunc(func() {
		signal.Stop(sigCh)
		close(stopCh)
		// Wait for the goroutine, but bounded: it may be stuck in a log
		// write that never completes, and this runs at the very end of the
		// shutdown, which must not hang on it.
		timer := time.NewTimer(shutdownSignalHandlerJoinTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
		}
	})
}
