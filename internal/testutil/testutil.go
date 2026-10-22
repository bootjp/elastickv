// Package testutil provides bounded waits and loopback listeners for tests.
package testutil

import (
	"context"
	"net"
	"testing"
	"time"
)

// Recv receives one value, failing if ch closes or the deadline expires.
func Recv[T any](tb testing.TB, ch <-chan T, d time.Duration, format string, args ...any) T {
	tb.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case value, ok := <-ch:
		if !ok {
			tb.Fatalf("channel closed: "+format, args...)
		}
		return value
	case <-timer.C:
		tb.Fatalf("receive timed out: "+format, args...)
		var zero T
		return zero
	}
}

// NoRecv asserts that ch stays open and produces no value during d.
func NoRecv[T any](tb testing.TB, ch <-chan T, d time.Duration, format string, args ...any) {
	tb.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ch:
		tb.Fatalf("unexpected receive or close: "+format, args...)
	case <-timer.C:
	}
}

// Closed requires ch to close without producing another value during d.
func Closed[T any](tb testing.TB, ch <-chan T, d time.Duration, format string, args ...any) {
	tb.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case _, ok := <-ch:
		if ok {
			tb.Fatalf("expected closed channel, received value: "+format, args...)
		}
	case <-timer.C:
		tb.Fatalf("close timed out: "+format, args...)
	}
}

// Listen binds an ephemeral IPv4 loopback port and closes it on cleanup.
func Listen(tb testing.TB) net.Listener {
	tb.Helper()
	var config net.ListenConfig
	lis, err := config.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	tb.Cleanup(func() { _ = lis.Close() })
	return lis
}
