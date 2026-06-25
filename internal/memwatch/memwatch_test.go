package memwatch

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// waitForDone asserts the watcher's goroutine exits within d, returning
// immediately on success and failing the test on timeout. Using the
// Watcher.Done() channel avoids the goleak dep and keeps the check local.
func waitForDone(t *testing.T, w *Watcher, d time.Duration) {
	t.Helper()
	select {
	case <-w.Done():
	case <-time.After(d):
		t.Fatalf("watcher goroutine did not exit within %v", d)
	}
}

// TestWatcher_FiresOnceAboveThreshold creates a watcher with a threshold
// so low the current heap is guaranteed to exceed it, verifies OnExceed
// fires, and verifies it fires only once even though the polling loop
// would otherwise observe "over threshold" on every subsequent tick.
func TestWatcher_FiresOnceAboveThreshold(t *testing.T) {
	t.Parallel()

	fired := make(chan struct{}, 8)
	var count atomic.Int32
	w := New(Config{
		// 1 byte threshold: HeapInuse is always > 1B in a live program.
		ThresholdBytes: 1,
		PollInterval:   5 * time.Millisecond,
		OnExceed: func() {
			count.Add(1)
			// non-blocking send: buffered channel so a pathological
			// double-fire is observable via the count, not a deadlock.
			select {
			case fired <- struct{}{}:
			default:
			}
		},
	})

	// Hold a live allocation so HeapInuse cannot collapse mid-test. We
	// touch it after the wait below so the compiler/escape analysis keeps
	// it on the heap for the duration of the test.
	ballast := make([]byte, 1<<20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Start(ctx)

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatalf("OnExceed was not invoked")
	}

	// Give the loop several more poll intervals to prove it does not fire
	// again. The watcher should have already returned on first fire, so
	// this also indirectly tests the loop-exit path.
	time.Sleep(50 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("OnExceed fired %d times, want exactly 1", got)
	}

	// Ensure Start actually returned.
	waitForDone(t, w, time.Second)

	// Keep ballast live past the assertions.
	runtime.KeepAlive(ballast)
}

// TestWatcher_DoesNotFireBelowThreshold runs the watcher for multiple poll
// intervals with a threshold far above any reasonable process HeapInuse
// and verifies OnExceed is never called.
func TestWatcher_DoesNotFireBelowThreshold(t *testing.T) {
	t.Parallel()

	var count atomic.Int32
	w := New(Config{
		// 1 TiB: a Go test binary will not reach this.
		ThresholdBytes: 1 << 40,
		PollInterval:   5 * time.Millisecond,
		OnExceed: func() {
			count.Add(1)
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Start(ctx)

	// Three polls' worth plus a safety margin.
	time.Sleep(30 * time.Millisecond)
	cancel()
	waitForDone(t, w, time.Second)

	if got := count.Load(); got != 0 {
		t.Fatalf("OnExceed fired %d times below threshold, want 0", got)
	}
}

// TestWatcher_StopsOnContextCancel verifies the watcher goroutine exits
// promptly when the supplied context is cancelled, with no leak.
func TestWatcher_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	w := New(Config{
		ThresholdBytes: 1 << 40, // never fires
		PollInterval:   10 * time.Millisecond,
		OnExceed:       func() { t.Fatalf("OnExceed unexpectedly fired") },
	})

	ctx, cancel := context.WithCancel(context.Background())
	go w.Start(ctx)

	// Let the goroutine park in its select.
	time.Sleep(20 * time.Millisecond)
	cancel()
	waitForDone(t, w, time.Second)
}

// TestWatcher_DisabledWhenThresholdZero verifies a threshold of zero
// disables the watcher: Start returns immediately and OnExceed is never
// invoked, even after generous wait time.
func TestWatcher_DisabledWhenThresholdZero(t *testing.T) {
	t.Parallel()

	var count atomic.Int32
	w := New(Config{
		ThresholdBytes: 0,
		PollInterval:   1 * time.Millisecond,
		OnExceed: func() {
			count.Add(1)
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)
	// When disabled, Start must return essentially immediately.
	waitForDone(t, w, 200*time.Millisecond)

	// Allocate something to grow the heap and confirm the (stopped)
	// watcher does not observe it.
	ballast := make([]byte, 4<<20)
	time.Sleep(20 * time.Millisecond)
	runtime.KeepAlive(ballast)

	if got := count.Load(); got != 0 {
		t.Fatalf("OnExceed fired %d times while disabled, want 0", got)
	}
}
