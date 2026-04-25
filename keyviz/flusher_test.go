package keyviz

import (
	"context"
	"testing"
	"time"
)

func TestRunFlusherTicksUntilCancel(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: 5 * time.Millisecond, HistoryColumns: 16})
	if !s.RegisterRoute(1, []byte("a"), []byte("b")) {
		t.Fatal("Register failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunFlusher(ctx, s, 5*time.Millisecond)
	}()

	// Drive Observe across ticker firings.
	for i := 0; i < 10; i++ {
		s.Observe(1, OpRead, 0, 0)
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunFlusher did not return after cancel")
	}

	cols := s.Snapshot(time.Time{}, time.Time{})
	if len(cols) == 0 {
		t.Fatal("expected at least one column from background flushes")
	}
}

// TestRunFlusherNilSamplerWaitsCtx asserts the nil-sampler contract
// (RunFlusher just blocks on ctx.Done so callers can hard-wire it
// regardless of whether keyviz is enabled).
func TestRunFlusherNilSamplerWaitsCtx(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunFlusher(ctx, nil, time.Millisecond)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunFlusher(nil) did not return on cancel")
	}
}
