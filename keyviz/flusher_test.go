package keyviz

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunFlusherTicksUntilCancel(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: 5 * time.Millisecond, HistoryColumns: 16})
	if !s.RegisterRoute(1, []byte("a"), []byte("b"), 0) {
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
		s.Observe(1, make([]byte, 0), OpRead, 0, LabelLegacy)
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

func TestRunFlusherAttachesAlignedHotKeysWindow(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{
		Step:              5 * time.Millisecond,
		HistoryColumns:    16,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  64,
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 1))

	ctx, cancel := context.WithCancel(context.Background())
	var done = make(chan struct{}, 2)
	go func() {
		RunHotKeysAggregator(ctx, s)
		done <- struct{}{}
	}()
	go func() {
		RunFlusher(ctx, s, s.Step())
		done <- struct{}{}
	}()
	for i := 0; i < 8; i++ {
		s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
	}

	require.Eventually(t, func() bool {
		for _, col := range s.Snapshot(time.Time{}, time.Time{}) {
			if len(col.HotKeys) == 0 {
				continue
			}
			snapshot := col.HotKeys[0]
			return snapshot.WindowStart.Equal(col.WindowStart) &&
				snapshot.WindowEnd.Equal(col.At) &&
				len(snapshot.Entries) > 0
		}
		return false
	}, time.Second, 5*time.Millisecond)
	cancel()
	<-done
	<-done
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
