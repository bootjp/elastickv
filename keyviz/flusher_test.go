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

	s.Observe(1, make([]byte, 0), OpRead, 0, LabelLegacy)
	// No deadline: parallel CPU-bound tests (and other packages under
	// `go test ./...`) can starve the ticker goroutine for over a second
	// on a -race runner. A flusher that never ticks or never returns
	// still fails, via the go test -timeout.
	waitForColumns(s, 1)
	cancel()
	<-done
}

func TestRunFlusherAttachesAlignedHotKeysWindow(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0)
	step := 5 * time.Millisecond
	s := NewMemSampler(MemSamplerOptions{
		Step:              step,
		HistoryColumns:    16,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  64,
		Now:               func() time.Time { return start },
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 1))

	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	done := make(chan struct{}, 2)
	go func() {
		RunHotKeysAggregator(ctx, s)
		done <- struct{}{}
	}()
	go func() {
		runFlusherTicks(ctx, s, ticks)
		done <- struct{}{}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		<-done
	})

	// Drive explicit boundaries instead of a wall-clock ticker so the
	// windows under test cannot be skipped or evicted by scheduler stalls.
	for window, writes := range []int{8, 3} {
		for range writes {
			s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
		}
		ticks <- start.Add(time.Duration(window+1) * step)
		waitForColumns(s, window+1)
	}

	columns := s.Snapshot(time.Time{}, time.Time{})
	require.Len(t, columns, 2)
	require.True(t, alignedHotKeysWindowObserved(columns))
	for i, writes := range []uint64{8, 3} {
		col := columns[i]
		require.Equal(t, start.Add(time.Duration(i)*step), col.WindowStart)
		require.Equal(t, start.Add(time.Duration(i+1)*step), col.At)
		require.Len(t, col.HotKeys, 1)
		require.Equal(t, writes, col.HotKeys[0].SampledN)
		require.Len(t, col.HotKeys[0].Entries, 1)
		require.Equal(t, []byte("hot"), col.HotKeys[0].Entries[0].Key)
	}
}

// waitForColumns blocks until s has retained at least n columns.
func waitForColumns(s *MemSampler, n int) {
	for len(s.Snapshot(time.Time{}, time.Time{})) < n {
		time.Sleep(time.Millisecond)
	}
}

func alignedHotKeysWindowObserved(columns []MatrixColumn) bool {
	observed := false
	for _, col := range columns {
		for _, snapshot := range col.HotKeys {
			if len(snapshot.Entries) == 0 {
				continue
			}
			if !snapshot.WindowStart.Equal(col.WindowStart) || !snapshot.WindowEnd.Equal(col.At) {
				return false
			}
			observed = true
		}
	}
	return observed
}

func TestAlignedHotKeysWindowObservedSkipsEmptyWindows(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	end := start.Add(5 * time.Millisecond)
	columns := []MatrixColumn{
		{WindowStart: start, At: end, HotKeys: []KeyvizHotKeysSnapshot{{WindowStart: start, WindowEnd: end}}},
		{WindowStart: end, At: end.Add(5 * time.Millisecond), HotKeys: []KeyvizHotKeysSnapshot{{
			WindowStart: end, WindowEnd: end.Add(5 * time.Millisecond), Entries: []KeyvizHotKeyEntry{{Key: []byte("hot")}},
		}}},
	}
	require.True(t, alignedHotKeysWindowObserved(columns))
	columns[1].HotKeys[0].WindowStart = start
	require.False(t, alignedHotKeysWindowObserved(columns), "a populated misaligned window must still fail")
	require.False(t, alignedHotKeysWindowObserved(columns[:1]), "empty windows cannot satisfy the assertion")
}

func TestFlusherAlignmentProbeAfterHistoryEviction(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	step := 5 * time.Millisecond
	s := NewMemSampler(MemSamplerOptions{
		Step: step, HistoryColumns: 2, HotKeysEnabled: true,
		HotKeysPerRoute: 8, HotKeysSampleRate: 1, HotKeysQueueSize: 32,
		HotKeysMaxKeyLen: 64, Now: func() time.Time { return start },
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 1))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHotKeysAggregator(ctx, s)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
	require.True(t, s.flushWindow(ctx, start.Add(step)))
	require.True(t, alignedHotKeysWindowObserved(s.Snapshot(time.Time{}, time.Time{})))
	for i := 2; i <= 3; i++ {
		require.True(t, s.flushWindow(ctx, start.Add(time.Duration(i)*step)))
	}
	require.False(t, alignedHotKeysWindowObserved(s.Snapshot(time.Time{}, time.Time{})), "the initial sample has been evicted")
	s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
	require.True(t, s.flushWindow(ctx, start.Add(4*step)))
	require.True(t, alignedHotKeysWindowObserved(s.Snapshot(time.Time{}, time.Time{})), "an earlier empty window must not hide the new sample")
}

func TestFlushWindowKeepsHotKeyAndCounterInSameColumn(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	s := NewMemSampler(MemSamplerOptions{
		Step:              time.Minute,
		HistoryColumns:    4,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  64,
		Now:               func() time.Time { return now },
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 1))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHotKeysAggregator(ctx, s)
	}()

	s.flushMu.Lock()
	observed := make(chan struct{})
	go func() {
		s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
		close(observed)
	}()
	select {
	case <-observed:
		t.Fatal("Observe crossed a hot-key flush boundary")
	case <-time.After(20 * time.Millisecond):
	}
	s.flushMu.Unlock()
	// Unbounded on purpose: a wall-clock budget here fails whenever a
	// loaded -race runner deschedules the Observe goroutine. A real
	// deadlock still fails via the go test -timeout.
	<-observed
	require.True(t, s.flushWindow(ctx, now.Add(time.Minute)))

	columns := s.Snapshot(time.Time{}, time.Time{})
	require.Len(t, columns, 1)
	require.Len(t, columns[0].Rows, 1)
	require.Equal(t, uint64(1), columns[0].Rows[0].Writes)
	require.Len(t, columns[0].HotKeys, 1)
	require.Equal(t, uint64(1), columns[0].HotKeys[0].SampledN)
	require.Len(t, columns[0].HotKeys[0].Entries, 1)
	require.Equal(t, []byte("hot"), columns[0].HotKeys[0].Entries[0].Key)

	cancel()
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
