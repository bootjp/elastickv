package keyviz

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hotKeysTestSampler constructs a MemSampler with hot-keys enabled and
// R=1 (every observe enqueues — deterministic for tests). The Step is
// set high so the aggregator's tick never fires during the test;
// publishAndReset is called explicitly when needed.
func hotKeysTestSampler(t *testing.T, capacity, queueSize, maxKeyLen int) *MemSampler {
	t.Helper()
	return NewMemSampler(MemSamplerOptions{
		Step:               time.Hour, // never ticks during the test
		HistoryColumns:     4,
		HotKeysEnabled:     true,
		HotKeysPerRoute:    capacity,
		HotKeysSampleRate:  1, // every Observe is sampled
		HotKeysQueueSize:   queueSize,
		HotKeysMaxKeyLen:   maxKeyLen,
		KeyBucketsPerRoute: 1,
	})
}

// drainAndPublish synchronously drains the channel into the per-route
// SS sketches and triggers one publish+reset cycle. Test-only shortcut
// that exercises the same code path as the aggregator's tick arm.
func drainAndPublish(a *hotKeysAggregator) {
	for len(a.ch) > 0 {
		a.consume(<-a.ch)
	}
	a.publishAndReset()
}

func TestHotKeysObservePublishesPerRouteSnapshot(t *testing.T) {
	t.Parallel()
	s := hotKeysTestSampler(t, 8, 32, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))

	for i := 0; i < 5; i++ {
		s.Observe(1, []byte("hot"), OpWrite, 0)
	}
	s.Observe(1, []byte("cold"), OpWrite, 0)

	require.Nil(t, s.HotKeysSnapshot(1), "no snapshot before publish")
	drainAndPublish(s.hotKeys)

	snap := s.HotKeysSnapshot(1)
	require.NotNil(t, snap)
	require.Equal(t, uint64(1), snap.RouteID)
	require.Equal(t, uint64(6), snap.SampledN, "every Observe enqueued at R=1")
	require.Equal(t, 1, snap.SampleRate)
	require.Equal(t, 8, snap.Capacity)
	require.Zero(t, snap.DroppedSamples)
	require.Zero(t, snap.SkippedLongKeys)
	require.False(t, snapDegraded(snap))

	got := map[string]uint64{}
	for _, e := range snap.Entries {
		got[string(e.Key)] = e.Count
	}
	require.Equal(t, uint64(5), got["hot"])
	require.Equal(t, uint64(1), got["cold"])
}

// snapDegraded mirrors the admin response's `degraded` rule (§5):
// drops > 0 OR skipped_long_keys > 0.
func snapDegraded(s *KeyvizHotKeysSnapshot) bool {
	return s.DroppedSamples > 0 || s.SkippedLongKeys > 0
}

// TestHotKeysLongKeySkippedBeforeSampling pins Codex round-7 L116: a
// key longer than HotKeysMaxKeyLen increments `skipped_long_keys` on
// every Observe — UNCONDITIONALLY, before the sample gate. So a long
// hot key cannot escape the degraded signal just because the sampler
// happened to roll against it on every call.
func TestHotKeysLongKeySkippedBeforeSampling(t *testing.T) {
	t.Parallel()
	// Use R = 1000 so the sample gate practically never fires. The
	// length check fires before the sample gate, so every long-key
	// Observe must still bump the counter.
	s := NewMemSampler(MemSamplerOptions{
		Step:              time.Hour,
		HistoryColumns:    4,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1000,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  4, // very short cap
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	tooLong := bytes.Repeat([]byte("x"), 100)
	for i := 0; i < 50; i++ {
		s.Observe(1, tooLong, OpWrite, 0)
	}
	drainAndPublish(s.hotKeys)
	snap := s.HotKeysSnapshot(1)
	// Long-key observes never enqueue, so the route's sketch is empty
	// (no entries, sampledN == 0) BUT the node-global skipped counter
	// fired on every call, marking the snapshot degraded.
	if snap != nil {
		require.Empty(t, snap.Entries)
		require.Zero(t, snap.SampledN)
		require.Equal(t, uint64(50), snap.SkippedLongKeys,
			"long-key check must run BEFORE the sample gate")
		require.True(t, snapDegraded(snap), "skip alone -> degraded")
	}
	// The aggregator may not have created a snapshot for this route
	// at all (no enqueues for it). Either way, the node-global counter
	// must reflect the 50 skips on the aggregator's view BEFORE
	// publish reset it — verify by re-observing one long key after
	// publish and checking the counter increments again.
	for i := 0; i < 3; i++ {
		s.Observe(1, tooLong, OpWrite, 0)
	}
	require.Equal(t, uint64(3), s.hotKeys.skipped.Load(), "skipped counter resets at publish and re-accumulates")
}

// TestHotKeysChannelFullCountsDrops pins Codex round-3 L138: a sampled
// event that hits a full bounded channel does NOT silently disappear —
// the `dropped_samples` counter increments and degraded fires.
func TestHotKeysChannelFullCountsDrops(t *testing.T) {
	t.Parallel()
	// Queue size 4; no aggregator goroutine running, so nothing drains.
	// At R=1, the 5th observe must drop.
	s := hotKeysTestSampler(t, 8, 4, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	for i := 0; i < 10; i++ {
		s.Observe(1, []byte("k"), OpWrite, 0)
	}
	require.Equal(t, 4, len(s.hotKeys.ch), "queue should be full (cap=4)")
	require.Equal(t, uint64(6), s.hotKeys.dropped.Load(), "10 observes - 4 enqueued = 6 drops")
}

// TestHotKeysAggregateRoutesNotTracked pins design §2.2: aggregates
// (RouteID's synthetic / Aggregate=true slots) are not eligible for
// hot-keys tracking. HotKeysSnapshot must return nil for them.
func TestHotKeysAggregateRoutesNotTracked(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{
		Step:              time.Hour,
		HistoryColumns:    4,
		MaxTrackedRoutes:  1, // force coarsening for the 2nd RegisterRoute
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  64,
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("m"), 0), "1st route gets its own slot")
	require.False(t, s.RegisterRoute(2, []byte("n"), []byte("z"), 0), "2nd route coarsens into a virtual bucket")
	// Observe against route 2: hits the aggregate bucket, which is
	// filtered out by the Observe-time !slot.Aggregate check.
	for i := 0; i < 5; i++ {
		s.Observe(2, []byte("would-be-hot"), OpWrite, 0)
	}
	drainAndPublish(s.hotKeys)
	require.Nil(t, s.HotKeysSnapshot(2), "aggregate routes are not tracked")
}

// TestHotKeysSnapshotResetClearsLiveSketch pins design §4 / Codex P2
// round-6 L186: each publish RESETS the live sketch, so a key hot in
// one window cannot dominate the next window's drill-down.
func TestHotKeysSnapshotResetClearsLiveSketch(t *testing.T) {
	t.Parallel()
	s := hotKeysTestSampler(t, 8, 32, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	// Window 1: "old" key hot.
	for i := 0; i < 10; i++ {
		s.Observe(1, []byte("old"), OpWrite, 0)
	}
	drainAndPublish(s.hotKeys)
	snap := s.HotKeysSnapshot(1)
	require.NotNil(t, snap)
	require.Equal(t, uint64(10), snap.SampledN)

	// Window 2: only one observe of a new key. The reset rule means
	// the new snapshot reflects ONLY window-2 traffic — "old" must
	// be absent, sampledN == 1.
	s.Observe(1, []byte("new"), OpWrite, 0)
	drainAndPublish(s.hotKeys)
	snap2 := s.HotKeysSnapshot(1)
	require.NotNil(t, snap2)
	require.Equal(t, uint64(1), snap2.SampledN, "reset must clear sampledN")
	require.Len(t, snap2.Entries, 1)
	require.Equal(t, []byte("new"), snap2.Entries[0].Key)
}

// TestHotKeysDisabledIsZeroCost pins the disabled-case contract: with
// HotKeysEnabled=false the sampler does not even allocate an
// aggregator, so the hot path's nil check short-circuits and no key
// bytes are retained anywhere.
func TestHotKeysDisabledIsZeroCost(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Hour, HistoryColumns: 4, HotKeysEnabled: false})
	require.Nil(t, s.hotKeys, "disabled => no aggregator constructed")
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	s.Observe(1, []byte("any"), OpWrite, 0) // must not panic; no-op for hot-keys
	require.Nil(t, s.HotKeysSnapshot(1))
}

// TestHotKeysAggregatorRunLoopPublishesOnTick exercises the actual
// run() goroutine (with a real ticker via the sampler's Step) and the
// graceful-shutdown final publish path.
func TestHotKeysAggregatorRunLoopPublishesOnTick(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{
		Step:              10 * time.Millisecond,
		HistoryColumns:    4,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  64,
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHotKeysAggregator(ctx, s)
	}()

	for i := 0; i < 7; i++ {
		s.Observe(1, []byte("hot"), OpWrite, 0)
	}
	// Wait long enough for at least one tick to fire.
	// 5 s is generous: under -race + parallel tests the goroutine
	// scheduling can keep the 10ms ticker from firing for a while.
	require.Eventually(t, func() bool {
		snap := s.HotKeysSnapshot(1)
		return snap != nil && snap.SampledN >= 7
	}, 5*time.Second, 10*time.Millisecond, "aggregator must publish after the tick fires")

	// Graceful shutdown: the run loop should drain & publish even
	// after ctx cancels, so a last-second observe is not lost.
	for i := 0; i < 3; i++ {
		s.Observe(1, []byte("late"), OpWrite, 0)
	}
	cancel()
	<-done
}

// TestHotKeysAggregatorRaceFree exercises the hot path under
// concurrent observers + a live aggregator goroutine; -race must pass.
func TestHotKeysAggregatorRaceFree(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{
		Step:              5 * time.Millisecond,
		HistoryColumns:    4,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   16,
		HotKeysSampleRate: 4,
		HotKeysQueueSize:  256,
		HotKeysMaxKeyLen:  64,
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	require.True(t, s.RegisterRoute(2, []byte("m"), []byte("z"), 0))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var wgRun sync.WaitGroup
	wgRun.Add(1)
	go func() {
		defer wgRun.Done()
		RunHotKeysAggregator(ctx, s)
	}()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			key := []byte{byte('k' + seed%4)}
			for i := 0; i < 500; i++ {
				var rid uint64 = 1
				if i%2 == 1 {
					rid = 2
				}
				s.Observe(rid, key, OpWrite, 0)
			}
		}(w)
	}
	wg.Wait()
	// Let the aggregator drain + publish a few ticks.
	time.Sleep(50 * time.Millisecond)
	// Snapshot reads must be lock-free and consistent.
	s1 := s.HotKeysSnapshot(1)
	s2 := s.HotKeysSnapshot(2)
	require.NotNil(t, s1)
	require.NotNil(t, s2)
	cancel()
	wgRun.Wait()
}
