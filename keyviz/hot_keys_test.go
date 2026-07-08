package keyviz

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hotKeysTestCapacity is the per-route Space-Saving capacity used by
// every direct hotKeysTestSampler test. Inlined here (rather than a
// fn parameter) because every caller passes the same value — varying
// only along queue/key-length axes — and unparam catches the dead arg.
const hotKeysTestCapacity = 8

// hotKeysTestSampler constructs a MemSampler with hot-keys enabled and
// R=1 (every observe enqueues — deterministic for tests). The Step is
// set high so the aggregator's tick never fires during the test;
// publishAndReset is called explicitly when needed.
func hotKeysTestSampler(t *testing.T, queueSize, maxKeyLen int) *MemSampler {
	t.Helper()
	return NewMemSampler(MemSamplerOptions{
		Step:               time.Hour, // never ticks during the test
		HistoryColumns:     4,
		HotKeysEnabled:     true,
		HotKeysPerRoute:    hotKeysTestCapacity,
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
	s := hotKeysTestSampler(t, 32, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))

	for i := 0; i < 5; i++ {
		s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
	}
	s.Observe(1, []byte("cold"), OpWrite, 0, LabelLegacy)

	require.Nil(t, s.HotKeysSnapshot(1, LabelLegacy), "no snapshot before publish")
	drainAndPublish(s.hotKeys)

	snap := s.HotKeysSnapshot(1, LabelLegacy)
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

func TestHotKeysSnapshotsAreLabelScoped(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{
		Step:                time.Hour,
		HistoryColumns:      4,
		KeyVizLabelsEnabled: true,
		HotKeysEnabled:      true,
		HotKeysPerRoute:     hotKeysTestCapacity,
		HotKeysSampleRate:   1,
		HotKeysQueueSize:    32,
		HotKeysMaxKeyLen:    64,
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), nil, 1))
	s.Observe(1, []byte("redis-hot"), OpWrite, 0, LabelRedis)
	s.Observe(1, []byte("dynamo-hot"), OpWrite, 0, LabelDynamo)
	drainAndPublish(s.hotKeys)

	redisSnap := s.HotKeysSnapshot(1, LabelRedis)
	require.NotNil(t, redisSnap)
	require.Equal(t, LabelRedis, redisSnap.Label)
	require.Equal(t, []byte("redis-hot"), redisSnap.Entries[0].Key)

	dynamoSnap := s.HotKeysSnapshot(1, LabelDynamo)
	require.NotNil(t, dynamoSnap)
	require.Equal(t, LabelDynamo, dynamoSnap.Label)
	require.Equal(t, []byte("dynamo-hot"), dynamoSnap.Entries[0].Key)
	require.Nil(t, s.HotKeysSnapshot(1, LabelLegacy))
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
		s.Observe(1, tooLong, OpWrite, 0, LabelLegacy)
	}
	// Long-key check runs BEFORE the sample gate, so all 50 observes
	// bump the node-global counter — independent of R.
	require.Equal(t, uint64(50), s.hotKeys.skipped.Load(),
		"long-key check must run BEFORE the sample gate (Codex P2 round-7 L116)")

	// No events enqueued (every observe was length-skipped), so
	// publishAndReset has no sketch to attach the node-global signal
	// to. publishAndReset must NOT discard the signal in that case —
	// it should carry the skipped count forward to the next window.
	drainAndPublish(s.hotKeys)
	require.Nil(t, s.HotKeysSnapshot(1, LabelLegacy), "no enqueues -> no per-route snapshot")
	require.Equal(t, uint64(50), s.hotKeys.skipped.Load(),
		"node-global skipped must carry forward when no sketch exists to attach it to")

	// Second sampler at R=1 (every observe samples deterministically)
	// to pin the surface-and-Swap behavior without flaking on the
	// sample gate: seed the live `skipped` counter directly, observe
	// one short key so a sketch exists, and verify the next publish
	// captures the skipped count INTO that snapshot via Swap and
	// resets the live counter atomically.
	s2 := hotKeysTestSampler(t, 32, 4)
	require.True(t, s2.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	s2.hotKeys.skipped.Store(50)
	s2.Observe(1, []byte("ok"), OpWrite, 0, LabelLegacy)
	drainAndPublish(s2.hotKeys)
	snap := s2.HotKeysSnapshot(1, LabelLegacy)
	require.NotNil(t, snap)
	require.Equal(t, uint64(50), snap.SkippedLongKeys, "skipped surfaced on the first sketch-bearing snapshot")
	require.True(t, snapDegraded(snap), "non-zero skipped -> degraded")
	require.Equal(t, uint64(0), s2.hotKeys.skipped.Load(), "Swap reset the live counter when published")
}

// TestHotKeysChannelFullCountsDrops pins Codex round-3 L138: a sampled
// event that hits a full bounded channel does NOT silently disappear —
// the `dropped_samples` counter increments and degraded fires.
func TestHotKeysChannelFullCountsDrops(t *testing.T) {
	t.Parallel()
	// Queue size 4; no aggregator goroutine running, so nothing drains.
	// At R=1, the 5th observe must drop.
	s := hotKeysTestSampler(t, 4, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	for i := 0; i < 10; i++ {
		s.Observe(1, []byte("k"), OpWrite, 0, LabelLegacy)
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
		s.Observe(2, []byte("would-be-hot"), OpWrite, 0, LabelLegacy)
	}
	drainAndPublish(s.hotKeys)
	require.Nil(t, s.HotKeysSnapshot(2, LabelLegacy), "aggregate routes are not tracked")
}

// TestHotKeysSnapshotResetClearsLiveSketch pins design §4 / Codex P2
// round-6 L186: each publish RESETS the live sketch, so a key hot in
// one window cannot dominate the next window's drill-down.
func TestHotKeysSnapshotResetClearsLiveSketch(t *testing.T) {
	t.Parallel()
	s := hotKeysTestSampler(t, 32, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	// Window 1: "old" key hot.
	for i := 0; i < 10; i++ {
		s.Observe(1, []byte("old"), OpWrite, 0, LabelLegacy)
	}
	drainAndPublish(s.hotKeys)
	snap := s.HotKeysSnapshot(1, LabelLegacy)
	require.NotNil(t, snap)
	require.Equal(t, uint64(10), snap.SampledN)

	// Window 2: only one observe of a new key. The reset rule means
	// the new snapshot reflects ONLY window-2 traffic — "old" must
	// be absent, sampledN == 1.
	s.Observe(1, []byte("new"), OpWrite, 0, LabelLegacy)
	drainAndPublish(s.hotKeys)
	snap2 := s.HotKeysSnapshot(1, LabelLegacy)
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
	s.Observe(1, []byte("any"), OpWrite, 0, LabelLegacy) // must not panic; no-op for hot-keys
	require.Nil(t, s.HotKeysSnapshot(1, LabelLegacy))
}

// TestHotKeysDropCounterAttachesToSnapshot pins the claude-bot 🔴
// fix: dropped counts must reach the published snapshot via Swap (not
// Load + later Store, which would lose drops that arrive between).
func TestHotKeysDropCounterAttachesToSnapshot(t *testing.T) {
	t.Parallel()
	// Queue size 4; no aggregator goroutine — observes accumulate so
	// the 5th onwards drops, and the dropped counter rises to 6.
	s := hotKeysTestSampler(t, 4, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	for i := 0; i < 10; i++ {
		s.Observe(1, []byte("k"), OpWrite, 0, LabelLegacy)
	}
	require.Equal(t, uint64(6), s.hotKeys.dropped.Load(), "10 sends - 4 enqueued = 6 drops")
	drainAndPublish(s.hotKeys)
	snap := s.HotKeysSnapshot(1, LabelLegacy)
	require.NotNil(t, snap)
	require.Equal(t, uint64(6), snap.DroppedSamples, "Swap captures drops into the published snapshot")
	require.Equal(t, uint64(0), s.hotKeys.dropped.Load(), "Swap reset the live counter")
	require.True(t, snapDegraded(snap))
}

// TestHotKeysAggregatorReleasesRemovedRouteState pins the gemini-HIGH
// fix: when a route is removed mid-window, the aggregator must drop
// its sketch + per-route counter, otherwise removed routes leak m ×
// key bytes (and a *uint64) indefinitely under route churn.
func TestHotKeysAggregatorReleasesRemovedRouteState(t *testing.T) {
	t.Parallel()
	s := hotKeysTestSampler(t, 32, 64)
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	for i := 0; i < 4; i++ {
		s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
	}
	drainAndPublish(s.hotKeys)
	require.Contains(t, s.hotKeys.sketches, slotKey{RouteID: 1, Label: LabelLegacy})
	require.Contains(t, s.hotKeys.perSlotN, slotKey{RouteID: 1, Label: LabelLegacy})

	// Route removed → next publish must drop its sketch + counter.
	s.RemoveRoute(1)
	drainAndPublish(s.hotKeys)
	require.NotContains(t, s.hotKeys.sketches, slotKey{RouteID: 1, Label: LabelLegacy}, "removed route's sketch must be released")
	require.NotContains(t, s.hotKeys.perSlotN, slotKey{RouteID: 1, Label: LabelLegacy}, "removed route's counter must be released")
}

// TestHotKeysAggregatorGracefulShutdownPublishes pins the ctx.Done arm
// of run(): a final drain + publish must fire so a last-second observe
// lands in a queryable snapshot rather than being silently discarded at
// shutdown. We use cancellation (not the tick) to drive the publish so
// the assertion is deterministic regardless of goroutine scheduling —
// the periodic-tick path is exercised by TestHotKeysAggregatorRaceFree.
func TestHotKeysAggregatorGracefulShutdownPublishes(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{
		// Step is set very long so the periodic tick cannot fire
		// before we cancel — the only publish must come from the
		// ctx.Done arm.
		Step:              time.Hour,
		HistoryColumns:    4,
		HotKeysEnabled:    true,
		HotKeysPerRoute:   8,
		HotKeysSampleRate: 1,
		HotKeysQueueSize:  32,
		HotKeysMaxKeyLen:  64,
	})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHotKeysAggregator(ctx, s)
	}()

	for i := 0; i < 7; i++ {
		s.Observe(1, []byte("hot"), OpWrite, 0, LabelLegacy)
	}
	// Cancel triggers the ctx.Done arm, which drains the channel
	// and publishes one final snapshot before returning.
	cancel()
	<-done

	snap := s.HotKeysSnapshot(1, LabelLegacy)
	require.NotNil(t, snap, "graceful shutdown must publish a final snapshot")
	require.GreaterOrEqual(t, snap.SampledN, uint64(7), "all observed events present in the final snapshot")
	require.Len(t, snap.Entries, 1)
	require.Equal(t, []byte("hot"), snap.Entries[0].Key)
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
				s.Observe(rid, key, OpWrite, 0, LabelLegacy)
			}
		}(w)
	}
	wg.Wait()
	// Wait for the aggregator to drain the queue and publish at least
	// one tick-driven snapshot for each route. The previous
	// time.Sleep(50ms) raced the aggregator's tick (Step=5ms) on slow
	// -race CI runners — the Run goroutine could be slow to schedule
	// at all, leaving hotKeysSnap as its initial nil atomic.Pointer
	// load by the time the assertions fired. require.Eventually with
	// a 3-second budget + 5ms poll cadence rides out any scheduler
	// jitter; snapshot reads themselves remain lock-free
	// (atomic.Pointer.Load), which is what this test pins. Observed
	// failure: Actions run 26765510693.
	require.Eventually(t, func() bool {
		return s.HotKeysSnapshot(1, LabelLegacy) != nil && s.HotKeysSnapshot(2, LabelLegacy) != nil
	}, 3*time.Second, 5*time.Millisecond, "aggregator must publish a snapshot for each route within the budget")
	cancel()
	wgRun.Wait()
}
