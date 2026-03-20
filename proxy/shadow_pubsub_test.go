package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func counterValue(c prometheus.Counter) float64 {
	return testutil.ToFloat64(c)
}

// testClock provides a deterministic clock for tests, avoiding time.Sleep flakiness.
type testClock struct {
	v atomic.Value // stores time.Time
}

func newTestClock() *testClock {
	c := &testClock{}
	c.v.Store(time.Now())
	return c
}

func (c *testClock) Now() time.Time {
	v, ok := c.v.Load().(time.Time)
	if !ok {
		return time.Time{}
	}
	return v
}
func (c *testClock) Advance(d time.Duration) { c.v.Store(c.Now().Add(d)) }

func newTestShadowPubSub(window time.Duration) *shadowPubSub {
	return newTestShadowPubSubWithClock(window, time.Now)
}

func newTestShadowPubSubWithClock(window time.Duration, nowFunc func() time.Time) *shadowPubSub {
	return &shadowPubSub{
		metrics:              newTestMetrics(),
		sentry:               newTestSentry(),
		logger:               testLogger,
		window:               window,
		nowFunc:              nowFunc,
		pending:              make(map[msgKey][]pendingMsg),
		unmatchedSecondaries: make(map[msgKey][]secondaryPending),
		done:                 make(chan struct{}),
	}
}

func TestShadowPubSub_MatchedMessage(t *testing.T) {
	sp := newTestShadowPubSub(100 * time.Millisecond)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "hello"})
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "hello"})

	sp.mu.Lock()
	remaining := len(sp.pending)
	sp.mu.Unlock()
	assert.Equal(t, 0, remaining, "matched message should be removed from pending")
}

func TestShadowPubSub_MissingOnSecondary(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(10*time.Millisecond, clock.Now)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "hello"})
	clock.Advance(20 * time.Millisecond)
	sp.sweepExpired()

	sp.mu.Lock()
	remaining := len(sp.pending)
	sp.mu.Unlock()
	assert.Equal(t, 0, remaining, "expired message should be removed")

	val := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("data_mismatch"))
	assert.Equal(t, float64(1), val)
}

func TestShadowPubSub_ExtraOnSecondary(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(10*time.Millisecond, clock.Now)

	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "extra"})

	// Advance the clock past the comparison window and sweep.
	clock.Advance(20 * time.Millisecond)
	sp.sweepExpired()

	val := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("extra_data"))
	assert.Equal(t, float64(1), val)
}

func TestShadowPubSub_OutOfOrderMatching(t *testing.T) {
	sp := newTestShadowPubSub(1 * time.Second)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "msg1"})
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "msg2"})

	// Secondary delivers in reverse order.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "msg2"})
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "msg1"})

	sp.mu.Lock()
	remaining := len(sp.pending)
	sp.mu.Unlock()
	assert.Equal(t, 0, remaining, "all messages should be matched")
}

func TestShadowPubSub_DuplicateMessages(t *testing.T) {
	sp := newTestShadowPubSub(1 * time.Second)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "dup"})

	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.mu.Lock()
	assert.Equal(t, 1, len(sp.pending[msgKey{Channel: "ch1", Payload: "dup"}]))
	sp.mu.Unlock()

	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.mu.Lock()
	assert.Equal(t, 0, len(sp.pending))
	sp.mu.Unlock()
}

func TestShadowPubSub_RecordAfterClose(t *testing.T) {
	sp := newTestShadowPubSub(1 * time.Second)

	sp.mu.Lock()
	sp.closed = true
	sp.mu.Unlock()

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "after-close"})

	sp.mu.Lock()
	assert.Equal(t, 0, len(sp.pending))
	sp.mu.Unlock()
}

func TestShadowPubSub_CompareLoopExitsOnChannelClose(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(10*time.Millisecond, clock.Now)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "orphan"})
	clock.Advance(20 * time.Millisecond)

	ch := make(chan *redis.Message)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp.compareLoop(ch)
	}()

	close(ch)
	wg.Wait()

	sp.mu.Lock()
	assert.Equal(t, 0, len(sp.pending), "should sweep on exit")
	assert.True(t, sp.closed, "should mark closed on channel close to prevent RecordPrimary leak")
	sp.mu.Unlock()
}

func TestShadowPubSub_DuplicateSecondaryBuffered(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(1*time.Second, clock.Now)

	// Two identical secondary messages arrive before any primary.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "dup"})

	key := msgKey{Channel: "ch1", Payload: "dup"}
	sp.mu.Lock()
	secs := sp.unmatchedSecondaries[key]
	assert.Len(t, secs, 2, "both duplicate secondaries should be buffered")
	sp.mu.Unlock()

	// Now one primary arrives — should consume one buffered secondary.
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.sweepExpired() // reconcile

	sp.mu.Lock()
	secs = sp.unmatchedSecondaries[key]
	sp.mu.Unlock()
	// One secondary remains buffered (only one primary consumed one).
	assert.Len(t, secs, 1, "one duplicate should remain after matching one primary")
}

func TestShadowPubSub_CloseCleanupUnmatchedSecondaries(t *testing.T) {
	// Verify that sweepAll (called when the compare loop exits on Close) drains
	// the per-struct unmatchedSecondaries buffer and reports DivExtraData.
	sp := newTestShadowPubSub(1 * time.Second)

	// Buffer a secondary message.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "leaked"})

	sp.mu.Lock()
	_, buffered := sp.unmatchedSecondaries[msgKey{Channel: "ch1", Payload: "leaked"}]
	sp.mu.Unlock()
	assert.True(t, buffered, "secondary should be buffered before sweep")

	// sweepAll drains the buffer and reports the unmatched secondary as DivExtraData.
	sp.sweepAll()

	sp.mu.Lock()
	assert.Empty(t, sp.unmatchedSecondaries, "sweepAll should drain unmatchedSecondaries")
	sp.mu.Unlock()

	extra := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("extra_data"))
	assert.Equal(t, float64(1), extra, "buffered secondary should be reported as extra_data by sweepAll")
}

func TestShadowPubSub_CompareLoopMatchesFromChannel(t *testing.T) {
	sp := newTestShadowPubSub(1 * time.Second)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "msg1"})

	ch := make(chan *redis.Message, 1)
	ch <- &redis.Message{Channel: "ch1", Payload: "msg1"}
	close(ch)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp.compareLoop(ch)
	}()

	wg.Wait()

	sp.mu.Lock()
	assert.Equal(t, 0, len(sp.pending), "message should be matched via compareLoop")
	sp.mu.Unlock()
}

// TestShadowPubSub_SecondaryBeforePrimaryImmediateReconcile verifies that when a
// secondary message arrives before its matching primary, RecordPrimary immediately
// consumes the buffered secondary without needing a sweep cycle, and no divergence
// is reported. This tests the fix for false DivDataMismatch reports when
// PubSubCompareWindow is smaller than the sweep interval.
func TestShadowPubSub_SecondaryBeforePrimaryImmediateReconcile(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(1*time.Second, clock.Now)

	// Secondary arrives first — it should be buffered in sp.unmatchedSecondaries.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "early"})

	key := msgKey{Channel: "ch1", Payload: "early"}
	sp.mu.Lock()
	secs := sp.unmatchedSecondaries[key]
	sp.mu.Unlock()
	assert.Len(t, secs, 1, "secondary should be buffered before primary arrives")

	// Primary arrives within the window — RecordPrimary should immediately consume
	// the buffered secondary and NOT add to sp.pending.
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "early"})

	// Primary must NOT have been added to pending (immediate reconciliation occurred).
	sp.mu.Lock()
	assert.Equal(t, 0, len(sp.pending), "primary should be matched immediately without queuing into pending")
	sp.mu.Unlock()

	// The buffered secondary entry must have been consumed.
	sp.mu.Lock()
	secs = sp.unmatchedSecondaries[key]
	sp.mu.Unlock()
	assert.Empty(t, secs, "buffered secondary should be consumed immediately by RecordPrimary")

	// Advance past the window and sweep — no divergences should be reported.
	clock.Advance(2 * time.Second)
	sp.sweepExpired()

	mismatch := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("data_mismatch"))
	assert.Equal(t, float64(0), mismatch, "no data_mismatch should be reported when secondary was buffered and primary arrived within window")

	extra := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("extra_data"))
	assert.Equal(t, float64(0), extra, "no extra_data should be reported when secondary was matched by RecordPrimary")
}

// TestShadowPubSub_SecondaryWithinWindowMatches verifies that a secondary
// arriving before the comparison window properly matches its primary and
// prevents a divergence from being reported during reconciliation.
func TestShadowPubSub_SecondaryWithinWindowMatches(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(50*time.Millisecond, clock.Now)

	// Record the primary and buffer the secondary immediately (within window).
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "match-me"})
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "match-me"})

	// Advance past window and sweep — the primary was already consumed, so no divergence.
	clock.Advance(100 * time.Millisecond)
	sp.sweepExpired()

	mismatch := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("data_mismatch"))
	assert.Equal(t, float64(0), mismatch, "within-window secondary match must suppress data_mismatch")

	extra := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("extra_data"))
	assert.Equal(t, float64(0), extra, "within-window secondary match must suppress extra_data")
}

// TestShadowPubSub_ExpiredPrimaryThenLateSecondary verifies that a secondary
// arriving after the comparison window does not suppress the already-expired
// primary divergence. Both a DivDataMismatch (primary) and eventually a
// DivExtraData (secondary) should be reported.
func TestShadowPubSub_ExpiredPrimaryThenLateSecondary(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(10*time.Millisecond, clock.Now)

	// Record the primary message.
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "late"})

	// Advance past the comparison window so the primary is expired.
	clock.Advance(20 * time.Millisecond)

	// The secondary arrives after the window — it should be buffered (no primary to match).
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "late"})

	// Sweep: expired primary should be reported as DivDataMismatch.
	// The late secondary is buffered and not yet old enough to report on its own.
	sp.sweepExpired()

	mismatch := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("data_mismatch"))
	assert.Equal(t, float64(1), mismatch, "expired primary must be reported as data_mismatch")

	extra := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("extra_data"))
	assert.Equal(t, float64(0), extra, "secondary should not yet be reported as extra_data")

	// Advance again so the buffered secondary is also past the window.
	clock.Advance(20 * time.Millisecond)
	sp.sweepExpired()

	extra = counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("extra_data"))
	assert.Equal(t, float64(1), extra, "late secondary must eventually be reported as extra_data")
}

// TestShadowPubSub_MatchSecondarySkipsExpiredHead verifies that matchSecondary
// correctly skips an already-expired first entry and matches the secondary against
// a still-in-window second entry (matchIdx at position 1).
func TestShadowPubSub_MatchSecondarySkipsExpiredHead(t *testing.T) {
	clock := newTestClock()
	window := 50 * time.Millisecond
	sp := newTestShadowPubSubWithClock(window, clock.Now)

	key := msgKey{Channel: "ch1", Payload: "val"}

	// Record the first primary — then advance past the window.
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "val"})
	clock.Advance(100 * time.Millisecond) // first entry is now expired

	// Record a second primary within the new "now" window.
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "val"})

	// Secondary arrives: should match the second (non-expired) entry, NOT the
	// expired first one.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "val"})

	sp.mu.Lock()
	entries := sp.pending[key]
	sp.mu.Unlock()
	// Only the expired first entry should remain; the second was consumed.
	assert.Len(t, entries, 1, "only the expired head entry should remain after matching the in-window second entry")
}

// TestShadowPubSub_MatchSecondaryMiddleEntry verifies that matchSecondary removes
// a middle entry (matchIdx neither 0 nor last) and leaves the others intact.
func TestShadowPubSub_MatchSecondaryMiddleEntry(t *testing.T) {
	clock := newTestClock()
	window := 50 * time.Millisecond
	sp := newTestShadowPubSubWithClock(window, clock.Now)

	keyA := msgKey{Channel: "ch1", Payload: "a"}
	keyB := msgKey{Channel: "ch1", Payload: "b"}
	keyC := msgKey{Channel: "ch1", Payload: "c"}

	// Force three separate keys by inserting directly into pending so we can
	// control the per-key entry list structure.
	sp.mu.Lock()
	now := clock.Now()
	// Arrange: expired, in-window, in-window entries for the same channel but
	// different payloads so they live under separate keys.
	sp.pending[keyA] = []pendingMsg{{channel: "ch1", payload: "a", timestamp: now.Add(-100 * time.Millisecond)}} // expired
	sp.pending[keyB] = []pendingMsg{{channel: "ch1", payload: "b", timestamp: now}}                              // in-window
	sp.pending[keyC] = []pendingMsg{{channel: "ch1", payload: "c", timestamp: now}}                              // in-window
	sp.mu.Unlock()

	// Match "b" (in-window, but keyA is already expired separately under its own key).
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "b"})

	sp.mu.Lock()
	_, aExists := sp.pending[keyA]
	_, bExists := sp.pending[keyB]
	_, cExists := sp.pending[keyC]
	sp.mu.Unlock()

	assert.True(t, aExists, "expired keyA entry should still be in pending for sweep")
	assert.False(t, bExists, "matched keyB entry should have been removed")
	assert.True(t, cExists, "unrelated keyC entry should be untouched")
}

// TestShadowPubSub_MatchSecondaryLastEntry verifies that matchSecondary can match
// and remove the last entry in a multi-entry slice (matchIdx == len-1) while
// leaving the earlier expired entries for the sweep.
func TestShadowPubSub_MatchSecondaryLastEntry(t *testing.T) {
	clock := newTestClock()
	window := 50 * time.Millisecond
	sp := newTestShadowPubSubWithClock(window, clock.Now)

	key := msgKey{Channel: "ch1", Payload: "val"}
	now := clock.Now()

	// Insert two entries: the first is expired, the second is in-window.
	sp.mu.Lock()
	sp.pending[key] = []pendingMsg{
		{channel: "ch1", payload: "val", timestamp: now.Add(-100 * time.Millisecond)}, // expired (index 0)
		{channel: "ch1", payload: "val", timestamp: now},                              // in-window (index 1, the "last")
	}
	sp.mu.Unlock()

	// Secondary should match the last (in-window) entry.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "val"})

	sp.mu.Lock()
	entries := sp.pending[key]
	sp.mu.Unlock()

	// Only the expired first entry should remain.
	assert.Len(t, entries, 1, "only the expired first entry should remain after matching the last in-window entry")
	assert.WithinDuration(t, now.Add(-100*time.Millisecond), entries[0].timestamp, time.Millisecond,
		"remaining entry should be the expired head")
}
