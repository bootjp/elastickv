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
		metrics: newTestMetrics(),
		sentry:  newTestSentry(),
		logger:  testLogger,
		window:  window,
		nowFunc: nowFunc,
		pending: make(map[msgKey][]pendingMsg),
		done:    make(chan struct{}),
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
	defer func() {
		unmatchedSecondaries.Lock()
		delete(unmatchedSecondaries.data, sp)
		unmatchedSecondaries.Unlock()
	}()

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
	sp := newTestShadowPubSub(10 * time.Millisecond)
	defer func() {
		// Clean up without calling Close (test has no real secondary connection).
		unmatchedSecondaries.Lock()
		delete(unmatchedSecondaries.data, sp)
		unmatchedSecondaries.Unlock()
	}()

	// Two identical secondary messages arrive before any primary.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "dup"})

	unmatchedSecondaries.Lock()
	key := msgKey{Channel: "ch1", Payload: "dup"}
	secs := unmatchedSecondaries.data[sp][key]
	assert.Len(t, secs, 2, "both duplicate secondaries should be buffered")
	unmatchedSecondaries.Unlock()

	// Now one primary arrives — should consume one buffered secondary.
	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "dup"})
	sp.sweepExpired() // reconcile

	unmatchedSecondaries.Lock()
	secs = unmatchedSecondaries.data[sp][key]
	unmatchedSecondaries.Unlock()
	// One secondary remains buffered (only one primary consumed one).
	assert.Len(t, secs, 1, "one duplicate should remain after matching one primary")
}

func TestShadowPubSub_CloseCleanupUnmatchedSecondaries(t *testing.T) {
	// Close() now tolerates nil secondary, so no mock client is needed.
	sp := newTestShadowPubSub(1 * time.Second)

	// Buffer a secondary message.
	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "leaked"})

	unmatchedSecondaries.Lock()
	_, exists := unmatchedSecondaries.data[sp]
	unmatchedSecondaries.Unlock()
	assert.True(t, exists, "secondary should be buffered before Close")

	sp.Close()

	unmatchedSecondaries.Lock()
	_, exists = unmatchedSecondaries.data[sp]
	unmatchedSecondaries.Unlock()
	assert.False(t, exists, "Close should clean up unmatchedSecondaries entry")
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

// arriving after the comparison window does not suppress the already-expired
// primary divergence. Both a DivDataMismatch (primary) and eventually a
// DivExtraData (secondary) should be reported.
func TestShadowPubSub_ExpiredPrimaryThenLateSecondary(t *testing.T) {
	clock := newTestClock()
	sp := newTestShadowPubSubWithClock(10*time.Millisecond, clock.Now)
	defer func() {
		unmatchedSecondaries.Lock()
		delete(unmatchedSecondaries.data, sp)
		unmatchedSecondaries.Unlock()
	}()

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
