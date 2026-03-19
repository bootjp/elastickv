package proxy

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func counterValue(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	pm, ok := c.(prometheus.Metric)
	if !ok {
		return 0
	}
	_ = pm.Write(m)
	return m.GetCounter().GetValue()
}

func newTestShadowPubSub(window time.Duration) *shadowPubSub {
	return &shadowPubSub{
		metrics: newTestMetrics(),
		sentry:  newTestSentry(),
		logger:  slog.Default(),
		window:  window,
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
	sp := newTestShadowPubSub(10 * time.Millisecond)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "hello"})
	time.Sleep(20 * time.Millisecond)
	sp.sweepExpired()

	sp.mu.Lock()
	remaining := len(sp.pending)
	sp.mu.Unlock()
	assert.Equal(t, 0, remaining, "expired message should be removed")

	val := counterValue(sp.metrics.PubSubShadowDivergences.WithLabelValues("data_mismatch"))
	assert.Equal(t, float64(1), val)
}

func TestShadowPubSub_ExtraOnSecondary(t *testing.T) {
	sp := newTestShadowPubSub(10 * time.Millisecond)

	sp.matchSecondary(&redis.Message{Channel: "ch1", Payload: "extra"})

	// matchSecondary now buffers unmatched secondaries; wait for the
	// comparison window to expire and sweep to trigger divergence reporting.
	time.Sleep(20 * time.Millisecond)
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
	sp := newTestShadowPubSub(10 * time.Millisecond)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "orphan"})
	time.Sleep(20 * time.Millisecond)

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
	sp.mu.Unlock()
}

func TestShadowPubSub_CompareLoopMatchesFromChannel(t *testing.T) {
	sp := newTestShadowPubSub(1 * time.Second)

	sp.RecordPrimary(&redis.Message{Channel: "ch1", Payload: "msg1"})

	ch := make(chan *redis.Message, 1)
	ch <- &redis.Message{Channel: "ch1", Payload: "msg1"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp.compareLoop(ch)
	}()

	// Give time for the message to be processed, then close.
	time.Sleep(10 * time.Millisecond)
	close(ch)
	wg.Wait()

	sp.mu.Lock()
	assert.Equal(t, 0, len(sp.pending), "message should be matched via compareLoop")
	sp.mu.Unlock()
}
