package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// msgKey is used as a map key for matching primary and secondary messages.
type msgKey struct {
	Channel string
	Payload string
}

// pendingMsg records a message awaiting its counterpart from the other source.
type pendingMsg struct {
	channel   string
	payload   string
	timestamp time.Time
}

// shadowPubSub subscribes to the secondary backend for the same channels
// and compares messages against the primary to detect divergences.
type shadowPubSub struct {
	secondary *redis.PubSub
	metrics   *ProxyMetrics
	sentry    *SentryReporter
	logger    *slog.Logger
	window    time.Duration

	mu      sync.Mutex
	pending map[msgKey][]pendingMsg // primary messages awaiting secondary match
	closed  bool
	done    chan struct{}
}

func newShadowPubSub(backend PubSubBackend, metrics *ProxyMetrics, sentry *SentryReporter, logger *slog.Logger, window time.Duration) *shadowPubSub {
	return &shadowPubSub{
		secondary: backend.NewPubSub(context.Background()),
		metrics:   metrics,
		sentry:    sentry,
		logger:    logger,
		window:    window,
		pending:   make(map[msgKey][]pendingMsg),
		done:      make(chan struct{}),
	}
}

// Start begins reading from the secondary and comparing messages.
// Must be called after initial subscribe.
func (sp *shadowPubSub) Start() {
	ch := sp.secondary.Channel()
	go func() {
		defer close(sp.done)
		sp.compareLoop(ch)
	}()
}

// Subscribe mirrors a SUBSCRIBE to the secondary.
func (sp *shadowPubSub) Subscribe(ctx context.Context, channels ...string) error {
	if err := sp.secondary.Subscribe(ctx, channels...); err != nil {
		return fmt.Errorf("shadow subscribe: %w", err)
	}
	return nil
}

// PSubscribe mirrors a PSUBSCRIBE to the secondary.
func (sp *shadowPubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	if err := sp.secondary.PSubscribe(ctx, patterns...); err != nil {
		return fmt.Errorf("shadow psubscribe: %w", err)
	}
	return nil
}

// Unsubscribe mirrors an UNSUBSCRIBE to the secondary.
func (sp *shadowPubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	if err := sp.secondary.Unsubscribe(ctx, channels...); err != nil {
		return fmt.Errorf("shadow unsubscribe: %w", err)
	}
	return nil
}

// PUnsubscribe mirrors a PUNSUBSCRIBE to the secondary.
func (sp *shadowPubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	if err := sp.secondary.PUnsubscribe(ctx, patterns...); err != nil {
		return fmt.Errorf("shadow punsubscribe: %w", err)
	}
	return nil
}

// RecordPrimary records a message received from the primary for comparison.
func (sp *shadowPubSub) RecordPrimary(msg *redis.Message) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.closed {
		return
	}
	key := msgKey{Channel: msg.Channel, Payload: msg.Payload}
	sp.pending[key] = append(sp.pending[key], pendingMsg{
		channel:   msg.Channel,
		payload:   msg.Payload,
		timestamp: time.Now(),
	})
}

// Close stops the shadow comparison and closes the secondary pub/sub.
func (sp *shadowPubSub) Close() {
	sp.mu.Lock()
	sp.closed = true
	sp.mu.Unlock()
	sp.secondary.Close()
	<-sp.done
}

// compareLoop reads from the secondary channel and matches messages.
func (sp *shadowPubSub) compareLoop(ch <-chan *redis.Message) {
	ticker := time.NewTicker(defaultPubSubSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				// Channel closed — secondary connection terminated.
				sp.sweepExpired()
				return
			}
			sp.matchSecondary(msg)
		case <-ticker.C:
			sp.sweepExpired()
		}
	}
}

// matchSecondary tries to match a secondary message against a pending primary message.
func (sp *shadowPubSub) matchSecondary(msg *redis.Message) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	key := msgKey{Channel: msg.Channel, Payload: msg.Payload}
	if entries, ok := sp.pending[key]; ok && len(entries) > 0 {
		// Match found — remove the oldest pending primary message.
		if len(entries) == 1 {
			delete(sp.pending, key)
		} else {
			sp.pending[key] = entries[1:]
		}
		return
	}

	// No matching primary message — extra on secondary.
	sp.reportDivergence(msg.Channel, msg.Payload, DivExtraData)
}

// sweepExpired reports primary messages that were not matched within the window.
func (sp *shadowPubSub) sweepExpired() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	now := time.Now()
	for key, entries := range sp.pending {
		var remaining []pendingMsg
		for _, e := range entries {
			if now.Sub(e.timestamp) >= sp.window {
				sp.reportDivergenceLocked(e.channel, e.payload, DivDataMismatch)
			} else {
				remaining = append(remaining, e)
			}
		}
		if len(remaining) == 0 {
			delete(sp.pending, key)
		} else {
			sp.pending[key] = remaining
		}
	}
}

func (sp *shadowPubSub) reportDivergence(channel, payload string, kind DivergenceKind) {
	sp.metrics.PubSubShadowDivergences.WithLabelValues(channel, kind.String()).Inc()
	sp.logger.Warn("pubsub shadow divergence",
		"channel", truncateValue(channel),
		"payload", truncateValue(payload),
		"kind", kind.String(),
	)
	sp.sentry.CaptureDivergence(Divergence{
		Command:    "SUBSCRIBE",
		Key:        channel,
		Kind:       kind,
		Primary:    payload,
		Secondary:  nil,
		DetectedAt: time.Now(),
	})
}

// reportDivergenceLocked is the same as reportDivergence but assumes mu is held.
// It releases the lock briefly for the report to avoid holding it during I/O.
func (sp *shadowPubSub) reportDivergenceLocked(channel, payload string, kind DivergenceKind) {
	// Metrics and logging are goroutine-safe; safe to call under lock.
	sp.metrics.PubSubShadowDivergences.WithLabelValues(channel, kind.String()).Inc()
	sp.logger.Warn("pubsub shadow divergence",
		"channel", truncateValue(channel),
		"payload", truncateValue(payload),
		"kind", kind.String(),
	)
	sp.sentry.CaptureDivergence(Divergence{
		Command:    "SUBSCRIBE",
		Key:        channel,
		Kind:       kind,
		Primary:    payload,
		Secondary:  nil,
		DetectedAt: time.Now(),
	})
}
