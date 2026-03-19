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
// Includes Pattern to correctly distinguish pmessage deliveries.
type msgKey struct {
	Pattern string
	Channel string
	Payload string
}

// secondaryPending represents a secondary message that has not yet been matched
// to a primary within the comparison window.
type secondaryPending struct {
	timestamp time.Time
	channel   string
	payload   string
}

// unmatchedSecondaries buffers unmatched secondary messages per shadowPubSub
// instance. This allows us to avoid reporting DivExtraData immediately when a
// secondary arrives before the corresponding primary (e.g. due to network
// jitter) and instead only report once the comparison window has elapsed.
var unmatchedSecondaries = struct {
	sync.Mutex
	data map[*shadowPubSub]map[msgKey]secondaryPending
}{
	data: make(map[*shadowPubSub]map[msgKey]secondaryPending),
}

// pendingMsg records a message awaiting its counterpart from the other source.
type pendingMsg struct {
	pattern   string
	channel   string
	payload   string
	timestamp time.Time
}

// divergenceEvent holds divergence info collected under lock for deferred reporting.
type divergenceEvent struct {
	channel string
	payload string
	kind    DivergenceKind
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
	started bool
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
	sp.mu.Lock()
	sp.started = true
	sp.mu.Unlock()
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
	key := msgKeyFromMessage(msg)
	sp.pending[key] = append(sp.pending[key], pendingMsg{
		pattern:   msg.Pattern,
		channel:   msg.Channel,
		payload:   msg.Payload,
		timestamp: time.Now(),
	})
}

// Close stops the shadow comparison and closes the secondary pub/sub.
// Safe to call even if Start was never called.
func (sp *shadowPubSub) Close() {
	sp.mu.Lock()
	sp.closed = true
	started := sp.started
	sp.mu.Unlock()
	sp.secondary.Close()
	if started {
		<-sp.done
	}
}

// compareLoop reads from the secondary channel and matches messages.
func (sp *shadowPubSub) compareLoop(ch <-chan *redis.Message) {
	ticker := time.NewTicker(defaultPubSubSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				// Channel closed — flush all remaining pending as divergences.
				sp.sweepAll()
				return
			}
			sp.matchSecondary(msg)
		case <-ticker.C:
			sp.sweepExpired()
		}
	}
}

// matchSecondary tries to match a secondary message against a pending primary message.
// Collects divergence info under lock and reports after releasing it.
func (sp *shadowPubSub) matchSecondary(msg *redis.Message) {
	sp.mu.Lock()

	key := msgKeyFromMessage(msg)
	if entries, ok := sp.pending[key]; ok && len(entries) > 0 {
		// Match found — remove the oldest pending primary message.
		if len(entries) == 1 {
			delete(sp.pending, key)
		} else {
			sp.pending[key] = entries[1:]
		}
		sp.mu.Unlock()
		return
	}

	sp.mu.Unlock()

	// No matching primary message at this moment. Buffer the secondary and only
	// report DivExtraData if it remains unmatched after the comparison window.
	now := time.Now()
	unmatchedSecondaries.Lock()
	defer unmatchedSecondaries.Unlock()

	perInstance, ok := unmatchedSecondaries.data[sp]
	if !ok {
		perInstance = make(map[msgKey]secondaryPending)
		unmatchedSecondaries.data[sp] = perInstance
	}

	perInstance[key] = secondaryPending{
		timestamp: now,
		channel:   msg.Channel,
		payload:   msg.Payload,
	}
}

// sweepExpired reports primary messages that were not matched within the window.
func (sp *shadowPubSub) sweepExpired() {
	var divergences []divergenceEvent

	sp.mu.Lock()
	now := time.Now()

	// Lock the unmatched secondary buffer while we reconcile primaries and
	// secondaries and age out any expired entries.
	unmatchedSecondaries.Lock()
	perInstance, ok := unmatchedSecondaries.data[sp]
	if !ok {
		perInstance = make(map[msgKey]secondaryPending)
		unmatchedSecondaries.data[sp] = perInstance
	}

	// First, reconcile pending primaries against buffered secondaries. If a
	// primary has a matching buffered secondary within the window, treat them
	// as matched and drop both without reporting a divergence.
	for key, entries := range sp.pending {
		var remaining []pendingMsg
		for _, e := range entries {
			if sec, ok := perInstance[key]; ok && now.Sub(sec.timestamp) < sp.window {
				// Matched with a buffered secondary; drop both.
				delete(perInstance, key)
				continue
			}
			if now.Sub(e.timestamp) >= sp.window {
				divergences = append(divergences, divergenceEvent{
					channel: e.channel,
					payload: e.payload,
					kind:    DivDataMismatch,
				})
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

	// Next, age out any remaining buffered secondaries that have exceeded the
	// comparison window and report them as extra_data divergences.
	for key, sec := range perInstance {
		if now.Sub(sec.timestamp) >= sp.window {
			divergences = append(divergences, divergenceEvent{
				channel: sec.channel,
				payload: sec.payload,
				kind:    DivExtraData,
			})
			delete(perInstance, key)
		}
	}

	// Clean up empty per-instance maps.
	if len(perInstance) == 0 {
		delete(unmatchedSecondaries.data, sp)
	}

	unmatchedSecondaries.Unlock()
	sp.mu.Unlock()

	for _, d := range divergences {
		sp.reportDivergence(d.channel, d.payload, d.kind)
	}
}

// sweepAll reports all remaining pending messages as divergences (used on shutdown).
func (sp *shadowPubSub) sweepAll() {
	var divergences []divergenceEvent

	sp.mu.Lock()
	for key, entries := range sp.pending {
		for _, e := range entries {
			divergences = append(divergences, divergenceEvent{
				channel: e.channel,
				payload: e.payload,
				kind:    DivDataMismatch,
			})
		}
		delete(sp.pending, key)
	}
	sp.mu.Unlock()

	for _, d := range divergences {
		sp.reportDivergence(d.channel, d.payload, d.kind)
	}
}

func (sp *shadowPubSub) reportDivergence(channel, payload string, kind DivergenceKind) {
	sp.metrics.PubSubShadowDivergences.WithLabelValues(kind.String()).Inc()
	sp.logger.Warn("pubsub shadow divergence",
		"channel", truncateValue(channel),
		"payload", truncateValue(payload),
		"kind", kind.String(),
	)

	var primary, secondary any
	switch kind { //nolint:exhaustive // only two kinds apply to pub/sub shadow
	case DivExtraData:
		primary = nil
		secondary = payload
	default:
		primary = payload
		secondary = nil
	}
	sp.sentry.CaptureDivergence(Divergence{
		Command:    "SUBSCRIBE",
		Key:        channel,
		Kind:       kind,
		Primary:    primary,
		Secondary:  secondary,
		DetectedAt: time.Now(),
	})
}

func msgKeyFromMessage(msg *redis.Message) msgKey {
	return msgKey{
		Pattern: msg.Pattern,
		Channel: msg.Channel,
		Payload: msg.Payload,
	}
}
