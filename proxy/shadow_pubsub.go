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

// pendingMsg records a message awaiting its counterpart from the other source.
type pendingMsg struct {
	pattern   string
	channel   string
	payload   string
	timestamp time.Time
}

// divergenceEvent holds divergence info collected under lock for deferred reporting.
type divergenceEvent struct {
	channel   string
	payload   string
	pattern   string // non-empty for PSUBSCRIBE/pmessage deliveries
	kind      DivergenceKind
	isPattern bool // true if this originated from a PSUBSCRIBE
}

// shadowPubSub subscribes to the secondary backend for the same channels
// and compares messages against the primary to detect divergences.
type shadowPubSub struct {
	secondary *redis.PubSub
	metrics   *ProxyMetrics
	sentry    *SentryReporter
	logger    *slog.Logger
	window    time.Duration
	nowFunc   func() time.Time // injectable clock; defaults to time.Now

	mu      sync.Mutex
	pending map[msgKey][]pendingMsg // primary messages awaiting secondary match
	// unmatchedSecondaries buffers secondary messages that arrived before a
	// corresponding primary. Protected by mu. Each key maps to a slice to
	// handle duplicate messages correctly.
	unmatchedSecondaries map[msgKey][]secondaryPending
	closed               bool
	started              bool
	startOnce            sync.Once
	done                 chan struct{}
}

func newShadowPubSub(backend PubSubBackend, metrics *ProxyMetrics, sentry *SentryReporter, logger *slog.Logger, window time.Duration) *shadowPubSub {
	return &shadowPubSub{
		secondary:            backend.NewPubSub(context.Background()),
		metrics:              metrics,
		sentry:               sentry,
		logger:               logger,
		window:               window,
		nowFunc:              time.Now,
		pending:              make(map[msgKey][]pendingMsg),
		unmatchedSecondaries: make(map[msgKey][]secondaryPending),
		done:                 make(chan struct{}),
	}
}

// Start begins reading from the secondary and comparing messages.
// Must be called after initial subscribe. Safe to call multiple times;
// only the first call launches the compare loop.
func (sp *shadowPubSub) Start() {
	sp.startOnce.Do(func() {
		sp.mu.Lock()
		sp.started = true
		sp.mu.Unlock()
		ch := sp.secondary.Channel()
		go func() {
			defer close(sp.done)
			sp.compareLoop(ch)
		}()
	})
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
// It first attempts to immediately reconcile with any buffered unmatched secondary
// that arrived earlier (within the comparison window). This avoids falsely reporting
// a data_mismatch when PubSubCompareWindow is smaller than the sweep interval.
func (sp *shadowPubSub) RecordPrimary(msg *redis.Message) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.closed {
		return
	}
	key := msgKeyFromMessage(msg)
	now := sp.nowFunc()

	// Attempt immediate reconciliation with any buffered unmatched secondary.
	if sp.reconcileWithBufferedSecondary(key, now) {
		// Primary and secondary matched; no need to queue.
		return
	}

	// No suitable secondary was buffered; queue this primary for later comparison.
	sp.pending[key] = append(sp.pending[key], pendingMsg{
		pattern:   msg.Pattern,
		channel:   msg.Channel,
		payload:   msg.Payload,
		timestamp: now,
	})
}

// reconcileWithBufferedSecondary checks whether an unmatched secondary message
// exists for the given key and is still within the comparison window. If one is
// found it is consumed and true is returned. Caller must hold sp.mu.
func (sp *shadowPubSub) reconcileWithBufferedSecondary(key msgKey, now time.Time) bool {
	secs, ok := sp.unmatchedSecondaries[key]
	if !ok || len(secs) == 0 {
		return false
	}
	for i, sec := range secs {
		dt := now.Sub(sec.timestamp)
		if dt > sp.window || dt < -sp.window {
			continue
		}
		// Consume this secondary — remove it from the slice.
		secs = append(secs[:i], secs[i+1:]...)
		if len(secs) == 0 {
			delete(sp.unmatchedSecondaries, key)
		} else {
			sp.unmatchedSecondaries[key] = secs
		}
		return true
	}
	return false
}

// Close stops the shadow comparison and closes the secondary pub/sub.
// Safe to call even if Start was never called.
func (sp *shadowPubSub) Close() {
	sp.mu.Lock()
	sp.closed = true
	started := sp.started
	sp.mu.Unlock()
	if sp.secondary != nil {
		sp.secondary.Close()
	}
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
				// Channel closed — mark as closed so RecordPrimary becomes a no-op,
				// then flush all remaining pending as divergences.
				sp.mu.Lock()
				sp.closed = true
				sp.mu.Unlock()
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
	defer sp.mu.Unlock()

	key := msgKeyFromMessage(msg)
	now := sp.nowFunc()
	if entries, ok := sp.pending[key]; ok && len(entries) > 0 {
		oldest := entries[0]
		if now.Sub(oldest.timestamp) <= sp.window {
			// Match found within window — remove the oldest pending primary message.
			if len(entries) == 1 {
				delete(sp.pending, key)
			} else {
				sp.pending[key] = entries[1:]
			}
			return
		}
		// The oldest pending primary is already past the window. Let the periodic
		// sweep report it as DivDataMismatch; fall through to buffer this secondary
		// so it can be reported as DivExtraData if it also remains unmatched.
	}

	// No matching primary message within the window. Buffer the secondary and only
	// report DivExtraData if it remains unmatched after the comparison window.
	sp.unmatchedSecondaries[key] = append(sp.unmatchedSecondaries[key], secondaryPending{
		timestamp: now,
		channel:   msg.Channel,
		payload:   msg.Payload,
	})
}

// sweepExpired reports primary messages that were not matched within the window.
func (sp *shadowPubSub) sweepExpired() {
	var divergences []divergenceEvent

	sp.mu.Lock()
	now := sp.nowFunc()

	// Expire old buffered secondaries first so they cannot be consumed as a
	// "match" during reconciliation (prevents bypassing the comparison window).
	divergences = sweepExpiredSecondaries(now, sp.window, sp.unmatchedSecondaries, divergences)
	divergences = sp.reconcilePrimaries(now, sp.unmatchedSecondaries, divergences)

	sp.mu.Unlock()

	for _, d := range divergences {
		sp.reportDivergence(d)
	}
}

// reconcilePrimaries matches pending primaries against buffered secondaries,
// reporting expired unmatched primaries as divergences.
// Caller must hold sp.mu.
func (sp *shadowPubSub) reconcilePrimaries(now time.Time, secBuf map[msgKey][]secondaryPending, out []divergenceEvent) []divergenceEvent {
	for key, entries := range sp.pending {
		var remaining []pendingMsg
		for _, e := range entries {
			if now.Sub(e.timestamp) >= sp.window {
				// Primary has expired — report as divergence regardless of any buffered
				// secondaries. A late secondary must not suppress a window violation.
				out = append(out, divergenceEvent{channel: e.channel, payload: e.payload, pattern: e.pattern, kind: DivDataMismatch, isPattern: e.pattern != ""})
				continue
			}
			if secs := secBuf[key]; len(secs) > 0 {
				// Match found within window — consume the oldest buffered secondary.
				if len(secs) == 1 {
					delete(secBuf, key)
				} else {
					secBuf[key] = secs[1:]
				}
				continue
			}
			remaining = append(remaining, e)
		}
		if len(remaining) == 0 {
			delete(sp.pending, key)
		} else {
			sp.pending[key] = remaining
		}
	}
	return out
}

// sweepExpiredSecondaries ages out buffered secondaries past the window.
func sweepExpiredSecondaries(now time.Time, window time.Duration, secBuf map[msgKey][]secondaryPending, out []divergenceEvent) []divergenceEvent {
	for key, secs := range secBuf {
		var remaining []secondaryPending
		for _, sec := range secs {
			if now.Sub(sec.timestamp) >= window {
				out = append(out, divergenceEvent{channel: sec.channel, payload: sec.payload, pattern: key.Pattern, kind: DivExtraData, isPattern: key.Pattern != ""})
			} else {
				remaining = append(remaining, sec)
			}
		}
		if len(remaining) == 0 {
			delete(secBuf, key)
		} else {
			secBuf[key] = remaining
		}
	}
	return out
}

// sweepAll reports all remaining pending primaries and buffered secondaries
// as divergences (used on shutdown / channel close).
func (sp *shadowPubSub) sweepAll() {
	var divergences []divergenceEvent

	sp.mu.Lock()
	for key, entries := range sp.pending {
		for _, e := range entries {
			divergences = append(divergences, divergenceEvent{
				channel:   e.channel,
				payload:   e.payload,
				pattern:   e.pattern,
				kind:      DivDataMismatch,
				isPattern: e.pattern != "",
			})
		}
		delete(sp.pending, key)
	}
	// Also drain any buffered unmatched secondaries for this instance.
	for key, secs := range sp.unmatchedSecondaries {
		for _, sec := range secs {
			divergences = append(divergences, divergenceEvent{
				channel:   sec.channel,
				payload:   sec.payload,
				pattern:   key.Pattern,
				kind:      DivExtraData,
				isPattern: key.Pattern != "",
			})
		}
		delete(sp.unmatchedSecondaries, key)
	}
	sp.mu.Unlock()

	for _, d := range divergences {
		sp.reportDivergence(d)
	}
}

func (sp *shadowPubSub) reportDivergence(d divergenceEvent) {
	sp.metrics.PubSubShadowDivergences.WithLabelValues(d.kind.String()).Inc()
	logAttrs := []any{
		"channel", truncateValue(d.channel),
		"payload", truncateValue(d.payload),
		"kind", d.kind.String(),
	}
	if d.pattern != "" {
		logAttrs = append(logAttrs, "pattern", truncateValue(d.pattern))
	}
	sp.logger.Warn("pubsub shadow divergence", logAttrs...)

	cmd := "SUBSCRIBE"
	if d.isPattern {
		cmd = "PSUBSCRIBE"
	}

	var primary, secondary any
	if d.kind == DivExtraData {
		primary = nil
		secondary = d.payload
	} else {
		primary = d.payload
		secondary = nil
	}
	sp.sentry.CaptureDivergence(Divergence{
		Command:    cmd,
		Key:        d.channel,
		Pattern:    d.pattern,
		Kind:       d.kind,
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
