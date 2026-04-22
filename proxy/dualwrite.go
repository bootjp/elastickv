package proxy

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// maxWriteGoroutines limits concurrent secondary write goroutines.
	maxWriteGoroutines = 4096
	// maxShadowGoroutines limits concurrent shadow read goroutines separately
	// so that secondary write failures cannot starve shadow reads.
	maxShadowGoroutines = 1024
	// maxScriptWriteGoroutines limits concurrent secondary Lua-script write goroutines
	// (EVAL / EVALSHA). Lua scripts under high load cause write conflicts in the Raft
	// layer, and each conflict triggers a full script re-execution. Capping the
	// concurrency reduces contention so individual scripts complete within
	// SecondaryTimeout. Excess secondary script writes may be dropped to keep
	// contention bounded; this is only tolerable in modes where the script write
	// is targeting the non-authoritative backend.
	maxScriptWriteGoroutines = 64

	// maxCompactedRetries caps retries when the secondary returns
	// "read timestamp has been compacted". Each attempt re-sends the command so
	// the secondary re-selects a fresh read snapshot; a small bound is enough
	// because the compaction waterline advances slowly relative to SecondaryTimeout.
	maxCompactedRetries = 3
	// compactedRetryInitialBackoff is the first delay before retrying a secondary
	// command that failed with a compacted-read error.
	compactedRetryInitialBackoff = 10 * time.Millisecond
	// compactedRetryMaxBackoff caps the jittered exponential backoff so it
	// stays well within SecondaryTimeout even if the retry policy is ever
	// widened.
	compactedRetryMaxBackoff = 100 * time.Millisecond
	// compactedRetryBackoffFactor is the multiplier applied to the backoff
	// between retries; 2 gives a conventional exponential schedule.
	compactedRetryBackoffFactor = 2
	// compactedRetryJitterDivisor bounds the jitter to backoff/divisor so
	// jitter stays small relative to the base delay.
	compactedRetryJitterDivisor = 2
)

// readTSCompactedMarker is the substring produced by
// store.ErrReadTSCompacted as it flows through gRPC (wrapped as
// FailedPrecondition) and Lua PCall. Matching on substring is necessary
// because both layers erase the typed error.
const readTSCompactedMarker = "read timestamp has been compacted"

func isReadTSCompactedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), readTSCompactedMarker)
}

// DualWriter routes commands to primary and secondary backends based on mode.
type DualWriter struct {
	primary   Backend
	secondary Backend
	cfg       ProxyConfig
	shadow    *ShadowReader
	metrics   *ProxyMetrics
	sentry    *SentryReporter
	logger    *slog.Logger

	writeSem  chan struct{} // bounds concurrent secondary write goroutines
	shadowSem chan struct{} // bounds concurrent shadow read goroutines
	scriptSem chan struct{} // bounds concurrent secondary Lua-script write goroutines

	wg       sync.WaitGroup
	mu       sync.Mutex // protects closed; held briefly to make wg.Add atomic with close check
	closed   bool
	scriptMu sync.RWMutex
	scripts  map[string]string
	// scriptOrder tracks insertion order for FIFO eviction of the bounded script cache.
	scriptOrder []string
}

// NewDualWriter creates a DualWriter with the given backends.
func NewDualWriter(primary, secondary Backend, cfg ProxyConfig, metrics *ProxyMetrics, sentryReporter *SentryReporter, logger *slog.Logger) *DualWriter {
	d := &DualWriter{
		primary:   primary,
		secondary: secondary,
		cfg:       cfg,
		metrics:   metrics,
		sentry:    sentryReporter,
		logger:    logger,
		writeSem:  make(chan struct{}, maxWriteGoroutines),
		shadowSem: make(chan struct{}, maxShadowGoroutines),
		scriptSem: make(chan struct{}, maxScriptWriteGoroutines),
		scripts:   make(map[string]string),
	}

	if cfg.Mode == ModeDualWriteShadow || cfg.Mode == ModeElasticKVPrimary {
		// Shadow reads go to the non-primary backend for comparison.
		// Note: main.go already swaps primary/secondary for ElasticKVPrimary mode,
		// so here "secondary" is always the non-primary backend.
		shadowBackend := secondary
		d.shadow = NewShadowReader(shadowBackend, metrics, sentryReporter, logger, cfg.ShadowTimeout)
	}

	return d
}

// Close waits for all in-flight async goroutines to finish.
// Should be called during graceful shutdown.
func (d *DualWriter) Close() {
	// Set closed under the mutex so that no concurrent goAsyncWithSem call
	// can slip a wg.Add(1) in after wg.Wait() starts (which would panic).
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	d.wg.Wait()
}

// Write sends a write command to the primary synchronously, then to the secondary asynchronously.
// cmd must be the pre-uppercased command name.
func (d *DualWriter) Write(ctx context.Context, cmd string, args [][]byte) (any, error) {
	iArgs := bytesArgsToInterfaces(args)

	start := time.Now()
	result := d.primary.Do(ctx, iArgs...)
	resp, err := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.primary.Name()).Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, redis.Nil) {
		d.metrics.PrimaryWriteErrors.Inc()
		d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "error").Inc()
		d.logger.Error("primary write failed", "cmd", cmd, "err", err)
		return nil, fmt.Errorf("primary write %s: %w", cmd, err)
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "ok").Inc()

	// Secondary: async fire-and-forget (bounded)
	if d.hasSecondaryWrite() {
		d.goWrite(func() { d.writeSecondary(cmd, iArgs) })
	}

	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

// Read sends a read command to the primary and optionally performs a shadow read.
// cmd must be the pre-uppercased command name.
func (d *DualWriter) Read(ctx context.Context, cmd string, args [][]byte) (any, error) {
	iArgs := bytesArgsToInterfaces(args)

	start := time.Now()
	result := d.primary.Do(ctx, iArgs...)
	resp, err := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.primary.Name()).Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, redis.Nil) {
		d.metrics.PrimaryReadErrors.Inc()
		d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "error").Inc()
		return nil, fmt.Errorf("primary read %s: %w", cmd, err)
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "ok").Inc()

	// Shadow read (bounded, separate semaphore from writes)
	if d.shadow != nil {
		shadowArgs := args
		shadowResp := resp
		shadowErr := err
		d.goShadow(func() {
			d.shadow.Compare(ctx, cmd, shadowArgs, shadowResp, shadowErr)
		})
	}

	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

// Blocking forwards a blocking command to the primary only.
// Optionally sends a short-timeout version to secondary for warmup.
// cmd must be the pre-uppercased command name.
func (d *DualWriter) Blocking(ctx context.Context, cmd string, args [][]byte) (any, error) {
	iArgs := bytesArgsToInterfaces(args)
	timeout := blockingCommandTimeout(cmd, args)

	start := time.Now()
	var result *redis.Cmd
	if blockingBackend, ok := d.primary.(blockingTimeoutBackend); ok {
		result = blockingBackend.DoWithTimeout(ctx, timeout, iArgs...)
	} else {
		result = d.primary.Do(ctx, iArgs...)
	}
	resp, err := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.primary.Name()).Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, redis.Nil) {
		d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "error").Inc()
		return nil, fmt.Errorf("primary blocking %s: %w", cmd, err)
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "ok").Inc()

	// Warmup: send to secondary with short timeout (fire-and-forget, bounded)
	if d.hasSecondaryWrite() {
		d.goWrite(func() {
			sCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			d.secondary.Do(sCtx, iArgs...)
		})
	}

	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

// Admin forwards an admin command to the primary only.
// cmd must be the pre-uppercased command name.
func (d *DualWriter) Admin(ctx context.Context, cmd string, args [][]byte) (any, error) {
	iArgs := bytesArgsToInterfaces(args)

	start := time.Now()
	result := d.primary.Do(ctx, iArgs...)
	resp, err := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.primary.Name()).Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, redis.Nil) {
		d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "error").Inc()
		return nil, fmt.Errorf("primary admin %s: %w", cmd, err)
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "ok").Inc()
	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

// Script forwards EVAL/EVALSHA to the primary, and async replays to secondary.
// cmd must be the pre-uppercased command name.
func (d *DualWriter) Script(ctx context.Context, cmd string, args [][]byte) (any, error) {
	iArgs := bytesArgsToInterfaces(args)

	start := time.Now()
	result := d.primary.Do(ctx, iArgs...)
	resp, err := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.primary.Name()).Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, redis.Nil) {
		d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "error").Inc()
		return nil, fmt.Errorf("primary script %s: %w", cmd, err)
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "ok").Inc()
	d.rememberScript(cmd, args)

	if d.hasSecondaryWrite() {
		d.goScript(func() { d.writeSecondary(cmd, iArgs) })
	}

	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

// writeSecondary sends the command to the secondary, handling the NOSCRIPT
// → EVAL fallback and transparently retrying when the secondary reports that
// the read snapshot has been compacted. A re-sent command causes the backend
// to re-select a fresh read timestamp, which is the only way to recover once
// the original startTS has fallen behind MinRetainedTS on a peer node.
//
// The secondary's raw redis error is kept in sErr (not wrapped) so that
// writeSecondary can classify it via errors.Is(sErr, redis.Nil), attach the
// original message to Sentry and the structured log, and so the retry
// predicate isReadTSCompactedError matches the exact substring coming back
// from gRPC.
func (d *DualWriter) writeSecondary(cmd string, iArgs []any) {
	sCtx, cancel := context.WithTimeout(context.Background(), d.cfg.SecondaryTimeout)
	defer cancel()

	start := time.Now()

	backoff := compactedRetryInitialBackoff
	var sErr error
	var attempt int
	var usedNOSCRIPTFallback bool
	args := iArgs
	for ; ; attempt++ {
		result := d.secondary.Do(sCtx, args...)
		_, sErr = result.Result()
		if isNoScriptError(sErr) {
			// After a successful NOSCRIPT→EVAL resolution, retries reuse the
			// resolved EVAL args so we don't waste a round-trip on the
			// known-missing EVALSHA every iteration.
			if fallbackArgs, ok := d.evalFallbackArgs(cmd, args); ok {
				usedNOSCRIPTFallback = true
				args = fallbackArgs
				result = d.secondary.Do(sCtx, args...)
				_, sErr = result.Result()
			}
		}
		if !isReadTSCompactedError(sErr) {
			break
		}
		if attempt >= maxCompactedRetries {
			break
		}
		d.logger.Debug("retrying secondary write on compacted snapshot",
			"cmd", cmd, "attempt", attempt+1, "backoff", backoff, "err", sErr)
		if !waitCompactedRetryBackoff(sCtx, backoff) {
			break
		}
		backoff = nextCompactedRetryBackoff(backoff)
	}

	elapsed := time.Since(start)
	d.metrics.CommandDuration.WithLabelValues(cmd, d.secondary.Name()).Observe(elapsed.Seconds())

	if sErr != nil && !errors.Is(sErr, redis.Nil) {
		d.recordSecondaryWriteFailure(cmd, iArgs, elapsed, attempt+1, usedNOSCRIPTFallback, sErr)
		return
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.secondary.Name(), "ok").Inc()
}

// recordSecondaryWriteFailure updates metrics, reports to Sentry, and emits a
// structured warning with enough context to diagnose EVALSHA timeouts.
func (d *DualWriter) recordSecondaryWriteFailure(cmd string, iArgs []any, elapsed time.Duration, attempts int, usedNOSCRIPTFallback bool, sErr error) {
	d.metrics.SecondaryWriteErrors.Inc()
	reason := classifySecondaryWriteError(sErr)
	d.metrics.SecondaryWriteErrorsByReason.WithLabelValues(cmd, reason).Inc()
	d.metrics.CommandTotal.WithLabelValues(cmd, d.secondary.Name(), "error").Inc()
	fingerprint := fmt.Sprintf("secondary_write_%s", cmd)
	if d.sentry.ShouldReport(fingerprint) {
		d.sentry.CaptureException(sErr, "secondary_write_failure", argsToBytes(iArgs))
	}
	warnArgs := []any{"cmd", cmd, "err", sErr, "elapsed", elapsed, "attempts", attempts}
	if (cmd == "EVALSHA" || cmd == "EVALSHA_RO") && len(iArgs) > 1 {
		switch v := iArgs[1].(type) {
		case []byte:
			warnArgs = append(warnArgs, "sha", string(v))
		case string:
			warnArgs = append(warnArgs, "sha", v)
		}
	}
	if usedNOSCRIPTFallback {
		warnArgs = append(warnArgs, "noscript_fallback", true)
	}
	d.logger.Warn("secondary write failed", warnArgs...)
}

// waitCompactedRetryBackoff sleeps for a jittered interval or returns early
// when the context is cancelled. Returns false if the caller should abort
// the retry loop (context done).
//
// Jitter is in [backoff, backoff + backoff/2) so that concurrent retries
// caused by a single compaction waterline advancement do not re-hit the
// secondary in lockstep. A NewTimer is used instead of time.After so the
// timer is released promptly on ctx cancellation (avoiding a leak until
// expiry when the async goroutine is shutting down).
func waitCompactedRetryBackoff(ctx context.Context, backoff time.Duration) bool {
	if backoff <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(backoff + jitterFor(backoff))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// jitterFor returns a random additive jitter in [0, backoff/compactedRetryJitterDivisor).
// crypto/rand is used so the code does not trip the gosec G404 rule that
// flags math/rand for randomness; the security grade does not matter for a
// retry spread but this avoids a blanket lint suppression.
func jitterFor(backoff time.Duration) time.Duration {
	window := int64(backoff / compactedRetryJitterDivisor)
	if window <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(window))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

func nextCompactedRetryBackoff(current time.Duration) time.Duration {
	next := current * compactedRetryBackoffFactor
	if next > compactedRetryMaxBackoff {
		return compactedRetryMaxBackoff
	}
	return next
}

// goWrite launches fn in a bounded write goroutine.
func (d *DualWriter) goWrite(fn func()) {
	d.goAsyncWithSem(d.writeSem, fn)
}

// goScript launches fn in a bounded Lua-script write goroutine.
// It uses a smaller semaphore than goWrite to cap the number of concurrent
// EVAL/EVALSHA secondary writes. When the cap is reached the write is dropped.
func (d *DualWriter) goScript(fn func()) {
	d.goAsyncWithSem(d.scriptSem, fn)
}

// goShadow launches fn in a bounded shadow-read goroutine.
func (d *DualWriter) goShadow(fn func()) {
	d.goAsyncWithSem(d.shadowSem, fn)
}

// goAsync launches fn using the write semaphore (for backward compat with txn replay).
func (d *DualWriter) goAsync(fn func()) {
	d.goWrite(fn)
}

// goAsyncWithSem launches fn in a bounded goroutine using the given semaphore.
// If the DualWriter is closing or the semaphore is full, the work is dropped.
func (d *DualWriter) goAsyncWithSem(sem chan struct{}, fn func()) {
	// Hold mu while checking closed and calling wg.Add so that Close() cannot
	// start wg.Wait() between the closed-check and the wg.Add(1) call.
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	select {
	case sem <- struct{}{}:
		d.wg.Add(1)
		d.mu.Unlock()
		go func() {
			defer func() {
				<-sem
				d.wg.Done()
			}()
			fn()
		}()
	default:
		d.mu.Unlock()
		d.metrics.AsyncDrops.Inc()
		d.logger.Warn("async goroutine limit reached, dropping secondary operation")
	}
}

func (d *DualWriter) hasSecondaryWrite() bool {
	switch d.cfg.Mode {
	case ModeDualWrite, ModeDualWriteShadow, ModeElasticKVPrimary:
		return true
	case ModeRedisOnly, ModeElasticKVOnly:
		return false
	}
	return false
}

// Primary returns the primary backend for direct use (e.g., PubSub).
func (d *DualWriter) Primary() Backend {
	return d.primary
}

// PubSubBackend returns the primary backend as a PubSubBackend, or nil.
func (d *DualWriter) PubSubBackend() PubSubBackend {
	if ps, ok := d.primary.(PubSubBackend); ok {
		return ps
	}
	return nil
}

// ShadowPubSubBackend returns the secondary backend as a PubSubBackend
// when shadow mode is active, or nil otherwise.
func (d *DualWriter) ShadowPubSubBackend() PubSubBackend {
	if d.shadow == nil {
		return nil
	}
	if ps, ok := d.secondary.(PubSubBackend); ok {
		return ps
	}
	return nil
}

// Secondary returns the secondary backend.
func (d *DualWriter) Secondary() Backend {
	return d.secondary
}

func argsToBytes(iArgs []any) [][]byte {
	out := make([][]byte, len(iArgs))
	for i, a := range iArgs {
		if b, ok := a.([]byte); ok {
			out[i] = b
		} else {
			out[i] = fmt.Appendf(nil, "%v", a)
		}
	}
	return out
}

// classifySecondaryWriteError maps a secondary-write error to a small fixed set
// of reason labels suitable for a Prometheus counter. The elastickv secondary
// backend is in-house, so matching on substrings of the error message is safe.
//
// Order matters: "retry limit exceeded" is checked before "write conflict"
// because the retry-limit message embeds the underlying conflict string, and
// we want the outer (retry_limit) classification to win.
func classifySecondaryWriteError(err error) string {
	if err == nil {
		return "other"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "retry limit exceeded"):
		return "retry_limit"
	case strings.Contains(msg, "write conflict"):
		return "write_conflict"
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "deadline exceeded"):
		return "deadline_exceeded"
	case strings.Contains(msg, "not leader"):
		return "not_leader"
	case strings.Contains(msg, "txn already committed") || strings.Contains(msg, "txn already aborted"):
		return "txn_already_finalized"
	default:
		return "other"
	}
}
