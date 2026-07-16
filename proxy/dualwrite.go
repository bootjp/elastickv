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
	"sync/atomic"
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
	// SecondaryTimeout. Strict dual-write script replays wait for capacity instead
	// of being dropped; best-effort users of goScript may still drop.
	maxScriptWriteGoroutines = 64

	// maxSecondaryTransientRetries caps proxy-level retries when the secondary
	// returns a transient OCC/read-snapshot error after exhausting its own retry
	// loop. SecondaryTimeout still bounds the whole replay.
	maxSecondaryTransientRetries = 3
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

	// asyncDropLogInterval keeps the secondary-drop warning useful without
	// emitting one log line per dropped fire-and-forget write under load.
	asyncDropLogInterval = 5 * time.Second
)

type leaderRefreshingBackend interface {
	RefreshLeaderNow(context.Context)
}

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

func isRetryableSecondaryWriteError(err error) bool {
	if err == nil {
		return false
	}
	if isReadTSCompactedError(err) {
		return true
	}
	if isElasticKVNotLeaderError(err) {
		return true
	}
	switch classifySecondaryWriteError(err) {
	case "retry_limit", "write_conflict", "txn_locked":
		return true
	default:
		return false
	}
}

func isNotLeaderError(err error) bool {
	return isElasticKVNotLeaderError(err)
}

func isElasticKVNotLeaderError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	if strings.HasPrefix(strings.ToUpper(msg), "NOTLEADER ") {
		return true
	}
	if strings.Contains(msg, "etcd raft engine is not leader") ||
		strings.Contains(msg, "raft engine: not leader") {
		return true
	}
	return msg == "leader not found" ||
		strings.HasSuffix(msg, "desc = leader not found")
}

func refreshSecondaryLeader(ctx context.Context, backend Backend, err error) {
	if !isNotLeaderError(err) {
		return
	}
	if refresher, ok := backend.(leaderRefreshingBackend); ok {
		refresher.RefreshLeaderNow(ctx)
	}
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
	asyncDropMu sync.Mutex
	// nextAsyncDropLog and suppressedAsyncDrops are accessed on the hot drop
	// path, so use atomics for the fast path and keep the mutex for the rare
	// logging path.
	nextAsyncDropLog     int64
	suppressedAsyncDrops int64
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
		writeSem:  make(chan struct{}, secondaryWriteConcurrency(cfg)),
		shadowSem: make(chan struct{}, maxShadowGoroutines),
		scriptSem: make(chan struct{}, secondaryScriptConcurrency(cfg)),
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

func secondaryWriteConcurrency(cfg ProxyConfig) int {
	return configuredConcurrencyOrDefault(cfg.SecondaryWriteConcurrency, maxWriteGoroutines)
}

func secondaryScriptConcurrency(cfg ProxyConfig) int {
	return configuredConcurrencyOrDefault(cfg.SecondaryScriptConcurrency, maxScriptWriteGoroutines)
}

func configuredConcurrencyOrDefault(configured, fallback int) int {
	if configured > 0 {
		return configured
	}
	return fallback
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

// Write sends a write command to the primary synchronously, then to the secondary.
// The secondary write uses a bounded async slot when possible, but applies
// caller backpressure instead of dropping when the slot pool is saturated.
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

	if d.hasSecondaryWrite() {
		d.runSecondaryWrite(func() { d.writeSecondary(cmd, iArgs) })
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

	if d.hasSecondaryWrite() {
		if replayCmd, replayArgs, ok := blockingReplayCommand(cmd, args, resp); ok {
			d.runSecondaryWrite(func() { d.writeSecondary(replayCmd, replayArgs) })
		}
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

// Script forwards EVAL/EVALSHA to the primary, and replays to secondary.
// Secondary script replays are concurrency-limited and apply caller
// backpressure rather than dropping when the script slot pool is saturated.
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
		d.runSecondaryScript(func() { d.writeSecondary(cmd, iArgs) })
	}

	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

// writeSecondary sends the command to the secondary, handling the NOSCRIPT
// → EVAL fallback and transparently retrying transient secondary errors. A
// re-sent command causes the backend to re-select a fresh timestamp and can
// also recover from hot-key OCC retry exhaustion in the secondary Redis adapter.
//
// The secondary's raw redis error is kept in sErr (not wrapped) so that
// writeSecondary can classify it via errors.Is(sErr, redis.Nil), attach the
// original message to Sentry and the structured log, and so the retry
// predicate can match the exact substring coming back from gRPC.
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
		if !isRetryableSecondaryWriteError(sErr) {
			break
		}
		if attempt >= maxSecondaryTransientRetries {
			break
		}
		reason := classifySecondaryWriteError(sErr)
		refreshSecondaryLeader(sCtx, d.secondary, sErr)
		d.logger.Debug("retrying secondary write after transient error",
			"cmd", cmd, "attempt", attempt+1, "backoff", backoff, "reason", reason, "err", sErr)
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

func (d *DualWriter) replaySecondaryPipeline(cmds [][]any) {
	d.runSecondaryWrite(func() {
		sCtx, cancel := context.WithTimeout(context.Background(), d.cfg.SecondaryTimeout)
		defer cancel()
		results, pErr := d.secondary.Pipeline(sCtx, cmds)
		if pErr != nil {
			d.logger.Warn("secondary txn replay failed", "err", pErr)
			d.metrics.SecondaryWriteErrors.Inc()
			d.metrics.SecondaryWriteErrorsByReason.WithLabelValues("PIPELINE", classifySecondaryWriteError(pErr)).Inc()
			return
		}
		if rErr := firstPipelineResultError(results); rErr != nil {
			d.logger.Warn("secondary txn replay failed", "err", rErr)
			d.metrics.SecondaryWriteErrors.Inc()
			d.metrics.SecondaryWriteErrorsByReason.WithLabelValues("PIPELINE", classifySecondaryWriteError(rErr)).Inc()
		}
	})
}

func firstPipelineResultError(results []*redis.Cmd) error {
	for _, result := range results {
		if result == nil {
			continue
		}
		err := result.Err()
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("pipeline result: %w", err)
		}
	}
	return nil
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
// It is best-effort: when the pool is saturated the work is dropped.
// Strict secondary writes should use runSecondaryWrite.
func (d *DualWriter) goWrite(fn func()) {
	d.goAsyncWithSem(d.writeSem, fn)
}

// goScript launches fn in a bounded Lua-script write goroutine.
// It is best-effort: when the pool is saturated the work is dropped.
// Strict secondary scripts should use runSecondaryScript.
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
		d.logAsyncDrop()
	}
}

// runSecondaryWrite launches a strict secondary write. It uses the async write
// pool while capacity is available, and otherwise blocks the caller until a
// slot is available. This preserves dual-write consistency while still bounding
// secondary backend concurrency.
func (d *DualWriter) runSecondaryWrite(fn func()) {
	d.runSecondaryWithBackpressure(d.writeSem, fn)
}

// runSecondaryScript is the strict variant of goScript for EVAL/EVALSHA replay.
func (d *DualWriter) runSecondaryScript(fn func()) {
	d.runSecondaryWithBackpressure(d.scriptSem, fn)
}

func (d *DualWriter) runSecondaryWithBackpressure(sem chan struct{}, fn func()) {
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
		return
	default:
		d.metrics.AsyncBackpressure.Inc()
		d.wg.Add(1)
		d.mu.Unlock()
	}

	defer d.wg.Done()
	sem <- struct{}{}
	defer func() { <-sem }()
	fn()
}

func (d *DualWriter) logAsyncDrop() {
	nowNano := time.Now().UnixNano()

	nextLog := atomic.LoadInt64(&d.nextAsyncDropLog)
	if nextLog != 0 && nowNano < nextLog {
		atomic.AddInt64(&d.suppressedAsyncDrops, 1)
		return
	}

	d.asyncDropMu.Lock()
	defer d.asyncDropMu.Unlock()

	nowNano = time.Now().UnixNano()
	nextLog = atomic.LoadInt64(&d.nextAsyncDropLog)
	if nextLog != 0 && nowNano < nextLog {
		atomic.AddInt64(&d.suppressedAsyncDrops, 1)
		return
	}
	suppressed := atomic.SwapInt64(&d.suppressedAsyncDrops, 0)
	atomic.StoreInt64(&d.nextAsyncDropLog, time.Now().Add(asyncDropLogInterval).UnixNano())

	if suppressed > 0 {
		d.logger.Warn("async goroutine limit reached, dropping secondary operation", "suppressed", suppressed)
		return
	}
	d.logger.Warn("async goroutine limit reached, dropping secondary operation")
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

// secondaryWriteErrorPatterns maps substrings found in secondary-write error
// messages to their Prometheus reason label. The list is scanned in order so
// more-specific patterns (e.g. "retry limit exceeded" which embeds "write
// conflict") must precede the generic ones to win the classification.
var secondaryWriteErrorPatterns = []struct {
	substr string
	reason string
}{
	{"retry limit exceeded", "retry_limit"},
	{"write conflict", "write_conflict"},
	{"deadline exceeded", "deadline_exceeded"},
	{"not leader", "not_leader"},
	{"leader not found", "not_leader"},
	{"txn already committed", "txn_already_finalized"},
	{"txn already aborted", "txn_already_finalized"},
	{"txn locked", "txn_locked"},
}

// classifySecondaryWriteError maps a secondary-write error to a small fixed set
// of reason labels suitable for a Prometheus counter. The elastickv secondary
// backend is in-house, so matching on substrings of the error message is safe.
//
// Order in secondaryWriteErrorPatterns matters (see its doc).
func classifySecondaryWriteError(err error) string {
	if err == nil {
		return "other"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	msg := err.Error()
	for _, p := range secondaryWriteErrorPatterns {
		if strings.Contains(msg, p.substr) {
			return p.reason
		}
	}
	return "other"
}
