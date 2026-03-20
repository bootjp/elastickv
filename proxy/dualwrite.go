package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
)

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

	wg     sync.WaitGroup
	mu     sync.Mutex // protects closed; held briefly to make wg.Add atomic with close check
	closed bool
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

	if d.hasSecondaryWrite() {
		d.goWrite(func() { d.writeSecondary(cmd, iArgs) })
	}

	return resp, err //nolint:wrapcheck // redis.Nil must pass through unwrapped for callers to detect nil replies
}

func (d *DualWriter) writeSecondary(cmd string, iArgs []any) {
	sCtx, cancel := context.WithTimeout(context.Background(), d.cfg.SecondaryTimeout)
	defer cancel()

	start := time.Now()
	result := d.secondary.Do(sCtx, iArgs...)
	_, sErr := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.secondary.Name()).Observe(time.Since(start).Seconds())

	if sErr != nil && !errors.Is(sErr, redis.Nil) {
		d.metrics.SecondaryWriteErrors.Inc()
		d.metrics.CommandTotal.WithLabelValues(cmd, d.secondary.Name(), "error").Inc()
		fingerprint := fmt.Sprintf("secondary_write_%s", cmd)
		if d.sentry.ShouldReport(fingerprint) {
			d.sentry.CaptureException(sErr, "secondary_write_failure", argsToBytes(iArgs))
		}
		d.logger.Warn("secondary write failed", "cmd", cmd, "err", sErr)
		return
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.secondary.Name(), "ok").Inc()
}

// goWrite launches fn in a bounded write goroutine.
func (d *DualWriter) goWrite(fn func()) {
	d.goAsyncWithSem(d.writeSem, fn)
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
