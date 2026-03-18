package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// maxAsyncGoroutines limits concurrent fire-and-forget goroutines to prevent
// goroutine explosion when the secondary backend is slow or down.
const maxAsyncGoroutines = 4096

// DualWriter routes commands to primary and secondary backends based on mode.
type DualWriter struct {
	primary   Backend
	secondary Backend
	cfg       ProxyConfig
	shadow    *ShadowReader
	metrics   *ProxyMetrics
	sentry    *SentryReporter
	logger    *slog.Logger

	// asyncSem bounds the number of concurrent async goroutines
	// (secondary writes + shadow reads).
	asyncSem chan struct{}
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
		asyncSem:  make(chan struct{}, maxAsyncGoroutines),
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

// Write sends a write command to the primary synchronously, then to the secondary asynchronously.
func (d *DualWriter) Write(ctx context.Context, args [][]byte) (any, error) {
	cmd := strings.ToUpper(string(args[0]))
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
		d.goAsync(func() { d.writeSecondary(cmd, iArgs) })
	}

	if err != nil {
		return resp, fmt.Errorf("primary write %s: %w", cmd, err)
	}
	return resp, nil
}

// Read sends a read command to the primary and optionally performs a shadow read.
func (d *DualWriter) Read(ctx context.Context, args [][]byte) (any, error) {
	cmd := strings.ToUpper(string(args[0]))
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

	// Shadow read (bounded)
	if d.shadow != nil {
		shadowArgs := args
		shadowResp := resp
		shadowErr := err
		d.goAsync(func() {
			d.shadow.Compare(context.Background(), cmd, shadowArgs, shadowResp, shadowErr)
		})
	}

	if err != nil {
		return resp, fmt.Errorf("primary read %s: %w", cmd, err)
	}
	return resp, nil
}

// Blocking forwards a blocking command to the primary only.
// Optionally sends a short-timeout version to secondary for warmup.
func (d *DualWriter) Blocking(ctx context.Context, args [][]byte) (any, error) {
	cmd := strings.ToUpper(string(args[0]))
	iArgs := bytesArgsToInterfaces(args)

	start := time.Now()
	result := d.primary.Do(ctx, iArgs...)
	resp, err := result.Result()
	d.metrics.CommandDuration.WithLabelValues(cmd, d.primary.Name()).Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, redis.Nil) {
		d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "error").Inc()
		return nil, fmt.Errorf("primary blocking %s: %w", cmd, err)
	}
	d.metrics.CommandTotal.WithLabelValues(cmd, d.primary.Name(), "ok").Inc()

	// Warmup: send to secondary with short timeout (fire-and-forget, bounded)
	if d.hasSecondaryWrite() {
		d.goAsync(func() {
			sCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			d.secondary.Do(sCtx, iArgs...)
		})
	}

	if err != nil {
		return resp, fmt.Errorf("primary blocking %s: %w", cmd, err)
	}
	return resp, nil
}

// Admin forwards an admin command to the primary only.
func (d *DualWriter) Admin(ctx context.Context, args [][]byte) (any, error) {
	cmd := strings.ToUpper(string(args[0]))
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
	if err != nil {
		return resp, fmt.Errorf("primary admin %s: %w", cmd, err)
	}
	return resp, nil
}

// Script forwards EVAL/EVALSHA to the primary, and async replays to secondary.
func (d *DualWriter) Script(ctx context.Context, args [][]byte) (any, error) {
	cmd := strings.ToUpper(string(args[0]))
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
		d.goAsync(func() { d.writeSecondary(cmd, iArgs) })
	}

	if err != nil {
		return resp, fmt.Errorf("primary script %s: %w", cmd, err)
	}
	return resp, nil
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

// goAsync launches fn in a bounded goroutine. If the semaphore is full,
// the work is dropped and a warning is logged rather than blocking the caller.
func (d *DualWriter) goAsync(fn func()) {
	select {
	case d.asyncSem <- struct{}{}:
		go func() {
			defer func() { <-d.asyncSem }()
			fn()
		}()
	default:
		// Semaphore full — drop async work to protect the proxy.
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
