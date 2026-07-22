package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bootjp/elastickv/proxy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	sentryFlushTimeout          = 2 * time.Second
	metricsShutdownTimeout      = 5 * time.Second
	secondaryConcurrencyDivisor = 2
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := proxy.DefaultConfig()
	var modeStr string
	primaryPoolSize := proxy.DefaultBackendOptions().PoolSize
	elasticKVPoolSize := proxy.DefaultElasticKVBackendOptions().PoolSize
	secondaryWriteConcurrency := 0
	secondaryScriptConcurrency := 0
	secondaryWriteQueueSize := 0
	secondaryScriptQueueSize := 0

	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "Proxy listen address")
	flag.StringVar(&cfg.PrimaryAddr, "primary", cfg.PrimaryAddr, "Primary (Redis) address")
	flag.IntVar(&cfg.PrimaryDB, "primary-db", cfg.PrimaryDB, "Primary Redis DB number")
	flag.StringVar(&cfg.PrimaryPassword, "primary-password", cfg.PrimaryPassword, "Primary Redis password")
	flag.StringVar(&cfg.SecondaryAddr, "secondary", cfg.SecondaryAddr, "Secondary (ElasticKV) address. Comma-separated list of seeds is supported; the proxy discovers the current Raft leader via INFO replication.")
	flag.IntVar(&cfg.SecondaryDB, "secondary-db", cfg.SecondaryDB, "Secondary Redis DB number")
	flag.StringVar(&cfg.SecondaryPassword, "secondary-password", cfg.SecondaryPassword, "Secondary Redis password")
	flag.IntVar(&primaryPoolSize, "primary-pool-size", primaryPoolSize, "Primary Redis backend connection pool size")
	flag.IntVar(&elasticKVPoolSize, "elastickv-pool-size", elasticKVPoolSize, "ElasticKV backend connection pool size")
	flag.IntVar(&secondaryWriteConcurrency, "secondary-write-concurrency", secondaryWriteConcurrency, "Maximum concurrent asynchronous secondary writes including scripts (0 = half of secondary backend pool size)")
	flag.IntVar(&secondaryScriptConcurrency, "secondary-script-concurrency", secondaryScriptConcurrency, "Maximum concurrent asynchronous secondary Lua-script writes within the write limit (0 = half of secondary write concurrency)")
	flag.IntVar(&secondaryWriteQueueSize, "secondary-write-queue-size", secondaryWriteQueueSize, "Maximum queued asynchronous secondary writes (0 = derived from write concurrency)")
	flag.IntVar(&secondaryScriptQueueSize, "secondary-script-queue-size", secondaryScriptQueueSize, "Maximum queued asynchronous secondary Lua-script writes (0 = derived from script concurrency)")
	flag.StringVar(&modeStr, "mode", "dual-write", "Proxy mode: redis-only, dual-write, dual-write-shadow, elastickv-primary, elastickv-only")
	flag.DurationVar(&cfg.SecondaryTimeout, "secondary-timeout", cfg.SecondaryTimeout, "Secondary write timeout")
	flag.DurationVar(&cfg.ShadowTimeout, "shadow-timeout", cfg.ShadowTimeout, "Shadow read timeout")
	flag.StringVar(&cfg.SentryDSN, "sentry-dsn", cfg.SentryDSN, "Sentry DSN (empty = disabled)")
	flag.StringVar(&cfg.SentryEnv, "sentry-env", cfg.SentryEnv, "Sentry environment")
	flag.Float64Var(&cfg.SentrySampleRate, "sentry-sample", cfg.SentrySampleRate, "Sentry sample rate")
	flag.StringVar(&cfg.MetricsAddr, "metrics", cfg.MetricsAddr, "Prometheus metrics address")
	flag.Parse()

	mode, resolvedWriteConcurrency, resolvedScriptConcurrency, err := resolveRuntimeOptions(
		modeStr,
		primaryPoolSize,
		elasticKVPoolSize,
		secondaryWriteConcurrency,
		secondaryScriptConcurrency,
		secondaryWriteQueueSize,
		secondaryScriptQueueSize,
	)
	if err != nil {
		return err
	}
	cfg.Mode = mode
	cfg.SecondaryWriteConcurrency = resolvedWriteConcurrency
	cfg.SecondaryScriptConcurrency = resolvedScriptConcurrency
	cfg.SecondaryWriteQueueCapacity = secondaryWriteQueueSize
	cfg.SecondaryScriptQueueCapacity = secondaryScriptQueueSize

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Sentry
	sentryReporter := proxy.NewSentryReporter(cfg.SentryDSN, cfg.SentryEnv, cfg.SentrySampleRate, logger)
	defer sentryReporter.Flush(sentryFlushTimeout)

	// Prometheus
	reg := prometheus.NewRegistry()
	metrics := proxy.NewProxyMetrics(reg)

	// Backends
	primaryOpts := proxy.DefaultBackendOptions()
	primaryOpts.DB = cfg.PrimaryDB
	primaryOpts.Password = cfg.PrimaryPassword
	primaryOpts.PoolSize = primaryPoolSize
	secondaryOpts := proxy.DefaultElasticKVBackendOptions()
	secondaryOpts.DB = cfg.SecondaryDB
	secondaryOpts.Password = cfg.SecondaryPassword
	secondaryOpts.PoolSize = elasticKVPoolSize

	secondarySeeds := parseAddrList(cfg.SecondaryAddr)
	if len(secondarySeeds) == 0 {
		return fmt.Errorf("at least one secondary address is required")
	}

	var primary, secondary proxy.Backend
	switch cfg.Mode {
	case proxy.ModeElasticKVPrimary, proxy.ModeElasticKVOnly:
		primary = proxy.NewLeaderAwareRedisBackend(secondarySeeds, "elastickv", secondaryOpts, logger)
		secondary = proxy.NewRedisBackendWithOptions(cfg.PrimaryAddr, "redis", primaryOpts)
	case proxy.ModeRedisOnly, proxy.ModeDualWrite, proxy.ModeDualWriteShadow:
		primary = proxy.NewRedisBackendWithOptions(cfg.PrimaryAddr, "redis", primaryOpts)
		secondary = proxy.NewLeaderAwareRedisBackend(secondarySeeds, "elastickv", secondaryOpts, logger)
	}
	defer primary.Close()
	defer secondary.Close()

	dual := proxy.NewDualWriter(primary, secondary, cfg, metrics, sentryReporter, logger)
	defer dual.Close() // wait for in-flight async goroutines
	srv := proxy.NewProxyServer(cfg, dual, metrics, sentryReporter, logger)

	// Context for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", cfg.MetricsAddr)
		if err != nil {
			logger.Error("metrics listen failed", "addr", cfg.MetricsAddr, "err", err)
			return
		}
		metricsSrv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
			defer shutdownCancel()
			if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
				logger.Warn("metrics server shutdown error", "err", err)
			}
		}()
		logger.Info("metrics server starting", "addr", cfg.MetricsAddr)
		if err := metricsSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "err", err)
		}
	}()

	// Start proxy
	if err := srv.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("proxy server: %w", err)
	}
	return nil
}

func resolveRuntimeOptions(
	modeStr string,
	primaryPoolSize, elasticKVPoolSize int,
	secondaryWriteConcurrency, secondaryScriptConcurrency int,
	secondaryWriteQueueSize, secondaryScriptQueueSize int,
) (proxy.ProxyMode, int, int, error) {
	mode, err := parseRuntimeOptions(
		modeStr,
		primaryPoolSize,
		elasticKVPoolSize,
		secondaryWriteConcurrency,
		secondaryScriptConcurrency,
		secondaryWriteQueueSize,
		secondaryScriptQueueSize,
	)
	if err != nil {
		return 0, 0, 0, err
	}
	writeConcurrency, scriptConcurrency := deriveSecondaryConcurrency(
		mode,
		primaryPoolSize,
		elasticKVPoolSize,
		secondaryWriteConcurrency,
		secondaryScriptConcurrency,
	)
	if err := validateSecondaryConcurrency(
		mode,
		primaryPoolSize,
		elasticKVPoolSize,
		writeConcurrency,
		scriptConcurrency,
	); err != nil {
		return 0, 0, 0, err
	}
	return mode, writeConcurrency, scriptConcurrency, nil
}

func parseRuntimeOptions(
	modeStr string,
	primaryPoolSize, elasticKVPoolSize int,
	secondaryWriteConcurrency, secondaryScriptConcurrency int,
	secondaryWriteQueueSize, secondaryScriptQueueSize int,
) (proxy.ProxyMode, error) {
	mode, ok := proxy.ParseProxyMode(modeStr)
	if !ok {
		return 0, fmt.Errorf("unknown mode: %s", modeStr)
	}
	if primaryPoolSize <= 0 {
		return 0, fmt.Errorf("primary-pool-size must be positive: %d", primaryPoolSize)
	}
	if elasticKVPoolSize <= 0 {
		return 0, fmt.Errorf("elastickv-pool-size must be positive: %d", elasticKVPoolSize)
	}
	if secondaryWriteConcurrency < 0 {
		return 0, fmt.Errorf("secondary-write-concurrency must be non-negative: %d", secondaryWriteConcurrency)
	}
	if secondaryScriptConcurrency < 0 {
		return 0, fmt.Errorf("secondary-script-concurrency must be non-negative: %d", secondaryScriptConcurrency)
	}
	if secondaryWriteQueueSize < 0 {
		return 0, fmt.Errorf("secondary-write-queue-size must be non-negative: %d", secondaryWriteQueueSize)
	}
	if secondaryScriptQueueSize < 0 {
		return 0, fmt.Errorf("secondary-script-queue-size must be non-negative: %d", secondaryScriptQueueSize)
	}
	return mode, nil
}

func validateSecondaryConcurrency(mode proxy.ProxyMode, primaryPoolSize, elasticKVPoolSize, writeConcurrency, scriptConcurrency int) error {
	if mode == proxy.ModeRedisOnly || mode == proxy.ModeElasticKVOnly {
		return nil
	}
	poolSize := secondaryBackendPoolSize(mode, primaryPoolSize, elasticKVPoolSize)
	if writeConcurrency > poolSize {
		return fmt.Errorf("secondary-write-concurrency %d exceeds secondary backend pool size %d", writeConcurrency, poolSize)
	}
	if scriptConcurrency > writeConcurrency {
		return fmt.Errorf("secondary-script-concurrency %d exceeds secondary-write-concurrency %d", scriptConcurrency, writeConcurrency)
	}
	return nil
}

func deriveSecondaryConcurrency(mode proxy.ProxyMode, primaryPoolSize, elasticKVPoolSize, writeConcurrency, scriptConcurrency int) (int, int) {
	if writeConcurrency == 0 {
		writeConcurrency = defaultSecondaryWriteConcurrency(secondaryBackendPoolSize(mode, primaryPoolSize, elasticKVPoolSize))
	}
	if scriptConcurrency == 0 {
		scriptConcurrency = defaultSecondaryScriptConcurrency(writeConcurrency)
	}
	return writeConcurrency, scriptConcurrency
}

func secondaryBackendPoolSize(mode proxy.ProxyMode, primaryPoolSize, elasticKVPoolSize int) int {
	if mode == proxy.ModeElasticKVPrimary {
		return primaryPoolSize
	}
	return elasticKVPoolSize
}

func defaultSecondaryWriteConcurrency(poolSize int) int {
	return atLeastOne(poolSize / secondaryConcurrencyDivisor)
}

func defaultSecondaryScriptConcurrency(writeConcurrency int) int {
	return atLeastOne(writeConcurrency / secondaryConcurrencyDivisor)
}

func atLeastOne(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func parseAddrList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
