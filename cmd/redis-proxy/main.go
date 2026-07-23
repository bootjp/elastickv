package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
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
	elasticKVDispatchTimeout    = 10 * time.Second
	backendTimeoutGrace         = time.Second
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
	secondaryBlockingReplayConcurrency := 0
	secondaryWriteQueueSize := 0
	secondaryScriptQueueSize := 0
	secondaryBlockingReplayQueueSize := 0

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
	flag.IntVar(&secondaryBlockingReplayConcurrency, "secondary-blocking-replay-concurrency", secondaryBlockingReplayConcurrency, "Maximum concurrent asynchronous secondary mutating blocking-command replays (0 = capped remaining secondary backend pool capacity after writes)")
	flag.IntVar(&secondaryWriteQueueSize, "secondary-write-queue-size", secondaryWriteQueueSize, "Maximum queued asynchronous secondary writes (0 = derived from write concurrency)")
	flag.IntVar(&secondaryScriptQueueSize, "secondary-script-queue-size", secondaryScriptQueueSize, "Maximum queued asynchronous secondary Lua-script writes (0 = derived from script concurrency)")
	flag.IntVar(&secondaryBlockingReplayQueueSize, "secondary-blocking-replay-queue-size", secondaryBlockingReplayQueueSize, "Maximum queued asynchronous secondary mutating blocking-command replays (0 = derived from blocking replay concurrency)")
	flag.StringVar(&modeStr, "mode", "dual-write", "Proxy mode: redis-only, dual-write, dual-write-shadow, elastickv-primary, elastickv-only")
	flag.DurationVar(&cfg.SecondaryTimeout, "secondary-timeout", cfg.SecondaryTimeout, "Secondary write timeout")
	flag.DurationVar(&cfg.ShadowTimeout, "shadow-timeout", cfg.ShadowTimeout, "Shadow read timeout")
	flag.StringVar(&cfg.SentryDSN, "sentry-dsn", cfg.SentryDSN, "Sentry DSN (empty = disabled)")
	flag.StringVar(&cfg.SentryEnv, "sentry-env", cfg.SentryEnv, "Sentry environment")
	flag.Float64Var(&cfg.SentrySampleRate, "sentry-sample", cfg.SentrySampleRate, "Sentry sample rate")
	flag.StringVar(&cfg.MetricsAddr, "metrics", cfg.MetricsAddr, "Prometheus metrics address")
	flag.StringVar(&cfg.PProfAddr, "pprof", cfg.PProfAddr, "pprof listen address (empty = disabled)")
	flag.BoolVar(&cfg.RedisOnlyRaw, "redis-only-raw", cfg.RedisOnlyRaw, "Use raw TCP bridging in redis-only mode")
	flag.Parse()

	mode, resolvedWriteConcurrency, resolvedScriptConcurrency, resolvedBlockingReplayConcurrency, err := resolveRuntimeOptions(
		modeStr,
		primaryPoolSize,
		elasticKVPoolSize,
		secondaryWriteConcurrency,
		secondaryScriptConcurrency,
		secondaryBlockingReplayConcurrency,
		secondaryWriteQueueSize,
		secondaryScriptQueueSize,
		secondaryBlockingReplayQueueSize,
	)
	if err != nil {
		return err
	}
	cfg.Mode = mode
	cfg.SecondaryWriteConcurrency = resolvedWriteConcurrency
	cfg.SecondaryScriptConcurrency = resolvedScriptConcurrency
	cfg.SecondaryBlockingReplayConcurrency = resolvedBlockingReplayConcurrency
	cfg.SecondaryWriteQueueCapacity = secondaryWriteQueueSize
	cfg.SecondaryScriptQueueCapacity = secondaryScriptQueueSize
	cfg.SecondaryBlockingReplayQueueCapacity = secondaryBlockingReplayQueueSize

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Sentry
	sentryReporter := proxy.NewSentryReporter(cfg.SentryDSN, cfg.SentryEnv, cfg.SentrySampleRate, logger)
	defer sentryReporter.Flush(sentryFlushTimeout)

	reg := prometheus.NewRegistry()
	metrics := proxy.NewProxyMetrics(reg)

	primary, secondary, err := newBackends(cfg, primaryPoolSize, elasticKVPoolSize, logger)
	if err != nil {
		return err
	}
	defer primary.Close()
	defer secondary.Close()

	dual := proxy.NewDualWriter(primary, secondary, cfg, metrics, sentryReporter, logger)
	defer dual.Close() // wait for in-flight async goroutines
	srv := proxy.NewProxyServer(cfg, dual, metrics, sentryReporter, logger)

	// Context for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go serveMetrics(ctx, cfg.MetricsAddr, reg, logger)

	if cfg.PProfAddr != "" {
		go servePProf(ctx, cfg.PProfAddr, logger)
	}

	// Start proxy
	if err := srv.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("proxy server: %w", err)
	}
	return nil
}

func newBackends(cfg proxy.ProxyConfig, primaryPoolSize, elasticKVPoolSize int, logger *slog.Logger) (proxy.Backend, proxy.Backend, error) {
	primaryOpts := proxy.DefaultBackendOptions()
	primaryOpts.DB = cfg.PrimaryDB
	primaryOpts.Password = cfg.PrimaryPassword
	primaryOpts.PoolSize = primaryPoolSize
	secondaryOpts := proxy.DefaultElasticKVBackendOptions()
	secondaryOpts.DB = cfg.SecondaryDB
	secondaryOpts.Password = cfg.SecondaryPassword
	secondaryOpts.PoolSize = elasticKVPoolSize
	alignElasticKVBackendTimeouts(&secondaryOpts, cfg.SecondaryTimeout)

	secondarySeeds := parseAddrList(cfg.SecondaryAddr)

	switch cfg.Mode {
	case proxy.ModeElasticKVPrimary:
		if len(secondarySeeds) == 0 {
			return nil, nil, fmt.Errorf("at least one secondary address is required")
		}
		return proxy.NewLeaderAwareRedisBackend(secondarySeeds, "elastickv", secondaryOpts, logger),
			proxy.NewRedisBackendWithOptions(cfg.PrimaryAddr, "redis", primaryOpts), nil
	case proxy.ModeElasticKVOnly:
		if len(secondarySeeds) == 0 {
			return nil, nil, fmt.Errorf("at least one secondary address is required")
		}
		return proxy.NewLeaderAwareRedisBackend(secondarySeeds, "elastickv", secondaryOpts, logger),
			proxy.NewNoopBackend("redis"), nil
	case proxy.ModeRedisOnly:
		return proxy.NewRedisBackendWithOptions(cfg.PrimaryAddr, "redis", primaryOpts),
			proxy.NewNoopBackend("elastickv"), nil
	case proxy.ModeDualWrite, proxy.ModeDualWriteShadow:
		if len(secondarySeeds) == 0 {
			return nil, nil, fmt.Errorf("at least one secondary address is required")
		}
		return proxy.NewRedisBackendWithOptions(cfg.PrimaryAddr, "redis", primaryOpts),
			proxy.NewLeaderAwareRedisBackend(secondarySeeds, "elastickv", secondaryOpts, logger), nil
	default:
		return nil, nil, fmt.Errorf("unsupported mode: %s", cfg.Mode.String())
	}
}

func alignElasticKVBackendTimeouts(opts *proxy.BackendOptions, operationTimeout time.Duration) {
	if opts == nil {
		return
	}
	floor := elasticKVDispatchTimeout
	if operationTimeout > floor {
		floor = operationTimeout
	}
	floor += backendTimeoutGrace
	if opts.ReadTimeout > 0 && opts.ReadTimeout < floor {
		opts.ReadTimeout = floor
	}
	if opts.WriteTimeout > 0 && opts.WriteTimeout < floor {
		opts.WriteTimeout = floor
	}
}

func serveMetrics(ctx context.Context, addr string, reg *prometheus.Registry, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	serveHTTP(ctx, addr, mux, "metrics", logger)
}

func servePProf(ctx context.Context, addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	serveHTTP(ctx, addr, mux, "pprof", logger)
}

func serveHTTP(ctx context.Context, addr string, handler http.Handler, name string, logger *slog.Logger) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		logger.Error(name+" listen failed", "addr", addr, "err", err)
		return
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn(name+" server shutdown error", "err", err)
		}
	}()
	logger.Info(name+" server starting", "addr", addr)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		logger.Error(name+" server error", "err", err)
	}
}

func resolveRuntimeOptions(
	modeStr string,
	primaryPoolSize, elasticKVPoolSize int,
	secondaryWriteConcurrency, secondaryScriptConcurrency, secondaryBlockingReplayConcurrency int,
	secondaryWriteQueueSize, secondaryScriptQueueSize, secondaryBlockingReplayQueueSize int,
) (proxy.ProxyMode, int, int, int, error) {
	mode, err := parseRuntimeOptions(
		modeStr,
		primaryPoolSize,
		elasticKVPoolSize,
		secondaryWriteConcurrency,
		secondaryScriptConcurrency,
		secondaryBlockingReplayConcurrency,
		secondaryWriteQueueSize,
		secondaryScriptQueueSize,
		secondaryBlockingReplayQueueSize,
	)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	writeConcurrency, scriptConcurrency, blockingReplayConcurrency := deriveSecondaryConcurrency(
		mode,
		primaryPoolSize,
		elasticKVPoolSize,
		secondaryWriteConcurrency,
		secondaryScriptConcurrency,
		secondaryBlockingReplayConcurrency,
	)
	if err := validateSecondaryConcurrency(
		mode,
		primaryPoolSize,
		elasticKVPoolSize,
		writeConcurrency,
		scriptConcurrency,
		blockingReplayConcurrency,
	); err != nil {
		return 0, 0, 0, 0, err
	}
	return mode, writeConcurrency, scriptConcurrency, blockingReplayConcurrency, nil
}

func parseRuntimeOptions(
	modeStr string,
	primaryPoolSize, elasticKVPoolSize int,
	secondaryWriteConcurrency, secondaryScriptConcurrency, secondaryBlockingReplayConcurrency int,
	secondaryWriteQueueSize, secondaryScriptQueueSize, secondaryBlockingReplayQueueSize int,
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
	if secondaryBlockingReplayConcurrency < 0 {
		return 0, fmt.Errorf("secondary-blocking-replay-concurrency must be non-negative: %d", secondaryBlockingReplayConcurrency)
	}
	if secondaryWriteQueueSize < 0 {
		return 0, fmt.Errorf("secondary-write-queue-size must be non-negative: %d", secondaryWriteQueueSize)
	}
	if secondaryScriptQueueSize < 0 {
		return 0, fmt.Errorf("secondary-script-queue-size must be non-negative: %d", secondaryScriptQueueSize)
	}
	if secondaryBlockingReplayQueueSize < 0 {
		return 0, fmt.Errorf("secondary-blocking-replay-queue-size must be non-negative: %d", secondaryBlockingReplayQueueSize)
	}
	return mode, nil
}

func validateSecondaryConcurrency(mode proxy.ProxyMode, primaryPoolSize, elasticKVPoolSize, writeConcurrency, scriptConcurrency, blockingReplayConcurrency int) error {
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
	if writeConcurrency+blockingReplayConcurrency > poolSize {
		return fmt.Errorf("secondary-write-concurrency %d plus secondary-blocking-replay-concurrency %d exceeds secondary backend pool size %d", writeConcurrency, blockingReplayConcurrency, poolSize)
	}
	return nil
}

func deriveSecondaryConcurrency(mode proxy.ProxyMode, primaryPoolSize, elasticKVPoolSize, writeConcurrency, scriptConcurrency, blockingReplayConcurrency int) (int, int, int) {
	poolSize := secondaryBackendPoolSize(mode, primaryPoolSize, elasticKVPoolSize)
	if writeConcurrency == 0 {
		writeConcurrency = defaultSecondaryWriteConcurrency(poolSize)
	}
	if scriptConcurrency == 0 {
		scriptConcurrency = defaultSecondaryScriptConcurrency(writeConcurrency)
	}
	if blockingReplayConcurrency == 0 {
		blockingReplayConcurrency = defaultSecondaryBlockingReplayConcurrency(poolSize, writeConcurrency)
	}
	return writeConcurrency, scriptConcurrency, blockingReplayConcurrency
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

func defaultSecondaryBlockingReplayConcurrency(poolSize, writeConcurrency int) int {
	remaining := poolSize - writeConcurrency
	if remaining <= 0 {
		return 0
	}
	if remaining > proxy.DefaultConfig().SecondaryBlockingReplayConcurrency {
		return proxy.DefaultConfig().SecondaryBlockingReplayConcurrency
	}
	return remaining
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
