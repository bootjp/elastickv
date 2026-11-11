package main

import (
	"context"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/memwatch"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"golang.org/x/sync/errgroup"
)

// memoryPressureExit is set to true by the memwatch OnExceed callback to
// signal that the subsequent graceful shutdown was triggered by user-space
// OOM avoidance rather than an ordinary SIGTERM. The process exits with a
// distinct non-zero code (exitCodeMemoryPressure) so operators reading
// logs can distinguish this case from a crash or an ordinary stop.
var memoryPressureExit atomic.Bool

// memoryShutdownThresholdEnvVar configures the heap-inuse ceiling at
// which memwatch triggers a graceful shutdown. Empty or "0" disables the
// watchdog (the default; existing operators see no behaviour change).
const memoryShutdownThresholdEnvVar = "ELASTICKV_MEMORY_SHUTDOWN_THRESHOLD_MB"

// memoryShutdownPollIntervalEnvVar overrides memwatch's default poll
// cadence. Accepts any time.ParseDuration string. Invalid values log a
// warning and fall through to the default.
const memoryShutdownPollIntervalEnvVar = "ELASTICKV_MEMORY_SHUTDOWN_POLL_INTERVAL"

const (
	lockResolverEnabledEnvVar        = "ELASTICKV_LOCK_RESOLVER_ENABLED"
	fsmCompactorEnabledEnvVar        = "ELASTICKV_FSM_COMPACTOR_ENABLED"
	redisDeltaCompactorEnabledEnvVar = "ELASTICKV_REDIS_DELTA_COMPACTOR_ENABLED"
)

const bytesPerMiB = 1024 * 1024

// memwatchConfigFromEnv resolves the memwatch Config from environment
// variables. It returns (cfg, true) when the watcher should run, or
// (_, false) when the operator has not opted in (the default). Errors in
// the optional poll-interval override are logged and ignored so a typo
// cannot take the process down.
func memwatchConfigFromEnv() (memwatch.Config, bool) {
	raw := strings.TrimSpace(os.Getenv(memoryShutdownThresholdEnvVar))
	if raw == "" {
		return memwatch.Config{}, false
	}
	mb, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		slog.Warn("invalid "+memoryShutdownThresholdEnvVar+"; watcher disabled",
			"value", raw, "error", err)
		return memwatch.Config{}, false
	}
	if mb == 0 {
		return memwatch.Config{}, false
	}
	// Guard against mb * bytesPerMiB wrapping past math.MaxUint64. The
	// value has no real use above this ceiling (the host does not have
	// exabytes of RAM), and a wrapped value would set an absurdly low
	// threshold that fires immediately.
	if mb > math.MaxUint64/bytesPerMiB {
		slog.Warn("value for "+memoryShutdownThresholdEnvVar+" would overflow uint64; watcher disabled",
			"value_mb", mb)
		return memwatch.Config{}, false
	}

	cfg := memwatch.Config{
		ThresholdBytes: mb * bytesPerMiB,
	}
	cfg.PollInterval = memwatch.DefaultPollInterval
	if rawInterval := strings.TrimSpace(os.Getenv(memoryShutdownPollIntervalEnvVar)); rawInterval != "" {
		d, err := time.ParseDuration(rawInterval)
		if err != nil || d <= 0 {
			slog.Warn("invalid "+memoryShutdownPollIntervalEnvVar+"; using default",
				"value", rawInterval, "error", err)
		} else {
			cfg.PollInterval = d
		}
	}
	return cfg, true
}

func enabledEnv(name string) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid "+name+"; using default",
			"value", raw,
			"default", true,
		)
		return true
	}
	return enabled
}

func startLockResolverIfEnabled(shardStore *kv.ShardStore, shardGroups map[uint64]*kv.ShardGroup, cleanup *internalutil.CleanupStack, opts ...kv.LockResolverOption) {
	if enabledEnv(lockResolverEnabledEnvVar) {
		lockResolver := kv.NewLockResolver(shardStore, shardGroups, nil, opts...)
		cleanup.Add(func() { lockResolver.Close() })
		return
	}
	slog.Info("background lock resolver disabled", "env", lockResolverEnabledEnvVar)
}

func startFSMCompactorIfEnabled(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, readTracker *kv.ActiveTimestampTracker) {
	if enabledEnv(fsmCompactorEnabledEnvVar) {
		var admission kv.OperationAdmission
		if domain := bootstrapDomainForRuntimes(runtimes); domain != nil {
			admission = domain.operationAdmission
		}
		compactor := kv.NewFSMCompactor(
			fsmCompactionRuntimes(runtimes),
			kv.WithFSMCompactorActiveTimestampTracker(readTracker),
			kv.WithFSMCompactorOperationAdmission(admission),
		)
		eg.Go(func() error {
			return compactor.Run(ctx)
		})
		return
	}
	slog.Info("fsm compactor disabled", "env", fsmCompactorEnabledEnvVar)
}

func newRedisDeltaCompactorIfEnabled(shardStore *kv.ShardStore, coordinate kv.Coordinator, metricsRegistry *monitoring.Registry) *adapter.DeltaCompactor {
	if enabledEnv(redisDeltaCompactorEnabledEnvVar) {
		var opts []adapter.DeltaCompactorOption
		if obs := metricsRegistry.RedisTTLInlineMigrationObserver(); obs != nil {
			opts = append(opts, adapter.WithDeltaCompactorTTLInlineMigrationObserver(obs))
		}
		return adapter.NewDeltaCompactor(shardStore, coordinate, opts...)
	}
	slog.Info("redis delta compactor disabled", "env", redisDeltaCompactorEnabledEnvVar)
	return nil
}

// startMemoryWatchdog optionally starts the memwatch goroutine. The
// watcher is off by default; it is enabled only when the operator sets
// ELASTICKV_MEMORY_SHUTDOWN_THRESHOLD_MB. On threshold crossing the
// callback flips the memoryPressureExit sentinel and cancels the root
// context so errgroup unwinds, CleanupStack runs, and the WAL is synced.
// It skips the signal handler's leadership handoff to relieve pressure
// immediately. We do NOT send a signal, call os.Exit, or touch the raft
// engine directly here.
func startMemoryWatchdog(ctx context.Context, eg *errgroup.Group, cancel context.CancelFunc) {
	cfg, enabled := memwatchConfigFromEnv()
	if !enabled {
		return
	}
	cfg.OnExceed = func() {
		memoryPressureExit.Store(true)
		cancel()
	}
	w := memwatch.New(cfg)
	slog.Info("memory watchdog enabled",
		"threshold_bytes", cfg.ThresholdBytes,
		"poll_interval", cfg.PollInterval,
	)
	eg.Go(func() error {
		w.Start(ctx)
		return nil
	})
}

func fsmCompactionRuntimes(runtimes []*raftGroupRuntime) []kv.FSMCompactRuntime {
	out := make([]kv.FSMCompactRuntime, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.engine == nil || runtime.store == nil {
			continue
		}
		out = append(out, kv.FSMCompactRuntime{
			GroupID:      runtime.spec.id,
			StatusReader: runtime.engine,
			Store:        runtime.store,
		})
	}
	return out
}
