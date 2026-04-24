// Package memwatch provides a user-space memory watchdog that triggers
// an orderly shutdown before the kernel OOM-killer sends SIGKILL.
//
// # Motivation
//
// elastickv runs in memory-constrained containers (e.g. 3GB RAM VMs). Go's
// runtime is unaware of the container/host memory limit, and even with
// GOMEMLIMIT set the process can still lose the race against the kernel
// OOM-killer under sustained memtable/goroutine growth. A SIGKILL leaves
// the Raft WAL potentially truncated mid-operation; a cooperative SIGTERM
// path lets the node sync the WAL and stop raft cleanly, avoiding the
// election storms and lease loss that follow crash-restarts.
//
// The watcher polls runtime.ReadMemStats at a fixed cadence. When
// HeapInuse crosses the configured threshold it invokes OnExceed once
// and exits. The watcher never calls os.Exit or sends signals itself;
// callers wire OnExceed to the existing shutdown path (typically a
// root context.CancelFunc).
//
// Wiring in elastickv (see main.go):
//
//	ctx, cancel := context.WithCancel(context.Background())
//	// ... build runtimes, servers, errgroup ...
//	w := memwatch.New(memwatch.Config{
//	    ThresholdBytes: threshold,
//	    PollInterval:   pollInterval,
//	    OnExceed: func() {
//	        memoryPressureExit.Store(true) // flips exit code to 2
//	        cancel()                       // fires the same shutdown path
//	    }, //                                  SIGTERM would use.
//	})
//	eg.Go(func() error { w.Start(runCtx); return nil })
//
// # Metric choice
//
// We read runtime.MemStats.HeapInuse. It is the closest Go-runtime-visible
// proxy for "how close are we to OOM" without a syscall per poll. RSS from
// /proc/self/status is more accurate but requires a read syscall on every
// poll; at the 1s cadence this watchdog runs that accuracy isn't worth the
// cost. We deliberately do NOT use MemStats.Sys, NumGC or Alloc: Sys and
// NumGC include memory the runtime has already released back to the OS (or
// are monotonic counters) and Alloc counts only currently-live heap objects,
// missing the span-level overhead that the OOM-killer actually sees.
package memwatch

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultPollInterval is the polling cadence used when Config.PollInterval
// is zero. One second is frequent enough to catch fast-growing memtables
// before the kernel kills the process, but infrequent enough that
// runtime.ReadMemStats (which stops the world briefly) doesn't become a
// meaningful source of latency on its own.
const DefaultPollInterval = time.Second

// Config configures a Watcher.
type Config struct {
	// ThresholdBytes is the HeapInuse threshold in bytes. When
	// runtime.MemStats.HeapInuse exceeds this value the watcher invokes
	// OnExceed exactly once and returns. A zero value disables the
	// watcher entirely (Start returns immediately).
	ThresholdBytes uint64

	// PollInterval is how often ReadMemStats is called. Defaults to
	// DefaultPollInterval when zero.
	PollInterval time.Duration

	// OnExceed is called at most once, from the watcher's own goroutine,
	// when the threshold is crossed. It must be non-blocking or at least
	// must not block the caller indefinitely (the watcher returns
	// immediately after invocation regardless). Typical implementations
	// cancel a root context and flag a process-wide exit-code sentinel.
	OnExceed func()

	// Logger, if non-nil, receives a single structured log line when the
	// threshold is crossed. When nil, slog.Default() is used.
	Logger *slog.Logger
}

// Watcher polls process memory and fires OnExceed once, when HeapInuse
// crosses the configured threshold. Callers get a single-shot notification
// and are expected to initiate graceful shutdown; Watcher does not call
// os.Exit or send signals itself.
type Watcher struct {
	cfg       Config
	fired     atomic.Bool
	started   atomic.Bool
	doneCh    chan struct{}
	closeOnce sync.Once
}

// New constructs a Watcher from the given Config. The Watcher does not
// start polling until Start is called.
func New(cfg Config) *Watcher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Watcher{
		cfg:    cfg,
		doneCh: make(chan struct{}),
	}
}

// Start runs the watchdog loop. It returns when ctx is cancelled, when
// OnExceed has fired, or immediately when ThresholdBytes is zero (the
// watcher is disabled). It is safe to call Start at most once per Watcher;
// subsequent calls return immediately because the done channel has already
// been closed.
func (w *Watcher) Start(ctx context.Context) {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	defer w.closeDoneOnce()

	if w.cfg.ThresholdBytes == 0 {
		// Disabled: do not even start a ticker, so an OFF-by-default
		// deployment pays zero cost.
		return
	}

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.checkAndMaybeFire() {
				return
			}
		}
	}
}

// Done returns a channel that is closed when Start returns. Tests can use
// it to assert the watcher goroutine actually exits (no leak) after
// ctx cancel or OnExceed.
func (w *Watcher) Done() <-chan struct{} {
	return w.doneCh
}

// closeDoneOnce closes doneCh at most once across the Watcher's lifetime.
// Per-Watcher sync.Once avoids the contention a shared package-level mutex
// would introduce if multiple watchers coexisted.
func (w *Watcher) closeDoneOnce() {
	w.closeOnce.Do(func() { close(w.doneCh) })
}

// checkAndMaybeFire reads MemStats once, and if HeapInuse is at or above
// the threshold and OnExceed has not already fired, invokes OnExceed and
// returns true to signal the loop to exit.
func (w *Watcher) checkAndMaybeFire() bool {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	if ms.HeapInuse < w.cfg.ThresholdBytes {
		return false
	}

	// CompareAndSwap so a (hypothetical) concurrent caller cannot cause
	// OnExceed to run twice. The watcher currently runs from one goroutine
	// but keeping the guard explicit documents the "single-shot" contract.
	if !w.fired.CompareAndSwap(false, true) {
		return true
	}

	w.cfg.Logger.Warn("memory pressure shutdown",
		"heap_inuse_bytes", ms.HeapInuse,
		"threshold_bytes", w.cfg.ThresholdBytes,
		"heap_alloc_bytes", ms.HeapAlloc,
		"heap_sys_bytes", ms.HeapSys,
		"next_gc_bytes", ms.NextGC,
	)

	if w.cfg.OnExceed != nil {
		w.cfg.OnExceed()
	}
	return true
}
