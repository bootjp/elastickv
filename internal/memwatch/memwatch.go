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
// The watcher samples runtime/metrics at a fixed cadence. When the live
// heap-in-use byte count crosses the configured threshold it invokes
// OnExceed once and exits. The watcher never calls os.Exit or sends
// signals itself; callers wire OnExceed to the existing shutdown path
// (typically a root context.CancelFunc).
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
// We sample `runtime/metrics` (Go 1.16+) rather than `runtime.ReadMemStats`.
// ReadMemStats triggers a stop-the-world pause proportional to the number of
// goroutines and heap size; at 1 s cadence that's typically negligible, but
// at a tighter MinPollInterval (10 ms) it begins to register. runtime/metrics
// readers are lock-free for the counters we need and do not stop the world.
//
// The threshold is compared against
//
//	/memory/classes/heap/objects:bytes + /memory/classes/heap/unused:bytes
//
// which is the runtime/metrics equivalent of MemStats.HeapInuse: bytes held
// in heap spans that are currently allocated from the OS, including span
// overhead, but excluding pages the runtime has released back. RSS from
// /proc/self/status is more accurate but requires a read syscall on every
// poll and is not what the Go allocator itself tracks. We deliberately do
// NOT compare against "total heap classes" (which includes released memory
// already returned to the OS) or "heap/objects" alone (which misses span
// fragmentation that the OOM-killer sees).
package memwatch

import (
	"context"
	"log/slog"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultPollInterval is the polling cadence used when Config.PollInterval
// is zero. One second is frequent enough to catch fast-growing memtables
// before the kernel kills the process, and infrequent enough that even
// aggressive log rollups don't observe the watcher as a hot sampler.
const DefaultPollInterval = time.Second

// MinPollInterval is the floor enforced by New. runtime/metrics reads are
// cheap but a sub-10ms cadence produces no detection benefit over 10ms
// (memory pressure does not move that fast on these VMs) and would churn
// the ticker for no gain.
const MinPollInterval = 10 * time.Millisecond

// Config configures a Watcher.
type Config struct {
	// ThresholdBytes is the heap-in-use threshold in bytes. When the
	// sampled heap-in-use crosses this value the watcher invokes OnExceed
	// exactly once and returns. A zero value disables the watcher entirely
	// (Start returns immediately).
	ThresholdBytes uint64

	// PollInterval is how often the metrics are sampled. Defaults to
	// DefaultPollInterval when zero; values below MinPollInterval are
	// clamped up to MinPollInterval.
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

// Metric sample indices — kept stable so samples[] can be reused across
// polls without reallocating or re-resolving names.
const (
	sampleHeapObjects = iota
	sampleHeapUnused
	sampleHeapReleased
	sampleGCGoal
	sampleCount
)

var metricNames = [sampleCount]string{
	sampleHeapObjects:  "/memory/classes/heap/objects:bytes",
	sampleHeapUnused:   "/memory/classes/heap/unused:bytes",
	sampleHeapReleased: "/memory/classes/heap/released:bytes",
	sampleGCGoal:       "/gc/heap/goal:bytes",
}

// Watcher polls process memory and fires OnExceed once, when heap-in-use
// crosses the configured threshold. Callers get a single-shot notification
// and are expected to initiate graceful shutdown; Watcher does not call
// os.Exit or send signals itself.
type Watcher struct {
	cfg       Config
	fired     atomic.Bool
	started   atomic.Bool
	doneCh    chan struct{}
	closeOnce sync.Once
	// samples is reused across polls; metric-name resolution happens once
	// in New so the hot path only walks a fixed []Sample.
	samples []metrics.Sample
}

// New constructs a Watcher from the given Config. The Watcher does not
// start polling until Start is called.
func New(cfg Config) *Watcher {
	switch {
	case cfg.PollInterval <= 0:
		cfg.PollInterval = DefaultPollInterval
	case cfg.PollInterval < MinPollInterval:
		cfg.PollInterval = MinPollInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	samples := make([]metrics.Sample, sampleCount)
	for i, name := range metricNames {
		samples[i].Name = name
	}
	return &Watcher{
		cfg:     cfg,
		doneCh:  make(chan struct{}),
		samples: samples,
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

	// Sample once before the first tick: if the process is already above
	// the threshold at Start (crashloop-restart after OOM, large startup
	// allocations, etc.), waiting for the first ticker cycle can let the
	// kernel OOM-kill the process we were supposed to protect.
	if w.checkAndMaybeFire() {
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

// sampleUint64 reads a named Uint64 sample from w.samples after metrics.Read
// has populated them. Returns 0 if the metric is not supported by the
// current Go runtime (runtime/metrics guarantees no panic, just
// KindBad). The watcher treats missing metrics as "no pressure detected";
// the primary metrics used by the threshold check have been present since
// Go 1.16, so this only matters for defensive correctness.
func (w *Watcher) sampleUint64(idx int) uint64 {
	if w.samples[idx].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return w.samples[idx].Value.Uint64()
}

// checkAndMaybeFire samples runtime/metrics once, computes heap-in-use, and
// if it is at or above the threshold and OnExceed has not already fired,
// invokes OnExceed and returns true to signal the loop to exit.
func (w *Watcher) checkAndMaybeFire() bool {
	metrics.Read(w.samples)

	objects := w.sampleUint64(sampleHeapObjects)
	unused := w.sampleUint64(sampleHeapUnused)
	// heap-in-use = allocated heap spans (live objects plus reusable free
	// slots the runtime still owns), matching MemStats.HeapInuse.
	heapInuse := objects + unused

	if heapInuse < w.cfg.ThresholdBytes {
		return false
	}

	// CompareAndSwap so a (hypothetical) concurrent caller cannot cause
	// OnExceed to run twice. The watcher currently runs from one goroutine
	// but keeping the guard explicit documents the "single-shot" contract.
	if !w.fired.CompareAndSwap(false, true) {
		return true
	}

	released := w.sampleUint64(sampleHeapReleased)
	gcGoal := w.sampleUint64(sampleGCGoal)

	w.cfg.Logger.Warn("memory pressure shutdown",
		"heap_inuse_bytes", heapInuse,
		"heap_objects_bytes", objects,
		"heap_released_bytes", released,
		"threshold_bytes", w.cfg.ThresholdBytes,
		"next_gc_bytes", gcGoal,
	)

	if w.cfg.OnExceed != nil {
		w.cfg.OnExceed()
	}
	return true
}
