package keyviz

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Per-cell hot-key drill-down (design 2026_05_28_proposed_keyviz_hot_key_topk).
// Off by default; opt in via MemSamplerOptions.HotKeysEnabled. Disabled-case
// overhead is one early-return branch on Observe; enabled-case overhead is a
// length check, the splitmix64 `nextSampleRoll() % R == 0` sample gate (see
// nextSampleRoll for why we avoid math/rand/v2 entirely), a pool clone, and
// a non-blocking channel send.

// hotKeyEvent ferries one sampled observation from the hot path to the
// aggregator. key is the pool's `*[]byte` (NOT the slice itself) so the
// aggregator can return it to the pool after copying the bytes into the
// Space-Saving sketch's own storage. The buffer is strictly transient
// between the hot path and the aggregator (design §4 pool-vs-snapshot
// ownership) — never aliased by an SS entry or a published snapshot.
type hotKeyEvent struct {
	slot slotKey
	key  *[]byte
}

// KeyvizHotKeysSnapshot is the immutable per-route view a drill-down
// handler reads. The aggregator deep-copies every key bytes before
// publishing, so a reader can hold it across resets / evictions on the
// live sketch without races (design §4 published-snapshot ownership).
//
// Exported so the admin handler can read it; the type lives in the
// keyviz package because the aggregator owns and publishes it.
type KeyvizHotKeysSnapshot struct {
	RouteID         uint64
	Label           Label
	SampledN        uint64    // events the sketch actually saw this window
	DroppedSamples  uint64    // node-global: bounded-queue back-pressure drops
	SkippedLongKeys uint64    // node-global: pre-sample length-cap rejects
	SnapshotAt      time.Time // when the aggregator deep-copied this snapshot
	SampleRate      int       // R at the time of snapshot
	Capacity        int       // m at the time of snapshot
	Entries         []KeyvizHotKeyEntry
}

// KeyvizHotKeyEntry is one tracked (key, count) pair in a snapshot.
// Count is the sketch's raw count (sampled-stream observations); the
// admin layer scales it by SampleRate to estimate true frequency and
// computes error_bound = SampleRate * SampledN / Capacity.
type KeyvizHotKeyEntry struct {
	Key   []byte
	Count uint64
}

// hotKeysAggregator runs as a single goroutine (single-writer to every
// per-route sketch). The hot path is the only writer to ch, the
// node-global drop/skip counters, and the per-route sampledN counter.
// Sketches and snapshots are owned exclusively by this goroutine.
type hotKeysAggregator struct {
	s          *MemSampler // back-reference, for table.Load() at publish time
	ch         chan hotKeyEvent
	keyPool    *sync.Pool // []byte buffers for hot-path key clones
	capacity   int        // m
	sampleRate int        // R (used by Observe)
	maxKeyLen  int        // hotKeysMaxKeyLen
	step       time.Duration
	clock      func() time.Time
	// Node-global hot-path counters (atomic.Uint64 incremented by Observe,
	// read once per publish by the aggregator).
	dropped atomic.Uint64
	skipped atomic.Uint64
	// Per-route/label sampled-N counters (incremented by the aggregator only,
	// reset on publish). Map mutation is guarded by the fact that the
	// aggregator is the single writer.
	perSlotN map[slotKey]*uint64
	// Per-route/label Space-Saving sketches (single-writer-by-aggregator).
	sketches map[slotKey]*spaceSaving
	// rngState backs the sample gate. We avoid math/rand/v2 entirely
	// (gosec G404 flags the package as a weak crypto generator — but
	// it's also the only built-in PRNG, and we'd otherwise need a
	// //nolint). atomic.Uint64.Add gives a lock-free monotonic
	// sequence; splitmix64 mixes it into a uniformly-distributed
	// uint64, breaking phase-lock against periodic / ordered workloads
	// (Codex P2 round-3 L135) without owning any shared Source state
	// (Codex P1 round-4 L137). The state is process-local to this
	// aggregator; concurrent Observe callers race on Add only.
	rngState atomic.Uint64
}

// splitmix64 standard parameters — public-domain mixer used unchanged
// across implementations. The golden-ratio increment is coprime with
// every power of two so the atomic counter walks the whole uint64 space.
const (
	splitmix64GoldenInc uint64 = 0x9E3779B97F4A7C15
	splitmix64Mul1      uint64 = 0xBF58476D1CE4E5B9
	splitmix64Mul2      uint64 = 0x94D049BB133111EB
	splitmix64Shift1    uint64 = 30
	splitmix64Shift2    uint64 = 27
	splitmix64Shift3    uint64 = 31
)

// nextSampleRoll returns a pseudo-random uint64 drawn from the
// aggregator's lock-free splitmix64 stream. The output is uniformly
// distributed over [0, 2^64), so `nextSampleRoll() % R == 0` is the
// design §4 probabilistic sample gate.
func (a *hotKeysAggregator) nextSampleRoll() uint64 {
	z := a.rngState.Add(splitmix64GoldenInc)
	z = (z ^ (z >> splitmix64Shift1)) * splitmix64Mul1
	z = (z ^ (z >> splitmix64Shift2)) * splitmix64Mul2
	return z ^ (z >> splitmix64Shift3)
}

// applyHotKeysDefaults clamps every HotKeys* field on opts to its
// Default / Max range. Extracted out of NewMemSampler so the constructor
// stays below the cyclop ceiling as the hot-keys knobs grew the option
// surface (5 new fields). Caller takes a pointer because the defaults
// must mutate the struct seen by the rest of NewMemSampler.
func applyHotKeysDefaults(opts *MemSamplerOptions) {
	if opts.HotKeysPerRoute <= 0 {
		opts.HotKeysPerRoute = DefaultHotKeysPerRoute
	}
	if opts.HotKeysPerRoute > MaxHotKeysPerRoute {
		opts.HotKeysPerRoute = MaxHotKeysPerRoute
	}
	if opts.HotKeysSampleRate <= 0 {
		opts.HotKeysSampleRate = DefaultHotKeysSampleRate
	}
	if opts.HotKeysSampleRate > MaxHotKeysSampleRate {
		opts.HotKeysSampleRate = MaxHotKeysSampleRate
	}
	if opts.HotKeysQueueSize <= 0 {
		opts.HotKeysQueueSize = DefaultHotKeysQueueSize
	}
	if opts.HotKeysQueueSize > MaxHotKeysQueueSize {
		opts.HotKeysQueueSize = MaxHotKeysQueueSize
	}
	if opts.HotKeysMaxKeyLen <= 0 {
		opts.HotKeysMaxKeyLen = DefaultHotKeysMaxKeyLen
	}
	if opts.HotKeysMaxKeyLen > MaxHotKeysMaxKeyLen {
		opts.HotKeysMaxKeyLen = MaxHotKeysMaxKeyLen
	}
}

func newHotKeysAggregator(s *MemSampler, opts MemSamplerOptions) *hotKeysAggregator {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	maxKeyLen := opts.HotKeysMaxKeyLen
	a := &hotKeysAggregator{
		s:          s,
		ch:         make(chan hotKeyEvent, opts.HotKeysQueueSize),
		capacity:   opts.HotKeysPerRoute,
		sampleRate: opts.HotKeysSampleRate,
		maxKeyLen:  maxKeyLen,
		step:       opts.Step,
		clock:      now,
		perSlotN:   map[slotKey]*uint64{},
		sketches:   map[slotKey]*spaceSaving{},
	}
	a.keyPool = &sync.Pool{
		New: func() any {
			b := make([]byte, 0, maxKeyLen)
			return &b
		},
	}
	return a
}

// borrowKeyBuffer returns a pooled buffer cleared to zero length but
// with maxKeyLen capacity, so Observe can append without further
// allocation for any key within the configured limit.
func (a *hotKeysAggregator) borrowKeyBuffer() *[]byte {
	// keyPool.New is set in newHotKeysAggregator to always return a
	// *[]byte, so the assertion can never fail in practice. The
	// comma-ok form keeps forcetypeassert happy and ensures a panic
	// would carry a more useful message than the runtime's default if
	// a future refactor ever changes the pool's New func.
	bp, ok := a.keyPool.Get().(*[]byte)
	if !ok || bp == nil {
		b := make([]byte, 0, a.maxKeyLen)
		bp = &b
	}
	*bp = (*bp)[:0]
	return bp
}

// releaseKeyBuffer returns the buffer to the pool. The aggregator
// calls this immediately after it has copied the key bytes into the
// SS entry's own storage (sketches.observe copies internally).
func (a *hotKeysAggregator) releaseKeyBuffer(bp *[]byte) {
	*bp = (*bp)[:0]
	a.keyPool.Put(bp)
}

// observe is invoked from the sampler's hot path when hot-keys is
// enabled. It performs the design §4 ordering:
//  1. length check FIRST (skipped_long_keys counted deterministically,
//     not subject to sampling — Codex P2 round-7 L116)
//  2. probabilistic sample gate (splitmix64 nextSampleRoll()%R == 0,
//     lock-free — Codex P1 round-4 L137)
//  3. key clone via sync.Pool
//  4. non-blocking channel send (drop-on-full + drop counter — Codex
//     P2 round-3 L138)
//
// Returns true iff the event was enqueued for the aggregator. The
// boolean is for tests; the production caller in Observe ignores it.
func (a *hotKeysAggregator) observe(routeID uint64, label Label, key []byte) bool {
	if len(key) > a.maxKeyLen {
		a.skipped.Add(1)
		return false
	}
	// Probabilistic sample gate driven by the aggregator's lock-free
	// splitmix64 stream (no shared Source ownership, no math/rand/v2
	// import — see nextSampleRoll). The u64NonNeg helper (sampler.go)
	// gives gosec the explicit non-negative guard it needs for the
	// int→uint64 cast; sampleRate is sampler-clamped to ≥1 anyway, so
	// the path that would return 0 here is unreachable.
	rate := u64NonNeg(a.sampleRate)
	if rate == 0 {
		rate = 1
	}
	if a.nextSampleRoll()%rate != 0 {
		return false
	}
	bp := a.borrowKeyBuffer()
	*bp = append(*bp, key...)
	select {
	case a.ch <- hotKeyEvent{slot: slotKey{RouteID: routeID, Label: label}, key: bp}:
		// bp will be released by the aggregator after it copies the key
		// into the SS entry's own storage.
		return true
	default:
		a.dropped.Add(1)
		a.releaseKeyBuffer(bp)
		return false
	}
}

// run is the aggregator goroutine's main loop. Single select with two
// arms (drain events / tick), matching the design §4 pseudocode:
//
//	for {
//	  select {
//	    case e := <-ch: updateSketch(e)
//	    case <-tick:
//	      for len(ch) > 0 { updateSketch(<-ch) }  // best-effort pre-drain
//	      publishAndReset()
//	  }
//	}
//
// Single-writer to every per-route/label SS sketch and to perSlotN, so no
// lock is needed on the update path. ctx cancellation drains a final
// publish so the last window's data isn't lost on shutdown.
func (a *hotKeysAggregator) run(ctx context.Context) {
	tick := time.NewTicker(a.step)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// Best-effort final drain + publish so an operator hitting
			// the drill-down right after a graceful shutdown gets the
			// last window's data rather than an empty snapshot.
			for len(a.ch) > 0 {
				a.consume(<-a.ch)
			}
			a.publishAndReset()
			return
		case e := <-a.ch:
			a.consume(e)
		case <-tick.C:
			// Pre-drain: process every event currently queued before we
			// publish + reset, so the just-finished window's tail-end
			// observations land in this snapshot rather than the next
			// (Codex P2 round-7 L193 — boundary smear is now bounded by
			// arrivals during publishAndReset, not by the channel depth
			// at the tick).
			for len(a.ch) > 0 {
				a.consume(<-a.ch)
			}
			a.publishAndReset()
		}
	}
}

// consume handles one event: drains the sample-N counter for the route,
// updates the Space-Saving sketch, and releases the pool buffer.
func (a *hotKeysAggregator) consume(e hotKeyEvent) {
	n, ok := a.perSlotN[e.slot]
	if !ok {
		n = new(uint64)
		a.perSlotN[e.slot] = n
	}
	*n++

	sk, ok := a.sketches[e.slot]
	if !ok {
		sk = newSpaceSaving(a.capacity)
		a.sketches[e.slot] = sk
	}
	// SS copies the bytes into its own entry storage; the pool buffer
	// is then safe to return immediately.
	sk.observe(*e.key)
	a.releaseKeyBuffer(e.key)
}

// publishAndReset is called from the tick branch after the pre-drain.
// It atomically (single-writer; no concurrent reader of the live
// sketch) deep-copies every per-route sketch into a fresh
// KeyvizHotKeysSnapshot, publishes it on the matching routeSlot via
// atomic.Pointer.Store, then resets the live sketches and the
// per-route / node-global counters so the next keyvizStep window
// starts empty (design §4 sketch reset; Codex P2 round-6 L186).
func (a *hotKeysAggregator) publishAndReset() {
	// When no per-route sketch exists this window (e.g. every observe
	// was filtered by the length cap, or no traffic at all on any
	// route), we have nowhere to attach the node-global drop / skip
	// counters — and Swap-resetting them here would silently discard
	// the signal. Carry them forward into the next window instead;
	// whichever publish tick first has a sketch will surface them.
	if len(a.sketches) == 0 {
		return
	}
	now := a.clock()
	// Swap-and-capture (not Load + later Store(0)) — the hot path can
	// still call a.dropped.Add(1) / a.skipped.Add(1) between a Load and
	// a Store, and those increments would belong to neither the snapshot
	// being published (already latched) nor the next window (the Store
	// would clobber them), leaving `degraded=false` during a publish-
	// straddling burst (claude bot 🔴 PR #854).
	dropped := a.dropped.Swap(0)
	skipped := a.skipped.Swap(0)
	tbl := a.s.table.Load()
	for skey, sk := range a.sketches {
		slot := lookupSlotForRoute(tbl, skey.RouteID, skey.Label)
		if slot == nil {
			// Route was removed mid-window: drop the sketch AND its
			// per-route counter so we don't accumulate dead entries
			// under route churn (gemini HIGH on PR #854 — without this,
			// every removed route's m × key bytes plus a *uint64 stays
			// in the aggregator forever).
			delete(a.sketches, skey)
			delete(a.perSlotN, skey)
			continue
		}
		n := uint64(0)
		if p, ok := a.perSlotN[skey]; ok {
			n = *p
		}
		snap := &KeyvizHotKeysSnapshot{
			RouteID:         skey.RouteID,
			Label:           skey.Label,
			SampledN:        n,
			DroppedSamples:  dropped,
			SkippedLongKeys: skipped,
			SnapshotAt:      now,
			SampleRate:      a.sampleRate,
			Capacity:        a.capacity,
			Entries:         toSnapshotEntries(sk.snapshot()),
		}
		slot.hotKeysSnap.Store(snap)
		sk.reset()
	}
	// Per-route counters reset; the node-global ones were already
	// captured-and-reset by the Swap(0) calls at the top of this
	// function — issuing another Store(0) here would re-clobber any
	// hot-path Add that arrived during the publish loop.
	for k := range a.perSlotN {
		*a.perSlotN[k] = 0
	}
}

func toSnapshotEntries(es []ssEntrySnap) []KeyvizHotKeyEntry {
	if len(es) == 0 {
		return nil
	}
	out := make([]KeyvizHotKeyEntry, len(es))
	for i, e := range es {
		// ssEntrySnap and KeyvizHotKeyEntry have identical field layout;
		// a struct conversion is what staticcheck S1016 asks for and
		// is cheaper than the field-by-field literal copy.
		out[i] = KeyvizHotKeyEntry(e)
	}
	return out
}

// lookupSlotForRoute resolves a real (non-aggregate) route's slot from
// the route table. Aggregate / virtual buckets are NOT eligible for
// hot-keys tracking (design §2.2): they coarsen multiple routes and
// "top-K within an aggregate" is not meaningful.
func lookupSlotForRoute(tbl *routeTable, routeID uint64, label Label) *routeSlot {
	if tbl == nil {
		return nil
	}
	if s, ok := tbl.slots[slotKey{RouteID: routeID, Label: label}]; ok && !s.Aggregate {
		return s
	}
	return nil
}
