// Package keyviz implements the in-memory sampler that backs the
// docs/admin_ui_key_visualizer_design.md heatmap.
//
// The sampler sits on the data-plane hot path and counts requests per
// Raft route. The contract for callers (today, kv.ShardedCoordinator):
//
//   - Construct a MemSampler with NewMemSampler.
//   - Wire it through the coordinator with a constructor option; the
//     coordinator calls Sampler.Observe at the dispatch entry, after
//     resolving the RouteID.
//   - Run Flush every StepSeconds in a background goroutine — RunFlusher
//     does this for you with the supplied clock and ctx.
//   - Read the rendered matrix via Snapshot for the Admin gRPC service.
//
// Hot-path properties (see design §5.1, §10):
//
//   - Observe is a single atomic.Pointer[routeTable].Load, a plain map
//     lookup against an immutable snapshot, and at most two
//     atomic.AddUint64 calls (one for the count, one for bytes —
//     skipped when both keyLen and valueLen are zero). No allocation,
//     no mutex.
//   - Flush drains the per-route counters with atomic.SwapUint64; no
//     pointer retirement, so a late writer cannot race past the snapshot
//     and lose counts.
//   - Adding / removing routes (RegisterRoute, RemoveRoute today;
//     ApplySplit / ApplyMerge in a future PR) builds a fresh
//     routeTable copy under a non-hot-path mutex and publishes it
//     with a single atomic.Pointer.Store. Routes mutated mid-step
//     keep their counters in the new table by design.
package keyviz

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Op identifies which counter family Observe should bump.
type Op uint8

const (
	OpRead Op = iota
	OpWrite
)

// Series selects which counter the matrix response should expose.
type Series uint8

const (
	SeriesReads Series = iota
	SeriesWrites
	SeriesReadBytes
	SeriesWriteBytes
)

// Sampler is the narrow interface the coordinator depends on. The
// nil-safe contract is documented per-method so a coordinator wired
// without a sampler compiles to a no-op call.
//
// Implementations MUST be nil-receiver-safe: a typed-nil
// implementation passed through this interface (e.g.
// `var s Sampler = (*MemSampler)(nil)`) must not panic when its
// methods are called. The coordinator stores the interface value as
// supplied and dispatches through it on the hot path; a guard at the
// call site only checks for an interface-nil, not a typed-nil.
type Sampler interface {
	// Observe records a single request against a route. Op identifies
	// the counter family. The key drives sub-range bucketing (the hot
	// key/sub-range heatmap, see docs/design/2026_05_25_implemented_keyviz_subrange_sampling.md);
	// len(key) and valueLen are summed into the matching *Bytes
	// counter; pass valueLen 0 for read-only ops where the response
	// size is irrelevant. Implementations must no-op (not panic) when
	// invoked on a typed-nil receiver.
	Observe(routeID uint64, key []byte, op Op, valueLen int)
}

// Defaults for MemSamplerOptions when fields are left zero.
const (
	DefaultStep                   = 60 * time.Second
	DefaultHistoryColumns         = 1440 // 24 hours at 60s steps.
	DefaultMaxTrackedRoutes       = 10_000
	DefaultMaxMemberRoutesPerSlot = 256
	// DefaultKeyBucketsPerRoute is 1 — no sub-range bucketing, i.e.
	// today's exact route-granular behaviour. Operators opt into
	// hot-key sub-range sampling by raising it.
	DefaultKeyBucketsPerRoute = 1
)

// Defaults / caps for the per-cell hot-key drill-down knobs (see
// docs/design/2026_05_28_proposed_keyviz_hot_key_topk.md §8). All
// off-by-default; HotKeysEnabled=false is the binary's existing
// behaviour — no extra hot-path cost, no real key bytes retained.
const (
	DefaultHotKeysPerRoute   = 64
	MaxHotKeysPerRoute       = 256
	DefaultHotKeysSampleRate = 16
	MaxHotKeysSampleRate     = 1024
	DefaultHotKeysQueueSize  = 8192
	MaxHotKeysQueueSize      = 65536
	DefaultHotKeysMaxKeyLen  = 1024
	MaxHotKeysMaxKeyLen      = 4096
)

// MaxKeyBucketsPerRoute caps KeyBucketsPerRoute. The cap is an
// operationally-safe bound, not merely a finite one: per-route memory
// is K × 4 × 8 bytes, so at the default MaxTrackedRoutes = 10_000 the
// cap of 256 bounds the worst case to ~80 MB of counters (vs ~1.28 GB
// at K=4096, which a monitoring subsystem must not need). The design's
// operator rule of thumb is K_max ≈ memBudget / (32 × MaxTrackedRoutes).
const MaxKeyBucketsPerRoute = 256

// subWindowBytes is the W from the design: the fixed byte window past
// the route's Start/End common prefix that the bucketer interpolates
// over. Eight bytes (one uint64) gives enough resolution for K up to
// the cap; keys whose distinguishing bytes sit past prefixLen + W
// collapse the route to a single bucket (computeSubLayout).
const subWindowBytes = 8

// MaxHistoryColumns is the upper bound on opts.HistoryColumns. The
// ring buffer pre-allocates a slice of capacity HistoryColumns at
// construction; misconfiguration (e.g. an operator typo of
// 100_000_000) would otherwise reserve gigabytes up front. 100 000
// columns at the default 60s Step is ~70 days of history — longer
// retention is the Phase 3 persistence path's job, not the in-memory
// ring's.
const MaxHistoryColumns = 100_000

// MemSamplerOptions configures NewMemSampler. Zero values fall back to
// the Default* constants above; passing a struct literal with only the
// fields you care about is the expected call style.
type MemSamplerOptions struct {
	// Step is the flush interval. The ring buffer's resolution.
	Step time.Duration
	// HistoryColumns caps the ring buffer length. Older columns are
	// dropped on push when the buffer is full.
	HistoryColumns int
	// MaxTrackedRoutes caps the number of routes whose counters are
	// kept individually before coarsening kicks in. RegisterRoute
	// returns false past this cap, the route ID maps into a virtual
	// bucket, and Snapshot reports it with Aggregate=true.
	MaxTrackedRoutes int
	// MaxMemberRoutesPerSlot caps how many distinct RouteIDs a single
	// virtual bucket records in MemberRoutes. Beyond this cap the
	// route still folds into the bucket counters (so traffic is not
	// dropped) but the routeID is not appended — keeping per-column
	// payload size bounded when total routes far exceed
	// MaxTrackedRoutes. Snapshot consumers should treat the list as
	// "first N members" rather than authoritative attribution.
	MaxMemberRoutesPerSlot int
	// KeyBucketsPerRoute (K) is how many order-preserving sub-range
	// buckets each individual route's [Start, End) is divided into for
	// the hot-key heatmap. 1 (the default) disables sub-bucketing and
	// reproduces today's route-granular behaviour exactly. Clamped to
	// [1, MaxKeyBucketsPerRoute] at construction. Virtual aggregate
	// buckets are never sub-divided regardless of this value.
	KeyBucketsPerRoute int
	// HotKeysEnabled opts in to per-route Top-K hot-key tracking that
	// backs the heatmap drill-down (Phase 2-A++; see
	// docs/design/2026_05_28_proposed_keyviz_hot_key_topk.md). When
	// false the hot path adds one early-return branch and nothing else
	// — disabled-case behaviour is byte-identical to today. When true
	// the sampler retains REAL key bytes in memory and exposes them on
	// the admin drill-down API (gated behind admin auth + audit).
	HotKeysEnabled bool
	// HotKeysPerRoute is the Space-Saving sketch capacity m per route
	// (default 64, cap 256 — design §8). Larger m tightens the noise
	// floor (error_bound ≈ N_total / m) at the cost of memory.
	HotKeysPerRoute int
	// HotKeysSampleRate is R: the hot path enqueues 1 in R observes.
	// Default 16, cap 1024. Higher R reduces hot-path cost but raises
	// the probability that a heavy hitter is missed (Chernoff variance
	// over the sampled stream).
	HotKeysSampleRate int
	// HotKeysQueueSize bounds the channel between the hot path and
	// the aggregator goroutine. Defaults 8192, cap 65536. Drops past
	// the queue are counted, not silent (`dropped_samples` →
	// `degraded`).
	HotKeysQueueSize int
	// HotKeysMaxKeyLen caps the key length sampled into the hot-keys
	// sketch. Default 1024 B, cap 4096 B. Longer keys bump the
	// node-global `skipped_long_keys` counter and contribute to the
	// snapshot's `degraded` flag — they are never truncated (truncation
	// would alias different keys).
	HotKeysMaxKeyLen int
	// Now overrides time.Now for tests; nil falls back to time.Now.
	Now func() time.Time
}

// MemSampler is the in-process Sampler implementation. The zero value
// is not usable — construct via NewMemSampler.
type MemSampler struct {
	opts MemSamplerOptions
	now  func() time.Time

	// table holds the immutable map of currently-tracked routes. The
	// hot path Observe() does a single Load + map lookup; mutations
	// (Register / Remove / Split / Merge) take routesMu, copy, mutate
	// the copy, then atomic-store the new pointer.
	table atomic.Pointer[routeTable]

	// routesMu serialises the routeTable copy-on-write update path.
	// Held only by non-hot-path callers.
	routesMu sync.Mutex

	// historyMu guards the ring buffer. Reads (Snapshot) and writes
	// (Flush) acquire it; Observe never touches it.
	historyMu sync.Mutex
	history   *ringBuffer

	// retiredSlots holds slots that RemoveRoute removed from the live
	// table. Each entry is drained for `remaining` Flushes — a grace
	// period that lets late Observe writers (which loaded the route
	// table before RemoveRoute's atomic.Store) finish their Add into
	// the slot before we forget about it. retainedFlushes controls the
	// length of that window. pendingPrunes is the same idea for
	// virtual-bucket member-route removals: the routeID stays in
	// MemberRoutes during the grace window so the row attribution
	// stays correct while the bucket counters still include the
	// removed route's pre-removal increments. Both are guarded by
	// retiredMu since they are only touched off the hot path.
	retiredMu     sync.Mutex
	retiredSlots  []retiredSlot
	pendingPrunes []memberPrune

	// virtualIDCounter hands out synthetic RouteIDs for new virtual
	// buckets, starting at MaxUint64 and decrementing. The synthetic
	// space cannot collide with real route IDs (which the coordinator
	// assigns from the low end), so a real RouteID can never appear
	// twice in the same column — once as an aggregate row, once as
	// an individual row — even under register/remove churn.
	virtualIDCounter atomic.Uint64

	// hotKeys is the per-route Top-K aggregator (design 2026_05_28
	// _proposed_keyviz_hot_key_topk). nil when HotKeysEnabled is false
	// — the Observe hot path then skips the feature with one branch.
	// Read from the hot path; assigned exactly once at construction.
	hotKeys *hotKeysAggregator

	// groupTermsMu guards groupTerms. SetLeaderTerm and Flush both
	// touch the map; Observe never does. The lock is fine-grained
	// enough that contention is bounded by leader-term flips, which
	// happen at most a few times per second across the whole cluster.
	groupTermsMu sync.RWMutex
	groupTerms   map[uint64]uint64
}

// retiredSlot tracks a removed slot through its post-removal grace
// period. retiredAt is the wall-clock instant of removal; the entry
// is drained on every Flush whose `now()` is within the configured
// grace window of retiredAt, then dropped.
type retiredSlot struct {
	slot      *routeSlot
	retiredAt time.Time
}

// memberPrune tracks a deferred virtual-bucket member-route removal.
// retiredAt is the wall-clock instant of removal; while now() is
// within the grace window, the routeID stays in bucket.MemberRoutes so
// flushed rows continue attributing the bucket's mixed counters to all
// members that contributed (including this one).
type memberPrune struct {
	bucket    *routeSlot
	routeID   uint64
	retiredAt time.Time
}

// minGraceWindow is the wall-clock floor for late-writer grace. It
// guards against Step being configured small enough (millisecond
// scale) that a flush-count-based grace window would expire before a
// preempted Observe goroutine resumes. Any goroutine still holding a
// pre-RemoveRoute table snapshot must finish its single atomic.Add
// before now-retiredAt exceeds this window.
const minGraceWindow = 5 * time.Second

// graceStepMultiplier is how many Step intervals we want the grace
// window to cover by default — picked so retired slots are drained
// at least twice before being dropped.
const graceStepMultiplier = 2

// graceWindow is the duration retired slots and pending member-prunes
// stay drainable after RemoveRoute. It's tied to wall-clock time, not
// Flush count, so a small Step doesn't shrink the grace below
// minGraceWindow.
func (s *MemSampler) graceWindow() time.Duration {
	g := graceStepMultiplier * s.opts.Step
	if g < minGraceWindow {
		g = minGraceWindow
	}
	return g
}

// routeTable is the COW snapshot Observe operates on. Once published
// via MemSampler.table.Store, fields are read-only.
type routeTable struct {
	// slots is the live route → slot map. Lookups under the hot path
	// run against this snapshot.
	slots map[uint64]*routeSlot
	// virtualForRoute maps a real RouteID that didn't fit under
	// MaxTrackedRoutes to its virtual-bucket slot. Observe consults
	// this fallback when slots[routeID] is missing.
	virtualForRoute map[uint64]*routeSlot
	// sortedSlots is the union of slots and virtualForRoute values
	// sorted by Start, used by Snapshot for stable row ordering.
	sortedSlots []*routeSlot
}

// routeSlot owns the atomic counters for a tracked route or virtual
// bucket. Counter fields are touched by Observe (atomic.AddUint64) and
// Flush (atomic.SwapUint64); the metadata (RouteID, Start, End,
// Aggregate, MemberRoutes) for an individual route slot is immutable
// after the slot is published.
//
// Virtual buckets are the exception: when a new RouteID over the
// MaxTrackedRoutes cap folds into an existing bucket, RegisterRoute
// extends MemberRoutes and may grow Start/End. metaMu serialises
// those updates against Flush's read-side iteration so a concurrent
// Flush cannot observe a half-extended slice. Observe never reads
// these fields, so the hot path remains lockless.
type routeSlot struct {
	metaMu  sync.RWMutex
	RouteID uint64
	// GroupID is the Raft group this route belongs to. Phase 2-C+
	// stamps this on every emitted MatrixRow so the cluster fan-out
	// aggregator can dedupe write samples by (raftGroupID,
	// leaderTerm) instead of the conservative max-merge that may
	// undercount during a leadership flip. 0 means "no group
	// attached" (legacy single-group deployments and the synthetic
	// virtual-bucket slots both use 0).
	GroupID uint64
	Start   []byte
	End     []byte
	// Aggregate marks virtual buckets that fold multiple coarsened
	// routes together (Snapshot surfaces this in MatrixRow).
	Aggregate    bool
	MemberRoutes []uint64
	// hiddenMembers stores routeIDs folded past
	// MaxMemberRoutesPerSlot. Used to dedup re-folds (so
	// MemberRoutesTotal doesn't drift on remove+re-register churn for
	// past-cap routes) and to drive accurate decrements in
	// pruneMemberRoute. nil for individual slots.
	hiddenMembers map[uint64]struct{}
	// MemberRoutesTotal is len(MemberRoutes) + len(hiddenMembers) — the
	// authoritative count of distinct routes folded into this bucket.
	// Always equals len(MemberRoutes) for individual (non-Aggregate)
	// slots.
	MemberRoutesTotal uint64

	// Sub-range layout (the hot-key heatmap). These fields are
	// established once when the slot's range is set (RegisterRoute via
	// initSubLayout) and are IMMUTABLE for the slot's lifetime — they
	// are NOT recomputed on grace-window re-registration. So the hot
	// path (subBucketIndex) and Flush (bound reconstruction) both read
	// them lock-free; there is no writer to race with. See the design
	// doc §4.2.
	//
	// subSpan == 0 (and len(subBuckets) == 1) marks a slot that is not
	// sub-divided: K==1, a virtual aggregate, or a degenerate window
	// (subEnd <= subStart). An unbounded-tail route (End == nil) with
	// K > 1 DOES sub-divide — over [subStart, MaxUint64] (§3.2) — so it
	// is not in this list. subBucketIndex hard-wires the index to 0 for
	// the subSpan == 0 slots, collapsing to today's route-level behaviour.
	subPrefixLen int
	subStart     uint64
	subEnd       uint64
	subSpan      uint64
	// subLo/subHi are immutable copies of the route bounds the layout
	// was built from, used by subBucketIndex's full-key edge clamp.
	// They are NOT slot.Start/End (which are mutable under metaMu on
	// grace-window re-registration and so can't be read on the lock-free
	// hot path) — they are part of the immutable layout. nil for
	// single-bucket slots (the clamp is unreachable there).
	subLo      []byte
	subHi      []byte
	subBuckets []subCounter

	// hotKeysSnap is the most-recently-published per-route Top-K
	// snapshot (design 2026_05_28_proposed_keyviz_hot_key_topk §4).
	// nil until the hot-keys aggregator's first publish for this route.
	// Drill-down handlers load it lock-free; the aggregator is the
	// single writer (Store) and deep-copies snapshot contents so a
	// subsequent reset / eviction on the live sketch cannot mutate a
	// snapshot a reader holds.
	hotKeysSnap atomic.Pointer[KeyvizHotKeysSnapshot]
}

// subCounter holds one sub-range bucket's four counter families. A
// routeSlot owns len(subBuckets) of these (>= 1). Touched by Observe
// (atomic.Add) and Flush (atomic.Swap); never value-copied (slots are
// shared by pointer, and the slice is indexed in place), so the
// embedded atomics are safe.
type subCounter struct {
	reads      atomic.Uint64
	writes     atomic.Uint64
	readBytes  atomic.Uint64
	writeBytes atomic.Uint64
}

// snapshotMeta returns a defensive copy of the slot's metadata under
// the read lock. Used by Flush so the row it emits doesn't share
// Start/End/MemberRoutes with the live slot (which a later
// RegisterRoute may extend, and which the snapshot API exports to
// external consumers that may mutate the bounds).
func (s *routeSlot) snapshotMeta() (start, end []byte, aggregate bool, members []uint64, membersTotal, groupID uint64) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	start = cloneBytes(s.Start)
	end = cloneBytes(s.End)
	aggregate = s.Aggregate
	if len(s.MemberRoutes) > 0 {
		members = append([]uint64(nil), s.MemberRoutes...)
	}
	membersTotal = s.MemberRoutesTotal
	groupID = s.GroupID
	return
}

// MatrixColumn is one slice of the heatmap at a single flush time.
type MatrixColumn struct {
	At   time.Time
	Rows []MatrixRow
}

// MatrixRow is a single route or virtual bucket's counter snapshot
// taken at flush time.
type MatrixRow struct {
	RouteID      uint64
	Start, End   []byte
	Aggregate    bool
	MemberRoutes []uint64
	// MemberRoutesTotal is how many distinct route IDs contributed to
	// this row's counters, including ones that exceeded
	// MaxMemberRoutesPerSlot and so are NOT listed in MemberRoutes.
	// Snapshot consumers should treat MemberRoutes as the visible
	// prefix of this list when MemberRoutesTotal > len(MemberRoutes).
	MemberRoutesTotal uint64

	// RaftGroupID + LeaderTerm carry the route's Raft identity at the
	// time the column was flushed. Stamped from the per-group term
	// snapshot SetLeaderTerm publishes (Phase 2-C+ fan-out merge
	// uses (RouteID/BucketID, RaftGroupID, LeaderTerm, columnAt) as
	// the dedupe key). Zero values mean "term not tracked" — emitted
	// when no SetLeaderTerm call has been made for the group yet, or
	// for synthetic virtual-bucket slots that span groups.
	RaftGroupID uint64
	LeaderTerm  uint64

	// SubBucket is this row's sub-range index within its parent route,
	// 0 for the first (or only) sub-bucket. SubBucketCount is how many
	// sub-buckets the route is divided into: 1 for a K==1 / aggregate /
	// degenerate / unbounded-tail slot, > 1 for a genuinely sub-divided
	// route. SubBucketCount is the disambiguator a row's BucketID
	// suffix needs — a K==1 slot's only row and a K>1 slot's bucket-0
	// row both have SubBucket == 0, so the count is what tells them
	// apart (the #subIdx suffix is added only when SubBucketCount > 1).
	// Both are int (bounded 0..MaxKeyBucketsPerRoute); they are
	// keyviz-internal (the proto KeyVizRow has no equivalent), so no
	// fixed-width type is required.
	SubBucket      int
	SubBucketCount int

	Reads      uint64
	Writes     uint64
	ReadBytes  uint64
	WriteBytes uint64
}

// NewMemSampler constructs a sampler with the supplied options. Zero
// fields fall back to the Default* constants; non-positive values
// (including explicitly-negative HistoryColumns) are clamped to a safe
// minimum by newRingBuffer. Always returns a usable sampler — callers
// should pass a zero options struct for the default configuration.
func NewMemSampler(opts MemSamplerOptions) *MemSampler {
	if opts.Step <= 0 {
		opts.Step = DefaultStep
	}
	if opts.HistoryColumns <= 0 {
		opts.HistoryColumns = DefaultHistoryColumns
	}
	if opts.HistoryColumns > MaxHistoryColumns {
		opts.HistoryColumns = MaxHistoryColumns
	}
	if opts.MaxTrackedRoutes <= 0 {
		opts.MaxTrackedRoutes = DefaultMaxTrackedRoutes
	}
	if opts.MaxMemberRoutesPerSlot <= 0 {
		opts.MaxMemberRoutesPerSlot = DefaultMaxMemberRoutesPerSlot
	}
	if opts.KeyBucketsPerRoute <= 0 {
		opts.KeyBucketsPerRoute = DefaultKeyBucketsPerRoute
	}
	if opts.KeyBucketsPerRoute > MaxKeyBucketsPerRoute {
		opts.KeyBucketsPerRoute = MaxKeyBucketsPerRoute
	}
	applyHotKeysDefaults(&opts)
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	s := &MemSampler{
		opts:       opts,
		now:        now,
		history:    newRingBuffer(opts.HistoryColumns),
		groupTerms: map[uint64]uint64{},
	}
	s.table.Store(newEmptyRouteTable())
	if opts.HotKeysEnabled {
		s.hotKeys = newHotKeysAggregator(s, opts)
	}
	return s
}

// HotKeysSnapshot returns the most-recently-published per-route Top-K
// snapshot for the given individual route, or nil if hot-keys tracking
// is disabled, the route is unknown, the route is an aggregate, or no
// publish has fired yet. Lock-free: an atomic.Pointer.Load (Codex-P1
// fix from the design's round-4 review — no shared mutable PRNG state,
// no per-route lock).
func (s *MemSampler) HotKeysSnapshot(routeID uint64) *KeyvizHotKeysSnapshot {
	if s == nil {
		return nil
	}
	slot := lookupSlotForRoute(s.table.Load(), routeID)
	if slot == nil {
		return nil
	}
	return slot.hotKeysSnap.Load()
}

// HotKeysOptions returns the effective hot-keys configuration after the
// NewMemSampler clamps. Used by the admin handler so the response can
// echo the same `sample_rate` / `m` values the sketch was built with,
// regardless of what an operator passed on the CLI. nil-receiver-safe.
func (s *MemSampler) HotKeysOptions() (enabled bool, capacity, sampleRate, maxKeyLen int) {
	if s == nil {
		return false, 0, 0, 0
	}
	return s.opts.HotKeysEnabled, s.opts.HotKeysPerRoute, s.opts.HotKeysSampleRate, s.opts.HotKeysMaxKeyLen
}

// SubBucketBoundsFor returns the [lo, hi) key bounds of sub-bucket
// subBucket within the individual route routeID, mirroring what Flush
// emits as a MatrixRow's Start/End. The admin hot-keys handler uses
// this to filter the route's top-m snapshot to keys in the
// drill-down-clicked sub-range (design §5).
//
// ok is false when the route doesn't exist, is an aggregate (those are
// not eligible for hot-keys), or subBucket is out of range
// [0, len(subBuckets)). For a single-bucket slot (K=1 / unbounded /
// degenerate), bound calls with subBucket == 0 succeed and return the
// route's own Start/End. hi == nil means the sub-bucket extends to the
// route's unbounded tail.
//
// nil-receiver-safe.
func (s *MemSampler) SubBucketBoundsFor(routeID uint64, subBucket int) (lo, hi []byte, ok bool) {
	if s == nil || subBucket < 0 {
		return nil, nil, false
	}
	slot := lookupSlotForRoute(s.table.Load(), routeID)
	if slot == nil {
		return nil, nil, false
	}
	if subBucket >= len(slot.subBuckets) {
		return nil, nil, false
	}
	start, end, _, _, _, _ := slot.snapshotMeta()
	if len(slot.subBuckets) <= 1 {
		return start, end, true
	}
	lo, hi = slot.subBucketBounds(subBucket, start, end)
	return lo, hi, true
}

// SetLeaderTerm publishes the current Raft leader term for the given
// group. Called by main.go on a periodic ticker that polls the engine
// Status, and on every term-change observed via the engine. Phase 2-C+
// stamps each MatrixRow's LeaderTerm from this snapshot at Flush time.
//
// Calling with term == 0 is allowed but treated as "term unknown"
// during merge — the canonical (groupID, term) dedupe key collapses
// to the legacy max-merge for cells whose LeaderTerm is 0. This lets
// nodes that have not finished engine startup contribute partial data
// without poisoning the merge.
//
// groupID == 0 is reserved for virtual aggregate buckets (which span
// multiple real groups). Calls with groupID == 0 are silently ignored
// so a future caller cannot accidentally stamp a non-zero term on
// aggregate rows — they must remain LeaderTerm == 0 so the fan-out
// merge falls back to max-merge for cross-group cells.
//
// nil-receiver-safe.
func (s *MemSampler) SetLeaderTerm(groupID, term uint64) {
	if s == nil || groupID == 0 {
		return
	}
	s.groupTermsMu.Lock()
	defer s.groupTermsMu.Unlock()
	s.groupTerms[groupID] = term
}

// snapshotGroupTerms returns a copy of the per-group term map. Called
// at the top of Flush so the column built below sees a stable view
// of the term mapping even if SetLeaderTerm fires concurrently.
// Returns nil when no terms have been published yet — `nil[k]` returns
// the zero value in Go, so the row-builder hot path needs no nil guard
// and avoids one allocation per Flush before SetLeaderTerm is wired.
func (s *MemSampler) snapshotGroupTerms() map[uint64]uint64 {
	s.groupTermsMu.RLock()
	defer s.groupTermsMu.RUnlock()
	if len(s.groupTerms) == 0 {
		return nil
	}
	out := make(map[uint64]uint64, len(s.groupTerms))
	for g, t := range s.groupTerms {
		out[g] = t
	}
	return out
}

func newEmptyRouteTable() *routeTable {
	return &routeTable{
		slots:           map[uint64]*routeSlot{},
		virtualForRoute: map[uint64]*routeSlot{},
	}
}

// Observe records one request against a route. Cost on a hit:
// atomic.Pointer.Load + plain map lookup + 2× atomic.AddUint64 (count
// and bytes). Misses (RouteID never registered) drop silently — the
// route-watch subscriber is responsible for Register before the
// coordinator publishes the new RouteID.
func (s *MemSampler) Observe(routeID uint64, key []byte, op Op, valueLen int) {
	if s == nil {
		return
	}
	tbl := s.table.Load()
	slot, ok := tbl.slots[routeID]
	if !ok {
		slot, ok = tbl.virtualForRoute[routeID]
		if !ok {
			return
		}
	}
	// Sub-range bucketing: pick the counter for the key's sub-bucket.
	// subBucketIndex reads only immutable layout fields (lock-free) and
	// returns 0 for non-sub-divided slots (len == 1), so this is one
	// extra index computation versus today on the hot path.
	sc := &slot.subBuckets[slot.subBucketIndex(key)]
	byteCount := uint64(0)
	if len(key) > 0 {
		byteCount += uint64(len(key))
	}
	if valueLen > 0 {
		byteCount += uint64(valueLen)
	}
	switch op {
	case OpRead:
		sc.reads.Add(1)
		if byteCount > 0 {
			sc.readBytes.Add(byteCount)
		}
	case OpWrite:
		sc.writes.Add(1)
		if byteCount > 0 {
			sc.writeBytes.Add(byteCount)
		}
		s.observeHotKey(routeID, slot, key)
	}
}

// observeHotKey is the per-Observe hot-key sampler dispatch, kept out of
// Observe itself to stay under the cyclop ceiling. v1 tracks WRITES
// only (design §5 series-picker note); reads follow in v2 once the
// hot-path cost is measured. The nil check short-circuits in disabled
// deploys with a single branch — Observe's disabled-case allocation
// contract is preserved. Aggregates (Aggregate==true) are not tracked
// (design §2.2); lookupSlotForRoute filters them at publish too.
func (s *MemSampler) observeHotKey(routeID uint64, slot *routeSlot, key []byte) {
	if s.hotKeys == nil || slot.Aggregate {
		return
	}
	s.hotKeys.observe(routeID, key)
}

// subBucketIndex maps key to a sub-range bucket index in
// [0, len(slot.subBuckets)). Lock-free: reads only the slot's immutable
// sub-layout fields. Returns 0 for non-sub-divided slots (len == 1 or
// subSpan == 0). See design §3.1.
func (slot *routeSlot) subBucketIndex(key []byte) int {
	k := len(slot.subBuckets)
	if k <= 1 || slot.subSpan == 0 {
		return 0
	}
	// Full-key edge clamp FIRST. The window only sees bytes past the
	// Start/End common prefix, so two keys that differ *within* that
	// prefix (i.e. a key outside [subLo, subHi)) would otherwise bucket
	// on their window alone and break global order preservation. Keys
	// inside the route share the prefix, so for them the window is
	// monotone; out-of-range keys pin to the boundary buckets.
	if bytes.Compare(key, slot.subLo) <= 0 {
		return 0
	}
	// subHi == nil is the unbounded-End sentinel (open high end, §3.2):
	// there is no finite upper bound to clamp against, so rely on the
	// window math + the w >= subEnd (== MaxUint64) guard below. A finite
	// route always has a non-nil subHi.
	if slot.subHi != nil && bytes.Compare(key, slot.subHi) >= 0 {
		return k - 1
	}
	w := windowUint64(key, slot.subPrefixLen)
	// In-range keys satisfy subStart <= w <= subEnd; these guards make
	// the subtraction underflow-proof and handle the window-equals-edge
	// case (keys that match Start/End in the first W suffix bytes).
	if w <= slot.subStart {
		return 0
	}
	if w >= slot.subEnd {
		return k - 1
	}
	idx := fracMul(u64NonNeg(k), w-slot.subStart, slot.subSpan)
	if idx >= u64NonNeg(k) {
		// Defensive only — the interior guard guarantees idx <= k-1.
		return k - 1
	}
	return intFromUint64(idx)
}

// u64NonNeg converts a known-non-negative int (a sub-bucket index or
// count, always 0..MaxKeyBucketsPerRoute, or a slice length) to uint64.
// The explicit guard makes gosec's G115 — which flags every int->uint64
// as a possible negative wrap — provably safe without a //nolint, which
// CLAUDE.md forbids.
func u64NonNeg(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// intFromUint64 narrows a uint64 that is known to fit in an int (here a
// sub-bucket index proven < k <= MaxKeyBucketsPerRoute). The math.MaxInt
// guard is the in-tree idiom gosec G115 accepts (cf. internal.Uint64ToInt).
func intFromUint64(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

// fracMul returns floor(a*b/c) with an exact 128-bit intermediate via
// math/bits, so the multiply cannot overflow uint64. Callers guarantee
// c > 0 and the quotient < 2^64 (both hold for sub-bucket math: the
// interior guard bounds a*b below c*2^64 because effK <= subSpan).
// Used by the forward index path (subBucketIndex).
func fracMul(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	q, _ := bits.Div64(hi, lo, c)
	return q
}

// fracMulCeil returns ceil(a*b/c) with the same exact 128-bit
// intermediate. boundaryAt uses ceil (not floor) so an emitted interior
// boundary is exactly the forward map's lower edge of bucket i:
// subBucketIndex puts w in bucket i iff w-subStart in
// [ceil(i*subSpan/k), ceil((i+1)*subSpan/k)). With floor boundaries a
// key landing exactly on a boundary value would be counted in the lower
// bucket yet excluded from its half-open [Start, End) label (Codex P2);
// ceil keeps the displayed ranges consistent with where keys are counted.
func fracMulCeil(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	q, r := bits.Div64(hi, lo, c)
	if r != 0 {
		q++
	}
	return q
}

// windowUint64 reads up to subWindowBytes of b starting at off,
// big-endian, zero-padding past the end of b. A short/absent window is
// the natural low end (0x00…) of the key space.
func windowUint64(b []byte, off int) uint64 {
	// Fast path: a full window is available — read it directly, skipping
	// the per-byte loop and bounds checks (hot path, Gemini medium).
	if off >= 0 && off+subWindowBytes <= len(b) {
		return binary.BigEndian.Uint64(b[off:])
	}
	var v uint64
	for i := 0; i < subWindowBytes; i++ {
		v <<= 8
		if off+i < len(b) {
			v |= uint64(b[off+i])
		}
	}
	return v
}

// initSubLayout computes the immutable sub-range layout for an
// individual route spanning [start, end) and allocates its subBuckets.
// Called once at slot creation (off the hot path). A route that cannot
// be sub-divided — K == 1, or a window that captures no difference —
// gets a single bucket (subSpan 0), which makes subBucketIndex
// hard-wire to 0 and reproduces today's behaviour. An unbounded `end`
// (End == nil) with K > 1 sub-divides over [subStart, MaxUint64] (§3.2),
// not a single bucket.
func (s *MemSampler) initSubLayout(slot *routeSlot, start, end []byte) {
	prefixLen, subStart, subEnd, subSpan, effK := computeSubLayout(start, end, s.opts.KeyBucketsPerRoute)
	slot.subPrefixLen = prefixLen
	slot.subStart = subStart
	slot.subEnd = subEnd
	slot.subSpan = subSpan
	if effK > 1 {
		// Immutable bound snapshots for subBucketIndex's full-key clamp.
		slot.subLo = cloneBytes(start)
		slot.subHi = cloneBytes(end)
	}
	slot.subBuckets = make([]subCounter, effK)
}

// computeSubLayout derives the sub-range layout for [start, end) divided
// into k buckets. effK is the effective bucket count: 1 (single bucket,
// subSpan 0) when the route cannot be sub-divided, otherwise
// min(k, subSpan) — capping at the span keeps every reconstructed
// boundary valid when the span is narrower than k (effKForSpan). See
// design §3.1–§3.2.
func computeSubLayout(start, end []byte, k int) (prefixLen int, subStart, subEnd, subSpan uint64, effK int) {
	if k <= 1 {
		return 0, 0, 0, 0, 1
	}
	if len(end) == 0 {
		// Unbounded high end (single-route cluster, or any cluster's
		// tail route). There is no End window to share a prefix with, so
		// bucket the leading W-byte window of the key across
		// [subStart, MaxUint64]. MaxUint64 (not 1<<64) keeps subSpan a
		// valid uint64 — the overflow that sank the original fallback.
		// See design §3.2.
		subStart = windowUint64(start, 0)
		if subStart == math.MaxUint64 {
			// start's window is already at the top of the space (all-0xFF
			// leading bytes) — nothing left to divide.
			return 0, subStart, subStart, 0, 1
		}
		subSpan = math.MaxUint64 - subStart
		return 0, subStart, math.MaxUint64, subSpan, effKForSpan(subSpan, k)
	}
	prefixLen = commonPrefixLen(start, end)
	subStart = windowUint64(start, prefixLen)
	subEnd = windowUint64(end, prefixLen)
	if subEnd <= subStart {
		// The W-byte window captures no difference (the routes differ
		// only past prefixLen + W): single bucket.
		return prefixLen, subStart, subEnd, 0, 1
	}
	subSpan = subEnd - subStart
	return prefixLen, subStart, subEnd, subSpan, effKForSpan(subSpan, k)
}

// effKForSpan caps the effective bucket count at the window span. A span
// smaller than k cannot be divided into k order-distinct sub-ranges:
// boundaryAt would round several interior boundaries back to subStart,
// and since a reconstructed boundary carries no bytes past the window it
// can sort BEFORE a route start that has suffix bytes — emitting a row
// with Start > End (Codex P2). Capping at subSpan keeps every emitted
// boundary strictly increasing in the window, so bounds stay valid and
// non-overlapping. subSpan is > 0 here (the degenerate subEnd<=subStart
// and all-0xFF cases return earlier), so effK >= 1.
func effKForSpan(subSpan uint64, k int) int {
	if subSpan < u64NonNeg(k) {
		return intFromUint64(subSpan)
	}
	return k
}

// commonPrefixLen returns the number of leading bytes a and b share.
func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// subBucketBounds reconstructs the [start, end) key bounds of sub-bucket
// i for a slot divided into len(subBuckets) buckets. Bucket 0 pins to
// the route's actual start and the last bucket pins to its actual end
// (the window arithmetic drops bytes past prefixLen + W, so the
// reconstructed extremes would otherwise fall short); interiors use
// boundaryAt's fracMulCeil — the ceil-dual of the forward path's floor
// fracMul, deliberately different so a boundary-value key lands in the
// same bucket it is counted in. Together this tiles [routeStart,
// routeEnd) exactly — each bucket's start equals the previous bucket's
// end. Caller guarantees len(subBuckets) > 1.
func (slot *routeSlot) subBucketBounds(i int, routeStart, routeEnd []byte) (start, end []byte) {
	last := len(slot.subBuckets) - 1
	if i == 0 {
		start = cloneBytes(routeStart)
	} else {
		start = slot.boundaryAt(i, routeStart)
	}
	if i == last {
		end = cloneBytes(routeEnd)
	} else {
		end = slot.boundaryAt(i+1, routeStart)
	}
	return start, end
}

// boundaryAt reconstructs bucket i's lower edge in key space:
// subStart + ceil(i*subSpan/k), written big-endian into the W-byte
// window at subPrefixLen, behind the route's shared prefix. ceil (via
// fracMulCeil) makes this exactly the forward map's lower edge of bucket
// i, so the emitted half-open [Start, End) ranges agree with where
// subBucketIndex counts boundary-value keys (Codex P2).
func (slot *routeSlot) boundaryAt(i int, routeStart []byte) []byte {
	off := slot.subStart + fracMulCeil(u64NonNeg(i), slot.subSpan, u64NonNeg(len(slot.subBuckets)))
	out := make([]byte, slot.subPrefixLen+subWindowBytes)
	copy(out, routeStart[:min(slot.subPrefixLen, len(routeStart))])
	binary.BigEndian.PutUint64(out[slot.subPrefixLen:], off)
	// Trim trailing zero bytes of the window. A variable-length key
	// shorter than prefix+W zero-pads to the same window value, so the
	// *minimal* key that buckets into this cell is the trimmed form.
	// Without trimming, a key like 0x10 (which subBucketIndex places in
	// the cell starting at 0x10) would sort BEFORE the emitted 8-byte
	// boundary 0x10 00…00, so the displayed [Start,End) would exclude
	// the very keys counted in the row — wrong labels for Redis-style
	// variable-length keyspaces (Codex P2). The prefix is never trimmed.
	end := len(out)
	for end > slot.subPrefixLen && out[end-1] == 0x00 {
		end--
	}
	return out[:end]
}

// RegisterRoute adds a (RouteID, [Start, End)) pair to the tracking
// set. groupID is the Raft group the route belongs to; it is stamped
// onto every MatrixRow this slot eventually emits so the Phase 2-C+
// fan-out merge can dedupe write samples by (groupID, leaderTerm).
// Pass 0 when the deployment doesn't use multiple groups (legacy /
// single-group setups); the legacy max-merge fallback handles those.
//
// Returns true when the route gets its own slot, false when the
// MaxTrackedRoutes cap was hit and the route was folded into a
// virtual aggregate bucket. Idempotent: calling twice with the same
// RouteID is a no-op (the original slot stays in place; a different
// groupID on the second call is silently ignored — RegisterRoute is
// not the right API to retag a slot, RemoveRoute + RegisterRoute is).
//
// If a previous RemoveRoute(routeID) queued a deferred member-prune
// and the route is now re-registered into the SAME bucket inside the
// grace window, that prune is cancelled — otherwise the routeID
// would disappear from bucket.MemberRoutes despite Observe still
// attributing fresh traffic to it. Prunes for different buckets (or
// when the route rejoins as an individual slot) are left alone so
// the old bucket's MemberRoutes is correctly cleaned up.
func (s *MemSampler) RegisterRoute(routeID uint64, start, end []byte, groupID uint64) bool {
	if s == nil {
		return false
	}
	s.routesMu.Lock()
	defer s.routesMu.Unlock()

	cur := s.table.Load()
	if _, ok := cur.slots[routeID]; ok {
		return true
	}
	if _, ok := cur.virtualForRoute[routeID]; ok {
		return false
	}

	next := copyRouteTable(cur)
	if len(next.slots) < s.opts.MaxTrackedRoutes {
		slot := s.reclaimRetiredSlot(routeID)
		if slot == nil {
			slot = &routeSlot{
				RouteID:           routeID,
				GroupID:           groupID,
				Start:             cloneBytes(start),
				End:               cloneBytes(end),
				MemberRoutesTotal: 1,
			}
			s.initSubLayout(slot, start, end)
		} else {
			// Re-registering the same routeID inside the grace window:
			// reuse the retired slot so any in-flight Observe writers
			// hitting the prior table snapshot land on the same slot
			// the new traffic uses, and Flush emits a single row per
			// RouteID instead of two (one live + one retired) with
			// counts split across them. Refresh the metadata fields to
			// match the new registration; counters are preserved.
			//
			// The sub-range layout (subPrefixLen/subStart/subEnd/subSpan
			// + subBuckets) is intentionally NOT recomputed here: it is
			// immutable for the slot's lifetime so the lock-free hot path
			// can read it without racing this writer, and the counters
			// must be preserved (not zeroed/resized) to honour the
			// no-loss contract. A same-RouteID re-registration carries
			// the same range in practice; the rare changed-range case
			// keeps the original sub-ranges for the reused slot's
			// lifetime — a bounded cosmetic mislabel, no lost counts.
			// See design §4.2.
			slot.metaMu.Lock()
			slot.GroupID = groupID
			slot.Start = cloneBytes(start)
			slot.End = cloneBytes(end)
			slot.MemberRoutesTotal = 1
			slot.metaMu.Unlock()
		}
		next.slots[routeID] = slot
		next.sortedSlots = appendSorted(next.sortedSlots, slot)
		s.table.Store(next)
		return true
	}

	// Coarsening: prefer the first virtual bucket whose [Start, End)
	// covers `start`; if none does, fall back to the first aggregate
	// in sortedSlots order (lowest Start). A new bucket is created
	// only when no virtual bucket exists yet. See findVirtualBucket
	// for the exact selection.
	bucket := findVirtualBucket(next.sortedSlots, start)
	if bucket == nil {
		bucket = &routeSlot{
			RouteID:           s.nextVirtualBucketID(),
			Start:             cloneBytes(start),
			End:               cloneBytes(end),
			Aggregate:         true,
			MemberRoutes:      []uint64{routeID},
			MemberRoutesTotal: 1,
			// Aggregates are never sub-divided (they already coarsen many
			// routes into one row) — a single counter bucket.
			subBuckets: make([]subCounter, 1),
		}
		next.sortedSlots = appendSorted(next.sortedSlots, bucket)
	} else {
		s.foldIntoBucket(next, bucket, routeID, start, end)
	}
	// The route is rejoining `bucket`; cancel any deferred prune for
	// this same routeID against this same bucket so it stays in
	// MemberRoutes. Prunes against other buckets are intentionally
	// left in place.
	s.cancelPendingPruneFor(bucket, routeID)
	next.virtualForRoute[routeID] = bucket
	s.table.Store(next)
	return false
}

// foldIntoBucket extends an existing virtual bucket to cover routeID's
// [start, end). Mutates bucket under its metaMu so a concurrent Flush
// iterating the previous table's snapshot doesn't observe a
// half-extended MemberRoutes slice or partially-updated Start/End.
// Counters live next to the metadata but are protected by their own
// atomic ops, not metaMu. If Start is lowered, sortedSlots is rebuilt
// to preserve Flush's key-order contract.
//
// MemberRoutes growth is capped by MaxMemberRoutesPerSlot — beyond
// that cap the bucket counters still absorb the route's traffic and
// the routeID is recorded in hiddenMembers (so dedup is correct on
// rejoin) but not appended to the visible MemberRoutes list.
func (s *MemSampler) foldIntoBucket(next *routeTable, bucket *routeSlot, routeID uint64, start, end []byte) {
	bucket.metaMu.Lock()
	addMemberToBucket(bucket, routeID, s.opts.MaxMemberRoutesPerSlot)
	if len(end) == 0 || (len(bucket.End) != 0 && bytesGT(end, bucket.End)) {
		bucket.End = cloneBytes(end)
	}
	startLowered := bytesLT(start, bucket.Start)
	if startLowered {
		bucket.Start = cloneBytes(start)
	}
	bucket.metaMu.Unlock()
	if startLowered {
		next.sortedSlots = rebuildSorted(next)
	}
}

// addMemberToBucket records routeID against bucket, choosing the
// visible MemberRoutes list when there's room and falling back to
// the hiddenMembers set past the cap. Both lists are deduped so a
// rejoin during the prune grace doesn't inflate MemberRoutesTotal.
// Caller holds bucket.metaMu.
func addMemberToBucket(bucket *routeSlot, routeID uint64, visibleCap int) {
	if memberRoutesContains(bucket.MemberRoutes, routeID) {
		return
	}
	if bucket.hiddenMembers != nil {
		if _, ok := bucket.hiddenMembers[routeID]; ok {
			return
		}
	}
	if len(bucket.MemberRoutes) < visibleCap {
		bucket.MemberRoutes = append(bucket.MemberRoutes, routeID)
	} else {
		if bucket.hiddenMembers == nil {
			bucket.hiddenMembers = make(map[uint64]struct{})
		}
		bucket.hiddenMembers[routeID] = struct{}{}
	}
	bucket.MemberRoutesTotal++
}

// RemoveRoute drops a RouteID from tracking. Counts accumulated since
// the last flush are NOT lost: the retired slot (or, for virtual-bucket
// members, just the membership entry) is queued for one final drain by
// the next Flush. Subsequent Observe(routeID, …) calls are silent
// no-ops. Idempotent.
func (s *MemSampler) RemoveRoute(routeID uint64) {
	if s == nil {
		return
	}
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	cur := s.table.Load()
	individual, isIndividual := cur.slots[routeID]
	bucket, isVirtual := cur.virtualForRoute[routeID]
	if !isIndividual && !isVirtual {
		return
	}

	next := copyRouteTable(cur)
	delete(next.slots, routeID)
	delete(next.virtualForRoute, routeID)

	retiredAt := s.now()
	switch {
	case isIndividual:
		// Pending counters in this slot must be harvested by upcoming
		// Flush cycles. Drain across the grace window so an Observe
		// call that loaded the prior table just before this
		// atomic.Store can still complete its Add into the slot — that
		// in-flight write is caught by a later drain rather than
		// silently lost.
		s.retiredMu.Lock()
		s.retiredSlots = append(s.retiredSlots, retiredSlot{slot: individual, retiredAt: retiredAt})
		s.retiredMu.Unlock()
	case isVirtual:
		// Defer pruning until after the bucket's pre-removal counters
		// have been drained. While the prune is pending the routeID
		// stays in MemberRoutes so the next few Flush rows attribute
		// the bucket's mixed counters to all members that contributed
		// to them — including the route we are removing.
		s.retiredMu.Lock()
		s.pendingPrunes = append(s.pendingPrunes, memberPrune{
			bucket: bucket, routeID: routeID, retiredAt: retiredAt,
		})
		// If this delete left the bucket with no remaining
		// virtualForRoute mapping, rebuildSorted will drop it from the
		// live sortedSlots. Queue it as a retired slot so Flush keeps
		// draining its counters across the grace window — otherwise
		// pre-removal increments and any in-flight Observe writers
		// hitting the prior table snapshot would be silently lost.
		if !bucketStillReferenced(next.virtualForRoute, bucket) {
			s.retiredSlots = append(s.retiredSlots, retiredSlot{
				slot: bucket, retiredAt: retiredAt,
			})
		}
		s.retiredMu.Unlock()
	}

	next.sortedSlots = rebuildSorted(next)
	s.table.Store(next)
}

// Flush drains every slot's counters with atomic.SwapUint64 and
// appends one MatrixColumn to the ring buffer. Idle slots (all
// counters zero) are skipped to keep the column compact. Slots that
// RemoveRoute retired since the previous Flush are drained alongside
// the live table, so route churn does not silently lose counts.
//
// Rows are emitted in Start-key order regardless of which slot list
// they came from (live, retired, or virtual-member-pruned), preserving
// the API contract that matrix consumers can rely on monotone Start
// across columns.
func (s *MemSampler) Flush() {
	if s == nil {
		return
	}
	col := MatrixColumn{At: s.now()}
	// Snapshot the per-group leader-term map once at the top of
	// Flush so every row in this column sees a consistent view, even
	// if SetLeaderTerm fires concurrently. Later rows can never
	// observe a *newer* term than earlier rows in the same column,
	// which preserves the merge-side invariant that all rows in a
	// (groupID, columnAt) tuple share a single leaderTerm value.
	terms := s.snapshotGroupTerms()
	tbl := s.table.Load()
	for _, slot := range tbl.sortedSlots {
		col.Rows = appendDrainedRow(col.Rows, slot, terms)
	}

	grace := s.graceWindow()
	s.retiredMu.Lock()
	s.retiredSlots = drainRetiredSlots(s.retiredSlots, &col.Rows, col.At, grace, terms)
	s.pendingPrunes = advancePendingPrunes(s.pendingPrunes, col.At, grace)
	s.retiredMu.Unlock()

	sort.SliceStable(col.Rows, func(i, j int) bool {
		return bytesLT(col.Rows[i].Start, col.Rows[j].Start)
	})

	s.historyMu.Lock()
	s.history.Push(col)
	s.historyMu.Unlock()
}

// drainRetiredSlots emits a row for each retired slot and returns the
// entries whose grace window has not yet elapsed. Rows are appended
// to *rows so the caller sees the slice growth. Entries whose
// elapsed time (now - retiredAt) has reached grace are dropped after
// this final drain. The dropped tail of the backing array is zeroed
// so released *routeSlot pointers do not stay GC-reachable through
// the reused capacity.
func drainRetiredSlots(retired []retiredSlot, rows *[]MatrixRow, now time.Time, grace time.Duration, terms map[uint64]uint64) []retiredSlot {
	keep := retired[:0]
	for _, r := range retired {
		*rows = appendDrainedRow(*rows, r.slot, terms)
		if now.Sub(r.retiredAt) < grace {
			keep = append(keep, r)
		}
	}
	clearTail(retired, len(keep))
	return keep
}

// advancePendingPrunes lets each pending member-prune live until its
// retiredAt+grace passes, then actually prunes the routeID from the
// bucket's MemberRoutes. Returns the entries still inside the grace
// window. Like drainRetiredSlots, the dropped tail is zeroed so
// released bucket pointers don't linger via the reused capacity.
func advancePendingPrunes(pending []memberPrune, now time.Time, grace time.Duration) []memberPrune {
	keep := pending[:0]
	for _, p := range pending {
		if now.Sub(p.retiredAt) < grace {
			keep = append(keep, p)
			continue
		}
		pruneMemberRoute(p.bucket, p.routeID)
	}
	clearPruneTail(pending, len(keep))
	return keep
}

// cancelPendingPruneFor drops any pendingPrune entry whose routeID
// AND bucket pointer both match — i.e. the same routeID is rejoining
// the same bucket it was just removed from. This stops a deferred
// prune from removing the routeID from MemberRoutes despite Observe
// still attributing traffic to it. Prunes against other buckets (or
// where the route rejoins as an individual slot) are left in place
// so the old bucket's MemberRoutes is correctly cleaned up.
//
// Holds retiredMu briefly off the hot path; callers must already
// hold routesMu so the cancellation pairs atomically with the
// route-table mutation.
func (s *MemSampler) cancelPendingPruneFor(bucket *routeSlot, routeID uint64) {
	s.retiredMu.Lock()
	defer s.retiredMu.Unlock()
	keep := s.pendingPrunes[:0]
	for _, p := range s.pendingPrunes {
		if p.routeID == routeID && p.bucket == bucket {
			continue
		}
		keep = append(keep, p)
	}
	clearPruneTail(s.pendingPrunes, len(keep))
	s.pendingPrunes = keep
}

// nextVirtualBucketID returns a synthetic RouteID for a brand-new
// virtual bucket. Synthetic IDs come from the high end of uint64 and
// decrement, so they cannot collide with real route IDs (which are
// assigned from the low end by the coordinator). Without this,
// stamping the bucket with the first folded real RouteID would mean
// that ID could later show up on TWO rows in the same column — one
// aggregate, one individual — if the original route is later
// re-registered as an individual slot.
func (s *MemSampler) nextVirtualBucketID() uint64 {
	// atomic.Uint64 starts at 0; adding ^uint64(0) wraps to MaxUint64
	// on the first call, MaxUint64-1 on the second, etc.
	return s.virtualIDCounter.Add(^uint64(0))
}

// reclaimRetiredSlot looks for a non-aggregate retired slot whose
// RouteID matches the supplied routeID and, if found, removes it
// from retiredSlots and returns it for reuse. This guarantees a
// route removed and re-registered inside the grace window is
// represented by a single *routeSlot — Flush would otherwise emit
// two rows with the same RouteID (one from the new live slot, one
// from the still-draining retired slot) and split counts across
// them. Aggregate (orphaned virtual bucket) entries are left in
// place because their RouteID lives in the synthetic namespace and
// a real-ID match against an aggregate would be coincidental.
func (s *MemSampler) reclaimRetiredSlot(routeID uint64) *routeSlot {
	s.retiredMu.Lock()
	defer s.retiredMu.Unlock()
	var reclaimed *routeSlot
	keep := s.retiredSlots[:0]
	for _, r := range s.retiredSlots {
		if reclaimed == nil && !r.slot.Aggregate && r.slot.RouteID == routeID {
			reclaimed = r.slot
			continue
		}
		keep = append(keep, r)
	}
	clearTail(s.retiredSlots, len(keep))
	s.retiredSlots = keep
	return reclaimed
}

// memberRoutesContains reports whether routeID is already listed in
// members. Used as a dedup guard so re-registering a routeID inside
// the prune grace window doesn't add a duplicate MemberRoutes entry.
func memberRoutesContains(members []uint64, routeID uint64) bool {
	for _, m := range members {
		if m == routeID {
			return true
		}
	}
	return false
}

// clearTail zeroes the [keepLen, len(s)) range of s so dropped entries
// don't keep their *routeSlot pointers GC-reachable through the
// reused backing array.
func clearTail(s []retiredSlot, keepLen int) {
	for i := keepLen; i < len(s); i++ {
		s[i] = retiredSlot{}
	}
}

// clearPruneTail is clearTail for the pendingPrunes queue.
func clearPruneTail(s []memberPrune, keepLen int) {
	for i := keepLen; i < len(s); i++ {
		s[i] = memberPrune{}
	}
}

// bucketStillReferenced reports whether any RouteID in
// virtualForRoute still maps to bucket. Used by RemoveRoute to detect
// when a virtual bucket has lost its last member and must be retired
// for grace draining.
func bucketStillReferenced(virtualForRoute map[uint64]*routeSlot, bucket *routeSlot) bool {
	for _, b := range virtualForRoute {
		if b == bucket {
			return true
		}
	}
	return false
}

// pruneMemberRoute removes routeID from bucket.MemberRoutes (or
// hiddenMembers) under the bucket's metaMu so a concurrent
// snapshotMeta reader sees a consistent view. MemberRoutesTotal is
// decremented whenever the routeID was actually present in either
// list — including past-cap members in hiddenMembers — so the
// reported route_count stays truthful across remove/re-register
// churn.
func pruneMemberRoute(bucket *routeSlot, routeID uint64) {
	bucket.metaMu.Lock()
	defer bucket.metaMu.Unlock()
	filtered := bucket.MemberRoutes[:0]
	removed := false
	for _, m := range bucket.MemberRoutes {
		if m == routeID {
			removed = true
			continue
		}
		filtered = append(filtered, m)
	}
	bucket.MemberRoutes = filtered
	if !removed && bucket.hiddenMembers != nil {
		if _, ok := bucket.hiddenMembers[routeID]; ok {
			delete(bucket.hiddenMembers, routeID)
			if len(bucket.hiddenMembers) == 0 {
				bucket.hiddenMembers = nil
			}
			removed = true
		}
	}
	if removed && bucket.MemberRoutesTotal > 0 {
		bucket.MemberRoutesTotal--
	}
}

// Step returns the configured flush interval after applying default
// fallbacks. Callers wiring up RunFlusher can use this to align their
// ticker with the sampler's expectations rather than passing the
// interval through two configuration paths.
func (s *MemSampler) Step() time.Duration {
	if s == nil {
		return DefaultStep
	}
	return s.opts.Step
}

// HistoryColumns returns the configured ring-buffer length after
// applying defaults and the MaxHistoryColumns clamp. Wiring tests use
// this to verify --keyvizHistoryColumns is forwarded end-to-end
// without exposing the internal opts struct.
func (s *MemSampler) HistoryColumns() int {
	if s == nil {
		return DefaultHistoryColumns
	}
	return s.opts.HistoryColumns
}

// appendDrainedRow swaps each non-empty sub-bucket's counters to zero
// and appends one MatrixRow per non-empty sub-bucket, with Start/End
// narrowed to that sub-bucket's bounds. Idle sub-buckets are skipped,
// so a route with one hot sub-range emits one row (not K). Slots that
// are not sub-divided (len(subBuckets) == 1) emit at most one row with
// the route's own bounds — today's behaviour.
//
// The route metadata (Start/End/Aggregate/MemberRoutes/GroupID) is read
// under the slot's metaMu via snapshotMeta so a concurrent RegisterRoute
// fold cannot race the read; the sub-range layout fields are immutable
// (read lock-free, see §4.2) and the counters are per-sub-bucket atomics.
func appendDrainedRow(rows []MatrixRow, slot *routeSlot, terms map[uint64]uint64) []MatrixRow {
	start, end, aggregate, members, membersTotal, groupID := slot.snapshotMeta()
	k := len(slot.subBuckets)
	for i := range slot.subBuckets {
		sc := &slot.subBuckets[i]
		reads := sc.reads.Swap(0)
		writes := sc.writes.Swap(0)
		readBytes := sc.readBytes.Swap(0)
		writeBytes := sc.writeBytes.Swap(0)
		if reads == 0 && writes == 0 && readBytes == 0 && writeBytes == 0 {
			continue
		}
		rowStart, rowEnd := start, end
		if k > 1 {
			rowStart, rowEnd = slot.subBucketBounds(i, start, end)
		}
		rows = append(rows, MatrixRow{
			RouteID:           slot.RouteID,
			RaftGroupID:       groupID,
			LeaderTerm:        terms[groupID],
			Start:             rowStart,
			End:               rowEnd,
			Aggregate:         aggregate,
			MemberRoutes:      members,
			MemberRoutesTotal: membersTotal,
			SubBucket:         i,
			SubBucketCount:    k,
			Reads:             reads,
			Writes:            writes,
			ReadBytes:         readBytes,
			WriteBytes:        writeBytes,
		})
	}
	return rows
}

// Snapshot returns the matrix columns in the supplied [from, to)
// half-open time range. Ordering is oldest-first. Series selects which
// MatrixRow value the caller is going to display; the slot metadata
// (RouteID, Start, End, Aggregate, MemberRoutes) is included on every
// row so a UI can render the same response with different series
// without a re-query.
func (s *MemSampler) Snapshot(from, to time.Time) []MatrixColumn {
	if s == nil {
		return nil
	}
	s.historyMu.Lock()
	defer s.historyMu.Unlock()
	return s.history.Range(from, to)
}

// helpers below -----------------------------------------------------

func copyRouteTable(src *routeTable) *routeTable {
	dst := &routeTable{
		slots:           make(map[uint64]*routeSlot, len(src.slots)+1),
		virtualForRoute: make(map[uint64]*routeSlot, len(src.virtualForRoute)+1),
		sortedSlots:     append([]*routeSlot(nil), src.sortedSlots...),
	}
	for k, v := range src.slots {
		dst.slots[k] = v
	}
	for k, v := range src.virtualForRoute {
		dst.virtualForRoute[k] = v
	}
	return dst
}

func appendSorted(slots []*routeSlot, slot *routeSlot) []*routeSlot {
	idx := sort.Search(len(slots), func(i int) bool {
		return bytesGE(slots[i].Start, slot.Start)
	})
	slots = append(slots, nil)
	copy(slots[idx+1:], slots[idx:])
	slots[idx] = slot
	return slots
}

func rebuildSorted(tbl *routeTable) []*routeSlot {
	out := make([]*routeSlot, 0, len(tbl.slots)+len(tbl.virtualForRoute))
	seen := make(map[*routeSlot]struct{}, cap(out))
	for _, s := range tbl.slots {
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, s := range tbl.virtualForRoute {
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytesLT(out[i].Start, out[j].Start)
	})
	return out
}

// findVirtualBucket returns the existing virtual bucket that covers
// start, preferring an exact range match. If no bucket contains
// start, returns the first aggregate in sortedSlots order so
// over-budget routes collapse into a single global bucket rather than
// fragmenting across many. Returns nil only when no virtual bucket
// exists yet — caller creates one in that case.
func findVirtualBucket(sorted []*routeSlot, start []byte) *routeSlot {
	for _, s := range sorted {
		if !s.Aggregate {
			continue
		}
		if bytesLE(s.Start, start) && (len(s.End) == 0 || bytesLT(start, s.End)) {
			return s
		}
	}
	for _, s := range sorted {
		if s.Aggregate {
			return s
		}
	}
	return nil
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func bytesLT(a, b []byte) bool { return bytes.Compare(a, b) < 0 }
func bytesLE(a, b []byte) bool { return bytes.Compare(a, b) <= 0 }
func bytesGT(a, b []byte) bool { return bytes.Compare(a, b) > 0 }
func bytesGE(a, b []byte) bool { return bytes.Compare(a, b) >= 0 }
