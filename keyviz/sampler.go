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
	// the counter family. keyLen and valueLen are summed into the
	// matching *Bytes counter; pass 0 for read-only ops where the
	// payload size is irrelevant. Implementations must no-op (not
	// panic) when invoked on a typed-nil receiver.
	Observe(routeID uint64, op Op, keyLen, valueLen int)
}

// Defaults for MemSamplerOptions when fields are left zero.
const (
	DefaultStep                   = 60 * time.Second
	DefaultHistoryColumns         = 1440 // 24 hours at 60s steps.
	DefaultMaxTrackedRoutes       = 10_000
	DefaultMaxMemberRoutesPerSlot = 256
)

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
	Start   []byte
	End     []byte
	// Aggregate marks virtual buckets that fold multiple coarsened
	// routes together (Snapshot surfaces this in MatrixRow).
	Aggregate    bool
	MemberRoutes []uint64
	// MemberRoutesTotal counts every distinct routeID that has folded
	// into this bucket, including ones beyond MaxMemberRoutesPerSlot
	// (which still contribute to the counters but are not appended to
	// MemberRoutes). Always equals len(MemberRoutes) for individual
	// (non-Aggregate) slots.
	MemberRoutesTotal uint64

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
func (s *routeSlot) snapshotMeta() (start, end []byte, aggregate bool, members []uint64, membersTotal uint64) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	start = cloneBytes(s.Start)
	end = cloneBytes(s.End)
	aggregate = s.Aggregate
	if len(s.MemberRoutes) > 0 {
		members = append([]uint64(nil), s.MemberRoutes...)
	}
	membersTotal = s.MemberRoutesTotal
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
	if opts.MaxTrackedRoutes <= 0 {
		opts.MaxTrackedRoutes = DefaultMaxTrackedRoutes
	}
	if opts.MaxMemberRoutesPerSlot <= 0 {
		opts.MaxMemberRoutesPerSlot = DefaultMaxMemberRoutesPerSlot
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	s := &MemSampler{
		opts:    opts,
		now:     now,
		history: newRingBuffer(opts.HistoryColumns),
	}
	s.table.Store(newEmptyRouteTable())
	return s
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
func (s *MemSampler) Observe(routeID uint64, op Op, keyLen, valueLen int) {
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
	byteCount := uint64(0)
	if keyLen > 0 {
		byteCount += uint64(keyLen)
	}
	if valueLen > 0 {
		byteCount += uint64(valueLen)
	}
	switch op {
	case OpRead:
		slot.reads.Add(1)
		if byteCount > 0 {
			slot.readBytes.Add(byteCount)
		}
	case OpWrite:
		slot.writes.Add(1)
		if byteCount > 0 {
			slot.writeBytes.Add(byteCount)
		}
	}
}

// RegisterRoute adds a (RouteID, [Start, End)) pair to the tracking
// set. Returns true when the route gets its own slot, false when the
// MaxTrackedRoutes cap was hit and the route was folded into a
// virtual aggregate bucket. Idempotent: calling twice with the same
// RouteID is a no-op (the original slot stays in place).
//
// If a previous RemoveRoute(routeID) queued a deferred member-prune
// and the route is now re-registered into the SAME bucket inside the
// grace window, that prune is cancelled — otherwise the routeID
// would disappear from bucket.MemberRoutes despite Observe still
// attributing fresh traffic to it. Prunes for different buckets (or
// when the route rejoins as an individual slot) are left alone so
// the old bucket's MemberRoutes is correctly cleaned up.
func (s *MemSampler) RegisterRoute(routeID uint64, start, end []byte) bool {
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
				Start:             cloneBytes(start),
				End:               cloneBytes(end),
				MemberRoutesTotal: 1,
			}
		} else {
			// Re-registering the same routeID inside the grace window:
			// reuse the retired slot so any in-flight Observe writers
			// hitting the prior table snapshot land on the same slot
			// the new traffic uses, and Flush emits a single row per
			// RouteID instead of two (one live + one retired) with
			// counts split across them. Refresh the metadata fields to
			// match the new registration; counters are preserved.
			slot.metaMu.Lock()
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
// that cap the bucket counters still absorb the route's traffic, but
// the routeID is not added to the visible member list.
func (s *MemSampler) foldIntoBucket(next *routeTable, bucket *routeSlot, routeID uint64, start, end []byte) {
	bucket.metaMu.Lock()
	if !memberRoutesContains(bucket.MemberRoutes, routeID) {
		bucket.MemberRoutesTotal++
		if len(bucket.MemberRoutes) < s.opts.MaxMemberRoutesPerSlot {
			bucket.MemberRoutes = append(bucket.MemberRoutes, routeID)
		}
	}
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
	tbl := s.table.Load()
	for _, slot := range tbl.sortedSlots {
		col.Rows = appendDrainedRow(col.Rows, slot)
	}

	grace := s.graceWindow()
	s.retiredMu.Lock()
	s.retiredSlots = drainRetiredSlots(s.retiredSlots, &col.Rows, col.At, grace)
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
func drainRetiredSlots(retired []retiredSlot, rows *[]MatrixRow, now time.Time, grace time.Duration) []retiredSlot {
	keep := retired[:0]
	for _, r := range retired {
		*rows = appendDrainedRow(*rows, r.slot)
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

// pruneMemberRoute removes routeID from bucket.MemberRoutes under the
// bucket's metaMu so a concurrent snapshotMeta reader sees a
// consistent view. MemberRoutesTotal is decremented when the routeID
// was visible in MemberRoutes (the only case we can confidently
// account for) — routes pruned past the visible cap stay in the
// total because we don't track individual past-cap members.
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

// appendDrainedRow swaps the slot's counters to zero and appends a
// MatrixRow when any counter was non-zero. Idle slots are skipped.
// Metadata is read under the slot's metaMu so a concurrent
// RegisterRoute fold cannot race with the row materialisation.
func appendDrainedRow(rows []MatrixRow, slot *routeSlot) []MatrixRow {
	reads := slot.reads.Swap(0)
	writes := slot.writes.Swap(0)
	readBytes := slot.readBytes.Swap(0)
	writeBytes := slot.writeBytes.Swap(0)
	if reads == 0 && writes == 0 && readBytes == 0 && writeBytes == 0 {
		return rows
	}
	start, end, aggregate, members, membersTotal := slot.snapshotMeta()
	return append(rows, MatrixRow{
		RouteID:           slot.RouteID,
		Start:             start,
		End:               end,
		Aggregate:         aggregate,
		MemberRoutes:      members,
		MemberRoutesTotal: membersTotal,
		Reads:             reads,
		Writes:            writes,
		ReadBytes:         readBytes,
		WriteBytes:        writeBytes,
	})
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
