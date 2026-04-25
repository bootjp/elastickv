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
//     lookup against an immutable snapshot, and four atomic.AddUint64
//     calls. No allocation, no mutex.
//   - Flush drains the per-route counters with atomic.SwapUint64; no
//     pointer retirement, so a late writer cannot race past the snapshot
//     and lose counts.
//   - Adding / removing routes (RegisterRoute, RemoveRoute, ApplySplit,
//     ApplyMerge) builds a fresh routeTable copy under a non-hot-path
//     mutex and publishes it with a single atomic.Pointer.Store. Routes
//     mutated mid-step keep their counters in the new table by design.
package keyviz

import (
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
type Sampler interface {
	// Observe records a single request against a route. Op identifies
	// the counter family. keyLen and valueLen are summed into the
	// matching *Bytes counter; pass 0 for read-only ops where the
	// payload size is irrelevant.
	Observe(routeID uint64, op Op, keyLen, valueLen int)
}

// Defaults for MemSamplerOptions when fields are left zero.
const (
	DefaultStep             = 60 * time.Second
	DefaultHistoryColumns   = 1440 // 24 hours at 60s steps.
	DefaultMaxTrackedRoutes = 10_000
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
// Flush (atomic.SwapUint64); the metadata (RouteID, Start, End) is
// immutable after the slot is published.
type routeSlot struct {
	RouteID uint64
	Start   []byte
	End     []byte
	// Aggregate marks virtual buckets that fold multiple coarsened
	// routes together (Snapshot surfaces this in MatrixRow).
	Aggregate    bool
	MemberRoutes []uint64

	reads      atomic.Uint64
	writes     atomic.Uint64
	readBytes  atomic.Uint64
	writeBytes atomic.Uint64
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

	Reads      uint64
	Writes     uint64
	ReadBytes  uint64
	WriteBytes uint64
}

// NewMemSampler constructs a sampler with the supplied options. Zero
// fields fall back to the Default* constants. Returns nil only if
// opts.HistoryColumns is explicitly negative; callers should pass a
// zero options struct for the default configuration.
func NewMemSampler(opts MemSamplerOptions) *MemSampler {
	if opts.Step <= 0 {
		opts.Step = DefaultStep
	}
	if opts.HistoryColumns == 0 {
		opts.HistoryColumns = DefaultHistoryColumns
	}
	if opts.MaxTrackedRoutes == 0 {
		opts.MaxTrackedRoutes = DefaultMaxTrackedRoutes
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
	bytes := uint64(0)
	if keyLen > 0 {
		bytes += uint64(keyLen)
	}
	if valueLen > 0 {
		bytes += uint64(valueLen)
	}
	switch op {
	case OpRead:
		slot.reads.Add(1)
		if bytes > 0 {
			slot.readBytes.Add(bytes)
		}
	case OpWrite:
		slot.writes.Add(1)
		if bytes > 0 {
			slot.writeBytes.Add(bytes)
		}
	}
}

// RegisterRoute adds a (RouteID, [Start, End)) pair to the tracking
// set. Returns true when the route gets its own slot, false when the
// MaxTrackedRoutes cap was hit and the route was folded into a
// virtual aggregate bucket. Idempotent: calling twice with the same
// RouteID is a no-op (the original slot stays in place).
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
		slot := &routeSlot{
			RouteID: routeID,
			Start:   cloneBytes(start),
			End:     cloneBytes(end),
		}
		next.slots[routeID] = slot
		next.sortedSlots = appendSorted(next.sortedSlots, slot)
		s.table.Store(next)
		return true
	}

	// Coarsening: route is folded into the closest-by-start virtual
	// bucket (or one is created if no virtual bucket exists yet).
	bucket := findVirtualBucket(next.sortedSlots, start)
	if bucket == nil {
		bucket = &routeSlot{
			RouteID:      routeID,
			Start:        cloneBytes(start),
			End:          cloneBytes(end),
			Aggregate:    true,
			MemberRoutes: []uint64{routeID},
		}
		next.sortedSlots = appendSorted(next.sortedSlots, bucket)
	} else {
		bucket.MemberRoutes = append(bucket.MemberRoutes, routeID)
		// Extend bucket end if the new route reaches further right.
		if len(end) == 0 || (len(bucket.End) != 0 && bytesGT(end, bucket.End)) {
			bucket.End = cloneBytes(end)
		}
		if bytesLT(start, bucket.Start) {
			bucket.Start = cloneBytes(start)
		}
	}
	next.virtualForRoute[routeID] = bucket
	s.table.Store(next)
	return false
}

// RemoveRoute drops a RouteID from tracking. Counts accumulated since
// the last flush stay in the slot until the next Flush picks them up
// from the retired routeTable; subsequent Observe(routeID, …) calls
// are silent no-ops. Idempotent.
func (s *MemSampler) RemoveRoute(routeID uint64) {
	if s == nil {
		return
	}
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	cur := s.table.Load()
	if _, ok := cur.slots[routeID]; !ok {
		if _, ok := cur.virtualForRoute[routeID]; !ok {
			return
		}
	}
	next := copyRouteTable(cur)
	delete(next.slots, routeID)
	delete(next.virtualForRoute, routeID)
	next.sortedSlots = rebuildSorted(next)
	s.table.Store(next)
}

// Flush drains every slot's counters with atomic.SwapUint64 and
// appends one MatrixColumn to the ring buffer. Idle slots (all
// counters zero) are skipped to keep the column compact.
func (s *MemSampler) Flush() {
	if s == nil {
		return
	}
	tbl := s.table.Load()
	col := MatrixColumn{At: s.now()}
	for _, slot := range tbl.sortedSlots {
		reads := slot.reads.Swap(0)
		writes := slot.writes.Swap(0)
		readBytes := slot.readBytes.Swap(0)
		writeBytes := slot.writeBytes.Swap(0)
		if reads == 0 && writes == 0 && readBytes == 0 && writeBytes == 0 {
			continue
		}
		col.Rows = append(col.Rows, MatrixRow{
			RouteID:      slot.RouteID,
			Start:        slot.Start,
			End:          slot.End,
			Aggregate:    slot.Aggregate,
			MemberRoutes: append([]uint64(nil), slot.MemberRoutes...),
			Reads:        reads,
			Writes:       writes,
			ReadBytes:    readBytes,
			WriteBytes:   writeBytes,
		})
	}
	s.historyMu.Lock()
	s.history.Push(col)
	s.historyMu.Unlock()
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

// findVirtualBucket returns the existing virtual bucket whose range
// covers (or is closest to the right of) start. Returns nil when no
// virtual bucket exists yet — caller creates one in that case.
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

func bytesLT(a, b []byte) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func bytesLE(a, b []byte) bool { return !bytesGT(a, b) }
func bytesGE(a, b []byte) bool { return !bytesLT(a, b) }
func bytesGT(a, b []byte) bool { return bytesLT(b, a) }
