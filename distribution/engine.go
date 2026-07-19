package distribution

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

// Route represents a mapping from a key range to a raft group.
// Ranges are right half-open intervals: [Start, End). Start is inclusive and
// End is exclusive. A nil End denotes an unbounded interval extending to
// positive infinity.
type Route struct {
	// RouteID is the durable identifier assigned by route catalog.
	// Zero means ephemeral/non-catalog routes.
	RouteID uint64
	// Start marks the inclusive beginning of the range.
	Start []byte
	// End marks the exclusive end of the range. nil means unbounded.
	End []byte
	// GroupID identifies the raft group for the range starting at Start.
	GroupID uint64
	// State tracks control-plane state for this route.
	State RouteState
	// StagedVisibilityActive allows serving reads to merge staged migration rows.
	StagedVisibilityActive bool
	// MigrationJobID identifies the active staged migration job.
	MigrationJobID uint64
	// MinWriteTSExclusive rejects writes at or below the migration cutover floor.
	MinWriteTSExclusive uint64
	// Load tracks the number of accesses served by this range.
	Load uint64
}

// Engine holds in-memory metadata of routes and provides timestamp generation.
type Engine struct {
	mu             sync.RWMutex
	routes         []Route
	catalogVersion uint64
	ts             atomic.Uint64
	// history is the M2 versioned-snapshot ring for Composed-1
	// (docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
	// §M2).  Keyed by catalogVersion; populated on every successful
	// ApplySnapshot and seeded by NewEngineWithDefaultRoute so a
	// transaction that observed catalogVersion = 0 (the engine's
	// pre-bootstrap snapshot) can still resolve its read-set owner.
	// Bounded by historyDepth via a FIFO eviction list
	// (historyOrder).  At M2 the ring is plumbing only — M3 will
	// read from it via verifyComposed1.
	history      map[uint64]RouteHistorySnapshot
	historyOrder []uint64
	historyDepth int
}

// DefaultRouteHistoryDepth is the size of Engine's versioned-snapshot
// ring used by the Composed-1 M2 plumbing.  32 is conservative
// against current single-leader catalog churn (operator-frequency,
// not data-plane) per the design doc §9 Q2; raise if a future
// control plane generates more than ~tens of versions per second.
const DefaultRouteHistoryDepth = 32

const defaultGroupID uint64 = 1
const minRouteCountForOrderValidation = 2

var (
	ErrEngineSnapshotVersionStale = errors.New("engine snapshot version is stale")
	ErrEngineSnapshotDuplicateID  = errors.New("engine snapshot has duplicate route id")
	ErrEngineSnapshotRouteOverlap = errors.New("engine snapshot has overlapping routes")
	ErrEngineSnapshotRouteOrder   = errors.New("engine snapshot has invalid route order")
)

// NewEngine creates an Engine with no hotspot splitting.
func NewEngine() *Engine {
	return &Engine{
		routes:       make([]Route, 0),
		history:      make(map[uint64]RouteHistorySnapshot, DefaultRouteHistoryDepth),
		historyOrder: make([]uint64, 0, DefaultRouteHistoryDepth),
		historyDepth: DefaultRouteHistoryDepth,
	}
}

// NewEngineWithDefaultRoute creates an Engine and registers a default route
// covering the full keyspace with a default group ID.  The default route is
// also recorded in the M2 history ring as the version-0 snapshot so
// transactions that observed catalogVersion = 0 can resolve their read-set
// owner through SnapshotAt(0).
func NewEngineWithDefaultRoute() *Engine {
	engine := NewEngine()
	engine.UpdateRoute([]byte(""), nil, defaultGroupID)
	engine.recordHistorySnapshotLocked()
	return engine
}

// Version returns current route catalog version applied to the engine.
func (e *Engine) Version() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.catalogVersion
}

// ApplySnapshot atomically replaces all in-memory routes with the provided
// catalog snapshot when the snapshot version is newer.
func (e *Engine) ApplySnapshot(snapshot CatalogSnapshot) error {
	e.mu.RLock()
	currentVersion := e.catalogVersion
	if snapshot.Version <= currentVersion {
		e.mu.RUnlock()
		if snapshot.Version < currentVersion {
			return staleSnapshotVersionErr(snapshot.Version, currentVersion)
		}
		return nil
	}
	e.mu.RUnlock()

	routes, err := routesFromCatalog(snapshot.Routes)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if snapshot.Version <= e.catalogVersion {
		if snapshot.Version < e.catalogVersion {
			return staleSnapshotVersionErr(snapshot.Version, e.catalogVersion)
		}
		return nil
	}

	e.routes = routes
	e.catalogVersion = snapshot.Version
	e.recordHistorySnapshotLocked()
	return nil
}

// RouteHistorySnapshot is a point-in-time view of the route catalog at
// a specific version.  Returned by Engine.SnapshotAt for the M3
// Composed-1 commit-time gate.  Carries an immutable copy of the
// catalog's routes at the recorded version so a caller can resolve
// ownership without holding the Engine lock.
type RouteHistorySnapshot struct {
	version uint64
	routes  []Route
}

// Version returns the catalog version this snapshot was recorded at.
func (s RouteHistorySnapshot) Version() uint64 { return s.version }

// OwnerOf returns the Raft group ID that owned key at this snapshot's
// version.  Returns (0, false) when no route covers key (the
// pre-bootstrap state or an explicitly-uncovered range).  Mirrors
// Engine.GetRoute's right-half-open interval semantics but against
// the historical snapshot, not the live engine state.
//
// Routes are sorted by Start (recordHistorySnapshotLocked clones from
// e.routes, which Engine.UpdateRoute / routesFromCatalog keep sorted),
// so the scan can break the moment key < r.Start — every later route
// has a strictly greater Start and cannot cover key either.  This
// matters because M3 puts OwnerOf on every txn commit's apply path
// (claude review on PR #894 — break-vs-continue lifts the worst-case
// scan from O(N) to "first non-covering gap" without changing the
// resolution semantics).
func (s RouteHistorySnapshot) OwnerOf(key []byte) (uint64, bool) {
	for _, r := range s.routes {
		if bytes.Compare(key, r.Start) < 0 {
			break
		}
		if r.End != nil && bytes.Compare(key, r.End) >= 0 {
			continue
		}
		return r.GroupID, true
	}
	return 0, false
}

// RouteOf returns the route that covered key at this snapshot's version.
func (s RouteHistorySnapshot) RouteOf(key []byte) (Route, bool) {
	for _, r := range s.routes {
		if bytes.Compare(key, r.Start) < 0 {
			break
		}
		if r.End != nil && bytes.Compare(key, r.End) >= 0 {
			continue
		}
		return cloneRoute(r), true
	}
	return Route{}, false
}

// IntersectingRoutes returns every route whose range intersects [start, end)
// in this snapshot. A nil end denotes +infinity.
func (s RouteHistorySnapshot) IntersectingRoutes(start, end []byte) []Route {
	out := make([]Route, 0)
	for _, r := range s.routes {
		if r.End != nil && bytes.Compare(r.End, start) <= 0 {
			continue
		}
		if end != nil && bytes.Compare(r.Start, end) >= 0 {
			break
		}
		out = append(out, cloneRoute(r))
	}
	return out
}

// Current returns the route catalog snapshot at the engine's current
// catalogVersion.  Returns (zero, false) when the history ring has
// not been initialised (bare-struct Engine).  Used by the M3
// Composed-1 cross-version-read fence (design doc §4.4) — the gate
// compares the txn's observed-version owner against the current
// owner so a route shift between BeginTxn and Commit is caught
// before it can produce a G1c anomaly across a cross-group
// MoveRange / SplitRange.
func (e *Engine) Current() (RouteHistorySnapshot, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	snap, ok := e.history[e.catalogVersion]
	return snap, ok
}

// SnapshotAt returns the route catalog snapshot recorded at version v.
// Returns (zero, false) when v is not in the ring — either because v
// is in the future (> catalogVersion), or because the FIFO ring has
// evicted v (it was older than the historyDepth-most-recent
// versions).  The M3 Composed-1 gate (design doc §4.3) treats the
// not-found case as a hard error and triggers a coordinator retry,
// so retention depth is a liveness knob, not a safety knob.
func (e *Engine) SnapshotAt(v uint64) (RouteHistorySnapshot, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	snap, ok := e.history[v]
	return snap, ok
}

// SetHistoryDepthForTest overrides the FIFO ring depth from outside
// the package.  Test-only.  Callers should set the depth before
// sharing the Engine with concurrent SnapshotAt/Current readers to
// avoid interleaving surprises around the eviction watermark, but
// the write itself is lock-protected (e.mu.Lock below) so it is
// safe to call from any goroutine that does not also expect a
// consistent SnapshotAt view across the depth change.
//
// Exists so tests in the kv package can drive eviction-trigger
// scenarios without adding a constructor option just for tests
// (claude review on PR #894).  Production code must use
// DefaultRouteHistoryDepth (32) or a future operator-exposed
// config knob.
//
// Fails fast on depth <= 0 (coderabbit minor on PR #895):
// recordHistorySnapshotLocked's eviction path indexes
// historyOrder[0], so a zero/negative depth would surface as a
// confusing index-out-of-range deep in the apply path instead of
// at the misconfigured test seam.  When shrinking depth below the
// current ring size, evict the excess oldest entries immediately
// rather than letting the next record see len(historyOrder) >
// historyDepth (gemini medium on PR #895 — without this trim, the
// next recordHistorySnapshotLocked's
// `make([]uint64, len-1, historyDepth)` would panic on len-1 >
// historyDepth).
func (e *Engine) SetHistoryDepthForTest(depth int) {
	if depth <= 0 {
		panic("SetHistoryDepthForTest: depth must be > 0")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.historyDepth = depth
	if len(e.historyOrder) > depth {
		excess := len(e.historyOrder) - depth
		for i := range excess {
			delete(e.history, e.historyOrder[i])
		}
		retained := make([]uint64, depth)
		copy(retained, e.historyOrder[excess:])
		e.historyOrder = retained
	}
}

// HistoryDepth returns the configured ring depth for diagnostics.
func (e *Engine) HistoryDepth() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.historyDepth
}

// recordHistorySnapshotLocked pushes the current (catalogVersion,
// routes) pair into the ring.  The `Locked` suffix is the Go
// convention for "caller MUST hold the receiver's lock" — checked
// by reviewers via name, not by the runtime (claude review on PR
// #894 — fragile lock contract).  Invoked from ApplySnapshot under
// the write lock and from NewEngineWithDefaultRoute before the
// Engine is shared with any concurrent reader.  Idempotent on
// re-record at the same version.
func (e *Engine) recordHistorySnapshotLocked() {
	if e.history == nil {
		// Engines constructed via the bare struct literal (e.g.
		// internal test seams) — no history ring configured.  Skip
		// the record so the M2 plumbing stays optional for those
		// paths; the M3 gate will observe SnapshotAt → (zero,
		// false) and trigger the soft-fail-as-retry path.
		return
	}
	if _, exists := e.history[e.catalogVersion]; exists {
		return
	}
	if len(e.historyOrder) >= e.historyDepth {
		evict := e.historyOrder[0]
		// Copy the retained tail into fresh storage rather than
		// reslicing.  `historyOrder[1:]` only advances the slice
		// header — the head of the original backing array stays
		// alive and grows unboundedly across evictions.  At depth=32
		// this is small, but the FIFO eviction is the only place
		// the array grows, and the compaction is free (single
		// allocation, single copy of <=historyDepth entries —
		// claude review on PR #894 — backing-array leak).
		retained := make([]uint64, len(e.historyOrder)-1, e.historyDepth)
		copy(retained, e.historyOrder[1:])
		e.historyOrder = retained
		delete(e.history, evict)
	}
	cloned := make([]Route, len(e.routes))
	copy(cloned, e.routes)
	e.history[e.catalogVersion] = RouteHistorySnapshot{
		version: e.catalogVersion,
		routes:  cloned,
	}
	e.historyOrder = append(e.historyOrder, e.catalogVersion)
}

func staleSnapshotVersionErr(snapshotVersion, currentVersion uint64) error {
	return errors.Wrapf(
		ErrEngineSnapshotVersionStale,
		"snapshot version %d is older than engine catalog version %d",
		snapshotVersion,
		currentVersion,
	)
}

// UpdateRoute registers or updates a route for the given key range.
// Routes are stored sorted by Start.
func (e *Engine) UpdateRoute(start, end []byte, group uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, Route{
		Start:   start,
		End:     end,
		GroupID: group,
		State:   RouteStateActive,
	})
	sort.Slice(e.routes, func(i, j int) bool {
		return bytes.Compare(e.routes[i].Start, e.routes[j].Start) < 0
	})
}

// GetRoute finds a route for the given key using right half-open intervals.
func (e *Engine) GetRoute(key []byte) (Route, bool) {
	route, _, ok := e.GetRouteWithVersion(key)
	return route, ok
}

// GetRouteWithVersion finds a route and returns the catalog version from the
// same locked snapshot. Callers can use the version as a read-routing fence.
func (e *Engine) GetRouteWithVersion(key []byte) (Route, uint64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	idx := e.routeIndex(key)
	if idx < 0 {
		return Route{}, e.catalogVersion, false
	}
	route := e.routes[idx]
	route.Start = CloneBytes(route.Start)
	route.End = CloneBytes(route.End)
	return route, e.catalogVersion, true
}

// NextTimestamp returns a monotonic increasing timestamp.
func (e *Engine) NextTimestamp() uint64 {
	return e.ts.Add(1)
}

// Stats returns a snapshot of current ranges and their load counters.
func (e *Engine) Stats() []Route {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := make([]Route, len(e.routes))
	for i, r := range e.routes {
		stats[i] = Route{
			RouteID:                r.RouteID,
			Start:                  CloneBytes(r.Start),
			End:                    CloneBytes(r.End),
			GroupID:                r.GroupID,
			State:                  r.State,
			StagedVisibilityActive: r.StagedVisibilityActive,
			MigrationJobID:         r.MigrationJobID,
			MinWriteTSExclusive:    r.MinWriteTSExclusive,
			Load:                   r.Load,
		}
	}
	return stats
}

// GetIntersectingRoutes returns all routes whose key ranges intersect with [start, end).
// A route [rStart, rEnd) intersects with [start, end) if:
// - rStart < end (or end is nil, meaning unbounded scan)
// - start < rEnd (or rEnd is nil, meaning unbounded route)
func (e *Engine) GetIntersectingRoutes(start, end []byte) []Route {
	routes, _ := e.GetIntersectingRoutesWithVersion(start, end)
	return routes
}

// GetIntersectingRoutesWithVersion returns intersecting routes and the catalog
// version from the same locked snapshot.
func (e *Engine) GetIntersectingRoutesWithVersion(start, end []byte) ([]Route, uint64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Route
	for i := range e.routes {
		r := &e.routes[i]
		// Check if route intersects with [start, end)
		// Route ends before scan starts: rEnd != nil && rEnd <= start
		if r.End != nil && bytes.Compare(r.End, start) <= 0 {
			continue
		}
		// Route starts at or after scan ends: end != nil && rStart >= end
		if end != nil && bytes.Compare(r.Start, end) >= 0 {
			break
		}
		// Route intersects with scan range
		result = append(result, Route{
			RouteID:                r.RouteID,
			Start:                  CloneBytes(r.Start),
			End:                    CloneBytes(r.End),
			GroupID:                r.GroupID,
			State:                  r.State,
			StagedVisibilityActive: r.StagedVisibilityActive,
			MigrationJobID:         r.MigrationJobID,
			MinWriteTSExclusive:    r.MinWriteTSExclusive,
			Load:                   r.Load,
		})
	}
	return result, e.catalogVersion
}

func cloneRoute(r Route) Route {
	return Route{
		RouteID:                r.RouteID,
		Start:                  CloneBytes(r.Start),
		End:                    CloneBytes(r.End),
		GroupID:                r.GroupID,
		State:                  r.State,
		StagedVisibilityActive: r.StagedVisibilityActive,
		MigrationJobID:         r.MigrationJobID,
		MinWriteTSExclusive:    r.MinWriteTSExclusive,
		Load:                   r.Load,
	}
}

func (e *Engine) routeIndex(key []byte) int {
	if len(e.routes) == 0 {
		return -1
	}
	i := sort.Search(len(e.routes), func(i int) bool {
		return bytes.Compare(e.routes[i].Start, key) > 0
	})
	if i == 0 {
		return -1
	}
	i--
	if end := e.routes[i].End; end != nil && bytes.Compare(key, end) >= 0 {
		return -1
	}
	return i
}

func routesFromCatalog(routes []RouteDescriptor) ([]Route, error) {
	if len(routes) == 0 {
		return []Route{}, nil
	}

	out := make([]Route, len(routes))
	seen := make(map[uint64]struct{}, len(routes))
	for i, rd := range routes {
		if err := validateRouteDescriptor(rd); err != nil {
			return nil, err
		}
		if _, exists := seen[rd.RouteID]; exists {
			return nil, errors.WithStack(ErrEngineSnapshotDuplicateID)
		}
		seen[rd.RouteID] = struct{}{}
		out[i] = Route{
			RouteID:                rd.RouteID,
			Start:                  CloneBytes(rd.Start),
			End:                    CloneBytes(rd.End),
			GroupID:                rd.GroupID,
			State:                  rd.State,
			StagedVisibilityActive: rd.StagedVisibilityActive,
			MigrationJobID:         rd.MigrationJobID,
			MinWriteTSExclusive:    rd.MinWriteTSExclusive,
			Load:                   0,
		}
	}

	// Engine assumes valid snapshots have unique Start keys.
	// Therefore, unlike routeDescriptorLess in catalog, sort by Start only.
	// If an invalid snapshot is supplied (duplicate Start or bad order),
	// validateRouteOrder rejects it after sorting.
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Start, out[j].Start) < 0
	})
	if err := validateRouteOrder(out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateRouteOrder(routes []Route) error {
	if len(routes) < minRouteCountForOrderValidation {
		return nil
	}
	for i := 1; i < len(routes); i++ {
		prev := routes[i-1]
		curr := routes[i]

		if bytes.Compare(prev.Start, curr.Start) >= 0 {
			return errors.WithStack(ErrEngineSnapshotRouteOrder)
		}
		if prev.End == nil {
			return errors.WithStack(ErrEngineSnapshotRouteOrder)
		}
		// Adjacent routes where prev.End == curr.Start are valid in [Start, End).
		// Only prev.End > curr.Start indicates an overlap.
		if bytes.Compare(prev.End, curr.Start) > 0 {
			return errors.WithStack(ErrEngineSnapshotRouteOverlap)
		}
	}
	return nil
}

// CloneBytes returns a copied byte slice.
func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
