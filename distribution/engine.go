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
	// Load tracks the number of accesses served by this range.
	Load uint64
}

// Engine holds in-memory metadata of routes and provides timestamp generation.
type Engine struct {
	mu               sync.RWMutex
	routes           []Route
	catalogVersion   uint64
	ts               uint64
	hotspotThreshold uint64
}

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
	return NewEngineWithThreshold(0)
}

// NewEngineWithDefaultRoute creates an Engine and registers a default route
// covering the full keyspace with a default group ID.
func NewEngineWithDefaultRoute() *Engine {
	engine := NewEngine()
	engine.UpdateRoute([]byte(""), nil, defaultGroupID)
	return engine
}

// NewEngineWithThreshold creates an Engine and sets a threshold for hotspot
// detection. A non-zero threshold enables automatic range splitting when the
// number of accesses to a range exceeds the threshold.
func NewEngineWithThreshold(threshold uint64) *Engine {
	return &Engine{routes: make([]Route, 0), hotspotThreshold: threshold}
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
	return nil
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	idx := e.routeIndex(key)
	if idx < 0 {
		return Route{}, false
	}
	return e.routes[idx], true
}

// NextTimestamp returns a monotonic increasing timestamp.
func (e *Engine) NextTimestamp() uint64 {
	return atomic.AddUint64(&e.ts, 1)
}

// RecordAccess increases the access counter for the range containing key and
// splits the range if it turns into a hotspot. The load counter is updated
// atomically under a read lock to allow concurrent access recording. If the
// hotspot threshold is exceeded, RecordAccess acquires a full write lock and
// re-checks the condition before splitting to avoid races with concurrent
// splits.
func (e *Engine) RecordAccess(key []byte) {
	e.mu.RLock()
	idx := e.routeIndex(key)
	if idx < 0 {
		e.mu.RUnlock()
		return
	}
	load := atomic.AddUint64(&e.routes[idx].Load, 1)
	threshold := e.hotspotThreshold
	e.mu.RUnlock()
	if threshold == 0 || load < threshold {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	idx = e.routeIndex(key)
	if idx < 0 {
		return
	}
	if e.routes[idx].Load >= threshold {
		e.splitRange(idx)
	}
}

// Stats returns a snapshot of current ranges and their load counters.
func (e *Engine) Stats() []Route {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := make([]Route, len(e.routes))
	for i, r := range e.routes {
		stats[i] = Route{
			RouteID: r.RouteID,
			Start:   cloneBytes(r.Start),
			End:     cloneBytes(r.End),
			GroupID: r.GroupID,
			State:   r.State,
			Load:    atomic.LoadUint64(&e.routes[i].Load),
		}
	}
	return stats
}

// GetIntersectingRoutes returns all routes whose key ranges intersect with [start, end).
// A route [rStart, rEnd) intersects with [start, end) if:
// - rStart < end (or end is nil, meaning unbounded scan)
// - start < rEnd (or rEnd is nil, meaning unbounded route)
func (e *Engine) GetIntersectingRoutes(start, end []byte) []Route {
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
			continue
		}
		// Route intersects with scan range
		result = append(result, Route{
			RouteID: r.RouteID,
			Start:   cloneBytes(r.Start),
			End:     cloneBytes(r.End),
			GroupID: r.GroupID,
			State:   r.State,
			Load:    atomic.LoadUint64(&r.Load),
		})
	}
	return result
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

func (e *Engine) splitRange(idx int) {
	r := e.routes[idx]
	if r.End == nil {
		// cannot split unbounded range; reset load to avoid repeated attempts
		e.routes[idx].Load = 0
		return
	}
	mid := midpoint(r.Start, r.End)
	if mid == nil {
		// cannot determine midpoint; reset load to avoid repeated attempts
		e.routes[idx].Load = 0
		return
	}
	// Auto-split routes are ephemeral in-memory entries until persisted through
	// catalog operations, so keep RouteID zero here.
	left := Route{RouteID: 0, Start: r.Start, End: mid, GroupID: r.GroupID, State: RouteStateActive}
	right := Route{RouteID: 0, Start: mid, End: r.End, GroupID: r.GroupID, State: RouteStateActive}
	// replace the range at idx with left and right in an idiomatic manner
	e.routes = append(e.routes[:idx+1], e.routes[idx:]...)
	e.routes[idx] = left
	e.routes[idx+1] = right
}

func routesFromCatalog(routes []RouteDescriptor) ([]Route, error) {
	if len(routes) == 0 {
		return []Route{}, nil
	}

	out := make([]Route, 0, len(routes))
	seen := make(map[uint64]struct{}, len(routes))
	for _, rd := range routes {
		if err := validateRouteDescriptor(rd); err != nil {
			return nil, err
		}
		if _, exists := seen[rd.RouteID]; exists {
			return nil, errors.WithStack(ErrEngineSnapshotDuplicateID)
		}
		seen[rd.RouteID] = struct{}{}
		out = append(out, Route{
			RouteID: rd.RouteID,
			Start:   cloneBytes(rd.Start),
			End:     cloneBytes(rd.End),
			GroupID: rd.GroupID,
			State:   rd.State,
			Load:    0,
		})
	}

	// Keep deterministic ordering consistent with catalog route ordering.
	// Duplicate Start keys are still rejected by validateRouteOrder.
	sort.Slice(out, func(i, j int) bool {
		return engineRouteLess(out[i], out[j])
	})
	if err := validateRouteOrder(out); err != nil {
		return nil, err
	}
	return out, nil
}

func engineRouteLess(left, right Route) bool {
	if c := bytes.Compare(left.Start, right.Start); c != 0 {
		return c < 0
	}
	if left.End == nil && right.End != nil {
		return false
	}
	if left.End != nil && right.End == nil {
		return true
	}
	if left.End != nil && right.End != nil {
		if c := bytes.Compare(left.End, right.End); c != 0 {
			return c < 0
		}
	}
	return left.RouteID < right.RouteID
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

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// midpoint returns a key that is lexicographically between a and b. It returns
// nil if such a key cannot be determined (e.g. a and b are too close).
func midpoint(a, b []byte) []byte {
	m := append(cloneBytes(a), 0)
	if bytes.Compare(m, b) >= 0 {
		return nil
	}
	return m
}
