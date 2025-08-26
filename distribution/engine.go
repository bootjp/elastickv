package distribution

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
)

// Route represents a mapping from a key range to a raft group.
// Ranges are right half-open intervals: [Start, End). Start is inclusive and
// End is exclusive. A nil End denotes an unbounded interval extending to
// positive infinity.
type Route struct {
	// Start marks the inclusive beginning of the range.
	Start []byte
	// End marks the exclusive end of the range. nil means unbounded.
	End []byte
	// GroupID identifies the raft group for the range starting at Start.
	GroupID uint64
}

// Engine holds in-memory metadata of routes and provides timestamp generation.
type Engine struct {
	mu     sync.RWMutex
	routes []Route
	ts     uint64
}

// NewEngine creates an Engine.
func NewEngine() *Engine {
	return &Engine{routes: make([]Route, 0)}
}

// UpdateRoute registers or updates a route for the given key range.
// Routes are stored sorted by Start.
func (e *Engine) UpdateRoute(start, end []byte, group uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, Route{Start: start, End: end, GroupID: group})
	sort.Slice(e.routes, func(i, j int) bool {
		return bytes.Compare(e.routes[i].Start, e.routes[j].Start) < 0
	})
}

// GetRoute finds a route for the given key using right half-open intervals.
func (e *Engine) GetRoute(key []byte) (Route, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.routes) == 0 {
		return Route{}, false
	}

	// Find the first route with Start > key.
	i := sort.Search(len(e.routes), func(i int) bool {
		return bytes.Compare(e.routes[i].Start, key) > 0
	})
	if i == 0 {
		return Route{}, false
	}
	r := e.routes[i-1]
	if r.End != nil && bytes.Compare(key, r.End) >= 0 {
		return Route{}, false
	}
	return r, true
}

// NextTimestamp returns a monotonic increasing timestamp.
func (e *Engine) NextTimestamp() uint64 {
	return atomic.AddUint64(&e.ts, 1)
}
