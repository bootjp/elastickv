package distribution

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
)

// Route represents a mapping from a starting key to a raft group.
// Ranges are right half-open infinite intervals: [Start, next Start).
// Start is inclusive and the range extends up to (but excluding) the next
// route's start. The last route is unbounded and covers all keys >= Start.
type Route struct {
	// Start marks the inclusive beginning of the range.
	Start []byte
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

// UpdateRoute registers or updates a route starting at the given key.
// Routes are stored sorted by Start.
func (e *Engine) UpdateRoute(start []byte, group uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, Route{Start: start, GroupID: group})
	sort.Slice(e.routes, func(i, j int) bool {
		return bytes.Compare(e.routes[i].Start, e.routes[j].Start) < 0
	})
}

// GetRoute finds a route for the given key using right half-open infinite
// intervals.
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
	return e.routes[i-1], true
}

// NextTimestamp returns a monotonic increasing timestamp.
func (e *Engine) NextTimestamp() uint64 {
	return atomic.AddUint64(&e.ts, 1)
}
