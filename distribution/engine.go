package distribution

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// Route represents mapping from key range to raft group.
type Route struct {
	Start   []byte
	End     []byte
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

// UpdateRoute registers or updates a route.
func (e *Engine) UpdateRoute(start, end []byte, group uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, Route{Start: start, End: end, GroupID: group})
}

// GetRoute finds a route for the given key.
func (e *Engine) GetRoute(key []byte) (Route, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, r := range e.routes {
		if bytes.Compare(key, r.Start) >= 0 && bytes.Compare(key, r.End) < 0 {
			return r, true
		}
	}
	return Route{}, false
}

// NextTimestamp returns a monotonic increasing timestamp.
func (e *Engine) NextTimestamp() uint64 {
	return atomic.AddUint64(&e.ts, 1)
}
