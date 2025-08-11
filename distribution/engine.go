package distribution

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// Route represents mapping from key range to raft group.
// The key range is half-open: [Start, End)
// meaning Start is inclusive and End is exclusive. If End is nil or empty
// the range is unbounded and covers all keys >= Start.
type Route struct {
	// Start marks the inclusive beginning of the range.
	Start []byte
        // End marks the exclusive end of the range. When empty, the range has
        // no upper bound.
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

// UpdateRoute registers or updates a route. The range is [start, end). If end
// is nil or empty the range is treated as unbounded above.
func (e *Engine) UpdateRoute(start, end []byte, group uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, Route{Start: start, End: end, GroupID: group})
}

// GetRoute finds a route for the given key. The search uses half-open ranges
// [Start, End). If End is empty the range is treated as unbounded above.
func (e *Engine) GetRoute(key []byte) (Route, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
        for _, r := range e.routes {
                if bytes.Compare(key, r.Start) >= 0 {
                        if len(r.End) == 0 || bytes.Compare(key, r.End) < 0 {
                                return r, true
                        }
                }
        }
	return Route{}, false
}

// NextTimestamp returns a monotonic increasing timestamp.
func (e *Engine) NextTimestamp() uint64 {
	return atomic.AddUint64(&e.ts, 1)
}
