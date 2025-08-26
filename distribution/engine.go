package distribution

import (
    "sync"
)

// Range represents a key range and its statistics.
type Range struct {
    Start string
    End   string
    RequestCount int
}

// RangeStat mirrors Range for external consumption.
type RangeStat struct {
    Start string
    End   string
    Count int
}

// Engine tracks ranges and their statistics.
type Engine struct {
    mu sync.Mutex
    ranges []Range
    splitThreshold int
    notify func(left, right Range)
}

// NewEngine creates a new Engine with a split threshold. The notify
// function is invoked whenever a range split occurs.
func NewEngine(threshold int, notify func(left, right Range)) *Engine {
    return &Engine{
        ranges: []Range{{Start: "", End: string(rune(0xffff))}},
        splitThreshold: threshold,
        notify: notify,
    }
}

// RecordRequest registers a request for the given key and triggers a split
// when the request count for the containing range exceeds the threshold.
func (e *Engine) RecordRequest(key string) {
    e.mu.Lock()
    defer e.mu.Unlock()

    idx := e.findRange(key)
    e.ranges[idx].RequestCount++
    if e.ranges[idx].RequestCount >= e.splitThreshold {
        e.splitRange(idx)
    }
}

// SplitRange splits the range containing the supplied key.
func (e *Engine) SplitRange(key string) {
    e.mu.Lock()
    defer e.mu.Unlock()
    idx := e.findRange(key)
    e.splitRange(idx)
}

// GetStats returns the statistics for all ranges.
func (e *Engine) GetStats() []RangeStat {
    e.mu.Lock()
    defer e.mu.Unlock()
    out := make([]RangeStat, len(e.ranges))
    for i, r := range e.ranges {
        out[i] = RangeStat{Start: r.Start, End: r.End, Count: r.RequestCount}
    }
    return out
}

// Ranges returns a copy of the current range metadata. Primarily used for tests.
func (e *Engine) Ranges() []Range {
    e.mu.Lock()
    defer e.mu.Unlock()
    out := make([]Range, len(e.ranges))
    copy(out, e.ranges)
    return out
}

func (e *Engine) findRange(key string) int {
    for i, r := range e.ranges {
        if (r.Start == "" || key >= r.Start) && (r.End == "" || key < r.End) {
            return i
        }
    }
    // default to last range
    return len(e.ranges) - 1
}

func (e *Engine) splitRange(idx int) {
    r := e.ranges[idx]
    mid := midpoint(r.Start, r.End)
    left := Range{Start: r.Start, End: mid}
    right := Range{Start: mid, End: r.End}

    // replace range with two new ranges
    newRanges := make([]Range, 0, len(e.ranges)+1)
    newRanges = append(newRanges, e.ranges[:idx]...)
    newRanges = append(newRanges, left, right)
    newRanges = append(newRanges, e.ranges[idx+1:]...)
    e.ranges = newRanges

    if e.notify != nil {
        e.notify(left, right)
    }
}

func midpoint(start, end string) string {
    // simplistic midpoint calculation based on the first byte of the bounds.
    s := byte('a')
    if len(start) > 0 {
        s = start[0]
    }
    e := byte('z')
    if len(end) > 0 && end != string(rune(0xffff)) {
        e = end[0]
    }
    m := s + (e-s)/2
    return string([]byte{m})
}

