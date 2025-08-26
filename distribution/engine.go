package distribution

import (
    "context"
    "strconv"
    "sync"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
)

// Range represents a key range and its statistics.
type Range struct {
    Start        string
    End          string
    RequestCount int
    LastTS       int64
}

// RangeStat mirrors Range for external consumption.
type RangeStat struct {
    Start     string
    End       string
    Count     int
    Timestamp int64
}

// Engine tracks ranges and their statistics.
type Engine struct {
    mu             sync.Mutex
    ranges         []Range
    splitThreshold int
    notify         func(left, right Range)
    ts             *timestampOracle
}

// NewEngine creates a new Engine with a split threshold backed by an etcd
// timestamp oracle. If cli is nil, timestamps are not persisted.
func NewEngine(threshold int, cli *clientv3.Client, notify func(left, right Range)) *Engine {
    return &Engine{
        ranges:         []Range{{Start: "", End: string(rune(0xffff))}},
        splitThreshold: threshold,
        notify:         notify,
        ts:             newTimestampOracle(cli, "timestamp"),
    }
}

// RecordRequest registers a request for the given key and triggers a split
// when the request count for the containing range exceeds the threshold.
func (e *Engine) RecordRequest(key string) {
    e.mu.Lock()
    defer e.mu.Unlock()

    idx := e.findRange(key)
    e.ranges[idx].RequestCount++
    e.ranges[idx].LastTS = e.ts.next()
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
        out[i] = RangeStat{Start: r.Start, End: r.End, Count: r.RequestCount, Timestamp: r.LastTS}
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
    e.ts.next()
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

// timestampOracle provides monotonic timestamps persisted to etcd to avoid
// rollback on restart.
type timestampOracle struct {
    mu   sync.Mutex
    cli  *clientv3.Client
    key  string
    last int64
}

func newTimestampOracle(cli *clientv3.Client, key string) *timestampOracle {
    o := &timestampOracle{cli: cli, key: key}
    if cli != nil {
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        if resp, err := cli.Get(ctx, key); err == nil && len(resp.Kvs) > 0 {
            if n, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64); err == nil {
                o.last = n
            }
        }
        cancel()
    }
    return o
}

func (o *timestampOracle) next() int64 {
    o.mu.Lock()
    defer o.mu.Unlock()
    for {
        now := time.Now().UnixNano()
        if now <= o.last {
            now = o.last + 1
        }
        if o.cli == nil {
            o.last = now
            return o.last
        }
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        cmp := clientv3.Compare(clientv3.Value(o.key), "=", strconv.FormatInt(o.last, 10))
        if o.last == 0 {
            cmp = clientv3.Compare(clientv3.Version(o.key), "=", 0)
        }
        txn := o.cli.Txn(ctx).If(cmp).Then(clientv3.OpPut(o.key, strconv.FormatInt(now, 10)))
        resp, err := txn.Commit()
        cancel()
        if err == nil && resp.Succeeded {
            o.last = now
            return o.last
        }
        ctx, cancel = context.WithTimeout(context.Background(), time.Second)
        if gr, err := o.cli.Get(ctx, o.key); err == nil && len(gr.Kvs) > 0 {
            if n, err := strconv.ParseInt(string(gr.Kvs[0].Value), 10, 64); err == nil {
                o.last = n
            }
        } else {
            o.last = 0
        }
        cancel()
    }
}

