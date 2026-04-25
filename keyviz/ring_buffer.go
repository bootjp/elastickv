package keyviz

import (
	"sort"
	"time"
)

// ringBuffer is a fixed-capacity circular buffer of MatrixColumn,
// oldest-first. Push drops the oldest entry once cap is reached.
// Range returns a half-open [from, to) slice in chronological order.
//
// The buffer is not goroutine-safe by itself; callers (MemSampler)
// guard it with historyMu.
type ringBuffer struct {
	cap     int
	buf     []MatrixColumn // logical-index ordered; head is the oldest
	wrapped bool
	pos     int // next write index when wrapped == false
}

func newRingBuffer(cap int) *ringBuffer {
	if cap < 1 {
		cap = 1
	}
	return &ringBuffer{cap: cap, buf: make([]MatrixColumn, 0, cap)}
}

// Push appends a column. Once length reaches cap the oldest entry is
// dropped (overwritten in place).
func (r *ringBuffer) Push(col MatrixColumn) {
	if !r.wrapped {
		r.buf = append(r.buf, col)
		if len(r.buf) >= r.cap {
			r.wrapped = true
			r.pos = 0
		}
		return
	}
	r.buf[r.pos] = col
	r.pos = (r.pos + 1) % r.cap
}

// Range returns the columns whose At falls in [from, to), oldest
// first. Either bound may be the zero Time, meaning unbounded on that
// side. The returned slice is freshly allocated; callers may mutate
// it freely.
func (r *ringBuffer) Range(from, to time.Time) []MatrixColumn {
	all := r.snapshotOrdered()
	// snapshotOrdered is already chronologically ordered.
	lo := 0
	if !from.IsZero() {
		lo = sort.Search(len(all), func(i int) bool {
			return !all[i].At.Before(from)
		})
	}
	hi := len(all)
	if !to.IsZero() {
		hi = sort.Search(len(all), func(i int) bool {
			return !all[i].At.Before(to)
		})
	}
	if lo > hi {
		return nil
	}
	out := make([]MatrixColumn, hi-lo)
	copy(out, all[lo:hi])
	return out
}

// snapshotOrdered returns a chronologically ordered (oldest first)
// view of the buffer contents in a fresh slice.
func (r *ringBuffer) snapshotOrdered() []MatrixColumn {
	if !r.wrapped {
		return append([]MatrixColumn(nil), r.buf...)
	}
	out := make([]MatrixColumn, 0, r.cap)
	out = append(out, r.buf[r.pos:]...)
	out = append(out, r.buf[:r.pos]...)
	return out
}
