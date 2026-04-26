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
// side. The returned slice is a deep copy: each column has its own
// Rows slice and each row owns its Start/End byte slices, so callers
// may mutate any field without corrupting stored history or racing
// with concurrent flushes.
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
	for i, src := range all[lo:hi] {
		out[i] = cloneColumn(src)
	}
	return out
}

// cloneColumn returns a deep copy of col: a fresh Rows slice with
// each row's Start/End and MemberRoutes independently allocated.
func cloneColumn(col MatrixColumn) MatrixColumn {
	rows := make([]MatrixRow, len(col.Rows))
	for i, row := range col.Rows {
		rows[i] = row
		rows[i].Start = cloneBytes(row.Start)
		rows[i].End = cloneBytes(row.End)
		if len(row.MemberRoutes) > 0 {
			rows[i].MemberRoutes = append([]uint64(nil), row.MemberRoutes...)
		}
	}
	return MatrixColumn{At: col.At, Rows: rows}
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
