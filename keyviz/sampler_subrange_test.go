package keyviz

import (
	"bytes"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// layoutSlot builds a routeSlot with the sub-range layout K would
// produce for [start, end), without going through the full sampler.
func layoutSlot(start, end []byte, k int) *routeSlot {
	s := &MemSampler{opts: MemSamplerOptions{KeyBucketsPerRoute: k}}
	slot := &routeSlot{Start: cloneBytes(start), End: cloneBytes(end)}
	s.initSubLayout(slot, start, end)
	return slot
}

func TestComputeSubLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		start, end       []byte
		k                int
		wantEffK         int
		wantSpanNonZero  bool
		wantPrefixLen    int
		checkStartEndSet bool
	}{
		{name: "k=1 single bucket", start: []byte("a"), end: []byte("z"), k: 1, wantEffK: 1},
		{name: "unbounded end single bucket", start: []byte("a"), end: nil, k: 8, wantEffK: 1},
		{
			name: "normal range subdivides", start: []byte("a"), end: []byte("b"), k: 4,
			wantEffK: 4, wantSpanNonZero: true, wantPrefixLen: 0, checkStartEndSet: true,
		},
		{
			name:  "shared prefix stripped",
			start: []byte("tenant/aaaa"), end: []byte("tenant/zzzz"), k: 8,
			wantEffK: 8, wantSpanNonZero: true, wantPrefixLen: 7, checkStartEndSet: true,
		},
		{
			// Equal start/end (empty range): the common prefix spans the
			// whole key, the window reads only past-end zeros for both, so
			// subEnd == subStart -> single bucket. (Because commonPrefixLen
			// lands exactly on the first differing byte, the window always
			// captures a real difference for start < end, so this empty
			// range is the only degenerate case in practice.)
			name:  "equal start and end single bucket",
			start: []byte("abc"), end: []byte("abc"),
			k: 16, wantEffK: 1, wantPrefixLen: 3,
		},
		{name: "nil start treated as zero", start: nil, end: []byte{0x10}, k: 4, wantEffK: 4, wantSpanNonZero: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prefixLen, subStart, subEnd, subSpan, effK := computeSubLayout(tc.start, tc.end, tc.k)
			require.Equal(t, tc.wantEffK, effK, "effK")
			if tc.wantSpanNonZero {
				require.Greater(t, subSpan, uint64(0), "subSpan should be non-zero")
				require.Equal(t, subEnd-subStart, subSpan)
			} else {
				require.Zero(t, subSpan, "subSpan should be 0 for single-bucket layout")
			}
			if tc.wantPrefixLen != 0 {
				require.Equal(t, tc.wantPrefixLen, prefixLen, "prefixLen")
			}
			if tc.name == "nil start treated as zero" {
				require.Zero(t, subStart, "nil start -> subStart 0")
			}
		})
	}
}

func TestSubBucketIndexEdges(t *testing.T) {
	t.Parallel()
	slot := layoutSlot([]byte{0x00}, []byte{0x40}, 4) // [0x00, 0x40) / 4 => width 0x10 each
	require.Len(t, slot.subBuckets, 4)

	// Below/at start pin to bucket 0; at/above end pin to last bucket.
	require.Equal(t, 0, slot.subBucketIndex([]byte{0x00}), "start pins to 0")
	require.Equal(t, 0, slot.subBucketIndex(nil), "nil key pins to 0")
	require.Equal(t, 3, slot.subBucketIndex([]byte{0x40}), "end pins to last")
	require.Equal(t, 3, slot.subBucketIndex([]byte{0xFF}), "above end pins to last")

	// Interior: 0x00-0x0F -> 0, 0x10-0x1F -> 1, 0x20-0x2F -> 2, 0x30-0x3F -> 3.
	require.Equal(t, 0, slot.subBucketIndex([]byte{0x0F}))
	require.Equal(t, 1, slot.subBucketIndex([]byte{0x10}))
	require.Equal(t, 2, slot.subBucketIndex([]byte{0x20}))
	require.Equal(t, 3, slot.subBucketIndex([]byte{0x3F}))
}

func TestSubBucketIndexSingleBucketAlwaysZero(t *testing.T) {
	t.Parallel()
	for _, slot := range []*routeSlot{
		layoutSlot([]byte("a"), []byte("z"), 1),   // K=1
		layoutSlot([]byte("a"), nil, 8),           // unbounded tail
		layoutSlot([]byte{0x00}, []byte{0x00}, 8), // degenerate
	} {
		require.Len(t, slot.subBuckets, 1)
		for _, key := range [][]byte{nil, {0x00}, {0x80}, {0xFF, 0xFF}} {
			require.Equal(t, 0, slot.subBucketIndex(key))
		}
	}
}

// TestSubBucketIndexMonotone is the rapid property: key1 <= key2 implies
// idx1 <= idx2 for any [start, end) and K (order preservation).
func TestSubBucketIndexMonotone(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		k := rapid.IntRange(1, MaxKeyBucketsPerRoute).Draw(rt, "k")
		start := rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(rt, "start")
		end := rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(rt, "end")
		slot := layoutSlot(start, end, k)

		k1 := rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(rt, "k1")
		k2 := rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(rt, "k2")
		if bytes.Compare(k1, k2) > 0 {
			k1, k2 = k2, k1
		}
		idx1 := slot.subBucketIndex(k1)
		idx2 := slot.subBucketIndex(k2)
		if idx1 > idx2 {
			rt.Fatalf("non-monotone: key %x->%d > key %x->%d (start=%x end=%x k=%d)",
				k1, idx1, k2, idx2, start, end, k)
		}
		// Index must always be in range.
		require.GreaterOrEqual(rt, idx1, 0)
		require.Less(rt, idx2, len(slot.subBuckets))
	})
}

// TestSamplerSubRangeHotKeyOnDistinctRow is the end-to-end goal: a hot
// sub-range surfaces as its own row, separate from a cold one.
func TestSamplerSubRangeHotKeyOnDistinctRow(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyBucketsPerRoute: 4})
	require.True(t, s.RegisterRoute(1, []byte{0x00}, []byte{0x40}, 0))

	// 100 writes into sub-bucket 3 ([0x30,0x40)), 1 into sub-bucket 0.
	for i := 0; i < 100; i++ {
		s.Observe(1, []byte{0x38}, OpWrite, 0)
	}
	s.Observe(1, []byte{0x01}, OpWrite, 0)
	s.Flush()

	cols := s.Snapshot(time.Time{}, time.Time{})
	require.Len(t, cols, 1)
	rows := cols[0].Rows
	require.Len(t, rows, 2, "one row per non-empty sub-bucket")
	for _, r := range rows {
		require.Equal(t, 4, r.SubBucketCount)
		require.Equal(t, uint64(1), r.RouteID)
	}
	// Sorted by Start: cold (bucket 0) first, hot (bucket 3) second.
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Start, rows[j].Start) < 0 })
	require.Equal(t, uint64(1), rows[0].Writes, "cold sub-range")
	require.Equal(t, uint64(100), rows[1].Writes, "hot sub-range")
	require.Equal(t, 0, rows[0].SubBucket)
	require.Equal(t, 3, rows[1].SubBucket)
}

// TestSamplerSubRangeBoundsTileExactly asserts the emitted sub-rows tile
// [routeStart, routeEnd) with no gap or overlap, with the first/last
// bounds pinned to the route's own bounds.
func TestSamplerSubRangeBoundsTileExactly(t *testing.T) {
	t.Parallel()
	const k = 4
	routeStart, routeEnd := []byte{0x00}, []byte{0x40}
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyBucketsPerRoute: k})
	require.True(t, s.RegisterRoute(1, routeStart, routeEnd, 0))
	// Hit every sub-bucket so all K rows are emitted.
	for _, key := range [][]byte{{0x00}, {0x10}, {0x20}, {0x30}} {
		s.Observe(1, key, OpWrite, 0)
	}
	s.Flush()
	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.Len(t, rows, k)
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Start, rows[j].Start) < 0 })

	require.Equal(t, routeStart, rows[0].Start, "first sub-row pins to route Start")
	require.Equal(t, routeEnd, rows[k-1].End, "last sub-row pins to route End")
	for i := 1; i < k; i++ {
		require.Equal(t, rows[i-1].End, rows[i].Start,
			"sub-row %d Start must equal sub-row %d End (contiguous, no gap)", i, i-1)
	}
}

// TestSamplerGraceWindowReRegistrationKeepsStaleLayout pins the design
// §4.2 contract: a same-RouteID re-registration inside the grace window
// reuses the slot and keeps its ORIGINAL sub-range layout (no recompute,
// no lost counts); only a fresh registration after grace expiry gets a
// new layout.
func TestSamplerGraceWindowReRegistrationKeepsStaleLayout(t *testing.T) {
	t.Parallel()
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 8, KeyBucketsPerRoute: 4})

	rangeAStart, rangeAEnd := []byte{0x00}, []byte{0x40}
	require.True(t, s.RegisterRoute(1, rangeAStart, rangeAEnd, 0))
	origStart := s.table.Load().slots[1].subStart
	origSpan := s.table.Load().slots[1].subSpan
	s.Observe(1, []byte{0x10}, OpWrite, 0)

	// Remove + re-register with a DIFFERENT range, inside the grace
	// window -> reclaimRetiredSlot reuses the slot, layout unchanged.
	s.RemoveRoute(1)
	require.True(t, s.RegisterRoute(1, []byte{0x80}, []byte{0xC0}, 0))
	reused := s.table.Load().slots[1]
	require.Equal(t, origStart, reused.subStart, "layout subStart must NOT be recomputed on in-grace re-registration")
	require.Equal(t, origSpan, reused.subSpan, "layout subSpan must NOT be recomputed")

	// The pre-removal count must survive (no zeroing on reclaim).
	s.Flush()
	require.Equal(t, uint64(1), totalWritesForRoute(s.Snapshot(time.Time{}, time.Time{}), 1),
		"pre-removal count lost on grace-window re-registration")

	// Companion: after grace expiry, a fresh registration gets a NEW
	// layout matching the new range.
	s.RemoveRoute(1)
	clk.Advance(s.graceWindow() + time.Second)
	s.Flush() // drop the retired slot
	require.True(t, s.RegisterRoute(1, []byte{0x80}, []byte{0xC0}, 0))
	fresh := s.table.Load().slots[1]
	freshStart := windowUint64([]byte{0x80}, 0)
	require.Equal(t, freshStart, fresh.subStart, "fresh post-grace registration must use the new range's layout")
}

// TestSamplerK1MatchesRouteLevel pins backward compatibility: with K=1,
// the sampler behaves exactly route-level — one row per route, full
// counts, no sub-bucket suffix signalling (SubBucketCount == 1).
func TestSamplerK1MatchesRouteLevel(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyBucketsPerRoute: 1})
	require.True(t, s.RegisterRoute(1, []byte{0x00}, []byte{0xFF}, 0))
	for _, key := range [][]byte{{0x01}, {0x80}, {0xFE}} {
		s.Observe(1, key, OpWrite, 0)
	}
	s.Flush()
	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.Len(t, rows, 1, "K=1 must collapse to one route-level row")
	require.Equal(t, uint64(3), rows[0].Writes)
	require.Equal(t, 1, rows[0].SubBucketCount)
	require.Equal(t, 0, rows[0].SubBucket)
}

// totalWritesForRoute sums Writes across all columns/sub-rows of a route.
func totalWritesForRoute(cols []MatrixColumn, routeID uint64) uint64 {
	var total uint64
	for _, col := range cols {
		for _, r := range col.Rows {
			if r.RouteID == routeID {
				total += r.Writes
			}
		}
	}
	return total
}

// BenchmarkObserveSubRange is the hot-path performance gate: it bounds
// the per-Observe regression sub-range bucketing adds versus K=1 (the
// extra cost is one windowUint64 + a bits.Mul64/Div64 and two
// bytes.Compare clamps). Run: go test -bench ObserveSubRange ./keyviz/.
func BenchmarkObserveSubRange(b *testing.B) {
	for _, k := range []int{1, 64} {
		b.Run("K="+strconv.Itoa(k), func(b *testing.B) {
			s := NewMemSampler(MemSamplerOptions{Step: time.Hour, HistoryColumns: 4, KeyBucketsPerRoute: k})
			s.RegisterRoute(1, make([]byte, 8), bytes.Repeat([]byte{0xFF}, 8), 0)
			key := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Observe(1, key, OpWrite, 64)
			}
		})
	}
}

// sanity: boundaryAt round-trips through the window encoding.
func TestBoundaryAtEncoding(t *testing.T) {
	t.Parallel()
	slot := layoutSlot([]byte{0x00}, []byte{0x40}, 4)
	b := slot.boundaryAt(1, []byte{0x00})
	// Bucket 1 of [0x00,0x40)/4 starts at 0x10 in the most-significant
	// window byte (0x10<<56); trailing zero bytes are trimmed so the
	// boundary is the minimal key in the cell, {0x10}, rather than the
	// 8-byte 0x10 00…00 (which would sort after short keys like 0x10).
	require.Equal(t, []byte{0x10}, b)
}
