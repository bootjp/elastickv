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
		{
			// Unbounded high end now sub-divides over [subStart, MaxUint64]
			// (§3.2 revised) rather than collapsing to one bucket.
			name: "unbounded end subdivides", start: []byte("a"), end: nil, k: 8,
			wantEffK: 8, wantSpanNonZero: true,
		},
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
		{
			// Unbounded end whose start's leading 8 bytes are all 0xFF:
			// subStart == MaxUint64, nothing left above it to divide, so
			// the subStart == MaxUint64 guard collapses to one bucket.
			name:  "all-0xFF start unbounded end single bucket",
			start: bytes.Repeat([]byte{0xFF}, 8), end: nil, k: 8, wantEffK: 1,
		},
		{
			// Unbounded high start: window 0xFF..FD leaves a span of 2 (<k),
			// so effK caps to the span (effKForSpan / Codex P2), keeping
			// reconstructed boundaries valid.
			name:  "unbounded high start caps effK to span",
			start: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD}, end: nil, k: 8,
			wantEffK: 2, wantSpanNonZero: true,
		},
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
		layoutSlot([]byte{0x00}, []byte{0x00}, 8), // degenerate (empty range)
	} {
		require.Len(t, slot.subBuckets, 1)
		for _, key := range [][]byte{nil, {0x00}, {0x80}, {0xFF, 0xFF}} {
			require.Equal(t, 0, slot.subBucketIndex(key))
		}
	}
}

// TestSubBucketIndexUnboundedEnd pins the §3.2 revision: an open high
// end ([start, nil)) — the single-route cluster and every cluster's
// tail route — now sub-divides over [subStart, MaxUint64] instead of
// collapsing to one bucket. The upper-edge clamp is skipped (subHi nil
// sentinel), so distinct leading bytes land in distinct buckets.
func TestSubBucketIndexUnboundedEnd(t *testing.T) {
	t.Parallel()
	// The single-route cluster the user hit: route owns the whole space.
	slot := layoutSlot(nil, nil, 4)
	require.Len(t, slot.subBuckets, 4, "[nil,nil) must sub-divide, not collapse to 1")
	require.Nil(t, slot.subHi, "unbounded end => subHi nil sentinel")

	// Leading byte spreads keys across the [0, MaxUint64] window:
	// 0x00.. -> low bucket, 0xFF.. -> top bucket, and order preserved.
	require.Equal(t, 0, slot.subBucketIndex([]byte{0x00}))
	require.Equal(t, 3, slot.subBucketIndex([]byte{0xFF}))
	// 0x40/0x80/0xC0 fall in quarters of [0, MaxUint64] -> buckets 1/2/3,
	// strictly increasing (Less, not LessOrEqual, so a collapse regresses).
	i40 := slot.subBucketIndex([]byte{0x40})
	i80 := slot.subBucketIndex([]byte{0x80})
	iC0 := slot.subBucketIndex([]byte{0xC0})
	require.Less(t, i40, i80)
	require.Less(t, i80, iC0)

	// A bounded-low / unbounded-high tail route ([0x80, nil)).
	tail := layoutSlot([]byte{0x80}, nil, 4)
	require.Len(t, tail.subBuckets, 4)
	require.Equal(t, 0, tail.subBucketIndex([]byte{0x00}), "key below tail Start pins to bucket 0")
	require.Equal(t, 3, tail.subBucketIndex([]byte{0xFF}))
}

// TestSamplerUnboundedRouteEmitsSubRanges is the end-to-end fix for the
// deployed single-route cluster: with K>1, an unbounded [nil,nil) route
// now produces multiple sub-range rows (previously exactly one). The
// last sub-bucket keeps the unbounded End (nil).
func TestSamplerUnboundedRouteEmitsSubRanges(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyBucketsPerRoute: 4})
	require.True(t, s.RegisterRoute(1, nil, nil, 0)) // whole keyspace, unbounded both ends
	for _, key := range [][]byte{{0x10}, {0x50}, {0x90}, {0xD0}} {
		s.Observe(1, key, OpWrite, 0, LabelLegacy)
	}
	s.Flush()
	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.Greater(t, len(rows), 1, "unbounded route must emit >1 sub-range row")
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Start, rows[j].Start) < 0 })
	require.Nil(t, rows[len(rows)-1].End, "last sub-bucket keeps the unbounded End (nil)")
	for _, r := range rows {
		require.Equal(t, 4, r.SubBucketCount)
	}
}

// TestSamplerUnboundedHighStartValidBounds pins Codex P2: a high start
// whose window is near MaxUint64 AND carries a suffix byte past the
// window must never emit a sub-row with Start > End. Capping effK at the
// span (effKForSpan) keeps reconstructed boundaries strictly above the
// route start. Without the cap, bucket 0 would emit Start > End here.
func TestSamplerUnboundedHighStartValidBounds(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyBucketsPerRoute: 4})
	start := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD, 0xFF} // window 0xFF..FD + suffix
	require.True(t, s.RegisterRoute(1, start, nil, 0))
	for _, key := range [][]byte{start, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}} {
		s.Observe(1, key, OpWrite, 0, LabelLegacy)
	}
	s.Flush()
	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.NotEmpty(t, rows)
	for _, r := range rows {
		if r.End != nil { // the last sub-bucket keeps the unbounded End (nil)
			require.Less(t, bytes.Compare(r.Start, r.End), 0,
				"sub-row must have Start < End (no invalid/overlapping bounds): %x..%x", r.Start, r.End)
		}
	}
}

// TestSubBucketBoundsContainCountedKey pins Codex P2 (boundary/bucket
// consistency): the emitted [Start, End) of the bucket subBucketIndex
// assigns a key to must actually CONTAIN that key — including keys that
// land exactly on an interior boundary value. ceil-based boundaryAt
// guarantees this; floor-based would mis-shelve boundary-value keys.
func TestSubBucketBoundsContainCountedKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		start, end []byte
		k          int
	}{
		{"unbounded whole space", nil, nil, 4},
		{"unbounded k not dividing span", nil, nil, 7},
		{"bounded divisible span", []byte{0x00}, []byte{0x40}, 4},
		// 2^62 / 7 is not integer, so ceil != floor for these boundaries —
		// deterministic coverage of the ceil reconstruction for bounded.
		{"bounded k not dividing span", []byte{0x00}, []byte{0x40}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			slot := layoutSlot(tc.start, tc.end, tc.k)
			// Sweep keys, plus the exact reconstructed interior boundaries
			// (the values most likely to expose a floor/ceil mismatch).
			keys := [][]byte{{0x00}, {0x01}, {0x3F}, {0x40}, {0x7F}, {0x80}, {0xC0}, {0xFE}, {0xFF}}
			for i := 1; i < len(slot.subBuckets); i++ {
				keys = append(keys, slot.boundaryAt(i, cloneBytes(tc.start)))
			}
			for _, key := range keys {
				// Only in-range keys (start <= key < end) are ever Observed
				// against this route; the out-of-range clamp (key >= End ->
				// last bucket) intentionally does not place such keys inside
				// a half-open range.
				if bytes.Compare(key, tc.start) < 0 {
					continue
				}
				if tc.end != nil && bytes.Compare(key, tc.end) >= 0 {
					continue
				}
				idx := slot.subBucketIndex(key)
				lo, hi := slot.subBucketBounds(idx, cloneBytes(tc.start), cloneBytes(tc.end))
				require.LessOrEqual(t, bytes.Compare(lo, key), 0,
					"%s: key %x counted in bucket %d but Start %x is above it", tc.name, key, idx, lo)
				if hi != nil {
					require.Less(t, bytes.Compare(key, hi), 0,
						"%s: key %x counted in bucket %d but End %x excludes it", tc.name, key, idx, hi)
				}
			}
		})
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
		s.Observe(1, []byte{0x38}, OpWrite, 0, LabelLegacy)
	}
	s.Observe(1, []byte{0x01}, OpWrite, 0, LabelLegacy)
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
		s.Observe(1, key, OpWrite, 0, LabelLegacy)
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
	origStart := s.table.Load().slots[slotKey{RouteID: 1, Label: LabelLegacy}].subStart
	origSpan := s.table.Load().slots[slotKey{RouteID: 1, Label: LabelLegacy}].subSpan
	s.Observe(1, []byte{0x10}, OpWrite, 0, LabelLegacy)

	// Remove + re-register with a DIFFERENT range, inside the grace
	// window -> reclaimRetiredSlot reuses the slot, layout unchanged.
	s.RemoveRoute(1)
	require.True(t, s.RegisterRoute(1, []byte{0x80}, []byte{0xC0}, 0))
	reused := s.table.Load().slots[slotKey{RouteID: 1, Label: LabelLegacy}]
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
	fresh := s.table.Load().slots[slotKey{RouteID: 1, Label: LabelLegacy}]
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
		s.Observe(1, key, OpWrite, 0, LabelLegacy)
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
				s.Observe(1, key, OpWrite, 64, LabelLegacy)
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
