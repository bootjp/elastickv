package keyviz

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestSampler(t *testing.T, opts MemSamplerOptions) (*MemSampler, *fakeClock) {
	t.Helper()
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	if opts.Now == nil {
		opts.Now = clk.Now
	}
	return NewMemSampler(opts), clk
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestNilSamplerObserveSafe(t *testing.T) {
	t.Parallel()
	var s *MemSampler // nil
	s.Observe(1, OpRead, 10, 20)
	if s.RegisterRoute(1, []byte("a"), []byte("b")) {
		t.Fatal("nil RegisterRoute should return false")
	}
	s.RemoveRoute(1)
	s.Flush()
	if got := s.Snapshot(time.Time{}, time.Time{}); got != nil {
		t.Fatalf("nil Snapshot should be nil, got %v", got)
	}
}

func TestObserveAndFlushBasic(t *testing.T) {
	t.Parallel()
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	if !s.RegisterRoute(1, []byte("a"), []byte("c")) {
		t.Fatal("RegisterRoute(1) returned false")
	}
	s.Observe(1, OpRead, 5, 10)
	s.Observe(1, OpRead, 5, 10)
	s.Observe(1, OpWrite, 5, 100)

	s.Flush()
	clk.Advance(time.Second)
	s.Flush() // empty

	cols := s.Snapshot(time.Time{}, time.Time{})
	if len(cols) != 2 {
		t.Fatalf("got %d columns, want 2", len(cols))
	}
	c0 := cols[0]
	if len(c0.Rows) != 1 {
		t.Fatalf("col0 rows = %d, want 1", len(c0.Rows))
	}
	r := c0.Rows[0]
	if r.RouteID != 1 || r.Reads != 2 || r.Writes != 1 || r.ReadBytes != 30 || r.WriteBytes != 105 {
		t.Fatalf("col0 row = %+v", r)
	}
	if len(cols[1].Rows) != 0 {
		t.Fatalf("col1 should be empty (no observe between flushes), got %v", cols[1].Rows)
	}
}

// TestNoCountsLostAcrossFlush asserts the SwapUint64 flush protocol
// doesn't drop counts even when many goroutines hammer Observe across
// the flush boundary. Repeats the exercise N times to flush out
// scheduling races.
func TestNoCountsLostAcrossFlush(t *testing.T) {
	t.Parallel()
	const (
		writers   = 8
		perWriter = 5_000
		flushes   = 20
	)
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Millisecond, HistoryColumns: 1024})
	if !s.RegisterRoute(1, []byte("a"), []byte("b")) {
		t.Fatal("Register failed")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < perWriter; n++ {
				select {
				case <-stop:
					return
				default:
				}
				s.Observe(1, OpRead, 1, 0)
			}
		}()
	}
	// Fire flushes from a separate goroutine while writers run.
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		for i := 0; i < flushes; i++ {
			s.Flush()
			time.Sleep(time.Microsecond * 100)
		}
	}()

	wg.Wait()
	close(stop)
	<-flushDone
	// One last flush so any straggler atomic.Add is harvested.
	s.Flush()

	var total uint64
	for _, col := range s.Snapshot(time.Time{}, time.Time{}) {
		for _, row := range col.Rows {
			total += row.Reads
		}
	}
	want := uint64(writers * perWriter)
	if total != want {
		t.Fatalf("total reads = %d, want %d (lost %d)", total, want, want-total)
	}
}

func TestRouteBudgetCoarsensIntoVirtualBucket(t *testing.T) {
	t.Parallel()
	s := budgetSetup(t)
	s.Observe(1, OpRead, 1, 0)
	s.Observe(2, OpRead, 1, 0)
	s.Observe(3, OpRead, 1, 0)
	s.Flush()
	rows := budgetSingleColumnRows(t, s)
	agg := findAggregateRow(t, rows)
	if len(agg.MemberRoutes) != 1 || agg.MemberRoutes[0] != 3 {
		t.Fatalf("aggregate.MemberRoutes = %v, want [3]", agg.MemberRoutes)
	}
}

func budgetSetup(t *testing.T) *MemSampler {
	t.Helper()
	s, _ := newTestSampler(t, MemSamplerOptions{
		Step:             time.Second,
		HistoryColumns:   4,
		MaxTrackedRoutes: 2,
	})
	if !s.RegisterRoute(1, []byte("a"), []byte("c")) {
		t.Fatal("route 1 should fit")
	}
	if !s.RegisterRoute(2, []byte("c"), []byte("e")) {
		t.Fatal("route 2 should fit")
	}
	if s.RegisterRoute(3, []byte("e"), []byte("g")) {
		t.Fatal("route 3 over budget should return false (folded into virtual bucket)")
	}
	return s
}

func budgetSingleColumnRows(t *testing.T, s *MemSampler) []MatrixRow {
	t.Helper()
	cols := s.Snapshot(time.Time{}, time.Time{})
	if len(cols) != 1 {
		t.Fatalf("len(cols) = %d", len(cols))
	}
	rows := cols[0].Rows
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	return rows
}

func findAggregateRow(t *testing.T, rows []MatrixRow) MatrixRow {
	t.Helper()
	for i := range rows {
		if rows[i].Aggregate {
			return rows[i]
		}
	}
	t.Fatalf("no aggregate row in output: %+v", rows)
	return MatrixRow{}
}

func TestSnapshotRangeFilters(t *testing.T) {
	t.Parallel()
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 8})
	s.RegisterRoute(1, []byte("a"), []byte("b"))
	for i := 0; i < 5; i++ {
		s.Observe(1, OpRead, 0, 0)
		s.Flush()
		clk.Advance(time.Second)
	}
	all := s.Snapshot(time.Time{}, time.Time{})
	if len(all) != 5 {
		t.Fatalf("all len = %d", len(all))
	}
	mid := all[1].At
	end := all[3].At
	got := s.Snapshot(mid, end)
	if len(got) != 2 {
		t.Fatalf("[mid,end) len = %d, want 2 (entries at +1s and +2s); cols=%v", len(got), columnTimes(all))
	}
	if !got[0].At.Equal(mid) {
		t.Fatalf("got[0].At = %v, want %v", got[0].At, mid)
	}
}

func TestRingBufferDropsOldest(t *testing.T) {
	t.Parallel()
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 3})
	s.RegisterRoute(1, []byte("a"), []byte("b"))
	for i := 0; i < 5; i++ {
		s.Observe(1, OpRead, 0, 0)
		s.Flush()
		clk.Advance(time.Second)
	}
	all := s.Snapshot(time.Time{}, time.Time{})
	if len(all) != 3 {
		t.Fatalf("len = %d, want 3 (cap)", len(all))
	}
	// Should be the most recent three columns, oldest first.
	for i := 1; i < len(all); i++ {
		if !all[i].At.After(all[i-1].At) {
			t.Fatalf("not chronological at %d: %v then %v", i, all[i-1].At, all[i].At)
		}
	}
}

func TestRemoveRouteSilencesObserve(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	s.RegisterRoute(1, []byte("a"), []byte("b"))
	s.Observe(1, OpRead, 0, 0)
	s.RemoveRoute(1)
	s.Observe(1, OpRead, 0, 0) // silently dropped
	s.Flush()
	cols := s.Snapshot(time.Time{}, time.Time{})
	for _, c := range cols {
		for _, r := range c.Rows {
			if r.RouteID == 1 && r.Reads > 1 {
				t.Fatalf("post-remove Observe leaked: %+v", r)
			}
		}
	}
}

// TestRemoveRouteHarvestsPendingCounts pins Codex P2: counts
// accumulated between the last flush and RemoveRoute must not be
// silently dropped. The retired slot is queued for one final drain.
func TestRemoveRouteHarvestsPendingCounts(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	s.RegisterRoute(1, []byte("a"), []byte("b"))
	for i := 0; i < 7; i++ {
		s.Observe(1, OpRead, 1, 0)
	}
	// Remove BEFORE flushing — pending 7 reads must surface.
	s.RemoveRoute(1)
	s.Flush()

	cols := s.Snapshot(time.Time{}, time.Time{})
	if len(cols) != 1 {
		t.Fatalf("len(cols) = %d, want 1", len(cols))
	}
	rows := cols[0].Rows
	if len(rows) != 1 || rows[0].RouteID != 1 || rows[0].Reads != 7 {
		t.Fatalf("rows = %+v, want one row with route=1 reads=7", rows)
	}
}

// TestRemoveVirtualMemberPrunesMemberRoutes pins Codex P2: when a
// coarsened (virtual-bucket) RouteID is removed, the bucket's
// MemberRoutes list must eventually drop that ID. Pruning is deferred
// across the wall-clock grace window so the row attribution stays
// correct while the bucket counters still include the removed route's
// pre-removal increments — TestRemoveVirtualMemberPruneDeferred pins
// the grace-window half of this contract.
func TestRemoveVirtualMemberPrunesMemberRoutes(t *testing.T) {
	t.Parallel()
	s, clk := setupVirtualBucketWithThreeMembers(t)
	s.RemoveRoute(2)
	// Drain at least once within grace, then advance past grace so the
	// next Flush actually executes the prune. One more Flush captures
	// the post-prune MemberRoutes state in a row.
	s.Observe(3, OpRead, 1, 0)
	s.Flush()
	clk.Advance(s.graceWindow() + time.Second)
	s.Observe(3, OpRead, 1, 0)
	s.Flush()
	s.Observe(3, OpRead, 1, 0)
	s.Flush()

	cols := s.Snapshot(time.Time{}, time.Time{})
	agg := cols[len(cols)-1].Rows[0]
	if memberRoutesContain(agg.MemberRoutes, 2) {
		t.Fatalf("after grace, removed route 2 still in MemberRoutes: %v", agg.MemberRoutes)
	}
	if len(agg.MemberRoutes) != 1 || agg.MemberRoutes[0] != 3 {
		t.Fatalf("MemberRoutes = %v, want [3]", agg.MemberRoutes)
	}
}

// TestRemoveVirtualMemberPruneDeferred pins the deferred-prune
// contract: the removed routeID stays in MemberRoutes throughout the
// wall-clock grace window so flushed rows whose counters still include
// that route's pre-removal increments attribute the traffic correctly.
func TestRemoveVirtualMemberPruneDeferred(t *testing.T) {
	t.Parallel()
	s, _ := setupVirtualBucketWithThreeMembers(t)
	s.RemoveRoute(2)
	// Two flushes inside the grace window — the clock is not advanced,
	// so each Flush sees now-retiredAt == 0 < grace and keeps the
	// prune entry.
	for i := 0; i < 2; i++ {
		s.Observe(3, OpRead, 1, 0)
		s.Flush()
		col := lastSnapshotColumn(t, s)
		agg := findAggregateRow(t, col.Rows)
		if !memberRoutesContain(agg.MemberRoutes, 2) {
			t.Fatalf("flush %d within grace dropped route 2 too early: %v", i, agg.MemberRoutes)
		}
	}
}

func lastSnapshotColumn(t *testing.T, s *MemSampler) MatrixColumn {
	t.Helper()
	cols := s.Snapshot(time.Time{}, time.Time{})
	if len(cols) == 0 {
		t.Fatal("snapshot empty")
	}
	return cols[len(cols)-1]
}

func memberRoutesContain(members []uint64, target uint64) bool {
	for _, m := range members {
		if m == target {
			return true
		}
	}
	return false
}

func setupVirtualBucketWithThreeMembers(t *testing.T) (*MemSampler, *fakeClock) {
	t.Helper()
	s, clk := newTestSampler(t, MemSamplerOptions{
		Step:             time.Second,
		HistoryColumns:   4,
		MaxTrackedRoutes: 1,
	})
	if !s.RegisterRoute(1, []byte("a"), []byte("c")) {
		t.Fatal("route 1 should fit")
	}
	if s.RegisterRoute(2, []byte("c"), []byte("e")) {
		t.Fatal("route 2 should fold (over budget)")
	}
	if s.RegisterRoute(3, []byte("e"), []byte("g")) {
		t.Fatal("route 3 should fold (over budget)")
	}
	return s, clk
}

// TestRegisterDoesNotRaceFlushOnVirtualBucket pins Codex P1: folding
// a new route into an existing virtual bucket must not race a
// concurrent Flush iterating the slot's metadata. -race detector
// makes the regression observable.
func TestRegisterDoesNotRaceFlushOnVirtualBucket(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{
		Step:             time.Millisecond,
		HistoryColumns:   1024,
		MaxTrackedRoutes: 1,
	})
	if !s.RegisterRoute(1, []byte("a"), []byte("b")) {
		t.Fatal("route 1 should fit")
	}
	// Seed a virtual bucket so subsequent Registers fold into it.
	if s.RegisterRoute(2, []byte("c"), []byte("d")) {
		t.Fatal("route 2 should fold")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 3; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			s.RegisterRoute(uint64(n), []byte{byte(n)}, []byte{byte(n + 1)}) //nolint:gosec // bounded test loop.
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Flush()
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	// No assertion needed — race detector failures are the test signal.
}

func TestObserveOnUnknownRouteIsNoop(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	s.Observe(42, OpRead, 0, 0)
	s.Flush()
	cols := s.Snapshot(time.Time{}, time.Time{})
	for _, c := range cols {
		if len(c.Rows) != 0 {
			t.Fatalf("unknown-route Observe leaked: %+v", c)
		}
	}
}

// TestConcurrentRegisterAndObserveRace is the -race regression: a
// concurrent RegisterRoute (COW table publish) must not race with
// Observe (Load + map lookup against the snapshot).
func TestConcurrentRegisterAndObserveRace(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 8})
	const routes = 64
	var observed atomic.Uint64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= routes; i++ {
			s.RegisterRoute(i, []byte{byte('a' + i%26)}, []byte{byte('a' + (i+1)%26)})
			time.Sleep(time.Microsecond)
		}
	}()
	wg.Add(4)
	for w := 0; w < 4; w++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := uint64(1); i <= routes; i++ {
					s.Observe(i, OpRead, 0, 0)
					observed.Add(1)
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	if observed.Load() == 0 {
		t.Fatal("observers ran zero iterations — schedule starvation?")
	}
}

func columnTimes(cols []MatrixColumn) []time.Time {
	out := make([]time.Time, len(cols))
	for i, c := range cols {
		out[i] = c.At
	}
	return out
}

// TestRemoveLastVirtualMemberHarvestsBucket pins Codex round-5 P1:
// removing the last member of a virtual bucket leaves the bucket
// orphaned in the route table — rebuildSorted no longer reaches it
// from virtualForRoute, so without the orphan-retire path Flush would
// silently lose any pre-removal counters the bucket still holds.
func TestRemoveLastVirtualMemberHarvestsBucket(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{
		Step:             time.Second,
		HistoryColumns:   4,
		MaxTrackedRoutes: 1,
	})
	mustRegister(t, s, 1, "a", "b")
	if s.RegisterRoute(2, []byte("c"), []byte("d")) {
		t.Fatal("route 2 over budget should fold into virtual bucket")
	}
	s.Observe(2, OpRead, 5, 7)
	s.RemoveRoute(2)
	s.Flush()

	rows := lastSnapshotColumn(t, s).Rows
	agg := findAggregateRow(t, rows)
	if agg.Reads != 1 || agg.ReadBytes != 12 {
		t.Fatalf("orphan bucket counts dropped: reads=%d bytes=%d (want 1, 12)",
			agg.Reads, agg.ReadBytes)
	}
}

// TestFlushSortsMixedLiveAndRetiredRows pins the row-ordering
// invariant when retired-slot drains land in the same column as live
// slot drains. Without a final sort the retired rows would be appended
// in queue order, producing a column whose Rows are not monotone by
// Start.
func TestFlushSortsMixedLiveAndRetiredRows(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	mustRegister(t, s, 1, "a", "b")
	mustRegister(t, s, 2, "m", "n")
	mustRegister(t, s, 3, "z", "{")
	s.Observe(2, OpRead, 0, 0)
	s.RemoveRoute(2)
	s.Observe(1, OpRead, 0, 0)
	s.Observe(3, OpRead, 0, 0)
	s.Flush()
	flushedRowsSorted(t, s)
}

// TestRetiredSlotGracePeriod asserts that a retired slot stays in the
// drain queue for the wall-clock grace window so an Observe goroutine
// that loaded the pre-RemoveRoute table snapshot can complete its
// atomic.Add into the slot and still have the increment harvested by
// a subsequent Flush. We simulate the late writer by reaching into
// the retired-slot queue directly and bumping the counter between
// flushes inside the grace window.
func TestRetiredSlotGracePeriod(t *testing.T) {
	t.Parallel()
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 8})
	if !s.RegisterRoute(1, []byte("a"), []byte("b")) {
		t.Fatal("RegisterRoute(1) returned false")
	}
	s.Observe(1, OpRead, 1, 2)
	s.RemoveRoute(1)

	lateSlot := graceQueueSingleSlot(t, s)
	s.Flush() // drain inside grace
	lateSlot.reads.Add(7)
	s.Flush() // drain inside grace, harvests the late-writer increment

	total := totalReadsForRoute(s.Snapshot(time.Time{}, time.Time{}), 1)
	if total != 1+7 {
		t.Fatalf("expected reads 8 (pre-remove + late-writer), got %d", total)
	}

	clk.Advance(s.graceWindow() + time.Second)
	s.Flush() // last drain, retiredAt+grace now passed → entry dropped
	s.retiredMu.Lock()
	leftover := len(s.retiredSlots)
	s.retiredMu.Unlock()
	if leftover != 0 {
		t.Fatalf("retired slot not released after grace, len=%d", leftover)
	}
}

func graceQueueSingleSlot(t *testing.T, s *MemSampler) *routeSlot {
	t.Helper()
	s.retiredMu.Lock()
	defer s.retiredMu.Unlock()
	if len(s.retiredSlots) != 1 {
		t.Fatalf("expected 1 retired slot, got %d", len(s.retiredSlots))
	}
	return s.retiredSlots[0].slot
}

func totalReadsForRoute(cols []MatrixColumn, routeID uint64) uint64 {
	var total uint64
	for _, c := range cols {
		for _, r := range c.Rows {
			if r.RouteID == routeID {
				total += r.Reads
			}
		}
	}
	return total
}

// TestRegisterFoldLowerStartReorders guards the sortedSlots ordering
// invariant: when an over-budget route folds into an existing virtual
// bucket and lowers the bucket's Start, the bucket must be repositioned
// in sortedSlots so Flush emits matrix rows in key order.
func TestRegisterFoldLowerStartReorders(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{
		Step:             time.Second,
		HistoryColumns:   4,
		MaxTrackedRoutes: 2,
	})
	mustRegister(t, s, 1, "m", "n")
	mustRegister(t, s, 2, "p", "q")
	// Route 3 is over budget — creates a virtual bucket at Start=r.
	if s.RegisterRoute(3, []byte("r"), []byte("s")) {
		t.Fatal("route 3 over budget should fold into virtual bucket")
	}
	// Route 4 is over budget AND has a Start ("a") below the bucket's
	// existing Start ("r"). The fold must lower bucket.Start to "a"
	// AND reposition the bucket within sortedSlots.
	if s.RegisterRoute(4, []byte("a"), []byte("b")) {
		t.Fatal("route 4 over budget should fold into virtual bucket")
	}
	s.Observe(1, OpRead, 0, 0)
	s.Observe(2, OpRead, 0, 0)
	s.Observe(3, OpRead, 0, 0)
	s.Observe(4, OpRead, 0, 0)
	s.Flush()

	rows := flushedRowsSorted(t, s)
	agg := findAggregateRow(t, rows)
	if string(agg.Start) != "a" {
		t.Fatalf("aggregate.Start = %q, want %q (fold did not lower Start)", agg.Start, "a")
	}
}

func mustRegister(t *testing.T, s *MemSampler, routeID uint64, start, end string) {
	t.Helper()
	if !s.RegisterRoute(routeID, []byte(start), []byte(end)) {
		t.Fatalf("RegisterRoute(%d) returned false", routeID)
	}
}

func flushedRowsSorted(t *testing.T, s *MemSampler) []MatrixRow {
	t.Helper()
	cols := s.Snapshot(time.Time{}, time.Time{})
	if len(cols) == 0 || len(cols[0].Rows) == 0 {
		t.Fatal("no rows after flush")
	}
	rows := cols[0].Rows
	for i := 1; i < len(rows); i++ {
		if !bytesLE(rows[i-1].Start, rows[i].Start) {
			t.Fatalf("rows not sorted: rows[%d].Start=%q > rows[%d].Start=%q",
				i-1, rows[i-1].Start, i, rows[i].Start)
		}
	}
	return rows
}

// TestSnapshotReturnsDeepCopy guards the public-API contract that the
// Snapshot result is fully owned by the caller: mutating row bounds or
// member-route slices must not corrupt later snapshots, and must not
// race with concurrent Flush/RegisterRoute.
func TestSnapshotReturnsDeepCopy(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	if !s.RegisterRoute(1, []byte("aaaa"), []byte("bbbb")) {
		t.Fatal("RegisterRoute(1) returned false")
	}
	s.Observe(1, OpRead, 1, 2)
	s.Flush()

	first := s.Snapshot(time.Time{}, time.Time{})
	if len(first) == 0 || len(first[0].Rows) == 0 {
		t.Fatalf("expected at least one row in first snapshot, got %+v", first)
	}
	first[0].Rows[0].Start[0] = 'X'
	first[0].Rows[0].End[0] = 'Y'
	first[0].Rows = nil

	second := s.Snapshot(time.Time{}, time.Time{})
	if len(second) == 0 || len(second[0].Rows) == 0 {
		t.Fatalf("second snapshot lost rows after caller mutation: %+v", second)
	}
	r := second[0].Rows[0]
	if string(r.Start) != "aaaa" || string(r.End) != "bbbb" {
		t.Fatalf("snapshot bounds aliased live state: start=%q end=%q", r.Start, r.End)
	}
}

// TestNonPositiveOptionsFallBackToDefaults pins Codex round-8 P2: a
// negative MaxTrackedRoutes used to bypass the zero-check and force
// every route into a virtual bucket. Confirm both zero and negative
// inputs land on the documented defaults so a bad CLI/env value
// doesn't silently destroy keyviz fidelity.
func TestNonPositiveOptionsFallBackToDefaults(t *testing.T) {
	t.Parallel()
	for _, val := range []int{0, -1, -10_000} {
		s, _ := newTestSampler(t, MemSamplerOptions{
			Step:                   time.Second,
			HistoryColumns:         val,
			MaxTrackedRoutes:       val,
			MaxMemberRoutesPerSlot: val,
		})
		if s.opts.HistoryColumns != DefaultHistoryColumns {
			t.Fatalf("HistoryColumns=%d → %d, want default %d", val, s.opts.HistoryColumns, DefaultHistoryColumns)
		}
		if s.opts.MaxTrackedRoutes != DefaultMaxTrackedRoutes {
			t.Fatalf("MaxTrackedRoutes=%d → %d, want default %d", val, s.opts.MaxTrackedRoutes, DefaultMaxTrackedRoutes)
		}
		if s.opts.MaxMemberRoutesPerSlot != DefaultMaxMemberRoutesPerSlot {
			t.Fatalf("MaxMemberRoutesPerSlot=%d → %d, want default %d", val, s.opts.MaxMemberRoutesPerSlot, DefaultMaxMemberRoutesPerSlot)
		}
	}
}

// TestStepAccessor pins the Step() accessor contract: returns the
// configured Step (after defaulting) for a constructed sampler, and
// returns DefaultStep for a typed nil so callers wiring RunFlusher
// against a disabled sampler don't crash.
func TestStepAccessor(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{Step: 250 * time.Millisecond})
	if got := s.Step(); got != 250*time.Millisecond {
		t.Fatalf("Step() = %v, want 250ms", got)
	}
	var nilSampler *MemSampler
	if got := nilSampler.Step(); got != DefaultStep {
		t.Fatalf("nil Step() = %v, want DefaultStep %v", got, DefaultStep)
	}
}

// TestMemberRoutesCappedAtConfiguredCap pins Codex round-7 P2: per-
// bucket MemberRoutes growth is bounded by MaxMemberRoutesPerSlot, so
// flushed columns don't scale with total folded routes when the
// deployment route count vastly exceeds MaxTrackedRoutes.
func TestMemberRoutesCappedAtConfiguredCap(t *testing.T) {
	t.Parallel()
	s, _ := newTestSampler(t, MemSamplerOptions{
		Step:                   time.Second,
		HistoryColumns:         4,
		MaxTrackedRoutes:       1,
		MaxMemberRoutesPerSlot: 3,
	})
	mustRegister(t, s, 1, "a", "b")
	for i := uint64(2); i < 10; i++ {
		key := []byte{byte('a' + i)}
		if s.RegisterRoute(i, key, append(key, 'z')) {
			t.Fatalf("route %d should fold (over budget)", i)
		}
		s.Observe(i, OpRead, 1, 0)
	}
	s.Flush()
	rows := lastSnapshotColumn(t, s).Rows
	agg := findAggregateRow(t, rows)
	if len(agg.MemberRoutes) > 3 {
		t.Fatalf("MemberRoutes exceeds cap=3: %v", agg.MemberRoutes)
	}
	// All 8 over-budget routes still drove the bucket counters even
	// though only the first 3 are recorded as members.
	if agg.Reads != 8 {
		t.Fatalf("bucket Reads = %d, want 8 (counters must absorb traffic past cap)", agg.Reads)
	}
}

// TestRetiredTailClearedAfterDrop pins Codex round-7 P2: after a
// retired slot's grace expires and drainRetiredSlots drops the entry,
// the *routeSlot pointer in the dropped tail of the backing array
// must be zeroed so it doesn't keep the slot GC-reachable through
// the reused capacity.
func TestRetiredTailClearedAfterDrop(t *testing.T) {
	t.Parallel()
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	mustRegister(t, s, 1, "a", "b")
	s.Observe(1, OpRead, 1, 1)
	s.RemoveRoute(1)

	s.retiredMu.Lock()
	if len(s.retiredSlots) != 1 {
		s.retiredMu.Unlock()
		t.Fatalf("expected 1 retired slot pre-flush, got %d", len(s.retiredSlots))
	}
	orig := s.retiredSlots[:1:1] // slice header pinning index 0 of the backing array
	s.retiredMu.Unlock()

	clk.Advance(s.graceWindow() + time.Second)
	s.Flush()

	s.retiredMu.Lock()
	leftover := len(s.retiredSlots)
	s.retiredMu.Unlock()
	if leftover != 0 {
		t.Fatalf("expected drain to drop entry, len=%d", leftover)
	}
	if orig[0].slot != nil {
		t.Fatal("dropped retiredSlot.slot pointer not zeroed in backing array — GC retention leak")
	}
}

// BenchmarkObserveHit pins the hot-path properties claimed in the
// package doc: a single atomic.Pointer.Load, a map lookup, and at
// most two atomic.AddUint64 calls — no allocation, no mutex. Use
// `go test -bench=BenchmarkObserveHit -benchmem ./keyviz/...` to
// catch regressions before they reach the coordinator wiring PR.
func BenchmarkObserveHit(b *testing.B) {
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	if !s.RegisterRoute(1, []byte("a"), []byte("b")) {
		b.Fatal("RegisterRoute(1) returned false")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Observe(1, OpRead, 16, 64)
	}
}

// BenchmarkObserveMiss exercises the unknown-route path so a future
// regression that grows allocations on misses (e.g. virtualForRoute
// fallback path) is caught.
func BenchmarkObserveMiss(b *testing.B) {
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Observe(99, OpRead, 16, 64)
	}
}
