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
// MemberRoutes list must drop that ID so later snapshots don't
// advertise stale members.
func TestRemoveVirtualMemberPrunesMemberRoutes(t *testing.T) {
	t.Parallel()
	s := setupVirtualBucketWithThreeMembers(t)
	s.RemoveRoute(2)
	s.Observe(3, OpRead, 1, 0)
	s.Flush()

	agg := findAggregateAcrossSnapshot(t, s.Snapshot(time.Time{}, time.Time{}))
	for _, m := range agg.MemberRoutes {
		if m == 2 {
			t.Fatalf("removed route 2 still in MemberRoutes: %v", agg.MemberRoutes)
		}
	}
	if len(agg.MemberRoutes) != 1 || agg.MemberRoutes[0] != 3 {
		t.Fatalf("MemberRoutes = %v, want [3]", agg.MemberRoutes)
	}
}

func setupVirtualBucketWithThreeMembers(t *testing.T) *MemSampler {
	t.Helper()
	s, _ := newTestSampler(t, MemSamplerOptions{
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
	return s
}

func findAggregateAcrossSnapshot(t *testing.T, cols []MatrixColumn) MatrixRow {
	t.Helper()
	for _, c := range cols {
		for _, r := range c.Rows {
			if r.Aggregate {
				return r
			}
		}
	}
	t.Fatal("no aggregate row in snapshot")
	return MatrixRow{}
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
