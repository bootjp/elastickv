package distribution

import (
	"bytes"
	"sync"
	"testing"
)

func TestEngineRouteLookup(t *testing.T) {
	e := NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	cases := []struct {
		key    []byte
		group  uint64
		expect bool
	}{
		{[]byte("0"), 0, false}, // before first route
		{[]byte("a"), 1, true},  // start is inclusive
		{[]byte("b"), 1, true},
		{[]byte("m"), 2, true}, // end is exclusive for first route
		{[]byte("x"), 2, true},
		{[]byte("za"), 2, true}, // last route is unbounded
	}

	for _, c := range cases {
		r, ok := e.GetRoute(c.key)
		if ok != c.expect {
			t.Fatalf("key %q expected ok=%v, got %v", c.key, c.expect, ok)
		}
		if ok && r.GroupID != c.group {
			t.Fatalf("key %q expected group %d, got %d", c.key, c.group, r.GroupID)
		}
	}
}

func TestEngineRouteUnmatchedAfterEnd(t *testing.T) {
	e := NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	if _, ok := e.GetRoute([]byte("x")); ok {
		t.Fatalf("expected no route for key beyond end")
	}
}

func TestEngineTimestampMonotonic(t *testing.T) {
	e := NewEngine()
	last := e.NextTimestamp()
	for i := 0; i < 100; i++ {
		ts := e.NextTimestamp()
		if ts <= last {
			t.Fatalf("timestamp not monotonic: %d <= %d", ts, last)
		}
		last = ts
	}
}

func TestEngineRecordAccessAndStats(t *testing.T) {
	e := NewEngineWithThreshold(0)
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.RecordAccess([]byte("b"))
	e.RecordAccess([]byte("b"))
	stats := e.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 route, got %d", len(stats))
	}
	if stats[0].Load != 2 {
		t.Fatalf("expected load 2, got %d", stats[0].Load)
	}
}

func TestEngineSplitOnHotspot(t *testing.T) {
	e := NewEngineWithThreshold(2)
	e.UpdateRoute([]byte("a"), []byte("c"), 1)
	e.RecordAccess([]byte("b"))
	e.RecordAccess([]byte("b"))
	stats := e.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 routes after split, got %d", len(stats))
	}
	midKey := []byte("a\x00")
	assertRange(t, stats[0], []byte("a"), midKey)
	assertRange(t, stats[1], midKey, []byte("c"))
	if stats[0].Load != 0 || stats[1].Load != 0 {
		t.Errorf("expected loads to be reset to 0, got %d, %d", stats[0].Load, stats[1].Load)
	}
	r, ok := e.GetRoute([]byte("b"))
	if !ok || (r.End != nil && bytes.Compare([]byte("b"), r.End) >= 0) {
		t.Fatalf("route does not contain key b")
	}
}

func TestEngineHotspotUnboundedResetsLoad(t *testing.T) {
	e := NewEngineWithThreshold(2)
	e.UpdateRoute([]byte("a"), nil, 1)
	e.RecordAccess([]byte("b"))
	e.RecordAccess([]byte("b"))
	stats := e.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 route, got %d", len(stats))
	}
	if stats[0].Load != 0 {
		t.Fatalf("expected load reset to 0, got %d", stats[0].Load)
	}
}

func TestEngineRecordAccessConcurrent(t *testing.T) {
	t.Parallel()
	e := NewEngineWithThreshold(6)
	e.UpdateRoute([]byte("a"), []byte("c"), 1)

	var wg sync.WaitGroup
	const workers = 10
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.RecordAccess([]byte("b"))
		}()
	}
	wg.Wait()

	stats := e.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 routes after concurrent split, got %d", len(stats))
	}
}

func TestNewEngineWithDefaultRoute(t *testing.T) {
	e := NewEngineWithDefaultRoute()
	stats := e.Stats()

	if len(stats) != 1 {
		t.Fatalf("expected 1 route, got %d", len(stats))
	}

	r := stats[0]
	if r.GroupID != defaultGroupID {
		t.Fatalf("expected group ID %d, got %d", defaultGroupID, r.GroupID)
	}
	if !bytes.Equal(r.Start, []byte("")) {
		t.Fatalf("expected start of keyspace (empty slice), got %q", r.Start)
	}
	if r.End != nil {
		t.Fatalf("expected end of keyspace (nil), got %q", r.End)
	}

	route, ok := e.GetRoute([]byte("any-key"))
	if !ok {
		t.Fatal("GetRoute should find the default route")
	}
	if route.GroupID != defaultGroupID {
		t.Fatalf("GetRoute: expected group ID %d, got %d", defaultGroupID, route.GroupID)
	}
}

func assertRange(t *testing.T, r Route, start, end []byte) {
	t.Helper()
	if !bytes.Equal(r.Start, start) || !bytes.Equal(r.End, end) {
		t.Errorf("expected range [%q, %q), got [%q, %q]", start, end, r.Start, r.End)
	}
}

func TestEngineGetIntersectingRoutes(t *testing.T) {
	e := NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), []byte("z"), 2)
	e.UpdateRoute([]byte("z"), nil, 3)

	cases := []struct {
		name   string
		start  []byte
		end    []byte
		groups []uint64
	}{
		{
			name:   "scan in first range",
			start:  []byte("b"),
			end:    []byte("d"),
			groups: []uint64{1},
		},
		{
			name:   "scan across first two ranges",
			start:  []byte("k"),
			end:    []byte("p"),
			groups: []uint64{1, 2},
		},
		{
			name:   "scan across all ranges",
			start:  []byte("a"),
			end:    nil,
			groups: []uint64{1, 2, 3},
		},
		{
			name:   "scan in last unbounded range",
			start:  []byte("za"),
			end:    nil,
			groups: []uint64{3},
		},
		{
			name:   "scan before first range",
			start:  []byte("0"),
			end:    []byte("9"),
			groups: []uint64{},
		},
		{
			name:   "scan at boundary",
			start:  []byte("m"),
			end:    []byte("n"),
			groups: []uint64{2},
		},
		{
			name:   "scan ending at boundary",
			start:  []byte("k"),
			end:    []byte("m"),
			groups: []uint64{1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			routes := e.GetIntersectingRoutes(c.start, c.end)
			if len(routes) != len(c.groups) {
				t.Fatalf("expected %d routes, got %d", len(c.groups), len(routes))
			}
			for i, expectedGroup := range c.groups {
				if routes[i].GroupID != expectedGroup {
					t.Errorf("route %d: expected group %d, got %d", i, expectedGroup, routes[i].GroupID)
				}
			}
		})
	}
}
