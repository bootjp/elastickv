package distribution

import (
	"bytes"
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
	// ensure the key b can still be resolved after split
	r, ok := e.GetRoute([]byte("b"))
	if !ok {
		t.Fatalf("expected route for key b after split")
	}
	if r.End != nil && bytes.Compare([]byte("b"), r.End) >= 0 {
		t.Fatalf("route does not contain key b")
	}
}
