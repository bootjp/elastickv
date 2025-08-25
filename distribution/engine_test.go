package distribution

import "testing"

func TestEngineRouteLookup(t *testing.T) {
	e := NewEngine()
	e.UpdateRoute([]byte("a"), 1)
	e.UpdateRoute([]byte("m"), 2)

	cases := []struct {
		key    []byte
		group  uint64
		expect bool
	}{
		{[]byte("0"), 0, false}, // before first route
		{[]byte("a"), 1, true},  // start is inclusive
		{[]byte("b"), 1, true},
		{[]byte("m"), 2, true}, // next start marks new range
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
