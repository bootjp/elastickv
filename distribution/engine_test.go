package distribution

import "testing"

func TestEngineRouteLookup(t *testing.T) {
	e := NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), []byte("z"), 2)

	r, ok := e.GetRoute([]byte("b"))
	if !ok || r.GroupID != 1 {
		t.Fatalf("expected group 1, got %v", r.GroupID)
	}

	r, ok = e.GetRoute([]byte("x"))
	if !ok || r.GroupID != 2 {
		t.Fatalf("expected group 2, got %v", r.GroupID)
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
