package distribution

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
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

func TestEngineApplySnapshot_ReplacesRoutesAndVersion(t *testing.T) {
	e := NewEngine()
	e.UpdateRoute([]byte("a"), []byte("z"), 1)

	if got := e.Version(); got != 0 {
		t.Fatalf("expected initial version 0, got %d", got)
	}

	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 10, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
			{RouteID: 11, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateWriteFenced},
		},
	})
	if err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	if got := e.Version(); got != 1 {
		t.Fatalf("expected version 1, got %d", got)
	}

	stats := e.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(stats))
	}
	if stats[0].RouteID != 10 || stats[0].State != RouteStateActive {
		t.Fatalf("unexpected first route metadata: %+v", stats[0])
	}
	if stats[1].RouteID != 11 || stats[1].State != RouteStateWriteFenced {
		t.Fatalf("unexpected second route metadata: %+v", stats[1])
	}
}

func TestEngineApplySnapshot_RejectsOldVersion(t *testing.T) {
	e := NewEngine()

	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 2,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
		},
	}); err != nil {
		t.Fatalf("first apply snapshot: %v", err)
	}

	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 2, Start: []byte(""), End: nil, GroupID: 9, State: RouteStateActive},
		},
	})
	if !errors.Is(err, ErrEngineSnapshotVersionStale) {
		t.Fatalf("expected ErrEngineSnapshotVersionStale, got %v", err)
	}
	if !strings.Contains(err.Error(), "snapshot version 1") || !strings.Contains(err.Error(), "engine catalog version 2") {
		t.Fatalf("expected stale error to include version context, got %v", err)
	}

	route, ok := e.GetRoute([]byte("k"))
	if !ok {
		t.Fatal("expected route after stale apply")
	}
	if route.GroupID != 1 || route.RouteID != 1 {
		t.Fatalf("expected route to remain unchanged, got %+v", route)
	}
}

func TestEngineApplySnapshot_RejectsDuplicateRouteIDs(t *testing.T) {
	e := NewEngine()

	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 10, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
			{RouteID: 10, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
		},
	})
	if !errors.Is(err, ErrEngineSnapshotDuplicateID) {
		t.Fatalf("expected ErrEngineSnapshotDuplicateID, got %v", err)
	}
}

func TestEngineApplySnapshot_RejectsOverlappingRoutes(t *testing.T) {
	e := NewEngine()

	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("n"), GroupID: 1, State: RouteStateActive},
			{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
		},
	})
	if !errors.Is(err, ErrEngineSnapshotRouteOverlap) {
		t.Fatalf("expected ErrEngineSnapshotRouteOverlap, got %v", err)
	}
}

func TestEngineApplySnapshot_RejectsInvalidRouteOrder(t *testing.T) {
	e := NewEngine()

	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
			{RouteID: 2, Start: []byte("m"), End: []byte("z"), GroupID: 2, State: RouteStateActive},
		},
	})
	if !errors.Is(err, ErrEngineSnapshotRouteOrder) {
		t.Fatalf("expected ErrEngineSnapshotRouteOrder, got %v", err)
	}
}

func TestEngineApplySnapshot_LookupBehavior(t *testing.T) {
	e := NewEngine()
	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: RouteStateActive},
			{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
		},
	})
	if err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	cases := []struct {
		key    []byte
		group  uint64
		expect bool
	}{
		{[]byte("0"), 0, false},
		{[]byte("a"), 1, true},
		{[]byte("b"), 1, true},
		{[]byte("m"), 2, true},
		{[]byte("x"), 2, true},
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

func TestEngineApplySnapshot_Concurrent(t *testing.T) {
	t.Parallel()

	e := NewEngine()
	const maxVersion uint64 = 20

	var wg sync.WaitGroup
	errs := make(chan error, maxVersion)

	for version := uint64(1); version <= maxVersion; version++ {
		v := version
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := e.ApplySnapshot(CatalogSnapshot{
				Version: v,
				Routes: []RouteDescriptor{
					{
						RouteID: 1,
						Start:   []byte("a"),
						End:     nil,
						GroupID: v,
						State:   RouteStateActive,
					},
				},
			})
			if err != nil && !errors.Is(err, ErrEngineSnapshotVersionStale) {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected apply snapshot error: %v", err)
	}

	if got := e.Version(); got != maxVersion {
		t.Fatalf("expected final version %d, got %d", maxVersion, got)
	}

	route, ok := e.GetRoute([]byte("a"))
	if !ok {
		t.Fatal("expected route after concurrent snapshots")
	}
	if route.GroupID != maxVersion {
		t.Fatalf("expected route group %d, got %d", maxVersion, route.GroupID)
	}
}
