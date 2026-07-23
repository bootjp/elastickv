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
	for range 100 {
		ts := e.NextTimestamp()
		if ts <= last {
			t.Fatalf("timestamp not monotonic: %d <= %d", ts, last)
		}
		last = ts
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

func TestEngineApplySnapshot_PreservesMigrationRouteFields(t *testing.T) {
	t.Parallel()

	e := NewEngine()
	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{
				RouteID:                7,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                2,
				State:                  RouteStateMigratingTarget,
				StagedVisibilityActive: true,
				MigrationJobID:         42,
				MinWriteTSExclusive:    99,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	route, ok := e.GetRoute([]byte("m"))
	if !ok {
		t.Fatal("expected route")
	}
	requireMigrationRouteFields(t, "GetRoute", route)

	stats := e.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat route, got %d", len(stats))
	}
	requireMigrationRouteFields(t, "Stats", stats[0])

	intersections := e.GetIntersectingRoutes([]byte("b"), []byte("c"))
	if len(intersections) != 1 {
		t.Fatalf("expected 1 intersecting route, got %d", len(intersections))
	}
	requireMigrationRouteFields(t, "GetIntersectingRoutes", intersections[0])
}

func TestEngineApplyDelta_PreservesMigrationRouteFields(t *testing.T) {
	t.Parallel()

	e := NewEngine()
	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{
				RouteID: 1,
				Start:   []byte(""),
				End:     nil,
				GroupID: 1,
				State:   RouteStateActive,
			},
		},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	err := e.ApplyDelta(CatalogDelta{
		PreviousVersion: 1,
		Version:         2,
		Mutations: []CatalogRouteMutation{
			{Op: CatalogMutationDelete, RouteID: 1},
			{
				Op:      CatalogMutationUpsert,
				RouteID: 7,
				Route: RouteDescriptor{
					RouteID:                7,
					Start:                  []byte("a"),
					End:                    []byte("z"),
					GroupID:                2,
					State:                  RouteStateMigratingTarget,
					StagedVisibilityActive: true,
					MigrationJobID:         42,
					MinWriteTSExclusive:    99,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}

	route, ok := e.GetRoute([]byte("m"))
	if !ok {
		t.Fatal("expected route")
	}
	requireMigrationRouteFields(t, "GetRoute", route)

	stats := e.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat route, got %d", len(stats))
	}
	requireMigrationRouteFields(t, "Stats", stats[0])

	intersections := e.GetIntersectingRoutes([]byte("b"), []byte("c"))
	if len(intersections) != 1 {
		t.Fatalf("expected 1 intersecting route, got %d", len(intersections))
	}
	requireMigrationRouteFields(t, "GetIntersectingRoutes", intersections[0])
}

func requireMigrationRouteFields(t *testing.T, label string, route Route) {
	t.Helper()
	if !route.StagedVisibilityActive {
		t.Fatalf("%s lost staged visibility: %+v", label, route)
	}
	if route.MigrationJobID != 42 {
		t.Fatalf("%s lost migration job id: %+v", label, route)
	}
	if route.MinWriteTSExclusive != 99 {
		t.Fatalf("%s lost min write ts: %+v", label, route)
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

func TestEngineRouteLookupsReturnMatchingCatalogVersion(t *testing.T) {
	t.Parallel()

	e := NewEngine()
	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 12,
		Routes: []RouteDescriptor{
			{RouteID: 10, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
			{RouteID: 11, Start: []byte("m"), GroupID: 2, State: RouteStateActive},
		},
	}); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	route, version, ok := e.GetRouteWithVersion([]byte("z"))
	if !ok || route.GroupID != 2 || version != 12 {
		t.Fatalf("unexpected atomic route lookup: route=%+v version=%d ok=%v", route, version, ok)
	}

	routes, version := e.GetIntersectingRoutesWithVersion([]byte("a"), []byte("z"))
	if len(routes) != 2 || version != 12 {
		t.Fatalf("unexpected atomic range lookup: routes=%+v version=%d", routes, version)
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

func TestEngineApplySnapshot_RejectsDuplicateRouteStarts(t *testing.T) {
	e := NewEngine()

	err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: RouteStateActive},
			{RouteID: 2, Start: []byte("a"), End: nil, GroupID: 2, State: RouteStateActive},
		},
	})
	if !errors.Is(err, ErrEngineSnapshotRouteOrder) {
		t.Fatalf("expected ErrEngineSnapshotRouteOrder, got %v", err)
	}
}

func TestEngineApplySnapshot_EmptyRoutesClearsState(t *testing.T) {
	e := NewEngine()

	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: nil, GroupID: 1, State: RouteStateActive},
		},
	}); err != nil {
		t.Fatalf("apply initial snapshot: %v", err)
	}

	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 2,
		Routes:  []RouteDescriptor{},
	}); err != nil {
		t.Fatalf("apply empty snapshot: %v", err)
	}

	if got := e.Version(); got != 2 {
		t.Fatalf("expected version 2, got %d", got)
	}
	if got := len(e.Stats()); got != 0 {
		t.Fatalf("expected 0 routes after empty snapshot, got %d", got)
	}
	if _, ok := e.GetRoute([]byte("a")); ok {
		t.Fatal("expected no route after empty snapshot")
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
		wg.Go(func() {

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
		})
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

// TestEngineSnapshotAt_RecordsApplySnapshot is the M2 round-trip
// witness for the Composed-1 versioned-snapshot ring (design doc
// §M2): every successful ApplySnapshot records the (version, routes)
// pair so SnapshotAt(v) can resolve OwnerOf(k) for any in-flight
// transaction that observed v at BeginTxn.
func TestEngineSnapshotAt_RecordsApplySnapshot(t *testing.T) {
	e := NewEngine()
	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 10, Start: []byte(""), End: []byte("m"), GroupID: 7, State: RouteStateActive},
			{RouteID: 11, Start: []byte("m"), End: nil, GroupID: 9, State: RouteStateActive},
		},
	}); err != nil {
		t.Fatalf("apply snapshot v1: %v", err)
	}

	snap, ok := e.SnapshotAt(1)
	if !ok {
		t.Fatal("expected SnapshotAt(1) to return the v1 snapshot")
	}
	if snap.Version() != 1 {
		t.Fatalf("expected snapshot version 1, got %d", snap.Version())
	}
	if owner, found := snap.OwnerOf([]byte("a")); !found || owner != 7 {
		t.Fatalf("expected key 'a' owner=7 in v1 snapshot; got owner=%d found=%v", owner, found)
	}
	if owner, found := snap.OwnerOf([]byte("z")); !found || owner != 9 {
		t.Fatalf("expected key 'z' owner=9 in v1 snapshot; got owner=%d found=%v", owner, found)
	}
}

// TestEngineSnapshotAt_PreservesHistoryAcrossVersions verifies the
// M3-critical property: after ApplySnapshot has moved the catalog
// forward, a SnapshotAt for the PRIOR version still returns the old
// routes.  Without this, the M3 verifyComposed1 gate could not
// resolve a txn whose observedVer is behind the current catalog —
// exactly the case the design doc §3 G1c trace requires.
func TestEngineSnapshotAt_PreservesHistoryAcrossVersions(t *testing.T) {
	e := NewEngine()
	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 10, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
		},
	}); err != nil {
		t.Fatalf("apply v1: %v", err)
	}
	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 2,
		Routes: []RouteDescriptor{
			{RouteID: 11, Start: []byte(""), End: nil, GroupID: 2, State: RouteStateActive},
		},
	}); err != nil {
		t.Fatalf("apply v2: %v", err)
	}

	snapV1, ok := e.SnapshotAt(1)
	if !ok {
		t.Fatal("expected v1 still in history after v2 applied")
	}
	if owner, _ := snapV1.OwnerOf([]byte("k")); owner != 1 {
		t.Fatalf("v1 snapshot must still show group=1 owner; got %d", owner)
	}
	snapV2, ok := e.SnapshotAt(2)
	if !ok {
		t.Fatal("expected v2 in history")
	}
	if owner, _ := snapV2.OwnerOf([]byte("k")); owner != 2 {
		t.Fatalf("v2 snapshot must show group=2 owner; got %d", owner)
	}
}

// TestEngineSnapshotAt_FIFOEviction verifies that the ring respects
// historyDepth: once more than depth versions have been applied, the
// oldest is evicted and SnapshotAt returns (zero, false) for it.
// The M3 gate (design doc §4.3) treats the not-found case as a hard
// retryable error, so retention depth is a liveness knob — this
// test pins the eviction order so a future depth change does not
// silently break the M3 contract.
func TestEngineSnapshotAt_FIFOEviction(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	// Force a tiny depth so the test is bounded and explicit.  The
	// direct field write is safe because `e` is local to this test
	// goroutine and the depth is set before any ApplySnapshot fires;
	// once the Engine is published to concurrent readers, the depth
	// would have to flow through a constructor option (claude review
	// on PR #894 — fragile-but-test-local lock contract).
	e.historyDepth = 3

	for v := uint64(1); v <= 5; v++ {
		if err := e.ApplySnapshot(CatalogSnapshot{
			Version: v,
			Routes: []RouteDescriptor{
				{RouteID: v, Start: []byte(""), End: nil, GroupID: v, State: RouteStateActive},
			},
		}); err != nil {
			t.Fatalf("apply v%d: %v", v, err)
		}
	}

	if _, ok := e.SnapshotAt(1); ok {
		t.Fatal("v1 must be evicted (oldest) under depth=3 after v2..v5 applied")
	}
	if _, ok := e.SnapshotAt(2); ok {
		t.Fatal("v2 must be evicted (second-oldest) under depth=3 after v3..v5 applied")
	}
	for v := uint64(3); v <= 5; v++ {
		snap, ok := e.SnapshotAt(v)
		if !ok {
			t.Fatalf("expected v%d retained (depth=3 keeps the 3 most recent)", v)
		}
		if owner, _ := snap.OwnerOf([]byte("k")); owner != v {
			t.Fatalf("v%d snapshot must show group=%d owner; got %d", v, v, owner)
		}
	}
}

// TestEngineSnapshotAt_UnknownVersionReturnsNotFound documents the
// M3-relevant contract: a version that has never been applied (e.g.
// in the future, or a typo) returns (zero, false).  The M3 gate
// uses this signal to emit ErrComposed1VersionGCd and trigger a
// coordinator retry.
func TestEngineSnapshotAt_UnknownVersionReturnsNotFound(t *testing.T) {
	e := NewEngineWithDefaultRoute()
	if _, ok := e.SnapshotAt(42); ok {
		t.Fatal("expected SnapshotAt for an unknown version to return false")
	}
}

// TestEngineSnapshotAt_SeedsVersionZeroForDefaultRoute verifies that
// the NewEngineWithDefaultRoute path records the version-0 default
// route snapshot.  Without this, every txn that observed v=0 (the
// common case before any ApplySnapshot lands) would fall through
// to the M3 not-found path and trigger a spurious retry on its first
// commit.
func TestEngineSnapshotAt_SeedsVersionZeroForDefaultRoute(t *testing.T) {
	e := NewEngineWithDefaultRoute()
	snap, ok := e.SnapshotAt(0)
	if !ok {
		t.Fatal("expected NewEngineWithDefaultRoute to seed a v0 history entry")
	}
	if snap.Version() != 0 {
		t.Fatalf("expected seed snapshot version 0, got %d", snap.Version())
	}
	// Default route covers the full keyspace ⇒ every key resolves to
	// defaultGroupID at v0.
	owner, ok := snap.OwnerOf([]byte("anything"))
	if !ok || owner != defaultGroupID {
		t.Fatalf("expected v0 snapshot to resolve every key to defaultGroupID=%d; got owner=%d found=%v", defaultGroupID, owner, ok)
	}
}

// TestEngineSnapshotAt_BareEngineHasNoHistory documents that an
// Engine constructed via the bare struct literal (e.g. via internal
// test seams) has a nil history map and SnapshotAt always returns
// (zero, false).  recordHistorySnapshot is nil-safe so ApplySnapshot
// still works on such an engine, but the M3 gate will treat every
// SnapshotAt as "not in ring" → soft-fail-as-retry, which is the
// correct posture for unconfigured engines.
func TestEngineSnapshotAt_BareEngineHasNoHistory(t *testing.T) {
	e := &Engine{routes: make([]Route, 0)} // bare struct literal — no history
	if err := e.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes:  []RouteDescriptor{{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive}},
	}); err != nil {
		t.Fatalf("apply on bare engine should succeed: %v", err)
	}
	if _, ok := e.SnapshotAt(1); ok {
		t.Fatal("bare engine has no history ring; SnapshotAt should always be false")
	}
}
