package distribution

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func TestCatalogVersionCodecRoundTrip(t *testing.T) {
	raw := EncodeCatalogVersion(42)
	got, err := DecodeCatalogVersion(raw)
	if err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestCatalogVersionCodecRejectsInvalidPayload(t *testing.T) {
	if _, err := DecodeCatalogVersion(nil); !errors.Is(err, ErrCatalogInvalidVersionRecord) {
		t.Fatalf("expected ErrCatalogInvalidVersionRecord, got %v", err)
	}
	if _, err := DecodeCatalogVersion([]byte{99, 0, 0, 0, 0, 0, 0, 0, 1}); !errors.Is(err, ErrCatalogInvalidVersionRecord) {
		t.Fatalf("expected ErrCatalogInvalidVersionRecord, got %v", err)
	}
}

func TestCatalogNextRouteIDCodecRoundTrip(t *testing.T) {
	raw := EncodeCatalogNextRouteID(77)
	got, err := DecodeCatalogNextRouteID(raw)
	if err != nil {
		t.Fatalf("decode next route id: %v", err)
	}
	if got != 77 {
		t.Fatalf("expected 77, got %d", got)
	}
}

func TestCatalogNextRouteIDCodecRejectsInvalidPayload(t *testing.T) {
	if _, err := DecodeCatalogNextRouteID(nil); !errors.Is(err, ErrCatalogInvalidNextRouteID) {
		t.Fatalf("expected ErrCatalogInvalidNextRouteID, got %v", err)
	}
	if _, err := DecodeCatalogNextRouteID(EncodeCatalogVersion(0)); !errors.Is(err, ErrCatalogInvalidNextRouteID) {
		t.Fatalf("expected ErrCatalogInvalidNextRouteID, got %v", err)
	}
}

func TestRouteDescriptorCodecRoundTrip(t *testing.T) {
	route := RouteDescriptor{
		RouteID:       7,
		Start:         []byte("a"),
		End:           []byte("m"),
		GroupID:       3,
		State:         RouteStateWriteFenced,
		ParentRouteID: 2,
	}
	raw, err := EncodeRouteDescriptor(route)
	if err != nil {
		t.Fatalf("encode route: %v", err)
	}
	got, err := DecodeRouteDescriptor(raw)
	if err != nil {
		t.Fatalf("decode route: %v", err)
	}
	assertRouteEqual(t, route, got)
}

func TestRouteDescriptorCodecRoundTripNilEnd(t *testing.T) {
	route := RouteDescriptor{
		RouteID:       9,
		Start:         []byte("m"),
		End:           nil,
		GroupID:       2,
		State:         RouteStateActive,
		ParentRouteID: 0,
	}
	raw, err := EncodeRouteDescriptor(route)
	if err != nil {
		t.Fatalf("encode route: %v", err)
	}
	got, err := DecodeRouteDescriptor(raw)
	if err != nil {
		t.Fatalf("decode route: %v", err)
	}
	assertRouteEqual(t, route, got)
}

func TestRouteDescriptorCodecRejectsTrailingBytes(t *testing.T) {
	route := RouteDescriptor{
		RouteID:       1,
		Start:         []byte("a"),
		End:           []byte("m"),
		GroupID:       1,
		State:         RouteStateActive,
		ParentRouteID: 0,
	}
	raw, err := EncodeRouteDescriptor(route)
	if err != nil {
		t.Fatalf("encode route: %v", err)
	}
	raw = append(raw, 0xff)

	_, err = DecodeRouteDescriptor(raw)
	if !errors.Is(err, ErrCatalogInvalidRouteRecord) {
		t.Fatalf("expected ErrCatalogInvalidRouteRecord, got %v", err)
	}
}

func TestCatalogRouteKeyHelpers(t *testing.T) {
	key := CatalogRouteKey(11)
	if !IsCatalogRouteKey(key) {
		t.Fatal("expected route key prefix match")
	}
	id, ok := CatalogRouteIDFromKey(key)
	if !ok {
		t.Fatal("expected route id parse to succeed")
	}
	if id != 11 {
		t.Fatalf("expected route id 11, got %d", id)
	}
	if _, ok := CatalogRouteIDFromKey([]byte("not-a-route-key")); ok {
		t.Fatal("expected parse failure for non-route key")
	}
}

func TestCatalogStoreSnapshotEmpty(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	snapshot, err := cs.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Version != 0 {
		t.Fatalf("expected empty version 0, got %d", snapshot.Version)
	}
	if len(snapshot.Routes) != 0 {
		t.Fatalf("expected no routes, got %d", len(snapshot.Routes))
	}
	if snapshot.ReadTS != 0 {
		t.Fatalf("expected empty read ts 0, got %d", snapshot.ReadTS)
	}
}

func TestCatalogStoreSaveAndSnapshot(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()

	saved, err := cs.Save(ctx, 0, []RouteDescriptor{
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         RouteStateActive,
			ParentRouteID: 1,
		},
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if saved.Version != 1 {
		t.Fatalf("expected version 1, got %d", saved.Version)
	}
	if len(saved.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(saved.Routes))
	}
	if !bytes.Equal(saved.Routes[0].Start, []byte("")) || !bytes.Equal(saved.Routes[1].Start, []byte("m")) {
		t.Fatalf("expected routes sorted by start key, got starts [%q,%q]", saved.Routes[0].Start, saved.Routes[1].Start)
	}

	snapshot, err := cs.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Version != 1 {
		t.Fatalf("expected version 1, got %d", snapshot.Version)
	}
	if snapshot.ReadTS == 0 {
		t.Fatal("expected snapshot read ts to be set")
	}
	if len(snapshot.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(snapshot.Routes))
	}
	assertRouteEqual(t, saved.Routes[0], snapshot.Routes[0])
	assertRouteEqual(t, saved.Routes[1], snapshot.Routes[1])
}

func TestCatalogStoreSaveAndSnapshotSortsRoutesByStart(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()

	saved, err := cs.Save(ctx, 0, []RouteDescriptor{
		{
			RouteID:       10,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       20,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if len(saved.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(saved.Routes))
	}
	if saved.Routes[0].RouteID != 20 || saved.Routes[1].RouteID != 10 {
		t.Fatalf("expected route order by start key [20,10], got [%d,%d]", saved.Routes[0].RouteID, saved.Routes[1].RouteID)
	}

	snapshot, err := cs.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Routes) != 2 {
		t.Fatalf("expected 2 routes in snapshot, got %d", len(snapshot.Routes))
	}
	if snapshot.Routes[0].RouteID != 20 || snapshot.Routes[1].RouteID != 10 {
		t.Fatalf("expected snapshot route order by start key [20,10], got [%d,%d]", snapshot.Routes[0].RouteID, snapshot.Routes[1].RouteID)
	}
}

func TestCatalogStoreNextRouteID_DefaultsToOne(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()

	next, err := cs.NextRouteID(ctx)
	if err != nil {
		t.Fatalf("next route id (empty): %v", err)
	}
	if next != 1 {
		t.Fatalf("expected empty next route id 1, got %d", next)
	}
}

func TestCatalogStoreNextRouteID_RemainsAfterDelete(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()

	first, err := cs.Save(ctx, 0, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("first save: %v", err)
	}
	if first.Version != 1 {
		t.Fatalf("expected version 1, got %d", first.Version)
	}

	next, err := cs.NextRouteID(ctx)
	if err != nil {
		t.Fatalf("next route id after first save: %v", err)
	}
	if next != 3 {
		t.Fatalf("expected next route id 3, got %d", next)
	}

	_, err = cs.Save(ctx, first.Version, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("second save: %v", err)
	}

	next, err = cs.NextRouteID(ctx)
	if err != nil {
		t.Fatalf("next route id after delete: %v", err)
	}
	if next != 3 {
		t.Fatalf("expected next route id to remain 3 after delete, got %d", next)
	}
}

func TestCatalogStoreNextRouteID_TracksHigherRouteID(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()

	first, err := cs.Save(ctx, 0, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("first save: %v", err)
	}

	second, err := cs.Save(ctx, first.Version, []RouteDescriptor{
		{RouteID: 10, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if second.Version != 2 {
		t.Fatalf("expected version 2, got %d", second.Version)
	}

	next, err := cs.NextRouteID(ctx)
	if err != nil {
		t.Fatalf("next route id after high id save: %v", err)
	}
	if next != 11 {
		t.Fatalf("expected next route id 11, got %d", next)
	}
}

func TestNextRouteIDFloor_RejectsOverflow(t *testing.T) {
	_, err := NextRouteIDFloor([]RouteDescriptor{
		{RouteID: math.MaxUint64},
	})
	if !errors.Is(err, ErrCatalogRouteIDOverflow) {
		t.Fatalf("expected ErrCatalogRouteIDOverflow, got %v", err)
	}
}

func TestCatalogStoreSaveRejectsVersionMismatch(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	routes := []RouteDescriptor{
		{
			RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive,
		},
	}

	if _, err := cs.Save(ctx, 0, routes); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if _, err := cs.Save(ctx, 0, routes); !errors.Is(err, ErrCatalogVersionMismatch) {
		t.Fatalf("expected ErrCatalogVersionMismatch, got %v", err)
	}
}

func TestCatalogStoreSaveRejectsDuplicateRouteIDs(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	_, err := cs.Save(ctx, 0, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
		{RouteID: 1, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
	})
	if !errors.Is(err, ErrCatalogDuplicateRouteID) {
		t.Fatalf("expected ErrCatalogDuplicateRouteID, got %v", err)
	}
}

func TestCatalogStoreSaveRejectsInvalidRouteRange(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	_, err := cs.Save(ctx, 0, []RouteDescriptor{
		{RouteID: 1, Start: []byte("z"), End: []byte("a"), GroupID: 1, State: RouteStateActive},
	})
	if !errors.Is(err, ErrCatalogInvalidRouteRange) {
		t.Fatalf("expected ErrCatalogInvalidRouteRange, got %v", err)
	}
}

func TestCatalogStoreSaveDeletesRemovedRoutes(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()

	_, err := cs.Save(ctx, 0, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("first save: %v", err)
	}

	_, err = cs.Save(ctx, 1, []RouteDescriptor{
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("second save: %v", err)
	}

	snapshot, err := cs.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Version != 2 {
		t.Fatalf("expected version 2, got %d", snapshot.Version)
	}
	if len(snapshot.Routes) != 1 {
		t.Fatalf("expected 1 route after delete, got %d", len(snapshot.Routes))
	}
	if snapshot.Routes[0].RouteID != 2 {
		t.Fatalf("expected remaining route id 2, got %d", snapshot.Routes[0].RouteID)
	}
}

func TestCatalogStoreSaveDoesNotRewriteUnchangedRoutes(t *testing.T) {
	st := store.NewMVCCStore()
	cs := NewCatalogStore(st)
	ctx := context.Background()

	routes := []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
	}
	_, err := cs.Save(ctx, 0, routes)
	if err != nil {
		t.Fatalf("first save: %v", err)
	}

	route1FirstTS, exists, err := st.LatestCommitTS(ctx, CatalogRouteKey(1))
	if err != nil {
		t.Fatalf("route 1 latest ts (first): %v", err)
	}
	if !exists {
		t.Fatal("expected route 1 key to exist")
	}

	_, err = cs.Save(ctx, 1, routes)
	if err != nil {
		t.Fatalf("second save: %v", err)
	}

	route1SecondTS, exists, err := st.LatestCommitTS(ctx, CatalogRouteKey(1))
	if err != nil {
		t.Fatalf("route 1 latest ts (second): %v", err)
	}
	if !exists {
		t.Fatal("expected route 1 key to exist after second save")
	}
	if route1SecondTS != route1FirstTS {
		t.Fatalf("expected unchanged route key commit ts to remain %d, got %d", route1FirstTS, route1SecondTS)
	}
}

func TestCatalogStoreSaveRejectsVersionOverflow(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()
	// Prepare current catalog version == max uint64.
	if err := st.PutAt(ctx, CatalogVersionKey(), EncodeCatalogVersion(^uint64(0)), 1, 0); err != nil {
		t.Fatalf("prepare version key: %v", err)
	}
	cs := NewCatalogStore(st)
	_, err := cs.Save(ctx, ^uint64(0), []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	})
	if !errors.Is(err, ErrCatalogVersionOverflow) {
		t.Fatalf("expected ErrCatalogVersionOverflow, got %v", err)
	}
}

func TestCatalogStoreSaveRejectsCommitTSOverflow(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()

	// Set catalog version at a high commit TS.
	if err := st.PutAt(ctx, CatalogVersionKey(), EncodeCatalogVersion(10), ^uint64(0)-1, 0); err != nil {
		t.Fatalf("prepare version key: %v", err)
	}
	// Push LastCommitTS to max uint64 so prepareSave overflows readTS+1.
	if err := st.PutAt(ctx, []byte("!dist|meta|sentinel"), []byte("x"), ^uint64(0), 0); err != nil {
		t.Fatalf("prepare sentinel key: %v", err)
	}

	cs := NewCatalogStore(st)
	_, err := cs.Save(ctx, 10, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	})
	if !errors.Is(err, ErrCatalogVersionOverflow) {
		t.Fatalf("expected ErrCatalogVersionOverflow, got %v", err)
	}
}

func TestCatalogStoreApplySaveMutations_UsesMonotonicCommitTS(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()
	cs := NewCatalogStore(st)

	plan, err := cs.prepareSave(ctx, 0, []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	})
	if err != nil {
		t.Fatalf("prepareSave: %v", err)
	}

	// Simulate unrelated writes advancing the global LastCommitTS after planning.
	advancedTS := plan.minCommitTS + 50
	if err := st.PutAt(ctx, []byte("unrelated"), []byte("v"), advancedTS, 0); err != nil {
		t.Fatalf("advance LastCommitTS: %v", err)
	}

	mutations, err := cs.buildSaveMutations(ctx, plan)
	if err != nil {
		t.Fatalf("buildSaveMutations: %v", err)
	}
	if err := cs.applySaveMutations(ctx, plan, mutations); err != nil {
		t.Fatalf("applySaveMutations: %v", err)
	}

	ts, exists, err := st.LatestCommitTS(ctx, CatalogVersionKey())
	if err != nil {
		t.Fatalf("LatestCommitTS(version key): %v", err)
	}
	if !exists {
		t.Fatal("expected catalog version key to exist")
	}
	if ts <= advancedTS {
		t.Fatalf("expected catalog commit TS to be > %d, got %d", advancedTS, ts)
	}
}

func assertRouteEqual(t *testing.T, want, got RouteDescriptor) {
	t.Helper()
	if want.RouteID != got.RouteID {
		t.Fatalf("route id mismatch: want %d, got %d", want.RouteID, got.RouteID)
	}
	if want.GroupID != got.GroupID {
		t.Fatalf("group id mismatch: want %d, got %d", want.GroupID, got.GroupID)
	}
	if want.ParentRouteID != got.ParentRouteID {
		t.Fatalf("parent route id mismatch: want %d, got %d", want.ParentRouteID, got.ParentRouteID)
	}
	if want.State != got.State {
		t.Fatalf("state mismatch: want %d, got %d", want.State, got.State)
	}
	if !bytes.Equal(want.Start, got.Start) {
		t.Fatalf("start mismatch: want %q, got %q", want.Start, got.Start)
	}
	if !bytes.Equal(want.End, got.End) {
		t.Fatalf("end mismatch: want %q, got %q", want.End, got.End)
	}
}
