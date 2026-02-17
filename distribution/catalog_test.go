package distribution

import (
	"bytes"
	"context"
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
	if saved.Routes[0].RouteID != 1 || saved.Routes[1].RouteID != 2 {
		t.Fatalf("expected sorted route ids [1,2], got [%d,%d]", saved.Routes[0].RouteID, saved.Routes[1].RouteID)
	}

	snapshot, err := cs.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Version != 1 {
		t.Fatalf("expected version 1, got %d", snapshot.Version)
	}
	if len(snapshot.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(snapshot.Routes))
	}
	assertRouteEqual(t, saved.Routes[0], snapshot.Routes[0])
	assertRouteEqual(t, saved.Routes[1], snapshot.Routes[1])
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
