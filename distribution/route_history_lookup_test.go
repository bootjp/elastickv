package distribution

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouteHistoryPointLookupMatchesScan(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	keys := [][]byte{nil, {0}, {0, 0xff}, {1}, {1, 0}, {1, 1}, {2}, {2, 0}, {3}, {0xff}, {0xff, 0}}
	for i, end := range [][]byte{{0xff}, nil} {
		routes := []RouteDescriptor{
			{RouteID: 1, GroupID: 7, Start: []byte{0}, End: []byte{1}, State: RouteStateActive},
			{RouteID: 2, GroupID: 8, Start: []byte{1}, End: []byte{1, 1}, State: RouteStateActive},
			{RouteID: 3, GroupID: 7, Start: []byte{2}, End: end, State: RouteStateActive},
		}
		version := uint64(i + 1)
		require.NoError(t, e.ApplySnapshot(CatalogSnapshot{Version: version, Routes: routes}))
		s, ok := e.SnapshotAt(version)
		require.True(t, ok)
		for _, key := range keys {
			want, found := firstCoveringDescriptor(routes, key)
			owner, hasOwner := s.OwnerOf(key)
			route, hasRoute := s.RouteOf(key)
			require.Equal(t, found, hasOwner, "version %d key %x", version, key)
			require.Equal(t, found, hasRoute, "version %d key %x", version, key)
			require.Equal(t, want.GroupID, owner)
			require.Equal(t, want.RouteID, route.RouteID)
		}
	}
}

func firstCoveringDescriptor(routes []RouteDescriptor, key []byte) (RouteDescriptor, bool) {
	for _, r := range routes {
		if bytes.Compare(key, r.Start) >= 0 && (r.End == nil || bytes.Compare(key, r.End) < 0) {
			return r, true
		}
	}
	return RouteDescriptor{}, false
}

func BenchmarkRouteHistoryOwnerOf(b *testing.B) {
	for _, n := range []int{1, 16, 256, 4096} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			e := NewEngine()
			routes := make([]RouteDescriptor, n)
			for i := range routes {
				start := binary.BigEndian.AppendUint64(nil, uint64(i))
				end := binary.BigEndian.AppendUint64(nil, uint64(i+1))
				routes[i] = RouteDescriptor{RouteID: uint64(i + 1), GroupID: 1, Start: start, End: end, State: RouteStateActive}
			}
			if err := e.ApplySnapshot(CatalogSnapshot{Version: 1, Routes: routes}); err != nil {
				b.Fatal(err)
			}
			s, _ := e.SnapshotAt(1)
			var i int
			for b.Loop() {
				_, _ = s.OwnerOf(routes[i].Start)
				i = (i + 73) % n
			}
		})
	}
}

func TestUpdateRouteOwnsPublishedBounds(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	if err := e.ApplySnapshot(CatalogSnapshot{Version: 1}); err != nil {
		t.Fatal(err)
	}
	start, end := []byte("b"), []byte("d")
	e.UpdateRoute(start, end, 7)
	if err := e.ApplyDelta(CatalogDelta{PreviousVersion: 1, Version: 2}); err != nil {
		t.Fatal(err)
	}
	start[0], end[0] = 'x', 'z'
	s, ok := e.SnapshotAt(2)
	if !ok {
		t.Fatal("missing snapshot")
	}
	if gid, ok := s.OwnerOf([]byte("c")); !ok || gid != 7 {
		t.Fatalf("expected published [b,d) to retain owner 7, got %d/%t", gid, ok)
	}
}
