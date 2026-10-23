package adapter

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestSplitRangeRefreshesDisjointNativeOwnersBeforeReturning(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	before, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	parent := splitUnrelatedOwnerParent(t, before)
	_, err = r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: parent,
		SplitKey: []byte("g"), ExpectedCatalogVersion: before.Version})
	require.NoError(t, err)
	// Neither native owner shares the split parent's group. No background
	// refresh runs between catalog publication and these foreground writes.
	require.NoError(t, r.put(t, 2, "n", "disjoint-owner-two", 1<<40))
	require.NoError(t, r.put(t, 3, "y", "disjoint-owner-three", 1<<40))
	r.read(t, "n", "disjoint-owner-two")
	r.read(t, "y", "disjoint-owner-three")
}

func TestSplitRangeRequiresDisjointOwnerSupportBeforeCatalogMutation(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	before, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	parent := splitUnrelatedOwnerParent(t, before)
	r.clients[3].internal.migrationPartitionProtocolReady = func() bool { return false }
	_, err = r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: parent,
		SplitKey: []byte("g"), ExpectedCatalogVersion: before.Version})
	require.Error(t, err)
	after, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Routes, after.Routes)
}

func splitUnrelatedOwnerParent(t *testing.T, snapshot distribution.CatalogSnapshot) uint64 {
	t.Helper()
	for _, route := range snapshot.Routes {
		if route.GroupID == 1 && bytes.Equal(route.End, []byte("m")) {
			return route.RouteID
		}
	}
	t.Fatal("retained proof-free parent is absent")
	return 0
}
