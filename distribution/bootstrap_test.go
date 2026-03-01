package distribution

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestEnsureCatalogSnapshot_SeedsEmptyCatalogFromEngine(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	engine := NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	snapshot, err := EnsureCatalogSnapshot(ctx, catalog, engine)
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapshot.Version)
	require.Len(t, snapshot.Routes, 2)
	require.Equal(t, uint64(1), snapshot.Routes[0].RouteID)
	require.Equal(t, uint64(2), snapshot.Routes[1].RouteID)

	left, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(1), left.RouteID)
	require.Equal(t, uint64(1), left.GroupID)
	right, ok := engine.GetRoute([]byte("x"))
	require.True(t, ok)
	require.Equal(t, uint64(2), right.RouteID)
	require.Equal(t, uint64(2), right.GroupID)
}

func TestEnsureCatalogSnapshot_UsesExistingCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []RouteDescriptor{
		{RouteID: 10, Start: []byte(""), End: []byte("m"), GroupID: 7, State: RouteStateWriteFenced},
		{RouteID: 11, Start: []byte("m"), End: nil, GroupID: 9, State: RouteStateActive},
	})
	require.NoError(t, err)

	engine := NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	snapshot, err := EnsureCatalogSnapshot(ctx, catalog, engine)
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapshot.Version)
	require.Len(t, snapshot.Routes, 2)
	require.Equal(t, uint64(10), snapshot.Routes[0].RouteID)
	require.Equal(t, uint64(11), snapshot.Routes[1].RouteID)

	left, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(10), left.RouteID)
	require.Equal(t, uint64(7), left.GroupID)
	require.Equal(t, RouteStateWriteFenced, left.State)
}
