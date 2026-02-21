package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestDistributionCatalogGroupID_UsesCatalogKeyRoute(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 2)
	engine.UpdateRoute([]byte("m"), nil, 3)

	groupID, err := distributionCatalogGroupID(engine)
	require.NoError(t, err)
	require.Equal(t, uint64(2), groupID)
}

func TestDistributionCatalogGroupID_FailsWhenRouteMissing(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 7)

	_, err := distributionCatalogGroupID(engine)
	require.Error(t, err)
	require.ErrorContains(t, err, "no shard route for distribution catalog key")
}

func TestDistributionCatalogGroupID_FailsWhenEngineMissing(t *testing.T) {
	t.Parallel()

	_, err := distributionCatalogGroupID(nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "distribution engine is required")
}

func TestSetupDistributionCatalog_FailsWhenRuntimeMissingForResolvedGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 9)

	_, err := setupDistributionCatalog(context.Background(), []*raftGroupRuntime{}, engine)
	require.Error(t, err)
	require.ErrorContains(t, err, "distribution catalog store is not available for group 9")
}

func TestSetupDistributionCatalog_UsesResolvedCatalogGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 5)
	rt := &raftGroupRuntime{
		spec:  groupSpec{id: 5},
		store: store.NewMVCCStore(),
	}
	t.Cleanup(func() {
		rt.Close()
	})

	catalog, err := setupDistributionCatalog(context.Background(), []*raftGroupRuntime{rt}, engine)
	require.NoError(t, err)
	require.NotNil(t, catalog)
}
