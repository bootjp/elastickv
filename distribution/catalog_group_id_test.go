package distribution

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDistributionCatalogGroupID_UsesCatalogKeyRoute(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 2)
	engine.UpdateRoute([]byte("m"), nil, 3)

	groupID, err := CatalogGroupID(engine)
	require.NoError(t, err)
	require.Equal(t, uint64(2), groupID)
}

func TestDistributionCatalogGroupID_FailsWhenRouteMissing(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 7)

	_, err := CatalogGroupID(engine)
	require.Error(t, err)
	require.ErrorContains(t, err, "no shard route for distribution catalog key")
}

func TestDistributionCatalogGroupID_FailsWhenEngineMissing(t *testing.T) {
	t.Parallel()

	_, err := CatalogGroupID(nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "distribution engine is required")
}
