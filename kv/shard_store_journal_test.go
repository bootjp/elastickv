package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/stretchr/testify/require"
)

func TestShardStoreJournalSingleGroupAtVersion(t *testing.T) {
	engine := distribution.NewEngineWithDefaultRoute()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte{}, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	shards := NewShardStore(engine, nil)
	initial := EncodeObservedRouteVersion(engine.Version())
	require.True(t, shards.JournalSingleGroupAtVersion(initial))
	require.False(t, shards.JournalSingleGroupAtVersion(0))

	shards.WithPartitionResolver(&fakePartitionResolver{})
	require.False(t, shards.JournalSingleGroupAtVersion(initial))
	shards.WithPartitionResolver(nil)
	require.True(t, shards.JournalSingleGroupAtVersion(initial))

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte{}, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	require.False(t, shards.JournalSingleGroupAtVersion(initial))
	require.False(t, shards.JournalSingleGroupAtVersion(EncodeObservedRouteVersion(engine.Version())))
}
