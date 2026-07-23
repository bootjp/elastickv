package distribution

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCatalogDeltaCodecRoundTrip(t *testing.T) {
	t.Parallel()

	delta := CatalogDelta{
		PreviousVersion: 7,
		Version:         8,
		Mutations: []CatalogRouteMutation{
			{Op: CatalogMutationDelete, RouteID: 1},
			{
				Op:      CatalogMutationUpsert,
				RouteID: 2,
				Route: RouteDescriptor{
					RouteID: 2, Start: []byte("m"), GroupID: 3,
					State: RouteStateMigratingTarget, ParentRouteID: 1,
				},
			},
		},
	}

	raw, err := EncodeCatalogDelta(delta)
	require.NoError(t, err)
	decoded, err := DecodeCatalogDelta(raw)
	require.NoError(t, err)
	require.Equal(t, delta, decoded)
}

func TestCatalogDeltaRejectsDeleteRoutePayload(t *testing.T) {
	t.Parallel()

	for _, route := range []RouteDescriptor{
		{Start: []byte("unexpected")},
		{Start: []byte{}},
	} {
		_, err := EncodeCatalogDelta(CatalogDelta{
			PreviousVersion: 1,
			Version:         2,
			Mutations: []CatalogRouteMutation{{
				Op:      CatalogMutationDelete,
				RouteID: 1,
				Route:   route,
			}},
		})
		require.ErrorIs(t, err, ErrCatalogInvalidDeltaMutation)
	}
}

func TestCatalogDeltaFromProtoRejectsDeleteRoutePayload(t *testing.T) {
	t.Parallel()

	_, err := catalogDeltaFromProto(&pb.CatalogDeltaRecord{
		PreviousVersion: 1,
		Version:         2,
		Mutations: []*pb.CatalogDeltaMutation{{
			Op:      pb.CatalogDeltaMutationOp_CATALOG_DELTA_MUTATION_OP_DELETE,
			RouteId: 1,
			Route:   &pb.RouteDescriptor{},
		}},
	})
	require.ErrorIs(t, err, ErrCatalogWatchEventInvalid)
}

func TestCatalogStoreSavePublishesContiguousDeltas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	firstRoutes := []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, firstRoutes)
	require.NoError(t, err)
	secondRoutes := []RouteDescriptor{
		{RouteID: 2, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive, ParentRouteID: 1},
		{RouteID: 3, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive, ParentRouteID: 1},
	}
	second, err := catalog.Save(ctx, first.Version, secondRoutes)
	require.NoError(t, err)

	floor, err := catalog.DeltaFloor(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, floor)
	changes, err := catalog.ChangesSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Nil(t, changes.Reset)
	require.Len(t, changes.Deltas, 2)
	require.EqualValues(t, 0, changes.Deltas[0].PreviousVersion)
	require.EqualValues(t, 1, changes.Deltas[0].Version)
	require.EqualValues(t, 1, changes.Deltas[1].PreviousVersion)
	require.EqualValues(t, 2, changes.Deltas[1].Version)

	engine := NewEngineWithDefaultRoute()
	for _, delta := range changes.Deltas {
		require.NoError(t, engine.ApplyDelta(delta))
	}
	require.Equal(t, second.Version, engine.Version())
	routes := engine.Stats()
	require.Len(t, routes, 2)
	require.EqualValues(t, 2, routes[0].RouteID)
	require.EqualValues(t, 3, routes[1].RouteID)
}

func TestCatalogStoreChangesSinceFallsBackAfterRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	routes := []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	}
	version := uint64(0)
	for range DefaultCatalogDeltaRetention + 1 {
		snapshot, err := catalog.Save(ctx, version, routes)
		require.NoError(t, err)
		version = snapshot.Version
	}

	floor, err := catalog.DeltaFloor(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, floor)
	_, err = st.GetAt(ctx, CatalogDeltaKey(1), st.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	changes, err := catalog.ChangesSince(ctx, 0, 10)
	require.NoError(t, err)
	require.NotNil(t, changes.Reset)
	require.Equal(t, version, changes.Reset.Version)
	require.Equal(t, routes, changes.Reset.Routes)
	require.Empty(t, changes.Deltas)
}

func TestCatalogStoreChangesSinceRejectsFutureCursor(t *testing.T) {
	t.Parallel()

	catalog := NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.ChangesSince(context.Background(), 1, 1)
	require.ErrorIs(t, err, ErrCatalogDeltaVersionFuture)
}

func TestCatalogStoreBuildDeltaRejectsDurableBaseMismatch(t *testing.T) {
	t.Parallel()

	catalog := NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.BuildDeltaMutationsAt(context.Background(), 0, CatalogDelta{
		PreviousVersion: 1,
		Version:         2,
	})
	require.ErrorIs(t, err, ErrCatalogDeltaBaseMismatch)
}

func TestCatalogStoreChangesSinceRejectsMissingRetainedDelta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	routes := []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	second, err := catalog.Save(ctx, first.Version, routes)
	require.NoError(t, err)
	startTS := st.LastCommitTS()
	require.NoError(t, st.ApplyMutations(ctx, []*store.KVPairMutation{
		{Op: store.OpTypeDelete, Key: CatalogDeltaKey(second.Version)},
	}, nil, startTS, startTS+1))

	_, err = catalog.ChangesSince(ctx, first.Version, 1)
	require.ErrorIs(t, err, ErrCatalogDeltaVersionGap)
}

func TestEngineApplyDeltaRejectsGapWithoutPublishing(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	require.NoError(t, engine.ApplySnapshot(CatalogSnapshot{
		Version: 3,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
		},
	}))
	before := engine.Stats()
	err := engine.ApplyDelta(CatalogDelta{
		PreviousVersion: 2,
		Version:         3,
	})
	require.NoError(t, err, "replaying the published version is idempotent")
	err = engine.ApplyDelta(CatalogDelta{
		PreviousVersion: 4,
		Version:         5,
	})
	require.ErrorIs(t, err, ErrEngineDeltaVersionGap)
	require.EqualValues(t, 3, engine.Version())
	require.Equal(t, before, engine.Stats())
}

func TestEngineApplyDeltaRejectsOverlapWithoutPublishing(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	require.NoError(t, engine.ApplySnapshot(CatalogSnapshot{
		Version: 1,
		Routes: []RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: RouteStateActive},
			{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: RouteStateActive},
		},
	}))
	before := engine.Stats()
	err := engine.ApplyDelta(CatalogDelta{
		PreviousVersion: 1,
		Version:         2,
		Mutations: []CatalogRouteMutation{{
			Op:      CatalogMutationUpsert,
			RouteID: 2,
			Route: RouteDescriptor{
				RouteID: 2, Start: []byte("l"), End: nil, GroupID: 2, State: RouteStateActive,
			},
		}},
	})
	require.ErrorIs(t, err, ErrEngineSnapshotRouteOverlap)
	require.EqualValues(t, 1, engine.Version())
	require.Equal(t, before, engine.Stats())
}

func TestCatalogWatcherAppliesBoundedDeltaBatches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	routes := []RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	second, err := catalog.Save(ctx, first.Version, routes)
	require.NoError(t, err)

	engine := NewEngineWithDefaultRoute()
	watcher := NewCatalogWatcher(catalog, engine, WithCatalogWatcherBatchSize(1))
	require.NoError(t, watcher.SyncOnce(ctx))
	require.Equal(t, first.Version, engine.Version())
	require.NoError(t, watcher.SyncOnce(ctx))
	require.Equal(t, second.Version, engine.Version())
}
