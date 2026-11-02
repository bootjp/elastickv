package distribution

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Only the two snapshot-read methods are exposed. A local timestamp lookup or
// any direct mutation in the planner calls the nil embedded interface and fails.
type bootstrapPlanReadOnlyStore struct {
	store.MVCCStore
	t       *testing.T
	inner   store.MVCCStore
	readTS  uint64
	getErr  error
	scanErr error
}

func (s *bootstrapPlanReadOnlyStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.t.Helper()
	require.Equal(s.t, s.readTS, ts)
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.inner.GetAt(ctx, key, ts)
}

func (s *bootstrapPlanReadOnlyStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.t.Helper()
	require.Equal(s.t, s.readTS, ts)
	if s.scanErr != nil {
		return nil, s.scanErr
	}
	return s.inner.ScanAt(ctx, start, end, limit, ts)
}

func bootstrapPlanSeeds() []Route {
	return []Route{
		{Start: []byte("m"), GroupID: 2},
		{End: []byte("m"), GroupID: 1},
	}
}

func bootstrapPlanStore(t *testing.T, pebble bool) store.MVCCStore {
	t.Helper()
	if !pebble {
		return store.NewMVCCStore()
	}
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func TestCatalogBootstrapPlanReadOnlyExactSnapshot(t *testing.T) {
	for _, name := range []string{"memory", "pebble"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := bootstrapPlanStore(t, name == "pebble")
			require.NoError(t, st.PutAt(ctx, []byte("user"), []byte("retained"), 1, 0))
			// A later version is deliberately invisible to the exact fenced read.
			require.NoError(t, st.PutAt(ctx, CatalogVersionKey(), EncodeCatalogVersion(7), 50, 0))
			reader := &bootstrapPlanReadOnlyStore{t: t, inner: st, readTS: 10}
			seeds := bootstrapPlanSeeds()
			plan, err := NewCatalogStore(reader).BuildBootstrapPlanAt(ctx, 10, seeds)
			require.NoError(t, err)
			require.Equal(t, uint64(10), plan.Snapshot.ReadTS)
			require.Equal(t, uint64(1), plan.Snapshot.Version)
			require.Equal(t, uint64(1), plan.Snapshot.Routes[0].RouteID)
			require.Equal(t, uint64(1), plan.Snapshot.Routes[0].GroupID)
			require.Equal(t, uint64(2), plan.Snapshot.Routes[1].RouteID)
			require.Equal(t, uint64(50), st.LastCommitTS())
			require.Len(t, plan.Mutations, 6)
			require.Len(t, plan.ReadKeys, len(plan.Mutations))
			for i, mutation := range plan.Mutations {
				require.Equal(t, store.OpTypePut, mutation.Op)
				require.Equal(t, mutation.Key, plan.ReadKeys[i])
			}
			reversed := []Route{seeds[1], seeds[0]}
			again, err := NewCatalogStore(reader).BuildBootstrapPlanAt(ctx, 10, reversed)
			require.NoError(t, err)
			require.Equal(t, plan, again)
			seeds[0].Start[0] = 'z'
			require.Equal(t, []byte("m"), plan.Snapshot.Routes[1].Start)
			plan.ReadKeys[0][0] = '?'
			require.Equal(t, byte('!'), plan.Mutations[0].Key[0])
			plan.ReadKeys[0][0] = '!'
			require.ErrorIs(t, st.ApplyMutations(ctx, plan.Mutations, plan.ReadKeys, 10, 51), store.ErrWriteConflict)
			_, err = st.GetAt(ctx, CatalogRouteKey(1), 51)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			value, err := st.GetAt(ctx, []byte("user"), 51)
			require.NoError(t, err)
			require.Equal(t, []byte("retained"), value)
		})
	}
}

func TestCatalogBootstrapPlanOneAtomicWinner(t *testing.T) {
	for _, name := range []string{"memory", "pebble"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := bootstrapPlanStore(t, name == "pebble")
			catalog := NewCatalogStore(&bootstrapPlanReadOnlyStore{t: t, inner: st, readTS: 10})
			first, err := catalog.BuildBootstrapPlanAt(ctx, 10, bootstrapPlanSeeds())
			require.NoError(t, err)
			second, err := catalog.BuildBootstrapPlanAt(ctx, 10, []Route{{GroupID: 9}})
			require.NoError(t, err)
			require.NoError(t, st.ApplyMutations(ctx, first.Mutations, first.ReadKeys, 10, 20))
			require.ErrorIs(t, st.ApplyMutations(ctx, second.Mutations, second.ReadKeys, 10, 21), store.ErrWriteConflict)
			durable := NewCatalogStore(st)
			snapshot, err := durable.SnapshotAt(ctx, 21)
			require.NoError(t, err)
			require.Equal(t, uint64(1), snapshot.Version)
			require.Len(t, snapshot.Routes, len(first.Snapshot.Routes))
			for i, route := range snapshot.Routes {
				assertRouteEqual(t, first.Snapshot.Routes[i], route)
			}
			nextID, err := durable.NextRouteIDAt(ctx, 21)
			require.NoError(t, err)
			require.Equal(t, uint64(3), nextID)
			changes, err := durable.catalogDeltaAt(ctx, 21, 1)
			require.NoError(t, err)
			require.Equal(t, uint64(0), changes.PreviousVersion)
			require.Len(t, changes.Mutations, 2)
			_, err = st.GetAt(ctx, CatalogDeltaKey(2), 21)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, err = durable.BuildBootstrapPlanAt(ctx, 21, bootstrapPlanSeeds())
			require.ErrorIs(t, err, ErrCatalogVersionMismatch)
		})
	}
}

func TestCatalogBootstrapPlanRejectsPartialCatalog(t *testing.T) {
	cases := []struct {
		name  string
		key   []byte
		value []byte
		want  error
	}{
		{"populated", CatalogVersionKey(), EncodeCatalogVersion(1), ErrCatalogVersionMismatch},
		{"bad_version", CatalogVersionKey(), []byte("bad"), ErrCatalogInvalidVersionRecord},
		{"orphan_route", CatalogRouteKey(99), []byte("bad"), ErrCatalogBootstrapState},
		{"orphan_next_id", CatalogNextRouteIDKey(), EncodeCatalogNextRouteID(1), ErrCatalogBootstrapState},
		{"orphan_delta", CatalogDeltaKey(99), []byte("bad"), ErrCatalogBootstrapState},
		{"orphan_floor", CatalogDeltaFloorKey(), EncodeCatalogVersion(0), ErrCatalogBootstrapState},
		{"orphan_split", []byte(catalogNextSplitJobIDKey), EncodeCatalogVersion(1), ErrCatalogBootstrapState},
		{"orphan_proof", []byte(ownershipPartitionCatalogPrefix + "proof"), []byte("bad"), ErrCatalogBootstrapState},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := store.NewMVCCStore()
			require.NoError(t, st.PutAt(ctx, tc.key, tc.value, 5, 0))
			_, err := NewCatalogStore(st).BuildBootstrapPlanAt(ctx, 10, bootstrapPlanSeeds())
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, uint64(5), st.LastCommitTS())
			value, err := st.GetAt(ctx, tc.key, 10)
			require.NoError(t, err)
			require.Equal(t, tc.value, value)
		})
	}
}

func TestCatalogBootstrapPlanRejectsInvalidSeeds(t *testing.T) {
	cases := []struct {
		name  string
		seeds []Route
		want  error
	}{
		{"empty", nil, ErrCatalogBootstrapSeed},
		{"assigned", []Route{{RouteID: 1, GroupID: 1}}, ErrCatalogBootstrapSeed},
		{"migration", []Route{{GroupID: 1, StagedVisibilityActive: true, MigrationJobID: 3}}, ErrCatalogBootstrapSeed},
		{"floor", []Route{{GroupID: 1, MinWriteTSExclusive: 100}}, ErrCatalogBootstrapSeed},
		{"zero_group", []Route{{}}, ErrCatalogGroupIDRequired},
		{"invalid_range", []Route{{Start: []byte("z"), End: []byte("a"), GroupID: 1}}, ErrCatalogInvalidRouteRange},
		{"overlap", []Route{{End: []byte("z"), GroupID: 1}, {Start: []byte("m"), GroupID: 2}}, ErrEngineSnapshotRouteOverlap},
		{"duplicate", []Route{{End: []byte("m"), GroupID: 1}, {GroupID: 2}}, ErrEngineSnapshotRouteOrder},
		{"invalid_state", []Route{{GroupID: 1, State: RouteState(255)}}, ErrCatalogInvalidRouteState},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			_, err := NewCatalogStore(st).BuildBootstrapPlanAt(context.Background(), 10, tc.seeds)
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, st.LastCommitTS())
		})
	}
}

func TestCatalogBootstrapPlanReadFailuresAndBounds(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	for _, ts := range []uint64{0, math.MaxUint64} {
		_, err := catalog.BuildBootstrapPlanAt(ctx, ts, bootstrapPlanSeeds())
		require.ErrorIs(t, err, ErrCatalogBootstrapReadTS)
	}
	_, err := (*CatalogStore)(nil).BuildBootstrapPlanAt(ctx, 10, bootstrapPlanSeeds())
	require.ErrorIs(t, err, ErrCatalogStoreRequired)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = catalog.BuildBootstrapPlanAt(canceled, 10, bootstrapPlanSeeds())
	require.ErrorIs(t, err, context.Canceled)
	failure := errors.New("disk read unavailable")
	for _, reader := range []*bootstrapPlanReadOnlyStore{
		{t: t, inner: st, readTS: 10, getErr: failure},
		{t: t, inner: st, readTS: 10, scanErr: failure},
	} {
		_, err = NewCatalogStore(reader).BuildBootstrapPlanAt(ctx, 10, bootstrapPlanSeeds())
		require.ErrorIs(t, err, failure)
	}
	require.NoError(t, st.PutAt(ctx, CatalogVersionKey(), EncodeCatalogVersion(0), 5, 0))
	_, err = catalog.BuildBootstrapPlanAt(ctx, 10, bootstrapPlanSeeds())
	require.NoError(t, err)
}

func TestCatalogBootstrapPlanMaximumPositiveSnapshot(t *testing.T) {
	for _, name := range []string{"memory", "pebble"} {
		t.Run(name, func(t *testing.T) {
			st := bootstrapPlanStore(t, name == "pebble")
			const readTS = math.MaxUint64 - 1
			reader := &bootstrapPlanReadOnlyStore{t: t, inner: st, readTS: readTS}
			plan, err := NewCatalogStore(reader).BuildBootstrapPlanAt(t.Context(), readTS, bootstrapPlanSeeds())
			require.NoError(t, err)
			require.NoError(t, st.ApplyMutations(t.Context(), plan.Mutations, plan.ReadKeys, readTS, readTS+1))
			snapshot, err := NewCatalogStore(st).SnapshotAt(t.Context(), math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, uint64(1), snapshot.Version)
			require.Len(t, snapshot.Routes, 2)
		})
	}
}

func TestCatalogBootstrapPlanEveryDependencyConflictsAtomically(t *testing.T) {
	for _, name := range []string{"memory", "pebble"} {
		for i := range 6 {
			t.Run(fmt.Sprintf("%s/%d", name, i), func(t *testing.T) {
				st := bootstrapPlanStore(t, name == "pebble")
				plan, err := NewCatalogStore(st).BuildBootstrapPlanAt(t.Context(), 10, bootstrapPlanSeeds())
				require.NoError(t, err)
				require.Len(t, plan.ReadKeys, 6)
				key := plan.ReadKeys[i]
				require.NoError(t, st.PutAt(t.Context(), key, []byte("concurrent"), 11, 0))
				require.ErrorIs(t, st.ApplyMutations(t.Context(), plan.Mutations, plan.ReadKeys, 10, 20), store.ErrWriteConflict)
				for _, mutation := range plan.Mutations {
					value, err := st.GetAt(t.Context(), mutation.Key, 20)
					if bytes.Equal(mutation.Key, key) {
						require.NoError(t, err)
						require.Equal(t, []byte("concurrent"), value)
					} else {
						require.ErrorIs(t, err, store.ErrKeyNotFound)
					}
				}
			})
		}
	}
}

func TestCatalogBootstrapPlanConcurrentWinner(t *testing.T) {
	for _, name := range []string{"memory", "pebble"} {
		t.Run(name, func(t *testing.T) {
			st := bootstrapPlanStore(t, name == "pebble")
			catalog := NewCatalogStore(st)
			first, err := catalog.BuildBootstrapPlanAt(t.Context(), 10, bootstrapPlanSeeds())
			require.NoError(t, err)
			second, err := catalog.BuildBootstrapPlanAt(t.Context(), 10, []Route{{GroupID: 9}})
			require.NoError(t, err)
			plans := []CatalogBootstrapPlan{first, second}
			start, results := make(chan struct{}), make(chan error, 2)
			for i, plan := range plans {
				go func() {
					<-start
					results <- st.ApplyMutations(t.Context(), plan.Mutations, plan.ReadKeys, 10, uint64(20+i))
				}()
			}
			close(start)
			err1, err2 := <-results, <-results
			if err1 == nil {
				require.ErrorIs(t, err2, store.ErrWriteConflict)
			} else {
				require.ErrorIs(t, err1, store.ErrWriteConflict)
				require.NoError(t, err2)
			}
			snapshot, err := catalog.SnapshotAt(t.Context(), 21)
			require.NoError(t, err)
			require.Equal(t, uint64(1), snapshot.Version)
			changes, err := catalog.catalogDeltaAt(t.Context(), 21, 1)
			require.NoError(t, err)
			require.Len(t, changes.Mutations, len(snapshot.Routes))
			for i, mutation := range changes.Mutations {
				assertRouteEqual(t, mutation.Route, snapshot.Routes[i])
			}
		})
	}
}
