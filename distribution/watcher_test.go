package distribution

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCatalogWatcherAppliesNewVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           nil,
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := NewEngine()
	initial, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(initial))

	watcher := NewCatalogWatcher(catalog, engine, WithCatalogWatcherInterval(5*time.Millisecond))
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- watcher.Run(watchCtx)
	}()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-errCh)
	})

	_, err = catalog.Save(ctx, initial.Version, []RouteDescriptor{
		{
			RouteID:       10,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 1,
		},
		{
			RouteID:       11,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         RouteStateActive,
			ParentRouteID: 1,
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		if engine.Version() != 2 {
			return false
		}
		left, ok := engine.GetRoute([]byte("b"))
		if !ok || left.GroupID != 1 || left.RouteID != 10 {
			return false
		}
		right, ok := engine.GetRoute([]byte("x"))
		return ok && right.GroupID == 2 && right.RouteID == 11
	}, time.Second, 10*time.Millisecond)
}

func TestCatalogWatcherNoOpWhenVersionUnchanged(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := NewEngine()
	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(snapshot))
	before := engine.Stats()
	require.Len(t, before, 2)

	watcher := NewCatalogWatcher(catalog, engine, WithCatalogWatcherInterval(5*time.Millisecond))
	require.NoError(t, watcher.SyncOnce(ctx))

	after := engine.Stats()
	require.Len(t, after, 2)
	require.True(t, bytes.Equal(before[0].Start, after[0].Start))
	require.True(t, bytes.Equal(before[1].Start, after[1].Start))
}

func TestCatalogWatcherNotifiesAfterApplyingSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []RouteDescriptor{{
		RouteID: 1,
		Start:   []byte(""),
		End:     nil,
		GroupID: 1,
		State:   RouteStateActive,
	}})
	require.NoError(t, err)

	var observed CatalogSnapshot
	watcher := NewCatalogWatcher(
		catalog,
		NewEngine(),
		WithCatalogWatcherSnapshotObserver(func(snapshot CatalogSnapshot) {
			observed = snapshot
		}),
	)
	require.NoError(t, watcher.SyncOnce(ctx))
	require.Equal(t, uint64(1), observed.Version)
	require.Len(t, observed.Routes, 1)
	require.Equal(t, uint64(1), observed.Routes[0].RouteID)
}

func TestCatalogWatcherNotifiesOnceWhenEngineAlreadyAppliedSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())
	snapshot, err := catalog.Save(ctx, 0, []RouteDescriptor{{
		RouteID: 1,
		Start:   []byte(""),
		End:     nil,
		GroupID: 1,
		State:   RouteStateActive,
	}})
	require.NoError(t, err)

	engine := NewEngine()
	require.NoError(t, engine.ApplySnapshot(snapshot))
	observed := 0
	watcher := NewCatalogWatcher(
		catalog,
		engine,
		WithCatalogWatcherSnapshotObserver(func(got CatalogSnapshot) {
			observed++
			require.Equal(t, snapshot.Version, got.Version)
			require.Equal(t, snapshot.Routes, got.Routes)
		}),
	)

	require.NoError(t, watcher.SyncOnce(ctx))
	require.NoError(t, watcher.SyncOnce(ctx))
	require.Equal(t, 1, observed)
}

func TestCatalogWatcherRetriesOnTransientReadError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	seedCatalog := NewCatalogStore(baseStore)
	_, err := seedCatalog.Save(ctx, 0, []RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           nil,
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	catalog := NewCatalogStore(&transientVersionReadStore{
		MVCCStore: baseStore,
		failures:  1,
	})
	engine := NewEngine()
	watcher := NewCatalogWatcher(catalog, engine, WithCatalogWatcherInterval(5*time.Millisecond))
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- watcher.Run(watchCtx)
	}()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-errCh)
	})

	require.Eventually(t, func() bool {
		return engine.Version() == 1
	}, time.Second, 10*time.Millisecond)
}

func TestCatalogWatcherLoggerOption(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	watcher := NewCatalogWatcher(
		NewCatalogStore(store.NewMVCCStore()),
		NewEngine(),
		WithCatalogWatcherLogger(logger),
	)
	require.Same(t, logger, watcher.logger)
}

type transientVersionReadStore struct {
	store.MVCCStore
	mu       sync.Mutex
	failures int
}

func (s *transientVersionReadStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if bytes.Equal(key, CatalogVersionKey()) {
		s.mu.Lock()
		shouldFail := s.failures > 0
		if shouldFail {
			s.failures--
		}
		s.mu.Unlock()
		if shouldFail {
			return nil, errors.New("transient catalog version read failure")
		}
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

// A node catching up across more deltas than one batch advances the engine to
// the batch's last version while the persisted catalog is already further
// ahead. The observer must still hear about the route table the engine is
// actually serving, otherwise writes routed to newly-applied route IDs go
// unattributed in KeyViz/autosplit for the whole catch-up.
func TestCatalogWatcherNotifiesAfterPartialDeltaBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := NewCatalogStore(store.NewMVCCStore())

	routes := []RouteDescriptor{{
		RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: RouteStateActive,
	}}
	last, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)

	// Three more catalog versions, each splitting off another route.
	for i := uint64(2); i <= 4; i++ {
		routes = append(routes, RouteDescriptor{
			RouteID: i,
			Start:   []byte{byte('a') + byte(i)},
			End:     nil,
			GroupID: i,
			State:   RouteStateActive,
		})
		for j := range routes[:len(routes)-1] {
			routes[j].End = routes[j+1].Start
		}
		last, err = catalog.Save(ctx, last.Version, routes)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(4), last.Version)

	var observed []CatalogSnapshot
	watcher := NewCatalogWatcher(
		catalog,
		NewEngine(),
		// One delta per sync, so the engine trails the catalog while catching up.
		WithCatalogWatcherBatchSize(1),
		WithCatalogWatcherSnapshotObserver(func(snapshot CatalogSnapshot) {
			observed = append(observed, snapshot)
		}),
	)

	require.NoError(t, watcher.SyncOnce(ctx))
	require.NotEmpty(t, observed, "a partial catch-up batch must still notify the observer")

	first := observed[0]
	require.Equal(t, watcher.engine.Version(), first.Version,
		"the observer must see the version the engine is actually serving")
	require.Less(t, first.Version, last.Version, "this case is only meaningful mid-catch-up")
	require.NotEmpty(t, first.Routes)
}
