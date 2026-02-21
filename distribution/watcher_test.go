package distribution

import (
	"bytes"
	"context"
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
	engine.RecordAccess([]byte("b"))
	before := engine.Stats()
	require.Len(t, before, 2)
	require.Equal(t, uint64(1), before[0].Load)

	watcher := NewCatalogWatcher(catalog, engine, WithCatalogWatcherInterval(5*time.Millisecond))
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- watcher.Run(watchCtx)
	}()

	time.Sleep(40 * time.Millisecond)
	cancel()
	require.NoError(t, <-errCh)

	after := engine.Stats()
	require.Len(t, after, 2)
	require.Equal(t, uint64(1), after[0].Load)
	require.True(t, bytes.Equal(before[0].Start, after[0].Start))
	require.True(t, bytes.Equal(before[1].Start, after[1].Start))
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
