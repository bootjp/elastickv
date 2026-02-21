package distribution

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

const defaultCatalogWatcherInterval = 100 * time.Millisecond

// CatalogWatcherOption customizes CatalogWatcher behavior.
type CatalogWatcherOption func(*CatalogWatcher)

// WithCatalogWatcherInterval sets the catalog polling interval.
func WithCatalogWatcherInterval(interval time.Duration) CatalogWatcherOption {
	return func(w *CatalogWatcher) {
		if interval > 0 {
			w.interval = interval
		}
	}
}

// CatalogWatcher periodically refreshes Engine from durable catalog snapshots.
type CatalogWatcher struct {
	catalog  *CatalogStore
	engine   *Engine
	interval time.Duration
}

// NewCatalogWatcher creates a watcher that polls the durable route catalog and
// applies newer snapshots to the in-memory engine.
func NewCatalogWatcher(catalog *CatalogStore, engine *Engine, opts ...CatalogWatcherOption) *CatalogWatcher {
	w := &CatalogWatcher{
		catalog:  catalog,
		engine:   engine,
		interval: defaultCatalogWatcherInterval,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	return w
}

// Run starts polling and only returns when ctx is canceled or initialization
// requirements are not met. Snapshot read/apply failures are retried.
func (w *CatalogWatcher) Run(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		_ = w.SyncOnce(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// SyncOnce applies the latest durable snapshot when its version is newer than
// the current engine version.
func (w *CatalogWatcher) SyncOnce(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	snapshot, err := w.catalog.Snapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot.Version == w.engine.Version() {
		return nil
	}
	if err := w.engine.ApplySnapshot(snapshot); err != nil {
		if errors.Is(err, ErrEngineSnapshotVersionStale) {
			return nil
		}
		return err
	}
	return nil
}

func (w *CatalogWatcher) validate() error {
	if err := ensureCatalogStore(w.catalog); err != nil {
		return err
	}
	if w.engine == nil {
		return errors.WithStack(ErrEngineRequired)
	}
	if w.interval <= 0 {
		return errors.WithStack(errors.New("catalog watcher interval must be positive"))
	}
	return nil
}
