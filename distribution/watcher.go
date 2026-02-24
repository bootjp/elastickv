package distribution

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
)

const defaultCatalogWatcherInterval = 100 * time.Millisecond

var (
	errCatalogWatcherContextRequired = errors.New("catalog watcher context is required")
	errCatalogWatcherInvalidInterval = errors.New("catalog watcher interval must be positive")
	errCatalogWatcherLoggerRequired  = errors.New("catalog watcher logger is required")
)

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

// WithCatalogWatcherLogger sets the logger for watcher background retries.
func WithCatalogWatcherLogger(logger *slog.Logger) CatalogWatcherOption {
	return func(w *CatalogWatcher) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// CatalogWatcher periodically refreshes Engine from durable catalog snapshots.
type CatalogWatcher struct {
	catalog  *CatalogStore
	engine   *Engine
	interval time.Duration
	logger   *slog.Logger
}

// NewCatalogWatcher creates a watcher that polls the durable route catalog and
// applies newer snapshots to the in-memory engine.
func NewCatalogWatcher(catalog *CatalogStore, engine *Engine, opts ...CatalogWatcherOption) *CatalogWatcher {
	w := &CatalogWatcher{
		catalog:  catalog,
		engine:   engine,
		interval: defaultCatalogWatcherInterval,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	return w
}

// RunCatalogWatcher runs CatalogWatcher with optional logger override.
func RunCatalogWatcher(ctx context.Context, catalog *CatalogStore, engine *Engine, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	catalogWatcher := NewCatalogWatcher(catalog, engine, WithCatalogWatcherLogger(logger))
	return catalogWatcher.Run(ctx)
}

// Run starts polling and only returns when ctx is canceled or initialization
// requirements are not met. Snapshot read/apply failures are retried.
func (w *CatalogWatcher) Run(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.WithStack(errCatalogWatcherContextRequired)
	}
	if err := w.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		w.logger.ErrorContext(ctx, "catalog watcher initial sync failed", "error", err)
	}

	timer := time.NewTimer(w.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := w.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.logger.ErrorContext(ctx, "catalog watcher sync failed", "error", err)
			}
			timer.Reset(w.interval)
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
		return errors.WithStack(errCatalogWatcherContextRequired)
	}

	readTS := w.catalog.store.LastCommitTS()
	catalogVersion, err := w.catalog.versionAt(ctx, readTS)
	if err != nil {
		return err
	}
	if catalogVersion <= w.engine.Version() {
		return nil
	}

	routes, err := w.catalog.routesAt(ctx, readTS)
	if err != nil {
		return err
	}
	snapshot := CatalogSnapshot{
		Version: catalogVersion,
		Routes:  routes,
		ReadTS:  readTS,
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
		return errors.WithStack(errCatalogWatcherInvalidInterval)
	}
	if w.logger == nil {
		return errors.WithStack(errCatalogWatcherLoggerRequired)
	}
	return nil
}
