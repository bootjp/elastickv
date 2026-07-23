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

// WithCatalogWatcherBatchSize sets the maximum number of deltas applied by one
// synchronization pass.
func WithCatalogWatcherBatchSize(batchSize int) CatalogWatcherOption {
	return func(w *CatalogWatcher) {
		if batchSize > 0 {
			w.batchSize = batchSize
		}
	}
}

// CatalogWatcher periodically refreshes Engine from durable catalog snapshots.
type CatalogWatcher struct {
	catalog   *CatalogStore
	engine    *Engine
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
}

// NewCatalogWatcher creates a watcher that polls the durable route catalog and
// applies newer snapshots to the in-memory engine.
func NewCatalogWatcher(catalog *CatalogStore, engine *Engine, opts ...CatalogWatcherOption) *CatalogWatcher {
	w := &CatalogWatcher{
		catalog:   catalog,
		engine:    engine,
		interval:  defaultCatalogWatcherInterval,
		batchSize: DefaultCatalogDeltaBatchSize,
		logger:    slog.Default(),
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

	changes, err := w.catalog.ChangesSince(ctx, w.engine.Version(), w.batchSize)
	if err != nil {
		return err
	}
	if changes.Reset != nil {
		if err := w.engine.ApplySnapshot(*changes.Reset); err != nil {
			if errors.Is(err, ErrEngineSnapshotVersionStale) {
				return nil
			}
			return err
		}
		return nil
	}
	for _, delta := range changes.Deltas {
		if err := w.engine.ApplyDelta(delta); err != nil {
			if errors.Is(err, ErrEngineSnapshotVersionStale) {
				continue
			}
			return err
		}
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
	if w.batchSize <= 0 {
		return errors.WithStack(ErrCatalogDeltaLimitInvalid)
	}
	if w.logger == nil {
		return errors.WithStack(errCatalogWatcherLoggerRequired)
	}
	return nil
}
