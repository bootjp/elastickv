package adapter

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

const (
	s3LegacyMigrationEnabledEnv       = "ELASTICKV_S3_LEGACY_MIGRATION_ENABLED"
	s3LegacyMigrationBurnInStartEnv   = "ELASTICKV_S3_LEGACY_MIGRATION_BURN_IN_START"
	s3LegacyMigrationScanIntervalEnv  = "ELASTICKV_S3_LEGACY_MIGRATION_SCAN_INTERVAL"
	s3LegacyMigrationPageSizeEnv      = "ELASTICKV_S3_LEGACY_MIGRATION_PAGE_SIZE"
	s3LegacyMigrationMaxCandidatesEnv = "ELASTICKV_S3_LEGACY_MIGRATION_MAX_CANDIDATES_PER_PASS"
	s3LegacyMigrationObjectTimeoutEnv = "ELASTICKV_S3_LEGACY_MIGRATION_OBJECT_TIMEOUT"

	s3LegacyMigrationBurnIn          = 30 * 24 * time.Hour
	s3LegacyMigrationDefaultInterval = 5 * time.Minute
	s3LegacyMigrationDefaultPageSize = 64
	s3LegacyMigrationDefaultMax      = 64
	s3LegacyMigrationDefaultTimeout  = 2 * time.Minute
)

// S3LegacyBlobMigrationConfig is an explicit rollout gate. BurnInStart is an
// operator-supplied date, not evidence that the production burn-in succeeded.
type S3LegacyBlobMigrationConfig struct {
	Enabled              bool
	BurnInStart          time.Time
	ScanInterval         time.Duration
	PageSize             int
	MaxCandidatesPerPass int
	ObjectTimeout        time.Duration
}

func defaultS3LegacyBlobMigrationConfig() S3LegacyBlobMigrationConfig {
	return S3LegacyBlobMigrationConfig{
		ScanInterval:         s3LegacyMigrationDefaultInterval,
		PageSize:             s3LegacyMigrationDefaultPageSize,
		MaxCandidatesPerPass: s3LegacyMigrationDefaultMax,
		ObjectTimeout:        s3LegacyMigrationDefaultTimeout,
	}
}

// S3LegacyBlobMigrationConfigFromEnv rejects malformed enabled settings.
// Disabled is the default, and no date is inferred from process startup.
func S3LegacyBlobMigrationConfigFromEnv() (S3LegacyBlobMigrationConfig, error) {
	cfg := defaultS3LegacyBlobMigrationConfig()
	raw := strings.TrimSpace(os.Getenv(s3LegacyMigrationEnabledEnv))
	if raw == "" {
		return cfg, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return cfg, errors.Wrapf(err, "parse %s", s3LegacyMigrationEnabledEnv)
	}
	cfg.Enabled = enabled
	if !enabled {
		return cfg, nil
	}
	start := strings.TrimSpace(os.Getenv(s3LegacyMigrationBurnInStartEnv))
	cfg.BurnInStart, err = time.Parse(time.RFC3339, start)
	if err != nil {
		return cfg, errors.Wrapf(err, "parse %s", s3LegacyMigrationBurnInStartEnv)
	}
	if cfg.ScanInterval, err = positiveS3MigrationDurationEnv(s3LegacyMigrationScanIntervalEnv, cfg.ScanInterval); err != nil {
		return cfg, err
	}
	if cfg.ObjectTimeout, err = positiveS3MigrationDurationEnv(s3LegacyMigrationObjectTimeoutEnv, cfg.ObjectTimeout); err != nil {
		return cfg, err
	}
	if cfg.PageSize, err = positiveEnvInt(s3LegacyMigrationPageSizeEnv, cfg.PageSize); err != nil {
		return cfg, err
	}
	if cfg.MaxCandidatesPerPass, err = positiveEnvInt(s3LegacyMigrationMaxCandidatesEnv, cfg.MaxCandidatesPerPass); err != nil {
		return cfg, err
	}
	return cfg, cfg.validate(time.Now())
}

func positiveS3MigrationDurationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return 0, errors.WithStack(errors.Newf("%s must be a positive duration", name))
	}
	return duration, nil
}

func (cfg S3LegacyBlobMigrationConfig) validate(now time.Time) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.BurnInStart.IsZero() || cfg.BurnInStart.After(now.Add(-s3LegacyMigrationBurnIn)) {
		return errors.New("s3 legacy migration requires an explicitly dated 30-day burn-in start")
	}
	if cfg.ScanInterval <= 0 || cfg.PageSize <= 0 || cfg.PageSize > s3MetaBatchOps ||
		cfg.MaxCandidatesPerPass <= 0 || cfg.ObjectTimeout <= 0 {
		return errors.New("s3 legacy migration requires positive bounded scan and object limits")
	}
	return nil
}

// WithS3LegacyBlobMigrationConfig installs a worker but does not start it.
// Production startup calls StartLegacyBlobMigration after StartBlobGC.
func WithS3LegacyBlobMigrationConfig(cfg S3LegacyBlobMigrationConfig) S3ServerOption {
	return func(s *S3Server) {
		if s != nil && cfg.Enabled {
			s.legacyMigration = &s3LegacyBlobMigrationWorker{config: cfg}
		}
	}
}

type s3LegacyBlobMigrationWorker struct {
	config S3LegacyBlobMigrationConfig

	mu          sync.Mutex
	cancel      context.CancelFunc
	started     bool
	stopped     bool
	done        chan struct{}
	joinTimeout time.Duration
	cursor      []byte // owned by the worker goroutine
}

func (s *S3Server) StartLegacyBlobMigration(ctx context.Context) error {
	if s == nil || s.legacyMigration == nil {
		return nil
	}
	return s.legacyMigration.start(ctx, s)
}

func (w *s3LegacyBlobMigrationWorker) start(ctx context.Context, s *S3Server) error {
	if err := w.validateStart(ctx, s); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return errors.New("s3 legacy migration worker has stopped")
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if w.started {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	w.cancel = cancel
	w.done = done
	w.started = true
	go func() {
		defer close(done)
		w.run(runCtx, s)
	}()
	return nil
}

func (w *s3LegacyBlobMigrationWorker) validateStart(ctx context.Context, s *S3Server) error {
	if ctx == nil {
		return errors.New("s3 legacy migration requires a context")
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if err := w.config.validate(time.Now()); err != nil {
		return err
	}
	if s.store == nil || s.coordinator == nil || !s.blobOffloadEnabled || s.blobCluster == nil || s.blobLocalStores == nil {
		return errors.New("s3 legacy migration requires the configured M3 blob data path")
	}
	s.blobGC.mu.Lock()
	gcStarted := s.blobGC.started
	s.blobGC.mu.Unlock()
	if !gcStarted {
		return errors.New("s3 legacy migration requires blob GC recovery to be started first")
	}
	return nil
}

func (w *s3LegacyBlobMigrationWorker) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	joinTimeout := w.joinTimeout
	w.cancel = nil
	w.started = false
	w.stopped = true
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return
	}
	if joinTimeout <= 0 {
		joinTimeout = s3CleanupJoinTimeout
	}
	timer := time.NewTimer(joinTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		slog.Warn("s3 legacy migration worker did not stop after cancellation", "timeout", joinTimeout)
	}
}

func (w *s3LegacyBlobMigrationWorker) run(ctx context.Context, s *S3Server) {
	ticker := time.NewTicker(w.config.ScanInterval)
	defer ticker.Stop()
	for {
		w.runPass(ctx, s)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *s3LegacyBlobMigrationWorker) runPass(ctx context.Context, s *S3Server) {
	if !s3LegacyMigrationAdmissionReady(ctx, s) {
		return
	}
	if err := s.recoverPendingS3Migrations(ctx); err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(ctx, "s3 legacy migration recovery before scan failed", "err", err)
		}
		return
	}
	if !s3LegacyMigrationAdmissionReady(ctx, s) {
		return
	}
	if err := w.scanCandidates(ctx, s); err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, "s3 legacy migration candidate scan failed", "err", err)
	}
}

func s3LegacyMigrationAdmissionReady(ctx context.Context, s *S3Server) bool {
	return ctx.Err() == nil && s.blobOffloadGCReady.Load() && s.isVerifiedS3Leader(ctx) &&
		s.s3BlobOffloadDecision(ctx).mode == s3BlobOffloadModeOffload
}

// scanCandidates advances one bounded slice of the manifest keyspace. The
// cursor wraps only after a complete traversal so already-converted objects
// cannot starve later candidates across repeated passes.
func (w *s3LegacyBlobMigrationWorker) scanCandidates(ctx context.Context, s *S3Server) error {
	prefix := []byte(s3keys.ObjectManifestPrefix)
	end := prefixScanEnd(prefix)
	if w.cursor == nil {
		w.cursor = prefix
	}
	for examined := 0; examined < w.config.MaxCandidatesPerPass; {
		if ctx.Err() != nil || !s.isVerifiedS3Leader(ctx) {
			if err := ctx.Err(); err != nil {
				return errors.WithStack(err)
			}
			return errors.New("s3 legacy migration lost leader during scan")
		}
		limit := min(w.config.PageSize, w.config.MaxCandidatesPerPass-examined)
		keys, err := w.scanCandidatePage(ctx, s, end, limit)
		if err != nil {
			return errors.Wrap(err, "scan s3 migration candidates")
		}
		if len(keys) == 0 {
			w.cursor = nil
			return nil
		}
		if err := w.processCandidatePage(ctx, s, keys); err != nil {
			return err
		}
		examined += len(keys)
		if len(keys) < limit {
			w.cursor = nil
			return nil
		}
	}
	return nil
}

func (w *s3LegacyBlobMigrationWorker) processCandidatePage(ctx context.Context, s *S3Server, keys [][]byte) error {
	for _, key := range keys {
		w.cursor = nextScanCursor(key)
		if err := w.migrateCandidate(ctx, s, key); err != nil {
			if ctx.Err() != nil {
				return errors.WithStack(ctx.Err())
			}
			slog.WarnContext(ctx, "s3 legacy migration candidate failed", "err", err)
		}
	}
	return nil
}

func (w *s3LegacyBlobMigrationWorker) scanCandidatePage(ctx context.Context, s *S3Server, end []byte, limit int) ([][]byte, error) {
	readTS := s.readTS()
	pin := s.pinReadTS(readTS)
	defer pin.Release()
	keys, err := s.store.ScanKeysAt(ctx, w.cursor, end, limit, readTS)
	return keys, errors.WithStack(err)
}

func (w *s3LegacyBlobMigrationWorker) migrateCandidate(ctx context.Context, s *S3Server, key []byte) error {
	if !s.blobOffloadGCReady.Load() || !s.isVerifiedS3Leader(ctx) ||
		s.s3BlobOffloadDecision(ctx).mode != s3BlobOffloadModeOffload {
		return errors.New("s3 legacy migration admission closed")
	}
	bucket, generation, object, ok := s3keys.ParseObjectManifestKey(key)
	if !ok {
		return errors.New("s3 legacy migration found malformed manifest key")
	}
	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, s.readTS())
	if err != nil {
		return err
	}
	if !exists || meta == nil || meta.Generation != generation {
		return nil
	}
	objectCtx, cancel := context.WithTimeout(ctx, w.config.ObjectTimeout)
	defer cancel()
	_, err = s.migrateS3LegacyObject(objectCtx, bucket, object)
	return err
}
