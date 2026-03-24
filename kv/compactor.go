package kv

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	defaultFSMCompactorInterval        = 5 * time.Minute
	defaultFSMCompactorRetentionWindow = 30 * time.Minute
	defaultFSMCompactorTimeout         = 5 * time.Second
)

type RaftStatsProvider interface {
	Stats() map[string]string
}

type FSMCompactRuntime struct {
	GroupID uint64
	Raft    RaftStatsProvider
	Store   store.MVCCStore
}

type FSMCompactorOption func(*FSMCompactor)

type FSMCompactor struct {
	runtimes        []FSMCompactRuntime
	tracker         *ActiveTimestampTracker
	interval        time.Duration
	retentionWindow time.Duration
	timeout         time.Duration
	logger          *slog.Logger
}

func WithFSMCompactorActiveTimestampTracker(tracker *ActiveTimestampTracker) FSMCompactorOption {
	return func(c *FSMCompactor) {
		c.tracker = tracker
	}
}

func WithFSMCompactorInterval(interval time.Duration) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if interval > 0 {
			c.interval = interval
		}
	}
}

func WithFSMCompactorRetentionWindow(window time.Duration) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if window > 0 {
			c.retentionWindow = window
		}
	}
}

func WithFSMCompactorTimeout(timeout time.Duration) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if timeout > 0 {
			c.timeout = timeout
		}
	}
}

func WithFSMCompactorLogger(logger *slog.Logger) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if logger != nil {
			c.logger = logger
		}
	}
}

func NewFSMCompactor(runtimes []FSMCompactRuntime, opts ...FSMCompactorOption) *FSMCompactor {
	c := &FSMCompactor{
		runtimes:        append([]FSMCompactRuntime(nil), runtimes...),
		interval:        defaultFSMCompactorInterval,
		retentionWindow: defaultFSMCompactorRetentionWindow,
		timeout:         defaultFSMCompactorTimeout,
		logger:          slog.Default(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

func (c *FSMCompactor) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("fsm compactor context is required")
	}
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.ErrorContext(ctx, "fsm compactor initial sync failed", "error", err)
	}

	timer := time.NewTimer(c.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := c.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.ErrorContext(ctx, "fsm compactor sync failed", "error", err)
			}
			timer.Reset(c.interval)
		}
	}
}

func (c *FSMCompactor) SyncOnce(ctx context.Context) error {
	if ctx == nil {
		return errors.New("fsm compactor context is required")
	}
	if err := c.validate(); err != nil {
		return err
	}
	var combined error
	for _, runtime := range c.runtimes {
		if err := c.compactRuntime(ctx, runtime); err != nil {
			combined = errors.CombineErrors(combined, err)
		}
	}
	return errors.WithStack(combined)
}

func (c *FSMCompactor) validate() error {
	if c.interval <= 0 {
		return errors.New("fsm compactor interval must be positive")
	}
	if c.retentionWindow <= 0 {
		return errors.New("fsm compactor retention window must be positive")
	}
	if c.logger == nil {
		return errors.New("fsm compactor logger is required")
	}
	return nil
}

func (c *FSMCompactor) compactRuntime(ctx context.Context, runtime FSMCompactRuntime) error {
	if runtime.Raft == nil || runtime.Store == nil {
		return nil
	}
	retention, ok := runtime.Store.(store.RetentionController)
	if !ok {
		return nil
	}

	stats := runtime.Raft.Stats()
	if shouldSkipFSMCompaction(stats) {
		return nil
	}

	lastCommitTS := runtime.Store.LastCommitTS()
	safeMinTS, ok := c.targetMinTS(lastCommitTS, retention.MinRetainedTS(), time.Now())
	if !ok {
		return nil
	}

	compactCtx, cancel := c.compactContext(ctx)
	defer cancel()

	if err := runtime.Store.Compact(compactCtx, safeMinTS); err != nil {
		return errors.Wrapf(err, "compact group %d", runtime.GroupID)
	}
	c.logger.InfoContext(compactCtx, "fsm compacted",
		"group_id", runtime.GroupID,
		"min_retained_ts", safeMinTS,
		"last_commit_ts", lastCommitTS,
	)
	return nil
}

func (c *FSMCompactor) targetMinTS(lastCommitTS, minRetainedTS uint64, now time.Time) (uint64, bool) {
	if lastCommitTS == 0 {
		return 0, false
	}
	safeMinTS := c.safeMinTS(now)
	if safeMinTS == 0 {
		return 0, false
	}
	if safeMinTS > lastCommitTS {
		safeMinTS = lastCommitTS
	}
	if safeMinTS <= minRetainedTS {
		return 0, false
	}
	return safeMinTS, true
}

func (c *FSMCompactor) compactContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

func shouldSkipFSMCompaction(stats map[string]string) bool {
	if stats == nil {
		return true
	}
	if stats["state"] == "Candidate" {
		return true
	}
	if parseFSMCompactionStatUint(stats["fsm_pending"]) > 0 {
		return true
	}
	return parseFSMCompactionStatUint(stats["applied_index"]) < parseFSMCompactionStatUint(stats["commit_index"])
}

func parseFSMCompactionStatUint(raw string) uint64 {
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func (c *FSMCompactor) safeMinTS(now time.Time) uint64 {
	cutoff := hlcTimestampFromTime(now.Add(-c.retentionWindow))
	if cutoff == 0 {
		return 0
	}
	oldest := uint64(0)
	if c.tracker != nil {
		oldest = c.tracker.Oldest()
	}
	if oldest != 0 && oldest <= cutoff {
		return oldest - 1
	}
	return cutoff
}

func hlcTimestampFromTime(t time.Time) uint64 {
	ms := t.UnixMilli()
	if ms <= 0 {
		return 0
	}
	return uint64(ms) << hlcLogicalBits
}
