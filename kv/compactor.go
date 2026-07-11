package kv

import (
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const (
	defaultFSMCompactorInterval        = 5 * time.Minute
	defaultFSMCompactorRetentionWindow = 30 * time.Minute
	defaultFSMCompactorTimeout         = 5 * time.Second
	defaultFSMCompactorLeaderTimeout   = 500 * time.Millisecond
	defaultFSMCompactorMaxL0Files      = 256
	defaultFSMCompactorMaxL0Sublevels  = 12
	defaultFSMCompactorMaxLSMDebtBytes = 512 << 20
)

type RaftStatusProvider interface {
	Status() raftengine.Status
}

type FSMCompactRuntime struct {
	GroupID      uint64
	StatusReader RaftStatusProvider
	Store        store.MVCCStore
}

type FSMCompactorOption func(*FSMCompactor)

type FSMCompactor struct {
	runtimes        []FSMCompactRuntime
	tracker         *ActiveTimestampTracker
	interval        time.Duration
	retentionWindow time.Duration
	timeout         time.Duration
	leaderTimeout   time.Duration
	maxL0Files      int64
	maxL0Sublevels  int32
	maxLSMDebtBytes uint64
	logger          *slog.Logger
}

type pebbleMetricsSource interface {
	Metrics() *pebble.Metrics
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

func WithFSMCompactorLeaderTimeout(timeout time.Duration) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if timeout > 0 {
			c.leaderTimeout = timeout
		}
	}
}

func WithFSMCompactorLSMBackpressureLimits(maxL0Files int64, maxDebtBytes uint64) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if maxL0Files > 0 {
			c.maxL0Files = maxL0Files
		}
		if maxDebtBytes > 0 {
			c.maxLSMDebtBytes = maxDebtBytes
		}
	}
}

func WithFSMCompactorLSMBackpressureSublevelLimit(maxL0Sublevels int32) FSMCompactorOption {
	return func(c *FSMCompactor) {
		if maxL0Sublevels > 0 {
			c.maxL0Sublevels = maxL0Sublevels
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
		leaderTimeout:   defaultFSMCompactorLeaderTimeout,
		maxL0Files:      defaultFSMCompactorMaxL0Files,
		maxL0Sublevels:  defaultFSMCompactorMaxL0Sublevels,
		maxLSMDebtBytes: defaultFSMCompactorMaxLSMDebtBytes,
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
	if runtime.StatusReader == nil || runtime.Store == nil {
		return nil
	}
	retention, ok := runtime.Store.(store.RetentionController)
	if !ok {
		return nil
	}

	status := runtime.StatusReader.Status()
	if shouldSkipFSMCompaction(status) {
		return nil
	}
	if overloaded, snap := c.lsmBackpressure(runtime.Store); overloaded {
		c.logger.WarnContext(ctx, "skipping fsm compaction under pebble backpressure",
			"group_id", runtime.GroupID,
			"l0_files", snap.Levels[0].TablesCount,
			"l0_sublevels", snap.Levels[0].Sublevels,
			"compaction_debt_bytes", snap.Compact.EstimatedDebt,
			"compactions_in_progress", snap.Compact.NumInProgress,
			"compaction_in_progress_bytes", snap.Compact.InProgressBytes,
		)
		return nil
	}

	lastCommitTS := runtime.Store.LastCommitTS()
	safeMinTS, ok := c.targetMinTS(lastCommitTS, retention.MinRetainedTS(), time.Now())
	if !ok {
		return nil
	}

	compactCtx, cancel := c.compactContext(ctx, status)
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

func (c *FSMCompactor) compactContext(ctx context.Context, status raftengine.Status) (context.Context, context.CancelFunc) {
	timeout := c.timeout
	if status.State == raftengine.StateLeader && c.leaderTimeout > 0 && (timeout <= 0 || c.leaderTimeout < timeout) {
		timeout = c.leaderTimeout
	}
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (c *FSMCompactor) lsmBackpressure(st store.MVCCStore) (bool, *pebble.Metrics) {
	source, ok := st.(pebbleMetricsSource)
	if !ok {
		return false, nil
	}
	snap := source.Metrics()
	if snap == nil {
		return false, nil
	}
	return lsmWriteBackpressured(snap, lsmBackpressureLimits{
		maxL0Files:      c.maxL0Files,
		maxL0Sublevels:  c.maxL0Sublevels,
		maxLSMDebtBytes: c.maxLSMDebtBytes,
	}), snap
}

func shouldSkipFSMCompaction(status raftengine.Status) bool {
	if status.State == raftengine.StateLeader && status.NumPeers > 0 {
		return true
	}
	if status.State == raftengine.StateCandidate {
		return true
	}
	if status.LeadTransferee != 0 || status.PendingConfChange {
		return true
	}
	if status.FSMPending > 0 {
		return true
	}
	return status.AppliedIndex < status.CommitIndex
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
