package snapshotoffload

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

// Scheduler is the §4 leader-only publisher: each process scans its own Raft
// groups on an interval and offloads a persisted snapshot when one exists that
// has not been published yet.
//
// It never asks the state machine for a snapshot. Snapshot cadence stays owned
// by the Raft engine, so this milestone can only publish what the engine has
// already persisted -- an offload that forced its own snapshot would change
// compaction behaviour, which §10 lists as a non-goal.
type Scheduler struct {
	groups      []OffloadGroup
	store       ObjectStore
	prefix      string
	sourceName  string
	binVersion  string
	spoolDir    string
	interval    time.Duration
	jitter      time.Duration
	concurrency int
	observer    SchedulerObserver
	logger      *slog.Logger
	now         func() time.Time
	// published caches the highest index this process has published per group.
	// It is an optimisation only: restart idempotency comes from the object
	// store, not from this map.
	mu        sync.Mutex
	published map[uint64]uint64
}

// OffloadGroup is one local Raft group the scheduler may publish for.
type OffloadGroup struct {
	GroupID uint64
	DataDir string
	// IsLeader is the cheap pre-check made before opening the snapshot.
	IsLeader func() bool
	// VerifyLeader is the authoritative check, re-run immediately before the
	// manifest is committed. See PublishOptions.VerifyLeader.
	VerifyLeader func(context.Context) error
}

// SchedulerObserver receives per-attempt outcomes for metrics.
type SchedulerObserver interface {
	ObserveSnapshotOffloadPublished(groupID, index uint64, payloadBytes int64, elapsed time.Duration)
	ObserveSnapshotOffloadSkipped(groupID uint64, reason string)
	ObserveSnapshotOffloadFailed(groupID uint64, err error)
}

type nopSchedulerObserver struct{}

func (nopSchedulerObserver) ObserveSnapshotOffloadPublished(uint64, uint64, int64, time.Duration) {}
func (nopSchedulerObserver) ObserveSnapshotOffloadSkipped(uint64, string)                         {}
func (nopSchedulerObserver) ObserveSnapshotOffloadFailed(uint64, error)                           {}

// Default scheduling parameters from §4.
const (
	DefaultSchedulerInterval    = 15 * time.Minute
	DefaultSchedulerConcurrency = 1
)

type SchedulerOption func(*Scheduler)

func WithSchedulerInterval(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		if d > 0 {
			s.interval = d
		}
	}
}

// WithSchedulerJitter spreads multi-group work so every group in a process does
// not contend for the upload slot on the same tick.
func WithSchedulerJitter(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		if d >= 0 {
			s.jitter = d
		}
	}
}

func WithSchedulerConcurrency(n int) SchedulerOption {
	return func(s *Scheduler) {
		if n > 0 {
			s.concurrency = n
		}
	}
}

func WithSchedulerObserver(o SchedulerObserver) SchedulerOption {
	return func(s *Scheduler) {
		if o != nil {
			s.observer = o
		}
	}
}

func WithSchedulerLogger(l *slog.Logger) SchedulerOption {
	return func(s *Scheduler) {
		if l != nil {
			s.logger = l
		}
	}
}

func WithSchedulerClock(now func() time.Time) SchedulerOption {
	return func(s *Scheduler) {
		if now != nil {
			s.now = now
		}
	}
}

func WithSchedulerSpoolDir(dir string) SchedulerOption {
	return func(s *Scheduler) { s.spoolDir = dir }
}

// NewScheduler builds the offload scheduler. It is opt-in: callers construct it
// only when object offload is configured.
func NewScheduler(store ObjectStore, groups []OffloadGroup, prefix, sourceCluster, binaryVersion string, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		groups:      groups,
		store:       store,
		prefix:      prefix,
		sourceName:  sourceCluster,
		binVersion:  binaryVersion,
		interval:    DefaultSchedulerInterval,
		jitter:      DefaultSchedulerInterval / 4, //nolint:mnd // a quarter interval spreads groups without doubling the period.
		concurrency: DefaultSchedulerConcurrency,
		observer:    nopSchedulerObserver{},
		logger:      slog.Default().With(slog.String("component", "snapshot-offload")),
		now:         time.Now,
		published:   make(map[uint64]uint64),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Scheduler) validate() error {
	switch {
	case s.store == nil:
		return errors.Wrap(ErrInvalidOptions, "snapshot offload scheduler requires an object store")
	case s.sourceName == "":
		return errors.Wrap(ErrInvalidOptions, "snapshot offload scheduler requires a source cluster name")
	}
	return nil
}

// Run scans on the configured interval until ctx is cancelled. Cancellation is
// the only stop condition; a failing group is retried on the next tick rather
// than tearing the loop down, because an object store outage must not stop the
// process.
func (s *Scheduler) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.Wrap(ErrInvalidOptions, "snapshot offload scheduler context is required")
	}
	if err := s.validate(); err != nil {
		return err
	}
	timer := time.NewTimer(s.nextDelay())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			s.SyncOnce(ctx)
			timer.Reset(s.nextDelay())
		}
	}
}

func (s *Scheduler) nextDelay() time.Duration {
	if s.jitter <= 0 {
		return s.interval
	}
	return s.interval + time.Duration(rand.Int64N(int64(s.jitter))) //nolint:gosec // scheduling jitter, not a security decision.
}

// SyncOnce runs one scan across every local group, bounded by the upload
// concurrency limit. Exported so tests and operators can force a pass.
func (s *Scheduler) SyncOnce(ctx context.Context) {
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for _, group := range s.groups {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(g OffloadGroup) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			s.publishGroup(ctx, g)
		}(group)
	}
	wg.Wait()
}

func (s *Scheduler) publishGroup(ctx context.Context, group OffloadGroup) {
	// Cheap pre-check first: a follower must not even open the snapshot.
	if group.IsLeader != nil && !group.IsLeader() {
		s.observer.ObserveSnapshotOffloadSkipped(group.GroupID, "not_leader")
		return
	}
	started := s.now()
	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         s.store,
		DataDir:       group.DataDir,
		Prefix:        s.prefix,
		GroupID:       group.GroupID,
		SourceCluster: s.sourceName,
		BinaryVersion: s.binVersion,
		SpoolDir:      s.spoolDir,
		VerifyLeader:  group.VerifyLeader,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		s.observer.ObserveSnapshotOffloadFailed(group.GroupID, err)
		s.logger.WarnContext(ctx, "snapshot offload publish failed",
			slog.Uint64("group_id", group.GroupID), slog.String("error", err.Error()))
		return
	}
	s.markPublished(group.GroupID, manifest.SnapshotIndex)
	s.observer.ObserveSnapshotOffloadPublished(
		group.GroupID, manifest.SnapshotIndex, manifest.Payload.Bytes, s.now().Sub(started))
}

func (s *Scheduler) markPublished(groupID, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index > s.published[groupID] {
		s.published[groupID] = index
	}
}

// LastPublishedIndex reports the highest index this process has published for a
// group. Zero means "nothing published by this process", not "nothing
// published": another node or a previous run may hold newer manifests.
func (s *Scheduler) LastPublishedIndex(groupID uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.published[groupID]
}
