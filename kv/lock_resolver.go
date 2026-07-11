package kv

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const (
	// lockResolverInterval is how often the background resolver runs.
	lockResolverInterval = 10 * time.Second
	// lockResolverBatchSize limits the number of locks scanned per group per cycle.
	lockResolverBatchSize = 100
	// lockResolverCycleBudget caps best-effort cleanup work so it cannot occupy
	// the Raft leader long enough to starve HLC lease renewal.
	lockResolverCycleBudget = 500 * time.Millisecond
	// lockResolverOperationBudget bounds a single background resolution attempt.
	// Foreground read/write paths still use their own caller deadlines.
	lockResolverOperationBudget = 250 * time.Millisecond
	lockResolverMaxL0Files      = defaultFSMCompactorMaxL0Files
	lockResolverMaxL0Sublevels  = defaultFSMCompactorMaxL0Sublevels
	lockResolverMaxLSMDebtBytes = defaultFSMCompactorMaxLSMDebtBytes
)

var errLockResolverBudgetExhausted = errors.New("lock resolver budget exhausted")

// LockResolver periodically scans for expired transaction locks and resolves
// them. This handles the case where secondary commit fails and leaves orphaned
// locks that no read path would discover (e.g., cold keys).
type LockResolver struct {
	store  *ShardStore
	groups map[uint64]*ShardGroup
	log    *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

// NewLockResolver creates and starts a background lock resolver.
func NewLockResolver(ss *ShardStore, groups map[uint64]*ShardGroup, log *slog.Logger) *LockResolver {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	lr := &LockResolver{
		store:  ss,
		groups: groups,
		log:    log,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go lr.run(ctx)
	return lr
}

// Close stops the background resolver and waits for it to finish.
func (lr *LockResolver) Close() {
	lr.cancel()
	<-lr.done
}

func (lr *LockResolver) run(ctx context.Context) {
	defer close(lr.done)

	ticker := time.NewTicker(lockResolverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lr.resolveAllGroups(ctx)
		}
	}
}

func (lr *LockResolver) resolveAllGroups(ctx context.Context) {
	passCtx, cancel := context.WithTimeout(ctx, lockResolverCycleBudget)
	defer cancel()

	for gid, g := range lr.groups {
		if ctx.Err() != nil {
			return
		}
		if passCtx.Err() != nil {
			lr.logLockResolverYield(gid, passCtx.Err())
			return
		}
		err := lr.resolveGroupLocks(passCtx, gid, g)
		if err != nil {
			if errors.Is(err, errLockResolverBudgetExhausted) || lockResolverBudgetExhausted(err, passCtx, ctx) {
				lr.logLockResolverYield(gid, err)
				return
			}
			lr.log.Warn("lock resolver: group scan failed",
				slog.Uint64("gid", gid),
				slog.Any("err", err),
			)
		}
	}
}

func (lr *LockResolver) resolveGroupLocks(ctx context.Context, gid uint64, g *ShardGroup) error {
	if !lr.groupReadyForBackgroundResolve(ctx, gid, g) {
		return nil
	}

	// Scan lock key range: [!txn|lock| ... prefixEnd(!txn|lock|))
	lockStart := txnLockKey(nil)
	lockEnd := prefixScanEnd(lockStart)

	lockKVs, err := g.Store.ScanAt(ctx, lockStart, lockEnd, lockResolverBatchSize, math.MaxUint64)
	if err != nil {
		return errors.WithStack(err)
	}

	var resolved, skipped int
	for _, kvp := range lockKVs {
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		if !lockResolverRaftReady(engineForGroup(g)) {
			return nil
		}
		outcome, err := lr.resolveScannedLock(ctx, gid, g, kvp)
		if err != nil {
			return err
		}
		resolvedDelta, skippedDelta := lockResolveOutcomeCounts(outcome)
		resolved += resolvedDelta
		skipped += skippedDelta
	}

	if resolved > 0 {
		lr.log.Info("lock resolver: resolved expired locks",
			slog.Uint64("gid", gid),
			slog.Int("resolved", resolved),
			slog.Int("skipped_active", skipped),
		)
	}
	return nil
}

func lockResolverGroupCanResolve(g *ShardGroup) bool {
	ready, _, _ := lockResolverGroupResolveState(g)
	return ready
}

func lockResolverGroupResolveState(g *ShardGroup) (ready bool, overloaded bool, snap *pebble.Metrics) {
	if g == nil || g.Store == nil {
		return false, false, nil
	}
	if !lockResolverRaftReady(engineForGroup(g)) {
		return false, false, nil
	}
	overloaded, snap = lockResolverLSMBackpressured(g.Store)
	return !overloaded, overloaded, snap
}

func lockResolveOutcomeCounts(outcome lockResolveOutcome) (resolved int, skippedActive int) {
	switch outcome {
	case lockResolveResolved:
		return 1, 0
	case lockResolveSkippedActive:
		return 0, 1
	case lockResolveIgnored:
		return 0, 0
	}
	return 0, 0
}

type lockResolveOutcome int

const (
	lockResolveIgnored lockResolveOutcome = iota
	lockResolveSkippedActive
	lockResolveResolved
)

func (lr *LockResolver) resolveScannedLock(ctx context.Context, gid uint64, g *ShardGroup, kvp *store.KVPair) (lockResolveOutcome, error) {
	userKey, ok := txnUserKeyFromLockKey(kvp.Key)
	if !ok {
		return lockResolveIgnored, nil
	}

	lock, err := decodeTxnLock(kvp.Value)
	if err != nil {
		lr.log.Warn("lock resolver: decode lock failed",
			slog.String("key", string(userKey)),
			slog.Any("err", err),
		)
		return lockResolveIgnored, nil
	}

	// Only resolve expired locks; active transaction locks are not touched.
	if !txnLockExpired(lock) {
		return lockResolveSkippedActive, nil
	}

	opCtx, cancel := context.WithTimeout(ctx, lockResolverOperationBudget)
	defer cancel()
	err = lr.resolveExpiredLock(opCtx, g, userKey, lock)
	if err == nil {
		return lockResolveResolved, nil
	}
	if lockResolverBudgetExhausted(err, opCtx, ctx) {
		marked := errors.Mark(err, errLockResolverBudgetExhausted)
		return lockResolveIgnored, errors.Wrap(marked, "lock resolver budget exhausted")
	}
	lr.log.Warn("lock resolver: resolve failed",
		slog.Uint64("gid", gid),
		slog.String("key", string(userKey)),
		slog.Uint64("start_ts", lock.StartTS),
		slog.Any("err", err),
	)
	return lockResolveIgnored, nil
}

func (lr *LockResolver) groupReadyForBackgroundResolve(ctx context.Context, gid uint64, g *ShardGroup) bool {
	ready, overloaded, snap := lockResolverGroupResolveState(g)
	if overloaded {
		lr.log.WarnContext(ctx, "lock resolver: skipping under pebble backpressure",
			slog.Uint64("gid", gid),
			slog.Any("l0_files", snap.Levels[0].TablesCount),
			slog.Any("l0_sublevels", snap.Levels[0].Sublevels),
			slog.Any("compaction_debt_bytes", snap.Compact.EstimatedDebt),
			slog.Any("compactions_in_progress", snap.Compact.NumInProgress),
			slog.Any("compaction_in_progress_bytes", snap.Compact.InProgressBytes),
		)
	}
	return ready
}

func (lr *LockResolver) primaryGroupReadyForBackgroundAbort(primaryKey []byte) bool {
	pg, ok := lr.store.groupForKey(primaryKey)
	return ok && lockResolverGroupCanResolve(pg)
}

func (lr *LockResolver) logLockResolverYield(gid uint64, err error) {
	lr.log.Warn("lock resolver: yielding after resolver budget",
		slog.Uint64("gid", gid),
		slog.Any("err", err),
	)
}

func lockResolverRaftReady(engine interface {
	raftengine.LeaderView
	raftengine.StatusReader
}) bool {
	if !isLeaderEngine(engine) {
		return false
	}
	status := engine.Status()
	if status.State != raftengine.StateLeader {
		return false
	}
	if status.LeadTransferee != 0 || status.PendingConfChange {
		return false
	}
	if status.FSMPending > 0 {
		return false
	}
	return status.AppliedIndex >= status.CommitIndex
}

func lockResolverLSMBackpressured(st store.MVCCStore) (bool, *pebble.Metrics) {
	source, ok := st.(pebbleMetricsSource)
	if !ok {
		return false, nil
	}
	snap := source.Metrics()
	if snap == nil {
		return false, nil
	}
	return lsmWriteBackpressured(snap, lsmBackpressureLimits{
		maxL0Files:      lockResolverMaxL0Files,
		maxL0Sublevels:  lockResolverMaxL0Sublevels,
		maxLSMDebtBytes: lockResolverMaxLSMDebtBytes,
	}), snap
}

func lockResolverBudgetExhausted(err error, workCtx, parentCtx context.Context) bool {
	if err == nil || workCtx == nil {
		return false
	}
	if parentCtx != nil && parentCtx.Err() != nil {
		return false
	}
	return workCtx.Err() != nil && errors.Is(err, workCtx.Err())
}

func (lr *LockResolver) resolveExpiredLock(ctx context.Context, g *ShardGroup, userKey []byte, lock txnLock) error {
	status, commitTS, err := lr.backgroundPrimaryTxnStatus(ctx, lock)
	if err != nil {
		return err
	}

	switch status {
	case txnStatusCommitted:
		return applyTxnResolution(ctx, g, pb.Phase_COMMIT, lock.StartTS, commitTS, lock.PrimaryKey, [][]byte{userKey})
	case txnStatusRolledBack:
		abortTS := abortTSFrom(lock.StartTS, commitTS)
		if abortTS <= lock.StartTS {
			return nil // cannot represent abort timestamp, skip
		}
		return applyTxnResolution(ctx, g, pb.Phase_ABORT, lock.StartTS, abortTS, lock.PrimaryKey, [][]byte{userKey})
	case txnStatusPending:
		// Lock is expired but primary is still pending. The background path
		// skips abort when the primary shard is not locally ready, so retry
		// cleanup on a later cycle.
		return nil
	default:
		return errors.Wrapf(ErrTxnInvalidMeta, "unknown txn status for key %s", string(userKey))
	}
}

func (lr *LockResolver) backgroundPrimaryTxnStatus(ctx context.Context, lock txnLock) (txnStatus, uint64, error) {
	status, commitTS, done, err := lr.store.primaryTxnRecordedStatus(ctx, lock.PrimaryKey, lock.StartTS)
	if err != nil || done {
		return status, commitTS, err
	}

	primaryLock, locked, err := lr.store.primaryTxnLock(ctx, lock.PrimaryKey, lock.StartTS)
	if err != nil {
		return txnStatusPending, 0, err
	}
	if !locked {
		return txnStatusRolledBack, 0, nil
	}
	if !txnLockExpired(primaryLock) {
		return txnStatusPending, 0, nil
	}
	if !lr.primaryGroupReadyForBackgroundAbort(lock.PrimaryKey) {
		return txnStatusPending, 0, nil
	}
	return lr.store.expiredPrimaryTxnStatus(ctx, lock.PrimaryKey, lock.StartTS)
}
