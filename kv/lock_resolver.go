package kv

import (
	"context"
	"log/slog"
	"math"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const (
	// lockResolverInterval is how often the background resolver runs.
	lockResolverInterval = 10 * time.Second
	// lockResolverBatchSize limits the number of locks scanned per group per cycle.
	lockResolverBatchSize = 100
	// txnNamespaceSentinel is appended to a namespace prefix key to produce a
	// key that sorts just after all valid keys in that namespace.
	txnNamespaceSentinel byte = 0xFF
)

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
	for gid, g := range lr.groups {
		if ctx.Err() != nil {
			return
		}
		// Only resolve on the leader to avoid duplicate work.
		if g.Raft == nil || g.Raft.State() != raft.Leader {
			continue
		}
		if err := lr.resolveGroupLocks(ctx, gid, g); err != nil {
			lr.log.Warn("lock resolver: group scan failed",
				slog.Uint64("gid", gid),
				slog.Any("err", err),
			)
		}
	}
}

func (lr *LockResolver) resolveGroupLocks(ctx context.Context, gid uint64, g *ShardGroup) error {
	if g.Store == nil {
		return nil
	}

	// Scan lock key range: [!txn|lock| ... !txn|lock|<max>)
	lockStart := txnLockKey(nil)                             // "!txn|lock|"
	lockEnd := append(txnLockKey(nil), txnNamespaceSentinel) // one past the lock namespace

	lockKVs, err := g.Store.ScanAt(ctx, lockStart, lockEnd, lockResolverBatchSize, math.MaxUint64)
	if err != nil {
		return errors.WithStack(err)
	}

	var resolved, skipped int
	for _, kvp := range lockKVs {
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		userKey, ok := txnUserKeyFromLockKey(kvp.Key)
		if !ok {
			continue
		}

		lock, err := decodeTxnLock(kvp.Value)
		if err != nil {
			lr.log.Warn("lock resolver: decode lock failed",
				slog.String("key", string(userKey)),
				slog.Any("err", err),
			)
			continue
		}

		// Only resolve expired locks — active transaction locks are not touched.
		if !txnLockExpired(lock) {
			skipped++
			continue
		}

		if err := lr.resolveExpiredLock(ctx, g, userKey, lock); err != nil {
			lr.log.Warn("lock resolver: resolve failed",
				slog.Uint64("gid", gid),
				slog.String("key", string(userKey)),
				slog.Uint64("start_ts", lock.StartTS),
				slog.Any("err", err),
			)
			continue
		}
		resolved++
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

func (lr *LockResolver) resolveExpiredLock(ctx context.Context, g *ShardGroup, userKey []byte, lock txnLock) error {
	status, commitTS, err := lr.store.primaryTxnStatus(ctx, lock.PrimaryKey, lock.StartTS)
	if err != nil {
		return err
	}

	switch status {
	case txnStatusCommitted:
		return applyTxnResolution(g, pb.Phase_COMMIT, lock.StartTS, commitTS, lock.PrimaryKey, [][]byte{userKey})
	case txnStatusRolledBack:
		abortTS := abortTSFrom(lock.StartTS, commitTS)
		if abortTS <= lock.StartTS {
			return nil // cannot represent abort timestamp, skip
		}
		return applyTxnResolution(g, pb.Phase_ABORT, lock.StartTS, abortTS, lock.PrimaryKey, [][]byte{userKey})
	case txnStatusPending:
		// Lock is expired but primary is still pending — the primary's
		// tryAbortExpiredPrimary inside primaryTxnStatus should have
		// attempted to abort it. If it couldn't (e.g., primary shard
		// unreachable), we skip and retry next cycle.
		return nil
	default:
		return errors.Wrapf(ErrTxnInvalidMeta, "unknown txn status for key %s", string(userKey))
	}
}
