package kv

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const linearizableReadPollInterval = 2 * time.Millisecond
const linearizableReadStableBootstrapChecks = 2

type linearizableRaft interface {
	raftLeaderVerifier
	Stats() map[string]string
}

type linearizableReadCoordinator interface {
	LinearizableRead(ctx context.Context) (uint64, error)
	LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error)
}

func linearizableReadIndex(ctx context.Context, r *raft.Raft) (uint64, error) {
	if r == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return linearizableReadIndexWithWaiter(ctx, r, appliedIndexWaiterForRaft(r))
}

func linearizableReadIndexWithWaiter(ctx context.Context, r linearizableRaft, waiter AppliedIndexWaiter) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	if r.State() != raft.Leader {
		return 0, errors.WithStack(raft.ErrNotLeader)
	}
	if err := defaultRaftLeaderVerifyCache.verify(r); err != nil {
		return 0, err
	}

	target := parseLinearizableReadStat(r.Stats(), "commit_index")
	if err := waitForLinearizableReadIndex(ctx, r, waiter, target); err != nil {
		return 0, errors.WithStack(err)
	}
	return target, nil
}

func waitForLinearizableReadIndex(ctx context.Context, r linearizableRaft, waiter AppliedIndexWaiter, target uint64) error {
	if target == 0 {
		return nil
	}
	done, err := waitForTrackedAppliedIndex(ctx, waiter, target)
	if done {
		return err
	}
	return waitForBootstrapAppliedIndex(ctx, r, waiter, target)
}

func waitForTrackedAppliedIndex(ctx context.Context, waiter AppliedIndexWaiter, target uint64) (bool, error) {
	if waiter == nil {
		return false, nil
	}
	current := waiter.AppliedIndex()
	if current >= target {
		return true, nil
	}
	if current == 0 {
		return false, nil
	}
	return true, errors.WithStack(waiter.WaitForAppliedIndex(ctx, target))
}

func waitForBootstrapAppliedIndex(ctx context.Context, r linearizableRaft, waiter AppliedIndexWaiter, target uint64) error {
	ticker := time.NewTicker(linearizableReadPollInterval)
	defer ticker.Stop()

	stableBootstrapReads := 0
	for {
		if hasReachedTarget(waiter, target) {
			return nil
		}

		if waiter == nil || waiter.AppliedIndex() == 0 {
			if bootstrapReadReady(r.Stats(), target) {
				stableBootstrapReads++
				if stableBootstrapReads >= linearizableReadStableBootstrapChecks {
					return nil
				}
			} else {
				stableBootstrapReads = 0
			}
		}

		select {
		case <-ctx.Done():
			return errors.WithStack(context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

func hasReachedTarget(waiter AppliedIndexWaiter, target uint64) bool {
	return waiter != nil && waiter.AppliedIndex() >= target
}

func bootstrapReadReady(stats map[string]string, target uint64) bool {
	return parseLinearizableReadStat(stats, "applied_index") >= target &&
		parseLinearizableReadStat(stats, "fsm_pending") == 0
}

func parseLinearizableReadStat(stats map[string]string, key string) uint64 {
	if stats == nil {
		return 0
	}
	raw := stats[key]
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func CoordinatorLinearizableRead(ctx context.Context, c Coordinator) (uint64, error) {
	if c == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	if reader, ok := any(c).(linearizableReadCoordinator); ok {
		index, err := reader.LinearizableRead(ctx)
		return index, errors.WithStack(err)
	}
	if err := c.VerifyLeader(); err != nil {
		return 0, errors.WithStack(err)
	}
	return 0, nil
}

func CoordinatorLinearizableReadForKey(ctx context.Context, c Coordinator, key []byte) (uint64, error) {
	if c == nil {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	if reader, ok := any(c).(linearizableReadCoordinator); ok {
		index, err := reader.LinearizableReadForKey(ctx, key)
		return index, errors.WithStack(err)
	}
	if err := c.VerifyLeaderForKey(key); err != nil {
		return 0, errors.WithStack(err)
	}
	return 0, nil
}
