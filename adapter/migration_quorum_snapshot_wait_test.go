package adapter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Snapshot serialization and publication share the caller's write/snapshot phase
// budget; a separate short deadline can reject valid, still-progressing replicas.
func waitForQuorumAcceptanceSnapshots(ctx context.Context, target uint64, readIndexes func() []uint64) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var indexes []uint64
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "native snapshot coverage target=%d snapshots=%v", target, indexes)
		}
		indexes = readIndexes()
		covered := len(indexes) != 0
		for _, index := range indexes {
			if index < target {
				covered = false
			}
		}
		if covered {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "native snapshot coverage target=%d snapshots=%v", target, indexes)
		case <-ticker.C:
		}
	}
}

func TestQuorumAcceptanceSnapshotWaitUsesPhaseBudget(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var learner atomic.Uint64
	learner.Store(21)
	delayed := time.AfterFunc(5*time.Second+250*time.Millisecond, func() { learner.Store(22) })
	defer delayed.Stop()
	err := waitForQuorumAcceptanceSnapshots(ctx, 22, func() []uint64 {
		return []uint64{22, 22, 22, learner.Load()}
	})
	require.NoError(t, err, "the learner may finish after five seconds within the existing phase deadline")
	require.Equal(t, uint64(22), learner.Load())
}

func TestQuorumAcceptanceSnapshotWaitRejectsPartialCoverage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := waitForQuorumAcceptanceSnapshots(ctx, 22, func() []uint64 {
		cancel()
		return []uint64{22, 22, 22, 21}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "target=22 snapshots=[22 22 22 21]")
}

func TestQuorumAcceptanceSnapshotWaitRejectsExpiredPhase(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithDeadline(t.Context(), time.Time{})
	defer cancel()
	calls := 0
	err := waitForQuorumAcceptanceSnapshots(ctx, 22, func() []uint64 {
		calls++
		return []uint64{22, 22, 22, 22}
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, calls)
}

func TestQuorumAcceptanceSnapshotWaitRejectsCanceledPhase(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	err := waitForQuorumAcceptanceSnapshots(ctx, 22, func() []uint64 {
		calls++
		return []uint64{22, 22, 22, 22}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestQuorumAcceptanceSnapshotWaitRejectsEmptyCoverage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := waitForQuorumAcceptanceSnapshots(ctx, 22, func() []uint64 {
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "target=22 snapshots=[]")
}

func TestQuorumAcceptanceSnapshotWaitAcceptsCompleteCoverage(t *testing.T) {
	t.Parallel()
	err := waitForQuorumAcceptanceSnapshots(t.Context(), 22, func() []uint64 {
		return []uint64{22, 23, 24, 25}
	})
	require.NoError(t, err)
}
