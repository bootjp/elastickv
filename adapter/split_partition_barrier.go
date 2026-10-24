package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const splitPartitionApplyPoll = 5 * time.Millisecond

func (s *DistributionServer) splitPartitionAdmissionView(ctx context.Context, id uint64, group *kv.ShardGroup) (store.MigrationServingState, error) {
	if group == nil || group.Store == nil || group.Engine == nil {
		return store.MigrationServingState{}, errors.WithStack(store.ErrNotSupported)
	}
	reader, ok := group.Store.(store.MigrationServingAdmissionReader)
	if !ok {
		return store.MigrationServingState{}, errors.WithStack(store.ErrNotSupported)
	}
	if err := s.fenceSplitPartitionGroup(ctx, id, group); err != nil {
		return store.MigrationServingState{}, err
	}
	view, err := reader.MigrationServingAdmissionState(ctx)
	return view, errors.WithStack(err)
}

func (s *DistributionServer) fenceSplitPartitionGroup(ctx context.Context, id uint64, group *kv.ShardGroup) error {
	attempt, cancel := context.WithTimeout(ctx, ownershipRefreshBudget)
	defer cancel()
	if group.Engine.State() == raftengine.StateLeader {
		_, err := group.Engine.LinearizableRead(attempt)
		return errors.WithStack(err)
	}
	// The root leader need not lead the data group. Obtain that exact group's
	// leader fence, then wait for this local native store to have applied it.
	// A representative-key fallback could fence a different owner after a split.
	leader, ok := s.coordinator.(interface {
		LeaseReadForGroup(context.Context, uint64) (uint64, error)
	})
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	index, err := leader.LeaseReadForGroup(attempt, id)
	if err != nil {
		return errors.WithStack(err)
	}
	return waitSplitPartitionApplied(attempt, group.Engine, index)
}

func waitSplitPartitionApplied(ctx context.Context, engine raftengine.LeaderView, index uint64) error {
	applied, ok := engine.(interface{ AppliedIndex() uint64 })
	if !ok || index == 0 {
		return errors.WithStack(store.ErrNotSupported)
	}
	ticker := time.NewTicker(splitPartitionApplyPoll)
	defer ticker.Stop()
	for applied.AppliedIndex() < index {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
	return errors.WithStack(ctx.Err())
}
