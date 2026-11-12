package main

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func configureCommitTSFenceActivation(coordinate *kv.ShardedCoordinator, enabled bool, runtimes []*raftGroupRuntime) {
	if enabled {
		coordinate.WithCommitTSFenceActivation().WithCommitTSFenceActivationReady(func() bool {
			return commitTSFenceRuntimesReady(runtimes)
		})
	}
}

func validateCommitTSFenceStartup(enabled, protected bool) error {
	if enabled && !protected {
		return errors.New("commitTSFence requires protected Raft catalog bootstrap; legacy startup catalog writes outside Raft")
	}
	return nil
}

func validateNoExistingCommitTSFenceActivation(ctx context.Context, runtimes []*raftGroupRuntime, protected bool) error {
	for _, rt := range runtimes {
		if rt == nil || rt.store == nil {
			continue
		}
		active, err := kv.CommitTSFenceEnabled(ctx, rt.store)
		if err != nil {
			return errors.Wrapf(err, "read commit-ts fence activation for group %d", rt.spec.id)
		}
		if active && !protected {
			return errors.WithStack(errors.Newf("group %d has an active commit-ts fence; protected Raft catalog bootstrap is required before startup", rt.spec.id))
		}
	}
	return nil
}
