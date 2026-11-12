package main

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

const raftJoinReadyPollInterval = 100 * time.Millisecond

func preparePublicServicesAfterRaftJoinReady(
	ctx context.Context,
	runtimes []*raftGroupRuntime,
	localID string,
	enabled bool,
	prepare func() error,
) error {
	if err := waitForRaftJoinReady(ctx, runtimes, localID, enabled); err != nil {
		return errors.Wrap(err, "wait for fresh raft join before binding public services")
	}
	return prepare()
}

func waitForRaftJoinReady(ctx context.Context, runtimes []*raftGroupRuntime, localID string, enabled bool) error {
	if !enabled {
		return nil
	}
	ticker := time.NewTicker(raftJoinReadyPollInterval)
	defer ticker.Stop()
	for {
		ready, err := raftJoinRuntimesReady(ctx, runtimes, localID)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

func raftJoinRuntimesReady(ctx context.Context, runtimes []*raftGroupRuntime, localID string) (bool, error) {
	if len(runtimes) == 0 {
		return false, nil
	}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.engine == nil {
			return false, nil
		}
		configuration, err := runtime.engine.Configuration(ctx)
		if err != nil {
			return false, errors.Wrapf(err, "read raft group %d join configuration", runtime.spec.ID)
		}
		if !configurationContainsMember(configuration, localID) {
			return false, nil
		}
		status := runtime.engine.Status()
		if status.Leader.ID == "" || status.PendingConfChange || status.AppliedIndex < status.CommitIndex {
			return false, nil
		}
	}
	return true, nil
}

func configurationContainsMember(configuration raftengine.Configuration, localID string) bool {
	for _, server := range configuration.Servers {
		if server.ID == localID && (server.Suffrage == "learner" || server.Suffrage == raftMemberSuffrageVoter) {
			return true
		}
	}
	return false
}
