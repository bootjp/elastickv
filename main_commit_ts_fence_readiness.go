package main

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// Activation must not infer readiness from a missing runtime callback. The
// legacy public gate permits that shape, but a fence changes Raft apply verdicts.
func commitTSFenceRuntimesReady(runtimes []*raftGroupRuntime) bool {
	if len(runtimes) == 0 {
		return false
	}
	domain := bootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return false
	}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.bootstrapDomain != domain || runtime.publicReady == nil || !runtime.publicReady() {
			return false
		}
	}
	return true
}

// Keep public listeners closed until every local replica has applied the
// activation entry. The renewal worker may propose it after DataReady, but
// DataReady alone does not imply that the fence is active yet.
func markPublicGateReadyAfterCommitTSFence(ctx context.Context, gate *startupPublicKVGate, runtimes []*raftGroupRuntime, enabled bool) error {
	if enabled {
		if err := waitForCommitTSFenceActivation(ctx, runtimes); err != nil {
			return err
		}
	}
	gate.markReady()
	return nil
}

func waitForCommitTSFenceActivation(ctx context.Context, runtimes []*raftGroupRuntime) error {
	ticker := time.NewTicker(bootstrapReadinessRetryInterval)
	defer ticker.Stop()
	for {
		active, err := commitTSFenceRuntimesActive(ctx, runtimes)
		if err != nil {
			return err
		}
		if active {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

func commitTSFenceRuntimesActive(ctx context.Context, runtimes []*raftGroupRuntime) (bool, error) {
	if len(runtimes) == 0 {
		return false, nil
	}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.store == nil {
			return false, nil
		}
		active, err := kv.CommitTSFenceEnabled(ctx, runtime.store)
		if err != nil {
			return false, errors.Wrapf(err, "read commit-ts fence activation for group %d", runtime.spec.id)
		}
		if !active {
			return false, nil
		}
	}
	return true, nil
}
