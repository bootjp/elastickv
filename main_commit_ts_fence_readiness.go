package main

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// Activation must not infer readiness from a missing runtime callback. The
// legacy public gate permits that shape, but a fence changes Raft apply verdicts.
func commitTSFenceRuntimesReady(runtimes []*clusterbootstrap.GroupRuntime) bool {
	if len(runtimes) == 0 {
		return false
	}
	domain := clusterbootstrap.BootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return false
	}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.BootstrapDomain != domain || runtime.PublicReady == nil || !runtime.PublicReady() {
			return false
		}
	}
	return true
}

// Keep public listeners closed until every local replica has applied the
// activation entry. The renewal worker may propose it after DataReady, but
// DataReady alone does not imply that the fence is active yet.
func markPublicGateReadyAfterCommitTSFence(ctx context.Context, gate *clusterbootstrap.StartupPublicKVGate, runtimes []*clusterbootstrap.GroupRuntime, enabled bool) error {
	if enabled {
		if err := waitForCommitTSFenceActivation(ctx, runtimes); err != nil {
			return err
		}
	}
	gate.MarkReady()
	return nil
}

func waitForCommitTSFenceActivation(ctx context.Context, runtimes []*clusterbootstrap.GroupRuntime) error {
	ticker := time.NewTicker(clusterbootstrap.BootstrapReadinessRetryInterval)
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

// Only KV data groups carry the activation marker. The dedicated TSO group's
// runtime store is its control store and its ShardGroup has no KV Store, so
// the coordinator never proposes the activation there; waiting on it would
// keep public traffic closed forever. At least one data group is required.
func commitTSFenceRuntimesActive(ctx context.Context, runtimes []*clusterbootstrap.GroupRuntime) (bool, error) {
	dataGroups := 0
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Store == nil {
			return false, nil
		}
		if runtime.Spec.ID == noderuntime.DedicatedTSORaftGroupID {
			continue
		}
		dataGroups++
		active, err := kv.CommitTSFenceEnabled(ctx, runtime.Store)
		if err != nil {
			return false, errors.Wrapf(err, "read commit-ts fence activation for group %d", runtime.Spec.ID)
		}
		if !active {
			return false, nil
		}
	}
	return dataGroups > 0, nil
}
