package main

import (
	"context"
	"log/slog"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/bootjp/elastickv/internal/snapshotoffloadwiring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// snapshotOffloadGroups uses snapshotEngine() for leadership callbacks so
// scheduler reads are synchronized with runtime shutdown.
func snapshotOffloadGroups(
	runtimes []*clusterbootstrap.GroupRuntime, raftDir, raftID string, multi bool,
) []snapshotoffload.OffloadGroup {
	groups := make([]snapshotoffload.OffloadGroup, 0, len(runtimes))
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		groups = append(groups, snapshotoffload.OffloadGroup{
			GroupID:      rt.Spec.ID,
			DataDir:      noderuntime.GroupDataDir(raftDir, raftID, rt.Spec.ID, multi),
			IsLeader:     snapshotOffloadIsLeader(rt),
			VerifyLeader: snapshotOffloadVerifyLeader(rt),
		})
	}
	return groups
}

func snapshotOffloadIsLeader(rt *clusterbootstrap.GroupRuntime) func() bool {
	return func() bool {
		engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
		return engine != nil && engine.State() == raftengine.StateLeader
	}
}

// snapshotOffloadVerifyLeader is the §4 pre-commit re-verification: a
// multi-gigabyte spool takes long enough to lose an election, so
// leadership must hold at the instant the manifest commits, not merely
// when the snapshot was opened.
func snapshotOffloadVerifyLeader(rt *clusterbootstrap.GroupRuntime) func(context.Context) error {
	return func(ctx context.Context) error {
		engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
		if engine == nil {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions,
				"snapshot offload: raft engine closed")
		}
		verifier, ok := engine.(interface {
			VerifyLeader(context.Context) error
		})
		if !ok {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions,
				"snapshot offload: raft engine cannot verify leadership")
		}
		return errors.Wrap(verifier.VerifyLeader(ctx), "snapshot offload: verify leadership")
	}
}

// startSnapshotOffload starts snapshot offload for this node's Raft groups;
// snapshotoffloadwiring.Start does the work once the runtimes are reduced
// to offload groups.
func startSnapshotOffload(
	ctx context.Context,
	eg *errgroup.Group,
	f snapshotoffloadwiring.Flags,
	runtimes []*clusterbootstrap.GroupRuntime,
	raftDir, raftID string,
	multi bool,
	observer snapshotoffload.SchedulerObserver,
	logger *slog.Logger,
) error {
	return errors.WithStack(snapshotoffloadwiring.Start(ctx, eg, f, snapshotOffloadGroups(runtimes, raftDir, raftID, multi), buildVersion(), observer, logger))
}
