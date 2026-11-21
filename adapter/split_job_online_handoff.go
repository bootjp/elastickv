package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	onlineSplitHandoffMaxFrames = 8
	onlineSplitHandoffMaxBytes  = 512 << 10
)

// Recheck the outstanding journal before the first fence. Writes after this
// observation are included in the seal, so the limits bound the observed tail.
func (s *DistributionServer) admitOnlineSplitHandoff(ctx context.Context, job distribution.SplitJob) (bool, error) {
	snapshot, route, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return false, err
	}
	if route.State == distribution.RouteStateWriteFenced {
		return true, nil
	}
	if route.State != distribution.RouteStateActive || route.GroupID != job.SourceGroupID {
		return false, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	pass := splitPreflightPass{route: route, catalogVersion: snapshot.Version}
	return s.checkActiveOnlineSplitTail(ctx, job, pass)
}

func (s *DistributionServer) checkActiveOnlineSplitTail(ctx context.Context, job distribution.SplitJob, pass splitPreflightPass) (bool, error) {
	goal, err := store.DecodeOnlineMigrationManifest(job.OnlineManifest)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if job.OnlineSequence < goal.Sequence {
		// Another runner may have advanced the goal before this job was reloaded.
		return false, nil
	}
	source, err := s.splitJobSourceClient(ctx, job, pass.route.GroupID)
	if err != nil {
		return false, err
	}
	page, head, err := exportOnlineSplitPage(ctx, source, job.JobID, job.SplitKey, pass.route.End, job.OnlineSequence)
	if err != nil {
		return false, err
	}
	if !sameOnlineSplitManifest(job.OnlineManifest, head) {
		return false, errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if head.Sealed {
		// A concurrent runner fenced and sealed after we observed the active route.
		return false, nil
	}
	withinBudget, err := onlineSplitHandoffWithinBudget(goal, head, job.OnlineSequence)
	if err != nil || withinBudget {
		return withinBudget, err
	}

	next := distribution.CloneSplitJob(job)
	next.OnlineManifest = bytes.Clone(page.GetOnlineManifest())
	return false, s.writeSplitPreflightJob(ctx, job, next, pass)
}

func onlineSplitHandoffWithinBudget(goal, head store.OnlineMigrationSession, acknowledged uint64) (bool, error) {
	if head.Sequence < acknowledged || head.Bytes < goal.Bytes || head.LastIndex < goal.LastIndex {
		return false, errors.WithStack(store.ErrMigrationCaptureCorrupt)
	}
	return head.Sequence-acknowledged <= onlineSplitHandoffMaxFrames && head.Bytes-goal.Bytes <= onlineSplitHandoffMaxBytes, nil
}
