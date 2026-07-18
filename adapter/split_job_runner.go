package adapter

import (
	"bytes"
	"context"
	"io"
	"math"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	defaultSplitMigrationChunkBytes      = 1 << 20
	defaultSplitMigrationMaxScannedBytes = 4 << 20
	defaultSplitMigrationLockProbeLimit  = 1024
)

func (s *DistributionServer) runSplitJobPhase(ctx context.Context, job distribution.SplitJob) error {
	if job.Phase != distribution.SplitJobPhaseCleanup && s.splitMigrationClientFactory == nil {
		return errors.New("split migration client factory is not configured")
	}
	if job.Phase == distribution.SplitJobPhaseBackfill || job.Phase == distribution.SplitJobPhaseDeltaCopy {
		return s.runSplitJobCopyPhase(ctx, job)
	}
	return s.runSplitJobControlPhase(ctx, job)
}

func (s *DistributionServer) runSplitJobCopyPhase(ctx context.Context, job distribution.SplitJob) error {
	if job.Phase == distribution.SplitJobPhaseBackfill {
		return s.copySplitJobPhase(ctx, job, distribution.SplitJobExportPhaseBackfill, 0, job.SnapshotTS)
	}
	return s.copySplitJobPhase(ctx, job, distribution.SplitJobExportPhaseDeltaCopy, job.DeltaFloor, job.FenceTS)
}

func (s *DistributionServer) runSplitJobControlPhase(ctx context.Context, job distribution.SplitJob) error {
	switch job.Phase {
	case distribution.SplitJobPhasePlanned:
		return s.beginSplitJobBackfill(ctx, job)
	case distribution.SplitJobPhaseFence:
		return s.finalizeSplitJobFence(ctx, job)
	case distribution.SplitJobPhaseCutover:
		return s.cutoverSplitJob(ctx, job)
	case distribution.SplitJobPhaseCleanup:
		return s.cleanupSplitJob(ctx, job)
	case distribution.SplitJobPhaseAbandoning:
		return errors.New("split job abandon cleanup is not configured")
	case distribution.SplitJobPhaseNone,
		distribution.SplitJobPhaseBackfill,
		distribution.SplitJobPhaseDeltaCopy,
		distribution.SplitJobPhaseDone,
		distribution.SplitJobPhaseFailed,
		distribution.SplitJobPhaseAbandoned:
		return nil
	}
	return errors.New("split job phase is not runnable")
}

func (s *DistributionServer) cleanupSplitJob(ctx context.Context, job distribution.SplitJob) error {
	if job.SourceRetentionPinTS > 0 && job.SourceRetentionPinTS != math.MaxUint64 {
		if err := s.releaseSplitJobSourceRetention(ctx, job); err != nil {
			return err
		}
		return nil
	}
	return s.promoteSplitJobTargetAndComplete(ctx, job)
}

func (s *DistributionServer) releaseSplitJobSourceRetention(ctx context.Context, job distribution.SplitJob) error {
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return err
	}
	sourceGroupID, routeEnd, ok := splitJobSourceSibling(snapshot.Routes, job)
	if !ok {
		return errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	source, _, err := s.splitMigrationClientFactory(ctx, job, sourceGroupID)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := applySplitMigrationControl(
		ctx,
		source,
		job,
		routeEnd,
		job.CutoverVersion,
		job.FenceTS,
		true,
		true,
		false,
		math.MaxUint64,
	); err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup {
			current.SourceRetentionPinTS = math.MaxUint64
			current.SourceCutoverAckCursor = []byte("retention-released")
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func splitJobSourceSibling(routes []distribution.RouteDescriptor, job distribution.SplitJob) (uint64, []byte, bool) {
	var sourceGroupID uint64
	var routeEnd []byte
	for _, route := range routes {
		if route.ParentRouteID != job.SourceRouteID {
			continue
		}
		if bytes.Equal(route.Start, job.SplitKey) {
			routeEnd = distribution.CloneBytes(route.End)
			continue
		}
		if bytes.Equal(route.End, job.SplitKey) {
			sourceGroupID = route.GroupID
		}
	}
	return sourceGroupID, routeEnd, sourceGroupID != 0
}

func (s *DistributionServer) beginSplitJobBackfill(ctx context.Context, job distribution.SplitJob) error {
	snapshot, parent, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return err
	}
	source, _, err := s.splitMigrationClientFactory(ctx, job, parent.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}
	if source == nil {
		return errors.New("split migration source client is nil")
	}
	snapshotTS := s.engine.NextTimestamp()
	if snapshotTS == 0 {
		return errors.New("split migration snapshot timestamp is zero")
	}
	// The source guard is applied before BACKFILL opens. This keeps the initial
	// implementation conservative: no write can land outside the copied window.
	if _, err := applySplitMigrationControl(ctx, source, job, parent.End, snapshot.Version+1, snapshotTS, false, false, true, 1); err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase != distribution.SplitJobPhasePlanned {
			return current, nil
		}
		current.Phase = distribution.SplitJobPhaseBackfill
		current.SnapshotTS = snapshotTS
		current.WriteTrackerArmed = true
		current.SourceRetentionPinTS = 1
		current.UpdatedAtMs = time.Now().UnixMilli()
		return current, nil
	})
}

func (s *DistributionServer) copySplitJobPhase(
	ctx context.Context,
	job distribution.SplitJob,
	exportPhase distribution.SplitJobExportPhase,
	minCommitTS uint64,
	maxCommitTS uint64,
) error {
	if maxCommitTS == 0 {
		return errors.New("split migration copy upper timestamp is zero")
	}
	_, sourceRoute, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return err
	}
	brackets, err := distribution.PlanExportBrackets(job.SplitKey, sourceRoute.End)
	if err != nil {
		return errors.WithStack(err)
	}
	progressIndex, bracket, ok := nextSplitJobBracket(job, brackets, exportPhase)
	if !ok {
		return s.advanceCompletedCopyPhase(ctx, job, exportPhase)
	}
	source, target, err := s.splitMigrationClientFactory(ctx, job, sourceRoute.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}
	if source == nil || target == nil {
		return errors.New("split migration source or target client is nil")
	}
	progress := job.BracketProgress[progressIndex]
	stream, err := source.ExportRangeVersions(ctx, &pb.ExportRangeVersionsRequest{
		RangeStart:           bracket.Start,
		RangeEnd:             bracket.End,
		MaxCommitTs:          maxCommitTS,
		MinCommitTs:          minCommitTS,
		Cursor:               progress.Cursor,
		ChunkBytes:           defaultSplitMigrationChunkBytes,
		RouteStart:           job.SplitKey,
		RouteEnd:             sourceRoute.End,
		MaxScannedBytes:      defaultSplitMigrationMaxScannedBytes,
		KeyFamily:            bracket.Family,
		ExcludeKnownInternal: bracket.ExcludeKnownInternal,
		ExcludePrefixes:      bracket.ExcludePrefixes,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return s.copySplitJobStream(ctx, job, progressIndex, bracket, progress, stream, target)
}

func (s *DistributionServer) copySplitJobStream(
	ctx context.Context,
	job distribution.SplitJob,
	progressIndex int,
	bracket distribution.MigrationBracket,
	progress distribution.SplitJobBracketProgress,
	stream grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse],
	target SplitMigrationClient,
) error {
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return errors.WithStack(recvErr)
		}
		nextCursor := distribution.CloneBytes(resp.GetNextCursor())
		batchSeq := progress.LastAckedBatchSeq + 1
		importResp, importErr := target.ImportRangeVersions(ctx, &pb.ImportRangeVersionsRequest{
			JobId:     job.JobID,
			Versions:  resp.GetVersions(),
			Cursor:    nextCursor,
			BracketId: bracket.BracketID,
			BatchSeq:  batchSeq,
		})
		if importErr != nil {
			return errors.WithStack(importErr)
		}
		if !bytes.Equal(importResp.GetAckedCursor(), nextCursor) {
			return errors.New("split migration import acknowledged a different cursor")
		}
		progress.Cursor = nextCursor
		progress.Done = resp.GetDone()
		progress.AcceptedRows += uint64(len(resp.GetVersions()))
		progress.LastAckedBatchSeq = batchSeq
		for _, version := range resp.GetVersions() {
			if version.GetCommitTs() > job.MaxImportedTS {
				job.MaxImportedTS = version.GetCommitTs()
			}
		}
		job.BracketProgress[progressIndex] = progress
		job.Cursor = nextCursor
		job.UpdatedAtMs = time.Now().UnixMilli()
		if err := s.persistSplitJobCopyProgress(ctx, job); err != nil {
			return err
		}
		if progress.Done {
			return nil
		}
	}
}

func nextSplitJobBracket(
	job distribution.SplitJob,
	brackets []distribution.MigrationBracket,
	exportPhase distribution.SplitJobExportPhase,
) (int, distribution.MigrationBracket, bool) {
	byID := make(map[uint64]distribution.MigrationBracket, len(brackets))
	for _, bracket := range brackets {
		byID[bracket.BracketID] = bracket
	}
	for i, progress := range job.BracketProgress {
		if progress.ExportPhase != exportPhase || progress.Done {
			continue
		}
		bracket, found := byID[progress.BracketID]
		if found && bracket.Family == progress.Family {
			return i, bracket, true
		}
	}
	return 0, distribution.MigrationBracket{}, false
}

func (s *DistributionServer) persistSplitJobCopyProgress(ctx context.Context, next distribution.SplitJob) error {
	return s.updateSplitJobViaCoordinator(ctx, next.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase != next.Phase {
			return current, nil
		}
		return distribution.CloneSplitJob(next), nil
	})
}

func (s *DistributionServer) advanceCompletedCopyPhase(ctx context.Context, job distribution.SplitJob, phase distribution.SplitJobExportPhase) error {
	switch phase {
	case distribution.SplitJobExportPhaseBackfill:
		return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
			if current.Phase == distribution.SplitJobPhaseBackfill {
				current.Phase = distribution.SplitJobPhaseFence
				current.UpdatedAtMs = time.Now().UnixMilli()
			}
			return current, nil
		})
	case distribution.SplitJobExportPhaseDeltaCopy:
		return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
			if current.Phase == distribution.SplitJobPhaseDeltaCopy {
				current.Phase = distribution.SplitJobPhaseCutover
				current.UpdatedAtMs = time.Now().UnixMilli()
			}
			return current, nil
		})
	case distribution.SplitJobExportPhaseNone:
		return errors.New("unknown split migration export phase")
	}
	return errors.New("unknown split migration export phase")
}

func (s *DistributionServer) finalizeSplitJobFence(ctx context.Context, job distribution.SplitJob) error {
	snapshot, sourceRoute, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return err
	}
	snapshot, sourceRoute, err = s.ensureSplitJobCatalogFence(ctx, job, snapshot, sourceRoute)
	if err != nil {
		return err
	}
	source, _, err := s.splitMigrationClientFactory(ctx, job, sourceRoute.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}
	minAdmittedTS, err := applySplitMigrationControl(
		ctx,
		source,
		job,
		sourceRoute.End,
		snapshot.Version,
		job.SnapshotTS,
		true,
		false,
		true,
		1,
	)
	if err != nil {
		return err
	}
	pendingLocks, err := splitJobPendingLocks(ctx, source, job.SplitKey, sourceRoute.End)
	if err != nil {
		return err
	}
	if pendingLocks {
		return nil
	}
	return s.commitSplitJobFenceState(ctx, job, snapshot.Version, minAdmittedTS)
}

func (s *DistributionServer) commitSplitJobFenceState(ctx context.Context, job distribution.SplitJob, fenceCatalogVersion, minAdmittedTS uint64) error {
	fenceTS := s.engine.NextTimestamp()
	deltaFloor := job.SnapshotTS
	if minAdmittedTS > 0 && minAdmittedTS-1 < deltaFloor {
		deltaFloor = minAdmittedTS - 1
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase != distribution.SplitJobPhaseFence {
			return current, nil
		}
		current.Phase = distribution.SplitJobPhaseDeltaCopy
		current.PostFenceDrainCompleted = true
		current.SnapshotMinAdmittedTS = minAdmittedTS
		current.DeltaFloor = deltaFloor
		current.FenceTS = fenceTS
		current.FenceCatalogVersion = fenceCatalogVersion
		current.FenceAckCursor = []byte("raft-applied")
		current.SourceRetentionPinTS = 1
		for i := range current.BracketProgress {
			current.BracketProgress[i].ExportPhase = distribution.SplitJobExportPhaseDeltaCopy
			current.BracketProgress[i].Cursor = nil
			current.BracketProgress[i].Done = false
			current.BracketProgress[i].ScannedBytes = 0
			current.BracketProgress[i].AcceptedRows = 0
		}
		current.UpdatedAtMs = time.Now().UnixMilli()
		return current, nil
	})
}

func (s *DistributionServer) ensureSplitJobCatalogFence(
	ctx context.Context,
	job distribution.SplitJob,
	snapshot distribution.CatalogSnapshot,
	sourceRoute distribution.RouteDescriptor,
) (distribution.CatalogSnapshot, distribution.RouteDescriptor, error) {
	if sourceRoute.RouteID != job.SourceRouteID {
		return snapshot, sourceRoute, nil
	}
	leftID, rightID, err := s.allocateChildRouteIDs(ctx, snapshot.ReadTS, snapshot.Routes)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.RouteDescriptor{}, err
	}
	left, right := splitCatalogRoutes(sourceRoute, job.SplitKey, leftID, rightID)
	right.State = distribution.RouteStateWriteFenced
	snapshot, err = s.saveSplitResultViaCoordinator(ctx, snapshot.ReadTS, snapshot.Version, sourceRoute.RouteID, nil, left, right)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.RouteDescriptor{}, err
	}
	if err := s.applyEngineSnapshot(snapshot); err != nil {
		return distribution.CatalogSnapshot{}, distribution.RouteDescriptor{}, err
	}
	return snapshot, right, nil
}

func splitJobPendingLocks(ctx context.Context, source SplitMigrationClient, routeStart []byte, routeEnd []byte) (bool, error) {
	resp, err := source.ProbeMigrationLocks(ctx, &pb.ProbeMigrationLocksRequest{
		RouteStart: routeStart,
		RouteEnd:   routeEnd,
		Limit:      defaultSplitMigrationLockProbeLimit,
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	return resp.GetPendingCount() != 0, nil
}

func (s *DistributionServer) cutoverSplitJob(ctx context.Context, job distribution.SplitJob) error {
	snapshot, sourceRoute, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return err
	}
	if snapshot.Version == math.MaxUint64 {
		return errors.New("split migration catalog version overflow")
	}
	expectedVersion := snapshot.Version + 1
	source, target, err := s.splitMigrationClientFactory(ctx, job, sourceRoute.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := applySplitMigrationControl(ctx, target, job, sourceRoute.End, expectedVersion, job.FenceTS, false, false, false, 0); err != nil {
		return err
	}
	if _, err := applySplitMigrationControl(ctx, source, job, sourceRoute.End, expectedVersion, job.FenceTS, true, true, false, 1); err != nil {
		return err
	}
	return s.commitSplitJobCutover(ctx, snapshot, sourceRoute, job, expectedVersion)
}

func applySplitMigrationControl(
	ctx context.Context,
	client SplitMigrationClient,
	job distribution.SplitJob,
	routeEnd []byte,
	expectedVersion uint64,
	minWriteTS uint64,
	sourceWriteFence bool,
	sourceReadFence bool,
	trackWrites bool,
	retentionPinTS uint64,
) (uint64, error) {
	if client == nil {
		return 0, errors.New("split migration control client is nil")
	}
	resp, err := client.ApplyTargetStagedReadiness(ctx, &pb.TargetStagedReadinessRequest{
		JobId:                  job.JobID,
		RouteStart:             job.SplitKey,
		RouteEnd:               routeEnd,
		ExpectedCutoverVersion: expectedVersion,
		MigrationJobId:         job.JobID,
		MinWriteTsExclusive:    minWriteTS,
		Armed:                  true,
		SourceWriteFence:       sourceWriteFence,
		SourceReadFence:        sourceReadFence,
		RetentionPinTs:         retentionPinTS,
		TrackWrites:            trackWrites,
	})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return resp.GetMinAdmittedTs(), nil
}

func (s *DistributionServer) splitJobSourceRoute(
	ctx context.Context,
	job distribution.SplitJob,
) (distribution.CatalogSnapshot, distribution.RouteDescriptor, error) {
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.RouteDescriptor{}, err
	}
	if parent, found := findRouteByID(snapshot.Routes, job.SourceRouteID); found {
		return snapshot, parent, nil
	}
	for _, route := range snapshot.Routes {
		if route.ParentRouteID == job.SourceRouteID && bytes.Equal(route.Start, job.SplitKey) {
			return snapshot, distribution.CloneRouteDescriptor(route), nil
		}
	}
	return distribution.CatalogSnapshot{}, distribution.RouteDescriptor{}, splitJobCatalogStatusError(distribution.ErrMigrationSourceRouteChanged)
}

func (s *DistributionServer) commitSplitJobCutover(
	ctx context.Context,
	snapshot distribution.CatalogSnapshot,
	right distribution.RouteDescriptor,
	job distribution.SplitJob,
	nextVersion uint64,
) error {
	right.GroupID = job.TargetGroupID
	right.State = distribution.RouteStateActive
	right.StagedVisibilityActive = true
	right.MigrationJobID = job.JobID
	right.MinWriteTSExclusive = job.FenceTS
	encodedRoute, err := distribution.EncodeRouteDescriptorForCatalogWrite(right, s.catalog.AllowsRouteDescriptorV2Writes())
	if err != nil {
		return errors.WithStack(err)
	}
	nextJob := distribution.CloneSplitJob(job)
	nextJob.Phase = distribution.SplitJobPhaseCleanup
	nextJob.CutoverVersion = nextVersion
	nextJob.CutoverReadFenceState = distribution.SplitJobBarrierArmed
	nextJob.TargetStagedReadinessState = distribution.SplitJobBarrierArmed
	nextJob.SourceCutoverReadFenceAckCursor = []byte("raft-applied")
	nextJob.TargetStagedReadinessAckCursor = []byte("raft-applied")
	nextJob.UpdatedAtMs = time.Now().UnixMilli()
	encodedJob, err := distribution.EncodeSplitJob(nextJob)
	if err != nil {
		return errors.WithStack(err)
	}
	versionKey := distribution.CatalogVersionKey()
	routeKey := distribution.CatalogRouteKey(right.RouteID)
	jobKey := distribution.CatalogSplitJobKey(job.JobID)
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: routeKey, Value: encodedRoute},
			{Op: kv.Put, Key: versionKey, Value: distribution.EncodeCatalogVersion(nextVersion)},
			{Op: kv.Put, Key: jobKey, Value: encodedJob},
		},
		IsTxn:    true,
		StartTS:  snapshot.ReadTS,
		ReadKeys: [][]byte{routeKey, versionKey, jobKey},
	}); err != nil {
		if errors.Is(err, store.ErrWriteConflict) {
			return grpcStatusError(codes.Aborted, errDistributionCatalogConflict.Error())
		}
		return errors.WithStack(err)
	}
	updated, err := s.loadCatalogSnapshotAtLeastVersion(ctx, nextVersion)
	if err != nil {
		return err
	}
	return s.applyEngineSnapshot(updated)
}
