package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *DistributionServer) startOnlineSplit(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, witness *splitTransferAdmissionWitness) (distribution.SplitJob, error) {
	state, err := probeSplitSourceOwnershipState(ctx, source, job.JobID, witness.snapshot.ConfigurationIndex)
	if err != nil {
		return job, err
	}
	if state.GetOnlineCaptureVersion() != store.OnlineMigrationVersion || state.GetOnlineSourceGeneration() == "" {
		return job, nil
	}
	sourceProbe := &pb.ProbeMigrationStateRequest{JobId: job.JobID, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED, RouteStart: witness.control.GetRouteStart(), RouteEnd: witness.control.GetRouteEnd(), ExpectedCatalogVersion: witness.control.GetExpectedCutoverVersion(), MigrationJobId: job.JobID, MinWriteTsExclusive: witness.control.GetMinWriteTsExclusive(), TrackWrites: true, RetentionPinTs: initialMigrationRetentionPinTS, RequireConfigurationIndex: true, ExpectedConfigurationIndex: witness.snapshot.ConfigurationIndex, OnlineCaptureVersion: store.OnlineMigrationVersion}
	if _, complete, err := s.syncSplitMigrationVoterBarrier(ctx, witness.groupID, nil, source, sourceProbe); err != nil || !complete {
		if err == nil {
			err = errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
		return job, err
	}
	page, manifest, err := exportOnlineSplitPage(ctx, source, job.JobID, job.SplitKey, witness.control.GetRouteEnd(), 0)
	if err != nil {
		return job, err
	}
	if err := s.prepareOnlineSplitTarget(ctx, job, witness.groupID, page, manifest); err != nil {
		return job, err
	}
	job.OnlineCaptureVersion = store.OnlineMigrationVersion
	job.OnlineManifest = bytes.Clone(page.GetOnlineManifest())
	return job, nil
}

func exportOnlineSplitPage(ctx context.Context, source SplitMigrationClient, job uint64, start, end []byte, seq uint64) (*pb.ExportRangeVersionsResponse, store.OnlineMigrationSession, error) {
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	stream, err := source.ExportRangeVersions(attempt, &pb.ExportRangeVersionsRequest{OnlineJobId: job, OnlineAfterSequence: seq, RouteStart: start, RouteEnd: end, MaxPages: 1})
	if err != nil {
		return nil, store.OnlineMigrationSession{}, errors.WithStack(err)
	}
	response, err := stream.Recv()
	if err != nil {
		return nil, store.OnlineMigrationSession{}, errors.WithStack(err)
	}
	manifest, err := store.DecodeOnlineMigrationManifest(response.GetOnlineManifest())
	if err != nil {
		return nil, manifest, errors.WithStack(err)
	}
	if manifest.Invalid != "" || manifest.Closed || manifest.Identity.JobID != job || !bytes.Equal(manifest.RouteStart, start) || !bytes.Equal(manifest.RouteEnd, end) || manifest.Sequence < seq {
		return nil, manifest, errors.WithStack(store.ErrOnlineMigrationInvalid)
	}
	return response, manifest, nil
}

func sameOnlineSplitManifest(saved []byte, current store.OnlineMigrationSession) bool {
	prior, err := store.DecodeOnlineMigrationManifest(saved)
	return err == nil && prior.Identity == current.Identity && prior.StartIndex == current.StartIndex && prior.ConfigurationIndex == current.ConfigurationIndex && bytes.Equal(prior.RouteStart, current.RouteStart) && bytes.Equal(prior.RouteEnd, current.RouteEnd)
}

const onlineSplitFramesPerTick = 128
const onlineSplitContinuationDelay = 10 * time.Millisecond

// A bounded pass checkpoints only positions already durable at the target.
// Replaying a pass whose catalog response was lost verifies each exact receipt.
func (s *DistributionServer) copyOnlineSplitPage(ctx context.Context, job distribution.SplitJob) (bool, error) {
	pass, err := s.openOnlineSplitPass(ctx, job)
	if err != nil {
		return false, err
	}
	deadline := time.Now().Add(preflightTimePerTick)
	bytesLeft := preflightBytesPerTick
	complete := false
	for range onlineSplitFramesPerTick {
		var size int
		size, complete, err = pass.advance(ctx, s)
		if err != nil {
			return false, err
		}
		bytesLeft -= size
		if complete || bytesLeft <= 0 || !time.Now().Before(deadline) {
			break
		}
	}
	if pass.next.OnlineSequence == job.OnlineSequence {
		return complete, nil
	}
	if err := s.persistOnlineSplitProgress(ctx, job, pass.next); err != nil {
		return false, err
	}
	return complete, nil
}

type onlineSplitPass struct {
	goal               uint64
	sealedTailAttested bool
	source             SplitMigrationClient
	target             SplitMigrationClient
	end                []byte
	next               distribution.SplitJob
}

func (pass *onlineSplitPass) advance(ctx context.Context, server *DistributionServer) (int, bool, error) {
	job := pass.next
	if pass.catchUpGoalReached() {
		return 0, true, nil
	}
	page, manifest, err := exportOnlineSplitPage(ctx, pass.source, job.JobID, job.SplitKey, pass.end, job.OnlineSequence)
	if err != nil {
		return 0, false, err
	}
	if err := pass.verifySourceManifest(manifest); err != nil {
		return 0, false, err
	}
	if page.GetDone() {
		return 0, true, validateOnlineSplitEnd(job, page, manifest)
	}
	frame, err := store.DecodeOnlineMigrationFrame(page.GetOnlineFrame(), manifest)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	if frame.Sequence != job.OnlineSequence+1 {
		return 0, false, errors.WithStack(store.ErrImportBatchGap)
	}
	if err := pass.attestSealedTail(ctx, server, manifest, page.GetOnlineManifest()); err != nil {
		return 0, false, err
	}
	if err := importOnlineSplitFrame(ctx, pass.target, job, page, frame.Sequence); err != nil {
		return 0, false, err
	}
	pass.next.OnlineSequence = frame.Sequence
	for _, effect := range frame.Entry.Effects {
		pass.next.MaxImportedTS = max(pass.next.MaxImportedTS, effect.CommitTS)
	}
	// The preflight completion checkpoint fixes this goal across passes.
	// Later writers belong to the tail captured by the subsequent seal.
	caughtUp := pass.catchUpGoalReached()
	return len(page.GetOnlineFrame()), caughtUp, nil
}

// Before importing a sealed tail, bind the existing target to its current
// membership. A controller can resume here after losing the seal response.
func (pass *onlineSplitPass) attestSealedTail(ctx context.Context, server *DistributionServer, seal store.OnlineMigrationSession, encoded []byte) error {
	if !seal.Sealed || pass.sealedTailAttested {
		return nil
	}
	job := pass.next
	members, err := server.splitTransferApplyMembers(ctx, job.TargetGroupID)
	if err != nil {
		return err
	}
	if err := verifyOnlineMemberCapabilities(ctx, job.JobID, members); err != nil {
		return err
	}
	control := splitMigrationControlRequest(job, seal.RouteEnd, 0, initialMigrationRetentionPinTS, false, false, false, 0)
	control.OnlineCaptureVersion = store.OnlineMigrationVersion
	control.OnlineTargetManifest = encoded
	control.RequireConfigurationIndex, control.ExpectedConfigurationIndex = true, members.ConfigurationIndex
	if _, err := pass.target.ApplyTargetStagedReadiness(ctx, control); err != nil {
		return errors.WithStack(err)
	}
	pass.sealedTailAttested = true
	return nil
}

func validateOnlineSplitEnd(job distribution.SplitJob, page *pb.ExportRangeVersionsResponse, manifest store.OnlineMigrationSession) error {
	if len(page.GetOnlineFrame()) != 0 || manifest.Sequence != job.OnlineSequence {
		return errors.WithStack(store.ErrMigrationCaptureCorrupt)
	}
	if job.Phase == distribution.SplitJobPhaseDeltaCopy && (!manifest.Sealed || manifest.Sequence != job.OnlineSealSequence) {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	return nil
}

func importOnlineSplitFrame(ctx context.Context, target SplitMigrationClient, job distribution.SplitJob, page *pb.ExportRangeVersionsResponse, sequence uint64) error {
	cursor := binary.BigEndian.AppendUint64(nil, sequence)
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	response, err := target.ImportRangeVersions(attempt, &pb.ImportRangeVersionsRequest{JobId: job.JobID, BracketId: store.OnlineMigrationBracketID, BatchSeq: sequence, Cursor: cursor, OnlineManifest: page.GetOnlineManifest(), OnlineFrame: page.GetOnlineFrame()})
	if err != nil {
		return errors.WithStack(err)
	}
	if !bytes.Equal(response.GetAckedCursor(), cursor) {
		return errors.WithStack(store.ErrMigrationCaptureConflict)
	}
	return nil
}

func (s *DistributionServer) persistOnlineSplitProgress(ctx context.Context, expected, next distribution.SplitJob) error {
	return s.updateSplitJobViaCoordinator(ctx, expected.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if !distribution.SplitJobsEquivalent(current, expected) {
			return current, errors.WithStack(distribution.ErrCatalogSplitJobConflict)
		}
		current.OnlineSequence = next.OnlineSequence
		current.MaxImportedTS = max(current.MaxImportedTS, next.MaxImportedTS)
		current.UpdatedAtMs = time.Now().UnixMilli()
		return current, nil
	})
}

func (s *DistributionServer) sealOnlineSplit(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, witness *splitTransferAdmissionWitness) (store.OnlineMigrationSession, error) {
	prior, err := store.DecodeOnlineMigrationManifest(job.OnlineManifest)
	if err != nil {
		return prior, errors.WithStack(err)
	}
	control := splitMigrationControlRequest(job, prior.RouteEnd, witness.control.GetExpectedCutoverVersion(), job.SnapshotTS, true, false, true, initialMigrationRetentionPinTS)
	control.OnlineCaptureVersion = store.OnlineMigrationVersion
	control.OnlineLineage = bytes.Clone(prior.Identity.Lineage[:])
	control.OnlineSeal = true
	control.RequireConfigurationIndex, control.ExpectedConfigurationIndex = true, witness.snapshot.ConfigurationIndex
	if _, err := source.ApplyTargetStagedReadiness(ctx, control); err != nil {
		return prior, errors.WithStack(err)
	}
	_, sealed, err := exportOnlineSplitPage(ctx, source, job.JobID, job.SplitKey, prior.RouteEnd, job.OnlineSequence)
	if err != nil {
		return sealed, err
	}
	if !sealed.Sealed || !sameOnlineSplitManifest(job.OnlineManifest, sealed) {
		return sealed, errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	return sealed, nil
}

func onlineSplitPermanentError(err error) bool {
	if errors.Is(err, store.ErrOnlineMigrationInvalid) || errors.Is(err, store.ErrOnlineMigrationIdentity) || errors.Is(err, store.ErrOnlineMigrationTargetNotEmpty) || errors.Is(err, store.ErrMigrationCaptureConflict) {
		return true
	}
	// FromError on a wrapped gRPC status includes the wrapper text in
	// Message. Recover the original status before matching the wire verdict.
	var wire interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &wire) || wire.GRPCStatus().Code() != codes.FailedPrecondition {
		return false
	}
	switch wire.GRPCStatus().Message() {
	case store.ErrOnlineMigrationInvalid.Error(), store.ErrOnlineMigrationIdentity.Error(), store.ErrOnlineMigrationTargetNotEmpty.Error(), store.ErrOnlineMigrationTargetClosed.Error(), store.ErrMigrationCaptureConflict.Error():
		return true
	default:
		return false
	}
}

func (s *DistributionServer) readyOnlineSplitTarget(ctx context.Context, job distribution.SplitJob, target SplitMigrationClient, end []byte, version uint64) error {
	if job.OnlineSequence != job.OnlineSealSequence {
		return errors.WithStack(store.ErrImportBatchGap)
	}
	source, err := s.splitJobSourceClient(ctx, job, job.SourceGroupID)
	if err != nil {
		return err
	}
	_, sealed, err := exportOnlineSplitPage(ctx, source, job.JobID, job.SplitKey, end, job.OnlineSequence)
	if err != nil {
		return err
	}
	if !sealed.Sealed || sealed.Sequence != job.OnlineSealSequence || !sameOnlineSplitManifest(job.OnlineManifest, sealed) {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	members, err := s.splitTransferApplyMembers(ctx, job.TargetGroupID)
	if err != nil {
		return err
	}
	if err := verifyOnlineMemberCapabilities(ctx, job.JobID, members); err != nil {
		return err
	}
	control := splitMigrationControlRequest(job, end, version, job.FenceTS, false, false, false, 0)
	control.OnlineCaptureVersion = store.OnlineMigrationVersion
	control.OnlineTargetManifest = bytes.Clone(job.OnlineManifest)
	control.OnlineReady = true
	control.OnlineSequence = job.OnlineSealSequence
	control.RequireConfigurationIndex, control.ExpectedConfigurationIndex = true, members.ConfigurationIndex
	if _, err := target.ApplyTargetStagedReadiness(ctx, control); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *DistributionServer) onlineSplitContinuation(ctx context.Context, previous distribution.SplitJob) (bool, error) {
	current, found, err := s.catalog.SplitJob(ctx, previous.JobID)
	if err != nil {
		return false, splitJobCatalogStatusError(err)
	}
	if !found || current.OnlineCaptureVersion == 0 || current.Phase == distribution.SplitJobPhaseDone || current.Phase == distribution.SplitJobPhaseAbandoned || current.Phase == distribution.SplitJobPhaseFailed {
		return false, nil
	}
	return !distribution.SplitJobsEquivalent(previous, current), nil
}

func verifyOnlineMemberCapabilities(ctx context.Context, jobID uint64, members SplitMigrationApplyMemberSnapshot) error {
	supported, err := onlineMemberCapabilities(ctx, jobID, members)
	if err != nil {
		return err
	}
	if !supported {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	return nil
}
func onlineMemberCapabilities(ctx context.Context, jobID uint64, members SplitMigrationApplyMemberSnapshot) (bool, error) {
	supported := true
	for _, member := range members.Members {
		resp, err := probeSplitOwnershipCapability(ctx, member, jobID)
		if err != nil {
			return false, err
		}
		if resp.GetAppliedConfigurationIndex() != members.ConfigurationIndex {
			return false, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
		supported = supported && resp.GetOnlineCaptureVersion() == store.OnlineMigrationVersion
	}
	return supported, nil
}

func (s *DistributionServer) confirmOnlineCutover(ctx context.Context, groupID uint64, probe *pb.ProbeMigrationStateRequest) error {
	members, err := s.splitTransferApplyMembers(ctx, groupID)
	if err != nil {
		return err
	}
	probe.RequireConfigurationIndex = true
	probe.ExpectedConfigurationIndex = members.ConfigurationIndex
	for _, member := range members.Members {
		if ack := probeSplitMigrationVoter(ctx, member, probe); !ack.acked {
			return splitMigrationVoterBarrierIncompleteError(probe.GetJobId(), probe.GetKind())
		}
	}
	_, err = s.revalidateSplitTransferMembers(ctx, &splitTransferAdmissionWitness{groupID: groupID, snapshot: members})
	return err
}

func (s *DistributionServer) onlineSplitMembersSupported(ctx context.Context, job distribution.SplitJob) (bool, error) {
	supported := true
	for _, group := range []uint64{job.SourceGroupID, job.TargetGroupID} {
		members, err := s.splitTransferApplyMembers(ctx, group)
		if err != nil {
			return false, err
		}
		groupSupported, err := onlineMemberCapabilities(ctx, job.JobID, members)
		if err != nil {
			return false, err
		}
		supported = supported && groupSupported
		if _, err := s.revalidateSplitTransferMembers(ctx, &splitTransferAdmissionWitness{groupID: group, snapshot: members}); err != nil {
			return false, err
		}
	}
	return supported, nil
}

func (s *DistributionServer) admitOnlineSplitFence(ctx context.Context, job distribution.SplitJob) (distribution.SplitJob, bool, error) {
	current, found, err := s.catalog.SplitJob(ctx, job.JobID)
	if err != nil || !found {
		return current, false, splitJobCatalogStatusError(err)
	}
	if current.Phase != distribution.SplitJobPhaseFence {
		return current, false, nil
	}
	leader, err := s.verifySplitJobRunnerLeader(ctx)
	if err != nil || !leader {
		return current, false, err
	}
	ready, err := s.splitJobCapabilityReady(ctx, current)
	if err != nil || !ready {
		return current, false, err
	}
	if err := s.ensureSplitJobNamespaceAdmission(ctx, current); err != nil {
		return current, false, err
	}
	return current, true, nil
}

func (s *DistributionServer) splitJobContinuationDelay(ctx context.Context) time.Duration {
	jobs, err := s.catalog.ListSplitJobs(ctx)
	if err != nil {
		return splitPreflightContinuationDelay
	}
	job, ok := nextRunnableSplitJob(jobs)
	if ok && job.OnlineCaptureVersion != 0 {
		return onlineSplitContinuationDelay
	}
	return splitPreflightContinuationDelay
}

func (s *DistributionServer) prepareOnlineSplitTarget(ctx context.Context, job distribution.SplitJob, sourceGroup uint64, page *pb.ExportRangeVersionsResponse, manifest store.OnlineMigrationSession) error {
	targetMembers, err := s.splitTransferApplyMembers(ctx, job.TargetGroupID)
	if err != nil {
		return err
	}
	if err := verifyOnlineMemberCapabilities(ctx, job.JobID, targetMembers); err != nil {
		return err
	}
	_, target, err := s.splitCopyClients(ctx, job, sourceGroup)
	if err != nil {
		return err
	}
	control := splitMigrationControlRequest(job, manifest.RouteEnd, 0, initialMigrationRetentionPinTS, false, false, false, 0)
	control.OnlineCaptureVersion = store.OnlineMigrationVersion
	control.OnlineTargetManifest = page.GetOnlineManifest()
	control.RequireConfigurationIndex, control.ExpectedConfigurationIndex = true, targetMembers.ConfigurationIndex
	if _, err := target.ApplyTargetStagedReadiness(ctx, control); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *DistributionServer) openOnlineSplitPass(ctx context.Context, job distribution.SplitJob) (onlineSplitPass, error) {
	source, target, end, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return onlineSplitPass{}, err
	}
	saved, err := store.DecodeOnlineMigrationManifest(job.OnlineManifest)
	if err != nil {
		return onlineSplitPass{}, errors.WithStack(err)
	}
	return onlineSplitPass{source: source, target: target, end: end, next: distribution.CloneSplitJob(job), goal: saved.Sequence}, nil
}

// This observation is persisted atomically with preflight completion. Retrying
// a lost completion response reloads that checkpoint rather than moving the goal.
func recordOnlineSplitCatchUpGoal(ctx context.Context, next *distribution.SplitJob, source SplitMigrationClient, end []byte) error {
	if next.OnlineCaptureVersion == 0 || next.Phase != distribution.SplitJobPhaseFence || !next.TransferPreflightDone {
		return nil
	}
	page, manifest, err := exportOnlineSplitPage(ctx, source, next.JobID, next.SplitKey, end, next.OnlineSequence)
	if err != nil {
		return err
	}
	if !sameOnlineSplitManifest(next.OnlineManifest, manifest) || manifest.Sealed {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	next.OnlineManifest = bytes.Clone(page.GetOnlineManifest())
	return nil
}

func (pass *onlineSplitPass) catchUpGoalReached() bool {
	return pass.next.Phase == distribution.SplitJobPhaseFence && pass.next.OnlineSequence >= pass.goal
}

func (pass *onlineSplitPass) verifySourceManifest(manifest store.OnlineMigrationSession) error {
	if !sameOnlineSplitManifest(pass.next.OnlineManifest, manifest) {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if pass.next.Phase == distribution.SplitJobPhaseFence && manifest.Sequence < pass.goal {
		return errors.WithStack(store.ErrMigrationCaptureCorrupt)
	}
	return nil
}
