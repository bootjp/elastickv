package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"sort"
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
	defaultSplitMigrationReadFenceGrace  = 30 * time.Second
	splitCleanupCursorVersion            = byte(1)
	splitVoterAckCursorVersion           = byte(1)
	splitCleanupCursorHeaderBytes        = 5
	splitJobHistoryLimit                 = 1000
	splitJobHistoryDeleteBatchLimit      = 1000
	splitJobHistoryTTL                   = 7 * 24 * time.Hour
	splitJobHistoryGCInterval            = time.Minute
)

var splitCleanupDoneCursor = []byte{splitCleanupCursorVersion, 0xff, 0xff, 0xff, 0xff}

type splitVoterAck struct {
	id      string
	address string
	acked   bool
}

func (s *DistributionServer) syncSplitMigrationVoterBarrier(
	ctx context.Context,
	groupID uint64,
	existing []byte,
	fallback SplitMigrationClient,
	req *pb.ProbeMigrationStateRequest,
) ([]byte, bool, error) {
	voters, err := s.splitMigrationVoters(ctx, groupID, fallback)
	if err != nil {
		return nil, false, err
	}
	prior, err := decodeSplitVoterAckCursor(existing)
	if err != nil {
		return nil, false, err
	}
	priorAcked := make(map[string]bool, len(prior))
	for _, voter := range prior {
		priorAcked[splitVoterAckKey(voter.id, voter.address)] = voter.acked
	}
	acks := make([]splitVoterAck, 0, len(voters))
	complete := true
	for _, voter := range voters {
		key := splitVoterAckKey(voter.ID, voter.Address)
		acked := priorAcked[key]
		resp, probeErr := voter.Client.ProbeMigrationState(ctx, req)
		if probeErr == nil {
			acked = resp.GetReady()
		}
		complete = complete && acked
		acks = append(acks, splitVoterAck{id: voter.ID, address: voter.Address, acked: acked})
	}
	return encodeSplitVoterAckCursor(acks), complete, nil
}

func (s *DistributionServer) splitMigrationVoters(ctx context.Context, groupID uint64, fallback SplitMigrationClient) ([]SplitMigrationVoter, error) {
	if s.splitMigrationVoterFactory == nil {
		if fallback == nil {
			return nil, errors.New("split migration voter factory is not configured")
		}
		return []SplitMigrationVoter{{ID: "leader", Address: "leader", Client: fallback}}, nil
	}
	voters, err := s.splitMigrationVoterFactory(ctx, groupID)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(voters) == 0 {
		return nil, errors.New("split migration voter set is empty")
	}
	if err := validateSplitMigrationVoters(voters); err != nil {
		return nil, err
	}
	return voters, nil
}

func validateSplitMigrationVoters(voters []SplitMigrationVoter) error {
	sort.Slice(voters, func(i, j int) bool {
		if voters[i].ID == voters[j].ID {
			return voters[i].Address < voters[j].Address
		}
		return voters[i].ID < voters[j].ID
	})
	for index, voter := range voters {
		if voter.ID == "" || voter.Address == "" || voter.Client == nil {
			return errors.New("split migration voter is incomplete")
		}
		if index > 0 && voter.ID == voters[index-1].ID {
			return errors.New("split migration voter id is duplicated")
		}
	}
	return nil
}

func splitVoterAckKey(id, address string) string {
	return id + "\x00" + address
}

func encodeSplitVoterAckCursor(acks []splitVoterAck) []byte {
	out := []byte{splitVoterAckCursorVersion}
	out = binary.AppendUvarint(out, uint64(len(acks)))
	for _, ack := range acks {
		out = binary.AppendUvarint(out, uint64(len(ack.id)))
		out = append(out, ack.id...)
		out = binary.AppendUvarint(out, uint64(len(ack.address)))
		out = append(out, ack.address...)
		if ack.acked {
			out = append(out, 1)
		} else {
			out = append(out, 0)
		}
	}
	return out
}

func decodeSplitVoterAckCursor(raw []byte) ([]splitVoterAck, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if raw[0] != splitVoterAckCursorVersion {
		return nil, errors.New("invalid split voter ack cursor version")
	}
	raw = raw[1:]
	count, rest, err := consumeSplitCursorUvarint(raw)
	if err != nil {
		return nil, err
	}
	raw = rest
	acks := make([]splitVoterAck, 0, count)
	for range count {
		id, next, err := consumeSplitCursorString(raw)
		if err != nil {
			return nil, err
		}
		address, next, err := consumeSplitCursorString(next)
		if err != nil || len(next) == 0 || next[0] > 1 {
			return nil, errors.New("invalid split voter ack cursor entry")
		}
		acks = append(acks, splitVoterAck{id: id, address: address, acked: next[0] == 1})
		raw = next[1:]
	}
	if len(raw) != 0 {
		return nil, errors.New("split voter ack cursor has trailing bytes")
	}
	return acks, nil
}

func consumeSplitCursorString(raw []byte) (string, []byte, error) {
	size, rest, err := consumeSplitCursorUvarint(raw)
	if err != nil || size > uint64(len(rest)) {
		return "", nil, errors.New("invalid split voter ack cursor string")
	}
	return string(rest[:size]), rest[size:], nil
}

func consumeSplitCursorUvarint(raw []byte) (uint64, []byte, error) {
	value, size := binary.Uvarint(raw)
	if size <= 0 {
		return 0, nil, errors.New("invalid split voter ack cursor integer")
	}
	return value, raw[size:], nil
}

func (s *DistributionServer) runSplitJobPhase(ctx context.Context, job distribution.SplitJob) error {
	if s.splitMigrationClientFactory == nil {
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
		return s.abandonSplitJob(ctx, job)
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
	if !job.TargetPromotionDone {
		return s.promoteSplitJobTargetAndComplete(ctx, job)
	}
	if !bytes.Equal(job.TargetClearedDescriptorAckCursor, splitCleanupDoneCursor) {
		if err := s.cleanupSplitJobTargetProofs(ctx, job); err != nil {
			return err
		}
		return nil
	}
	if !bytes.Equal(job.SourceCutoverAckCursor, splitCleanupDoneCursor) {
		if err := s.ackSplitJobSourceRouteRemoval(ctx, job); err != nil {
			return err
		}
		return nil
	}
	if !bytes.Equal(job.SourceReadDrainCursor, splitCleanupDoneCursor) {
		if err := s.ackSplitJobSourceReadDrain(ctx, job); err != nil {
			return err
		}
		return nil
	}
	if !bytes.Equal(job.Cursor, splitCleanupDoneCursor) {
		return s.cleanupSplitJobSourceData(ctx, job)
	}
	if job.SourceRetentionPinTS != math.MaxUint64 {
		return s.cleanupSplitJobSourceProofs(ctx, job)
	}
	return s.finishSplitJobHistory(ctx, job, distribution.SplitJobPhaseDone)
}

func (s *DistributionServer) splitJobMigrationClients(ctx context.Context, job distribution.SplitJob) (SplitMigrationClient, SplitMigrationClient, []byte, error) {
	if s.splitMigrationClientFactory == nil {
		return nil, nil, nil, errors.New("split migration client factory is not configured")
	}
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	sourceGroupID, routeEnd, ok := splitJobSourceSibling(snapshot.Routes, job)
	if !ok {
		if parent, found := findRouteByID(snapshot.Routes, job.SourceRouteID); found {
			sourceGroupID = parent.GroupID
			routeEnd = distribution.CloneBytes(parent.End)
			ok = true
		}
	}
	if !ok {
		return nil, nil, nil, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	source, target, err := s.splitMigrationClientFactory(ctx, job, sourceGroupID)
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	if source == nil || target == nil {
		return nil, nil, nil, errors.New("split migration source or target client is nil")
	}
	return source, target, routeEnd, nil
}

func (s *DistributionServer) cleanupSplitJobTargetProofs(ctx context.Context, job distribution.SplitJob) error {
	_, target, routeEnd, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return err
	}
	descriptorCursor, descriptorComplete, err := s.syncSplitMigrationVoterBarrier(ctx, job.TargetGroupID, job.TargetClearedDescriptorAckCursor, target, &pb.ProbeMigrationStateRequest{
		JobId:                  job.JobID,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_TARGET_DESCRIPTOR_CLEARED,
		RouteStart:             job.SplitKey,
		RouteEnd:               routeEnd,
		ExpectedCatalogVersion: job.CutoverVersion,
		ExpectedGroupId:        job.TargetGroupID,
		MinWriteTsExclusive:    job.FenceTS,
	})
	if err != nil {
		return err
	}
	if !descriptorComplete {
		return s.persistSplitJobTargetCleanupCursor(ctx, job.JobID, descriptorCursor)
	}
	if _, err := target.CleanupMigration(ctx, &pb.CleanupMigrationRequest{
		JobId: job.JobID,
		Mode:  pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA,
	}); err != nil {
		return errors.WithStack(err)
	}
	_, metadataComplete, err := s.syncSplitMigrationVoterBarrier(ctx, job.TargetGroupID, nil, target, &pb.ProbeMigrationStateRequest{
		JobId: job.JobID,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	if err != nil || !metadataComplete {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup && current.TargetPromotionDone {
			current.TargetClearedDescriptorAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) persistSplitJobTargetCleanupCursor(ctx context.Context, jobID uint64, cursor []byte) error {
	return s.updateSplitJobViaCoordinator(ctx, jobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup {
			current.TargetClearedDescriptorAckCursor = distribution.CloneBytes(cursor)
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) ackSplitJobSourceRouteRemoval(ctx context.Context, job distribution.SplitJob) error {
	source, _, routeEnd, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return err
	}
	sourceGroupID, err := s.splitJobSourceGroupID(ctx, job)
	if err != nil {
		return err
	}
	cursor, complete, err := s.syncSplitMigrationVoterBarrier(ctx, sourceGroupID, job.SourceCutoverAckCursor, source, &pb.ProbeMigrationStateRequest{
		JobId:                  job.JobID,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_ROUTE_REMOVED,
		RouteStart:             job.SplitKey,
		RouteEnd:               routeEnd,
		ExpectedCatalogVersion: job.CutoverVersion,
		ExpectedGroupId:        job.TargetGroupID,
	})
	if err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup {
			current.SourceCutoverAckCursor = distribution.CloneBytes(cursor)
			if complete {
				current.SourceCutoverAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			}
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) ackSplitJobSourceReadDrain(ctx context.Context, job distribution.SplitJob) error {
	source, _, _, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return err
	}
	sourceGroupID, err := s.splitJobSourceGroupID(ctx, job)
	if err != nil {
		return err
	}
	notBefore := int64(job.PromotionCompletedTS>>kv.HLCLogicalBits) + defaultSplitMigrationReadFenceGrace.Milliseconds() //nolint:gosec // HLC physical millis fit int64.
	cursor, complete, err := s.syncSplitMigrationVoterBarrier(ctx, sourceGroupID, job.SourceReadDrainCursor, source, &pb.ProbeMigrationStateRequest{
		JobId:                job.JobID,
		Kind:                 pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED,
		ReadDrainNotBeforeMs: notBefore,
	})
	if err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup {
			current.SourceReadDrainCursor = distribution.CloneBytes(cursor)
			if complete {
				current.SourceReadDrainCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			}
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) splitJobSourceGroupID(ctx context.Context, job distribution.SplitJob) (uint64, error) {
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return 0, err
	}
	groupID, _, ok := splitJobSourceSibling(snapshot.Routes, job)
	if !ok {
		if parent, found := findRouteByID(snapshot.Routes, job.SourceRouteID); found {
			return parent.GroupID, nil
		}
		return 0, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	return groupID, nil
}

func (s *DistributionServer) cleanupSplitJobSourceData(ctx context.Context, job distribution.SplitJob) error {
	source, _, routeEnd, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return err
	}
	brackets, err := distribution.PlanExportBrackets(job.SplitKey, routeEnd)
	if err != nil {
		return errors.WithStack(err)
	}
	index, cursor, done, err := decodeSplitCleanupCursor(job.Cursor, len(brackets))
	if err != nil || done {
		return err
	}
	bracket := brackets[index]
	resp, err := source.CleanupMigration(ctx, splitMigrationCleanupRequest(job, routeEnd, bracket, cursor, job.FenceTS))
	if err != nil {
		return errors.WithStack(err)
	}
	encoded, err := nextSplitCleanupCursor(resp, cursor, index, len(brackets))
	if err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup && bytes.Equal(current.Cursor, job.Cursor) {
			current.Cursor = encoded
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func nextSplitCleanupCursor(resp *pb.CleanupMigrationResponse, cursor []byte, index, bracketCount int) ([]byte, error) {
	if !resp.GetDone() {
		next := resp.GetNextCursor()
		if len(next) == 0 || bytes.Equal(next, cursor) {
			return nil, errors.New("split migration source cleanup made no cursor progress")
		}
		return encodeSplitCleanupCursor(index, next, false), nil
	}
	nextIndex := index + 1
	return encodeSplitCleanupCursor(nextIndex, nil, nextIndex >= bracketCount), nil
}

func splitMigrationCleanupRequest(job distribution.SplitJob, routeEnd []byte, bracket distribution.MigrationBracket, cursor []byte, maxCommitTS uint64) *pb.CleanupMigrationRequest {
	return &pb.CleanupMigrationRequest{
		JobId:                 job.JobID,
		Mode:                  pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart:            bracket.Start,
		RangeEnd:              bracket.End,
		Cursor:                cursor,
		MaxCommitTs:           maxCommitTS,
		MaxVersions:           defaultSplitPromotionMaxVersions,
		MaxBytes:              defaultSplitPromotionMaxBytes,
		MaxScannedBytes:       defaultSplitPromotionMaxScanned,
		KeyFamily:             bracket.Family,
		RouteStart:            job.SplitKey,
		RouteEnd:              routeEnd,
		ExcludeKnownInternal:  bracket.ExcludeKnownInternal,
		ExcludePrefixes:       bracket.ExcludePrefixes,
		RequiresRouteKeyCheck: bracket.RequiresRouteKeyCheck,
		RequiresDecodedS3:     bracket.RequiresDecodedS3,
	}
}

func (s *DistributionServer) cleanupSplitJobSourceProofs(ctx context.Context, job distribution.SplitJob) error {
	if err := s.cleanupSplitJobSourceMetadata(ctx, job); err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseCleanup {
			current.SourceCutoverAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			current.SourceRetentionPinTS = math.MaxUint64
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func encodeSplitCleanupCursor(index int, cursor []byte, done bool) []byte {
	if done {
		return distribution.CloneBytes(splitCleanupDoneCursor)
	}
	out := make([]byte, splitCleanupCursorHeaderBytes+len(cursor))
	out[0] = splitCleanupCursorVersion
	binary.BigEndian.PutUint32(out[1:], uint32(index)) //nolint:gosec // bracket count is bounded.
	copy(out[splitCleanupCursorHeaderBytes:], cursor)
	return out
}

func decodeSplitCleanupCursor(raw []byte, bracketCount int) (int, []byte, bool, error) {
	if len(raw) == 0 {
		return 0, nil, bracketCount == 0, nil
	}
	if bytes.Equal(raw, splitCleanupDoneCursor) {
		return bracketCount, nil, true, nil
	}
	if len(raw) < splitCleanupCursorHeaderBytes || raw[0] != splitCleanupCursorVersion {
		return 0, nil, false, errors.New("invalid split cleanup cursor")
	}
	index := int(binary.BigEndian.Uint32(raw[1:splitCleanupCursorHeaderBytes]))
	if index < 0 || index >= bracketCount {
		return 0, nil, false, errors.New("split cleanup cursor bracket is out of range")
	}
	return index, distribution.CloneBytes(raw[splitCleanupCursorHeaderBytes:]), false, nil
}

func (s *DistributionServer) abandonSplitJob(ctx context.Context, job distribution.SplitJob) error {
	if err := s.rollbackAbandonedSplitJobFence(ctx, job); err != nil {
		return err
	}
	if !bytes.Equal(job.TargetClearedDescriptorAckCursor, splitCleanupDoneCursor) {
		return s.cleanupAbandonedSplitJobTarget(ctx, job)
	}
	if !bytes.Equal(job.SourceCutoverAckCursor, splitCleanupDoneCursor) {
		return s.cleanupAbandonedSplitJobSource(ctx, job)
	}
	return s.finishSplitJobHistory(ctx, job, distribution.SplitJobPhaseAbandoned)
}

func (s *DistributionServer) cleanupAbandonedSplitJobTarget(ctx context.Context, job distribution.SplitJob) error {
	_, target, _, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return err
	}
	_, cursor, done, err := decodeSplitCleanupCursor(job.TargetClearedDescriptorAckCursor, 1)
	if err != nil {
		return err
	}
	if !done {
		complete, err := s.cleanupAbandonedSplitJobTargetVersions(ctx, target, job, cursor)
		if err != nil || !complete {
			return err
		}
	}
	if _, err := target.CleanupMigration(ctx, &pb.CleanupMigrationRequest{
		JobId: job.JobID,
		Mode:  pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA,
	}); err != nil {
		return errors.WithStack(err)
	}
	_, metadataComplete, err := s.syncSplitMigrationVoterBarrier(ctx, job.TargetGroupID, nil, target, &pb.ProbeMigrationStateRequest{
		JobId: job.JobID,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	if err != nil || !metadataComplete {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseAbandoning {
			current.TargetClearedDescriptorAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) cleanupAbandonedSplitJobTargetVersions(
	ctx context.Context,
	target SplitMigrationClient,
	job distribution.SplitJob,
	cursor []byte,
) (bool, error) {
	prefix := distribution.MigrationStagedDataKeyPrefix(job.JobID)
	resp, err := target.CleanupMigration(ctx, &pb.CleanupMigrationRequest{
		JobId:           job.JobID,
		Mode:            pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart:      prefix,
		RangeEnd:        store.PrefixScanEnd(prefix),
		Cursor:          cursor,
		MaxCommitTs:     math.MaxUint64,
		MaxVersions:     defaultSplitPromotionMaxVersions,
		MaxBytes:        defaultSplitPromotionMaxBytes,
		MaxScannedBytes: defaultSplitPromotionMaxScanned,
		KeyFamily:       distribution.MigrationFamilyUser,
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	if resp.GetDone() {
		return true, nil
	}
	if len(resp.GetNextCursor()) == 0 || bytes.Equal(resp.GetNextCursor(), cursor) {
		return false, errors.New("split migration abandon cleanup made no cursor progress")
	}
	encoded := encodeSplitCleanupCursor(0, resp.GetNextCursor(), false)
	err = s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseAbandoning && bytes.Equal(current.TargetClearedDescriptorAckCursor, job.TargetClearedDescriptorAckCursor) {
			current.TargetClearedDescriptorAckCursor = encoded
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
	return false, err
}

func (s *DistributionServer) cleanupAbandonedSplitJobSource(ctx context.Context, job distribution.SplitJob) error {
	if err := s.cleanupSplitJobSourceMetadata(ctx, job); err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseAbandoning {
			current.SourceCutoverAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			current.SourceRetentionPinTS = math.MaxUint64
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) cleanupSplitJobSourceMetadata(ctx context.Context, job distribution.SplitJob) error {
	source, _, _, err := s.splitJobMigrationClients(ctx, job)
	if err != nil {
		return err
	}
	if _, err := source.CleanupMigration(ctx, &pb.CleanupMigrationRequest{
		JobId: job.JobID,
		Mode:  pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA,
	}); err != nil {
		return errors.WithStack(err)
	}
	sourceGroupID, err := s.splitJobSourceGroupID(ctx, job)
	if err != nil {
		return err
	}
	_, metadataComplete, err := s.syncSplitMigrationVoterBarrier(ctx, sourceGroupID, nil, source, &pb.ProbeMigrationStateRequest{
		JobId: job.JobID,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	if err != nil || !metadataComplete {
		return err
	}
	return nil
}

func (s *DistributionServer) rollbackAbandonedSplitJobFence(ctx context.Context, job distribution.SplitJob) error {
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return err
	}
	fenced := abandonedSplitJobFencedRoute(snapshot.Routes, job)
	if fenced == nil {
		return nil
	}
	if snapshot.Version == math.MaxUint64 {
		return errors.New("split migration catalog version overflow")
	}
	fenced.State = distribution.RouteStateActive
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(*fenced, s.catalog.AllowsRouteDescriptorV2Writes())
	if err != nil {
		return errors.WithStack(err)
	}
	nextVersion := snapshot.Version + 1
	if err := s.commitAbandonedSplitJobFenceRollback(ctx, snapshot, *fenced, encoded, nextVersion); err != nil {
		return err
	}
	loaded, err := s.loadCatalogSnapshotAtLeastVersion(ctx, nextVersion)
	if err != nil {
		return err
	}
	return s.applyEngineSnapshot(loaded)
}

func abandonedSplitJobFencedRoute(routes []distribution.RouteDescriptor, job distribution.SplitJob) *distribution.RouteDescriptor {
	for i := range routes {
		route := &routes[i]
		if route.ParentRouteID == job.SourceRouteID && bytes.Equal(route.Start, job.SplitKey) && route.State == distribution.RouteStateWriteFenced {
			return route
		}
	}
	return nil
}

func (s *DistributionServer) commitAbandonedSplitJobFenceRollback(
	ctx context.Context,
	snapshot distribution.CatalogSnapshot,
	fenced distribution.RouteDescriptor,
	encoded []byte,
	nextVersion uint64,
) error {
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: distribution.CatalogRouteKey(fenced.RouteID), Value: encoded},
			{Op: kv.Put, Key: distribution.CatalogVersionKey(), Value: distribution.EncodeCatalogVersion(nextVersion)},
		},
		IsTxn:   true,
		StartTS: snapshot.ReadTS,
		ReadKeys: [][]byte{
			distribution.CatalogRouteKey(fenced.RouteID),
			distribution.CatalogVersionKey(),
		},
	}); err != nil {
		return splitJobCoordinatorStatusError(err)
	}
	return nil
}

func (s *DistributionServer) finishSplitJobHistory(ctx context.Context, job distribution.SplitJob, terminalPhase distribution.SplitJobPhase) error {
	current, readTS, err := s.catalog.LiveSplitJobForUpdate(ctx, job.JobID)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	if current.Phase != job.Phase {
		return nil
	}
	nowMs := time.Now().UnixMilli()
	next := distribution.CloneSplitJob(current)
	next.Phase = terminalPhase
	next.RetryPhase = distribution.SplitJobPhaseNone
	next.AbandonFromPhase = distribution.SplitJobPhaseNone
	next.TerminalAtMs = nowMs
	next.UpdatedAtMs = nowMs
	encoded, err := distribution.EncodeSplitJob(next)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	liveKey := distribution.CatalogSplitJobKey(job.JobID)
	historyKey := distribution.CatalogSplitJobHistoryKey(nowMs, job.JobID)
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: historyKey, Value: encoded},
			{Op: kv.Del, Key: liveKey},
		},
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{liveKey, historyKey},
	}); err != nil {
		return splitJobCoordinatorStatusError(err)
	}
	return nil
}

func (s *DistributionServer) gcSplitJobHistory(ctx context.Context, jobs []distribution.SplitJob, now time.Time) error {
	s.mu.Lock()
	if !s.splitJobHistoryGCLast.IsZero() && now.Sub(s.splitJobHistoryGCLast) < splitJobHistoryGCInterval {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	keys := splitJobHistoryGCKeys(jobs, now)
	if len(keys) > 0 {
		snapshot, err := s.loadCatalogSnapshot(ctx)
		if err != nil {
			return err
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(keys))
		for _, key := range keys {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
		}
		if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			Elems:    elems,
			IsTxn:    true,
			StartTS:  snapshot.ReadTS,
			ReadKeys: keys,
		}); err != nil {
			return splitJobCoordinatorStatusError(err)
		}
	}
	s.mu.Lock()
	s.splitJobHistoryGCLast = now
	s.mu.Unlock()
	return nil
}

func splitJobHistoryGCKeys(jobs []distribution.SplitJob, now time.Time) [][]byte {
	history := make([]distribution.SplitJob, 0, len(jobs))
	for _, job := range jobs {
		if (job.Phase == distribution.SplitJobPhaseDone || job.Phase == distribution.SplitJobPhaseAbandoned) && job.TerminalAtMs > 0 {
			history = append(history, job)
		}
	}
	sort.Slice(history, func(i, j int) bool {
		if history[i].TerminalAtMs == history[j].TerminalAtMs {
			return history[i].JobID < history[j].JobID
		}
		return history[i].TerminalAtMs < history[j].TerminalAtMs
	})
	cutoff := now.Add(-splitJobHistoryTTL).UnixMilli()
	excess := len(history) - splitJobHistoryLimit
	keys := make([][]byte, 0, min(len(history), splitJobHistoryDeleteBatchLimit))
	for i, job := range history {
		if job.TerminalAtMs >= cutoff && i >= excess {
			continue
		}
		keys = append(keys, distribution.CatalogSplitJobHistoryKey(job.TerminalAtMs, job.JobID))
		if len(keys) == splitJobHistoryDeleteBatchLimit {
			break
		}
	}
	return keys
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
	ackCursor, complete, err := s.armSplitJobSourceTracker(ctx, job, snapshot, parent, source)
	if err != nil {
		return err
	}
	if !complete {
		return s.persistSplitJobBackfillAck(ctx, job.JobID, ackCursor)
	}
	return s.openSplitJobBackfill(ctx, job, source)
}

const initialMigrationRetentionPinTS = uint64(1)

func (s *DistributionServer) armSplitJobSourceTracker(
	ctx context.Context,
	job distribution.SplitJob,
	snapshot distribution.CatalogSnapshot,
	parent distribution.RouteDescriptor,
	source SplitMigrationClient,
) ([]byte, bool, error) {
	// Arm the write tracker and its no-prune retention pin before choosing the
	// snapshot boundary. This closes the window where a low-ts write could land
	// after timestamp selection without being represented in the delta floor.
	if _, err := applySplitMigrationControl(ctx, source, job, parent.End, snapshot.Version+1, initialMigrationRetentionPinTS, false, false, true, initialMigrationRetentionPinTS); err != nil {
		return nil, false, err
	}
	return s.syncSplitMigrationVoterBarrier(ctx, parent.GroupID, job.FenceAckCursor, source, &pb.ProbeMigrationStateRequest{
		JobId:                  job.JobID,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		RouteStart:             job.SplitKey,
		RouteEnd:               parent.End,
		ExpectedCatalogVersion: snapshot.Version + 1,
		MigrationJobId:         job.JobID,
		MinWriteTsExclusive:    initialMigrationRetentionPinTS,
		TrackWrites:            true,
		RetentionPinTs:         initialMigrationRetentionPinTS,
	})
}

func (s *DistributionServer) persistSplitJobBackfillAck(ctx context.Context, jobID uint64, ackCursor []byte) error {
	return s.updateSplitJobViaCoordinator(ctx, jobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhasePlanned {
			current.FenceAckCursor = ackCursor
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) openSplitJobBackfill(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient) error {
	issued, err := source.IssueMigrationTimestamp(ctx, &pb.IssueMigrationTimestampRequest{})
	if err != nil {
		return errors.WithStack(err)
	}
	snapshotTS := issued.GetTimestamp()
	if snapshotTS == 0 || snapshotTS <= issued.GetLastCommitTs() {
		return errors.New("split migration source issued an invalid snapshot timestamp")
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase != distribution.SplitJobPhasePlanned {
			return current, nil
		}
		current.Phase = distribution.SplitJobPhaseBackfill
		current.SnapshotTS = snapshotTS
		current.WriteTrackerArmed = true
		current.SourceRetentionPinTS = initialMigrationRetentionPinTS
		current.FenceAckCursor = nil
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
	source, minAdmittedTS, ackCursor, complete, err := s.applySplitJobFenceBarrier(ctx, job, snapshot, sourceRoute)
	if err != nil {
		return err
	}
	if !complete {
		return s.persistSplitJobFenceAck(ctx, job.JobID, ackCursor)
	}
	pendingLocks, err := splitJobPendingLocks(ctx, source, job.SplitKey, sourceRoute.End)
	if err != nil {
		return err
	}
	if pendingLocks {
		return nil
	}
	issued, err := source.IssueMigrationTimestamp(ctx, &pb.IssueMigrationTimestampRequest{})
	if err != nil {
		return errors.WithStack(err)
	}
	if issued.GetTimestamp() == 0 || issued.GetTimestamp() <= issued.GetLastCommitTs() {
		return errors.New("split migration source issued an invalid fence timestamp")
	}
	return s.commitSplitJobFenceState(ctx, job, snapshot.Version, minAdmittedTS, ackCursor, issued.GetTimestamp())
}

func (s *DistributionServer) applySplitJobFenceBarrier(
	ctx context.Context,
	job distribution.SplitJob,
	snapshot distribution.CatalogSnapshot,
	sourceRoute distribution.RouteDescriptor,
) (SplitMigrationClient, uint64, []byte, bool, error) {
	source, _, err := s.splitMigrationClientFactory(ctx, job, sourceRoute.GroupID)
	if err != nil {
		return nil, 0, nil, false, errors.WithStack(err)
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
		return nil, 0, nil, false, err
	}
	ackCursor, complete, err := s.syncSplitMigrationVoterBarrier(ctx, sourceRoute.GroupID, job.FenceAckCursor, source, &pb.ProbeMigrationStateRequest{
		JobId:                  job.JobID,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		RouteStart:             job.SplitKey,
		RouteEnd:               sourceRoute.End,
		ExpectedCatalogVersion: snapshot.Version,
		MigrationJobId:         job.JobID,
		MinWriteTsExclusive:    job.SnapshotTS,
		SourceWriteFence:       true,
		TrackWrites:            true,
		RetentionPinTs:         1,
	})
	if err != nil {
		return nil, 0, nil, false, err
	}
	return source, minAdmittedTS, ackCursor, complete, nil
}

func (s *DistributionServer) persistSplitJobFenceAck(ctx context.Context, jobID uint64, ackCursor []byte) error {
	return s.updateSplitJobViaCoordinator(ctx, jobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase == distribution.SplitJobPhaseFence {
			current.FenceAckCursor = distribution.CloneBytes(ackCursor)
			current.UpdatedAtMs = time.Now().UnixMilli()
		}
		return current, nil
	})
}

func (s *DistributionServer) commitSplitJobFenceState(ctx context.Context, job distribution.SplitJob, fenceCatalogVersion, minAdmittedTS uint64, ackCursor []byte, fenceTS uint64) error {
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
		current.FenceAckCursor = distribution.CloneBytes(ackCursor)
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
	if job.CutoverVersion == 0 || job.CutoverVersion != expectedVersion {
		return s.armSplitJobCutoverWitness(ctx, job, expectedVersion)
	}
	targetCursor, sourceCursor, targetComplete, sourceComplete, err := s.applySplitJobCutoverBarriers(ctx, job, sourceRoute, expectedVersion)
	if err != nil {
		return err
	}
	if splitJobCutoverBarrierPending(job, targetComplete, sourceComplete) {
		return s.persistSplitJobCutoverAcks(ctx, job.JobID, targetCursor, sourceCursor, targetComplete, sourceComplete)
	}
	return s.commitSplitJobCutover(ctx, snapshot, sourceRoute, job, expectedVersion)
}

func (s *DistributionServer) applySplitJobCutoverBarriers(
	ctx context.Context,
	job distribution.SplitJob,
	sourceRoute distribution.RouteDescriptor,
	expectedVersion uint64,
) ([]byte, []byte, bool, bool, error) {
	source, target, err := s.splitMigrationClientFactory(ctx, job, sourceRoute.GroupID)
	if err != nil {
		return nil, nil, false, false, errors.WithStack(err)
	}
	if _, err := applySplitMigrationControl(ctx, target, job, sourceRoute.End, expectedVersion, job.FenceTS, false, false, false, 0); err != nil {
		return nil, nil, false, false, err
	}
	if _, err := applySplitMigrationControl(ctx, source, job, sourceRoute.End, expectedVersion, job.FenceTS, true, true, false, 1); err != nil {
		return nil, nil, false, false, err
	}
	targetCursor, targetComplete, err := s.syncSplitMigrationVoterBarrier(ctx, job.TargetGroupID, job.TargetStagedReadinessAckCursor, target, &pb.ProbeMigrationStateRequest{
		JobId:                  job.JobID,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		RouteStart:             job.SplitKey,
		RouteEnd:               sourceRoute.End,
		ExpectedCatalogVersion: expectedVersion,
		MigrationJobId:         job.JobID,
		MinWriteTsExclusive:    job.FenceTS,
	})
	if err != nil {
		return nil, nil, false, false, err
	}
	sourceCursor, sourceComplete, err := s.syncSplitMigrationVoterBarrier(ctx, sourceRoute.GroupID, job.SourceCutoverReadFenceAckCursor, source, &pb.ProbeMigrationStateRequest{
		JobId:                  job.JobID,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		RouteStart:             job.SplitKey,
		RouteEnd:               sourceRoute.End,
		ExpectedCatalogVersion: expectedVersion,
		MigrationJobId:         job.JobID,
		MinWriteTsExclusive:    job.FenceTS,
		SourceWriteFence:       true,
		SourceReadFence:        true,
		RetentionPinTs:         1,
	})
	if err != nil {
		return nil, nil, false, false, err
	}
	return targetCursor, sourceCursor, targetComplete, sourceComplete, nil
}

func splitJobCutoverBarrierPending(job distribution.SplitJob, targetComplete, sourceComplete bool) bool {
	return !targetComplete || !sourceComplete ||
		job.TargetStagedReadinessState != distribution.SplitJobBarrierArmed ||
		job.CutoverReadFenceState != distribution.SplitJobBarrierArmed
}

func (s *DistributionServer) armSplitJobCutoverWitness(ctx context.Context, job distribution.SplitJob, expectedVersion uint64) error {
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase != distribution.SplitJobPhaseCutover {
			return current, nil
		}
		current.CutoverVersion = expectedVersion
		current.CutoverReadFenceState = distribution.SplitJobBarrierArming
		current.TargetStagedReadinessState = distribution.SplitJobBarrierArming
		current.SourceCutoverReadFenceAckCursor = nil
		current.TargetStagedReadinessAckCursor = nil
		current.UpdatedAtMs = time.Now().UnixMilli()
		return current, nil
	})
}

func (s *DistributionServer) persistSplitJobCutoverAcks(
	ctx context.Context,
	jobID uint64,
	targetCursor []byte,
	sourceCursor []byte,
	targetComplete bool,
	sourceComplete bool,
) error {
	return s.updateSplitJobViaCoordinator(ctx, jobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if current.Phase != distribution.SplitJobPhaseCutover {
			return current, nil
		}
		current.TargetStagedReadinessAckCursor = distribution.CloneBytes(targetCursor)
		current.SourceCutoverReadFenceAckCursor = distribution.CloneBytes(sourceCursor)
		current.TargetStagedReadinessState = distribution.SplitJobBarrierArming
		if targetComplete {
			current.TargetStagedReadinessState = distribution.SplitJobBarrierArmed
		}
		current.CutoverReadFenceState = distribution.SplitJobBarrierArming
		if sourceComplete {
			current.CutoverReadFenceState = distribution.SplitJobBarrierArmed
		}
		current.UpdatedAtMs = time.Now().UnixMilli()
		return current, nil
	})
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
	nextJob.Cursor = nil
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
