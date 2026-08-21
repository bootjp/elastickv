package store

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func cleanupExportOptions(opts CleanupVersionsOptions) ExportVersionsOptions {
	return ExportVersionsOptions{
		StartKey:             opts.StartKey,
		EndKey:               opts.EndKey,
		Cursor:               opts.Cursor,
		MaxCommitTSInclusive: opts.MaxCommitTS,
		MaxVersions:          opts.MaxVersions,
		MaxBytes:             opts.MaxBytes,
		MaxScannedBytes:      opts.MaxScannedBytes,
		KeyFamily:            opts.KeyFamily,
		AcceptVersion:        opts.AcceptVersion,
	}
}

func cleanupResult(exported ExportVersionsResult) CleanupVersionsResult {
	var deletedBytes uint64
	for _, version := range exported.Versions {
		deletedBytes += versionExportSize(version.Key, len(version.Value))
	}
	return CleanupVersionsResult{
		NextCursor:   bytes.Clone(exported.NextCursor),
		Done:         exported.Done,
		DeletedRows:  uint64(len(exported.Versions)), //nolint:gosec // bounded by MaxVersions.
		DeletedBytes: deletedBytes,
		ScannedBytes: exported.ScannedBytes,
	}
}

func (s *mvccStore) CleanupVersions(ctx context.Context, opts CleanupVersionsOptions) (CleanupVersionsResult, error) {
	if opts.MaxVersions <= 0 {
		return CleanupVersionsResult{Done: true}, nil
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	exportOpts := normalizeExportVersionsOptions(cleanupExportOptions(opts))
	pos, err := decodeExportCursorForOptions(exportOpts)
	if err != nil {
		return CleanupVersionsResult{}, err
	}
	exported, err := s.exportVersionsLocked(ctx, exportOpts, pos)
	if err != nil {
		return CleanupVersionsResult{}, err
	}
	for _, version := range exported.Versions {
		s.removeVersionLocked(version.Key, version.CommitTS)
	}
	return cleanupResult(exported), nil
}

func (s *mvccStore) ClearMigrationState(_ context.Context, jobID, _ uint64) error {
	if jobID == 0 {
		return errors.New("migration cleanup job id is required")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for id := range s.migrationAcks {
		if id.jobID == jobID {
			delete(s.migrationAcks, id)
		}
	}
	delete(s.migrationHLCFloors, jobID)
	delete(s.migrationPromotions, jobID)
	delete(s.migrationReadiness, jobID)
	s.migrationReadinessCache = upsertReadinessCacheWithoutJob(s.migrationReadinessCache, jobID)
	return nil
}

func upsertReadinessCacheWithoutJob(states []TargetStagedReadinessState, jobID uint64) []TargetStagedReadinessState {
	out := states[:0]
	for _, state := range states {
		if state.JobID != jobID {
			out = append(out, state)
		}
	}
	return out
}

func (s *pebbleStore) CleanupVersions(ctx context.Context, opts CleanupVersionsOptions) (CleanupVersionsResult, error) {
	if opts.MaxVersions <= 0 {
		return CleanupVersionsResult{Done: true}, nil
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	exported, err := s.exportVersionsLocked(ctx, cleanupExportOptions(opts))
	if err != nil {
		return CleanupVersionsResult{}, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, version := range exported.Versions {
		if err := batch.Delete(encodeKey(version.Key, version.CommitTS), nil); err != nil {
			return CleanupVersionsResult{}, errors.WithStack(err)
		}
	}
	if opts.AppliedIndex > 0 {
		if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, opts.AppliedIndex); err != nil {
			return CleanupVersionsResult{}, err
		}
	}
	if err := batch.Commit(s.cleanupWriteOpts(opts.AppliedIndex)); err != nil {
		return CleanupVersionsResult{}, errors.WithStack(err)
	}
	return cleanupResult(exported), nil
}

func (s *pebbleStore) ClearMigrationState(_ context.Context, jobID, appliedIndex uint64) error {
	if jobID == 0 {
		return errors.New("migration cleanup job id is required")
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	metadata, err := s.migrationMetadataWithoutJob(jobID)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := stageMigrationMetadataCleanup(batch, jobID, appliedIndex, metadata); err != nil {
		return err
	}
	if err := batch.Commit(s.cleanupWriteOpts(appliedIndex)); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	s.migrationReadinessCache = upsertReadinessCacheWithoutJob(s.migrationReadinessCache, jobID)
	s.mtx.Unlock()
	return nil
}

type migrationMetadataState struct {
	acks       map[migrationAckID]migrationImportAck
	floors     map[uint64]uint64
	promotions map[uint64]PromotionState
}

func (s *pebbleStore) migrationMetadataWithoutJob(jobID uint64) (migrationMetadataState, error) {
	acks, err := s.readMigrationImportAcks()
	if err != nil {
		return migrationMetadataState{}, err
	}
	for id := range acks {
		if id.jobID == jobID {
			delete(acks, id)
		}
	}
	floors, err := s.readMigrationHLCFloors()
	if err != nil {
		return migrationMetadataState{}, err
	}
	delete(floors, jobID)
	promotions, err := s.readPebblePromotionStates()
	if err != nil {
		return migrationMetadataState{}, err
	}
	delete(promotions, jobID)
	return migrationMetadataState{acks: acks, floors: floors, promotions: promotions}, nil
}

func stageMigrationMetadataCleanup(batch *pebble.Batch, jobID, appliedIndex uint64, metadata migrationMetadataState) error {
	if err := batch.Set(migrationAckMetaKeyBytes, encodeMigrationImportAcks(metadata.acks), nil); err != nil {
		return errors.WithStack(err)
	}
	if err := batch.Set(migrationHLCFloorMetaKeyBytes, encodeMigrationHLCFloors(metadata.floors), nil); err != nil {
		return errors.WithStack(err)
	}
	if err := batch.Set(migrationPromoteMetaKeyBytes, encodeMigrationPromotionStates(metadata.promotions), nil); err != nil {
		return errors.WithStack(err)
	}
	if err := batch.Delete(migrationReadyKey(jobID), nil); err != nil {
		return errors.WithStack(err)
	}
	if appliedIndex == 0 {
		return nil
	}
	return setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex)
}

func (s *pebbleStore) cleanupWriteOpts(appliedIndex uint64) *pebble.WriteOptions {
	if appliedIndex > 0 {
		return s.raftApplyWriteOpts()
	}
	return s.directApplyWriteOpts()
}

var _ MigrationCleaner = (*mvccStore)(nil)
var _ MigrationCleaner = (*pebbleStore)(nil)
