package store

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func (s *pebbleStore) ExportVersions(ctx context.Context, opts ExportVersionsOptions) (ExportVersionsResult, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	return s.exportVersionsLocked(ctx, opts)
}

func (s *pebbleStore) exportVersionsLocked(ctx context.Context, opts ExportVersionsOptions) (ExportVersionsResult, error) {
	opts = normalizeExportVersionsOptions(opts)
	pos, err := decodeExportCursorForOptions(opts)
	if err != nil {
		return ExportVersionsResult{}, err
	}
	if opts.MaxVersions <= 0 {
		return ExportVersionsResult{Done: true}, nil
	}

	iter, err := s.db.NewIter(pebbleExportIterOptions(opts))
	if err != nil {
		return ExportVersionsResult{}, errors.WithStack(err)
	}
	defer iter.Close()

	seek, err := pebbleExportSeekKey(opts, pos)
	if err != nil {
		return ExportVersionsResult{}, err
	}
	result := newExportVersionsResult(opts.MaxVersions)
	if err := s.runPebbleExportLoop(ctx, iter, seek, opts, pos, &result); err != nil {
		if errors.Is(err, errExportChunkFull) {
			return result, nil
		}
		if errors.Is(err, errExportReachedEnd) {
			result.Done = true
			result.NextCursor = nil
			return result, nil
		}
		return result, err
	}
	if err := iter.Error(); err != nil {
		return ExportVersionsResult{}, errors.WithStack(err)
	}
	result.Done = true
	result.NextCursor = nil
	return result, nil
}

func (s *pebbleStore) runPebbleExportLoop(
	ctx context.Context,
	iter *pebble.Iterator,
	seek []byte,
	opts ExportVersionsOptions,
	pos exportCursorPosition,
	result *ExportVersionsResult,
) error {
	for ok := iter.SeekGE(seek); ok; {
		advance, done, err := s.exportPebbleIteratorPosition(ctx, iter, opts, pos, result)
		if err != nil {
			return err
		}
		if !done {
			return errExportChunkFull
		}
		if advance {
			ok = iter.Next()
			continue
		}
		ok = iter.Valid()
	}
	return nil
}

func pebbleExportIterOptions(opts ExportVersionsOptions) *pebble.IterOptions {
	return &pebble.IterOptions{}
}

func pebbleExportSeekKey(opts ExportVersionsOptions, pos exportCursorPosition) ([]byte, error) {
	if !pos.hasKey {
		return encodeKey(opts.StartKey, math.MaxUint64), nil
	}
	if err := validateExportCursorRange(opts, pos); err != nil {
		return nil, err
	}
	return encodeKey(pos.key, pos.commitTS), nil
}

func (s *pebbleStore) exportPebbleIteratorPosition(
	ctx context.Context,
	iter *pebble.Iterator,
	opts ExportVersionsOptions,
	pos exportCursorPosition,
	result *ExportVersionsResult,
) (advance bool, done bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, false, errors.WithStack(err)
	}
	rawKey := iter.Key()
	if isPebbleExportMetadataKey(rawKey) {
		return true, true, nil
	}
	userKey, commitTS := decodeKeyView(rawKey)
	if userKey == nil {
		return true, true, nil
	}
	if pebbleExportCursorWholeKeySkipped(pos, userKey, commitTS) {
		done := advancePebbleExportPastCurrentUserKey(iter, opts, userKey, pos.tag, result)
		return false, done, nil
	}
	if pebbleExportCursorEqual(pos, userKey, commitTS) {
		return true, true, nil
	}
	if skipped, err := s.skipPebbleExportKeyOutsideRange(iter, opts, userKey, commitTS, result); skipped || err != nil {
		return false, true, err
	}
	if commitTS <= opts.MinCommitTSExclusive {
		return s.skipPebbleExportVersionBelowMinTS(iter, opts, userKey, commitTS, result)
	}
	done, err = s.exportPebbleVersion(iter, opts, userKey, commitTS, result)
	return true, done, err
}

func isPebbleExportMetadataKey(rawKey []byte) bool {
	return isPebbleMetaKey(rawKey)
}

func (s *pebbleStore) skipPebbleExportKeyOutsideRange(
	iter *pebble.Iterator,
	opts ExportVersionsOptions,
	userKey []byte,
	commitTS uint64,
	result *ExportVersionsResult,
) (bool, error) {
	if opts.StartKey != nil && bytes.Compare(userKey, opts.StartKey) < 0 {
		return true, skipPebbleExportWholeKey(iter, opts, userKey, commitTS, exportCursorTagSkippedKey, result)
	}
	if opts.EndKey == nil || bytes.Compare(userKey, opts.EndKey) < 0 {
		return false, nil
	}
	if pebbleExportCanStopAtEndKey(opts.StartKey, opts.EndKey, userKey) {
		return true, errExportReachedEnd
	}
	return true, skipPebbleExportWholeKey(iter, opts, userKey, commitTS, exportCursorTagSkippedKey, result)
}

func skipPebbleExportWholeKey(
	iter *pebble.Iterator,
	opts ExportVersionsOptions,
	userKey []byte,
	commitTS uint64,
	tag byte,
	result *ExportVersionsResult,
) error {
	rawValue := iter.Value()
	result.ScannedBytes += versionExportSize(userKey, len(rawValue))
	result.NextCursor = encodeExportCursor(userKey, commitTS, tag)
	if finishExportIfLimited(opts, result) {
		result.Done = false
		return errExportChunkFull
	}
	if !advancePebbleExportPastCurrentUserKey(iter, opts, userKey, tag, result) {
		return errExportChunkFull
	}
	return nil
}

func (s *pebbleStore) skipPebbleExportVersionBelowMinTS(
	iter *pebble.Iterator,
	opts ExportVersionsOptions,
	userKey []byte,
	commitTS uint64,
	result *ExportVersionsResult,
) (advance bool, done bool, err error) {
	rawValue := iter.Value()
	result.ScannedBytes += versionExportSize(userKey, len(rawValue))
	result.NextCursor = encodeExportCursor(userKey, commitTS, exportCursorTagPrunedKey)
	if finishExportIfLimited(opts, result) {
		result.Done = false
		return false, false, nil
	}
	return false, advancePebbleExportPastCurrentUserKey(iter, opts, userKey, exportCursorTagPrunedKey, result), nil
}

func advancePebbleExportPastCurrentUserKey(
	iter *pebble.Iterator,
	opts ExportVersionsOptions,
	userKey []byte,
	tag byte,
	result *ExportVersionsResult,
) bool {
	userKey = bytes.Clone(userKey)
	for iter.Next() {
		currentUserKey, commitTS := decodeKeyView(iter.Key())
		if !bytes.Equal(currentUserKey, userKey) {
			return true
		}
		rawValue := iter.Value()
		result.ScannedBytes += versionExportSize(currentUserKey, len(rawValue))
		result.NextCursor = encodeExportCursor(currentUserKey, commitTS, tag)
		if finishExportIfLimited(opts, result) {
			result.Done = false
			return false
		}
	}
	return true
}

func pebbleExportCanStopAtEndKey(startKey, endKey, userKey []byte) bool {
	if len(startKey) == 0 {
		return false
	}
	for prefixLen := 1; prefixLen <= len(userKey); prefixLen++ {
		prefix := userKey[:prefixLen]
		if bytes.Compare(prefix, startKey) < 0 {
			continue
		}
		if bytes.Compare(prefix, endKey) < 0 {
			return false
		}
	}
	return true
}

func pebbleExportCursorEqual(pos exportCursorPosition, userKey []byte, commitTS uint64) bool {
	return pos.hasKey && bytes.Equal(userKey, pos.key) && commitTS == pos.commitTS
}

func pebbleExportCursorWholeKeySkipped(pos exportCursorPosition, userKey []byte, commitTS uint64) bool {
	return pos.hasKey &&
		(pos.tag == exportCursorTagPrunedKey || pos.tag == exportCursorTagSkippedKey) &&
		bytes.Equal(userKey, pos.key) &&
		commitTS == pos.commitTS
}

func (s *pebbleStore) exportPebbleVersion(
	iter *pebble.Iterator,
	opts ExportVersionsOptions,
	userKey []byte,
	commitTS uint64,
	result *ExportVersionsResult,
) (bool, error) {
	tag := exportCursorTagScanned
	rawValue := iter.Value()
	result.ScannedBytes += versionExportSize(userKey, len(rawValue))
	if shouldExportPebbleVersion(opts, userKey, commitTS) {
		version, err := s.decodeExportedPebbleVersion(iter, userKey, commitTS, opts.KeyFamily)
		if err != nil {
			return false, err
		}
		if opts.AcceptVersion != nil && !opts.AcceptVersion(version.Key, version.Value) {
			result.NextCursor = encodeExportCursor(userKey, commitTS, exportCursorTagScanned)
			if finishExportIfLimited(opts, result) {
				result.Done = false
				return false, nil
			}
			return true, nil
		}
		result.Versions = append(result.Versions, version)
		result.ExportedBytes += versionExportSize(userKey, len(version.Value))
		result.AcceptedRows++
		tag = exportCursorTagEmitted
	}
	result.NextCursor = encodeExportCursor(userKey, commitTS, tag)
	if finishExportIfLimited(opts, result) {
		result.Done = false
		return false, nil
	}
	return true, nil
}

func shouldExportPebbleVersion(opts ExportVersionsOptions, userKey []byte, commitTS uint64) bool {
	if shouldSkipMigrationExportKey(userKey) {
		return false
	}
	if opts.AcceptKey != nil && !opts.AcceptKey(userKey) {
		return false
	}
	return opts.MaxCommitTSInclusive == 0 || commitTS <= opts.MaxCommitTSInclusive
}

func (s *pebbleStore) decodeExportedPebbleVersion(iter *pebble.Iterator, userKey []byte, commitTS uint64, keyFamily uint32) (MVCCVersion, error) {
	sv, err := decodeValue(iter.Value())
	if err != nil {
		return MVCCVersion{}, errors.WithStack(err)
	}
	value, err := s.decryptForKey(iter.Key(), sv, sv.Value)
	if err != nil {
		return MVCCVersion{}, err
	}
	if sv.Tombstone {
		value = nil
	}
	return MVCCVersion{
		Key:       bytes.Clone(userKey),
		CommitTS:  commitTS,
		Tombstone: sv.Tombstone,
		Value:     bytes.Clone(value),
		KeyFamily: keyFamily,
		ExpireAt:  sv.ExpireAt,
	}, nil
}

func (s *pebbleStore) ImportVersions(ctx context.Context, opts ImportVersionsOptions) (ImportVersionsResult, error) {
	return s.importVersionsWithOpts(ctx, opts, s.directApplyWriteOpts(), true)
}

func (s *pebbleStore) ImportVersionsRaft(ctx context.Context, opts ImportVersionsOptions) (ImportVersionsResult, error) {
	return s.importVersionsWithOpts(ctx, opts, s.raftApplyWriteOpts(), false)
}

func (s *pebbleStore) importVersionsWithOpts(ctx context.Context, opts ImportVersionsOptions, writeOpts *pebble.WriteOptions, gateRegistration bool) (ImportVersionsResult, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	duplicate, ackedCursor, err := s.validatePebbleImportBatch(opts)
	if err != nil {
		return ImportVersionsResult{}, err
	}
	if duplicate {
		if err := s.commitPebbleImportAppliedIndex(opts.AppliedIndex, writeOpts); err != nil {
			return ImportVersionsResult{}, err
		}
		return ImportVersionsResult{AckedCursor: ackedCursor, Duplicate: true}, nil
	}

	batchMax := importBatchMaxTS(opts.Versions)
	if err := s.commitPebbleImportBatch(opts, batchMax, writeOpts, gateRegistration); err != nil {
		return ImportVersionsResult{}, errors.WithStack(err)
	}
	s.log.InfoContext(ctx, "import_versions",
		"job_id", opts.JobID,
		"bracket_id", opts.BracketID,
		"batch_seq", opts.BatchSeq,
		"versions", len(opts.Versions),
		"max_imported_ts", batchMax,
	)
	return ImportVersionsResult{AckedCursor: bytes.Clone(opts.Cursor), MaxImportedTS: batchMax}, nil
}

func (s *pebbleStore) commitPebbleImportAppliedIndex(appliedIndex uint64, writeOpts *pebble.WriteOptions) error {
	if appliedIndex == 0 {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
		return err
	}
	return errors.WithStack(batch.Commit(writeOpts))
}

func (s *pebbleStore) validatePebbleImportBatch(opts ImportVersionsOptions) (bool, []byte, error) {
	existing, hasExisting, err := s.readMigrationImportAck(opts.JobID, opts.BracketID)
	if err != nil {
		return false, nil, err
	}
	duplicate, err := validateNextImportBatch(existing, hasExisting, opts.BatchSeq)
	if err != nil {
		return false, nil, err
	}
	if duplicate {
		return true, bytes.Clone(existing.cursor), nil
	}
	for _, version := range opts.Versions {
		if err := validateImportVersion(version); err != nil {
			return false, nil, err
		}
	}
	return false, nil, nil
}

func (s *pebbleStore) commitPebbleImportBatch(opts ImportVersionsOptions, batchMax uint64, writeOpts *pebble.WriteOptions, gateRegistration bool) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.applyImportVersionsBatch(batch, opts.Versions, gateRegistration); err != nil {
		return err
	}
	if err := s.stageMigrationImportAck(batch, opts.JobID, opts.BracketID, migrationImportAck{
		batchSeq: opts.BatchSeq,
		cursor:   opts.Cursor,
	}); err != nil {
		return err
	}
	unlock, newLastTS, err := s.stageMigrationClockMetadataIfNeeded(batch, opts.JobID, batchMax)
	if err != nil {
		return err
	}
	defer unlock()
	if err := stagePebbleAppliedIndex(batch, opts.AppliedIndex); err != nil {
		return err
	}
	if err := batch.Commit(writeOpts); err != nil {
		return errors.WithStack(err)
	}
	if batchMax > 0 {
		s.lastCommitTS = newLastTS
	}
	return nil
}

func stagePebbleAppliedIndex(batch *pebble.Batch, appliedIndex uint64) error {
	if appliedIndex == 0 {
		return nil
	}
	return setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex)
}

func (s *pebbleStore) stageMigrationImportAck(batch *pebble.Batch, jobID, bracketID uint64, ack migrationImportAck) error {
	acks, err := s.readMigrationImportAcks()
	if err != nil {
		return err
	}
	acks[migrationAckID{jobID: jobID, bracketID: bracketID}] = migrationImportAck{
		batchSeq: ack.batchSeq,
		cursor:   bytes.Clone(ack.cursor),
	}
	return errors.WithStack(batch.Set(migrationAckMetaKeyBytes, encodeMigrationImportAcks(acks), nil))
}

func (s *pebbleStore) stageMigrationClockMetadataIfNeeded(batch *pebble.Batch, jobID, batchMax uint64) (func(), uint64, error) {
	if batchMax == 0 {
		return func() {}, 0, nil
	}
	return s.stageMigrationClockMetadata(batch, jobID, batchMax)
}

func (s *pebbleStore) applyImportVersionsBatch(batch *pebble.Batch, versions []MVCCVersion, gateRegistration bool) error {
	for _, version := range versions {
		k, err := encodePebbleUserVersionKey(version.Key, version.CommitTS)
		if err != nil {
			return err
		}
		var encoded []byte
		if version.Tombstone {
			encoded = encodeValue(nil, true, 0, encStateCleartext)
		} else {
			body, encState, err := s.encryptForKey(k, version.Value, version.ExpireAt, gateRegistration)
			if err != nil {
				return err
			}
			encoded = encodeValue(body, false, version.ExpireAt, encState)
		}
		if err := batch.Set(k, encoded, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *pebbleStore) stageMigrationClockMetadata(batch *pebble.Batch, jobID, batchMax uint64) (func(), uint64, error) {
	s.mtx.Lock()
	unlock := func() { s.mtx.Unlock() }
	newLastTS := s.lastCommitTS
	if batchMax > newLastTS {
		newLastTS = batchMax
	}
	if err := setPebbleUint64InBatch(batch, metaLastCommitTSBytes, newLastTS); err != nil {
		unlock()
		return nil, 0, err
	}
	floor, err := s.readMigrationHLCFloorLocked(jobID)
	if err != nil {
		unlock()
		return nil, 0, err
	}
	if batchMax > floor {
		floors, err := s.readMigrationHLCFloors()
		if err != nil {
			unlock()
			return nil, 0, err
		}
		floors[jobID] = batchMax
		if err := batch.Set(migrationHLCFloorMetaKeyBytes, encodeMigrationHLCFloors(floors), nil); err != nil {
			unlock()
			return nil, 0, errors.WithStack(err)
		}
	}
	return unlock, newLastTS, nil
}

func (s *pebbleStore) readMigrationImportAck(jobID, bracketID uint64) (migrationImportAck, bool, error) {
	acks, err := s.readMigrationImportAcks()
	if err != nil {
		return migrationImportAck{}, false, err
	}
	ack, ok := acks[migrationAckID{jobID: jobID, bracketID: bracketID}]
	return ack, ok, nil
}

func (s *pebbleStore) readMigrationImportAcks() (map[migrationAckID]migrationImportAck, error) {
	val, closer, err := s.db.Get(migrationAckMetaKeyBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return make(map[migrationAckID]migrationImportAck), nil
		}
		return nil, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	acks, ok := decodeMigrationImportAcks(val)
	if !ok {
		return nil, errors.New("corrupt migration import ack metadata")
	}
	return acks, nil
}

func (s *pebbleStore) readMigrationHLCFloorLocked(jobID uint64) (uint64, error) {
	floors, err := s.readMigrationHLCFloors()
	if err != nil {
		return 0, err
	}
	return floors[jobID], nil
}

func (s *pebbleStore) readMigrationHLCFloors() (map[uint64]uint64, error) {
	val, closer, err := s.db.Get(migrationHLCFloorMetaKeyBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return make(map[uint64]uint64), nil
		}
		return nil, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	floors, ok := decodeMigrationHLCFloors(val)
	if !ok {
		return nil, errors.New("corrupt migration HLC floor metadata")
	}
	return floors, nil
}

func (s *pebbleStore) MigrationHLCFloor(_ context.Context, jobID uint64) (uint64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	floor, err := s.readMigrationHLCFloorLocked(jobID)
	if err != nil {
		return 0, err
	}
	return floor, nil
}
