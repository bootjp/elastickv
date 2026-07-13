package store

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func (s *pebbleStore) ExportVersions(ctx context.Context, opts ExportVersionsOptions) (ExportVersionsResult, error) {
	pos, err := decodeExportCursor(opts.Cursor)
	if err != nil {
		return ExportVersionsResult{}, err
	}
	if opts.MaxVersions <= 0 {
		return ExportVersionsResult{Done: true}, nil
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

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
	iterOpts := &pebble.IterOptions{
		LowerBound: encodeKey(opts.StartKey, math.MaxUint64),
	}
	if opts.EndKey != nil {
		iterOpts.UpperBound = encodeKey(opts.EndKey, math.MaxUint64)
	}
	return iterOpts
}

func pebbleExportSeekKey(opts ExportVersionsOptions, pos exportCursorPosition) ([]byte, error) {
	if len(pos.key) == 0 {
		return encodeKey(opts.StartKey, math.MaxUint64), nil
	}
	if opts.StartKey != nil && bytes.Compare(pos.key, opts.StartKey) < 0 {
		return nil, errors.WithStack(ErrInvalidExportCursor)
	}
	if opts.EndKey != nil && bytes.Compare(pos.key, opts.EndKey) >= 0 {
		return nil, errors.WithStack(ErrInvalidExportCursor)
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
	if isPebbleMetaKey(rawKey) {
		return true, true, nil
	}
	userKey, commitTS := decodeKeyView(rawKey)
	if userKey == nil || pebbleExportCursorEqual(pos, userKey, commitTS) {
		return true, true, nil
	}
	if opts.EndKey != nil && bytes.Compare(userKey, opts.EndKey) >= 0 {
		return false, true, errExportReachedEnd
	}
	if commitTS <= opts.MinCommitTSExclusive {
		_ = s.skipToNextUserKey(iter, userKey)
		return false, true, nil
	}
	done, err = s.exportPebbleVersion(iter, opts, userKey, commitTS, result)
	return true, done, err
}

func pebbleExportCursorEqual(pos exportCursorPosition, userKey []byte, commitTS uint64) bool {
	return len(pos.key) > 0 && bytes.Equal(userKey, pos.key) && commitTS == pos.commitTS
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
	var value []byte
	if sv.Tombstone {
		value = nil
	} else {
		value, err = s.decryptForKey(iter.Key(), sv, sv.Value)
		if err != nil {
			return MVCCVersion{}, err
		}
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
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	duplicate, ackedCursor, err := s.validatePebbleImportBatch(opts)
	if err != nil {
		return ImportVersionsResult{}, err
	}
	if duplicate {
		return ImportVersionsResult{AckedCursor: ackedCursor, Duplicate: true}, nil
	}

	batchMax := importBatchMaxTS(opts.Versions)
	if err := s.commitPebbleImportBatch(opts, batchMax); err != nil {
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

func (s *pebbleStore) commitPebbleImportBatch(opts ImportVersionsOptions, batchMax uint64) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.applyImportVersionsBatch(batch, opts.Versions); err != nil {
		return err
	}
	if err := batch.Set(migrationAckKey(opts.JobID, opts.BracketID), encodeMigrationImportAck(migrationImportAck{
		batchSeq: opts.BatchSeq,
		cursor:   opts.Cursor,
	}), nil); err != nil {
		return errors.WithStack(err)
	}
	unlock, newLastTS, err := s.stageMigrationClockMetadataIfNeeded(batch, opts.JobID, batchMax)
	if err != nil {
		return err
	}
	defer unlock()
	if err := batch.Commit(s.directApplyWriteOpts()); err != nil {
		return errors.WithStack(err)
	}
	if batchMax > 0 {
		s.lastCommitTS = newLastTS
	}
	return nil
}

func (s *pebbleStore) stageMigrationClockMetadataIfNeeded(batch *pebble.Batch, jobID, batchMax uint64) (func(), uint64, error) {
	if batchMax == 0 {
		return func() {}, 0, nil
	}
	return s.stageMigrationClockMetadata(batch, jobID, batchMax)
}

func (s *pebbleStore) applyImportVersionsBatch(batch *pebble.Batch, versions []MVCCVersion) error {
	for _, version := range versions {
		k := encodeKey(version.Key, version.CommitTS)
		var encoded []byte
		if version.Tombstone {
			encoded = encodeValue(nil, true, 0, encStateCleartext)
		} else {
			body, encState, err := s.encryptForKey(k, version.Value, version.ExpireAt, true)
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
		if err := setPebbleUint64InBatch(batch, migrationHLCFloorKey(jobID), batchMax); err != nil {
			unlock()
			return nil, 0, err
		}
	}
	return unlock, newLastTS, nil
}

func (s *pebbleStore) readMigrationImportAck(jobID, bracketID uint64) (migrationImportAck, bool, error) {
	val, closer, err := s.db.Get(migrationAckKey(jobID, bracketID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return migrationImportAck{}, false, nil
		}
		return migrationImportAck{}, false, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	ack, ok := decodeMigrationImportAck(val)
	if !ok {
		return migrationImportAck{}, false, errors.New("corrupt migration import ack")
	}
	return ack, true, nil
}

func (s *pebbleStore) readMigrationHLCFloorLocked(jobID uint64) (uint64, error) {
	return readPebbleUint64(s.db, migrationHLCFloorKey(jobID))
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
