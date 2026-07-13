package store

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/emirpasic/gods/maps/treemap"
)

const (
	exportCursorTagEmitted byte = iota
	exportCursorTagScanned

	migrationAckPrefix        = "!migstage|ack|"
	migrationHLCFloorPrefix   = "!migstage|hlc_floor|"
	migrationPromotePrefix    = "!migstage|promote|"
	migrationReadyPrefix      = "!migstage|ready|"
	migrationUint64Bytes      = 8
	migrationAckKeyIDBytes    = 2 * migrationUint64Bytes
	exportVersionSizeOverhead = 24
)

type exportCursorPosition struct {
	key      []byte
	commitTS uint64
	tag      byte
}

type migrationImportAck struct {
	batchSeq uint64
	cursor   []byte
}

func encodeExportCursor(key []byte, commitTS uint64, tag byte) []byte {
	var buf []byte
	buf = binary.AppendUvarint(buf, lenAsUint64(len(key)))
	buf = append(buf, key...)
	buf = binary.AppendUvarint(buf, commitTS)
	buf = append(buf, tag)
	return buf
}

func decodeExportCursor(cursor []byte) (exportCursorPosition, error) {
	if len(cursor) == 0 {
		return exportCursorPosition{}, nil
	}
	keyLen, n := binary.Uvarint(cursor)
	if n <= 0 {
		return exportCursorPosition{}, errors.WithStack(ErrInvalidExportCursor)
	}
	rest := cursor[n:]
	if keyLen > lenAsUint64(len(rest)) {
		return exportCursorPosition{}, errors.WithStack(ErrInvalidExportCursor)
	}
	key := bytes.Clone(rest[:keyLen])
	rest = rest[keyLen:]
	commitTS, n := binary.Uvarint(rest)
	if n <= 0 {
		return exportCursorPosition{}, errors.WithStack(ErrInvalidExportCursor)
	}
	rest = rest[n:]
	if len(rest) != 1 {
		return exportCursorPosition{}, errors.WithStack(ErrInvalidExportCursor)
	}
	tag := rest[0]
	if tag != exportCursorTagEmitted && tag != exportCursorTagScanned {
		return exportCursorPosition{}, errors.WithStack(ErrInvalidExportCursor)
	}
	return exportCursorPosition{key: key, commitTS: commitTS, tag: tag}, nil
}

func migrationAckKey(jobID, bracketID uint64) []byte {
	key := make([]byte, len(migrationAckPrefix)+migrationAckKeyIDBytes)
	copy(key, migrationAckPrefix)
	binary.BigEndian.PutUint64(key[len(migrationAckPrefix):], jobID)
	binary.BigEndian.PutUint64(key[len(migrationAckPrefix)+migrationUint64Bytes:], bracketID)
	return key
}

func migrationHLCFloorKey(jobID uint64) []byte {
	key := make([]byte, len(migrationHLCFloorPrefix)+migrationUint64Bytes)
	copy(key, migrationHLCFloorPrefix)
	binary.BigEndian.PutUint64(key[len(migrationHLCFloorPrefix):], jobID)
	return key
}

func migrationPromoteKey(jobID uint64) []byte {
	key := make([]byte, len(migrationPromotePrefix)+migrationUint64Bytes)
	copy(key, migrationPromotePrefix)
	binary.BigEndian.PutUint64(key[len(migrationPromotePrefix):], jobID)
	return key
}

func migrationReadyKey(jobID uint64) []byte {
	key := make([]byte, len(migrationReadyPrefix)+migrationUint64Bytes)
	copy(key, migrationReadyPrefix)
	binary.BigEndian.PutUint64(key[len(migrationReadyPrefix):], jobID)
	return key
}

func isMigrationMetadataKey(rawKey []byte) bool {
	return (len(rawKey) == len(migrationAckPrefix)+migrationAckKeyIDBytes && bytes.HasPrefix(rawKey, []byte(migrationAckPrefix))) ||
		(len(rawKey) == len(migrationHLCFloorPrefix)+migrationUint64Bytes && bytes.HasPrefix(rawKey, []byte(migrationHLCFloorPrefix))) ||
		(len(rawKey) == len(migrationPromotePrefix)+migrationUint64Bytes && bytes.HasPrefix(rawKey, []byte(migrationPromotePrefix))) ||
		(len(rawKey) == len(migrationReadyPrefix)+migrationUint64Bytes && bytes.HasPrefix(rawKey, []byte(migrationReadyPrefix)))
}

func encodeMigrationImportAck(ack migrationImportAck) []byte {
	buf := make([]byte, 0, migrationUint64Bytes+binary.MaxVarintLen64+len(ack.cursor))
	buf = binary.BigEndian.AppendUint64(buf, ack.batchSeq)
	buf = binary.AppendUvarint(buf, lenAsUint64(len(ack.cursor)))
	buf = append(buf, ack.cursor...)
	return buf
}

func decodeMigrationImportAck(data []byte) (migrationImportAck, bool) {
	if len(data) < migrationUint64Bytes {
		return migrationImportAck{}, false
	}
	ack := migrationImportAck{batchSeq: binary.BigEndian.Uint64(data[:migrationUint64Bytes])}
	cursorLen, n := binary.Uvarint(data[migrationUint64Bytes:])
	if n <= 0 {
		return migrationImportAck{}, false
	}
	rest := data[migrationUint64Bytes+n:]
	if cursorLen != lenAsUint64(len(rest)) {
		return migrationImportAck{}, false
	}
	ack.cursor = bytes.Clone(rest)
	return ack, true
}

func validateImportVersion(version MVCCVersion) error {
	if version.CommitTS == 0 {
		return errors.New("migration import version has zero commit_ts")
	}
	if version.Tombstone {
		if version.ExpireAt != 0 {
			return errors.New("migration import tombstone carries expire_at")
		}
		if len(version.Value) != 0 {
			return errors.New("migration import tombstone carries value")
		}
		return nil
	}
	return validateValueSize(version.Value)
}

func versionExportSize(key []byte, valueLen int) uint64 {
	return lenAsUint64(len(key)) + lenAsUint64(valueLen) + exportVersionSizeOverhead
}

func lenAsUint64(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64(n) //nolint:gosec // slice lengths are non-negative and bounded by addressable memory.
}

func importBatchMaxTS(versions []MVCCVersion) uint64 {
	var maxTS uint64
	for _, version := range versions {
		if version.CommitTS > maxTS {
			maxTS = version.CommitTS
		}
	}
	return maxTS
}

func validateNextImportBatch(existing migrationImportAck, hasExisting bool, batchSeq uint64) (duplicate bool, err error) {
	if hasExisting {
		if batchSeq <= existing.batchSeq {
			return true, nil
		}
		if batchSeq != existing.batchSeq+1 {
			return false, errors.WithStack(ErrImportBatchGap)
		}
		return false, nil
	}
	if batchSeq != 1 {
		return false, errors.WithStack(ErrImportBatchGap)
	}
	return false, nil
}

func (s *mvccStore) ExportVersions(ctx context.Context, opts ExportVersionsOptions) (ExportVersionsResult, error) {
	pos, err := decodeExportCursor(opts.Cursor)
	if err != nil {
		return ExportVersionsResult{}, err
	}
	if opts.MaxVersions <= 0 {
		return ExportVersionsResult{Done: true}, nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	result := newExportVersionsResult(opts.MaxVersions)
	it := s.tree.Iterator()
	if !s.seekMemoryExportStart(&it, opts.StartKey, pos.key) {
		result.Done = true
		return result, nil
	}

	for ok := true; ok; ok = it.Next() {
		key, ok := it.Key().([]byte)
		if err := checkExportKey(ctx, key, ok, opts.EndKey); err != nil {
			if errors.Is(err, errExportReachedEnd) {
				result.Done = true
				result.NextCursor = nil
				return result, nil
			}
			return ExportVersionsResult{}, err
		}
		if !ok {
			continue
		}
		done, err := exportMemoryIteratorKey(ctx, opts, pos, key, it.Value(), &result)
		if err != nil || !done {
			return result, err
		}
	}
	result.Done = true
	result.NextCursor = nil
	return result, nil
}

var errExportReachedEnd = errors.New("export reached end")
var errExportChunkFull = errors.New("export chunk full")

func checkExportKey(ctx context.Context, key []byte, keyOK bool, end []byte) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if !keyOK {
		return nil
	}
	if end != nil && bytes.Compare(key, end) >= 0 {
		return errExportReachedEnd
	}
	return nil
}

func newExportVersionsResult(maxVersions int) ExportVersionsResult {
	return ExportVersionsResult{
		Versions: make([]MVCCVersion, 0, min(maxVersions, scanResultCapacityLimit)),
	}
}

func (s *mvccStore) seekMemoryExportStart(it *treemap.Iterator, startKey, cursorKey []byte) bool {
	if len(cursorKey) > 0 {
		return seekForwardIteratorStart(s.tree, it, cursorKey)
	}
	return seekForwardIteratorStart(s.tree, it, startKey)
}

func exportMemoryIteratorKey(
	ctx context.Context,
	opts ExportVersionsOptions,
	pos exportCursorPosition,
	key []byte,
	value any,
	result *ExportVersionsResult,
) (bool, error) {
	versions, _ := value.([]VersionedValue)
	cursorCommitTS := uint64(0)
	if len(pos.key) > 0 && bytes.Equal(key, pos.key) {
		cursorCommitTS = pos.commitTS
	}
	return exportMemoryVersionsForKey(ctx, opts, cursorCommitTS, key, versions, result)
}

func finishExportIfLimited(opts ExportVersionsOptions, result *ExportVersionsResult) bool {
	return len(result.Versions) >= opts.MaxVersions ||
		(opts.MaxBytes > 0 && result.ExportedBytes >= opts.MaxBytes) ||
		(opts.MaxScannedBytes > 0 && result.ScannedBytes >= opts.MaxScannedBytes)
}

func appendMemoryExportVersion(opts ExportVersionsOptions, key []byte, version VersionedValue, result *ExportVersionsResult) byte {
	if opts.AcceptKey != nil && !opts.AcceptKey(key) {
		return exportCursorTagScanned
	}
	if opts.MaxCommitTSInclusive != 0 && version.TS > opts.MaxCommitTSInclusive {
		return exportCursorTagScanned
	}
	result.Versions = append(result.Versions, MVCCVersion{
		Key:       bytes.Clone(key),
		CommitTS:  version.TS,
		Tombstone: version.Tombstone,
		Value:     bytes.Clone(version.Value),
		KeyFamily: opts.KeyFamily,
		ExpireAt:  version.ExpireAt,
	})
	result.ExportedBytes += versionExportSize(key, len(version.Value))
	result.AcceptedRows++
	return exportCursorTagEmitted
}

func finishMemoryExportPosition(opts ExportVersionsOptions, key []byte, version VersionedValue, tag byte, result *ExportVersionsResult) bool {
	result.ScannedBytes += versionExportSize(key, len(version.Value))
	result.NextCursor = encodeExportCursor(key, version.TS, tag)
	if finishExportIfLimited(opts, result) {
		result.Done = false
		return false
	}
	return true
}

func shouldSkipMemoryVersion(cursorCommitTS uint64, version VersionedValue) bool {
	return cursorCommitTS != 0 && version.TS >= cursorCommitTS
}

func exportMemoryVersion(opts ExportVersionsOptions, cursorCommitTS uint64, key []byte, version VersionedValue, result *ExportVersionsResult) bool {
	if shouldSkipMemoryVersion(cursorCommitTS, version) {
		return true
	}
	if version.TS <= opts.MinCommitTSExclusive {
		return false
	}
	tag := appendMemoryExportVersion(opts, key, version, result)
	return finishMemoryExportPosition(opts, key, version, tag, result)
}

func exportMemoryVersionsForKey(
	ctx context.Context,
	opts ExportVersionsOptions,
	cursorCommitTS uint64,
	key []byte,
	versions []VersionedValue,
	result *ExportVersionsResult,
) (bool, error) {
	for i := len(versions) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return false, errors.WithStack(err)
		}
		if !exportMemoryVersion(opts, cursorCommitTS, key, versions[i], result) {
			return !finishExportIfLimited(opts, result), nil
		}
		if !result.Done && finishExportIfLimited(opts, result) {
			return false, nil
		}
	}
	return true, nil
}

func (s *mvccStore) ImportVersions(_ context.Context, opts ImportVersionsOptions) (ImportVersionsResult, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	key := string(migrationAckKey(opts.JobID, opts.BracketID))
	existing, hasExisting := s.migrationAcks[key]
	duplicate, err := validateNextImportBatch(existing, hasExisting, opts.BatchSeq)
	if err != nil {
		return ImportVersionsResult{}, err
	}
	if duplicate {
		return ImportVersionsResult{AckedCursor: bytes.Clone(existing.cursor), Duplicate: true}, nil
	}

	for _, version := range opts.Versions {
		if err := validateImportVersion(version); err != nil {
			return ImportVersionsResult{}, err
		}
	}
	for _, version := range opts.Versions {
		if version.Tombstone {
			s.deleteVersionLocked(version.Key, version.CommitTS)
			continue
		}
		s.putVersionLocked(version.Key, version.Value, version.CommitTS, version.ExpireAt)
	}

	batchMax := importBatchMaxTS(opts.Versions)
	if batchMax > s.lastCommitTS {
		s.lastCommitTS = batchMax
	}
	if batchMax > s.migrationHLCFloors[opts.JobID] {
		s.migrationHLCFloors[opts.JobID] = batchMax
	}
	s.migrationAcks[key] = migrationImportAck{batchSeq: opts.BatchSeq, cursor: bytes.Clone(opts.Cursor)}
	return ImportVersionsResult{AckedCursor: bytes.Clone(opts.Cursor), MaxImportedTS: batchMax}, nil
}

func (s *mvccStore) MigrationHLCFloor(_ context.Context, jobID uint64) (uint64, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.migrationHLCFloors[jobID], nil
}
