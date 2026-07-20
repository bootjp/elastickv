package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/emirpasic/gods/maps/treemap"
)

const (
	exportCursorTagEmitted byte = iota
	exportCursorTagScanned
	exportCursorTagPrunedKey
	exportCursorTagSkippedKey

	migrationAckMetaKey      = "_migack"
	migrationHLCFloorMetaKey = "_mighlc"
	migrationPromoteMetaKey  = "_migpromote"
	migrationMetadataVersion = 1

	migrationAckPrefix                 = "!migstage|ack|"
	migrationReadyPrefix               = "!migstage|ready|"
	migrationUint64Bytes               = 8
	migrationAckKeyIDBytes             = 2 * migrationUint64Bytes
	exportVersionSizeOverhead          = 24
	defaultSparseExportMaxScannedBytes = 1 << 20
)

var (
	migrationAckMetaKeyBytes      = []byte(migrationAckMetaKey)
	migrationHLCFloorMetaKeyBytes = []byte(migrationHLCFloorMetaKey)
	migrationPromoteMetaKeyBytes  = []byte(migrationPromoteMetaKey)
)

type exportCursorPosition struct {
	key      []byte
	commitTS uint64
	tag      byte
	hasKey   bool
}

type migrationAckID struct {
	jobID     uint64
	bracketID uint64
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
	if tag != exportCursorTagEmitted &&
		tag != exportCursorTagScanned &&
		tag != exportCursorTagPrunedKey &&
		tag != exportCursorTagSkippedKey {
		return exportCursorPosition{}, errors.WithStack(ErrInvalidExportCursor)
	}
	return exportCursorPosition{key: key, commitTS: commitTS, tag: tag, hasKey: true}, nil
}

// ValidateExportCursorForRange verifies that an export cursor decodes and
// resumes inside the supplied key interval. Skipped-key cursors are accepted
// only when they describe a key outside the interval.
func ValidateExportCursorForRange(cursor, startKey, endKey []byte) error {
	pos, err := decodeExportCursor(cursor)
	if err != nil {
		return err
	}
	return validateExportCursorPositionForRange(pos, startKey, endKey)
}

// ValidatePromotionCursorForRange verifies a promotion cursor before it is
// proposed to Raft. Promotion scans emit only accepted positions, so callers
// must not resume from sparse-scan-only cursor tags.
func ValidatePromotionCursorForRange(cursor, startKey, endKey []byte) error {
	pos, err := decodeExportCursor(cursor)
	if err != nil {
		return err
	}
	if !pos.hasKey {
		return nil
	}
	if pos.tag != exportCursorTagEmitted {
		return errors.WithStack(ErrInvalidExportCursor)
	}
	return validateExportCursorPositionForRange(pos, startKey, endKey)
}

func validateExportCursorPositionForRange(pos exportCursorPosition, startKey, endKey []byte) error {
	if !pos.hasKey {
		return nil
	}
	if pos.tag == exportCursorTagSkippedKey {
		opts := ExportVersionsOptions{StartKey: startKey, EndKey: endKey}
		if !exportSkippedCursorOutsideRange(opts, pos.key) {
			return errors.WithStack(ErrInvalidExportCursor)
		}
		return nil
	}
	if startKey != nil && bytes.Compare(pos.key, startKey) < 0 {
		return errors.WithStack(ErrInvalidExportCursor)
	}
	if endKey != nil && bytes.Compare(pos.key, endKey) >= 0 {
		return errors.WithStack(ErrInvalidExportCursor)
	}
	return nil
}

func decodeExportCursorForOptions(opts ExportVersionsOptions) (exportCursorPosition, error) {
	pos, err := decodeExportCursor(opts.Cursor)
	if err != nil {
		return exportCursorPosition{}, err
	}
	if err := validateExportCursorRange(opts, pos); err != nil {
		return exportCursorPosition{}, err
	}
	return normalizeExportCursorPositionForRange(opts, pos), nil
}

func validateExportCursorRange(opts ExportVersionsOptions, pos exportCursorPosition) error {
	if !pos.hasKey {
		return nil
	}
	if pos.tag == exportCursorTagSkippedKey {
		if !exportSkippedCursorOutsideRange(opts, pos.key) {
			return errors.WithStack(ErrInvalidExportCursor)
		}
		return nil
	}
	if opts.StartKey != nil && bytes.Compare(pos.key, opts.StartKey) < 0 {
		return errors.WithStack(ErrInvalidExportCursor)
	}
	if opts.EndKey != nil && bytes.Compare(pos.key, opts.EndKey) >= 0 {
		return errors.WithStack(ErrInvalidExportCursor)
	}
	return nil
}

func exportSkippedCursorOutsideRange(opts ExportVersionsOptions, key []byte) bool {
	return (opts.StartKey != nil && bytes.Compare(key, opts.StartKey) < 0) ||
		(opts.EndKey != nil && bytes.Compare(key, opts.EndKey) >= 0)
}

func normalizeExportCursorPositionForRange(opts ExportVersionsOptions, pos exportCursorPosition) exportCursorPosition {
	if !pos.hasKey || pos.tag != exportCursorTagSkippedKey || opts.StartKey == nil {
		return pos
	}
	if bytes.Compare(pos.key, opts.StartKey) >= 0 {
		return pos
	}
	return exportCursorPosition{}
}

func normalizeExportVersionsOptions(opts ExportVersionsOptions) ExportVersionsOptions {
	if opts.EndKey != nil && len(opts.EndKey) == 0 {
		opts.EndKey = nil
	}
	if exportUsesSparseScanBudget(opts) && opts.MaxScannedBytes == 0 {
		opts.MaxScannedBytes = defaultSparseExportMaxScannedBytes
	}
	return opts
}

func exportUsesSparseScanBudget(opts ExportVersionsOptions) bool {
	return opts.AcceptKey != nil ||
		opts.AcceptVersion != nil ||
		opts.MaxCommitTSInclusive != 0 ||
		opts.MinCommitTSExclusive != 0 ||
		opts.StartKey != nil ||
		opts.EndKey != nil
}

func migrationReadyKey(jobID uint64) []byte {
	key := make([]byte, len(migrationReadyPrefix)+migrationUint64Bytes)
	copy(key, migrationReadyPrefix)
	binary.BigEndian.PutUint64(key[len(migrationReadyPrefix):], jobID)
	return key
}

func isMigrationMetadataKey(rawKey []byte) bool {
	return bytes.Equal(rawKey, migrationAckMetaKeyBytes) ||
		bytes.Equal(rawKey, migrationHLCFloorMetaKeyBytes) ||
		bytes.Equal(rawKey, migrationPromoteMetaKeyBytes) ||
		(len(rawKey) == len(migrationReadyPrefix)+migrationUint64Bytes && bytes.HasPrefix(rawKey, []byte(migrationReadyPrefix)))
}

func encodeMigrationImportAcks(acks map[migrationAckID]migrationImportAck) []byte {
	ids := make([]migrationAckID, 0, len(acks))
	for id := range acks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].jobID != ids[j].jobID {
			return ids[i].jobID < ids[j].jobID
		}
		return ids[i].bracketID < ids[j].bracketID
	})

	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(ids)*(3*migrationUint64Bytes+binary.MaxVarintLen64))
	buf = append(buf, migrationMetadataVersion)
	buf = binary.AppendUvarint(buf, lenAsUint64(len(ids)))
	for _, id := range ids {
		ack := acks[id]
		buf = binary.BigEndian.AppendUint64(buf, id.jobID)
		buf = binary.BigEndian.AppendUint64(buf, id.bracketID)
		buf = binary.BigEndian.AppendUint64(buf, ack.batchSeq)
		buf = binary.AppendUvarint(buf, lenAsUint64(len(ack.cursor)))
		buf = append(buf, ack.cursor...)
	}
	return buf
}

func decodeMigrationImportAcks(data []byte) (map[migrationAckID]migrationImportAck, bool) {
	if len(data) == 0 || data[0] != migrationMetadataVersion {
		return nil, false
	}
	rest := data[1:]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, false
	}
	rest = rest[n:]
	acks := make(map[migrationAckID]migrationImportAck)
	for i := uint64(0); i < count; i++ {
		if len(rest) < 3*migrationUint64Bytes {
			return nil, false
		}
		id := migrationAckID{
			jobID:     binary.BigEndian.Uint64(rest[:migrationUint64Bytes]),
			bracketID: binary.BigEndian.Uint64(rest[migrationUint64Bytes : 2*migrationUint64Bytes]),
		}
		ack := migrationImportAck{batchSeq: binary.BigEndian.Uint64(rest[2*migrationUint64Bytes : 3*migrationUint64Bytes])}
		rest = rest[3*migrationUint64Bytes:]
		cursorLen, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, false
		}
		rest = rest[n:]
		if cursorLen > lenAsUint64(len(rest)) {
			return nil, false
		}
		ack.cursor = bytes.Clone(rest[:cursorLen])
		rest = rest[cursorLen:]
		acks[id] = ack
	}
	return acks, len(rest) == 0
}

func encodeMigrationHLCFloors(floors map[uint64]uint64) []byte {
	jobIDs := make([]uint64, 0, len(floors))
	for jobID := range floors {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Slice(jobIDs, func(i, j int) bool { return jobIDs[i] < jobIDs[j] })

	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(jobIDs)*2*migrationUint64Bytes)
	buf = append(buf, migrationMetadataVersion)
	buf = binary.AppendUvarint(buf, lenAsUint64(len(jobIDs)))
	for _, jobID := range jobIDs {
		buf = binary.BigEndian.AppendUint64(buf, jobID)
		buf = binary.BigEndian.AppendUint64(buf, floors[jobID])
	}
	return buf
}

func decodeMigrationHLCFloors(data []byte) (map[uint64]uint64, bool) {
	if len(data) == 0 || data[0] != migrationMetadataVersion {
		return nil, false
	}
	rest := data[1:]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, false
	}
	rest = rest[n:]
	floors := make(map[uint64]uint64)
	for i := uint64(0); i < count; i++ {
		if len(rest) < 2*migrationUint64Bytes {
			return nil, false
		}
		jobID := binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
		floor := binary.BigEndian.Uint64(rest[migrationUint64Bytes : 2*migrationUint64Bytes])
		rest = rest[2*migrationUint64Bytes:]
		floors[jobID] = floor
	}
	return floors, len(rest) == 0
}

func encodeMigrationPromotionStates(states map[uint64]PromotionState) []byte {
	jobIDs := make([]uint64, 0, len(states))
	for jobID := range states {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Slice(jobIDs, func(i, j int) bool { return jobIDs[i] < jobIDs[j] })

	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(jobIDs)*(migrationUint64Bytes+binary.MaxVarintLen64))
	buf = append(buf, migrationMetadataVersion)
	buf = binary.AppendUvarint(buf, lenAsUint64(len(jobIDs)))
	for _, jobID := range jobIDs {
		encoded := encodePromotionState(states[jobID])
		buf = binary.BigEndian.AppendUint64(buf, jobID)
		buf = binary.AppendUvarint(buf, lenAsUint64(len(encoded)))
		buf = append(buf, encoded...)
	}
	return buf
}

func decodeMigrationPromotionStates(data []byte) (map[uint64]PromotionState, bool) {
	if len(data) == 0 || data[0] != migrationMetadataVersion {
		return nil, false
	}
	rest := data[1:]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, false
	}
	rest = rest[n:]
	states := make(map[uint64]PromotionState)
	for i := uint64(0); i < count; i++ {
		if len(rest) < migrationUint64Bytes {
			return nil, false
		}
		jobID := binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
		rest = rest[migrationUint64Bytes:]
		stateLen, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, false
		}
		rest = rest[n:]
		if stateLen > lenAsUint64(len(rest)) {
			return nil, false
		}
		stateEnd := int(stateLen) //nolint:gosec // bounded by len(rest) above.
		state, ok := decodePromotionState(rest[:stateEnd])
		if !ok {
			return nil, false
		}
		states[jobID] = state
		rest = rest[stateEnd:]
	}
	return states, len(rest) == 0
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
	opts = normalizeExportVersionsOptions(opts)
	pos, err := decodeExportCursorForOptions(opts)
	if err != nil {
		return ExportVersionsResult{}, err
	}
	if opts.MaxVersions <= 0 {
		return ExportVersionsResult{Done: true}, nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if err := s.checkExportReadTSLocked(opts); err != nil {
		return ExportVersionsResult{}, err
	}

	return s.exportVersionsLocked(ctx, opts, pos)
}

func (s *mvccStore) exportVersionsLocked(ctx context.Context, opts ExportVersionsOptions, pos exportCursorPosition) (ExportVersionsResult, error) {
	result := newExportVersionsResult(opts.MaxVersions)
	it := s.tree.Iterator()
	if !s.seekMemoryExportStart(&it, opts.StartKey, pos) {
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

func (s *mvccStore) checkExportReadTSLocked(opts ExportVersionsOptions) error {
	if readTSCompacted(exportRetentionReadTS(opts), s.minRetainedTS) {
		return ErrReadTSCompacted
	}
	return nil
}

func exportRetentionReadTS(opts ExportVersionsOptions) uint64 {
	if opts.ReadTS != 0 {
		return opts.ReadTS
	}
	return opts.MaxCommitTSInclusive
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

func (s *mvccStore) seekMemoryExportStart(it *treemap.Iterator, startKey []byte, pos exportCursorPosition) bool {
	if pos.hasKey {
		return seekForwardIteratorStart(s.tree, it, pos.key)
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
	if pos.hasKey && pos.tag == exportCursorTagPrunedKey && bytes.Equal(key, pos.key) {
		return true, nil
	}
	cursorCommitTS := uint64(0)
	if pos.hasKey && bytes.Equal(key, pos.key) {
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
	if shouldSkipMigrationExportKey(key) {
		return exportCursorTagScanned
	}
	if opts.AcceptKey != nil && !opts.AcceptKey(key) {
		return exportCursorTagScanned
	}
<<<<<<< HEAD
	if opts.AcceptVersion != nil && !opts.AcceptVersion(key, version.Value) {
		return exportCursorTagScanned
	}
	if opts.MaxCommitTSInclusive != 0 && version.TS > opts.MaxCommitTSInclusive {
=======
	if opts.MaxCommitTSInclusive != 0 && version.TS > opts.MaxCommitTSInclusive {
		return exportCursorTagScanned
	}
	if opts.AcceptVersion != nil && !opts.AcceptVersion(key, version.Value) {
>>>>>>> origin/design/hotspot-split-m2-promotion-complete
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

func shouldSkipMigrationExportKey(key []byte) bool {
	return bytes.HasPrefix(key, txnLockKeyPrefix)
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
		if shouldSkipMemoryVersion(cursorCommitTS, versions[i]) {
			continue
		}
		if versions[i].TS <= opts.MinCommitTSExclusive {
			if !finishMemoryExportPosition(opts, key, versions[i], exportCursorTagPrunedKey, result) {
				return false, nil
			}
			return true, nil
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

	id := migrationAckID{jobID: opts.JobID, bracketID: opts.BracketID}
	existing, hasExisting := s.migrationAcks[id]
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
	s.migrationAcks[id] = migrationImportAck{batchSeq: opts.BatchSeq, cursor: bytes.Clone(opts.Cursor)}
	return ImportVersionsResult{AckedCursor: bytes.Clone(opts.Cursor), MaxImportedTS: batchMax}, nil
}

func (s *mvccStore) ImportVersionsRaft(ctx context.Context, opts ImportVersionsOptions) (ImportVersionsResult, error) {
	return s.ImportVersions(ctx, opts)
}

func (s *mvccStore) MigrationHLCFloor(_ context.Context, jobID uint64) (uint64, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.migrationHLCFloors[jobID], nil
}
