package store

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const migrationPromotionDoneFlag byte = 1

type promotedVersion struct {
	staged MVCCVersion
	target MVCCVersion
}

func validatePromoteVersionsOptions(opts PromoteVersionsOptions) error {
	if opts.TargetKey == nil {
		return errors.New("migration promote target key mapper is required")
	}
	return nil
}

func promotedVersionsFromStaged(opts PromoteVersionsOptions, versions []MVCCVersion) ([]promotedVersion, PromoteVersionsResult, error) {
	out := make([]promotedVersion, 0, len(versions))
	result := PromoteVersionsResult{PromotedRows: uint64(len(versions))} //nolint:gosec // len is bounded by MaxVersions.
	for _, staged := range versions {
		targetKey, ok := opts.TargetKey(staged.Key)
		if !ok {
			return nil, PromoteVersionsResult{}, errors.WithStack(errors.Newf("migration promote target key rejected staged key %q", string(staged.Key)))
		}
		target := MVCCVersion{
			Key:       bytes.Clone(targetKey),
			CommitTS:  staged.CommitTS,
			Tombstone: staged.Tombstone,
			Value:     bytes.Clone(staged.Value),
			KeyFamily: staged.KeyFamily,
			ExpireAt:  staged.ExpireAt,
		}
		if err := validateImportVersion(target); err != nil {
			return nil, PromoteVersionsResult{}, err
		}
		result.PromotedBytes += versionExportSize(target.Key, len(target.Value))
		if target.CommitTS > result.MaxPromotedTS {
			result.MaxPromotedTS = target.CommitTS
		}
		out = append(out, promotedVersion{
			staged: staged,
			target: target,
		})
	}
	return out, result, nil
}

func (s *mvccStore) PromoteVersions(ctx context.Context, opts PromoteVersionsOptions) (PromoteVersionsResult, error) {
	if err := validatePromoteVersionsOptions(opts); err != nil {
		return PromoteVersionsResult{}, err
	}
	if opts.MaxVersions <= 0 {
		return PromoteVersionsResult{Done: true}, nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	state, cursor := s.promotionStateAndCursorLocked(opts)
	if opts.JobID != 0 && state.Done {
		return PromoteVersionsResult{Done: true, TotalPromotedRows: state.PromotedRows}, nil
	}
	exported, toPromote, promoted, err := s.planMemoryPromotionLocked(ctx, opts, cursor)
	if err != nil {
		return PromoteVersionsResult{}, err
	}
	s.applyMemoryPromotionLocked(toPromote)
	if promoted.MaxPromotedTS > s.lastCommitTS {
		s.lastCommitTS = promoted.MaxPromotedTS
	}
	result, updatedState := finishPromotionResult(opts, state, exported, promoted)
	if updatedState != nil {
		s.migrationPromotions[opts.JobID] = clonePromotionState(*updatedState)
	}
	return result, nil
}

func (s *mvccStore) planMemoryPromotionLocked(
	ctx context.Context,
	opts PromoteVersionsOptions,
	cursor []byte,
) (ExportVersionsResult, []promotedVersion, PromoteVersionsResult, error) {
	pos, err := decodeExportCursor(cursor)
	if err != nil {
		return ExportVersionsResult{}, nil, PromoteVersionsResult{}, err
	}
	opts.Cursor = cursor
	exported, err := s.exportMemoryVersionsLocked(ctx, opts, pos)
	if err != nil {
		return ExportVersionsResult{}, nil, PromoteVersionsResult{}, err
	}
	toPromote, promoted, err := promotedVersionsFromStaged(opts, exported.Versions)
	return exported, toPromote, promoted, err
}

func (s *mvccStore) applyMemoryPromotionLocked(toPromote []promotedVersion) {
	for _, version := range toPromote {
		if version.target.Tombstone {
			s.deleteVersionLocked(version.target.Key, version.target.CommitTS)
		} else {
			s.putVersionLocked(version.target.Key, version.target.Value, version.target.CommitTS, version.target.ExpireAt)
		}
		s.removeVersionLocked(version.staged.Key, version.staged.CommitTS)
	}
}

func (s *mvccStore) promotionStateAndCursorLocked(opts PromoteVersionsOptions) (PromotionState, []byte) {
	if opts.JobID == 0 {
		return PromotionState{}, opts.Cursor
	}
	state := clonePromotionState(s.migrationPromotions[opts.JobID])
	if len(state.Cursor) == 0 {
		return state, opts.Cursor
	}
	return state, state.Cursor
}

func (s *mvccStore) MigrationPromotionState(_ context.Context, jobID uint64) (PromotionState, bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	state, ok := s.migrationPromotions[jobID]
	return clonePromotionState(state), ok, nil
}

func (s *mvccStore) exportMemoryVersionsLocked(ctx context.Context, opts PromoteVersionsOptions, pos exportCursorPosition) (ExportVersionsResult, error) {
	exportOpts := ExportVersionsOptions{
		StartKey:        opts.StartKey,
		EndKey:          opts.EndKey,
		Cursor:          opts.Cursor,
		MaxVersions:     opts.MaxVersions,
		MaxBytes:        opts.MaxBytes,
		MaxScannedBytes: opts.MaxScannedBytes,
	}
	result := newExportVersionsResult(opts.MaxVersions)
	it := s.tree.Iterator()
	if !s.seekMemoryExportStart(&it, opts.StartKey, pos.key) {
		result.Done = true
		return result, nil
	}
	for ok := true; ok; ok = it.Next() {
		key, keyOK := it.Key().([]byte)
		if err := checkExportKey(ctx, key, keyOK, opts.EndKey); err != nil {
			if errors.Is(err, errExportReachedEnd) {
				result.Done = true
				result.NextCursor = nil
				return result, nil
			}
			return ExportVersionsResult{}, err
		}
		if !keyOK {
			continue
		}
		done, err := exportMemoryIteratorKey(ctx, exportOpts, pos, key, it.Value(), &result)
		if err != nil || !done {
			return result, err
		}
	}
	result.Done = true
	result.NextCursor = nil
	return result, nil
}

func (s *mvccStore) removeVersionLocked(key []byte, commitTS uint64) bool {
	existing, ok := s.tree.Get(key)
	if !ok {
		return false
	}
	versions, _ := existing.([]VersionedValue)
	idx := findVersionIndex(versions, commitTS)
	if idx < 0 {
		return false
	}
	next := make([]VersionedValue, len(versions)-1)
	copy(next, versions[:idx])
	copy(next[idx:], versions[idx+1:])
	if len(next) == 0 {
		s.tree.Remove(key)
		return true
	}
	s.tree.Put(bytes.Clone(key), next)
	return true
}

func findVersionIndex(versions []VersionedValue, commitTS uint64) int {
	for i := range versions {
		if versions[i].TS == commitTS {
			return i
		}
	}
	return -1
}

func (s *pebbleStore) PromoteVersions(ctx context.Context, opts PromoteVersionsOptions) (PromoteVersionsResult, error) {
	if err := validatePromoteVersionsOptions(opts); err != nil {
		return PromoteVersionsResult{}, err
	}
	if opts.MaxVersions <= 0 {
		return PromoteVersionsResult{Done: true}, nil
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	state, cursor, err := s.pebblePromotionStateAndCursor(opts)
	if err != nil {
		return PromoteVersionsResult{}, err
	}
	if opts.JobID != 0 && state.Done {
		return PromoteVersionsResult{Done: true, TotalPromotedRows: state.PromotedRows}, nil
	}
	opts.Cursor = cursor
	exported, toPromote, promoted, err := s.planPebblePromotionLocked(ctx, opts)
	if err != nil {
		return PromoteVersionsResult{}, err
	}
	result, stateToWrite := finishPromotionResult(opts, state, exported, promoted)
	return s.finishPebblePromotion(toPromote, opts.JobID, stateToWrite, result, opts.AppliedIndex)
}

func (s *pebbleStore) finishPebblePromotion(
	toPromote []promotedVersion,
	jobID uint64,
	stateToWrite *PromotionState,
	result PromoteVersionsResult,
	appliedIndex uint64,
) (PromoteVersionsResult, error) {
	if len(toPromote) == 0 && stateToWrite == nil {
		return result, nil
	}
	if err := s.commitPebblePromoteVersions(toPromote, jobID, stateToWrite, appliedIndex); err != nil {
		return PromoteVersionsResult{}, err
	}
	return result, nil
}

func (s *pebbleStore) planPebblePromotionLocked(
	ctx context.Context,
	opts PromoteVersionsOptions,
) (ExportVersionsResult, []promotedVersion, PromoteVersionsResult, error) {
	exported, err := s.exportVersionsLocked(ctx, ExportVersionsOptions{
		StartKey:        opts.StartKey,
		EndKey:          opts.EndKey,
		Cursor:          opts.Cursor,
		MaxVersions:     opts.MaxVersions,
		MaxBytes:        opts.MaxBytes,
		MaxScannedBytes: opts.MaxScannedBytes,
	})
	if err != nil {
		return ExportVersionsResult{}, nil, PromoteVersionsResult{}, err
	}
	toPromote, promoted, err := promotedVersionsFromStaged(opts, exported.Versions)
	return exported, toPromote, promoted, err
}

func finishPromotionResult(
	opts PromoteVersionsOptions,
	state PromotionState,
	exported ExportVersionsResult,
	promoted PromoteVersionsResult,
) (PromoteVersionsResult, *PromotionState) {
	promoted.NextCursor = exported.NextCursor
	promoted.Done = exported.Done
	promoted.ScannedBytes = exported.ScannedBytes
	promoted.TotalPromotedRows = promoted.PromotedRows
	if opts.JobID == 0 {
		return promoted, nil
	}
	state.Cursor = bytes.Clone(exported.NextCursor)
	state.Done = exported.Done
	state.PromotedRows += promoted.PromotedRows
	state.LastError = ""
	promoted.TotalPromotedRows = state.PromotedRows
	return promoted, &state
}

func (s *pebbleStore) pebblePromotionStateAndCursor(opts PromoteVersionsOptions) (PromotionState, []byte, error) {
	if opts.JobID == 0 {
		return PromotionState{}, opts.Cursor, nil
	}
	state, ok, err := s.readPebblePromotionState(opts.JobID)
	if err != nil {
		return PromotionState{}, nil, err
	}
	if !ok || len(state.Cursor) == 0 {
		return state, opts.Cursor, nil
	}
	return state, state.Cursor, nil
}

func (s *pebbleStore) MigrationPromotionState(_ context.Context, jobID uint64) (PromotionState, bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.readPebblePromotionState(jobID)
}

func (s *pebbleStore) readPebblePromotionState(jobID uint64) (PromotionState, bool, error) {
	val, closer, err := s.db.Get(migrationPromoteKey(jobID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return PromotionState{}, false, nil
		}
		return PromotionState{}, false, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	state, ok := decodePromotionState(val)
	if !ok {
		return PromotionState{}, false, errors.New("corrupt migration promotion state")
	}
	return state, true, nil
}

func (s *pebbleStore) commitPebblePromoteVersions(versions []promotedVersion, jobID uint64, state *PromotionState, appliedIndex uint64) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	targets := make([]MVCCVersion, 0, len(versions))
	for _, version := range versions {
		targets = append(targets, version.target)
	}
	if err := s.applyImportVersionsBatch(batch, targets); err != nil {
		return err
	}
	for _, version := range versions {
		if err := batch.Delete(encodeKey(version.staged.Key, version.staged.CommitTS), nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if state != nil {
		if err := batch.Set(migrationPromoteKey(jobID), encodePromotionState(*state), nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if appliedIndex > 0 {
		if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
			return err
		}
	}
	if err := batch.Commit(s.directApplyWriteOpts()); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func encodePromotionState(state PromotionState) []byte {
	buf := make([]byte, 0, 1+migrationUint64Bytes+binary.MaxVarintLen64*2+len(state.Cursor)+len(state.LastError))
	if state.Done {
		buf = append(buf, migrationPromotionDoneFlag)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.BigEndian.AppendUint64(buf, state.PromotedRows)
	buf = binary.AppendUvarint(buf, lenAsUint64(len(state.Cursor)))
	buf = append(buf, state.Cursor...)
	buf = binary.AppendUvarint(buf, lenAsUint64(len(state.LastError)))
	buf = append(buf, state.LastError...)
	return buf
}

func decodePromotionState(data []byte) (PromotionState, bool) {
	if len(data) < 1+migrationUint64Bytes {
		return PromotionState{}, false
	}
	state := PromotionState{
		Done:         data[0]&migrationPromotionDoneFlag != 0,
		PromotedRows: binary.BigEndian.Uint64(data[1 : 1+migrationUint64Bytes]),
	}
	rest := data[1+migrationUint64Bytes:]
	cursorLen, n := binary.Uvarint(rest)
	if n <= 0 || cursorLen > lenAsUint64(len(rest[n:])) {
		return PromotionState{}, false
	}
	rest = rest[n:]
	cursorEnd := int(cursorLen) //nolint:gosec // bounded by len(rest) above.
	state.Cursor = bytes.Clone(rest[:cursorEnd])
	rest = rest[cursorEnd:]
	errLen, n := binary.Uvarint(rest)
	if n <= 0 || errLen != lenAsUint64(len(rest[n:])) {
		return PromotionState{}, false
	}
	state.LastError = string(rest[n:])
	return state, true
}

func clonePromotionState(state PromotionState) PromotionState {
	state.Cursor = bytes.Clone(state.Cursor)
	return state
}

var _ MigrationPromoter = (*mvccStore)(nil)
var _ MigrationPromoter = (*pebbleStore)(nil)
var _ MigrationPromotionStateReader = (*mvccStore)(nil)
var _ MigrationPromotionStateReader = (*pebbleStore)(nil)
