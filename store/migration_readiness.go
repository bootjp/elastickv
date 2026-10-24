package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const (
	targetReadinessCodecVersionV1    byte = 1
	targetReadinessCodecVersion      byte = 2
	targetReadinessGuardCodecVersion byte = 3
	targetReadinessArmedFlag         byte = 1
	targetReadinessSourceWrite       byte = 1
	targetReadinessSourceRead        byte = 2
	targetReadinessTrackWrites       byte = 4
	targetReadinessTransferSizeGuard byte = 8
)

// MigrationTransferSizeGuardVersion is independent of the ownership protocol
// and does not change the admission contract of legacy write trackers.
const MigrationTransferSizeGuardVersion uint32 = 1

// invalidReadinessState marks a validation verdict so apply can tell it apart
// from a replica-local persistence failure: the former is a pure function of
// the request and must stay an ordinary rejection, the latter must halt.
func invalidReadinessState(msg string) error {
	return errors.WithStack(errors.Mark(errors.New(msg), ErrInvalidReadinessState))
}

func validateTargetStagedReadinessState(state TargetStagedReadinessState) error {
	switch {
	case state.JobID == 0:
		return invalidReadinessState("target staged readiness job_id is required")
	case state.MigrationJobID == 0:
		return invalidReadinessState("target staged readiness migration_job_id is required")
	case state.Armed && state.MinWriteTSExclusive == 0:
		return invalidReadinessState("target staged readiness min_write_ts_exclusive is required when armed")
	case state.RouteEnd != nil && bytes.Compare(state.RouteStart, state.RouteEnd) >= 0:
		return invalidReadinessState("target staged readiness route range is invalid")
	}
	return validateSourceMigrationControlState(state)
}

func validateSourceMigrationControlState(state TargetStagedReadinessState) error {
	if err := validateMigrationTransferSizeGuard(state); err != nil {
		return err
	}
	if state.SourceReadFence && !state.SourceWriteFence {
		return invalidReadinessState("source read fence requires source write fence")
	}
	if (state.SourceWriteFence || state.SourceReadFence) && state.RetentionPinTS == 0 {
		return invalidReadinessState("source migration control retention pin is required")
	}
	if state.TrackWrites && state.RetentionPinTS == 0 {
		return invalidReadinessState("migration write tracker retention pin is required")
	}
	return nil
}

func validateMigrationTransferSizeGuard(state TargetStagedReadinessState) error {
	if state.TransferSizeGuardVersion != 0 &&
		(state.TransferSizeGuardVersion != MigrationTransferSizeGuardVersion || !state.Armed || !state.TrackWrites) {
		return invalidReadinessState("transfer size guard requires supported version and armed source tracker")
	}
	return nil
}

func (s *mvccStore) ApplyTargetStagedReadiness(_ context.Context, state TargetStagedReadinessState) error {
	if err := validateTargetStagedReadinessState(state); err != nil {
		return err
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if _, retired := s.migrationRetired[state.JobID]; retired {
		return errors.WithStack(ErrMigrationRetired)
	}
	cloned := cloneTargetStagedReadinessState(state)
	s.migrationReadiness[state.JobID] = cloned
	s.migrationReadinessCache = upsertTargetStagedReadinessStateCache(s.migrationReadinessCache, cloned)
	if state.Armed {
		s.lastCommitTS = max(s.lastCommitTS, state.MinWriteTSExclusive)
		s.migrationHLCFloors[state.JobID] = max(s.migrationHLCFloors[state.JobID], state.MinWriteTSExclusive)
	}
	return nil
}

func (s *mvccStore) ApplyTargetStagedReadinessAt(ctx context.Context, state TargetStagedReadinessState, _ uint64) error {
	return s.ApplyTargetStagedReadiness(ctx, state)
}

func (s *mvccStore) MigrationTargetReadinessStates(_ context.Context) ([]TargetStagedReadinessState, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return cloneTargetStagedReadinessStates(s.migrationReadinessCache), nil
}

func (s *pebbleStore) ApplyTargetStagedReadiness(_ context.Context, state TargetStagedReadinessState) error {
	return s.applyTargetStagedReadiness(state, 0, false)
}

func (s *pebbleStore) ApplyTargetStagedReadinessAt(_ context.Context, state TargetStagedReadinessState, appliedIndex uint64) error {
	return s.applyTargetStagedReadiness(state, appliedIndex, true)
}

func (s *pebbleStore) applyTargetStagedReadiness(state TargetStagedReadinessState, appliedIndex uint64, raftApply bool) error {
	if err := validateTargetStagedReadinessState(state); err != nil {
		return err
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := s.checkMigrationNotRetiredLocked(state.JobID); err != nil {
		return err
	}

	writeAppliedIndex, err := s.shouldWriteAppliedIndexLocked(appliedIndex)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := stageTargetReadinessGuard(batch, state, appliedIndex, writeAppliedIndex); err != nil {
		return err
	}

	// The clock floor must survive the same crash boundary as the guard and
	// applied index, even when the moved range contains no imported versions.
	floor := uint64(0)
	if state.Armed {
		floor = state.MinWriteTSExclusive
	}
	unlock, lastTS, err := s.stageMigrationClockMetadata(batch, state.JobID, floor)
	if err != nil {
		return err
	}
	defer unlock()
	writeOpts := s.directApplyWriteOpts()
	if raftApply {
		writeOpts = s.raftApplyWriteOpts()
	}
	if err := batch.Commit(writeOpts); err != nil {
		return errors.WithStack(err)
	}
	s.lastCommitTS = lastTS
	s.migrationReadinessCache = upsertTargetStagedReadinessStateCache(s.migrationReadinessCache, state)
	return nil
}

func stageTargetReadinessGuard(batch *pebble.Batch, state TargetStagedReadinessState, appliedIndex uint64, writeAppliedIndex bool) error {
	if err := batch.Set(migrationReadyKey(state.JobID), encodeTargetStagedReadinessState(state), nil); err != nil {
		return errors.WithStack(err)
	}
	if writeAppliedIndex {
		return setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex)
	}
	return nil
}

func (s *pebbleStore) MigrationTargetReadinessStates(_ context.Context) ([]TargetStagedReadinessState, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return cloneTargetStagedReadinessStates(s.migrationReadinessCache), nil
}

func (s *pebbleStore) loadPebbleTargetReadinessStatesFromDB() ([]TargetStagedReadinessState, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(migrationReadyPrefix),
		UpperBound: PrefixScanEnd([]byte(migrationReadyPrefix)),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	var out []TargetStagedReadinessState
	for valid := iter.First(); valid; valid = iter.Next() {
		if !isTargetReadinessRecordKey(iter.Key()) {
			continue
		}
		state, ok := decodeTargetStagedReadinessState(iter.Value())
		if !ok {
			return nil, errors.New("corrupt target staged readiness state")
		}
		out = append(out, state)
	}
	if err := iter.Error(); err != nil {
		return nil, errors.WithStack(err)
	}
	sortTargetStagedReadinessStates(out)
	return out, nil
}

func isTargetReadinessRecordKey(rawKey []byte) bool {
	return len(rawKey) == len(migrationReadyPrefix)+migrationUint64Bytes &&
		bytes.HasPrefix(rawKey, []byte(migrationReadyPrefix))
}

func encodeTargetStagedReadinessState(state TargetStagedReadinessState) []byte {
	buf := make([]byte, 0, targetReadinessEncodedSize(state))
	buf = append(buf, targetReadinessCodecVersion)
	if state.TransferSizeGuardVersion != 0 {
		buf[0] = targetReadinessGuardCodecVersion
	}
	if state.Armed {
		buf = append(buf, targetReadinessArmedFlag)
	} else {
		buf = append(buf, 0)
	}
	var sourceFlags byte
	if state.SourceWriteFence {
		sourceFlags |= targetReadinessSourceWrite
	}
	if state.SourceReadFence {
		sourceFlags |= targetReadinessSourceRead
	}
	if state.TrackWrites {
		sourceFlags |= targetReadinessTrackWrites
	}
	if state.TransferSizeGuardVersion == MigrationTransferSizeGuardVersion {
		sourceFlags |= targetReadinessTransferSizeGuard
	}
	buf = append(buf, sourceFlags)
	buf = binary.BigEndian.AppendUint64(buf, state.JobID)
	buf = binary.BigEndian.AppendUint64(buf, state.ExpectedCutoverVersion)
	buf = binary.BigEndian.AppendUint64(buf, state.MigrationJobID)
	buf = binary.BigEndian.AppendUint64(buf, state.MinWriteTSExclusive)
	buf = binary.BigEndian.AppendUint64(buf, state.RetentionPinTS)
	buf = binary.BigEndian.AppendUint64(buf, state.MinAdmittedTS)
	buf = appendVarBytes(buf, state.RouteStart)
	if state.RouteEnd == nil {
		buf = append(buf, 0)
		return buf
	}
	buf = append(buf, 1)
	return appendVarBytes(buf, state.RouteEnd)
}

func decodeTargetStagedReadinessState(data []byte) (TargetStagedReadinessState, bool) {
	state, rest, ok := decodeTargetReadinessHeader(data)
	if !ok {
		return TargetStagedReadinessState{}, false
	}
	if !decodeTargetReadinessRange(&state, rest) {
		return TargetStagedReadinessState{}, false
	}
	if err := validateTargetStagedReadinessState(state); err != nil {
		return TargetStagedReadinessState{}, false
	}
	return state, true
}

func decodeTargetReadinessHeader(data []byte) (TargetStagedReadinessState, []byte, bool) {
	if len(data) == 0 {
		return TargetStagedReadinessState{}, nil, false
	}
	if data[0] == targetReadinessCodecVersionV1 {
		return decodeTargetReadinessHeaderV1(data)
	}
	if len(data) < 3+6*migrationUint64Bytes || (data[0] != targetReadinessCodecVersion && data[0] != targetReadinessGuardCodecVersion) {
		return TargetStagedReadinessState{}, nil, false
	}
	if data[1] != 0 && data[1] != targetReadinessArmedFlag {
		return TargetStagedReadinessState{}, nil, false
	}
	sourceFlags := data[2]
	if !validTargetReadinessSourceFlags(data[0], sourceFlags) {
		return TargetStagedReadinessState{}, nil, false
	}
	state := TargetStagedReadinessState{
		Armed:            data[1] == targetReadinessArmedFlag,
		SourceWriteFence: sourceFlags&targetReadinessSourceWrite != 0,
		SourceReadFence:  sourceFlags&targetReadinessSourceRead != 0,
		TrackWrites:      sourceFlags&targetReadinessTrackWrites != 0,
	}
	if sourceFlags&targetReadinessTransferSizeGuard != 0 {
		state.TransferSizeGuardVersion = MigrationTransferSizeGuardVersion
	}
	rest := data[3:]
	state.JobID = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.ExpectedCutoverVersion = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.MigrationJobID = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.MinWriteTSExclusive = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.RetentionPinTS = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.MinAdmittedTS = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	return state, rest, true
}

func validTargetReadinessSourceFlags(version, sourceFlags byte) bool {
	allowedFlags := targetReadinessSourceWrite | targetReadinessSourceRead | targetReadinessTrackWrites
	if version == targetReadinessGuardCodecVersion {
		allowedFlags |= targetReadinessTransferSizeGuard
		if sourceFlags&targetReadinessTransferSizeGuard == 0 {
			return false
		}
	}
	return sourceFlags & ^allowedFlags == 0
}

func decodeTargetReadinessHeaderV1(data []byte) (TargetStagedReadinessState, []byte, bool) {
	if len(data) < 2+4*migrationUint64Bytes || data[1] != 0 && data[1] != targetReadinessArmedFlag {
		return TargetStagedReadinessState{}, nil, false
	}
	state := TargetStagedReadinessState{Armed: data[1] == targetReadinessArmedFlag}
	rest := data[2:]
	state.JobID = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.ExpectedCutoverVersion = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.MigrationJobID = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	state.MinWriteTSExclusive = binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
	rest = rest[migrationUint64Bytes:]
	return state, rest, true
}

func decodeTargetReadinessRange(state *TargetStagedReadinessState, data []byte) bool {
	routeStart, rest, ok := readVarBytes(data)
	if !ok || len(rest) == 0 {
		return false
	}
	state.RouteStart = routeStart
	endPresent := rest[0]
	rest = rest[1:]
	if endPresent == 0 {
		return len(rest) == 0
	}
	if endPresent != 1 {
		return false
	}
	state.RouteEnd, rest, ok = readVarBytes(rest)
	return ok && len(rest) == 0
}

func targetReadinessEncodedSize(state TargetStagedReadinessState) int {
	size := 3 + 6*migrationUint64Bytes + binary.MaxVarintLen64 + len(state.RouteStart) + 1
	if state.RouteEnd != nil {
		size += binary.MaxVarintLen64 + len(state.RouteEnd)
	}
	return size
}

func appendVarBytes(dst []byte, value []byte) []byte {
	dst = binary.AppendUvarint(dst, lenAsUint64(len(value)))
	return append(dst, value...)
}

func readVarBytes(data []byte) ([]byte, []byte, bool) {
	l, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, false
	}
	rest := data[n:]
	if l > lenAsUint64(len(rest)) {
		return nil, nil, false
	}
	return bytes.Clone(rest[:l]), rest[l:], true
}

func cloneTargetStagedReadinessState(state TargetStagedReadinessState) TargetStagedReadinessState {
	return TargetStagedReadinessState{
		JobID:                    state.JobID,
		RouteStart:               bytes.Clone(state.RouteStart),
		RouteEnd:                 bytes.Clone(state.RouteEnd),
		ExpectedCutoverVersion:   state.ExpectedCutoverVersion,
		MigrationJobID:           state.MigrationJobID,
		MinWriteTSExclusive:      state.MinWriteTSExclusive,
		Armed:                    state.Armed,
		SourceWriteFence:         state.SourceWriteFence,
		SourceReadFence:          state.SourceReadFence,
		RetentionPinTS:           state.RetentionPinTS,
		TrackWrites:              state.TrackWrites,
		MinAdmittedTS:            state.MinAdmittedTS,
		TransferSizeGuardVersion: state.TransferSizeGuardVersion,
	}
}

func cloneTargetStagedReadinessStates(states []TargetStagedReadinessState) []TargetStagedReadinessState {
	if len(states) == 0 {
		return nil
	}
	out := make([]TargetStagedReadinessState, 0, len(states))
	for _, state := range states {
		out = append(out, cloneTargetStagedReadinessState(state))
	}
	return out
}

func upsertTargetStagedReadinessStateCache(cache []TargetStagedReadinessState, state TargetStagedReadinessState) []TargetStagedReadinessState {
	cloned := cloneTargetStagedReadinessState(state)
	for i := range cache {
		if cache[i].JobID == cloned.JobID {
			cache[i] = cloned
			return cache
		}
	}
	cache = append(cache, cloned)
	sortTargetStagedReadinessStates(cache)
	return cache
}

func sortTargetStagedReadinessStates(states []TargetStagedReadinessState) {
	sort.Slice(states, func(i, j int) bool {
		return states[i].JobID < states[j].JobID
	})
}

var _ MigrationTargetReadinessReader = (*mvccStore)(nil)
var _ MigrationTargetReadinessReader = (*pebbleStore)(nil)
var _ MigrationTargetReadinessWriter = (*mvccStore)(nil)
var _ MigrationTargetReadinessWriter = (*pebbleStore)(nil)
var _ MigrationTargetReadinessRaftWriter = (*mvccStore)(nil)
var _ MigrationTargetReadinessRaftWriter = (*pebbleStore)(nil)
