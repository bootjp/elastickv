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
	targetReadinessCodecVersion byte = 1
	targetReadinessArmedFlag    byte = 1
)

func validateTargetStagedReadinessState(state TargetStagedReadinessState) error {
	switch {
	case state.JobID == 0:
		return errors.New("target staged readiness job_id is required")
	case state.MigrationJobID == 0:
		return errors.New("target staged readiness migration_job_id is required")
	case state.RouteEnd != nil && bytes.Compare(state.RouteStart, state.RouteEnd) >= 0:
		return errors.New("target staged readiness route range is invalid")
	default:
		return nil
	}
}

func (s *mvccStore) ApplyTargetStagedReadiness(_ context.Context, state TargetStagedReadinessState) error {
	if err := validateTargetStagedReadinessState(state); err != nil {
		return err
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	cloned := cloneTargetStagedReadinessState(state)
	s.migrationReadiness[state.JobID] = cloned
	s.migrationReadinessCache = upsertTargetStagedReadinessStateCache(s.migrationReadinessCache, cloned)
	return nil
}

func (s *mvccStore) MigrationTargetReadinessStates(_ context.Context) ([]TargetStagedReadinessState, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return cloneTargetStagedReadinessStates(s.migrationReadinessCache), nil
}

func (s *pebbleStore) ApplyTargetStagedReadiness(_ context.Context, state TargetStagedReadinessState) error {
	if err := validateTargetStagedReadinessState(state); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := s.db.Set(migrationReadyKey(state.JobID), encodeTargetStagedReadinessState(state), s.directApplyWriteOpts()); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.migrationReadinessCache = upsertTargetStagedReadinessStateCache(s.migrationReadinessCache, state)
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
	if state.Armed {
		buf = append(buf, targetReadinessArmedFlag)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.BigEndian.AppendUint64(buf, state.JobID)
	buf = binary.BigEndian.AppendUint64(buf, state.ExpectedCutoverVersion)
	buf = binary.BigEndian.AppendUint64(buf, state.MigrationJobID)
	buf = binary.BigEndian.AppendUint64(buf, state.MinWriteTSExclusive)
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
	if len(data) < 2+4*migrationUint64Bytes || data[0] != targetReadinessCodecVersion {
		return TargetStagedReadinessState{}, nil, false
	}
	if data[1] != 0 && data[1] != targetReadinessArmedFlag {
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
	size := 2 + 4*migrationUint64Bytes + binary.MaxVarintLen64 + len(state.RouteStart) + 1
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
		JobID:                  state.JobID,
		RouteStart:             bytes.Clone(state.RouteStart),
		RouteEnd:               bytes.Clone(state.RouteEnd),
		ExpectedCutoverVersion: state.ExpectedCutoverVersion,
		MigrationJobID:         state.MigrationJobID,
		MinWriteTSExclusive:    state.MinWriteTSExclusive,
		Armed:                  state.Armed,
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
