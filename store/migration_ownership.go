package store

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

var migrationOwnershipMetaKeyBytes = []byte("!migstage|ownership")

var ErrMigrationOwnership = errors.New("migration ownership evidence mismatch; retry attestation")

type MigrationOwnershipRole uint8

const (
	MigrationOwner MigrationOwnershipRole = iota + 1
	MigrationNonOwner
)

// MigrationOwnershipState survives guard retirement. A newer catalog cache is
// not an ownership acquisition: only a newer replicated certificate can replace
// a NONOWNER verdict for the same range.
type MigrationOwnershipState struct {
	JobID               uint64
	LocalGroupID        uint64
	OwnerGroupID        uint64
	CatalogVersion      uint64
	ConfigurationIndex  uint64
	AppliedIndex        uint64
	MinWriteTSExclusive uint64
	RouteStart          []byte
	RouteEnd            []byte
	Role                MigrationOwnershipRole
	Staged              bool
}

type MigrationServingState struct {
	Ownership []MigrationOwnershipState
	Readiness []TargetStagedReadinessState
}

// MigrationOwnershipStore reads guard and serving evidence in one store view.
// ClearMigrationStateWithOwnership replaces a guard, retires its job, and
// installs serving evidence at the same durable applied boundary.
type MigrationOwnershipStore interface {
	MigrationServingState(context.Context) (MigrationServingState, error)
	AcquireMigrationOwnership(context.Context, MigrationOwnershipState, uint64) error
	ClearMigrationStateWithOwnership(context.Context, MigrationOwnershipState, uint64) error
}

func IsMigrationOwnershipMetadataKey(key []byte) bool {
	return bytes.Equal(key, migrationOwnershipMetaKeyBytes)
}

func validMigrationOwnership(state MigrationOwnershipState) bool {
	if !validMigrationOwnershipIdentity(state) ||
		(len(state.RouteEnd) > 0 && bytes.Compare(state.RouteStart, state.RouteEnd) >= 0) {
		return false
	}
	switch state.Role {
	case MigrationOwner:
		return state.LocalGroupID == state.OwnerGroupID
	case MigrationNonOwner:
		return state.LocalGroupID != state.OwnerGroupID && !state.Staged
	default:
		return false
	}
}

func validMigrationOwnershipIdentity(state MigrationOwnershipState) bool {
	return state.JobID != 0 && state.LocalGroupID != 0 && state.OwnerGroupID != 0 &&
		state.CatalogVersion != 0 && state.ConfigurationIndex != 0 && state.AppliedIndex != 0 && state.MinWriteTSExclusive != 0
}

func cloneMigrationOwnership(states []MigrationOwnershipState) []MigrationOwnershipState {
	out := make([]MigrationOwnershipState, len(states))
	for i, state := range states {
		out[i] = state
		out[i].RouteStart = bytes.Clone(state.RouteStart)
		out[i].RouteEnd = bytes.Clone(state.RouteEnd)
	}
	return out
}

func encodeMigrationOwnership(states []MigrationOwnershipState) ([]byte, error) {
	ordered := cloneMigrationOwnership(states)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].JobID < ordered[j].JobID })
	data, err := json.Marshal(ordered)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return append([]byte{migrationMetadataVersion}, data...), nil
}

func decodeMigrationOwnership(data []byte) ([]MigrationOwnershipState, error) {
	if len(data) == 0 || data[0] != migrationMetadataVersion {
		return nil, errors.New("invalid migration ownership encoding")
	}
	var states []MigrationOwnershipState
	if err := json.Unmarshal(data[1:], &states); err != nil {
		return nil, errors.Wrap(err, "decode migration ownership")
	}
	seen := make(map[uint64]struct{}, len(states))
	for _, state := range states {
		if _, duplicate := seen[state.JobID]; duplicate || !validMigrationOwnership(state) {
			return nil, errors.New("invalid migration ownership certificate")
		}
		seen[state.JobID] = struct{}{}
	}
	return states, nil
}

func (s *pebbleStore) readMigrationOwnership() ([]MigrationOwnershipState, error) {
	data, closer, err := s.db.Get(migrationOwnershipMetaKeyBytes)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	return decodeMigrationOwnership(data)
}

func (s *mvccStore) MigrationServingState(_ context.Context) (MigrationServingState, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return MigrationServingState{
		Ownership: cloneMigrationOwnership(s.migrationOwnership),
		Readiness: cloneReadinessStates(s.migrationReadinessCache),
	}, nil
}

func (s *pebbleStore) MigrationServingState(ctx context.Context) (MigrationServingState, error) {
	// The raw batch callback already owns applyMu. Its context is the only
	// supported way to read this view without reentering that mutex.
	if s.raftEntryScopeFrom(ctx) == nil {
		s.applyMu.Lock()
		defer s.applyMu.Unlock()
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	states, err := s.readMigrationOwnership()
	if err != nil {
		return MigrationServingState{}, err
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return MigrationServingState{Ownership: states, Readiness: cloneReadinessStates(s.migrationReadinessCache)}, nil
}

func cloneReadinessStates(states []TargetStagedReadinessState) []TargetStagedReadinessState {
	out := make([]TargetStagedReadinessState, len(states))
	for i, state := range states {
		out[i] = state
		out[i].RouteStart = bytes.Clone(state.RouteStart)
		out[i].RouteEnd = bytes.Clone(state.RouteEnd)
	}
	return out
}

func replaceMigrationOwnership(states []MigrationOwnershipState, state MigrationOwnershipState) ([]MigrationOwnershipState, error) {
	if !validMigrationOwnership(state) {
		return nil, errors.WithStack(ErrMigrationOwnership)
	}
	out := cloneMigrationOwnership(states)
	for _, old := range out {
		if old.LocalGroupID != state.LocalGroupID {
			return nil, errors.WithStack(ErrMigrationOwnership)
		}
		if old.JobID == state.JobID {
			// Retrying a completed clear cannot revise its committed certificate.
			state.AppliedIndex = old.AppliedIndex
			if !sameMigrationOwnership(old, state) {
				return nil, errors.WithStack(ErrMigrationOwnership)
			}
			return out, nil
		}
		if migrationOwnershipOverlaps(old, state) && old.CatalogVersion >= state.CatalogVersion {
			return nil, errors.WithStack(ErrMigrationOwnership)
		}
	}
	return append(out, cloneMigrationOwnership([]MigrationOwnershipState{state})[0]), nil
}

func sameMigrationOwnership(a, b MigrationOwnershipState) bool {
	return sameMigrationOwnershipRange(a, b) && a.CatalogVersion == b.CatalogVersion && a.ConfigurationIndex == b.ConfigurationIndex &&
		a.AppliedIndex == b.AppliedIndex && a.MinWriteTSExclusive == b.MinWriteTSExclusive &&
		a.Role == b.Role && a.Staged == b.Staged
}

func sameMigrationOwnershipRange(a, b MigrationOwnershipState) bool {
	return a.JobID == b.JobID && a.LocalGroupID == b.LocalGroupID && a.OwnerGroupID == b.OwnerGroupID &&
		bytes.Equal(a.RouteStart, b.RouteStart) && bytes.Equal(a.RouteEnd, b.RouteEnd)
}

func migrationOwnershipOverlaps(a, b MigrationOwnershipState) bool {
	return (len(a.RouteEnd) == 0 || bytes.Compare(b.RouteStart, a.RouteEnd) < 0) &&
		(len(b.RouteEnd) == 0 || bytes.Compare(a.RouteStart, b.RouteEnd) < 0)
}

func validateOwnershipGuard(state MigrationOwnershipState, guards []TargetStagedReadinessState, retired bool) error {
	if retired {
		return nil // The immutable existing certificate is checked by replacement.
	}
	for _, guard := range guards {
		if !ownershipGuardMatchesRange(state, guard) {
			continue
		}
		if ownershipSourceRemovalMatches(state, guard) {
			return nil
		}
		// Rollback to the source retains its tracked live data. Target ownership
		// acquisition needs a separate data-ready control and is not accepted here.
		if ownershipRollbackMatches(state, guard) {
			return nil
		}
	}
	return errors.WithStack(ErrMigrationOwnership)
}

func ownershipGuardMatchesRange(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return guard.JobID == state.JobID && guard.MigrationJobID == state.JobID && guard.Armed &&
		bytes.Equal(guard.RouteStart, state.RouteStart) && bytes.Equal(guard.RouteEnd, state.RouteEnd)
}

func ownershipSourceRemovalMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return state.Role == MigrationNonOwner && guard.SourceWriteFence && guard.SourceReadFence &&
		state.CatalogVersion >= guard.ExpectedCutoverVersion && state.MinWriteTSExclusive == guard.MinWriteTSExclusive
}

func ownershipRollbackMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return state.Role == MigrationOwner && (guard.TrackWrites || guard.SourceWriteFence) && !state.Staged &&
		state.CatalogVersion > guard.ExpectedCutoverVersion && state.MinWriteTSExclusive == guard.MinWriteTSExclusive
}

func validateOwnershipAcquisition(state MigrationOwnershipState, guards []TargetStagedReadinessState) error {
	if !validMigrationOwnership(state) || state.Role != MigrationOwner {
		return errors.WithStack(ErrMigrationOwnership)
	}
	for _, guard := range guards {
		if ownershipGuardMatchesRange(state, guard) && ownershipTargetMatches(state, guard) {
			return nil
		}
	}
	return errors.WithStack(ErrMigrationOwnership)
}

func ownershipTargetMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return !guard.TrackWrites && !guard.SourceWriteFence && !guard.SourceReadFence && state.Staged &&
		guard.ExpectedCutoverVersion == state.CatalogVersion && guard.MinWriteTSExclusive == state.MinWriteTSExclusive
}

func (s *mvccStore) AcquireMigrationOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	state.AppliedIndex = appliedIndex
	if _, retired := s.migrationRetired[state.JobID]; retired {
		return errors.WithStack(ErrMigrationRetired)
	}
	if err := validateOwnershipAcquisition(state, s.migrationReadinessCache); err != nil {
		return err
	}
	states, err := replaceMigrationOwnership(s.migrationOwnership, state)
	if err == nil {
		s.migrationOwnership = states
	}
	return err
}

func (s *pebbleStore) AcquireMigrationOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	state.AppliedIndex = appliedIndex
	if err := s.checkMigrationNotRetiredLocked(state.JobID); err != nil {
		return err
	}
	s.mtx.RLock()
	err := validateOwnershipAcquisition(state, s.migrationReadinessCache)
	s.mtx.RUnlock()
	if err != nil {
		return err
	}
	states, err := s.readMigrationOwnership()
	if err != nil {
		return err
	}
	states, err = replaceMigrationOwnership(states, state)
	if err != nil {
		return err
	}
	data, err := encodeMigrationOwnership(states)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(migrationOwnershipMetaKeyBytes, data, nil); err != nil {
		return errors.WithStack(err)
	}
	if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
		return err
	}
	return errors.WithStack(batch.Commit(s.cleanupWriteOpts(appliedIndex)))
}

func (s *mvccStore) ClearMigrationStateWithOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	state.AppliedIndex = appliedIndex
	_, retired := s.migrationRetired[state.JobID]
	if err := validateOwnershipGuard(state, s.migrationReadinessCache, retired); err != nil {
		return err
	}
	states, err := replaceMigrationOwnership(s.migrationOwnership, state)
	if err != nil {
		return err
	}
	if retired && !hasMigrationOwnership(s.migrationOwnership, state.JobID) {
		return errors.WithStack(ErrMigrationOwnership)
	}
	s.migrationOwnership = states
	s.clearMigrationStateLocked(state.JobID)
	return nil
}

func hasMigrationOwnership(states []MigrationOwnershipState, jobID uint64) bool {
	for _, state := range states {
		if state.JobID == jobID {
			return true
		}
	}
	return false
}

func (s *pebbleStore) ClearMigrationStateWithOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	state.AppliedIndex = appliedIndex
	states, err := s.readMigrationOwnership()
	if err != nil {
		return err
	}
	retired, err := s.readMigrationRetiredJobs()
	if err != nil {
		return err
	}
	_, isRetired := retired[state.JobID]
	s.mtx.RLock()
	err = validateOwnershipGuard(state, s.migrationReadinessCache, isRetired)
	s.mtx.RUnlock()
	if err != nil {
		return err
	}
	if isRetired && !hasMigrationOwnership(states, state.JobID) {
		return errors.WithStack(ErrMigrationOwnership)
	}
	states, err = replaceMigrationOwnership(states, state)
	if err != nil {
		return err
	}
	return s.clearMigrationStateWithOwnershipLocked(state.JobID, appliedIndex, states)
}

func (s *pebbleStore) clearMigrationStateWithOwnershipLocked(jobID, appliedIndex uint64, states []MigrationOwnershipState) error {
	metadata, err := s.migrationMetadataWithoutJob(jobID)
	if err != nil {
		return err
	}
	data, err := encodeMigrationOwnership(states)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := stageMigrationMetadataCleanup(batch, jobID, appliedIndex, metadata); err != nil {
		return err
	}
	if err := batch.Set(migrationOwnershipMetaKeyBytes, data, nil); err != nil {
		return errors.WithStack(err)
	}
	if err := batch.Commit(s.cleanupWriteOpts(appliedIndex)); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	s.migrationReadinessCache = upsertReadinessCacheWithoutJob(s.migrationReadinessCache, jobID)
	s.mtx.Unlock()
	return nil
}
