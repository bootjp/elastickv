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
	// Generation is a replicated ownership namespace, never a configuration
	// index, snapshot index, peer list, or watcher-derived value.
	Generation           string
	JobID                uint64
	LocalGroupID         uint64
	OwnerGroupID         uint64
	CatalogVersion       uint64
	ConfigurationIndex   uint64
	AppliedIndex         uint64
	MinWriteTSExclusive  uint64
	RouteStart           []byte
	RouteEnd             []byte
	Role                 MigrationOwnershipRole
	Staged               bool
	RouteID              uint64
	ParentRouteID        uint64
	DescriptorHash       []byte
	Receipts             []MigrationOwnershipReceipt
	WriteFloorKnown      bool
	SourceTrackerVersion uint64
	SourceAppliedIndex   uint64
}

type MigrationOwnershipOperation uint8

const (
	MigrationOwnershipAcquire MigrationOwnershipOperation = iota + 1
	MigrationOwnershipClear
	MigrationOwnershipRefresh
)

// Receipts name immutable operations; the outer state's version/index fields
// are the latest separately replicated unchanged-descriptor attestation.
type MigrationOwnershipReceipt struct {
	Operation          MigrationOwnershipOperation
	CatalogVersion     uint64
	ConfigurationIndex uint64
	AppliedIndex       uint64
	Staged             bool
	DescriptorHash     []byte
}

type MigrationServingState struct {
	Ownership  []MigrationOwnershipState
	Readiness  []TargetStagedReadinessState
	Partitions MigrationOwnershipPartitionState
}

// MigrationOwnershipStore reads guard and serving evidence in one store view.
// ClearMigrationStateWithOwnership replaces a guard, retires its job, and
// installs serving evidence at the same durable applied boundary.
type MigrationOwnershipStore interface {
	MigrationServingState(context.Context) (MigrationServingState, error)
	AcquireMigrationOwnership(context.Context, MigrationOwnershipState, uint64) error
	ClearMigrationStateWithOwnership(context.Context, MigrationOwnershipState, uint64) error
}

// MigrationOwnershipRefresher is separate so old runtimes cannot accidentally
// advertise refresh support by embedding the original ownership interface.
type MigrationOwnershipRefresher interface {
	RefreshMigrationOwnership(context.Context, MigrationOwnershipState, uint64) error
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
		state.CatalogVersion != 0 && state.ConfigurationIndex != 0 && state.AppliedIndex != 0 &&
		(state.MinWriteTSExclusive != 0 || state.WriteFloorKnown)
}

func cloneMigrationOwnership(states []MigrationOwnershipState) []MigrationOwnershipState {
	out := make([]MigrationOwnershipState, len(states))
	for i, state := range states {
		out[i] = state
		out[i].RouteStart = bytes.Clone(state.RouteStart)
		out[i].RouteEnd = bytes.Clone(state.RouteEnd)
		out[i].Receipts = append([]MigrationOwnershipReceipt(nil), state.Receipts...)
		out[i].DescriptorHash = bytes.Clone(state.DescriptorHash)
		for j := range out[i].Receipts {
			out[i].Receipts[j].DescriptorHash = bytes.Clone(state.Receipts[j].DescriptorHash)
		}
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
		if _, duplicate := seen[state.JobID]; duplicate || !validMigrationOwnership(state) || !validOwnershipReceipts(state) {
			return nil, errors.New("invalid migration ownership certificate")
		}
		seen[state.JobID] = struct{}{}
	}
	return states, nil
}

func validOwnershipReceipts(state MigrationOwnershipState) bool {
	const maxOwnershipReceipts = 2
	if len(state.Receipts) > maxOwnershipReceipts {
		return false
	}
	seen := make(map[MigrationOwnershipOperation]bool)
	for _, receipt := range state.Receipts {
		if seen[receipt.Operation] || !validOwnershipReceipt(state, receipt) {
			return false
		}
		seen[receipt.Operation] = true
	}
	return true
}

func validOwnershipReceipt(state MigrationOwnershipState, receipt MigrationOwnershipReceipt) bool {
	return (receipt.Operation == MigrationOwnershipAcquire || receipt.Operation == MigrationOwnershipClear) &&
		receipt.CatalogVersion != 0 && receipt.CatalogVersion <= state.CatalogVersion && receipt.ConfigurationIndex != 0 &&
		receipt.ConfigurationIndex <= state.ConfigurationIndex && receipt.AppliedIndex != 0 && receipt.AppliedIndex <= state.AppliedIndex
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
		Ownership:  cloneMigrationOwnership(s.migrationOwnership),
		Readiness:  cloneReadinessStates(s.migrationReadinessCache),
		Partitions: clonePartitionState(s.migrationPartitions),
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
	partitions, err := s.readMigrationOwnershipPartitions()
	if err != nil {
		return MigrationServingState{}, err
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return MigrationServingState{Ownership: states, Readiness: cloneReadinessStates(s.migrationReadinessCache), Partitions: partitions}, nil
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

func replaceMigrationOwnership(states []MigrationOwnershipState, state MigrationOwnershipState, operation MigrationOwnershipOperation) ([]MigrationOwnershipState, error) {
	if !validMigrationOwnership(state) {
		return nil, errors.WithStack(ErrMigrationOwnership)
	}
	out := cloneMigrationOwnership(states)
	for i, old := range out {
		if old.LocalGroupID != state.LocalGroupID || old.Generation != state.Generation {
			return nil, errors.WithStack(ErrMigrationOwnership)
		}
		if old.JobID == state.JobID {
			updated, err := updateMigrationOwnershipAttestation(old, state, operation)
			if err != nil {
				return nil, err
			}
			out[i] = cloneMigrationOwnership([]MigrationOwnershipState{updated})[0]
			return out, nil
		}
		if migrationOwnershipOverlaps(old, state) && old.CatalogVersion >= state.CatalogVersion {
			return nil, errors.WithStack(ErrMigrationOwnership)
		}
	}
	if operation == MigrationOwnershipRefresh {
		return nil, errors.WithStack(ErrMigrationOwnership)
	}
	state.Receipts = []MigrationOwnershipReceipt{ownershipOperationReceipt(state, operation)}
	return append(out, cloneMigrationOwnership([]MigrationOwnershipState{state})[0]), nil
}

func ownershipOperationReceipt(state MigrationOwnershipState, operation MigrationOwnershipOperation) MigrationOwnershipReceipt {
	return MigrationOwnershipReceipt{Operation: operation, CatalogVersion: state.CatalogVersion,
		ConfigurationIndex: state.ConfigurationIndex, AppliedIndex: state.AppliedIndex, Staged: state.Staged,
		DescriptorHash: bytes.Clone(state.DescriptorHash)}
}

func updateMigrationOwnershipAttestation(old, state MigrationOwnershipState, operation MigrationOwnershipOperation) (MigrationOwnershipState, error) {
	if operation == MigrationOwnershipClear && ownershipSourceTransition(old, state) {
		state.Receipts = append(append([]MigrationOwnershipReceipt(nil), old.Receipts...), ownershipOperationReceipt(state, operation))
		return state, nil
	}
	if !sameMigrationOwnershipDescriptor(old, state) {
		return MigrationOwnershipState{}, errors.WithStack(ErrMigrationOwnership)
	}
	if operation == MigrationOwnershipRefresh {
		return refreshMigrationOwnershipAttestation(old, state)
	}
	return replayMigrationOwnershipOperation(old, state, operation)
}

func refreshMigrationOwnershipAttestation(old, state MigrationOwnershipState) (MigrationOwnershipState, error) {
	if state.CatalogVersion < old.CatalogVersion || state.ConfigurationIndex < old.ConfigurationIndex || state.AppliedIndex < old.AppliedIndex ||
		state.Staged != old.Staged || !bytes.Equal(state.DescriptorHash, old.DescriptorHash) {
		return MigrationOwnershipState{}, errors.WithStack(ErrMigrationOwnership)
	}
	state.Receipts = append([]MigrationOwnershipReceipt(nil), old.Receipts...)
	return state, nil
}

func replayMigrationOwnershipOperation(old, state MigrationOwnershipState, operation MigrationOwnershipOperation) (MigrationOwnershipState, error) {
	for _, receipt := range old.Receipts {
		if receipt.Operation != operation {
			continue
		}
		if receipt.CatalogVersion != state.CatalogVersion || receipt.Staged != state.Staged || !bytes.Equal(receipt.DescriptorHash, state.DescriptorHash) {
			return MigrationOwnershipState{}, errors.WithStack(ErrMigrationOwnership)
		}
		return old, nil
	}
	if operation != MigrationOwnershipClear || !old.Staged || state.Staged || state.CatalogVersion < old.CatalogVersion {
		return MigrationOwnershipState{}, errors.WithStack(ErrMigrationOwnership)
	}
	state.Receipts = append(append([]MigrationOwnershipReceipt(nil), old.Receipts...), ownershipOperationReceipt(state, operation))
	return state, nil
}

func sameMigrationOwnershipDescriptor(a, b MigrationOwnershipState) bool {
	return sameMigrationOwnershipRange(a, b) && a.Generation == b.Generation && a.Role == b.Role &&
		ownershipWriteFloorKnown(a) == ownershipWriteFloorKnown(b) && a.SourceTrackerVersion == b.SourceTrackerVersion && a.SourceAppliedIndex == b.SourceAppliedIndex &&
		a.MinWriteTSExclusive == b.MinWriteTSExclusive && a.RouteID == b.RouteID && a.ParentRouteID == b.ParentRouteID
}

func sameMigrationOwnershipRange(a, b MigrationOwnershipState) bool {
	return a.JobID == b.JobID && a.LocalGroupID == b.LocalGroupID && a.OwnerGroupID == b.OwnerGroupID &&
		bytes.Equal(a.RouteStart, b.RouteStart) && bytes.Equal(a.RouteEnd, b.RouteEnd)
}

func migrationOwnershipOverlaps(a, b MigrationOwnershipState) bool {
	return (len(a.RouteEnd) == 0 || bytes.Compare(b.RouteStart, a.RouteEnd) < 0) &&
		(len(b.RouteEnd) == 0 || bytes.Compare(a.RouteStart, b.RouteEnd) < 0)
}

func validateOwnershipGuard(state MigrationOwnershipState, guards []TargetStagedReadinessState, retired bool, promoted ...bool) error {
	if retired {
		return nil // The immutable existing certificate is checked by replacement.
	}
	for _, guard := range guards {
		if !ownershipGuardMatchesRange(state, guard) {
			continue
		}
		// Rollback to the source retains its tracked live data. Target ownership
		// acquisition needs a separate data-ready control and is not accepted here.
		if ownershipClearMatches(state, guard, len(promoted) > 0 && promoted[0]) {
			return nil
		}
	}
	if !ownershipJobGuardExists(guards, state.JobID) && !state.Staged && len(state.Generation) >= 16 && len(state.DescriptorHash) == 32 {
		return nil
	}
	return errors.WithStack(ErrMigrationOwnership)
}

func ownershipClearMatches(state MigrationOwnershipState, guard TargetStagedReadinessState, promoted bool) bool {
	return ownershipSourceRemovalMatches(state, guard) || ownershipAbandonedTargetMatches(state, guard) ||
		ownershipRollbackMatches(state, guard) || (promoted && ownershipPromotedTargetMatches(state, guard))
}

func ownershipJobGuardExists(guards []TargetStagedReadinessState, jobID uint64) bool {
	for _, guard := range guards {
		if guard.JobID == jobID {
			return true
		}
	}
	return false
}

func ownershipPromotedTargetMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return state.Role == MigrationOwner && !state.Staged && !guard.TrackWrites && !guard.SourceWriteFence && !guard.SourceReadFence &&
		state.CatalogVersion >= guard.ExpectedCutoverVersion && state.MinWriteTSExclusive == guard.MinWriteTSExclusive
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
		state.CatalogVersion >= guard.ExpectedCutoverVersion && state.MinWriteTSExclusive >= guard.MinWriteTSExclusive
}

func ownershipAbandonedTargetMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return state.Role == MigrationNonOwner && !guard.TrackWrites && !guard.SourceWriteFence && !guard.SourceReadFence &&
		state.CatalogVersion >= guard.ExpectedCutoverVersion && state.MinWriteTSExclusive >= guard.MinWriteTSExclusive
}

func validateOwnershipAcquisition(state MigrationOwnershipState, guards []TargetStagedReadinessState) error {
	if !validMigrationOwnership(state) || state.Role != MigrationOwner {
		return errors.WithStack(ErrMigrationOwnership)
	}
	for _, guard := range guards {
		if ownershipGuardMatchesRange(state, guard) && (ownershipTargetMatches(state, guard) || ownershipSourceAcquisitionMatches(state, guard)) {
			return nil
		}
	}
	return errors.WithStack(ErrMigrationOwnership)
}

func ownershipTargetMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return !guard.TrackWrites && !guard.SourceWriteFence && !guard.SourceReadFence && state.Staged &&
		ownershipAcquisitionVersionMatches(state, guard) && guard.MinWriteTSExclusive == state.MinWriteTSExclusive
}

func ownershipAcquisitionVersionMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	if guard.ExpectedCutoverVersion == state.CatalogVersion {
		return true
	}
	// Only the authenticated committed-descriptor control may attest a later
	// version. Legacy unversioned controls never gain this ability implicitly.
	return len(state.Generation) >= 16 && len(state.DescriptorHash) == 32 && state.CatalogVersion > guard.ExpectedCutoverVersion
}

func (s *mvccStore) AcquireMigrationOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	state.AppliedIndex = appliedIndex
	if state.SourceTrackerVersion != 0 {
		state.SourceAppliedIndex = appliedIndex
	}
	if partitionJobReplay(s.migrationPartitions, s.migrationOwnership, state, MigrationOwnershipAcquire) {
		return nil
	}
	if _, retired := s.migrationRetired[state.JobID]; retired {
		return errors.WithStack(ErrMigrationRetired)
	}
	if err := validateOwnershipAcquisition(state, s.migrationReadinessCache); err != nil {
		return err
	}
	states, err := replaceMigrationOwnership(s.migrationOwnership, state, MigrationOwnershipAcquire)
	if err != nil {
		return err
	}
	partitions, err := s.syncPartitionJobLocked(states, state, MigrationOwnershipAcquire, appliedIndex)
	if err != nil {
		return err
	}
	s.migrationOwnership, s.migrationPartitions = states, partitions
	s.migrationAdmission = buildMigrationServingAdmission(states, partitions)
	return nil
}

func (s *pebbleStore) AcquireMigrationOwnership(ctx context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	if s.raftEntryScopeFrom(ctx) != nil {
		return errors.WithStack(ErrMigrationOwnership)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	state.AppliedIndex = appliedIndex
	if state.SourceTrackerVersion != 0 {
		state.SourceAppliedIndex = appliedIndex
	}
	old, err := s.readMigrationOwnership()
	if err != nil {
		return err
	}
	partitions, err := s.readMigrationOwnershipPartitions()
	if err != nil {
		return err
	}
	if partitionJobReplay(partitions, old, state, MigrationOwnershipAcquire) {
		return s.persistMigrationOwnershipLocked(old, appliedIndex, partitions)
	}
	if err := s.checkMigrationNotRetiredLocked(state.JobID); err != nil {
		return err
	}
	s.mtx.RLock()
	err = validateOwnershipAcquisition(state, s.migrationReadinessCache)
	s.mtx.RUnlock()
	if err != nil {
		return err
	}
	states, err := replaceMigrationOwnership(old, state, MigrationOwnershipAcquire)
	if err != nil {
		return err
	}
	partitions, err = s.syncPartitionJobLocked(ctx, partitions, old, states, state, MigrationOwnershipAcquire, appliedIndex)
	if err != nil {
		return err
	}
	return s.persistMigrationOwnershipLocked(states, appliedIndex, partitions)
}

func (s *mvccStore) RefreshMigrationOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	state.AppliedIndex = appliedIndex
	if state.Generation == "" || state.RouteID == 0 {
		return errors.WithStack(ErrMigrationOwnership)
	}
	states, err := replaceMigrationOwnership(s.migrationOwnership, state, MigrationOwnershipRefresh)
	if err != nil {
		return err
	}
	partitions, err := s.syncPartitionJobLocked(states, state, MigrationOwnershipRefresh, appliedIndex)
	if err != nil {
		return err
	}
	s.migrationOwnership, s.migrationPartitions = states, partitions
	s.migrationAdmission = buildMigrationServingAdmission(states, partitions)
	return nil
}

func (s *pebbleStore) RefreshMigrationOwnership(ctx context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	if s.raftEntryScopeFrom(ctx) != nil {
		return errors.WithStack(ErrMigrationOwnership)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	state.AppliedIndex = appliedIndex
	if state.Generation == "" || state.RouteID == 0 {
		return errors.WithStack(ErrMigrationOwnership)
	}
	states, err := s.readMigrationOwnership()
	if err != nil {
		return err
	}
	old := states
	states, err = replaceMigrationOwnership(states, state, MigrationOwnershipRefresh)
	if err != nil {
		return err
	}
	partitions, err := s.readMigrationOwnershipPartitions()
	if err != nil {
		return err
	}
	partitions, err = s.syncPartitionJobLocked(ctx, partitions, old, states, state, MigrationOwnershipRefresh, appliedIndex)
	if err != nil {
		return err
	}
	return s.persistMigrationOwnershipLocked(states, appliedIndex, partitions)
}

func (s *pebbleStore) persistMigrationOwnershipLocked(states []MigrationOwnershipState, appliedIndex uint64, partitions MigrationOwnershipPartitionState) error {
	data, err := encodeMigrationOwnership(states)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(migrationOwnershipMetaKeyBytes, data, nil); err != nil {
		return errors.WithStack(err)
	}
	if err := stageSnapshotPartitions(batch, partitions); err != nil {
		return err
	}
	if err := s.stagePartitionAppliedIndex(batch, appliedIndex); err != nil {
		return err
	}
	if err := batch.Commit(s.cleanupWriteOpts(appliedIndex)); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	s.migrationAdmission = buildMigrationServingAdmission(states, partitions)
	s.mtx.Unlock()
	return nil
}

func (s *mvccStore) ClearMigrationStateWithOwnership(_ context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	state.AppliedIndex = appliedIndex
	if partitionJobReplay(s.migrationPartitions, s.migrationOwnership, state, MigrationOwnershipClear) {
		return nil
	}
	_, retired := s.migrationRetired[state.JobID]
	if err := validateOwnershipGuard(state, s.migrationReadinessCache, retired, s.migrationPromotions[state.JobID].Done); err != nil {
		return err
	}
	states, err := replaceMigrationOwnership(s.migrationOwnership, state, MigrationOwnershipClear)
	if err != nil {
		return err
	}
	if err := validateRetiredOwnership(s.migrationOwnership, state.JobID, retired); err != nil {
		return err
	}
	partitions, err := s.syncPartitionJobLocked(states, state, MigrationOwnershipClear, appliedIndex)
	if err != nil {
		return err
	}
	s.migrationOwnership, s.migrationPartitions = states, partitions
	s.migrationAdmission = buildMigrationServingAdmission(states, partitions)
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

func validateRetiredOwnership(states []MigrationOwnershipState, jobID uint64, retired bool) error {
	if retired && !hasMigrationOwnership(states, jobID) {
		return errors.WithStack(ErrMigrationOwnership)
	}
	return nil
}

func (s *pebbleStore) ClearMigrationStateWithOwnership(ctx context.Context, state MigrationOwnershipState, appliedIndex uint64) error {
	if s.raftEntryScopeFrom(ctx) != nil {
		return errors.WithStack(ErrMigrationOwnership)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	state.AppliedIndex = appliedIndex
	states, err := s.readMigrationOwnership()
	if err != nil {
		return err
	}
	partitions, err := s.readMigrationOwnershipPartitions()
	if err != nil {
		return err
	}
	if partitionJobReplay(partitions, states, state, MigrationOwnershipClear) {
		return s.persistMigrationOwnershipLocked(states, appliedIndex, partitions)
	}
	if err := s.validateOwnershipClearLocked(state, states); err != nil {
		return err
	}
	old := states
	states, err = replaceMigrationOwnership(states, state, MigrationOwnershipClear)
	if err != nil {
		return err
	}
	partitions, err = s.syncPartitionJobLocked(ctx, partitions, old, states, state, MigrationOwnershipClear, appliedIndex)
	if err != nil {
		return err
	}
	return s.clearMigrationStateWithOwnershipLocked(state.JobID, appliedIndex, states, partitions)
}

func (s *pebbleStore) validateOwnershipClearLocked(state MigrationOwnershipState, states []MigrationOwnershipState) error {
	retired, err := s.readMigrationRetiredJobs()
	if err != nil {
		return err
	}
	_, isRetired := retired[state.JobID]
	promotions, err := s.readPebblePromotionStates()
	if err != nil {
		return err
	}
	s.mtx.RLock()
	err = validateOwnershipGuard(state, s.migrationReadinessCache, isRetired, promotions[state.JobID].Done)
	s.mtx.RUnlock()
	if err != nil {
		return err
	}
	return validateRetiredOwnership(states, state.JobID, isRetired)
}

func (s *pebbleStore) clearMigrationStateWithOwnershipLocked(jobID, appliedIndex uint64, states []MigrationOwnershipState, partitions MigrationOwnershipPartitionState) error {
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
	if err := stageSnapshotPartitions(batch, partitions); err != nil {
		return err
	}
	if err := s.stagePartitionAppliedIndex(batch, appliedIndex); err != nil {
		return err
	}
	if err := batch.Commit(s.cleanupWriteOpts(appliedIndex)); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	s.migrationAdmission = buildMigrationServingAdmission(states, partitions)
	s.migrationReadinessCache = upsertReadinessCacheWithoutJob(s.migrationReadinessCache, jobID)
	s.mtx.Unlock()
	return nil
}
