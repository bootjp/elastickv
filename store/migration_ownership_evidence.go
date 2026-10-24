package store

import "bytes"

// MigrationOwnershipEvidenceValid deliberately does not tighten the snapshot
// codec: legacy records remain recoverable, but cannot authorize serving.
func MigrationOwnershipEvidenceValid(state MigrationOwnershipState) bool {
	if !validMigrationOwnership(state) || !validOwnershipReceipts(state) || !ownershipDescriptorKnown(state) || len(state.Receipts) == 0 {
		return false
	}
	if !ownershipLatestReceiptMatches(state) {
		return false
	}
	if state.SourceTrackerVersion != 0 {
		return ownershipSourceBoundaryKnown(state)
	}
	return state.SourceAppliedIndex == 0 && ownershipTerminalReceiptMatches(state)
}

func ownershipTerminalReceiptMatches(state MigrationOwnershipState) bool {
	latest := state.Receipts[len(state.Receipts)-1]
	return (state.Staged && latest.Operation == MigrationOwnershipAcquire) || (!state.Staged && latest.Operation == MigrationOwnershipClear)
}

func ownershipLatestReceiptMatches(state MigrationOwnershipState) bool {
	latest := state.Receipts[len(state.Receipts)-1]
	return len(latest.DescriptorHash) == 32 && bytes.Equal(latest.DescriptorHash, state.DescriptorHash) && latest.Staged == state.Staged
}

func ownershipDescriptorKnown(state MigrationOwnershipState) bool {
	return len(state.Generation) >= 16 && len(state.Generation) <= 128 && state.RouteID != 0 && len(state.DescriptorHash) == 32
}

func ownershipWriteFloorKnown(state MigrationOwnershipState) bool {
	return state.WriteFloorKnown || state.MinWriteTSExclusive != 0
}

func ownershipSourceBoundaryKnown(state MigrationOwnershipState) bool {
	first := state.Receipts[0]
	return state.Role == MigrationOwner && !state.Staged && state.WriteFloorKnown && state.SourceAppliedIndex != 0 &&
		state.SourceAppliedIndex <= state.AppliedIndex && first.Operation == MigrationOwnershipAcquire && first.AppliedIndex == state.SourceAppliedIndex
}

func ownershipSourceAcquisitionMatches(state MigrationOwnershipState, guard TargetStagedReadinessState) bool {
	return ownershipDescriptorKnown(state) && state.WriteFloorKnown && !state.Staged && state.SourceTrackerVersion != 0 &&
		state.SourceAppliedIndex == state.AppliedIndex && guard.TrackWrites && !guard.SourceWriteFence && !guard.SourceReadFence &&
		state.SourceTrackerVersion == guard.ExpectedCutoverVersion && guard.RetentionPinTS != 0
}

func ownershipSourceTransition(old, state MigrationOwnershipState) bool {
	return ownershipSourceTransitionIdentity(old, state) && state.CatalogVersion > old.CatalogVersion &&
		state.ConfigurationIndex >= old.ConfigurationIndex && state.AppliedIndex > old.AppliedIndex && !state.Staged &&
		state.MinWriteTSExclusive >= old.MinWriteTSExclusive
}

func ownershipSourceTransitionIdentity(old, state MigrationOwnershipState) bool {
	return old.SourceTrackerVersion != 0 && MigrationOwnershipEvidenceValid(old) && state.SourceTrackerVersion == 0 && state.SourceAppliedIndex == 0 &&
		old.JobID == state.JobID && old.LocalGroupID == state.LocalGroupID && old.Generation == state.Generation &&
		bytes.Equal(old.RouteStart, state.RouteStart) && bytes.Equal(old.RouteEnd, state.RouteEnd)
}
