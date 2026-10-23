package store

import (
	"bytes"
	"encoding/hex"

	"github.com/cockroachdb/errors"
)

func partitionJobReplay(s MigrationOwnershipPartitionState, ledger []MigrationOwnershipState, requested MigrationOwnershipState, operation MigrationOwnershipOperation) bool {
	if !partitionHasEvidence(s) || operation == MigrationOwnershipRefresh {
		return false
	}
	for _, old := range ledger {
		if old.JobID != requested.JobID {
			continue
		}
		return partitionLegacyOperationReplay(old, requested, operation)
	}
	return false
}

func partitionLegacyOperationReplay(old, requested MigrationOwnershipState, operation MigrationOwnershipOperation) bool {
	if requested.SourceTrackerVersion != 0 && requested.SourceAppliedIndex == requested.AppliedIndex {
		requested.SourceAppliedIndex = old.SourceAppliedIndex
	}
	if !sameMigrationOwnershipDescriptor(old, requested) {
		return false
	}
	for _, r := range old.Receipts {
		if partitionJobReceiptMatches(r, requested, operation) {
			return true
		}
	}
	return false
}

func partitionJobReceiptMatches(r MigrationOwnershipReceipt, requested MigrationOwnershipState, operation MigrationOwnershipOperation) bool {
	return r.Operation == operation && r.CatalogVersion == requested.CatalogVersion && r.ConfigurationIndex == requested.ConfigurationIndex &&
		r.Staged == requested.Staged && bytes.Equal(r.DescriptorHash, requested.DescriptorHash)
}

// Only callers that already validated the real job guard may invoke this.
// External partition controls cannot choose the internal JobOperation opcode.
func syncPartitionJob(s MigrationOwnershipPartitionState, oldLedger, ledger []MigrationOwnershipState, requested MigrationOwnershipState, operation MigrationOwnershipOperation, index, configuration uint64, ownerChangeAllowed bool) (MigrationOwnershipPartitionState, error) {
	if !partitionHasEvidence(s) {
		return s, nil
	}
	if partitionJobReplay(s, oldLedger, requested, operation) {
		return clonePartitionState(s), nil
	}
	if !partitionJobEpochMatches(s, requested, configuration) {
		return s, ErrMigrationOwnership
	}
	state := partitionJobState(ledger, requested.JobID)
	if !MigrationOwnershipEvidenceValid(state) {
		return s, ErrMigrationOwnership
	}
	child := partitionFromLegacy(state)
	replacement, err := selectPartitionJobReplacement(s, oldLedger, child, index, ownerChangeAllowed)
	if err != nil {
		return s, err
	}
	if operation == MigrationOwnershipRefresh {
		var unadopted bool
		child, unadopted, err = refreshPartitionJobChild(s, child, replacement.matched)
		if err != nil || unadopted {
			return clonePartitionState(s), err
		}
	}
	tx, id, err := makePartitionJobTransition(state, operation, replacement.before, child, index)
	if err != nil {
		return s, err
	}
	return appendPartitionReceipt(s, replacement.retained, tx, index, id)
}

func partitionJobEpochMatches(s MigrationOwnershipPartitionState, requested MigrationOwnershipState, configuration uint64) bool {
	return configuration != 0 && requested.ConfigurationIndex == configuration && requested.Generation == s.Generation
}

func partitionJobState(ledger []MigrationOwnershipState, jobID uint64) MigrationOwnershipState {
	for _, p := range ledger {
		if p.JobID == jobID {
			return p
		}
	}
	return MigrationOwnershipState{}
}

type partitionJobReplacement struct {
	before            []MigrationOwnershipPartitionReference
	retained, matched []MigrationOwnershipPartition
}

func selectPartitionJobReplacement(s MigrationOwnershipPartitionState, oldLedger []MigrationOwnershipState, child MigrationOwnershipPartition, index uint64, ownerChangeAllowed bool) (partitionJobReplacement, error) {
	var result partitionJobReplacement
	candidates, err := partitionCandidates(s, oldLedger)
	if err != nil {
		return result, err
	}
	for _, c := range candidates {
		if !rangesOverlap(c.partition, child) {
			if c.reference.LegacyJobID == 0 {
				result.retained = append(result.retained, c.partition)
			}
			continue
		}
		if !validPartitionJobReplacement(c, child, index) {
			return result, ErrMigrationOwnership
		}
		result.before = append(result.before, c.reference)
		result.matched = append(result.matched, c.partition)
		if child.Role == MigrationOwner && c.partition.Role == MigrationNonOwner &&
			(!ownerChangeAllowed || child.CatalogVersion <= c.partition.CatalogVersion) {
			return result, ErrMigrationOwnership
		}
	}
	return result, nil
}

func validPartitionJobReplacement(c partitionCandidate, child MigrationOwnershipPartition, index uint64) bool {
	return partitionRangeContains(child, c.partition) && len(c.reference.AttestationHash) == 32 &&
		c.partition.Generation == child.Generation && c.partition.LocalGroupID == child.LocalGroupID &&
		c.partition.MinWriteTSExclusive <= child.MinWriteTSExclusive && c.partition.CatalogVersion <= child.CatalogVersion &&
		c.partition.ConfigurationIndex <= child.ConfigurationIndex && c.partition.AppliedIndex <= index
}

func partitionRangeContains(outer, inner MigrationOwnershipPartition) bool {
	return bytes.Compare(outer.RouteStart, inner.RouteStart) <= 0 &&
		(len(outer.RouteEnd) == 0 || (len(inner.RouteEnd) != 0 && bytes.Compare(inner.RouteEnd, outer.RouteEnd) <= 0))
}

func refreshPartitionJobChild(s MigrationOwnershipPartitionState, child MigrationOwnershipPartition, matched []MigrationOwnershipPartition) (MigrationOwnershipPartition, bool, error) {
	// An unadopted legacy interval remains in the per-job view. An adopted
	// interval must be exactly one unchanged active descriptor, never first
	// match by JobID among multiple descendants.
	active := 0
	for _, p := range s.Partitions {
		if p.MigrationJobID == child.MigrationJobID {
			active++
		}
	}
	if active == 0 {
		return child, true, nil
	}
	if active != 1 || len(matched) != 1 || len(matched[0].PartitionID) == 0 {
		return child, false, ErrMigrationOwnership
	}
	old := matched[0]
	if !samePartitionJobDescriptor(old, child) || !inheritedPartition(old, child) {
		return child, false, ErrMigrationOwnership
	}
	child.PartitionID = bytes.Clone(old.PartitionID)
	return child, false, nil
}

func samePartitionJobDescriptor(a, b MigrationOwnershipPartition) bool {
	return bytes.Equal(a.RouteStart, b.RouteStart) && bytes.Equal(a.RouteEnd, b.RouteEnd) &&
		a.RouteID == b.RouteID && a.ParentRouteID == b.ParentRouteID && bytes.Equal(a.DescriptorHash, b.DescriptorHash)
}

func makePartitionJobTransition(state MigrationOwnershipState, operation MigrationOwnershipOperation, before []MigrationOwnershipPartitionReference, child MigrationOwnershipPartition, index uint64) (MigrationOwnershipPartitionTransition, []byte, error) {
	// Bind the exact operation origins AND the new full attestation. In
	// particular distinct configuration-bound refresh attempts cannot rewrite
	// the operation receipt from a previous configuration.
	identity, err := partitionDigest("elastickv/ownership-partition/job/v1", struct {
		Operation MigrationOwnershipOperation
		State     MigrationOwnershipState
	}{operation, state})
	if err != nil {
		return MigrationOwnershipPartitionTransition{}, nil, errors.WithStack(err)
	}
	tx := MigrationOwnershipPartitionTransition{OperationID: "job/" + hex.EncodeToString(identity), Operation: MigrationOwnershipPartitionJobOperation,
		Generation: state.Generation, LocalGroupID: state.LocalGroupID, CatalogVersion: state.CatalogVersion,
		ConfigurationIndex: state.ConfigurationIndex, Before: before, After: []MigrationOwnershipPartition{child}}
	if operation != MigrationOwnershipRefresh {
		id, hashErr := MigrationOwnershipPartitionIDFor(tx, 0)
		if hashErr != nil {
			return tx, nil, hashErr
		}
		tx.After[0].PartitionID = id
	}
	receiptID, err := MigrationOwnershipPartitionReceiptIDFor(tx)
	if err != nil {
		return tx, nil, err
	}
	tx.After[0].AppliedIndex, tx.After[0].ReceiptID = index, bytes.Clone(receiptID)
	return tx, receiptID, nil
}
