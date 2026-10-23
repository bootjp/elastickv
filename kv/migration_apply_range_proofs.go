package kv

import (
	"bytes"
	"slices"
	"strconv"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Legacy job certificates and active partitions share interval selection, but
// retain separate evidence validation and identities. A staged-data job is not
// a serving partition ID: multiple children can read the same staged lineage.
type migrationAdmissionProof struct {
	store.MigrationOwnershipState
	partition *store.MigrationOwnershipPartition
	identity  string
}

func migrationAdmissionProofs(view store.MigrationServingState) []migrationAdmissionProof {
	proofs := make([]migrationAdmissionProof, 0, len(view.Ownership)+len(view.Partitions.Partitions))
	for _, state := range view.Ownership {
		proofs = append(proofs, migrationAdmissionProof{MigrationOwnershipState: state, identity: "job/" + strconv.FormatUint(state.JobID, 10)})
	}
	for i := range view.Partitions.Partitions {
		p := &view.Partitions.Partitions[i]
		proofs = append(proofs, migrationAdmissionProof{partition: p, identity: "partition/" + string(p.PartitionID),
			MigrationOwnershipState: store.MigrationOwnershipState{
				Generation: p.Generation, JobID: p.MigrationJobID, LocalGroupID: p.LocalGroupID, OwnerGroupID: p.OwnerGroupID,
				CatalogVersion: p.CatalogVersion, ConfigurationIndex: p.ConfigurationIndex, AppliedIndex: p.AppliedIndex,
				RouteStart: p.RouteStart, RouteEnd: p.RouteEnd, RouteID: p.RouteID, ParentRouteID: p.ParentRouteID,
				DescriptorHash: p.DescriptorHash, Role: p.Role, Staged: p.Staged, MinWriteTSExclusive: p.MinWriteTSExclusive,
				WriteFloorKnown: p.WriteFloorKnown, SourceTrackerVersion: p.SourceTrackerVersion, SourceAppliedIndex: p.SourceAppliedIndex,
			}})
	}
	return proofs
}

func (p migrationAdmissionProof) evidenceValid(view store.MigrationServingState) bool {
	if p.partition != nil {
		return store.MigrationOwnershipPartitionEvidenceValid(view.Partitions, *p.partition)
	}
	return store.MigrationOwnershipEvidenceValid(p.MigrationOwnershipState)
}

func migrationAdmissionFloor(view store.MigrationServingState) uint64 {
	floor := migrationServingFloor(view.Ownership)
	for _, partition := range view.Partitions.Partitions {
		floor = max(floor, partition.CatalogVersion)
	}
	return floor
}

func migrationAdmissionBoundaries(proofs []migrationAdmissionProof, r migrationApplyRange) ([][]byte, bool) {
	boundaries := [][]byte{r.start}
	var intersects bool
	for _, proof := range proofs {
		if !routeRangeIntersects(r.start, r.end, proof.RouteStart, proof.RouteEnd) {
			continue
		}
		intersects = true
		for _, boundary := range [][]byte{proof.RouteStart, proof.RouteEnd} {
			if bytes.Compare(boundary, r.start) > 0 && (len(r.end) == 0 || bytes.Compare(boundary, r.end) < 0) {
				boundaries = append(boundaries, boundary)
			}
		}
	}
	slices.SortFunc(boundaries, bytes.Compare)
	return slices.CompactFunc(boundaries, bytes.Equal), intersects
}

func migrationAdmissionCertificate(proofs []migrationAdmissionProof, start, end []byte) (migrationAdmissionProof, error) {
	var selected migrationAdmissionProof
	for _, proof := range proofs {
		if !routeRangeIntersects(start, end, proof.RouteStart, proof.RouteEnd) {
			continue
		}
		if proof.CatalogVersion == selected.CatalogVersion && proof.identity != selected.identity {
			return selected, errors.WithStack(ErrRouteCutoverPending)
		}
		if proof.CatalogVersion > selected.CatalogVersion {
			selected = proof
		}
	}
	if selected.identity == "" || bytes.Compare(selected.RouteStart, start) > 0 ||
		(len(selected.RouteEnd) > 0 && (len(end) == 0 || bytes.Compare(end, selected.RouteEnd) > 0)) {
		return selected, errors.WithStack(ErrRouteCutoverPending)
	}
	return selected, nil
}

func migrationAdmissionRangeProofs(view store.MigrationServingState, r migrationApplyRange) ([]migrationAdmissionProof, error) {
	for _, legacy := range view.Ownership {
		if routeRangeIntersects(r.start, r.end, legacy.RouteStart, legacy.RouteEnd) && !store.MigrationOwnershipEvidenceValid(legacy) {
			return nil, errors.WithStack(ErrRouteCutoverPending)
		}
	}
	all := migrationAdmissionProofs(view)
	boundaries, intersects := migrationAdmissionBoundaries(all, r)
	if !intersects {
		return nil, nil
	}
	proofs := make([]migrationAdmissionProof, 0, len(boundaries))
	for i, start := range boundaries {
		end := r.end
		if i+1 < len(boundaries) {
			end = boundaries[i+1]
		}
		proof, err := migrationAdmissionCertificate(all, start, end)
		if err != nil {
			return nil, err
		}
		if !proof.evidenceValid(view) {
			return nil, errors.WithStack(ErrRouteCutoverPending)
		}
		proofs = append(proofs, proof)
	}
	return proofs, nil
}

func migrationAdmissionOwnershipProofs(view store.MigrationServingState, ranges []migrationApplyRange, groupID, version uint64) ([]migrationAdmissionProof, error) {
	var proofs []migrationAdmissionProof
	var uncovered bool
	for _, r := range ranges {
		selected, err := migrationAdmissionRangeProofs(view, r)
		if err != nil {
			return nil, err
		}
		for _, proof := range selected {
			if proof.Role != store.MigrationOwner || proof.LocalGroupID != groupID || proof.CatalogVersion != version {
				return nil, errors.WithStack(ErrRouteCutoverPending)
			}
		}
		uncovered = uncovered || len(selected) == 0
		proofs = append(proofs, selected...)
	}
	if uncovered && len(proofs) != 0 {
		return nil, errors.WithStack(ErrRouteCutoverPending)
	}
	return proofs, nil
}

// Partition only the checked interval, never the certificates themselves.
// Every segment must select its newest complete certificate; holes, stale
// versions and superseding NONOWNER records cannot borrow adjacent authority.
func migrationApplyRangeProofs(states []store.MigrationOwnershipState, r migrationApplyRange, groupID, version uint64) ([]store.MigrationOwnershipState, error) {
	boundaries, intersects := migrationApplyProofBoundaries(states, r)
	if !intersects {
		return nil, nil
	}
	proofs := make([]store.MigrationOwnershipState, 0, len(boundaries))
	for i, start := range boundaries {
		end := r.end
		if i+1 < len(boundaries) {
			end = boundaries[i+1]
		}
		proof, found, err := migrationServingCertificate(states, start, end)
		if err != nil {
			return nil, err
		}
		if !found || !migrationApplyProofAuthorizes(proof, groupID, version) {
			return nil, errors.WithStack(ErrRouteCutoverPending)
		}
		proofs = append(proofs, proof)
	}
	return proofs, nil
}

func migrationApplyProofAuthorizes(proof store.MigrationOwnershipState, groupID, version uint64) bool {
	return store.MigrationOwnershipEvidenceValid(proof) && proof.LocalGroupID == groupID &&
		proof.Role == store.MigrationOwner && proof.CatalogVersion == version
}

func migrationApplyProofBoundaries(states []store.MigrationOwnershipState, r migrationApplyRange) ([][]byte, bool) {
	boundaries := [][]byte{r.start}
	var intersects bool
	for _, proof := range states {
		if !routeRangeIntersects(r.start, r.end, proof.RouteStart, proof.RouteEnd) {
			continue
		}
		intersects = true
		for _, boundary := range [][]byte{proof.RouteStart, proof.RouteEnd} {
			if bytes.Compare(boundary, r.start) > 0 && (len(r.end) == 0 || bytes.Compare(boundary, r.end) < 0) {
				boundaries = append(boundaries, boundary)
			}
		}
	}
	slices.SortFunc(boundaries, bytes.Compare)
	return slices.CompactFunc(boundaries, bytes.Equal), intersects
}
