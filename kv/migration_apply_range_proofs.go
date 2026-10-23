package kv

import (
	"bytes"
	"slices"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

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
