package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func migrationServingFloor(states []store.MigrationOwnershipState) uint64 {
	var floor uint64
	for _, state := range states {
		if !store.MigrationOwnershipEvidenceValid(state) {
			continue
		}
		floor = max(floor, state.CatalogVersion)
	}
	return floor
}

// Partially covered or partially superseded requests retry as one request.
// An uncovered portion must not acquire a fabricated floor-zero proof.
func migrationServingCertificate(states []store.MigrationOwnershipState, start, end []byte) (store.MigrationOwnershipState, bool, error) {
	var selected store.MigrationOwnershipState
	var intersects bool
	for _, state := range states {
		if !routeRangeIntersects(start, end, state.RouteStart, state.RouteEnd) {
			continue
		}
		intersects = true
		if state.CatalogVersion > selected.CatalogVersion {
			selected = state
		}
	}
	if !intersects {
		return selected, false, nil
	}
	if bytes.Compare(selected.RouteStart, start) > 0 ||
		(len(selected.RouteEnd) > 0 && (end == nil || bytes.Compare(end, selected.RouteEnd) > 0)) {
		return selected, true, errors.WithStack(ErrRouteCutoverPending)
	}
	return selected, true, nil
}

func readMigrationServingState(ctx context.Context, st store.MVCCStore) (store.MigrationServingState, error) {
	if reader, ok := st.(store.MigrationOwnershipStore); ok {
		view, err := reader.MigrationServingState(ctx)
		return view, errors.WithStack(err)
	}
	if reader, ok := st.(store.MigrationTargetReadinessReader); ok {
		guards, err := reader.MigrationTargetReadinessStates(ctx)
		return store.MigrationServingState{Readiness: guards}, errors.WithStack(err)
	}
	return store.MigrationServingState{}, nil
}

func (s *ShardStore) verifyMigrationServingFloor(ctx context.Context, requested uint64) error {
	for _, group := range s.groups {
		if group == nil || group.Store == nil {
			continue
		}
		view, err := readMigrationServingState(ctx, group.Store)
		if err != nil {
			return err
		}
		floor := migrationServingFloor(view.Ownership)
		if floor > s.ReadRouteVersion() || (requested != 0 && requested < floor) {
			return errors.WithStack(ErrRouteCutoverPending)
		}
	}
	return nil
}

// MigrationReadAdmissionReady reports the concrete read gate/store pairing,
// not an environment flag. The Internal runtime separately attests its epoch.
func (s *ShardStore) MigrationReadAdmissionReady(groupID uint64) bool {
	if s == nil || s.engine == nil || groupID == 0 {
		return false
	}
	group := s.groups[groupID]
	if group == nil {
		return false
	}
	_, ok := group.Store.(store.MigrationOwnershipStore)
	return ok
}

func (s *ShardStore) verifyMigrationServingRange(view store.MigrationServingState, route distribution.Route, start, end []byte) error {
	if len(view.Ownership) == 0 {
		return nil
	}
	if s.ReadRouteVersion() < migrationServingFloor(view.Ownership) {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	proof, found, err := migrationServingCertificate(view.Ownership, start, end)
	if err != nil || !found {
		return err
	}
	if proof.LocalGroupID != route.GroupID || proof.Role != store.MigrationOwner {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	if !store.MigrationOwnershipEvidenceValid(proof) {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	return s.verifyMigrationServingOwnerRoutes(proof, route, start, end)
}

func (s *ShardStore) verifyMigrationServingOwnerRoutes(proof store.MigrationOwnershipState, route distribution.Route, start, end []byte) error {
	routes, version, ok := s.readinessProofRoutes(route, start, end)
	if !ok || version < proof.CatalogVersion {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	for _, candidate := range routes {
		if candidate.GroupID != proof.OwnerGroupID || candidate.MinWriteTSExclusive < proof.MinWriteTSExclusive {
			return errors.WithStack(ErrRouteCutoverPending)
		}
	}
	return nil
}

func migrationServingFootprint(req *pb.Request) ([]migrationApplyRange, error) {
	ranges := migrationApplyFootprint(req)
	if !req.GetIsTxn() {
		return ranges, nil
	}
	meta, _, err := extractTxnMeta(req.GetMutations())
	if err != nil {
		return nil, err
	}
	if migrationPrimaryParticipates(req, meta.PrimaryKey) {
		start, end := readinessRouteRangeForScan(meta.PrimaryKey, nextScanCursor(meta.PrimaryKey))
		ranges = append(ranges, migrationApplyRange{start, end})
	}
	return ranges, nil
}

func migrationPrimaryParticipates(req *pb.Request, primary []byte) bool {
	for _, mutation := range req.GetMutations() {
		if mutation != nil && bytes.Equal(mutation.GetKey(), primary) {
			return true
		}
	}
	for _, key := range req.GetReadKeys() {
		if bytes.Equal(key, primary) {
			return true
		}
	}
	return false
}

func migrationApplyOwnershipProofs(states []store.MigrationOwnershipState, ranges []migrationApplyRange, groupID, version uint64) ([]store.MigrationOwnershipState, error) {
	proofs := make([]store.MigrationOwnershipState, 0, len(ranges))
	var uncovered bool
	for _, r := range ranges {
		rangeProofs, err := migrationApplyRangeProofs(states, r, groupID, version)
		if err != nil {
			return nil, err
		}
		if len(rangeProofs) == 0 {
			uncovered = true
			continue
		}
		proofs = append(proofs, rangeProofs...)
	}
	if uncovered && len(proofs) > 0 {
		return nil, errors.WithStack(ErrRouteCutoverPending)
	}
	return proofs, nil
}

func migrationOwnershipHistory(proofs []store.MigrationOwnershipState, version uint64) (RouteHistory, error) {
	routes := make([]distribution.RouteDescriptor, 0, len(proofs))
	seen := make(map[uint64]struct{})
	for _, proof := range proofs {
		if _, duplicate := seen[proof.JobID]; duplicate {
			continue
		}
		seen[proof.JobID] = struct{}{}
		route := distribution.RouteDescriptor{RouteID: proof.JobID, Start: proof.RouteStart, End: proof.RouteEnd,
			GroupID: proof.LocalGroupID, State: distribution.RouteStateActive, MinWriteTSExclusive: proof.MinWriteTSExclusive}
		if proof.Staged {
			route.MigrationJobID, route.StagedVisibilityActive = proof.JobID, true
		}
		routes = append(routes, route)
	}
	engine := distribution.NewEngine()
	if err := engine.ApplySnapshot(distribution.CatalogSnapshot{Version: version, Routes: routes}); err != nil {
		return nil, errors.WithStack(err)
	}
	return WrapDistributionEngine(engine), nil
}
