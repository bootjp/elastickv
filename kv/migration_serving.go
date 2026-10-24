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

func readMigrationServingState(ctx context.Context, st store.MVCCStore) (store.MigrationServingState, error) {
	if reader, ok := st.(store.MigrationServingAdmissionReader); ok {
		view, err := reader.MigrationServingAdmissionState(ctx)
		return view, errors.WithStack(err)
	}
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

type migrationServingReadVersionKey struct{}

func migrationServingReadContext(ctx context.Context, version uint64) context.Context {
	if version == 0 {
		return ctx
	}
	if prior, ok := ctx.Value(migrationServingReadVersionKey{}).(uint64); ok && prior <= version {
		return ctx
	}
	return context.WithValue(ctx, migrationServingReadVersionKey{}, version)
}

func (s *ShardStore) verifyMigrationServingReadFloor(ctx context.Context, view store.MigrationServingState) error {
	floor := migrationAdmissionFloor(view)
	requested, _ := ctx.Value(migrationServingReadVersionKey{}).(uint64)
	if floor > s.ReadRouteVersion() || (requested != 0 && requested < floor) {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	return nil
}

func (s *ShardStore) verifyMigrationServingFloor(ctx context.Context, requested uint64) error {
	for _, group := range s.groups {
		if group == nil || group.Store == nil {
			continue
		}
		floor, err := migrationStoreServingFloor(ctx, group.Store)
		if err != nil {
			return err
		}
		if floor > s.ReadRouteVersion() || (requested != 0 && requested < floor) {
			return errors.WithStack(ErrRouteCutoverPending)
		}
	}
	return nil
}

func migrationStoreServingFloor(ctx context.Context, st store.MVCCStore) (uint64, error) {
	if reader, ok := st.(store.MigrationServingSummaryReader); ok {
		summary, err := reader.MigrationServingSummary(ctx)
		return summary.CatalogVersionFloor, errors.WithStack(err)
	}
	view, err := readMigrationServingState(ctx, st)
	return migrationAdmissionFloor(view), err
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
	if s.ReadRouteVersion() < migrationAdmissionFloor(view) {
		return errors.WithStack(ErrRouteCutoverPending)
	}
	proofs, err := migrationAdmissionRangeProofs(view, migrationApplyRange{start, end})
	if err != nil {
		return err
	}
	for _, proof := range proofs {
		if proof.LocalGroupID != route.GroupID || proof.Role != store.MigrationOwner {
			return errors.WithStack(ErrRouteCutoverPending)
		}
		proofStart, proofEnd := prefixDeleteRouteIntersection(distribution.Route{Start: proof.RouteStart, End: proof.RouteEnd}, start, end)
		if err := s.verifyMigrationAdmissionOwnerRoutes(proof, route, proofStart, proofEnd); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) verifyMigrationAdmissionOwnerRoutes(proof migrationAdmissionProof, route distribution.Route, start, end []byte) error {
	if err := s.verifyMigrationServingOwnerRoutes(proof.MigrationOwnershipState, route, start, end); err != nil {
		return err
	}
	if proof.partition == nil {
		return nil
	}
	routes, _, _ := s.readinessProofRoutes(route, start, end)
	cursor := start
	for _, candidate := range routes {
		if !migrationAdmissionRouteMatches(proof, candidate, cursor) {
			return errors.WithStack(ErrRouteCutoverPending)
		}
		if len(candidate.End) == 0 || (len(end) != 0 && bytes.Compare(candidate.End, end) >= 0) {
			return nil
		}
		cursor = candidate.End
	}
	return errors.WithStack(ErrRouteCutoverPending)
}

func migrationAdmissionRouteMatches(proof migrationAdmissionProof, route distribution.Route, cursor []byte) bool {
	return bytes.Compare(route.Start, cursor) <= 0 && routeHasStagedVisibility(route) == proof.Staged &&
		(!proof.Staged || route.MigrationJobID == proof.JobID)
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

func migrationAdmissionHistory(proofs []migrationAdmissionProof, version uint64) (RouteHistory, error) {
	routes := make([]distribution.RouteDescriptor, 0, len(proofs))
	seen := make(map[string]bool)
	for _, proof := range proofs {
		if seen[proof.identity] {
			continue
		}
		seen[proof.identity] = true
		routeID := proof.RouteID
		if proof.partition == nil {
			routeID = proof.JobID
		}
		route := distribution.RouteDescriptor{RouteID: routeID, Start: proof.RouteStart, End: proof.RouteEnd,
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
