package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

// An import changes only private staged keys. Maintenance which publishes or
// removes live keys is checked against the concrete bounded batch instead.
func (f *kvFSM) onlineMaintenanceGuard(ctx context.Context, cleanup *pb.CleanupMigrationRequest) (*store.OnlineMaintenanceGuard, error) {
	sessions, err := f.onlineMigrationSessions(ctx)
	if err != nil || len(sessions) == 0 {
		return nil, err
	}
	protected := sessions[:0]
	for _, session := range sessions {
		if cleanup != nil && f.onlineCleanupAuthorized(cleanup, session) {
			view, err := readMigrationServingState(ctx, f.store)
			if err != nil {
				return nil, err
			}
			if onlineCleanupFencesMatch(view.Readiness, cleanup) {
				continue
			}
		}
		protected = append(protected, session)
	}
	return &store.OnlineMaintenanceGuard{Sessions: protected, Intersects: onlineMaintenanceIntersects}, nil
}

func onlineMaintenanceIntersects(key []byte, session store.OnlineMigrationSession) bool {
	start, end := readinessRouteRangeForScan(key, nextScanCursor(key))
	return onlineLegacyKeyIntersects(key, session) || routeRangeIntersects(start, end, session.RouteStart, session.RouteEnd)
}

// The runner derives this NONOWNER proof from the committed post-cutover
// descriptor after route removal and read drain. Bind it to this exact job,
// generation, group, range and timestamp before allowing source deletion.
func (f *kvFSM) onlineCleanupAuthorized(req *pb.CleanupMigrationRequest, session store.OnlineMigrationSession) bool {
	if !session.Sealed || req.GetJobId() != session.Identity.JobID || !req.GetRequireConfigurationIndex() || req.GetExpectedConfigurationIndex() != f.AppliedConfigurationIndex() {
		return false
	}
	proof, err := MigrationOwnershipStateFromProto(req.GetJobId(), req.GetOwnership())
	return err == nil && onlineSourceRemovalProof(proof, session, f.shardGroupID) &&
		proof.MinWriteTSExclusive == req.GetMaxCommitTs() &&
		onlineRangeMatches(req.GetRouteStart(), req.GetRouteEnd(), session)
}

func onlineRangeMatches(start, end []byte, session store.OnlineMigrationSession) bool {
	return bytes.Equal(start, session.RouteStart) && bytes.Equal(end, session.RouteEnd)
}

func onlineSourceRemovalProof(proof store.MigrationOwnershipState, session store.OnlineMigrationSession, group uint64) bool {
	return proof.Role == store.MigrationNonOwner && proof.LocalGroupID == session.Identity.SourceGroupID &&
		proof.LocalGroupID == group && proof.OwnerGroupID != 0 && proof.OwnerGroupID != group &&
		proof.WriteFloorKnown && proof.MinWriteTSExclusive != 0 &&
		store.OnlineMigrationLineage(proof.Generation) == session.Identity.Lineage &&
		onlineRangeMatches(proof.RouteStart, proof.RouteEnd, session)
}

func onlineCleanupFencesMatch(states []store.TargetStagedReadinessState, req *pb.CleanupMigrationRequest) bool {
	for _, state := range states {
		if state.JobID == req.GetJobId() && state.MigrationJobID == req.GetJobId() &&
			onlineSourceReadWriteFenced(state, req) && bytes.Equal(req.GetRouteStart(), state.RouteStart) &&
			bytes.Equal(req.GetRouteEnd(), state.RouteEnd) {
			return true
		}
	}
	return false
}

func onlineSourceReadWriteFenced(state store.TargetStagedReadinessState, req *pb.CleanupMigrationRequest) bool {
	return state.Armed && state.SourceWriteFence && state.SourceReadFence && state.ExpectedCutoverVersion > 0 &&
		req.GetOwnership().GetCatalogVersion() >= state.ExpectedCutoverVersion && req.GetMaxCommitTs() == state.MinWriteTSExclusive
}
