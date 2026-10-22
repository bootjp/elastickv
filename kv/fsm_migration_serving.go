package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// CheckMigrationRequest performs advisory preproposal admission against the
// group's local durable migration state. req must already have its final
// timestamps, metadata and footprint. The FSM repeats this check at apply;
// a successful check does not authorize a different or later request.
func CheckMigrationRequest(ctx context.Context, localStore store.MVCCStore, groupID uint64, req *pb.Request) error {
	if localStore == nil || req == nil {
		return errors.WithStack(ErrInvalidRequest)
	}
	f := &kvFSM{store: localStore, shardGroupID: groupID}
	_, err := f.withMigrationOwnershipEvidence(ctx, req)
	return err
}

func (f *kvFSM) withMigrationOwnershipEvidence(ctx context.Context, req *pb.Request) (context.Context, error) {
	view, err := readMigrationServingState(ctx, f.store)
	if err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	ranges, err := migrationServingFootprint(req)
	if err != nil {
		return ctx, err
	}
	if !migrationApplyIntersects(view, ranges) {
		return ctx, nil
	}
	if drain, err := f.matchesMigrationResolution(ctx, req); drain || err != nil {
		if err != nil {
			return ctx, err
		}
		return context.WithValue(ctx, migrationResolutionContextKey{}, req), nil
	}
	return f.withMigrationCreationEvidence(ctx, req, view, ranges)
}

func (f *kvFSM) withMigrationCreationEvidence(ctx context.Context, req *pb.Request, view store.MigrationServingState, ranges []migrationApplyRange) (context.Context, error) {
	if !migrationCreationGuardsProven(view, ranges) {
		return ctx, errors.WithStack(ErrRouteCutoverPending)
	}
	proofs, version, err := migrationRequestOwnershipProofs(view.Ownership, req, f.shardGroupID)
	if err != nil {
		return ctx, err
	}
	if len(proofs) == 0 {
		return ctx, errors.WithStack(ErrRouteCutoverPending)
	}
	history, err := migrationOwnershipHistory(proofs, version)
	if err != nil {
		return ctx, errors.Wrap(ErrRouteCutoverPending, "incomplete migration apply history")
	}
	return withApplyRouteHistory(ctx, history), nil
}

type migrationResolutionContextKey struct{}

func migrationResolutionAuthorized(ctx context.Context, req *pb.Request) bool {
	authorized, ok := ctx.Value(migrationResolutionContextKey{}).(*pb.Request)
	return ok && authorized == req
}

func migrationApplyIntersects(view store.MigrationServingState, ranges []migrationApplyRange) bool {
	for _, r := range ranges {
		for _, proof := range view.Ownership {
			if routeRangeIntersects(r.start, r.end, proof.RouteStart, proof.RouteEnd) {
				return true
			}
		}
		for _, guard := range view.Readiness {
			if guard.Armed && routeRangeIntersects(r.start, r.end, guard.RouteStart, guard.RouteEnd) {
				return true
			}
		}
	}
	return false
}

func migrationCreationGuardsProven(view store.MigrationServingState, ranges []migrationApplyRange) bool {
	for _, guard := range view.Readiness {
		if guard.Armed && migrationGuardIntersects(guard, ranges) && !migrationTargetGuardProven(guard, view.Ownership) {
			return false
		}
	}
	return true
}

func migrationGuardIntersects(guard store.TargetStagedReadinessState, ranges []migrationApplyRange) bool {
	for _, r := range ranges {
		if routeRangeIntersects(r.start, r.end, guard.RouteStart, guard.RouteEnd) {
			return true
		}
	}
	return false
}

func migrationTargetGuardProven(guard store.TargetStagedReadinessState, proofs []store.MigrationOwnershipState) bool {
	// Source tracking does not attest the preexisting owner or write floor.
	// A target guard alone only predicts a future cutover, not its publication.
	if guard.TrackWrites || guard.SourceWriteFence || guard.SourceReadFence {
		return false
	}
	for _, proof := range proofs {
		if migrationTargetOwnershipMatches(proof, guard) {
			return true
		}
	}
	return false
}

func migrationTargetOwnershipMatches(proof store.MigrationOwnershipState, guard store.TargetStagedReadinessState) bool {
	return guard.MigrationJobID == guard.JobID && proof.JobID == guard.JobID && proof.Role == store.MigrationOwner && proof.Staged &&
		proof.CatalogVersion == guard.ExpectedCutoverVersion && proof.MinWriteTSExclusive == guard.MinWriteTSExclusive &&
		bytes.Equal(proof.RouteStart, guard.RouteStart) && bytes.Equal(proof.RouteEnd, guard.RouteEnd)
}

func migrationRequestOwnershipProofs(states []store.MigrationOwnershipState, req *pb.Request, groupID uint64) ([]store.MigrationOwnershipState, uint64, error) {
	ranges, err := migrationServingFootprint(req)
	if err != nil || len(ranges) == 0 {
		return nil, 0, err
	}
	if !migrationApplyIntersects(store.MigrationServingState{Ownership: states}, ranges) {
		return nil, 0, nil
	}
	version, pinned := DecodeObservedRouteVersion(req.GetObservedRouteVersion())
	if !pinned {
		return nil, version, errors.WithStack(ErrRouteCutoverPending)
	}
	proofs, err := migrationApplyOwnershipProofs(states, ranges, groupID, version)
	return proofs, version, err
}

func (f *kvFSM) matchesMigrationResolution(ctx context.Context, req *pb.Request) (bool, error) {
	if !req.GetIsTxn() || (req.GetPhase() != pb.Phase_COMMIT && req.GetPhase() != pb.Phase_ABORT) {
		return false, nil
	}
	meta, mutations, err := extractTxnMeta(req.GetMutations())
	if err != nil {
		return false, err
	}
	if len(mutations) == 0 || len(meta.PrimaryKey) == 0 {
		return false, nil
	}
	return f.migrationResolutionMutationsMatch(ctx, req, meta, mutations)
}

func (f *kvFSM) migrationResolutionMutationsMatch(ctx context.Context, req *pb.Request, meta TxnMeta, mutations []*pb.Mutation) (bool, error) {
	for _, mut := range mutations {
		if mut == nil || len(mut.GetKey()) == 0 || isTxnInternalKey(mut.GetKey()) {
			return false, errors.WithStack(ErrInvalidRequest)
		}
		if err := rejectReservedControlMutations([]*pb.Mutation{mut}); err != nil {
			return false, err
		}
		matches, err := f.migrationResolutionKeyMatches(ctx, req, meta, mut.GetKey())
		if err != nil || !matches {
			return false, err
		}
	}
	return true, nil
}

func (f *kvFSM) migrationResolutionKeyMatches(ctx context.Context, req *pb.Request, meta TxnMeta, key []byte) (bool, error) {
	lock, _, _, found, err := f.txnLockForMutationKey(ctx, key, req.GetReadKeys())
	if err != nil {
		return false, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	if !found {
		return f.migrationMissingResolutionKey(ctx, req, meta, key)
	}
	if lock.StartTS != req.GetTs() || !bytes.Equal(lock.PrimaryKey, meta.PrimaryKey) ||
		lock.IsPrimaryKey != bytes.Equal(key, meta.PrimaryKey) {
		return false, nil
	}
	if req.GetPhase() == pb.Phase_COMMIT && lock.CommitTS != 0 && lock.CommitTS != meta.CommitTS {
		return false, errors.WithStack(ErrTxnInvalidMeta)
	}
	return true, nil
}

func (f *kvFSM) migrationMissingResolutionKey(ctx context.Context, req *pb.Request, meta TxnMeta, key []byte) (bool, error) {
	intentKeys := append([][]byte{txnIntentKey(key)}, stagedVisibilityKeysForReadAlias(req.GetReadKeys(), key, txnIntentKey(key))...)
	for _, intentKey := range intentKeys {
		exists, err := f.store.ExistsAt(ctx, intentKey, ^uint64(0))
		if err != nil {
			return false, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
		}
		if exists {
			return false, nil
		}
	}
	if !bytes.Equal(key, meta.PrimaryKey) {
		// No secondary lock/intent means both handlers produce no mutation for
		// this key. It cannot authorize creation of a primary terminal record.
		return true, nil
	}
	return f.migrationPrimaryTerminalMatches(ctx, req, meta)
}

func (f *kvFSM) migrationPrimaryTerminalMatches(ctx context.Context, req *pb.Request, meta TxnMeta) (bool, error) {
	commitTS, committed, err := f.txnCommitTS(ctx, meta.PrimaryKey, req.GetTs())
	if err == nil && !committed {
		commitTS, committed, err = f.stagedTxnCommitTS(ctx, meta.PrimaryKey, req.GetTs(), req.GetReadKeys())
	}
	if err != nil {
		return false, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	if committed {
		if req.GetPhase() == pb.Phase_ABORT {
			return false, errors.WithStack(ErrTxnAlreadyCommitted)
		}
		if commitTS != meta.CommitTS {
			return false, errors.WithStack(ErrTxnInvalidMeta)
		}
		return true, nil
	}
	return f.migrationPrimaryRollbackMatches(ctx, req, meta)
}

func (f *kvFSM) migrationPrimaryRollbackMatches(ctx context.Context, req *pb.Request, meta TxnMeta) (bool, error) {
	key := txnRollbackKey(meta.PrimaryKey, req.GetTs())
	exists, err := f.store.ExistsAt(ctx, key, ^uint64(0))
	if err == nil && !exists {
		exists, err = f.stagedTxnRecordExists(ctx, meta.PrimaryKey, key, req.GetReadKeys())
	}
	if err != nil {
		return false, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	if exists && req.GetPhase() == pb.Phase_COMMIT {
		return false, errors.WithStack(ErrTxnAlreadyAborted)
	}
	return exists, nil
}
