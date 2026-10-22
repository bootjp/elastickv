package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (f *kvFSM) withMigrationOwnershipEvidence(ctx context.Context, req *pb.Request) (context.Context, error) {
	view, err := readMigrationServingState(ctx, f.store)
	if err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	if len(view.Ownership) == 0 {
		return ctx, nil
	}
	if drain, err := f.matchesMigrationResolution(ctx, req); drain || err != nil {
		return ctx, err
	}
	proofs, version, err := migrationRequestOwnershipProofs(view.Ownership, req, f.shardGroupID)
	if err != nil || len(proofs) == 0 {
		return ctx, err
	}
	history, err := migrationOwnershipHistory(proofs, version)
	if err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	return context.WithValue(ctx, migrationApplyRoutesKey{}, history), nil
}

func migrationRequestOwnershipProofs(states []store.MigrationOwnershipState, req *pb.Request, groupID uint64) ([]store.MigrationOwnershipState, uint64, error) {
	ranges, err := migrationServingFootprint(req)
	if err != nil || len(ranges) == 0 {
		return nil, 0, err
	}
	version, pinned := DecodeObservedRouteVersion(req.GetObservedRouteVersion())
	if !pinned || version < migrationServingFloor(states) {
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
	for _, mut := range mutations {
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
	if !found || lock.StartTS != req.GetTs() || !bytes.Equal(lock.PrimaryKey, meta.PrimaryKey) {
		return false, nil
	}
	if req.GetPhase() == pb.Phase_COMMIT && lock.CommitTS != 0 && lock.CommitTS != meta.CommitTS {
		return false, nil
	}
	return true, nil
}
