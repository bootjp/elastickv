package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// ErrMigrationValueTooLarge applies only while a source write tracker owns
// the moving range. It does not change the store's ordinary value-size limit.
var ErrMigrationValueTooLarge = errors.New("value exceeds active migration transfer budget")

func (f *kvFSM) verifyMigrationValueAdmission(ctx context.Context, req *pb.Request) error {
	if req.GetIsTxn() && (req.GetPhase() == pb.Phase_COMMIT || req.GetPhase() == pb.Phase_ABORT) {
		return nil
	}
	for _, mut := range req.GetMutations() {
		if mut == nil || mut.GetOp() != pb.Op_PUT || isTxnInternalKey(mut.GetKey()) ||
			store.MigrationVersionFitsTransfer(mut.GetKey(), mut.GetValue()) {
			continue
		}
		if err := f.verifyMigrationValueForMutation(ctx, mut); err != nil {
			return err
		}
	}
	return nil
}

func (f *kvFSM) verifyMigrationValueForMutation(ctx context.Context, mut *pb.Mutation) error {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return errors.WithStack(errors.Mark(err, errMigrationTrackerPersistence))
	}
	for _, state := range states {
		if state.Armed && state.TrackWrites && migrationMutationsIntersect([]*pb.Mutation{mut}, state.RouteStart, state.RouteEnd) {
			return errors.Wrapf(ErrMigrationValueTooLarge, "job %d key %q", state.JobID, mut.GetKey())
		}
	}
	return nil
}
