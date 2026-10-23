package kv

import (
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// ErrMigrationTransferTooLarge is a deterministic rejection of new data in
// an explicitly guarded moving interval, not a replica-local apply failure.
var ErrMigrationTransferTooLarge = errors.Wrap(store.ErrValueTooLarge, "new version exceeds moving-range transfer budget")

// Validate the actual rows, including encoded PREPARE locks and intents, before
// recording tracker progress or applying any row of the request. COMMIT/ABORT
// intentionally do not call this: already authenticated old resolutions drain.
func (f *kvFSM) verifyMigrationTransferSize(ctx context.Context, muts []*store.KVPairMutation) error {
	// Most requests contain only small rows. Avoid a metadata read entirely
	// unless a row can violate the per-version transfer budget.
	var states []store.TargetStagedReadinessState
	loaded := false
	for _, mut := range muts {
		if mut == nil || store.MigrationVersionFitsTransfer(mut.Key, mut.Value) {
			continue
		}
		if !loaded {
			reader, ok := f.store.(store.MigrationTargetReadinessReader)
			if !ok {
				return nil // Stores without readiness cannot activate this guard.
			}
			var err error
			states, err = reader.MigrationTargetReadinessStates(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			loaded = true
		}
		if jobID, guarded := transferSizeGuardJob(states, mut.Key); guarded {
			return errors.Wrapf(ErrMigrationTransferTooLarge,
				"migration job %d: key bytes %d, encoded value bytes %d", jobID, len(mut.Key), len(mut.Value))
		}
	}
	return nil
}

func transferSizeGuardJob(states []store.TargetStagedReadinessState, key []byte) (uint64, bool) {
	if embedded, ok := txnRouteKey(key); ok {
		key = embedded
	}
	start, end := readinessRouteRangeForScan(key, nextScanCursor(key))
	for _, state := range states {
		if state.Armed && state.TransferSizeGuardVersion != 0 && routeRangeIntersects(start, end, state.RouteStart, state.RouteEnd) {
			return state.JobID, true
		}
	}
	return 0, false
}
