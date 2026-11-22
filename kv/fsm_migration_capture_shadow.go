package kv

import (
	"context"
	"crypto/sha256"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// MigrationCaptureShadowConfig enables diagnostic capture of explicit nonempty
// Raw point entries. It is not a migration capability, active capture session,
// ownership proof or stable source cursor. Unsupported entry shapes take the
// normal apply path and are NOT covered. No server CLI flag enables this mode.
type MigrationCaptureShadowConfig struct {
	SourceGroupID uint64
	JobID         uint64
	Generation    uint64
	Lineage       [32]byte
	Limits        store.MigrationCaptureLimits
}

func WithMigrationCaptureShadow(config MigrationCaptureShadowConfig) FSMOption {
	return func(f *kvFSM) {
		f.migrationCaptureShadow = &config
	}
}

func migrationCaptureShadowEligible(requests []*pb.Request) bool {
	if len(requests) == 0 || len(requests) > store.MaxMigrationCaptureRequests {
		return false
	}
	for _, request := range requests {
		if !migrationCaptureShadowPointRequest(request) {
			return false
		}
	}
	return true
}

func migrationCaptureShadowPointRequest(request *pb.Request) bool {
	if !migrationCaptureShadowHeader(request) {
		return false
	}
	for _, mutation := range request.Mutations {
		if mutation == nil || len(mutation.Key) == 0 || (mutation.Op != pb.Op_PUT && mutation.Op != pb.Op_DEL) {
			return false
		}
	}
	return true
}

func migrationCaptureShadowHeader(request *pb.Request) bool {
	return request != nil && !request.IsTxn && request.Phase == pb.Phase_NONE && request.Ts != 0 && len(request.GetTimestampBindingOperationId()) == 0
}

func (f *kvFSM) applyMigrationCaptureShadow(ctx context.Context, data []byte, requests []*pb.Request) any {
	cfg := f.migrationCaptureShadow
	id := store.MigrationCaptureIdentity{SourceGroupID: cfg.SourceGroupID, JobID: cfg.JobID, Generation: cfg.Generation, Lineage: cfg.Lineage, Index: f.pendingApplyIdx}
	if f.shardGroupID != 0 && f.shardGroupID != cfg.SourceGroupID {
		return haltErr(errors.Mark(store.ErrMigrationCaptureInput, ErrRaftEntryApply))
	}
	results := make([]error, len(requests))
	entered := false
	f.beginDeferredApplyNotices()
	entry, err := store.CaptureMigrationRaftEntry(ctx, f.store, id, sha256.Sum256(data), cfg.Limits, func(tx context.Context, writer *store.MigrationCaptureWriter) error {
		entered = true
		for i, request := range requests {
			if err := writer.Request(func() (store.MigrationCaptureOutcome, error) {
				results[i] = f.applyRequestErr(tx, request)
				return migrationCaptureShadowOutcome(results[i])
			}); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})
	if err != nil {
		f.discardDeferredApplyNotices()
		return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "kv/fsm: shadow capture entry"))
	}
	if !entered {
		results, err = migrationCaptureShadowReplay(entry, len(requests))
		if err != nil {
			f.discardDeferredApplyNotices()
			return haltErr(errors.Mark(err, ErrRaftEntryApply))
		}
	}
	f.flushDeferredApplyNotices()
	return migrationCaptureShadowResponse(results)
}

func migrationCaptureShadowResponse(results []error) any {
	if len(results) == 1 {
		return results[0]
	}
	for _, result := range results {
		if result != nil {
			return &fsmApplyResponse{results: results}
		}
	}
	return nil
}

func migrationCaptureShadowOutcome(err error) (store.MigrationCaptureOutcome, error) {
	if err == nil {
		return store.MigrationCaptureApplied, nil
	}
	if !isolatedMutationFailure(err) || !isRawApplyVerdict(err) {
		return "", errors.WithStack(err)
	}
	if errors.Is(err, store.ErrWriteConflict) {
		return store.MigrationCaptureConflict, nil
	}
	return store.MigrationCaptureRejected, nil
}

func migrationCaptureShadowReplay(entry store.MigrationCaptureEntry, expected int) ([]error, error) {
	if len(entry.Outcomes) != expected {
		return nil, errors.WithStack(store.ErrMigrationCaptureCorrupt)
	}
	results := make([]error, len(entry.Outcomes))
	for i, outcome := range entry.Outcomes {
		switch outcome {
		case store.MigrationCaptureApplied:
		case store.MigrationCaptureConflict:
			results[i] = errors.WithStack(store.ErrWriteConflict)
		case store.MigrationCaptureRejected:
			// The shadow receipt preserves the verdict class, not every
			// protocol-specific reason. Never turn a rejected replay into ACK.
			results[i] = errors.WithStack(ErrTransactionOutcomeUnknown)
		default:
			results[i] = errors.WithStack(ErrTransactionOutcomeUnknown)
		}
	}
	return results, nil
}
