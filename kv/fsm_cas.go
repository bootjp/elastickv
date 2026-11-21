package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// ErrCASMismatch means the latest logical record failed the exact-version
// predicate at the replicated commit timestamp.
var ErrCASMismatch = errors.New("compare-and-swap version mismatch")

func requestUsesCAS(req *pb.Request) bool {
	for _, mut := range req.GetMutations() {
		if usesCAS(mut) {
			return true
		}
	}
	return false
}

func usesCAS(mut *pb.Mutation) bool {
	return mut.GetOp() == pb.Op_COMPARE_AND_SWAP || mut.GetExpectedCommitTs() != 0
}

func (f *kvFSM) applyCASEntry(ctx context.Context, reqs []*pb.Request, apply func(context.Context, *pb.Request) error) any {
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok {
		return casEntryHalt(store.ErrNotSupported)
	}
	resp := &fsmApplyResponse{results: make([]error, len(reqs))}
	f.beginDeferredApplyNotices()
	err := applier.ApplyRaftEntry(ctx, f.pendingApplyIdx, func(ctx context.Context) error {
		return f.stageCASEntry(ctx, reqs, resp.results, apply)
	})
	if err != nil {
		f.discardDeferredApplyNotices()
		return casEntryHalt(err)
	}
	f.flushDeferredApplyNotices()
	if len(reqs) == 1 {
		return resp.results[0]
	}
	for _, err := range resp.results {
		if err != nil {
			return resp
		}
	}
	return nil
}

func (f *kvFSM) stageCASEntry(ctx context.Context, reqs []*pb.Request, results []error, apply func(context.Context, *pb.Request) error) error {
	replayed, err := store.StageRaftEntryProgress(ctx, f.pendingApplyIdx)
	if err != nil || replayed {
		return errors.WithStack(err)
	}
	for i, req := range reqs {
		err := apply(ctx, req)
		if err != nil && !isCASApplyVerdict(err) {
			return errors.Wrapf(err, "CAS request %d", i)
		}
		results[i] = err
	}
	return nil
}

func (f *kvFSM) applyFramedCAS(ctx context.Context, req *pb.Request) error {
	mut := req.Mutations[1]
	// A committed owner change or write fence takes precedence over the
	// more general exclusion of ranges with migration history.
	if err := f.checkAppliedVersionCASRoute(ctx, req.GetVersionCasGroupId(), req.GetObservedRouteVersion(), mut.Key); err != nil {
		return err
	}
	if err := f.verifySourceWriteFenceForRange(ctx, mut.Key, nextScanCursor(mut.Key)); err != nil {
		return err
	}
	if err := checkVersionCASMigrationRange(ctx, f.store, mut.Key); err != nil {
		return err
	}
	ctx, commitTS, err := f.prepareRequestForApply(ctx, req)
	if err != nil {
		return err
	}
	// The catalog and migration guards proved this is an initial range
	// without staged visibility aliases.
	return f.compareAndStageCAS(ctx, mut, nil, req.GetTs(), commitTS)
}

func casEntryHalt(err error) any {
	if classified := haltResponseFor(err); classified != nil {
		err = classified.HaltApply()
	}
	return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "kv/fsm: apply CAS entry"))
}

func isCASApplyVerdict(err error) bool {
	if haltResponseFor(err) != nil {
		return false
	}
	return isRawApplyVerdict(err) || errors.Is(err, ErrCASMismatch) ||
		errors.Is(err, ErrTxnCommitTSRequired) || errors.Is(err, ErrVersionCASUnavailable)
}

func (f *kvFSM) compareAndStageCAS(ctx context.Context, mut *pb.Mutation, readKeys [][]byte, startTS, commitTS uint64) error {
	_, _, _, locked, err := f.txnLockForMutationKey(ctx, mut.Key, readKeys)
	if err != nil {
		return err
	}
	if locked {
		return NewTxnLockedError(mut.Key)
	}
	watermark, err := store.RaftEntryCommitWatermark(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if commitTS <= watermark {
		return errors.WithStack(store.NewWriteConflictError(mut.Key))
	}
	head, err := latestCASRecord(ctx, mut.Key, readKeys)
	if err != nil {
		return err
	}
	if !casRecordMatches(head, mut.ExpectedCommitTs, commitTS) {
		return ErrCASMismatch
	}
	// The token matches. A start timestamp behind that version still fails OCC.
	if head.TS > startTS {
		return errors.WithStack(store.NewWriteConflictError(mut.Key))
	}
	return f.stageCASReplacement(ctx, mut, readKeys, startTS, commitTS, head.ExpireAt)
}

func casRecordMatches(head store.VersionedValue, expectedTS, commitTS uint64) bool {
	return head.TS == expectedTS && !head.Tombstone && (head.ExpireAt == 0 || head.ExpireAt > commitTS)
}

func latestCASRecord(ctx context.Context, key []byte, readKeys [][]byte) (store.VersionedValue, error) {
	keys := append([][]byte{key}, stagedVisibilityKeysForReadAlias(readKeys, key, key)...)
	var head store.VersionedValue
	for _, key := range keys {
		record, err := store.RaftEntryLatestRecord(ctx, key)
		if errors.Is(err, store.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return store.VersionedValue{}, errors.WithStack(err)
		}
		// When versions tie, the staged alias wins as on the point-read path.
		if record.TS >= head.TS {
			head = record
		}
	}
	return head, nil
}

func (f *kvFSM) stageCASReplacement(ctx context.Context, mut *pb.Mutation, readKeys [][]byte, startTS, commitTS, expireAt uint64) error {
	muts := []*store.KVPairMutation{{Op: store.OpTypePut, Key: mut.Key, Value: mut.Value, ExpireAt: expireAt}}
	if err := f.verifyMigrationTransferSize(ctx, muts); err != nil {
		return err
	}
	if err := f.store.ApplyMutationsRaftAtFenced(ctx, muts, readKeys, startTS, commitTS, f.pendingApplyIdx); err != nil {
		return errors.WithStack(err)
	}
	if err := f.recordMigrationWrite(ctx, []*pb.Mutation{mut}, commitTS); err != nil {
		return err
	}
	f.notifyApplyObserver(commitTS, pb.Op_PUT, mut.Key)
	return nil
}
