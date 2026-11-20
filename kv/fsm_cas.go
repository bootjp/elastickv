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

func (f *kvFSM) applyCASEntry(ctx context.Context, reqs []*pb.Request, protectMigrationRange bool) any {
	if protectMigrationRange && f.pendingApplyIdx == 0 {
		return casEntryHalt(errors.New("CAS entry requires a Raft applied index"))
	}
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok {
		return casEntryHalt(store.ErrNotSupported)
	}
	resp := &fsmApplyResponse{results: make([]error, len(reqs))}
	f.beginDeferredApplyNotices()
	err := applier.ApplyRaftEntry(ctx, f.pendingApplyIdx, func(ctx context.Context) error {
		return f.stageCASEntry(ctx, reqs, resp.results, protectMigrationRange)
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

func (f *kvFSM) stageCASEntry(ctx context.Context, reqs []*pb.Request, results []error, protectMigrationRange bool) error {
	replayed, err := store.StageRaftEntryProgress(ctx, f.pendingApplyIdx)
	if err != nil || replayed {
		return errors.WithStack(err)
	}
	if protectMigrationRange {
		// Frame decoding validated the standalone request and mutation layout.
		err := checkVersionCASMigrationRange(ctx, f.store, reqs[0].Mutations[1].Key)
		if errors.Is(err, ErrVersionCASUnavailable) {
			results[0] = err
			return nil
		}
		if err != nil {
			return err
		}
	}
	return f.stageCASRequests(ctx, reqs, results)
}

func (f *kvFSM) stageCASRequests(ctx context.Context, reqs []*pb.Request, results []error) error {
	// The replicated frame only admits a standalone CAS. Keep the scoped
	// path fail-closed to non-CAS requests if used with a decoded batch.
	for _, req := range reqs {
		if !requestUsesCAS(req) {
			for i := range results {
				results[i] = ErrInvalidRequest
			}
			return nil
		}
	}
	for i, req := range reqs {
		err := f.applyPreparedCAS(ctx, req)
		if err != nil && !isCASApplyVerdict(err) {
			return errors.Wrapf(err, "CAS request %d", i)
		}
		results[i] = err
	}
	return nil
}

func (f *kvFSM) applyPreparedCAS(ctx context.Context, req *pb.Request) error {
	if err := f.validateCASForApply(req); err != nil {
		return err
	}
	ctx, commitTS, err := f.prepareRequestForApply(ctx, req)
	if err != nil {
		return err
	}
	return f.handleCASRequest(ctx, req, commitTS)
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
		errors.Is(err, ErrTxnCommitTSRequired) || errors.Is(err, ErrComposed1Violation)
}

func (f *kvFSM) validateCASForApply(r *pb.Request) error {
	if !requestUsesCAS(r) {
		return nil
	}
	if !f.commitTSFenceEnabled.Load() {
		return errors.Wrap(ErrInvalidRequest, "CAS requires the replicated commit timestamp fence")
	}
	return ValidateCASProposalShape([]*pb.Request{r})
}

func (f *kvFSM) handleCASRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	muts := r.Mutations[1:]
	mut := muts[0]
	if err := f.verifyTxnRouteAdmission(ctx, r); err != nil {
		return err
	}
	if _, err := f.uniqueMutationsAboveFloorForRequest(ctx, r, muts, commitTS); err != nil {
		return err
	}
	readKeys, err := f.onePhaseConflictReadKeys(ctx, muts, r.ReadKeys)
	if err != nil {
		return err
	}
	return f.compareAndStageCAS(ctx, mut, readKeys, r.Ts, commitTS)
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
	if head.TS > startTS {
		return errors.WithStack(store.NewWriteConflictError(mut.Key))
	}
	if !casRecordMatches(head, mut.ExpectedCommitTs, commitTS) {
		return ErrCASMismatch
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
