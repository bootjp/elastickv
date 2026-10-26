package filesystem

import (
	"context"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	unlinkIntentVersion = 1
	unlinkResultSuccess = 0
	unlinkResultMissing = 1
	unlinkResultNotDir  = 2
	unlinkResultIsDir   = 3
	unlinkEffectLimit   = 64 << 10
	unlinkIntentHeader  = 9
)

// UnlinkWithOperation records one file removal and its result under id.
// Directory removal has a separate emptiness predicate and planner.
func (s *Service) UnlinkWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte) error {
	if err := validateName(name); err != nil {
		return err
	}
	intent := make([]byte, unlinkIntentHeader+len(name))
	intent[0] = unlinkIntentVersion
	binary.BigEndian.PutUint64(intent[1:9], parent)
	copy(intent[unlinkIntentHeader:], name)
	journal, err := NewOperationJournal(s)
	if err != nil {
		return err
	}
	admitted, err := journal.Admit(ctx, id, intent, 1, unlinkEffectLimit)
	if err != nil {
		return err
	}
	if admitted.State != OperationPending || admitted.AttemptID != [16]byte{} {
		return resolveUnlinkOperation(ctx, journal, id)
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	if err != nil {
		return err
	}
	return s.executeUnlinkOperation(ctx, journal, id, attempt, parent, name, intent)
}

func (s *Service) executeUnlinkOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, parent uint64, name, intent []byte) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		err := s.publishUnlinkOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, parent, name, intent)
		if !isDefinitiveWriteConflict(err) {
			return err
		}
		attempt, err = journal.AdvanceGeneration(deadlineCtx, id, attempt.AttemptID)
		if err != nil {
			return err
		}
		if retry == writeConflictRetryLimit-1 {
			return errors.WithStack(ErrContentionLimit)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishUnlinkOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, parent uint64, name, intent []byte) error {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return err
	}
	plan, cleanup, semanticErr, err := s.planUnlinkOperation(ctx, snapshot, parent, name, intent)
	if err != nil {
		return err
	}
	if semanticErr != nil {
		return rejectUnlinkOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		err = decodeUnlinkOperationResult(record)
	} else if !isDefinitiveWriteConflict(err) {
		err = resolveUnlinkAttempt(ctx, journal, id, attempt)
	}
	if err == nil {
		s.reclaimCommittedOrphanBestEffort(ctx, cleanup)
	}
	return err
}

func (s *Service) planUnlinkOperation(ctx context.Context, snapshot operationPlanningSnapshot, parent uint64, name, intent []byte) (operationAttemptPlan, *chunkDeletePlan, error, error) {
	ts := snapshot.ReadTS
	entryKey := fskeys.DirEntryKey(parent, name)
	plan := operationAttemptPlan{Intent: intent, ReadTS: ts, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(parent), entryKey}}
	parentMeta, semanticErr, err := s.unlinkParentAt(ctx, parent, ts)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	if semanticErr != nil {
		return plan, nil, semanticErr, nil
	}
	meta, targetKey, semanticErr, err := s.unlinkFileAt(ctx, parent, name, ts)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	if len(targetKey) != 0 {
		plan.ReadKeys = append(plan.ReadKeys, targetKey)
	}
	if semanticErr != nil {
		return plan, nil, semanticErr, nil
	}
	if err = s.ensureActiveHomeAt(ctx, meta, ts); err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	now := s.now().UnixNano()
	parentMeta.MtimeNsec, parentMeta.CtimeNsec = now, now
	fileElems, fileReads, delta, cleanup, err := s.unlinkFileTxnParts(ctx, ts, meta, now)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	parentEffects, err := unlinkParentEffects(parent, parentMeta, now)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Effects = append([]*kv.Elem[kv.OP]{{Op: kv.Del, Key: entryKey}}, fileElems...)
	plan.Effects = append(plan.Effects, parentEffects...)
	plan.ReadKeys = append(plan.ReadKeys, fileReads...)
	plan.Effects, plan.ReadKeys, err = s.appendUsageUpdate(ctx, ts, plan.Effects, plan.ReadKeys, delta)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Result = []byte{unlinkResultSuccess}
	return plan, cleanup, nil, nil
}

func unlinkParentEffects(parent uint64, meta *InodeMeta, now int64) ([]*kv.Elem[kv.OP], error) {
	parentElem, err := putElem(fskeys.InodeKey(parent), meta)
	if err != nil {
		return nil, err
	}
	versionElem, err := putElem(fskeys.DirVersionKey(parent), unixNsecVersion(now))
	if err != nil {
		return nil, err
	}
	return []*kv.Elem[kv.OP]{parentElem, versionElem}, nil
}

func (s *Service) unlinkParentAt(ctx context.Context, parent, ts uint64) (*InodeMeta, error, error) {
	meta, err := s.inodeAt(ctx, parent, ts)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if meta.Type != TypeDirectory {
		return nil, ErrNotDir, nil
	}
	return meta, nil, nil
}

func (s *Service) unlinkFileAt(ctx context.Context, parent uint64, name []byte, ts uint64) (*InodeMeta, []byte, error, error) {
	entry, err := s.dirEntryAt(ctx, parent, name, ts)
	if errors.Is(err, ErrNotFound) {
		return nil, nil, ErrNotFound, nil
	}
	if err != nil {
		return nil, nil, nil, err
	}
	targetKey := fskeys.InodeKey(entry.Inode)
	meta, err := s.inodeAt(ctx, entry.Inode, ts)
	if errors.Is(err, ErrNotFound) {
		return nil, targetKey, ErrNotFound, nil
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if meta.Type == TypeDirectory {
		return nil, targetKey, ErrIsDir, nil
	}
	return meta, targetKey, nil, nil
}

func decodeUnlinkOperationResult(record operationRecord) error {
	if len(record.Result) != 1 {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	switch record.Result[0] {
	case unlinkResultSuccess:
		if record.State == OperationCommitted {
			return nil
		}
	case unlinkResultMissing:
		if record.State == OperationRejected {
			return errors.WithStack(ErrNotFound)
		}
	case unlinkResultNotDir:
		if record.State == OperationRejected {
			return errors.WithStack(ErrNotDir)
		}
	case unlinkResultIsDir:
		if record.State == OperationRejected {
			return errors.WithStack(ErrIsDir)
		}
	}
	return errors.WithStack(ErrOperationRecordCorrupt)
}

func resolveUnlinkOperation(ctx context.Context, journal *OperationJournal, id OperationID) error {
	record, err := journal.Resolve(ctx, id)
	if errors.Is(err, ErrOperationPending) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if err != nil {
		return err
	}
	return decodeUnlinkOperationResult(record)
}

func resolveUnlinkAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) error {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeUnlinkOperationResult(record)
	}
	return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func rejectUnlinkOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) error {
	var status byte
	switch {
	case errors.Is(semanticErr, ErrNotFound):
		status = unlinkResultMissing
	case errors.Is(semanticErr, ErrNotDir):
		status = unlinkResultNotDir
	case errors.Is(semanticErr, ErrIsDir):
		status = unlinkResultIsDir
	default:
		return semanticErr
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent,
		[]byte{status}, plan.ReadKeys)
	if err != nil {
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !IsWriteConflict(err) && !errors.Is(err, ErrOperationGeneration) {
			return err
		}
		return resolveUnlinkAttempt(ctx, journal, id, attempt)
	}
	return decodeUnlinkOperationResult(record)
}
