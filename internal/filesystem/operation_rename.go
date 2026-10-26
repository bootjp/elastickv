package filesystem

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	renameIntentVersion    = 1
	renameEffectLimit      = 64 << 10
	renameResultSuccess    = 0
	renameResultMissing    = 1
	renameResultNotDir     = 2
	renameResultIsDir      = 3
	renameResultNotEmpty   = 4
	renameResultContention = 5
)

type renameOperationIntent struct {
	Version uint8  `json:"version"`
	Parent  uint64 `json:"parent"`
	OldName []byte `json:"old_name"`
	NewName []byte `json:"new_name"`
}

// RenameWithOperation journals one same-directory rename and its result.
func (s *Service) RenameWithOperation(ctx context.Context, id OperationID, oldParent uint64, oldName []byte, newParent uint64, newName []byte) error {
	if oldParent != newParent {
		return errors.WithStack(ErrCrossDevice)
	}
	if err := validateName(oldName); err != nil {
		return err
	}
	if err := validateName(newName); err != nil {
		return err
	}
	oldName = bytes.Clone(oldName)
	newName = bytes.Clone(newName)
	intent, err := json.Marshal(renameOperationIntent{
		Version: renameIntentVersion, Parent: oldParent,
		OldName: oldName, NewName: newName,
	})
	if err != nil {
		return errors.Wrap(err, "encode rename intent")
	}
	journal, err := NewOperationJournal(s)
	if err != nil {
		return err
	}
	admitted, err := journal.Admit(ctx, id, intent, 1, renameEffectLimit)
	if err != nil {
		return err
	}
	if admitted.State != OperationPending || admitted.AttemptID != [16]byte{} {
		return resolveRenameOperation(ctx, journal, id)
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	if err != nil {
		return err
	}
	return s.executeRenameOperation(ctx, journal, id, attempt, oldParent, oldName, newName, intent)
}

func (s *Service) executeRenameOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, parent uint64, oldName, newName, intent []byte) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		err := s.publishRenameOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, parent, oldName, newName, intent)
		if !isDefinitiveWriteConflict(err) {
			return err
		}
		attempt, err = journal.AdvanceGeneration(deadlineCtx, id, attempt.AttemptID)
		if err != nil {
			return err
		}
		if retry == writeConflictRetryLimit-1 {
			record, cancelErr := cancelOperationContention(ctx, journal, id, []byte{renameResultContention})
			if cancelErr != nil {
				return cancelErr
			}
			return decodeRenameOperationResult(record)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishRenameOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, parent uint64, oldName, newName, intent []byte) error {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return err
	}
	plan, cleanup, semanticErr, err := s.planRenameOperation(ctx, snapshot, parent, oldName, newName, intent)
	if err != nil {
		return err
	}
	if semanticErr != nil {
		return rejectRenameOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		err = decodeRenameOperationResult(record)
	} else if !isDefinitiveWriteConflict(err) {
		err = resolveRenameAttempt(ctx, journal, id, attempt)
	}
	if err == nil {
		s.reclaimCommittedOrphanBestEffort(ctx, cleanup)
	}
	return err
}

func (s *Service) planRenameOperation(ctx context.Context, snapshot operationPlanningSnapshot, parent uint64, oldName, newName, intent []byte) (operationAttemptPlan, *chunkDeletePlan, error, error) {
	ts := snapshot.ReadTS
	oldKey := fskeys.DirEntryKey(parent, oldName)
	newKey := fskeys.DirEntryKey(parent, newName)
	plan := operationAttemptPlan{Intent: intent, ReadTS: ts, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(parent), oldKey, newKey}}
	parentMeta, semanticErr, err := s.unlinkParentAt(ctx, parent, ts)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	if semanticErr != nil {
		return plan, nil, semanticErr, nil
	}
	oldEntry, err := s.dirEntryAt(ctx, parent, oldName, ts)
	if errors.Is(err, ErrNotFound) {
		return plan, nil, ErrNotFound, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	if bytes.Equal(oldName, newName) {
		plan.Result = []byte{renameResultSuccess}
		return plan, nil, nil, nil
	}
	replacedMeta, err := s.renameReplacedFileMeta(ctx, parent, newName, oldEntry, ts)
	if err != nil {
		return plan, nil, err, nil
	}
	return s.planRenameEffects(ctx, plan, parentMeta, oldEntry, replacedMeta, parent, oldKey, newKey)
}

func (s *Service) planRenameEffects(ctx context.Context, plan operationAttemptPlan, parentMeta *InodeMeta, oldEntry DirEntry, replaced *InodeMeta, parent uint64, oldKey, newKey []byte) (operationAttemptPlan, *chunkDeletePlan, error, error) {
	now := s.now().UnixNano()
	parentMeta.MtimeNsec, parentMeta.CtimeNsec = now, now
	putNew, err := putElem(newKey, DirEntry{Inode: oldEntry.Inode, Type: oldEntry.Type})
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	putVersion, err := putElem(fskeys.DirVersionKey(parent), unixNsecVersion(now))
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Effects = []*kv.Elem[kv.OP]{{Op: kv.Del, Key: oldKey}, putNew, putVersion}
	targetElems, targetReads, delta, cleanup, err := s.renameTargetTxnParts(ctx, plan.ReadTS, parentMeta, replaced, now)
	if errors.Is(err, ErrNotEmpty) {
		if replaced != nil {
			plan.ReadKeys = append(plan.ReadKeys, fskeys.DirVersionKey(replaced.Inode))
		}
		return plan, nil, ErrNotEmpty, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Effects = append(plan.Effects, targetElems...)
	plan.ReadKeys = append(plan.ReadKeys, targetReads...)
	putParent, err := putElem(fskeys.InodeKey(parent), parentMeta)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Effects = append(plan.Effects, putParent)
	plan.Effects, plan.ReadKeys, err = s.appendUsageUpdate(ctx, plan.ReadTS, plan.Effects, plan.ReadKeys, delta)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Result = []byte{renameResultSuccess}
	return plan, cleanup, nil, nil
}

func decodeRenameOperationResult(record operationRecord) error {
	if len(record.Result) != 1 {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationCommitted && record.Result[0] == renameResultSuccess {
		return nil
	}
	if record.State == OperationCancelled {
		return cancelledOperationError(record.Result[0], renameResultContention)
	}
	if record.State != OperationRejected {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	switch record.Result[0] {
	case renameResultMissing:
		return errors.WithStack(ErrNotFound)
	case renameResultNotDir:
		return errors.WithStack(ErrNotDir)
	case renameResultIsDir:
		return errors.WithStack(ErrIsDir)
	case renameResultNotEmpty:
		return errors.WithStack(ErrNotEmpty)
	}
	return errors.WithStack(ErrOperationRecordCorrupt)
}

func resolveRenameOperation(ctx context.Context, journal *OperationJournal, id OperationID) error {
	record, err := journal.Resolve(ctx, id)
	if errors.Is(err, ErrOperationPending) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if err != nil {
		return err
	}
	return decodeRenameOperationResult(record)
}

func resolveRenameAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) error {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeRenameOperationResult(record)
	}
	return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func rejectRenameOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) error {
	var status byte
	switch {
	case errors.Is(semanticErr, ErrNotFound):
		status = renameResultMissing
	case errors.Is(semanticErr, ErrNotDir):
		status = renameResultNotDir
	case errors.Is(semanticErr, ErrIsDir):
		status = renameResultIsDir
	case errors.Is(semanticErr, ErrNotEmpty):
		status = renameResultNotEmpty
	default:
		return semanticErr
	}
	return rejectNamespaceOperation(ctx, journal, id, attempt, plan, status, decodeRenameOperationResult)
}
