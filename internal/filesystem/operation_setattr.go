package filesystem

import (
	"context"
	"encoding/json"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	setAttrOperationVersion     = 1
	setAttrOperationResultLimit = 512
	setAttrOperationEffectLimit = 256
	setAttrSizeEffectLimit      = 64 << 10
	setAttrResultNotFound       = 1
	setAttrResultIsDir          = 2
	setAttrResultContention     = 3
)

type setAttrOperationIntent struct {
	Version uint8       `json:"version"`
	Inode   uint64      `json:"inode"`
	Mask    SetAttrMask `json:"mask"`
	Attrs   SetAttr     `json:"attrs"`
}

type setAttrOperationResult struct {
	Version uint8 `json:"version"`
	Stat    Stat  `json:"stat"`
	Error   uint8 `json:"error"`
}

// SetAttrWithOperation journals attribute changes and retains the original Stat.
func (s *Service) SetAttrWithOperation(ctx context.Context, id OperationID, inode uint64, mask SetAttrMask, attrs SetAttr) (Stat, error) {
	intent, err := json.Marshal(setAttrOperationIntent{Version: setAttrOperationVersion, Inode: inode, Mask: mask, Attrs: attrs})
	if err != nil {
		return Stat{}, errors.Wrap(err, "encode setattr intent")
	}
	journal, err := NewOperationJournal(s)
	if err != nil {
		return Stat{}, err
	}
	effectLimit := uint32(setAttrOperationEffectLimit)
	if mask.Size {
		effectLimit = setAttrSizeEffectLimit
	}
	admitted, err := journal.Admit(ctx, id, intent, setAttrOperationResultLimit, effectLimit)
	if err != nil {
		return Stat{}, err
	}
	if admitted.State != OperationPending || admitted.AttemptID != [16]byte{} {
		return resolveSetAttrOperation(ctx, journal, id)
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	if err != nil {
		return Stat{}, err
	}
	return s.executeSetAttrOperation(ctx, journal, id, attempt, inode, mask, attrs, intent)
}

func (s *Service) executeSetAttrOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, inode uint64, mask SetAttrMask, attrs SetAttr, intent []byte) (Stat, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		stat, err := s.publishSetAttrOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, inode, mask, attrs, intent)
		if !isDefinitiveWriteConflict(err) {
			return stat, err
		}
		attempt, err = journal.AdvanceGeneration(deadlineCtx, id, attempt.AttemptID)
		if err != nil {
			return Stat{}, err
		}
		if retry == writeConflictRetryLimit-1 {
			result, marshalErr := json.Marshal(setAttrOperationResult{Version: setAttrOperationVersion, Error: setAttrResultContention})
			if marshalErr != nil {
				return Stat{}, errors.Wrap(marshalErr, "encode setattr contention")
			}
			record, cancelErr := cancelOperationContention(ctx, journal, id, result)
			if cancelErr != nil {
				return Stat{}, cancelErr
			}
			return decodeSetAttrOperationResult(record)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return Stat{}, err
		}
	}
	return Stat{}, errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishSetAttrOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode uint64, mask SetAttrMask, attrs SetAttr, intent []byte) (Stat, error) {
	plan, cleanup, semanticErr, err := s.prepareSetAttrOperationPlan(ctx, journal, id, attempt, inode, mask, attrs, intent)
	if err != nil {
		return Stat{}, err
	}
	if semanticErr != nil {
		return rejectSetAttrOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		stat, decodeErr := decodeSetAttrOperationResult(record)
		if decodeErr == nil {
			s.reclaimCommittedOrphanBestEffort(ctx, cleanup)
		}
		return stat, decodeErr
	}
	if !isDefinitiveWriteConflict(err) {
		stat, resolveErr := resolveSetAttrAttempt(ctx, journal, id, attempt)
		if resolveErr == nil {
			s.reclaimCommittedOrphanBestEffort(ctx, cleanup)
		}
		return stat, resolveErr
	}
	return Stat{}, err
}

func (s *Service) prepareSetAttrOperationPlan(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode uint64, mask SetAttrMask, attrs SetAttr, intent []byte) (operationAttemptPlan, *chunkDeletePlan, error, error) {
	for retry := range writeConflictRetryLimit {
		cleaned, err := s.prepareSetAttrCleanup(ctx, inode, mask.Size)
		if err != nil {
			return operationAttemptPlan{}, nil, nil, err
		}
		snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
		if err != nil {
			return operationAttemptPlan{}, nil, nil, err
		}
		plan, cleanup, semanticErr, err := s.planSetAttrOperation(ctx, snapshot, cleaned, inode, mask, attrs, intent)
		if errors.Is(err, errWriteCleanupChanged) {
			if err = waitWriteConflictRetry(ctx, retry); err != nil {
				return operationAttemptPlan{}, nil, nil, err
			}
			continue
		}
		return plan, cleanup, semanticErr, err
	}
	return operationAttemptPlan{}, nil, nil, errors.WithStack(ErrContentionLimit)
}

func (s *Service) prepareSetAttrCleanup(ctx context.Context, inode uint64, sizeChange bool) (*InodeMeta, error) {
	if !sizeChange {
		return nil, nil
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return nil, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil || meta.Type == TypeDirectory {
		return meta, err
	}
	if err = s.ensureActiveHomeAt(ctx, meta, ts); err != nil {
		return nil, err
	}
	_, cleaned, _, err := s.cleanupStaleChunksBeforeSizeChange(ctx, inode, meta)
	return cleaned, err
}

func (s *Service) planSetAttrOperation(ctx context.Context, snapshot operationPlanningSnapshot, cleaned *InodeMeta, inode uint64, mask SetAttrMask, attrs SetAttr, intent []byte) (operationAttemptPlan, *chunkDeletePlan, error, error) {
	plan := operationAttemptPlan{Intent: intent, ReadTS: snapshot.ReadTS, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(inode), fskeys.HomeKey(inode)}}
	meta, err := s.inodeAt(ctx, inode, snapshot.ReadTS)
	if errors.Is(err, ErrNotFound) {
		return plan, nil, ErrNotFound, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	oldSize := meta.Size
	var cleanup *chunkDeletePlan
	if mask.Size {
		cleanup, err = s.planSetAttrSize(ctx, &plan, meta, cleaned, attrs.Size)
		if errors.Is(err, ErrIsDir) {
			return plan, nil, ErrIsDir, nil
		}
		if err != nil {
			return operationAttemptPlan{}, nil, nil, err
		}
	}
	now := s.now().UnixNano()
	applySetAttrFields(meta, mask, attrs, oldSize, now)
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Effects = append(plan.Effects, elem)
	plan.Result, err = json.Marshal(setAttrOperationResult{Version: setAttrOperationVersion, Stat: meta.Stat()})
	return plan, cleanup, nil, errors.Wrap(err, "encode setattr result")
}

func (s *Service) planSetAttrSize(ctx context.Context, plan *operationAttemptPlan, meta, cleaned *InodeMeta, size uint64) (*chunkDeletePlan, error) {
	if meta.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	if err := s.ensureActiveHomeAt(ctx, meta, plan.ReadTS); err != nil {
		return nil, err
	}
	if cleaned == nil || (size > meta.Size && (meta.Size != cleaned.Size || meta.HomeSlot != cleaned.HomeSlot)) {
		return nil, errWriteCleanupChanged
	}
	if size < meta.Size {
		elems, reads, delta, cleanup, err := s.truncateChunkElems(ctx, meta, plan.ReadTS, size, meta.effectiveChunkSize(s.chunkSize))
		if err != nil {
			return nil, err
		}
		plan.Effects = append(plan.Effects, elems...)
		plan.ReadKeys = append(plan.ReadKeys, reads...)
		plan.Effects, plan.ReadKeys, err = s.appendUsageUpdate(ctx, plan.ReadTS, plan.Effects, plan.ReadKeys, delta)
		if err != nil {
			return nil, err
		}
		meta.Size = size
		return cleanup, nil
	}
	meta.Size = size
	return nil, nil
}

func applySetAttrFields(meta *InodeMeta, mask SetAttrMask, attrs SetAttr, oldSize uint64, now int64) {
	if mask.Mode {
		meta.Mode = attrs.Mode
	}
	if mask.UID {
		meta.UID = attrs.UID
	}
	if mask.GID {
		meta.GID = attrs.GID
	}
	if mask.Atime {
		meta.AtimeNsec = attrs.AtimeNsec
	}
	if mask.Mtime {
		meta.MtimeNsec = attrs.MtimeNsec
	} else if mask.Size && attrs.Size != oldSize {
		meta.MtimeNsec = now
	}
}

func decodeSetAttrOperationResult(record operationRecord) (Stat, error) {
	var result setAttrOperationResult
	if len(record.Result) == 0 || len(record.Result) > setAttrOperationResultLimit {
		return Stat{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if err := json.Unmarshal(record.Result, &result); err != nil {
		return Stat{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if result.Version != setAttrOperationVersion {
		return Stat{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationRejected {
		return Stat{}, rejectedSetAttrError(result.Error)
	}
	if record.State == OperationCancelled {
		return Stat{}, cancelledOperationError(result.Error, setAttrResultContention)
	}
	if record.State != OperationCommitted || result.Error != 0 || result.Stat.Inode == 0 {
		return Stat{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return result.Stat, nil
}

func rejectedSetAttrError(status uint8) error {
	switch status {
	case setAttrResultNotFound:
		return errors.WithStack(ErrNotFound)
	case setAttrResultIsDir:
		return errors.WithStack(ErrIsDir)
	default:
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
}

func resolveSetAttrOperation(ctx context.Context, journal *OperationJournal, id OperationID) (Stat, error) {
	record, err := journal.Resolve(ctx, id)
	if err != nil {
		if errors.Is(err, ErrOperationPending) {
			return Stat{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
		}
		return Stat{}, err
	}
	return decodeSetAttrOperationResult(record)
}

func resolveSetAttrAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) (Stat, error) {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeSetAttrOperationResult(record)
	}
	return Stat{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func rejectSetAttrOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) (Stat, error) {
	status := uint8(setAttrResultNotFound)
	if errors.Is(semanticErr, ErrIsDir) {
		status = setAttrResultIsDir
	} else if !errors.Is(semanticErr, ErrNotFound) {
		return Stat{}, semanticErr
	}
	result, err := json.Marshal(setAttrOperationResult{Version: setAttrOperationVersion, Error: status})
	if err != nil {
		return Stat{}, errors.Wrap(err, "encode setattr rejection")
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent, result, plan.ReadKeys)
	if err != nil {
		if isDefinitiveWriteConflict(err) {
			return Stat{}, err
		}
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !errors.Is(err, ErrOperationGeneration) {
			return Stat{}, err
		}
		return resolveSetAttrAttempt(ctx, journal, id, attempt)
	}
	return decodeSetAttrOperationResult(record)
}
