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

// SetAttrWithOperation journals metadata-only attribute changes. Size changes
// require a separate planner because chunk deletion may span transactions.
func (s *Service) SetAttrWithOperation(ctx context.Context, id OperationID, inode uint64, mask SetAttrMask, attrs SetAttr) (Stat, error) {
	if mask.Size {
		return Stat{}, errors.WithStack(ErrUnsupported)
	}
	intent, err := json.Marshal(setAttrOperationIntent{Version: setAttrOperationVersion, Inode: inode, Mask: mask, Attrs: attrs})
	if err != nil {
		return Stat{}, errors.Wrap(err, "encode setattr intent")
	}
	journal, err := NewOperationJournal(s)
	if err != nil {
		return Stat{}, err
	}
	admitted, err := journal.Admit(ctx, id, intent, setAttrOperationResultLimit, setAttrOperationEffectLimit)
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
			return Stat{}, errors.WithStack(ErrContentionLimit)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return Stat{}, err
		}
	}
	return Stat{}, errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishSetAttrOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode uint64, mask SetAttrMask, attrs SetAttr, intent []byte) (Stat, error) {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return Stat{}, err
	}
	plan, semanticErr, err := s.planSetAttrOperation(ctx, snapshot, inode, mask, attrs, intent)
	if err != nil {
		return Stat{}, err
	}
	if semanticErr != nil {
		return rejectSetAttrOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		return decodeSetAttrOperationResult(record)
	}
	if !isDefinitiveWriteConflict(err) {
		return resolveSetAttrAttempt(ctx, journal, id, attempt)
	}
	return Stat{}, err
}

func (s *Service) planSetAttrOperation(ctx context.Context, snapshot operationPlanningSnapshot, inode uint64, mask SetAttrMask, attrs SetAttr, intent []byte) (operationAttemptPlan, error, error) {
	plan := operationAttemptPlan{Intent: intent, ReadTS: snapshot.ReadTS, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(inode), fskeys.HomeKey(inode)}}
	meta, err := s.inodeAt(ctx, inode, snapshot.ReadTS)
	if errors.Is(err, ErrNotFound) {
		return plan, ErrNotFound, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	now := s.now().UnixNano()
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
	}
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	plan.Effects = []*kv.Elem[kv.OP]{elem}
	plan.Result, err = json.Marshal(setAttrOperationResult{Version: setAttrOperationVersion, Stat: meta.Stat()})
	return plan, nil, errors.Wrap(err, "encode setattr result")
}

func decodeSetAttrOperationResult(record operationRecord) (Stat, error) {
	var result setAttrOperationResult
	if len(record.Result) == 0 || len(record.Result) > setAttrOperationResultLimit ||
		json.Unmarshal(record.Result, &result) != nil || result.Version != setAttrOperationVersion {
		return Stat{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationRejected && result.Error == 1 {
		return Stat{}, errors.WithStack(ErrNotFound)
	}
	if record.State != OperationCommitted || result.Error != 0 || result.Stat.Inode == 0 {
		return Stat{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return result.Stat, nil
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
	if !errors.Is(semanticErr, ErrNotFound) {
		return Stat{}, semanticErr
	}
	result, err := json.Marshal(setAttrOperationResult{Version: setAttrOperationVersion, Error: 1})
	if err != nil {
		return Stat{}, errors.Wrap(err, "encode setattr rejection")
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent, result, plan.ReadKeys)
	if err != nil {
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !IsWriteConflict(err) && !errors.Is(err, ErrOperationGeneration) {
			return Stat{}, err
		}
		return resolveSetAttrAttempt(ctx, journal, id, attempt)
	}
	return decodeSetAttrOperationResult(record)
}
