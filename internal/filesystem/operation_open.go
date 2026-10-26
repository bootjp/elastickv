package filesystem

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	openOperationVersion     = 1
	openOperationResultLimit = 128
	openOperationEffectLimit = 256
	openResultMissing        = 1
)

type openOperationIntent struct {
	Version  uint8  `json:"version"`
	Inode    uint64 `json:"inode"`
	ClientID []byte `json:"client_id"`
}

type openOperationResult struct {
	Version uint8  `json:"version"`
	FH      uint64 `json:"fh"`
	Error   uint8  `json:"error"`
}

// OpenWithOperation retains the original handle and its lease publication.
func (s *Service) OpenWithOperation(ctx context.Context, id OperationID, inode uint64, clientID []byte) (uint64, error) {
	clientID = bytes.Clone(clientID)
	intent, err := json.Marshal(openOperationIntent{Version: openOperationVersion, Inode: inode, ClientID: clientID})
	if err != nil {
		return 0, errors.Wrap(err, "encode open intent")
	}
	journal, err := NewOperationJournal(s)
	if err != nil {
		return 0, err
	}
	admitted, err := journal.Admit(ctx, id, intent, openOperationResultLimit, openOperationEffectLimit)
	if err != nil {
		return 0, err
	}
	if admitted.State != OperationPending || admitted.AttemptID != [16]byte{} {
		return resolveOpenOperation(ctx, journal, id)
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	if err != nil {
		return 0, err
	}
	return s.executeOpenOperation(ctx, journal, id, attempt, inode, clientID, intent)
}

func (s *Service) executeOpenOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, inode uint64, clientID, intent []byte) (uint64, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		fh, err := s.publishOpenOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, inode, clientID, intent)
		if !isDefinitiveWriteConflict(err) {
			return fh, err
		}
		attempt, err = journal.AdvanceGeneration(deadlineCtx, id, attempt.AttemptID)
		if err != nil {
			return 0, err
		}
		if retry == writeConflictRetryLimit-1 {
			return 0, errors.WithStack(ErrContentionLimit)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return 0, err
		}
	}
	return 0, errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishOpenOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode uint64, clientID, intent []byte) (uint64, error) {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return 0, err
	}
	plan, semanticErr, err := s.planOpenOperation(ctx, snapshot, inode, clientID, intent)
	if err != nil {
		return 0, err
	}
	if semanticErr != nil {
		return rejectOpenOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		return decodeOpenOperationResult(record)
	}
	if !isDefinitiveWriteConflict(err) {
		return resolveOpenAttempt(ctx, journal, id, attempt)
	}
	return 0, err
}

func (s *Service) planOpenOperation(ctx context.Context, snapshot operationPlanningSnapshot, inode uint64, clientID, intent []byte) (operationAttemptPlan, error, error) {
	ts := snapshot.ReadTS
	plan := operationAttemptPlan{Intent: intent, ReadTS: ts, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(inode)}}
	meta, err := s.inodeAt(ctx, inode, ts)
	if errors.Is(err, ErrNotFound) {
		return plan, ErrNotFound, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	if meta.Orphaned || meta.Nlink == 0 {
		return plan, ErrNotFound, nil
	}
	fh, err := s.allocID()
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	refElem, fenceElem, refKey, err := s.planOpenLease(ctx, inode, clientID, fh, ts)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	plan.ReadKeys = append(plan.ReadKeys, refKey, fskeys.RefFenceKey(inode))
	plan.Effects = []*kv.Elem[kv.OP]{refElem, fenceElem}
	plan.Result, err = json.Marshal(openOperationResult{Version: openOperationVersion, FH: fh})
	return plan, nil, errors.Wrap(err, "encode open result")
}

func (s *Service) planOpenLease(ctx context.Context, inode uint64, clientID []byte, fh, ts uint64) (*kv.Elem[kv.OP], *kv.Elem[kv.OP], []byte, error) {
	if fh == 0 {
		return nil, nil, nil, errors.WithStack(store.ErrWriteConflict)
	}
	refKey := fskeys.RefKey(inode, clientID, fh)
	if _, err := s.store.GetAt(ctx, refKey, ts); err == nil {
		return nil, nil, nil, errors.WithStack(store.ErrWriteConflict)
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil, nil, errors.Wrap(err, "read open handle lease")
	}
	refElem, err := s.refPutElem(inode, clientID, fh)
	if err != nil {
		return nil, nil, nil, err
	}
	fenceElem, err := s.refFenceElem(inode)
	if err != nil {
		return nil, nil, nil, err
	}
	return refElem, fenceElem, refKey, nil
}

func decodeOpenOperationResult(record operationRecord) (uint64, error) {
	var result openOperationResult
	if len(record.Result) == 0 || len(record.Result) > openOperationResultLimit || json.Unmarshal(record.Result, &result) != nil || result.Version != openOperationVersion {
		return 0, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationRejected {
		if result.Error == openResultMissing {
			return 0, errors.WithStack(ErrNotFound)
		}
		return 0, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State != OperationCommitted || result.Error != 0 || result.FH == 0 {
		return 0, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return result.FH, nil
}

func resolveOpenOperation(ctx context.Context, journal *OperationJournal, id OperationID) (uint64, error) {
	record, err := journal.Resolve(ctx, id)
	if errors.Is(err, ErrOperationPending) {
		return 0, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if err != nil {
		return 0, err
	}
	return decodeOpenOperationResult(record)
}

func resolveOpenAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) (uint64, error) {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeOpenOperationResult(record)
	}
	return 0, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func rejectOpenOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) (uint64, error) {
	if !errors.Is(semanticErr, ErrNotFound) {
		return 0, semanticErr
	}
	result, err := json.Marshal(openOperationResult{Version: openOperationVersion, Error: openResultMissing})
	if err != nil {
		return 0, errors.Wrap(err, "encode open rejection")
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent, result, plan.ReadKeys)
	if err != nil {
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !IsWriteConflict(err) && !errors.Is(err, ErrOperationGeneration) {
			return 0, err
		}
		return resolveOpenAttempt(ctx, journal, id, attempt)
	}
	return decodeOpenOperationResult(record)
}
