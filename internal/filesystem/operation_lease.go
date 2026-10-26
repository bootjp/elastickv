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
	leaseOperationVersion     = 1
	leaseOperationResultLimit = 1
	leaseOperationEffectLimit = 256
	leaseResultSuccess        = 0
	leaseResultMissing        = 1
)

type leaseOperationIntent struct {
	Version  uint8  `json:"version"`
	Inode    uint64 `json:"inode"`
	FH       uint64 `json:"fh"`
	ClientID []byte `json:"client_id"`
}

// RefreshOpenHandleLeaseWithOperation cannot recreate a released or reaped lease.
func (s *Service) RefreshOpenHandleLeaseWithOperation(ctx context.Context, id OperationID, inode, fh uint64, clientID []byte) error {
	clientID = bytes.Clone(clientID)
	intent, err := json.Marshal(leaseOperationIntent{Version: leaseOperationVersion, Inode: inode, FH: fh, ClientID: clientID})
	if err != nil {
		return errors.Wrap(err, "encode lease intent")
	}
	journal, attempt, terminal, err := s.admitHandleOperation(ctx, id, intent, leaseOperationResultLimit, leaseOperationEffectLimit)
	if err != nil {
		return err
	}
	if terminal {
		return resolveLeaseOperation(ctx, journal, id)
	}
	return s.executeLeaseOperation(ctx, journal, id, attempt, inode, fh, clientID, intent)
}

func (s *Service) executeLeaseOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, inode, fh uint64, clientID, intent []byte) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		err := s.publishLeaseOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, inode, fh, clientID, intent)
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

func (s *Service) publishLeaseOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode, fh uint64, clientID, intent []byte) error {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return err
	}
	plan, semanticErr, err := s.planLeaseOperation(ctx, snapshot, inode, fh, clientID, intent)
	if err != nil {
		return err
	}
	if semanticErr != nil {
		return rejectLeaseOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		return decodeLeaseOperationResult(record)
	}
	if !isDefinitiveWriteConflict(err) {
		return resolveLeaseAttempt(ctx, journal, id, attempt)
	}
	return err
}

func (s *Service) planLeaseOperation(ctx context.Context, snapshot operationPlanningSnapshot, inode, fh uint64, clientID, intent []byte) (operationAttemptPlan, error, error) {
	refKey := fskeys.RefKey(inode, clientID, fh)
	plan := operationAttemptPlan{Intent: intent, ReadTS: snapshot.ReadTS, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(inode), refKey}}
	if _, err := s.inodeAt(ctx, inode, snapshot.ReadTS); errors.Is(err, ErrNotFound) {
		return plan, ErrNotFound, nil
	} else if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	if _, err := s.store.GetAt(ctx, refKey, snapshot.ReadTS); errors.Is(err, store.ErrKeyNotFound) {
		return plan, ErrNotFound, nil
	} else if err != nil {
		return operationAttemptPlan{}, nil, errors.Wrap(err, "read open handle lease")
	}
	elem, err := s.refPutElem(inode, clientID, fh)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	plan.Effects = []*kv.Elem[kv.OP]{elem}
	plan.Result = []byte{leaseResultSuccess}
	return plan, nil, nil
}

func decodeLeaseOperationResult(record operationRecord) error {
	if len(record.Result) != 1 {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationCommitted && record.Result[0] == leaseResultSuccess {
		return nil
	}
	if record.State == OperationRejected && record.Result[0] == leaseResultMissing {
		return errors.WithStack(ErrNotFound)
	}
	return errors.WithStack(ErrOperationRecordCorrupt)
}

func resolveLeaseOperation(ctx context.Context, journal *OperationJournal, id OperationID) error {
	record, err := journal.Resolve(ctx, id)
	if errors.Is(err, ErrOperationPending) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if err != nil {
		return err
	}
	return decodeLeaseOperationResult(record)
}

func resolveLeaseAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) error {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeLeaseOperationResult(record)
	}
	return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func rejectLeaseOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) error {
	if !errors.Is(semanticErr, ErrNotFound) {
		return semanticErr
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent,
		[]byte{leaseResultMissing}, plan.ReadKeys)
	if err != nil {
		if isDefinitiveWriteConflict(err) {
			return err
		}
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !errors.Is(err, ErrOperationGeneration) {
			return err
		}
		return resolveLeaseAttempt(ctx, journal, id, attempt)
	}
	return decodeLeaseOperationResult(record)
}
