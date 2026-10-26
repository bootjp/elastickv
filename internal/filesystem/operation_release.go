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
	releaseOperationVersion     = 1
	releaseResultContention     = 2
	releaseOperationResultLimit = 1
	releaseOperationEffectLimit = 64 << 10
)

type releaseOperationIntent struct {
	Version  uint8  `json:"version"`
	Inode    uint64 `json:"inode"`
	FH       uint64 `json:"fh"`
	ClientID []byte `json:"client_id"`
}

// ReleaseWithOperation makes a handle release terminal under one operation ID.
func (s *Service) ReleaseWithOperation(ctx context.Context, id OperationID, inode, fh uint64, clientID []byte) error {
	clientID = bytes.Clone(clientID)
	intent, err := json.Marshal(releaseOperationIntent{Version: releaseOperationVersion, Inode: inode, FH: fh, ClientID: clientID})
	if err != nil {
		return errors.Wrap(err, "encode release intent")
	}
	effectLimit, err := releaseEffectLimit(inode, fh, clientID)
	if err != nil {
		return err
	}
	journal, attempt, terminal, err := s.admitHandleOperation(ctx, id, intent, releaseOperationResultLimit, effectLimit)
	if err != nil {
		return err
	}
	if terminal {
		return resolveReleaseOperation(ctx, journal, id)
	}
	return s.executeReleaseOperation(ctx, journal, id, attempt, inode, fh, clientID, intent)
}

func releaseEffectLimit(inode, fh uint64, clientID []byte) (uint32, error) {
	return operationEffectLimitWithSlack(releaseOperationEffectLimit, fskeys.RefKey(inode, clientID, fh))
}

func (s *Service) admitHandleOperation(ctx context.Context, id OperationID, intent []byte, resultLimit, effectLimit uint32) (*OperationJournal, operationRecord, bool, error) {
	journal, err := NewOperationJournal(s)
	if err != nil {
		return nil, operationRecord{}, false, err
	}
	admitted, err := journal.Admit(ctx, id, intent, resultLimit, effectLimit)
	if err != nil {
		return nil, operationRecord{}, false, err
	}
	if admitted.State != OperationPending || admitted.AttemptID != [16]byte{} {
		return journal, operationRecord{}, true, nil
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	return journal, attempt, false, err
}

func (s *Service) executeReleaseOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, inode, fh uint64, clientID, intent []byte) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		err := s.publishReleaseOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, inode, fh, clientID, intent)
		if !isDefinitiveWriteConflict(err) {
			return err
		}
		attempt, err = journal.AdvanceGeneration(deadlineCtx, id, attempt.AttemptID)
		if err != nil {
			return err
		}
		if retry == writeConflictRetryLimit-1 {
			record, cancelErr := cancelOperationContention(ctx, journal, id, []byte{releaseResultContention})
			if cancelErr != nil {
				return cancelErr
			}
			return decodeReleaseOperationResult(record)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishReleaseOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode, fh uint64, clientID, intent []byte) error {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return err
	}
	plan, cleanup, err := s.planReleaseOperation(ctx, snapshot, inode, fh, clientID, intent)
	if err != nil {
		return err
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		err = decodeReleaseOperationResult(record)
	} else if !isDefinitiveWriteConflict(err) {
		err = resolveReleaseAttempt(ctx, journal, id, attempt)
	}
	if err == nil {
		s.reclaimCommittedOrphanBestEffort(ctx, cleanup)
	}
	return err
}

func (s *Service) planReleaseOperation(ctx context.Context, snapshot operationPlanningSnapshot, inode, fh uint64, clientID, intent []byte) (operationAttemptPlan, *chunkDeletePlan, error) {
	refKey := fskeys.RefKey(inode, clientID, fh)
	elems, reads, delta, cleanup, _, err := s.releaseRefTxnParts(ctx, snapshot.ReadTS, inode, refKey)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	elems, reads, err = s.appendUsageUpdate(ctx, snapshot.ReadTS, elems, reads, delta)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	return operationAttemptPlan{Intent: intent, ReadTS: snapshot.ReadTS, RouteVersion: snapshot.RouteVersion,
		ReadKeys: reads, Effects: elems, Result: []byte{releaseOperationVersion}}, cleanup, nil
}

func decodeReleaseOperationResult(record operationRecord) error {
	if record.State == OperationCancelled && len(record.Result) == 1 && record.Result[0] == releaseResultContention {
		return errors.WithStack(ErrContentionLimit)
	}
	if record.State != OperationCommitted || len(record.Result) != 1 || record.Result[0] != releaseOperationVersion {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	return nil
}

func resolveReleaseOperation(ctx context.Context, journal *OperationJournal, id OperationID) error {
	record, err := journal.Resolve(ctx, id)
	if errors.Is(err, ErrOperationPending) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if err != nil {
		return err
	}
	return decodeReleaseOperationResult(record)
}

func resolveReleaseAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) error {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && outcome == AttemptCommitted {
		return decodeReleaseOperationResult(record)
	}
	return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}
