package filesystem

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	writeOperationIntentVersion  = 1
	writeOperationIntentHeader   = 25
	writeOperationResultBytes    = 9
	writeOperationMaxEffectLimit = 1 << 20
	writeOperationEffectSlack    = 256
	writeOperationMaxData        = 4 << 20
	writeResultSuccess           = 0
	writeResultIsDir             = 1
	writeResultNotFound          = 2
)

var errWriteCleanupChanged = errors.New("filesystem: write cleanup snapshot changed")

// WriteWithOperation publishes one fixed-offset write and its original byte
// count under id. A pending duplicate is resolved without issuing a new write.
// The caller owns acknowledgement after it has attempted the FUSE reply.
func (s *Service) WriteWithOperation(ctx context.Context, id OperationID, inode, offset uint64, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) > writeOperationMaxData {
		return 0, errors.WithStack(ErrOperationQuota)
	}
	end, overflow := addUint64(offset, uint64(len(data)))
	if overflow {
		return 0, errors.WithStack(ErrInvalid)
	}
	data = bytes.Clone(data)
	intent := encodeWriteOperationIntent(inode, offset, data)
	effectLimit, err := s.writeOperationEffectLimit(ctx, inode, offset, end)
	if err != nil {
		return 0, err
	}
	journal, err := NewOperationJournal(s)
	if err != nil {
		return 0, err
	}
	admitted, err := journal.Admit(ctx, id, intent, writeOperationResultBytes, effectLimit)
	if err != nil {
		return 0, err
	}
	if admitted.State != OperationPending {
		return resolveWriteOperation(ctx, journal, id)
	}
	if admitted.AttemptID != [16]byte{} {
		return resolveWriteOperation(ctx, journal, id)
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	if err != nil {
		return 0, err
	}
	return s.executeWriteOperation(ctx, journal, id, attempt, inode, offset, end, data, intent)
}

func (s *Service) writeOperationEffectLimit(ctx context.Context, inode, offset, end uint64) (uint32, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return 0, err
	}
	chunkSize := s.chunkSize
	homeSlot := inode
	meta, err := s.inodeAt(ctx, inode, ts)
	if err == nil {
		chunkSize = meta.effectiveChunkSize(s.chunkSize)
		homeSlot = meta.HomeSlot
	} else if !errors.Is(err, ErrNotFound) {
		return 0, err
	}
	chunks := (end-1)/chunkSize - offset/chunkSize + 1
	if chunks > maxOperationEffectKeys-2 {
		return 0, errors.WithStack(ErrOperationQuota)
	}
	chunkKey := fskeys.ChunkKey(homeSlot, inode, 0)
	usageKey := fskeys.UsageRouteKey(kv.RouteKey(chunkKey))
	limit := chunks*uint64(len(chunkKey)) + uint64(len(fskeys.InodeKey(inode))+len(usageKey)+writeOperationEffectSlack)
	if limit > writeOperationMaxEffectLimit {
		return 0, errors.WithStack(ErrOperationQuota)
	}
	return uint32(limit), nil
}

func (s *Service) executeWriteOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, inode, offset, end uint64, data, intent []byte) (int, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		if err := deadlineCtx.Err(); err != nil {
			return 0, errors.WithStack(err)
		}
		plan, semanticErr, err := s.planJournalWrite(deadlineCtx, journal, id, attempt.AttemptID, inode, offset, end, data, intent)
		if err != nil {
			return 0, err
		}
		if semanticErr != nil {
			return publishWriteRejection(deadlineCtx, journal, id, attempt.AttemptID, plan, semanticErr)
		}
		record, err := journal.Publish(deadlineCtx, id, attempt.AttemptID, plan)
		if err == nil {
			return decodeWriteOperationResult(record)
		}
		if !isDefinitiveWriteConflict(err) {
			return resolveWriteAttempt(deadlineCtx, journal, id, attempt.AttemptID)
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

func encodeWriteOperationIntent(inode, offset uint64, data []byte) []byte {
	intent := make([]byte, writeOperationIntentHeader+len(data))
	intent[0] = writeOperationIntentVersion
	binary.BigEndian.PutUint64(intent[1:9], inode)
	binary.BigEndian.PutUint64(intent[9:17], offset)
	binary.BigEndian.PutUint64(intent[17:25], uint64(len(data)))
	copy(intent[writeOperationIntentHeader:], data)
	return intent
}

func encodeWriteOperationResult(count uint64, status byte) []byte {
	result := make([]byte, writeOperationResultBytes)
	result[0] = status
	binary.BigEndian.PutUint64(result[1:], count)
	return result
}

func decodeWriteOperationResult(record operationRecord) (int, error) {
	if len(record.Result) != writeOperationResultBytes {
		return 0, errors.WithStack(ErrOperationRecordCorrupt)
	}
	switch record.Result[0] {
	case writeResultSuccess:
		if record.State != OperationCommitted {
			return 0, errors.WithStack(ErrOperationRecordCorrupt)
		}
		return checkedInt(binary.BigEndian.Uint64(record.Result[1:]))
	case writeResultIsDir:
		if record.State != OperationRejected {
			return 0, errors.WithStack(ErrOperationRecordCorrupt)
		}
		return 0, errors.WithStack(ErrIsDir)
	case writeResultNotFound:
		if record.State != OperationRejected {
			return 0, errors.WithStack(ErrOperationRecordCorrupt)
		}
		return 0, errors.WithStack(ErrNotFound)
	default:
		return 0, errors.WithStack(ErrOperationRecordCorrupt)
	}
}

func resolveWriteOperation(ctx context.Context, journal *OperationJournal, id OperationID) (int, error) {
	record, err := journal.Resolve(ctx, id)
	if err != nil {
		if errors.Is(err, ErrOperationPending) {
			return 0, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
		}
		return 0, err
	}
	return decodeWriteOperationResult(record)
}

func resolveWriteAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) (int, error) {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeWriteOperationResult(record)
	}
	return 0, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func publishWriteRejection(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) (int, error) {
	var status byte
	switch {
	case errors.Is(semanticErr, ErrIsDir):
		status = writeResultIsDir
	case errors.Is(semanticErr, ErrNotFound):
		status = writeResultNotFound
	default:
		return 0, semanticErr
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent,
		encodeWriteOperationResult(0, status), plan.ReadKeys)
	if err != nil {
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !IsWriteConflict(err) && !errors.Is(err, ErrOperationGeneration) {
			return 0, err
		}
		return resolveWriteAttempt(ctx, journal, id, attempt)
	}
	return decodeWriteOperationResult(record)
}

func (s *Service) planJournalWrite(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, inode, offset, end uint64, data, intent []byte) (operationAttemptPlan, error, error) {
	for retry := range writeConflictRetryLimit {
		cleaned, err := s.prepareWriteCleanup(ctx, inode, end)
		if err != nil {
			return operationAttemptPlan{}, nil, err
		}
		snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
		if err != nil {
			return operationAttemptPlan{}, nil, err
		}
		plan, semanticErr, err := s.planJournalWriteAt(ctx, snapshot, cleaned, inode, offset, end, data, intent)
		if errors.Is(err, errWriteCleanupChanged) {
			if err = waitWriteConflictRetry(ctx, retry); err != nil {
				return operationAttemptPlan{}, nil, err
			}
			continue
		}
		return plan, semanticErr, err
	}
	return operationAttemptPlan{}, nil, errors.WithStack(ErrContentionLimit)
}

func (s *Service) planJournalWriteAt(ctx context.Context, snapshot operationPlanningSnapshot, cleaned *InodeMeta, inode, offset, end uint64, data, intent []byte) (operationAttemptPlan, error, error) {
	plan := operationAttemptPlan{Intent: intent, ReadTS: snapshot.ReadTS, RouteVersion: snapshot.RouteVersion}
	meta, err := s.inodeAt(ctx, inode, snapshot.ReadTS)
	if errors.Is(err, ErrNotFound) {
		plan.ReadKeys = [][]byte{fskeys.InodeKey(inode)}
		return plan, ErrNotFound, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	if meta.Type == TypeDirectory {
		plan.ReadKeys = [][]byte{fskeys.InodeKey(inode)}
		return plan, ErrIsDir, nil
	}
	if err = s.ensureActiveHomeAt(ctx, meta, snapshot.ReadTS); err != nil {
		return operationAttemptPlan{}, nil, err
	}
	if end > meta.Size && (meta.Size != cleaned.Size || meta.HomeSlot != cleaned.HomeSlot) {
		return operationAttemptPlan{}, nil, errWriteCleanupChanged
	}
	plan.Effects, plan.ReadKeys, err = s.planWriteTxn(ctx, meta, snapshot.ReadTS, offset, end, data)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	plan.Result = encodeWriteOperationResult(uint64(len(data)), writeResultSuccess)
	return plan, nil, nil
}

func (s *Service) prepareWriteCleanup(ctx context.Context, inode, end uint64) (*InodeMeta, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return nil, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if errors.Is(err, ErrNotFound) {
		return &InodeMeta{}, nil
	}
	if err != nil {
		return nil, err
	}
	if meta.Type == TypeDirectory || end <= meta.Size {
		return meta, nil
	}
	if err = s.ensureActiveHomeAt(ctx, meta, ts); err != nil {
		return nil, err
	}
	_, cleaned, _, err := s.cleanupStaleChunksBeforeSizeChange(ctx, inode, meta)
	return cleaned, err
}
