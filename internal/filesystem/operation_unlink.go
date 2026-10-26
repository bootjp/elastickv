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
	unlinkIntentVersion  = 1
	unlinkResultSuccess  = 0
	unlinkResultMissing  = 1
	unlinkResultNotDir   = 2
	unlinkResultIsDir    = 3
	unlinkResultNotEmpty = 4
	unlinkEffectLimit    = 64 << 10
	unlinkIntentHeader   = 10
)

// UnlinkWithOperation records one file removal and its result under id.
// Directory removal has a separate emptiness predicate and planner.
func (s *Service) UnlinkWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte) error {
	return s.unlinkNodeWithOperation(ctx, id, parent, name, false)
}

// RmdirWithOperation journals an empty-directory removal and its result.
func (s *Service) RmdirWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte) error {
	return s.unlinkNodeWithOperation(ctx, id, parent, name, true)
}

func (s *Service) unlinkNodeWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte, directory bool) error {
	if err := validateName(name); err != nil {
		return err
	}
	name = bytes.Clone(name)
	intent := make([]byte, unlinkIntentHeader+len(name))
	intent[0] = unlinkIntentVersion
	binary.BigEndian.PutUint64(intent[1:9], parent)
	if directory {
		intent[9] = 1
	}
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
	return s.executeUnlinkOperation(ctx, journal, id, attempt, parent, name, directory, intent)
}

func (s *Service) executeUnlinkOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, parent uint64, name []byte, directory bool, intent []byte) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		err := s.publishUnlinkOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, parent, name, directory, intent)
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

func (s *Service) publishUnlinkOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, parent uint64, name []byte, directory bool, intent []byte) error {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return err
	}
	plan, cleanup, semanticErr, err := s.planUnlinkOperation(ctx, snapshot, parent, name, directory, intent)
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

func (s *Service) planUnlinkOperation(ctx context.Context, snapshot operationPlanningSnapshot, parent uint64, name []byte, directory bool, intent []byte) (operationAttemptPlan, *chunkDeletePlan, error, error) {
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
	meta, targetKey, semanticErr, err := s.unlinkTargetAt(ctx, parent, name, ts, directory)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	if len(targetKey) != 0 {
		plan.ReadKeys = append(plan.ReadKeys, targetKey)
	}
	appendUnlinkDirectoryPredicate(&plan, meta, directory, semanticErr)
	if semanticErr != nil {
		return plan, nil, semanticErr, nil
	}
	now := s.now().UnixNano()
	parentMeta.MtimeNsec, parentMeta.CtimeNsec = now, now
	nodeElems, nodeReads, delta, cleanup, err := s.unlinkNodeTxnParts(ctx, ts, parentMeta, meta, directory, now)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	parentEffects, err := unlinkParentEffects(parent, parentMeta, now)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Effects = append([]*kv.Elem[kv.OP]{{Op: kv.Del, Key: entryKey}}, nodeElems...)
	plan.Effects = append(plan.Effects, parentEffects...)
	plan.ReadKeys = append(plan.ReadKeys, nodeReads...)
	plan.Effects, plan.ReadKeys, err = s.appendUsageUpdate(ctx, ts, plan.Effects, plan.ReadKeys, delta)
	if err != nil {
		return operationAttemptPlan{}, nil, nil, err
	}
	plan.Result = []byte{unlinkResultSuccess}
	return plan, cleanup, nil, nil
}

func appendUnlinkDirectoryPredicate(plan *operationAttemptPlan, meta *InodeMeta, directory bool, semanticErr error) {
	if directory && meta != nil && semanticErr != nil {
		plan.ReadKeys = append(plan.ReadKeys, fskeys.DirVersionKey(meta.Inode))
	}
}

func (s *Service) unlinkNodeTxnParts(ctx context.Context, ts uint64, parent, target *InodeMeta, directory bool, now int64) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, error) {
	if !directory {
		return s.unlinkFileTxnParts(ctx, ts, target, now)
	}
	if parent.Nlink > fileInitialNlink {
		parent.Nlink--
	}
	keys := [][]byte{fskeys.InodeKey(target.Inode), fskeys.HomeKey(target.Inode), fskeys.DirVersionKey(target.Inode)}
	effects := make([]*kv.Elem[kv.OP], 0, len(keys))
	for _, key := range keys {
		effects = append(effects, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	return effects, keys[1:], usageDelta{filesSub: 1}, nil, nil
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

func (s *Service) unlinkTargetAt(ctx context.Context, parent uint64, name []byte, ts uint64, directory bool) (*InodeMeta, []byte, error, error) {
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
	semanticErr, err := s.validateUnlinkTarget(ctx, meta, ts, directory)
	if err != nil {
		return nil, nil, nil, err
	}
	if semanticErr != nil {
		return meta, targetKey, semanticErr, nil
	}
	return meta, targetKey, nil, nil
}

func (s *Service) validateUnlinkTarget(ctx context.Context, meta *InodeMeta, ts uint64, directory bool) (error, error) {
	if directory && meta.Type != TypeDirectory {
		return ErrNotDir, nil
	}
	if !directory && meta.Type == TypeDirectory {
		return ErrIsDir, nil
	}
	if directory {
		if err := s.ensureDirectoryEmpty(ctx, meta.Inode, ts); err != nil {
			if errors.Is(err, ErrNotEmpty) {
				return ErrNotEmpty, nil
			}
			return nil, err
		}
	} else if err := s.ensureActiveHomeAt(ctx, meta, ts); err != nil {
		return nil, err
	}
	return nil, nil
}

func decodeUnlinkOperationResult(record operationRecord) error {
	if len(record.Result) != 1 {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationCommitted && record.Result[0] == unlinkResultSuccess {
		return nil
	}
	if record.State != OperationRejected {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	switch record.Result[0] {
	case unlinkResultMissing:
		return errors.WithStack(ErrNotFound)
	case unlinkResultNotDir:
		return errors.WithStack(ErrNotDir)
	case unlinkResultIsDir:
		return errors.WithStack(ErrIsDir)
	case unlinkResultNotEmpty:
		return errors.WithStack(ErrNotEmpty)
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
	case errors.Is(semanticErr, ErrNotEmpty):
		status = unlinkResultNotEmpty
	default:
		return semanticErr
	}
	return rejectNamespaceOperation(ctx, journal, id, attempt, plan, status, decodeUnlinkOperationResult)
}
