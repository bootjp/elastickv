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
	createOperationVersion     = 1
	createOperationResultLimit = 512
	createOperationEffectLimit = 4096
	createResultMissing        = 1
	createResultNotDir         = 2
	createResultExists         = 3
)

type createOperationIntent struct {
	Version uint8         `json:"version"`
	Parent  uint64        `json:"parent"`
	Name    []byte        `json:"name"`
	Type    FileType      `json:"type"`
	Options CreateOptions `json:"options"`
}

type createOperationResult struct {
	Version uint8        `json:"version"`
	Result  CreateResult `json:"result"`
	Error   uint8        `json:"error"`
}

// CreateWithOperation retains the created inode, open handle and original Stat.
func (s *Service) CreateWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte, opts CreateOptions) (CreateResult, error) {
	return s.createNodeWithOperation(ctx, id, parent, name, TypeFile, opts)
}

// MkdirWithOperation retains the created directory inode and original Stat.
func (s *Service) MkdirWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte, opts CreateOptions) (CreateResult, error) {
	return s.createNodeWithOperation(ctx, id, parent, name, TypeDirectory, opts)
}

func (s *Service) createNodeWithOperation(ctx context.Context, id OperationID, parent uint64, name []byte, typ FileType, opts CreateOptions) (CreateResult, error) {
	if err := validateName(name); err != nil {
		return CreateResult{}, err
	}
	name = bytes.Clone(name)
	opts.ClientID = bytes.Clone(opts.ClientID)
	intent, err := json.Marshal(createOperationIntent{Version: createOperationVersion, Parent: parent, Name: name, Type: typ, Options: opts})
	if err != nil {
		return CreateResult{}, errors.Wrap(err, "encode create intent")
	}
	journal, err := NewOperationJournal(s)
	if err != nil {
		return CreateResult{}, err
	}
	admitted, err := journal.Admit(ctx, id, intent, createOperationResultLimit, createOperationEffectLimit)
	if err != nil {
		return CreateResult{}, err
	}
	if admitted.State != OperationPending || admitted.AttemptID != [16]byte{} {
		return resolveCreateOperation(ctx, journal, id)
	}
	attempt, err := journal.BeginAttempt(ctx, id)
	if err != nil {
		return CreateResult{}, err
	}
	return s.executeCreateOperation(ctx, journal, id, attempt, parent, name, typ, opts, intent)
}

func (s *Service) executeCreateOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt operationRecord, parent uint64, name []byte, typ FileType, opts CreateOptions, intent []byte) (CreateResult, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, writeConflictRetryBudget)
	defer cancel()
	for retry := range writeConflictRetryLimit {
		result, err := s.publishCreateOperationAttempt(deadlineCtx, journal, id, attempt.AttemptID, parent, name, typ, opts, intent)
		if !isDefinitiveWriteConflict(err) {
			return result, err
		}
		attempt, err = journal.AdvanceGeneration(deadlineCtx, id, attempt.AttemptID)
		if err != nil {
			return CreateResult{}, err
		}
		if retry == writeConflictRetryLimit-1 {
			return CreateResult{}, errors.WithStack(ErrContentionLimit)
		}
		if err = waitWriteConflictRetry(deadlineCtx, retry); err != nil {
			return CreateResult{}, err
		}
	}
	return CreateResult{}, errors.WithStack(ErrContentionLimit)
}

func (s *Service) publishCreateOperationAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, parent uint64, name []byte, typ FileType, opts CreateOptions, intent []byte) (CreateResult, error) {
	snapshot, err := journal.PlanSnapshotAtRoute(ctx, id, attempt)
	if err != nil {
		return CreateResult{}, err
	}
	plan, semanticErr, err := s.planCreateOperation(ctx, snapshot, parent, name, typ, opts, intent)
	if err != nil {
		return CreateResult{}, err
	}
	if semanticErr != nil {
		return rejectCreateOperation(ctx, journal, id, attempt, plan, semanticErr)
	}
	record, err := journal.Publish(ctx, id, attempt, plan)
	if err == nil {
		return decodeCreateOperationResult(record)
	}
	if !isDefinitiveWriteConflict(err) {
		return resolveCreateAttempt(ctx, journal, id, attempt)
	}
	return CreateResult{}, err
}

func (s *Service) planCreateOperation(ctx context.Context, snapshot operationPlanningSnapshot, parent uint64, name []byte, typ FileType, opts CreateOptions, intent []byte) (operationAttemptPlan, error, error) {
	ts := snapshot.ReadTS
	entryKey := fskeys.DirEntryKey(parent, name)
	plan := operationAttemptPlan{Intent: intent, ReadTS: ts, RouteVersion: snapshot.RouteVersion,
		ReadKeys: [][]byte{fskeys.InodeKey(parent), entryKey}}
	parentMeta, err := s.inodeAt(ctx, parent, ts)
	if errors.Is(err, ErrNotFound) {
		return plan, ErrNotFound, nil
	}
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	if parentMeta.Type != TypeDirectory {
		return plan, ErrNotDir, nil
	}
	if _, err = s.dirEntryAt(ctx, parent, name, ts); err == nil {
		return plan, ErrExists, nil
	} else if !errors.Is(err, ErrNotFound) {
		return operationAttemptPlan{}, nil, err
	}
	inode, err := s.allocateInode(ctx, ts)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	result, effects, err := s.createOperationEffects(parent, parentMeta, inode, name, typ, opts)
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	plan.ReadKeys = append(plan.ReadKeys, fskeys.InodeKey(inode))
	plan.Effects, plan.ReadKeys, err = s.appendUsageUpdate(ctx, ts, effects, plan.ReadKeys, usageDelta{filesAdd: 1})
	if err != nil {
		return operationAttemptPlan{}, nil, err
	}
	plan.Result, err = json.Marshal(createOperationResult{Version: createOperationVersion, Result: result})
	return plan, nil, errors.Wrap(err, "encode create result")
}

func (s *Service) createOperationEffects(parent uint64, parentMeta *InodeMeta, inode uint64, name []byte, typ FileType, opts CreateOptions) (CreateResult, []*kv.Elem[kv.OP], error) {
	now := s.now().UnixNano()
	homeSlot := s.homeSlot(inode)
	nlink := fileInitialNlink
	if typ == TypeDirectory {
		nlink = directoryInitialNlink
		parentMeta.Nlink++
	}
	meta := InodeMeta{Inode: inode, Parent: parent, Type: typ, Mode: opts.Mode, UID: opts.UID, GID: opts.GID,
		AtimeNsec: now, MtimeNsec: now, CtimeNsec: now, ChunkSize: s.chunkSize,
		HomeSlot: homeSlot, Epoch: initialEpoch, Nlink: nlink, Generation: defaultGeneration}
	parentMeta.MtimeNsec, parentMeta.CtimeNsec = now, now
	effects, err := putElems(
		fskeys.InodeKey(inode), meta,
		fskeys.HomeKey(inode), Home{HomeSlot: homeSlot, State: HomeStateActive, Epoch: initialEpoch},
		fskeys.DirEntryKey(parent, name), DirEntry{Inode: inode, Type: typ},
		fskeys.InodeKey(parent), parentMeta,
		fskeys.DirVersionKey(parent), unixNsecVersion(now),
	)
	if err != nil {
		return CreateResult{}, nil, err
	}
	var fh uint64
	if len(opts.ClientID) > 0 {
		fh, err = s.allocID()
		if err != nil {
			return CreateResult{}, nil, err
		}
		ref, err := s.refPutElem(inode, opts.ClientID, fh)
		if err != nil {
			return CreateResult{}, nil, err
		}
		effects = append(effects, ref)
	}
	return CreateResult{Inode: inode, FH: fh, Stat: meta.Stat()}, effects, nil
}

func decodeCreateOperationResult(record operationRecord) (CreateResult, error) {
	var result createOperationResult
	if len(record.Result) == 0 || len(record.Result) > createOperationResultLimit || json.Unmarshal(record.Result, &result) != nil || result.Version != createOperationVersion {
		return CreateResult{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State == OperationRejected {
		return CreateResult{}, rejectedCreateError(result.Error)
	}
	if record.State != OperationCommitted || result.Error != 0 || result.Result.Inode == 0 || result.Result.Stat.Inode != result.Result.Inode {
		return CreateResult{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return result.Result, nil
}

func rejectedCreateError(status uint8) error {
	switch status {
	case createResultMissing:
		return errors.WithStack(ErrNotFound)
	case createResultNotDir:
		return errors.WithStack(ErrNotDir)
	case createResultExists:
		return errors.WithStack(ErrExists)
	default:
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
}

func resolveCreateOperation(ctx context.Context, journal *OperationJournal, id OperationID) (CreateResult, error) {
	record, err := journal.Resolve(ctx, id)
	if errors.Is(err, ErrOperationPending) {
		return CreateResult{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if err != nil {
		return CreateResult{}, err
	}
	return decodeCreateOperationResult(record)
}

func resolveCreateAttempt(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte) (CreateResult, error) {
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decodeCreateOperationResult(record)
	}
	return CreateResult{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func rejectCreateOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, semanticErr error) (CreateResult, error) {
	var status uint8
	switch {
	case errors.Is(semanticErr, ErrNotFound):
		status = createResultMissing
	case errors.Is(semanticErr, ErrNotDir):
		status = createResultNotDir
	case errors.Is(semanticErr, ErrExists):
		status = createResultExists
	default:
		return CreateResult{}, semanticErr
	}
	result, err := json.Marshal(createOperationResult{Version: createOperationVersion, Error: status})
	if err != nil {
		return CreateResult{}, errors.Wrap(err, "encode create rejection")
	}
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent, result, plan.ReadKeys)
	if err != nil {
		if isDefinitiveWriteConflict(err) {
			return CreateResult{}, err
		}
		if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !errors.Is(err, ErrOperationGeneration) {
			return CreateResult{}, err
		}
		return resolveCreateAttempt(ctx, journal, id, attempt)
	}
	return decodeCreateOperationResult(record)
}
