package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// BeginAttempt records a fresh transaction generation before any effect is
// dispatched. A duplicate caller receives the same active attempt identity.
func (j *OperationJournal) BeginAttempt(ctx context.Context, id OperationID) (operationRecord, error) {
	for retry := range operationCASRetries {
		ts, record, session, err := j.pendingSnapshot(ctx, id)
		if err != nil {
			return operationRecord{}, err
		}
		if record.AttemptID != [16]byte{} {
			return record, nil
		}
		if record.Generation == ^uint64(0) {
			return operationRecord{}, errors.WithStack(ErrOperationGeneration)
		}
		record.AttemptID, err = randomOperationToken()
		if err != nil {
			return operationRecord{}, err
		}
		record.Generation++
		if err = j.storeRecordAndSession(ctx, ts, record, session, nil, nil); err != nil {
			if IsWriteConflict(err) {
				if err = waitOperationCASRetry(ctx, retry); err != nil {
					return operationRecord{}, err
				}
				continue
			}
			return operationRecord{}, err
		}
		return record, nil
	}
	return operationRecord{}, errors.WithStack(ErrContentionLimit)
}

// AdvanceGeneration fences an old attempt by committing a new version of its
// transaction primary. A delayed old publication either committed that primary
// first (making this CAS fail) or must fail its read-set validation against the
// new version. Legacy rows without the primary guarantee remain unresolved.
func (j *OperationJournal) AdvanceGeneration(ctx context.Context, id OperationID, old [16]byte) (operationRecord, error) {
	if old == [16]byte{} {
		return operationRecord{}, errors.WithStack(ErrInvalid)
	}
	for retry := range operationCASRetries {
		observed, ready := j.backend.JournalRouteVersion()
		if !ready {
			return operationRecord{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
		}
		record, err := j.advanceGenerationOnce(ctx, id, old, observed)
		if !IsWriteConflict(err) {
			return record, err
		}
		if err = waitOperationCASRetry(ctx, retry); err != nil {
			return operationRecord{}, err
		}
	}
	return operationRecord{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

func (j *OperationJournal) advanceGenerationOnce(ctx context.Context, id OperationID, old [16]byte, observed uint64) (operationRecord, error) {
	ts, record, session, err := j.pendingSnapshot(ctx, id)
	if err != nil {
		return operationRecord{}, err
	}
	if slices.Contains(record.FencedAttempts, old) {
		return record, nil
	}
	record, session, err = prepareGenerationFence(record, session, old)
	if err != nil {
		return operationRecord{}, err
	}
	if err = j.storeRecordAndSessionAtRoute(ctx, ts, record, session, nil, nil, observed); err != nil {
		return operationRecord{}, err
	}
	return record, nil
}

func prepareGenerationFence(record operationRecord, session operationSession, old [16]byte) (operationRecord, operationSession, error) {
	if !record.RecordIsPrimary || record.AttemptID != old {
		return operationRecord{}, operationSession{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if record.Generation == ^uint64(0) || len(record.FencedAttempts) >= maxOperationFencedAttempts {
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationGeneration)
	}
	if operationFenceReservation > session.MaxBytes-session.RetainedBytes {
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationQuota)
	}
	next, err := randomOperationToken()
	if err != nil {
		return operationRecord{}, operationSession{}, err
	}
	record.FencedAttempts = append(record.FencedAttempts, old)
	record.AttemptID = next
	record.Generation++
	record.Reservation += operationFenceReservation
	session.RetainedBytes += operationFenceReservation
	return record, session, nil
}

type operationAttemptPlan struct {
	Intent  []byte
	Effects []*kv.Elem[kv.OP]
	ReadTS  uint64
	// RouteVersion is captured before the planner reads any routed key.
	RouteVersion uint64
	ReadKeys     [][]byte
	Result       []byte
}

type operationPlanningSnapshot struct {
	ReadTS       uint64
	RouteVersion uint64
}

// PlanSnapshotAtRoute pins the placement used by the planner. A sharded
// planner must carry RouteVersion into publication or semantic rejection.
func (j *OperationJournal) PlanSnapshotAtRoute(ctx context.Context, id OperationID, attempt [16]byte) (operationPlanningSnapshot, error) {
	version, ready := j.backend.JournalRouteVersion()
	if !ready {
		return operationPlanningSnapshot{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	ts, err := j.PlanSnapshot(ctx, id, attempt)
	if err != nil {
		return operationPlanningSnapshot{}, err
	}
	return operationPlanningSnapshot{ReadTS: ts, RouteVersion: version}, nil
}

// PlanSnapshot establishes the OCC timestamp before a planner reads data.
// Every predicate key read at this timestamp must be included in ReadKeys.
func (j *OperationJournal) PlanSnapshot(ctx context.Context, id OperationID, attempt [16]byte) (uint64, error) {
	ts, record, _, err := j.pendingSnapshot(ctx, id)
	if err != nil {
		return 0, err
	}
	if attempt == [16]byte{} || record.AttemptID != attempt {
		return 0, errors.WithStack(ErrOperationGeneration)
	}
	return ts, nil
}

// AttemptOutcome is backed by the replicated operation row. In particular,
// absence of a row or an RPC timeout never means Aborted.
type AttemptOutcome uint8

const (
	AttemptUnknown AttemptOutcome = iota
	AttemptCommitted
	AttemptAborted
)

func (j *OperationJournal) ResolveAttempt(ctx context.Context, id OperationID, attempt [16]byte) (AttemptOutcome, operationRecord, error) {
	if attempt == [16]byte{} {
		return AttemptUnknown, operationRecord{}, errors.WithStack(ErrInvalid)
	}
	record, err := j.Resolve(ctx, id)
	return classifyAttemptOutcome(id, attempt, record, err)
}

func classifyAttemptOutcome(id OperationID, attempt [16]byte, record operationRecord, err error) (AttemptOutcome, operationRecord, error) {
	if err != nil && !errors.Is(err, ErrOperationPending) && !errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return AttemptUnknown, record, err
	}
	if record.ID != id {
		return AttemptUnknown, record, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if record.AttemptID != attempt {
		return classifyPriorAttempt(attempt, record)
	}
	if record.State == OperationCommitted && err == nil {
		return AttemptCommitted, record, nil
	}
	if record.State == OperationCancelled || record.State == OperationRejected {
		return AttemptAborted, record, nil
	}
	return AttemptUnknown, record, err
}

func classifyPriorAttempt(attempt [16]byte, record operationRecord) (AttemptOutcome, operationRecord, error) {
	if record.RecordIsPrimary && slices.Contains(record.FencedAttempts, attempt) {
		return AttemptAborted, record, nil
	}
	// A different attempt ID without a durable primary fence is not an abort
	// certificate for an older transaction.
	return AttemptUnknown, record, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}

// Publish commits the result record and every effect under the recorded
// generation. The session key is also a mutation, so closing the session and
// an in-flight attempt cannot both succeed on the same snapshot.
func (j *OperationJournal) Publish(ctx context.Context, id OperationID, attempt [16]byte, plan operationAttemptPlan) (operationRecord, error) {
	if len(plan.ReadKeys) != 0 && plan.ReadTS == 0 {
		return operationRecord{}, errors.WithStack(ErrInvalid)
	}
	if err := j.requirePlanningRoute(plan.RouteVersion); err != nil {
		if duplicate, resolveErr := j.resolveDuplicatePublication(ctx, id, attempt, plan.Intent, ErrOperationGeneration); resolveErr == nil {
			return duplicate, nil
		}
		return operationRecord{}, err
	}
	ts, record, session, err := j.publicationSnapshot(ctx, id, plan.ReadTS)
	if err != nil {
		return j.resolveDuplicatePublication(ctx, id, attempt, plan.Intent, err)
	}
	if attempt == [16]byte{} || record.AttemptID != attempt {
		return operationRecord{}, errors.WithStack(ErrOperationGeneration)
	}
	effectKeys, err := validateOperationPlan(record, session, plan)
	if err != nil {
		return operationRecord{}, err
	}
	groups, proofs, routedEffects, err := j.operationProofs(record.ID, plan.Effects)
	if err != nil {
		return operationRecord{}, err
	}
	record.State = OperationCommitted
	record.Result = append([]byte(nil), plan.Result...)
	record.EffectKeys = effectKeys
	record.ProofGroups = groups
	session.Pending--
	effects := slices.Concat(routedEffects, proofs)
	if err = j.storeRecordAndSessionAtRoute(ctx, ts, record, session, effects, plan.ReadKeys, plan.RouteVersion); err != nil {
		return j.resolveDuplicatePublication(ctx, id, attempt, plan.Intent, err)
	}
	return j.Resolve(ctx, id)
}

func (j *OperationJournal) requirePlanningRoute(observed uint64) error {
	current, ready := j.backend.JournalRouteVersion()
	// A changed catalog invalidates the planner's reads even if the commit
	// coordinator could still find a route for every write key.
	if !ready || (current != 0 && current != observed) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return nil
}

func (j *OperationJournal) publicationSnapshot(ctx context.Context, id OperationID, readTS uint64) (uint64, operationRecord, operationSession, error) {
	if readTS == 0 {
		return j.pendingSnapshot(ctx, id)
	}
	record, session, err := j.pendingSnapshotAt(ctx, id, readTS, false)
	return readTS, record, session, err
}

func (j *OperationJournal) operationProofs(id OperationID, effects []*kv.Elem[kv.OP]) ([]uint64, []*kv.Elem[kv.OP], []*kv.Elem[kv.OP], error) {
	groups := make([]uint64, 0, len(effects))
	routedEffects := make([]*kv.Elem[kv.OP], len(effects))
	for index, effect := range effects {
		groupID := effect.GroupID
		routedEffects[index] = effect
		if groupID == 0 {
			var err error
			groupID, err = j.backend.GroupIDForKey(effect.Key)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "route operation receipt")
			}
			routed := *effect
			routed.RequireGroupMatch = true
			routed.ExpectedGroupID = groupID
			routedEffects[index] = &routed
		}
		groups = append(groups, groupID)
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	if len(groups) > maxOperationProofGroups {
		return nil, nil, nil, errors.WithStack(ErrOperationQuota)
	}
	proofs := make([]*kv.Elem[kv.OP], 0, len(groups))
	for _, groupID := range groups {
		proofs = append(proofs, &kv.Elem[kv.OP]{
			Op: kv.Put, Key: fskeys.OperationProofKey(id.Filesystem, id.Session, id.Sequence, groupID),
			Value: []byte{1}, GroupID: groupID,
		})
	}
	return groups, proofs, routedEffects, nil
}

func (j *OperationJournal) resolveDuplicatePublication(ctx context.Context, id OperationID, attempt [16]byte, intent []byte, originalErr error) (operationRecord, error) {
	if !errors.Is(originalErr, ErrOperationGeneration) && !IsWriteConflict(originalErr) {
		return operationRecord{}, originalErr
	}
	record, err := j.Resolve(ctx, id)
	if record.State == OperationCommitted && record.AttemptID == attempt && bytes.Equal(record.Intent, intent) {
		return record, err
	}
	return operationRecord{}, originalErr
}

func validateOperationPlan(record operationRecord, session operationSession, plan operationAttemptPlan) ([][]byte, error) {
	if record.Fingerprint != sha256.Sum256(plan.Intent) || !bytes.Equal(record.Intent, plan.Intent) {
		return nil, errors.WithStack(ErrOperationIDReused)
	}
	if uint64(len(plan.Result)) > uint64(record.ResultLimit) {
		return nil, errors.WithStack(ErrOperationQuota)
	}
	effectKeys, err := operationEffectKeys(plan.Effects, record.ID.key(), session.ID.key())
	if err != nil {
		return nil, err
	}
	effectBytes := uint64(0)
	for _, key := range effectKeys {
		effectBytes += uint64(len(key))
	}
	if effectBytes > uint64(record.EffectLimit) {
		return nil, errors.WithStack(ErrOperationQuota)
	}
	return effectKeys, nil
}

// Reject publishes a terminal semantic failure only after the caller has
// validated its predicates on the same snapshot. OCC conflicts remain pending
// attempts and must not be stored as semantic rejection.
func (j *OperationJournal) Reject(ctx context.Context, id OperationID, attempt [16]byte, readTS uint64, intent []byte, result []byte, readKeys [][]byte) (operationRecord, error) {
	return j.RejectAtRoute(ctx, id, attempt, readTS, 0, intent, result, readKeys)
}

func (j *OperationJournal) RejectAtRoute(ctx context.Context, id OperationID, attempt [16]byte, readTS, routeVersion uint64, intent []byte, result []byte, readKeys [][]byte) (operationRecord, error) {
	if readTS == 0 {
		return operationRecord{}, errors.WithStack(ErrInvalid)
	}
	if err := j.requirePlanningRoute(routeVersion); err != nil {
		if duplicate, resolveErr := j.resolveDuplicateRejection(ctx, id, attempt, intent, ErrOperationGeneration); resolveErr == nil {
			return duplicate, nil
		}
		return operationRecord{}, err
	}
	ts, record, session, err := j.publicationSnapshot(ctx, id, readTS)
	if err != nil {
		return j.resolveDuplicateRejection(ctx, id, attempt, intent, err)
	}
	if err := validateOperationRejection(record, attempt, intent, result); err != nil {
		return operationRecord{}, err
	}
	record.State = OperationRejected
	record.Result = append([]byte(nil), result...)
	session.Pending--
	if err = j.storeRecordAndSessionAtRoute(ctx, ts, record, session, nil, readKeys, routeVersion); err != nil {
		return j.resolveDuplicateRejection(ctx, id, attempt, intent, err)
	}
	return j.Resolve(ctx, id)
}

func validateOperationRejection(record operationRecord, attempt [16]byte, intent, result []byte) error {
	if attempt == [16]byte{} || record.AttemptID != attempt {
		return errors.WithStack(ErrOperationGeneration)
	}
	if record.Fingerprint != sha256.Sum256(intent) || !bytes.Equal(record.Intent, intent) {
		return errors.WithStack(ErrOperationIDReused)
	}
	if uint64(len(result)) > uint64(record.ResultLimit) {
		return errors.WithStack(ErrOperationQuota)
	}
	return nil
}

func (j *OperationJournal) resolveDuplicateRejection(ctx context.Context, id OperationID, attempt [16]byte, intent []byte, originalErr error) (operationRecord, error) {
	if !errors.Is(originalErr, ErrOperationGeneration) && !IsWriteConflict(originalErr) {
		return operationRecord{}, originalErr
	}
	record, err := j.Resolve(ctx, id)
	if err == nil && record.State == OperationRejected && record.AttemptID == attempt && bytes.Equal(record.Intent, intent) {
		return record, nil
	}
	return operationRecord{}, originalErr
}

// Cancel must win an OCC transaction against any active publication. A lost
// cancellation response remains unknown until Resolve observes a terminal row.
func (j *OperationJournal) Cancel(ctx context.Context, id OperationID) (operationRecord, error) {
	return j.cancelWithResult(ctx, id, nil)
}

// CancelWithResult retains a typed failure when planning ended before any
// publication was dispatched. The result is immutable once cancellation wins.
func (j *OperationJournal) CancelWithResult(ctx context.Context, id OperationID, result []byte) (operationRecord, error) {
	return j.cancelWithResult(ctx, id, result)
}

func (j *OperationJournal) cancelWithResult(ctx context.Context, id OperationID, result []byte) (operationRecord, error) {
	ts, record, session, err := j.pendingSnapshotAllowClosed(ctx, id, true)
	if err != nil {
		return j.resolveWinningCancellation(ctx, id, err)
	}
	if uint64(len(result)) > uint64(record.ResultLimit) {
		return operationRecord{}, errors.WithStack(ErrOperationQuota)
	}
	record.State = OperationCancelled
	record.Result = append([]byte(nil), result...)
	session.Pending--
	if err = j.storeRecordAndSession(ctx, ts, record, session, nil, nil); err != nil {
		return operationRecord{}, err
	}
	return j.Resolve(ctx, id)
}

func (j *OperationJournal) resolveWinningCancellation(ctx context.Context, id OperationID, originalErr error) (operationRecord, error) {
	if !errors.Is(originalErr, ErrOperationGeneration) {
		return operationRecord{}, originalErr
	}
	resolved, err := j.Resolve(ctx, id)
	if err != nil {
		return resolved, err
	}
	if resolved.State != OperationPending {
		return resolved, nil
	}
	return operationRecord{}, originalErr
}

// Resolve is a leader-fenced read. A missing result is still pending, and a
// committed result is returned only when every effect's version is visible.
func (j *OperationJournal) Resolve(ctx context.Context, id OperationID) (operationRecord, error) {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return operationRecord{}, errors.Wrap(err, "fence operation resolution")
	}
	record, err := j.resolveRecordAt(ctx, id, ts)
	if err != nil {
		return record, err
	}
	if record.State != OperationCommitted {
		return record, nil
	}
	return record, j.verifyOperationEffects(ctx, record)
}

func (j *OperationJournal) resolveRecordAt(ctx context.Context, id OperationID, ts uint64) (operationRecord, error) {
	session, exists, err := j.loadSessionAt(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}, ts)
	if err != nil {
		return operationRecord{}, err
	}
	if !exists {
		return operationRecord{}, errors.WithStack(ErrOperationSessionNotFound)
	}
	record, found, err := j.loadRecordAt(ctx, id, ts)
	if err != nil {
		return operationRecord{}, err
	}
	if !found {
		if session.retired(id.Sequence) {
			return operationRecord{}, errors.WithStack(ErrOperationRetired)
		}
		return operationRecord{}, errors.WithStack(ErrOperationPending)
	}
	if record.State == OperationPending {
		return record, errors.WithStack(ErrOperationPending)
	}
	if record.CommitTS == 0 {
		return operationRecord{}, errors.Wrap(ErrOperationRecordCorrupt, "terminal operation has no commit timestamp")
	}
	return record, nil
}

func (j *OperationJournal) verifyOperationEffects(ctx context.Context, record operationRecord) error {
	for _, groupID := range record.ProofGroups {
		key := fskeys.OperationProofKey(record.ID.Filesystem, record.ID.Session, record.ID.Sequence, groupID)
		present, err := j.backend.CommittedVersionAtGroup(ctx, key, groupID, record.CommitTS)
		if err != nil {
			return errors.Wrap(err, "resolve operation effect")
		}
		if !present {
			return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
		}
	}
	return nil
}

func (j *OperationJournal) pendingSnapshot(ctx context.Context, id OperationID) (uint64, operationRecord, operationSession, error) {
	return j.pendingSnapshotAllowClosed(ctx, id, false)
}

func (j *OperationJournal) pendingSnapshotAllowClosed(ctx context.Context, id OperationID, allowClosed bool) (uint64, operationRecord, operationSession, error) {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return 0, operationRecord{}, operationSession{}, errors.Wrap(err, "fence operation attempt")
	}
	record, session, err := j.pendingSnapshotAt(ctx, id, ts, allowClosed)
	return ts, record, session, err
}

func (j *OperationJournal) pendingSnapshotAt(ctx context.Context, id OperationID, ts uint64, allowClosed bool) (operationRecord, operationSession, error) {
	session, exists, err := j.loadSessionAt(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}, ts)
	if err != nil {
		return operationRecord{}, operationSession{}, err
	}
	if !exists {
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationSessionNotFound)
	}
	if session.Closed && !allowClosed {
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationSessionClosed)
	}
	record, found, err := j.loadRecordAt(ctx, id, ts)
	if err != nil {
		return operationRecord{}, operationSession{}, err
	}
	if err = validatePendingSnapshot(record, session, found); err != nil {
		return operationRecord{}, operationSession{}, err
	}
	return record, session, nil
}

func validatePendingSnapshot(record operationRecord, session operationSession, found bool) error {
	if !found || record.State != OperationPending {
		return errors.WithStack(ErrOperationGeneration)
	}
	if record.ID.Session != session.ID.Session || session.Pending == 0 {
		return errors.Wrapf(ErrOperationRecordCorrupt,
			"pending operation lacks session accounting: sequence=%d generation=%d pending=%d retained_count=%d retained_bytes=%d reservation=%d",
			record.ID.Sequence, record.Generation, session.Pending, session.RetainedCount,
			session.RetainedBytes, record.Reservation)
	}
	return nil
}

func (j *OperationJournal) terminalAt(ctx context.Context, id OperationID, ts uint64) (operationRecord, operationSession, error) {
	session, exists, err := j.loadSessionAt(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}, ts)
	if err != nil {
		return operationRecord{}, operationSession{}, err
	}
	if !exists {
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationSessionNotFound)
	}
	record, found, err := j.loadRecordAt(ctx, id, ts)
	if err != nil {
		return operationRecord{}, operationSession{}, err
	}
	if !found {
		if session.retired(id.Sequence) {
			return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationRetired)
		}
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationPending)
	}
	if record.State == OperationPending {
		return operationRecord{}, operationSession{}, errors.WithStack(ErrOperationPending)
	}
	return record, session, nil
}

// Acknowledge retires a terminal identity only after the caller has attempted
// its reply. The session retains a reject-before watermark and bounded gaps,
// so deleting the detailed row cannot allow a delayed duplicate to execute.
func (j *OperationJournal) Acknowledge(ctx context.Context, id OperationID) error {
	if _, err := j.Resolve(ctx, id); err != nil {
		if errors.Is(err, ErrOperationRetired) {
			return nil
		}
		return err
	}
	for retry := range operationCASRetries {
		err := j.acknowledgeOnce(ctx, id)
		if errors.Is(err, ErrOperationRetired) {
			return nil
		}
		if err == nil {
			return nil
		}
		if !IsWriteConflict(err) {
			return errors.Wrap(err, "acknowledge operation")
		}
		if err = waitOperationCASRetry(ctx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

// RetireUnadmitted fences an identity whose reply was attempted before an
// operation record was admitted. The session and record read set makes this
// race atomically with admission: an in-flight admission either wins and must
// be resolved normally, or observes the retired identity and cannot publish.
func (j *OperationJournal) RetireUnadmitted(ctx context.Context, id OperationID) error {
	for retry := range operationCASRetries {
		err := j.retireUnadmittedOnce(ctx, id)
		if err == nil {
			return nil
		}
		if !IsWriteConflict(err) {
			return errors.Wrap(err, "retire unadmitted operation")
		}
		if err = waitOperationCASRetry(ctx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

func (j *OperationJournal) retireUnadmittedOnce(ctx context.Context, id OperationID) error {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "fence unadmitted operation retirement")
	}
	session, exists, err := j.loadSessionAt(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}, ts)
	if err != nil {
		return err
	}
	if !exists {
		return errors.WithStack(ErrOperationSessionNotFound)
	}
	if session.retired(id.Sequence) {
		return nil
	}
	_, found, err := j.loadRecordAt(ctx, id, ts)
	if err != nil {
		return err
	}
	if found {
		return errors.WithStack(ErrOperationGeneration)
	}
	if err := advanceAckWatermark(&session, id.Sequence); err != nil {
		return err
	}
	raw, err := encodeOperationSession(session)
	if err != nil {
		return errors.Wrap(err, "encode unadmitted retirement session")
	}
	_, err = j.backend.Commit(ctx, ts, []*kv.Elem[kv.OP]{{Op: kv.Put, Key: session.ID.key(), Value: raw}},
		[][]byte{id.key(), session.ID.key()})
	return errors.Wrap(err, "commit unadmitted operation retirement")
}

func (j *OperationJournal) acknowledgeOnce(ctx context.Context, id OperationID) error {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "fence operation acknowledgement")
	}
	record, session, err := j.terminalAt(ctx, id, ts)
	if err != nil {
		return err
	}
	if err = reserveAckRetirement(&session, record); err != nil {
		return err
	}
	raw, err := encodeOperationSession(session)
	if err != nil {
		return errors.Wrap(err, "encode acknowledged session")
	}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: id.key()},
		{Op: kv.Put, Key: session.ID.key(), Value: raw},
	}
	for _, groupID := range record.ProofGroups {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del,
			Key: fskeys.OperationProofKey(id.Filesystem, id.Session, id.Sequence, groupID), GroupID: groupID})
	}
	_, err = j.backend.Commit(ctx, ts, elems, [][]byte{id.key(), session.ID.key()})
	return errors.Wrap(err, "commit operation acknowledgement")
}

func reserveAckRetirement(session *operationSession, record operationRecord) error {
	// The session fence may become visible before the primary record deletion
	// on another group. A repeated ACK must not consume the reservation twice.
	if session.retired(record.ID.Sequence) {
		return errors.WithStack(ErrOperationRetired)
	}
	if session.RetainedCount == 0 || session.RetainedBytes < record.Reservation {
		return errors.Wrapf(ErrOperationRecordCorrupt,
			"ack reservation exceeds retained usage: sequence=%d generation=%d state=%d retained_count=%d retained_bytes=%d reservation=%d pending=%d reject_before=%d ack_gaps=%d",
			record.ID.Sequence, record.Generation, record.State, session.RetainedCount,
			session.RetainedBytes, record.Reservation, session.Pending,
			session.RejectBefore, len(session.AckGaps))
	}
	if err := advanceAckWatermark(session, record.ID.Sequence); err != nil {
		return err
	}
	session.RetainedCount--
	session.RetainedBytes -= record.Reservation
	return nil
}

func advanceAckWatermark(session *operationSession, sequence uint64) error {
	if sequence == session.RejectBefore+1 {
		session.RejectBefore++
		for len(session.AckGaps) > 0 && session.AckGaps[0] == session.RejectBefore+1 {
			session.RejectBefore++
			session.AckGaps = session.AckGaps[1:]
		}
		return nil
	}
	if len(session.AckGaps) >= maxOperationAckGaps {
		return errors.WithStack(ErrOperationQuota)
	}
	session.AckGaps = append(session.AckGaps, sequence)
	slices.Sort(session.AckGaps)
	return nil
}

// CloseSession is a durable fence. Pending attempts must reach a terminal
// state first, and the closed row stays as a non-expiring incarnation tombstone.
func (j *OperationJournal) CloseSession(ctx context.Context, id OperationSessionID) error {
	return j.setSessionClosed(ctx, id, false)
}

// FenceSession stops a crashed incarnation from publishing any new result.
// Pending operations remain durable and must be resolved/cancelled before a
// replacement session for the same owner can be registered.
func (j *OperationJournal) FenceSession(ctx context.Context, id OperationSessionID) error {
	return j.setSessionClosed(ctx, id, true)
}

// RecoverOwnerSession fences a previous mount incarnation and resolves its
// pending attempts before the owner registers a replacement session.
func (j *OperationJournal) RecoverOwnerSession(ctx context.Context, filesystemID [16]byte, owner string) error {
	if filesystemID == [16]byte{} || owner == "" {
		return errors.WithStack(ErrInvalid)
	}
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "snapshot previous operation owner")
	}
	previous, exists, err := j.loadOwnerAt(ctx, fskeys.OperationOwnerKey(filesystemID, []byte(owner)), ts)
	if err != nil || !exists {
		return err
	}
	id := OperationSessionID{Filesystem: filesystemID, Session: previous}
	if err := j.FenceSession(ctx, id); err != nil {
		return err
	}
	for {
		resolved, err := j.RecoverClosedSession(ctx, id, defaultIntentRecoveryLimit)
		if err != nil || resolved == 0 {
			return err
		}
	}
}

func (j *OperationJournal) setSessionClosed(ctx context.Context, id OperationSessionID, allowPending bool) error {
	for retry := range operationCASRetries {
		err := j.closeSessionOnce(ctx, id, allowPending)
		if err == nil {
			return nil
		}
		if !IsWriteConflict(err) {
			return errors.Wrap(err, "close operation session")
		}
		if err = waitOperationCASRetry(ctx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

func (j *OperationJournal) closeSessionOnce(ctx context.Context, id OperationSessionID, allowPending bool) error {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "fence operation session closure")
	}
	session, exists, err := j.loadSessionAt(ctx, id, ts)
	if err != nil {
		return err
	}
	if !exists {
		return errors.WithStack(ErrOperationSessionNotFound)
	}
	if session.Pending != 0 && !allowPending {
		return errors.WithStack(ErrOperationPending)
	}
	if session.Closed {
		return nil
	}
	session.Closed = true
	raw, err := encodeOperationSession(session)
	if err != nil {
		return err
	}
	_, err = j.backend.Commit(ctx, ts, []*kv.Elem[kv.OP]{{Op: kv.Put, Key: id.key(), Value: raw}}, [][]byte{id.key()})
	return errors.Wrap(err, "close operation session")
}

// RecoverClosedSession cancels pending operations after their session fence
// wins. A conflicting prepared transaction leaves its outcome unknown; this
// method stops and must be resumed after the transaction resolver progresses.
func (j *OperationJournal) RecoverClosedSession(ctx context.Context, id OperationSessionID, limit int) (int, error) {
	limit = operationRecoveryLimit(limit)
	ts, err := j.closedSessionSnapshot(ctx, id)
	if err != nil {
		return 0, err
	}
	prefix := fskeys.OperationPrefix(id.Filesystem, id.Session)
	cancelled := 0
	cursor := prefix
	for cancelled < limit {
		page, scanErr := j.backend.ScanAt(ctx, cursor, prefixEnd(prefix), defaultIntentRecoveryLimit, ts)
		if scanErr != nil {
			return cancelled, errors.Wrap(scanErr, "scan closed-session operations")
		}
		for _, pair := range page {
			changed, recoverErr := j.recoverOperationPair(ctx, id, prefix, pair)
			if recoverErr != nil {
				return cancelled, recoverErr
			}
			if changed {
				cancelled++
			}
			if cancelled == limit {
				return cancelled, nil
			}
		}
		if len(page) < defaultIntentRecoveryLimit {
			break
		}
		cursor = append(append([]byte(nil), page[len(page)-1].Key...), 0)
	}
	return cancelled, nil
}

func operationRecoveryLimit(limit int) int {
	if limit <= 0 || limit > defaultIntentRecoveryLimit {
		return defaultIntentRecoveryLimit
	}
	return limit
}

func (j *OperationJournal) closedSessionSnapshot(ctx context.Context, id OperationSessionID) (uint64, error) {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "fence closed-session recovery")
	}
	session, exists, err := j.loadSessionAt(ctx, id, ts)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, errors.WithStack(ErrOperationSessionNotFound)
	}
	if !session.Closed {
		return 0, errors.WithStack(ErrOperationSessionClosed)
	}
	return ts, nil
}

func (j *OperationJournal) recoverOperationPair(ctx context.Context, id OperationSessionID, prefix []byte, pair *store.KVPair) (bool, error) {
	if pair == nil || len(pair.Key) != len(prefix)+8 {
		return false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	opID := OperationID{Filesystem: id.Filesystem, Session: id.Session, Sequence: binary.BigEndian.Uint64(pair.Key[len(prefix):])}
	record, err := decodeOperationRecord(pair.Value)
	if err != nil || record.ID != opID {
		return false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State != OperationPending {
		return false, nil
	}
	_, err = j.Cancel(ctx, opID)
	return err == nil, errors.Wrap(err, "recover pending operation")
}
