package filesystem

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// rejectNamespaceOperation preserves a definitive OCC conflict for the caller's
// generation-fencing retry loop. Only an uncertain decision is resolved.
func rejectNamespaceOperation(ctx context.Context, journal *OperationJournal, id OperationID, attempt [16]byte, plan operationAttemptPlan, status byte, decode func(operationRecord) error) error {
	record, err := journal.RejectAtRoute(ctx, id, attempt, plan.ReadTS, plan.RouteVersion, plan.Intent,
		[]byte{status}, plan.ReadKeys)
	if err == nil {
		return decode(record)
	}
	if isDefinitiveWriteConflict(err) || (!errors.Is(err, kv.ErrTransactionOutcomeUnknown) && !errors.Is(err, ErrOperationGeneration)) {
		return err
	}
	outcome, record, err := journal.ResolveAttempt(ctx, id, attempt)
	if err == nil && (outcome == AttemptCommitted || (outcome == AttemptAborted && record.State == OperationRejected)) {
		return decode(record)
	}
	return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}
