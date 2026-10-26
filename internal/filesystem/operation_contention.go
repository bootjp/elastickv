package filesystem

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const operationCASMaxBackoffShift = 5

func waitOperationCASRetry(ctx context.Context, retry int) error {
	return waitWriteConflictRetry(ctx, min(retry, operationCASMaxBackoffShift))
}

// An admitted attempt needs a durable decision even if its FUSE request is
// interrupted. The bounded context keeps publication and rejection live.
func operationDecisionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), writeConflictRetryBudget)
}

func cancelledOperationError(status, contentionStatus uint8) error {
	if status == contentionStatus {
		return errors.WithStack(ErrContentionLimit)
	}
	return errors.WithStack(ErrOperationRecordCorrupt)
}

// cancelOperationContention retains a terminal result after the last
// definitively rejected attempt has been fenced. A failure to prove the
// cancellation leaves the outcome unknown rather than claiming it was aborted.
func cancelOperationContention(ctx context.Context, journal *OperationJournal, id OperationID, result []byte) (operationRecord, error) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeConflictRetryBudget)
	defer cancel()
	for range operationCASRetries {
		record, err := journal.CancelWithResult(cleanupCtx, id, result)
		if err == nil {
			return record, nil
		}
		if !isDefinitiveWriteConflict(err) {
			break
		}
	}
	record, err := journal.Resolve(cleanupCtx, id)
	if err == nil {
		return record, nil
	}
	return operationRecord{}, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
}
