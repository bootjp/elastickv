package filesystem

import (
	"context"

	"github.com/cockroachdb/errors"
)

// VerifyOperationReceipt reads the authoritative terminal record and checks
// every participant proof before a local FUSE barrier can discharge it.
func (s *Service) VerifyOperationReceipt(ctx context.Context, id OperationID) error {
	journal, err := NewOperationJournal(s)
	if err != nil {
		return err
	}
	record, err := journal.Resolve(ctx, id)
	if err != nil {
		return err
	}
	if record.State != OperationCommitted {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	return nil
}

// AcknowledgeOperation retires a terminal operation after its FUSE reply was
// attempted and its participant proofs were verified by the adapter.
func (s *Service) AcknowledgeOperation(ctx context.Context, id OperationID) error {
	journal, err := NewOperationJournal(s)
	if err != nil {
		return err
	}
	if _, err := journal.Resolve(ctx, id); errors.Is(err, ErrOperationPending) {
		if _, err := journal.Cancel(ctx, id); errors.Is(err, ErrOperationPending) {
			return journal.RetireUnadmitted(ctx, id)
		} else if err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, ErrOperationRetired) {
		return err
	}
	return journal.Acknowledge(ctx, id)
}

func (s *Service) CloseOperationSession(ctx context.Context, id OperationSessionID) error {
	journal, err := NewOperationJournal(s)
	if err != nil {
		return err
	}
	return journal.CloseSession(ctx, id)
}
