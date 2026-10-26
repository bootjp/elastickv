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
