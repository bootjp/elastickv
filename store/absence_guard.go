package store

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func hasAbsenceGuards(mutations []*KVPairMutation) bool {
	for _, mutation := range mutations {
		if mutation.RequireAbsent {
			return true
		}
	}
	return false
}

// Called under applyMu, against the indexed batch for entry-scoped apply so
// earlier requests in that same Raft entry are also visible to the predicate.
func (s *pebbleStore) checkAbsenceGuardsWith(ctx context.Context, r pebble.Reader, mutations []*KVPairMutation) error {
	if !hasAbsenceGuards(mutations) {
		return nil
	}
	iter, err := r.NewIter(&pebble.IterOptions{})
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()
	var seekKey, upperBound []byte
	for _, mutation := range mutations {
		if !mutation.RequireAbsent {
			continue
		}
		_, exists, err := s.latestCommitTSWithIter(ctx, iter, mutation.Key, &seekKey, &upperBound, false, 0)
		if err != nil {
			return err
		}
		if exists {
			s.writeConflicts.record(WriteConflictKindWrite, classifyWriteConflictKey(mutation.Key))
			return NewWriteConflictError(mutation.Key)
		}
	}
	return nil
}
