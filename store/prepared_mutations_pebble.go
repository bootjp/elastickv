package store

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func (s *pebbleStore) ApplyPreparedMutationsRaftAt(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, originalStartTS, commitTS uint64, expected []MVCCVersion, appliedIndex uint64) error {
	if scope := s.raftEntryScopeFrom(ctx); scope != nil {
		return s.stagePreparedMutations(ctx, scope, mutations, readKeys, originalStartTS, commitTS, expected, appliedIndex)
	}
	return s.ApplyRaftEntry(ctx, appliedIndex, func(ctx context.Context) error {
		return s.ApplyPreparedMutationsRaftAt(ctx, mutations, readKeys, originalStartTS, commitTS, expected, appliedIndex)
	})
}

func (s *pebbleStore) stagePreparedMutations(ctx context.Context, scope *raftEntryScope, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64, expected []MVCCVersion, appliedIndex uint64) error {
	if err := scope.checkDataIndex(appliedIndex); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != scope.db {
		return scope.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
	}
	if err := s.checkPreparedApplyIndex(appliedIndex); err != nil {
		return scope.fail(err)
	}
	conflicts, err := preparedConflictMutations(ctx, mutations, expected, commitTS)
	if err != nil {
		return scope.failUnlessVerdict(err)
	}
	if err := s.comparePreparedVersions(ctx, scope.batch, expected); err != nil {
		return scope.failUnlessVerdict(err)
	}
	// Force OCC even for non-advertised versions. Read keys are never filtered.
	if err := s.checkApplyConflictsWith(ctx, scope.batch, scope.hasCommitsAfter, conflicts, readKeys, startTS, true); err != nil {
		return scope.failUnlessVerdict(err)
	}
	encoded, err := s.encodeMutationsForBatch(mutations, commitTS, false)
	if err != nil {
		return scope.failUnlessVerdict(err)
	}
	if err := stageEncodedMutations(scope.batch, encoded); err != nil {
		return scope.fail(err)
	}
	scope.observe(commitTS, true)
	return nil
}

func (s *pebbleStore) checkPreparedApplyIndex(index uint64) error {
	if index == 0 {
		return nil
	}
	persisted, present, err := s.readAppliedIndexLocked()
	if err != nil {
		return err
	}
	if present && persisted > index {
		return errors.Wrap(ErrRaftEntryIndexMismatch, "prepared apply index precedes durable applied index")
	}
	return nil
}

func (s *pebbleStore) comparePreparedVersions(ctx context.Context, reader pebble.Reader, expected []MVCCVersion) (result error) {
	if len(expected) == 0 {
		return nil
	}
	iter, err := reader.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			result = errors.WithStack(closeErr)
		}
	}()
	for _, version := range expected {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if err := s.comparePreparedVersion(iter, version); err != nil {
			return err
		}
	}
	return nil
}

func (s *pebbleStore) comparePreparedVersion(iter *pebble.Iterator, expected MVCCVersion) error {
	if !iter.SeekGE(encodeKey(expected.Key, math.MaxUint64)) {
		if err := iter.Error(); err != nil {
			return errors.WithStack(err)
		}
		return NewWriteConflictError(expected.Key)
	}
	key, ts := decodeKeyView(iter.Key())
	if !bytes.Equal(key, expected.Key) {
		return NewWriteConflictError(expected.Key)
	}
	value, err := decodeValue(iter.Value())
	if err != nil {
		return errors.WithStack(err)
	}
	// Authenticate the header before inspecting tombstone/expiry metadata.
	plain, err := s.decryptForKey(iter.Key(), value, value.Value)
	if err != nil {
		return err
	}
	if !preparedVersionMatches(expected, ts, value.Tombstone, value.ExpireAt, plain) {
		return NewWriteConflictError(expected.Key)
	}
	return nil
}
