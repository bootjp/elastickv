package store

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
)

// PreparedMutationApplier compares the latest prepared cleanup versions and
// applies their resolution atomically. User writes and all read keys retain
// OCC at originalStartTS; only exact compared cleanup deletes are exempt.
// Callers must verify the durable transaction proof before using this capability.
type PreparedMutationApplier interface {
	ApplyPreparedMutationsRaftAt(context.Context, []*KVPairMutation, [][]byte, uint64, uint64, []MVCCVersion, uint64) error
}

// Like txnLockKeyPrefix in store.go, this must match kv's namespace. A shared
// definition cannot live in kv without introducing a store -> kv import cycle.
var txnIntentKeyPrefix = []byte("!txn|int|")

func preparedCleanupKey(key []byte) bool {
	return (bytes.HasPrefix(key, txnLockKeyPrefix) && len(key) > len(txnLockKeyPrefix)) ||
		(bytes.HasPrefix(key, txnIntentKeyPrefix) && len(key) > len(txnIntentKeyPrefix))
}

// Validate the exemption set before filtering writes for OCC. Duplicate or
// unused expectations never confer authority, nor does a put on a cleanup key.
func preparedConflictMutations(ctx context.Context, mutations []*KVPairMutation, expected []MVCCVersion, commitTS uint64) ([]*KVPairMutation, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	known, err := preparedExpectedKeys(expected, commitTS)
	if err != nil {
		return nil, err
	}
	conflicts := make([]*KVPairMutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation == nil {
			return nil, errors.WithStack(ErrUnknownOp)
		}
		key := string(mutation.Key)
		if seen, exempt := known[key]; exempt {
			if seen || !preparedCleanupDelete(mutation) {
				return nil, NewWriteConflictError(mutation.Key)
			}
			known[key] = true
		} else {
			conflicts = append(conflicts, mutation)
		}
	}
	for _, version := range expected {
		if !known[string(version.Key)] {
			return nil, NewWriteConflictError(version.Key)
		}
	}
	return conflicts, validateMemoryMutations(ctx, mutations, commitTS)
}

func preparedExpectedKeys(expected []MVCCVersion, commitTS uint64) (map[string]bool, error) {
	known := make(map[string]bool, len(expected))
	for _, version := range expected {
		key := string(version.Key)
		if _, duplicate := known[key]; duplicate || !validPreparedExpected(version, commitTS) {
			return nil, NewWriteConflictError(version.Key)
		}
		known[key] = false
	}
	return known, nil
}

func validPreparedExpected(version MVCCVersion, commitTS uint64) bool {
	return preparedCleanupKey(version.Key) && version.CommitTS != 0 && version.CommitTS == commitTS &&
		!version.Tombstone && version.KeyFamily == 0
}

func preparedCleanupDelete(mutation *KVPairMutation) bool {
	return mutation.Op == OpTypeDelete && !mutation.RequireAbsent && mutation.ExpireAt == 0 && len(mutation.Value) == 0
}

func preparedVersionMatches(expected MVCCVersion, ts uint64, tombstone bool, expireAt uint64, value []byte) bool {
	return !tombstone && expected.CommitTS == ts && expected.ExpireAt == expireAt && bytes.Equal(expected.Value, value)
}

func (s *mvccStore) ApplyPreparedMutationsRaftAt(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, originalStartTS, commitTS uint64, expected []MVCCVersion, appliedIndex uint64) error {
	if err := s.memoryEntryIndex(ctx, appliedIndex); err != nil {
		return err
	}
	if scope := s.memoryEntryScope(ctx); scope != nil {
		if err := scope.allowData(); err != nil {
			return err
		}
		return scope.result(scope.staged.applyPreparedMutations(ctx, mutations, readKeys, originalStartTS, commitTS, expected))
	}
	return s.applyPreparedMutations(ctx, mutations, readKeys, originalStartTS, commitTS, expected)
}

func (s *mvccStore) applyPreparedMutations(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64, expected []MVCCVersion) error {
	conflicts, err := preparedConflictMutations(ctx, mutations, expected, commitTS)
	if err != nil {
		return err
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, version := range expected {
		actual, found := s.latestVersionLocked(version.Key)
		if !found || !preparedVersionMatches(version, actual.TS, actual.Tombstone, actual.ExpireAt, actual.Value) {
			return NewWriteConflictError(version.Key)
		}
	}
	if err := s.checkConflictsLocked(conflicts, readKeys, startTS); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	s.alignCommitTS(commitTS)
	for _, mutation := range mutations {
		switch mutation.Op {
		case OpTypePut:
			s.putVersionLocked(mutation.Key, mutation.Value, commitTS, mutation.ExpireAt)
		case OpTypeDelete:
			s.deleteVersionLocked(mutation.Key, commitTS)
		}
	}
	return nil
}
