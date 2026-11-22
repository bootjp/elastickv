package store

import (
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// The FSM first proves the request's logical footprint is outside all capture
// sessions. Recheck the actual indexed-batch keys here before writing anything;
// an earlier request in the same entry may have introduced another key.
func (s *pebbleStore) verifyUncapturedPrefixDeletes(scope *raftEntryScope, iter *pebble.Iterator, deletes []PrefixDelete) error {
	if scope.capture == nil {
		return nil
	}
	if !scope.capture.recording || scope.capture.accept == nil {
		return errors.WithStack(ErrNotSupported)
	}
	for _, del := range deletes {
		if err := s.verifyUncapturedPrefix(scope, iter, del); err != nil {
			return err
		}
	}
	return nil
}

func (s *pebbleStore) verifyUncapturedPrefix(scope *raftEntryScope, iter *pebble.Iterator, del PrefixDelete) error {
	for iter.SeekGE(encodeKey(del.Prefix, math.MaxUint64)); iter.Valid(); {
		key, _, ok := nextScannableUserKey(iter)
		if !ok {
			break
		}
		action := s.classifyDeletePrefixKey(key, del.Prefix, del.ExcludePrefix)
		if action == deletePrefixStop {
			break
		}
		if action != deletePrefixSkip && scope.capture.accept(key) {
			return errors.WithStack(ErrNotSupported)
		}
		if !s.skipToNextUserKey(iter, key) {
			break
		}
	}
	return errors.WithStack(iter.Error())
}

func (s *pebbleStore) stageCaptureSafePrefixDeletes(scope *raftEntryScope, iter *pebble.Iterator, deletes []PrefixDelete, commitTS uint64) error {
	if scope.capture == nil {
		return s.stageDeletePrefixes(iter, scope.batch, deletes, commitTS)
	}
	if err := s.verifyUncapturedPrefixDeletes(scope, iter, deletes); err != nil {
		return err
	}
	before := scope.batch.Count()
	if err := s.stageDeletePrefixes(iter, scope.batch, deletes, commitTS); err != nil {
		return err
	}
	if scope.batch.Count() != before {
		scope.capture.stagedRequest = true
	}
	return nil
}
