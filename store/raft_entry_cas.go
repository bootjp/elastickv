package store

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// StageRaftEntryProgress checks for replay and stages the current entry's
// index even when all of its requests are rejected. Call it before applying
// any requests in the scope. An error from the entry callback discards it.
func StageRaftEntryProgress(ctx context.Context, index uint64) (replayed bool, err error) {
	if sc, _ := ctx.Value(raftEntryScopeKey{}).(*raftEntryScope); sc != nil {
		return sc.stageProgress(index)
	}
	if sc, _ := ctx.Value(memoryEntryKey{}).(*memoryRaftEntry); sc != nil {
		return sc.stageProgress(index)
	}
	return false, errors.Wrap(ErrNotSupported, "Raft entry progress requires a data scope")
}

func (sc *raftEntryScope) stageProgress(index uint64) (bool, error) {
	if err := sc.checkDataIndex(index); err != nil {
		return false, err
	}
	if index == 0 {
		return false, nil
	}
	s := sc.store
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != sc.db {
		return false, sc.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
	}
	persisted, present, err := s.readAppliedIndexLocked()
	if err != nil {
		return false, sc.fail(err)
	}
	if present && index <= persisted {
		return true, nil
	}
	sc.touched = true
	return false, nil
}

func (sc *memoryRaftEntry) stageProgress(index uint64) (bool, error) {
	if err := sc.allowData(); err != nil {
		return false, err
	}
	if index != sc.index {
		return false, sc.fail(errors.WithStack(ErrRaftEntryIndexMismatch))
	}
	if index == 0 {
		return false, nil
	}
	if index <= sc.staged.controlAppliedIndex {
		return true, nil
	}
	sc.staged.controlAppliedIndex = index
	return false, nil
}

// RaftEntryCommitWatermark returns the advertised commit timestamp including
// writes staged earlier in this entry.
func RaftEntryCommitWatermark(ctx context.Context) (uint64, error) {
	if sc, _ := ctx.Value(raftEntryScopeKey{}).(*raftEntryScope); sc != nil {
		return max(sc.store.LastCommitTS(), sc.maxAdvertisedCommitTS), nil
	}
	if sc, _ := ctx.Value(memoryEntryKey{}).(*memoryRaftEntry); sc != nil {
		return sc.staged.LastCommitTS(), nil
	}
	return 0, errors.Wrap(ErrNotSupported, "Raft entry watermark requires a data scope")
}

// RaftEntryLatestRecord reads the version and authenticated visibility
// metadata of the head in the entry view, including tombstones and expired
// records, without returning the previous value. The caller evaluates
// liveness at the replicated write timestamp.
func RaftEntryLatestRecord(ctx context.Context, key []byte) (VersionedValue, error) {
	if sc, _ := ctx.Value(raftEntryScopeKey{}).(*raftEntryScope); sc != nil {
		return sc.latestRecord(key)
	}
	if sc, _ := ctx.Value(memoryEntryKey{}).(*memoryRaftEntry); sc != nil {
		version, exists := sc.staged.latestVersionLocked(key)
		if !exists {
			return VersionedValue{}, ErrKeyNotFound
		}
		version.Value = nil
		return version, nil
	}
	return VersionedValue{}, errors.Wrap(ErrNotSupported, "Raft entry head read requires a data scope")
}

func (sc *raftEntryScope) latestRecord(key []byte) (VersionedValue, error) {
	s := sc.store
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != sc.db {
		return VersionedValue{}, errors.WithStack(ErrRaftEntryStoreSwapped)
	}
	iter, err := sc.batch.NewIter(&pebble.IterOptions{
		LowerBound: encodeKey(key, math.MaxUint64),
		UpperBound: keyUpperBound(key),
	})
	if err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}
	record, err := s.readRaftEntryHead(iter, key)
	if closeErr := iter.Close(); closeErr != nil {
		return VersionedValue{}, errors.Wrap(closeErr, "close Raft entry head iterator")
	}
	return record, err
}

func (s *pebbleStore) readRaftEntryHead(iter *pebble.Iterator, key []byte) (VersionedValue, error) {
	if !iter.First() || !skipPebbleMetadataForward(iter) {
		if err := iter.Error(); err != nil {
			return VersionedValue{}, errors.WithStack(err)
		}
		return VersionedValue{}, ErrKeyNotFound
	}
	k := iter.Key()
	userKey, ts := decodeKeyView(k)
	if !bytes.Equal(userKey, key) {
		return VersionedValue{}, ErrKeyNotFound
	}
	raw, err := iter.ValueAndErr()
	if err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}
	value, err := decodeValueView(raw)
	if err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}
	if _, _, err := s.decryptAuthenticatedForKey(k, value, value.Value); err != nil {
		return VersionedValue{}, err
	}
	return VersionedValue{TS: ts, Tombstone: value.Tombstone, ExpireAt: value.ExpireAt}, nil
}
