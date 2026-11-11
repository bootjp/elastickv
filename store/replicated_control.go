package store

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// This is a child of the already-reserved transaction metadata namespace.
// Reserving a new !txn| family would hide valid pre-upgrade user keys.
const ReplicatedControlPrefix = "!txn|meta|ttl-inline-control|"

var ErrReplicatedControlCollision = errors.New("replicated control state collides with existing MVCC versions")

// ReplicatedControlStore persists one FSM control-state replacement and its
// applied index atomically. False means the durable store already applied that
// index; a caller must reload its control state rather than install its stale
// computed replacement. The memory backend, like ApplyMutationsRaftAt, has no
// durable applied-index reader; its FSM must deduplicate via its saved state.
type ReplicatedControlStore interface {
	ApplyReplicatedControlState(ctx context.Context, key, value []byte, appliedIndex uint64) (bool, error)
}

// IsReplicatedControlKey identifies state reserved for typed FSM commands.
func IsReplicatedControlKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ReplicatedControlPrefix))
}

func validateReplicatedControlState(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if !IsReplicatedControlKey(key) || len(key) == len(ReplicatedControlPrefix) {
		return errors.New("replicated control state requires a reserved control key")
	}
	return validateValueSize(value)
}

func (s *mvccStore) ApplyReplicatedControlState(ctx context.Context, key, value []byte, _ uint64) (bool, error) {
	if sc := s.memoryEntryScope(ctx); sc != nil {
		ok, err := sc.staged.ApplyReplicatedControlState(ctx, key, value, 0)
		return ok, sc.result(err)
	}
	if err := validateReplicatedControlState(ctx, key, value); err != nil {
		return false, err
	}
	s.applyMu.Lock()
	s.mtx.Lock()
	defer s.applyMu.Unlock()
	defer s.mtx.Unlock()
	if value, exists := s.entryTreeGet(key); exists {
		versions, _ := value.([]VersionedValue)
		for _, version := range versions {
			if version.TS != 0 || version.Tombstone || version.ExpireAt != 0 {
				return false, errors.WithStack(ErrReplicatedControlCollision)
			}
		}
	}
	// Version zero is a fixed internal metadata slot, not a user timestamp.
	// Replacing it allocates no HLC value and cannot raise LastCommitTS.
	s.putVersionLocked(key, value, 0, 0)
	return true, nil
}

func (s *pebbleStore) ApplyReplicatedControlState(ctx context.Context, key, value []byte, appliedIndex uint64) (bool, error) {
	if err := validateReplicatedControlState(ctx, key, value); err != nil {
		return false, err
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if appliedIndex != 0 {
		current, present, err := s.readAppliedIndexLocked()
		if err != nil {
			return false, err
		}
		if present && current >= appliedIndex {
			return false, nil
		}
	}
	if err := s.checkReplicatedControlCollision(ctx, key); err != nil {
		return false, err
	}
	return s.writeReplicatedControlBatch(key, value, appliedIndex)
}

func (s *pebbleStore) checkReplicatedControlCollision(ctx context.Context, key []byte) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()
	var seekKey, upperBound []byte
	ts, exists, err := s.latestCommitTSWithIter(ctx, iter, key, &seekKey, &upperBound, false, 0)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if ts != 0 {
		return errors.WithStack(ErrReplicatedControlCollision)
	}
	tombstone, expireAt, _, err := decodeValueHeader(iter.Value())
	if err != nil {
		return errors.WithStack(err)
	}
	if tombstone || expireAt != 0 {
		return errors.WithStack(ErrReplicatedControlCollision)
	}
	return nil
}

func (s *pebbleStore) writeReplicatedControlBatch(key, value []byte, appliedIndex uint64) (bool, error) {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.applyMutationsBatch(batch, []*KVPairMutation{{Op: OpTypePut, Key: key, Value: value}}, 0, false); err != nil {
		return false, err
	}
	if appliedIndex != 0 {
		if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
			return false, err
		}
	}
	// Control state is a recovery/admission fence even with FSM nosync enabled.
	if err := batch.Commit(pebble.Sync); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}
