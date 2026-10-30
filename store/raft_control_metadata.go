package store

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// RaftControlMetadataPrefix is a raw, non-MVCC namespace beneath the already
// reserved transaction metadata family. It is not a new user-key namespace.
const RaftControlMetadataPrefix = "!txn|meta|raft-control|"

var ErrRaftControlScopeRequired = errors.New("store: control metadata requires an indexed raft entry scope")
var ErrRaftControlKey = errors.New("store: invalid raft control metadata key")
var ErrRaftControlStaleIndex = errors.New("store: protected raft entry would regress applied index")
var ErrExternalSnapshotControlState = errors.New("store: protected control state requires explicit v5 external rebase")

// RaftControlMetadataStore stages opaque policy, reservation, receipt, journal
// and learner records in the SAME transaction as inner indexed mutations.
// Record keys are nonempty suffixes, not physical namespace-prefixed keys.
// Staging requires ApplyRaftEntry with a nonzero index and its callback context.
// Read returns private bytes and ErrKeyNotFound for a missing record. The
// caller owns record codecs, provenance, duplicate validation and retention.
// A rejection receipt is staged followed by a nil callback result; returning
// any callback error aborts ALL changes, including the receipt.
type RaftControlMetadataStore interface {
	// ApplyRaftControlMetadata is a synced metadata-only transaction for J's
	// PREPARED/PUBLISHED phases. It cannot mutate data or advance applied index.
	ApplyRaftControlMetadata(context.Context, func(context.Context) error) error
	// StageRaftEntryMutationsPreservingLastCommitTS stages an inner MVCC
	// configuration projection without advertising a new data watermark.
	// Unlike the direct auxiliary-write API, it requires an indexed scope.
	StageRaftEntryMutationsPreservingLastCommitTS(context.Context, []*KVPairMutation, [][]byte, uint64, uint64) error
	ReadRaftControlMetadata(context.Context, []byte) ([]byte, error)
	StageRaftControlMetadata(context.Context, []byte, []byte) error
	DeleteRaftControlMetadata(context.Context, []byte) error
}

func (s *pebbleStore) StageRaftEntryMutationsPreservingLastCommitTS(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	scope := s.raftEntryScopeFrom(ctx)
	if scope == nil {
		return errors.WithStack(ErrRaftControlScopeRequired)
	}
	if scope.appliedIndex == 0 {
		return scope.fail(errors.WithStack(ErrRaftControlScopeRequired))
	}
	scope.forceSync = true
	return s.stagePreservingMutationsInRaftEntry(ctx, scope, mutations, readKeys, startTS, commitTS)
}

func (s *mvccStore) StageRaftEntryMutationsPreservingLastCommitTS(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	scope := s.memoryEntryScope(ctx)
	if scope == nil {
		return errors.WithStack(ErrRaftControlScopeRequired)
	}
	if scope.index == 0 {
		return scope.fail(errors.WithStack(ErrRaftControlScopeRequired))
	}
	if err := scope.allowData(); err != nil {
		return err
	}
	return scope.result(scope.staged.ApplyMutationsPreservingLastCommitTS(ctx, mutations, readKeys, startTS, commitTS))
}

func (s *pebbleStore) ApplyRaftControlMetadata(ctx context.Context, fn func(context.Context) error) error {
	return s.applyRaftEntry(ctx, 0, true, fn)
}

func (s *mvccStore) ApplyRaftControlMetadata(ctx context.Context, fn func(context.Context) error) error {
	return s.applyMemoryRaftEntry(ctx, 0, true, fn)
}

func IsRaftControlMetadataKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(RaftControlMetadataPrefix))
}

func raftControlKey(key []byte) ([]byte, error) {
	if len(key) == 0 || len(key) > maxSnapshotKeySize-len(RaftControlMetadataPrefix) {
		return nil, errors.WithStack(ErrRaftControlKey)
	}
	return append([]byte(RaftControlMetadataPrefix), key...), nil
}

func validateRaftControlStage(ctx context.Context, key, value []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := validateValueSize(value); err != nil {
		return nil, err
	}
	return raftControlKey(key)
}

func validateOrdinaryControlKey(key []byte) error {
	if IsRaftControlMetadataKey(key) {
		return errors.WithStack(errMVCCMetadataKeyCollision)
	}
	return nil
}

func validateControlVersionKey(key []byte, timestamp uint64) error {
	if err := validateOrdinaryControlKey(key); err != nil {
		return err
	}
	// A short logical key can borrow the inverted timestamp suffix to collide
	// with a raw metadata row. Long keys were checked without allocating above.
	if len(key) < len(RaftControlMetadataPrefix) && len(key)+timestampSize >= len(RaftControlMetadataPrefix) && IsRaftControlMetadataKey(encodeKey(key, timestamp)) {
		return errors.WithStack(errMVCCMetadataKeyCollision)
	}
	return nil
}

func (s *pebbleStore) ReadRaftControlMetadata(ctx context.Context, key []byte) ([]byte, error) {
	physical, err := raftControlKey(key)
	if err != nil {
		return nil, s.failControlRead(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, s.failControlRead(ctx, errors.WithStack(err))
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	var reader pebble.Reader = s.db
	if scope := s.raftEntryScopeFrom(ctx); scope != nil {
		if scope.failed != nil {
			return nil, scope.failed
		}
		if s.db != scope.db {
			return nil, scope.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
		}
		reader = scope.batch
	}
	value, closer, err := reader.Get(physical)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, s.failControlRead(ctx, errors.WithStack(err))
	}
	decoded, err := s.decodeRaftControlValue(physical, value)
	err = errors.Join(err, errors.WithStack(closer.Close()))
	if err != nil {
		return nil, s.failControlRead(ctx, err)
	}
	return decoded, nil
}

func (s *pebbleStore) failControlRead(ctx context.Context, err error) error {
	if scope := s.raftEntryScopeFrom(ctx); scope != nil {
		return scope.fail(err)
	}
	return err
}

func (s *pebbleStore) decodeRaftControlValue(key, value []byte) ([]byte, error) {
	sv, err := decodeValue(value)
	if err != nil {
		return nil, err
	}
	if sv.Tombstone || sv.ExpireAt != 0 {
		return nil, errors.New("store: corrupt raft control metadata value")
	}
	plain, flag, err := s.decryptAuthenticatedForKey(key, sv, sv.Value)
	if err != nil {
		return nil, err
	}
	plain, err = decompressAuthenticatedValue(plain, flag)
	return bytes.Clone(plain), err
}

func (s *pebbleStore) StageRaftControlMetadata(ctx context.Context, key, value []byte) error {
	return s.stageRaftControlMetadata(ctx, key, value, false)
}

func (s *pebbleStore) DeleteRaftControlMetadata(ctx context.Context, key []byte) error {
	return s.stageRaftControlMetadata(ctx, key, nil, true)
}

func (s *pebbleStore) stageRaftControlMetadata(ctx context.Context, key, value []byte, remove bool) error {
	scope := s.raftEntryScopeFrom(ctx)
	if scope == nil {
		return errors.WithStack(ErrRaftControlScopeRequired)
	}
	if scope.appliedIndex == 0 && !scope.controlOnly {
		return scope.fail(errors.WithStack(ErrRaftControlScopeRequired))
	}
	if scope.failed != nil {
		return scope.failed
	}
	physical, err := validateRaftControlStage(ctx, key, value)
	if err != nil {
		return scope.fail(err)
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != scope.db {
		return scope.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
	}
	if remove {
		err = scope.batch.Delete(physical, nil)
	} else {
		var body []byte
		var state byte
		body, state, err = s.encryptForKey(physical, value, 0, false)
		if err == nil {
			err = scope.batch.Set(physical, encodeValue(body, false, 0, state), nil)
		}
	}
	if err != nil {
		return scope.fail(errors.WithStack(err))
	}
	scope.touched = true
	scope.forceSync = true
	return nil
}

func (s *mvccStore) ReadRaftControlMetadata(ctx context.Context, key []byte) ([]byte, error) {
	if scope := s.memoryEntryScope(ctx); scope != nil {
		if scope.failed != nil {
			return nil, scope.failed
		}
		value, err := scope.staged.ReadRaftControlMetadata(ctx, key)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, scope.fail(err)
		}
		return value, err
	}
	if _, err := raftControlKey(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if change, ok := s.controlOverlay[string(key)]; ok {
		if change.removed {
			return nil, ErrKeyNotFound
		}
		return bytes.Clone(change.value), nil
	}
	value, ok := s.raftControl[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return bytes.Clone(value), nil
}

func (s *mvccStore) StageRaftControlMetadata(ctx context.Context, key, value []byte) error {
	return s.stageMemoryControlMetadata(ctx, key, value, false)
}

func (s *mvccStore) DeleteRaftControlMetadata(ctx context.Context, key []byte) error {
	return s.stageMemoryControlMetadata(ctx, key, nil, true)
}

func (s *mvccStore) stageMemoryControlMetadata(ctx context.Context, key, value []byte, remove bool) error {
	scope := s.memoryEntryScope(ctx)
	if scope == nil {
		return errors.WithStack(ErrRaftControlScopeRequired)
	}
	if scope.index == 0 && !scope.controlOnly {
		return scope.fail(errors.WithStack(ErrRaftControlScopeRequired))
	}
	if scope.failed != nil {
		return scope.failed
	}
	if !scope.controlOnly && scope.index < scope.staged.controlAppliedIndex {
		return scope.fail(errors.WithStack(ErrRaftControlStaleIndex))
	}
	if _, err := validateRaftControlStage(ctx, key, value); err != nil {
		return scope.fail(err)
	}
	if remove {
		scope.staged.controlOverlay[string(key)] = memoryControlValue{removed: true}
	} else {
		scope.staged.controlOverlay[string(key)] = memoryControlValue{value: bytes.Clone(value)}
	}
	scope.staged.controlAppliedIndex = max(scope.staged.controlAppliedIndex, scope.index)
	scope.touched = true
	return nil
}
