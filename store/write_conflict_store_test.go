package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPebbleStoreApplyMutations_RecordsWriteConflict verifies that a
// write-write conflict inside ApplyMutations increments the
// (kind=write, key_prefix=<bucket>) counter. Mirrors the real lost-
// update scenario: a txn reads at startTS=5, another commits a new
// version at commitTS=10, then the first txn tries to commit with
// startTS=5 — must fail and be counted.
func TestPebbleStoreApplyMutations_RecordsWriteConflict(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-writeconflict-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	key := []byte("!redis|str|hotkey")

	// Commit v1 at ts=10.
	require.NoError(t, s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: key, Value: []byte("v1")},
	}, nil, 0, 10))

	// Another txn tries to commit with startTS=5 (older than the committed v1).
	err = s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: key, Value: []byte("v2")},
	}, nil, 5, 11)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWriteConflict))

	ps, ok := s.(*pebbleStore)
	require.True(t, ok)
	require.EqualValues(t, 1, ps.WriteConflictCount())
	snap := ps.WriteConflictCountsByPrefix()
	assert.EqualValues(t, 1, snap[EncodeWriteConflictLabel(WriteConflictKindWrite, writeConflictClassRedisString)])
	assert.Zero(t, snap[EncodeWriteConflictLabel(WriteConflictKindRead, writeConflictClassRedisString)])
}

// TestPebbleStoreApplyMutations_RecordsReadConflict covers the RW
// conflict path: a txn's read set sees a newer committed version.
// Same metric, different kind label.
func TestPebbleStoreApplyMutations_RecordsReadConflict(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-readconflict-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	readKey := []byte("!txn|rb|primary\x00\x00\x00\x00\x00\x00\x00\x01")
	writeKey := []byte("!redis|str|other")

	// Commit a newer version at the read key so the next txn's
	// readKeys check observes ts > startTS.
	require.NoError(t, s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: readKey, Value: []byte("rollback_record")},
	}, nil, 0, 20))

	err = s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: writeKey, Value: []byte("v1")},
	}, [][]byte{readKey}, 10, 21)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWriteConflict))

	ps, ok := s.(*pebbleStore)
	require.True(t, ok)
	require.EqualValues(t, 1, ps.WriteConflictCount())
	snap := ps.WriteConflictCountsByPrefix()
	assert.EqualValues(t, 1, snap[EncodeWriteConflictLabel(WriteConflictKindRead, writeConflictClassTxnRollback)])
}

// TestMVCCStoreApplyMutations_RecordsConflicts verifies the in-memory
// implementation records conflicts identically to the pebble-backed
// one so both show up in the same Prometheus series.
func TestMVCCStoreApplyMutations_RecordsConflicts(t *testing.T) {
	s := NewMVCCStore()
	defer s.Close()
	ctx := context.Background()

	writeKey := []byte("!zs|scr|leaderboard")
	require.NoError(t, s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: writeKey, Value: []byte("v1")},
	}, nil, 0, 10))

	err := s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: writeKey, Value: []byte("v2")},
	}, nil, 5, 11)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWriteConflict))

	snap := s.WriteConflictCountsByPrefix()
	assert.EqualValues(t, 1, snap[EncodeWriteConflictLabel(WriteConflictKindWrite, writeConflictClassZSet)])
}

// TestMVCCStoreWriteConflictCountsByPrefix_EmptyByDefault guards the
// invariant that a freshly-constructed store returns a non-nil,
// empty map (so the monitoring collector does not have to nil-check).
func TestMVCCStoreWriteConflictCountsByPrefix_EmptyByDefault(t *testing.T) {
	s := NewMVCCStore()
	defer s.Close()

	snap := s.WriteConflictCountsByPrefix()
	require.NotNil(t, snap)
	assert.Empty(t, snap)
}
