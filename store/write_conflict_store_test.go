package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMVCCStoreApplyMutations_RecordsConflicts preserves the lost-update
// write conflict and newer-read-version cases for each metric key class.
func TestMVCCStoreApplyMutations_RecordsConflicts(t *testing.T) {
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		cases := []struct {
			name  string
			key   []byte
			kind  string
			class string
		}{
			{"redis write", []byte("!redis|str|hotkey"), WriteConflictKindWrite, writeConflictClassRedisString},
			{"zset write", []byte("!zs|scr|leaderboard"), WriteConflictKindWrite, writeConflictClassZSet},
			{"rollback read", []byte("!txn|rb|primary\x00\x00\x00\x00\x00\x00\x00\x01"), WriteConflictKindRead, writeConflictClassTxnRollback},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := newStore(t)
				ctx := context.Background()
				commitTS, startTS := uint64(10), uint64(5)
				writeKey := tc.key
				var readKeys [][]byte
				if tc.kind == WriteConflictKindRead {
					commitTS, startTS = 20, 10
					writeKey = []byte("!redis|str|other")
					readKeys = [][]byte{tc.key}
				}
				require.NoError(t, s.ApplyMutations(ctx, []*KVPairMutation{
					{Op: OpTypePut, Key: tc.key, Value: []byte("v1")},
				}, nil, 0, commitTS))
				err := s.ApplyMutations(ctx, []*KVPairMutation{
					{Op: OpTypePut, Key: writeKey, Value: []byte("v2")},
				}, readKeys, startTS, commitTS+1)
				require.Error(t, err)
				require.ErrorIs(t, err, ErrWriteConflict)
				counter, ok := s.(interface{ WriteConflictCount() uint64 })
				require.True(t, ok)
				require.EqualValues(t, 1, counter.WriteConflictCount())
				snap := s.WriteConflictCountsByPrefix()
				assert.EqualValues(t, 1, snap[EncodeWriteConflictLabel(tc.kind, tc.class)])
				assert.Zero(t, snap[EncodeWriteConflictLabel(WriteConflictKindRead, writeConflictClassRedisString)])
			})
		}
	})
}

// TestMVCCStoreWriteConflictCountsByPrefix_EmptyByDefault guards the
// non-nil empty map required by the monitoring collector.
func TestMVCCStoreWriteConflictCountsByPrefix_EmptyByDefault(t *testing.T) {
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		snap := newStore(t).WriteConflictCountsByPrefix()
		require.NotNil(t, snap)
		assert.Empty(t, snap)
	})
}
