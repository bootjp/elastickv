package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnePhaseApplyRejectsTimestampAtAppliedWatermark(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(*testing.T) MVCCStore
	}{
		{"memory", func(*testing.T) MVCCStore { return NewMVCCStore() }},
		{"pebble", newApplyIndexPebbleStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := tc.new(t)
			put := func(key, val string) []*KVPairMutation {
				return []*KVPairMutation{{Op: OpTypePut, Key: []byte(key), Value: []byte(val)}}
			}
			require.NoError(t, st.ApplyMutationsRaftAtFenced(ctx, put("first", "45"), nil, 99, 100, 10))
			for _, ts := range []uint64{100, 99} {
				err := st.ApplyMutationsRaftAtFenced(ctx, put("later", "44"), nil, 98, ts, 11)
				require.ErrorIs(t, err, ErrWriteConflict)
			}
			_, err := st.GetAt(ctx, []byte("later"), 100)
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, uint64(100), st.LastCommitTS())
			require.NoError(t, st.ApplyMutationsRaftAtFenced(ctx, put("later", "44"), nil, 100, 101, 11))
		})
	}
}

func TestOnePhaseRaftEntryRejectsCollidingAppend(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)
	key := ListItemKey([]byte("jepsen-list"), 43)
	put := func(value string) []*KVPairMutation {
		return []*KVPairMutation{{Op: OpTypePut, Key: key, Value: []byte(value)}}
	}
	require.NoError(t, applier.ApplyRaftEntry(ctx, 10, func(ctx context.Context) error {
		return st.ApplyMutationsRaftAtFenced(ctx, put("45"), nil, 99, 100, 10)
	}))
	require.NoError(t, applier.ApplyRaftEntry(ctx, 11, func(ctx context.Context) error {
		err := st.ApplyMutationsRaftAtFenced(ctx, put("44"), nil, 99, 100, 11)
		require.ErrorIs(t, err, ErrWriteConflict)
		return nil
	}))
	got, err := st.GetAt(ctx, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("45"), got)
}
