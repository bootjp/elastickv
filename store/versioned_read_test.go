package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetVersionAtVisibility(t *testing.T) {
	t.Parallel()
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		ctx := context.Background()
		st := newStore(t)
		key := []byte("key")
		require.NoError(t, st.PutAt(ctx, key, []byte("old"), 10, 0))
		require.NoError(t, st.PutAt(ctx, key, []byte{}, 20, 25))
		require.NoError(t, st.PutAt(ctx, key, []byte("new"), 30, 0))
		require.NoError(t, st.DeleteAt(ctx, key, 40))

		for _, tc := range []struct {
			snapshot uint64
			want     VersionedValue
		}{
			{9, VersionedValue{}},
			{19, VersionedValue{TS: 10, Value: []byte("old")}},
			{24, VersionedValue{TS: 20, Value: []byte{}, ExpireAt: 25}},
			{25, VersionedValue{}},
			{39, VersionedValue{TS: 30, Value: []byte("new")}},
			{40, VersionedValue{}},
		} {
			got, err := GetVersionAt(ctx, st, key, tc.snapshot)
			require.Equal(t, tc.want, got, "snapshot %d", tc.snapshot)
			if tc.want.TS == 0 {
				require.ErrorIs(t, err, ErrKeyNotFound)
			} else {
				require.NoError(t, err)
			}
		}

		got, err := GetVersionAt(ctx, st, key, 19)
		require.NoError(t, err)
		got.Value[0] = 'X'
		got, err = GetVersionAt(ctx, st, key, 19)
		require.NoError(t, err)
		require.Equal(t, []byte("old"), got.Value)

		retention, ok := st.(RetentionController)
		require.True(t, ok)
		retention.SetMinRetainedTS(20)
		_, err = GetVersionAt(ctx, st, key, 19)
		require.ErrorIs(t, err, ErrReadTSCompacted)
	})
}

func TestGetVersionAtEncrypted(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 17)
	ctx := context.Background()
	key := []byte("encrypted")
	value := bytes.Repeat([]byte("compressible"), 100)
	require.NoError(t, f.mvcc.PutAt(ctx, key, value, 10, 20))
	got, err := GetVersionAt(ctx, f.mvcc, key, 15)
	require.NoError(t, err)
	require.Equal(t, VersionedValue{TS: 10, Value: value, ExpireAt: 20}, got)
	_, err = GetVersionAt(ctx, f.mvcc, key, 20)
	require.ErrorIs(t, err, ErrKeyNotFound)

	f.tamperPebbleValue(t, key, 10, func(raw []byte) []byte {
		raw[0] |= 1 // tombstone bit in the authenticated header
		return raw
	})
	_, err = GetVersionAt(ctx, f.mvcc, key, 20)
	require.ErrorIs(t, err, ErrEncryptedReadIntegrity)
}

func TestGetVersionAtSeesStagedRaftEntry(t *testing.T) {
	t.Parallel()
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		ctx := context.Background()
		st := newStore(t)
		key := []byte("key")
		require.NoError(t, st.PutAt(ctx, key, []byte("old"), 10, 0))
		applier, ok := st.(RaftEntryApplier)
		require.True(t, ok)
		require.NoError(t, applier.ApplyRaftEntry(ctx, 1, func(entryCtx context.Context) error {
			mutations := raftEntryPut("key", "staged")
			mutations[0].ExpireAt = 30
			if err := st.ApplyMutationsRaftAt(entryCtx, mutations, nil, 20, 20, 1); err != nil {
				return err
			}
			got, err := GetVersionAt(entryCtx, st, key, 25)
			require.NoError(t, err)
			require.Equal(t, VersionedValue{TS: 20, Value: []byte("staged"), ExpireAt: 30}, got)
			return nil
		}))
	})
}
