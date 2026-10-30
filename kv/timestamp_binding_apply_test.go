package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/bootjp/elastickv/store/storetest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestApplyTimestampBoundCreation(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			binding := testTimestampBinding()
			calls := 0
			write := func(ctx context.Context) error {
				calls++
				return st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte("bound"), Value: []byte("first")}}, nil, binding.Timestamp, binding.Timestamp, 1)
			}
			already, err := ApplyTimestampBoundCreation(t.Context(), st, 1, binding, write)
			require.NoError(t, err)
			require.False(t, already)
			require.Equal(t, 1, calls)
			already, err = ApplyTimestampBoundCreation(t.Context(), st, 2, binding, write)
			require.NoError(t, err)
			require.True(t, already)
			require.Equal(t, 1, calls, "exact retry must not execute the write again")
			got, err := st.GetAt(t.Context(), []byte("bound"), binding.Timestamp)
			require.NoError(t, err)
			require.Equal(t, []byte("first"), got)
		})
	}
}

func TestApplyTimestampBoundCreationRollsBackWithWrite(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			binding := testTimestampBinding()
			boom := errors.New("write rejected")
			_, err := ApplyTimestampBoundCreation(t.Context(), st, 1, binding, func(ctx context.Context) error {
				require.NoError(t, st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte("bound"), Value: []byte("lost")}}, nil, binding.Timestamp, binding.Timestamp, 1))
				return boom
			})
			require.ErrorIs(t, err, boom)
			_, err = st.GetAt(t.Context(), []byte("bound"), binding.Timestamp)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			metadata, _ := testTimestampBindingStores(t, st)
			operationKey, timestampKey := binding.keys()
			_, err = metadata.ReadRaftControlMetadata(t.Context(), operationKey)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, err = metadata.ReadRaftControlMetadata(t.Context(), timestampKey)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}
