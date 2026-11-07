package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/bootjp/elastickv/store/storetest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func testTimestampBinding() TimestampBinding {
	return TimestampBinding{Timestamp: 100, OperationID: [16]byte{1}, ManifestDigest: [32]byte{2}, IsTxn: true, Phase: pb.Phase_PREPARE}
}

func testTimestampBindingStores(t *testing.T, st store.MVCCStore) (store.RaftControlMetadataStore, store.RaftEntryApplier) {
	t.Helper()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	return metadata, applier
}

func TestTimestampBindingAtomicAndIdempotent(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			metadata, applier := testTimestampBindingStores(t, st)
			binding := testTimestampBinding()
			operationKey, timestampKey := binding.keys()
			boom := errors.New("abort data entry")
			err := applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
				already, err := StageTimestampBinding(ctx, metadata, binding)
				require.NoError(t, err)
				require.False(t, already)
				require.NoError(t, st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte("value"), Value: []byte("aborted")}}, nil, 100, 100, 1))
				return boom
			})
			require.ErrorIs(t, err, boom)
			_, err = metadata.ReadRaftControlMetadata(t.Context(), operationKey)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, err = metadata.ReadRaftControlMetadata(t.Context(), timestampKey)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, err = st.GetAt(t.Context(), []byte("value"), 100)
			require.ErrorIs(t, err, store.ErrKeyNotFound)

			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 2, func(ctx context.Context) error {
				already, err := StageTimestampBinding(ctx, metadata, binding)
				require.NoError(t, err)
				require.False(t, already)
				return st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte("value"), Value: []byte("committed")}}, nil, 100, 100, 2)
			}))
			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 3, func(ctx context.Context) error {
				already, err := StageTimestampBinding(ctx, metadata, binding)
				require.True(t, already)
				return err
			}))
			got, err := st.GetAt(t.Context(), []byte("value"), 100)
			require.NoError(t, err)
			require.Equal(t, []byte("committed"), got)

			for _, changed := range []TimestampBinding{
				func() TimestampBinding { b := binding; b.Timestamp++; return b }(),
				func() TimestampBinding { b := binding; b.OperationID[0]++; return b }(),
				func() TimestampBinding { b := binding; b.ManifestDigest[0]++; return b }(),
				func() TimestampBinding { b := binding; b.Phase = pb.Phase_NONE; return b }(),
			} {
				err := applier.ApplyRaftEntry(t.Context(), 4, func(ctx context.Context) error {
					_, err := StageTimestampBinding(ctx, metadata, changed)
					return err
				})
				require.ErrorIs(t, err, ErrTimestampBindingConflict)
			}
		})
	}
}

func TestTimestampBindingRequiresIndexedScope(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, _ := testTimestampBindingStores(t, st)
	_, err := StageTimestampBinding(t.Context(), metadata, testTimestampBinding())
	require.ErrorIs(t, err, store.ErrRaftControlScopeRequired)
}

func TestTimestampBindingPebbleReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	binding := testTimestampBinding()
	metadata, applier := testTimestampBindingStores(t, st)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		already, err := StageTimestampBinding(ctx, metadata, binding)
		require.False(t, already)
		return err
	}))
	require.NoError(t, st.Close())

	st, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	metadata, applier = testTimestampBindingStores(t, st)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 2, func(ctx context.Context) error {
		already, err := StageTimestampBinding(ctx, metadata, binding)
		require.True(t, already)
		return err
	}))
}

func TestTimestampBindingConflictAbortsWholeEntry(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			metadata, applier := testTimestampBindingStores(t, st)
			binding := testTimestampBinding()
			conflicting := binding
			conflicting.OperationID[0]++
			err := applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
				already, err := StageTimestampBinding(ctx, metadata, binding)
				require.NoError(t, err)
				require.False(t, already)
				_, err = StageTimestampBinding(ctx, metadata, conflicting)
				return err
			})
			require.ErrorIs(t, err, ErrTimestampBindingConflict)
			operationKey, timestampKey := binding.keys()
			_, err = metadata.ReadRaftControlMetadata(t.Context(), operationKey)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, err = metadata.ReadRaftControlMetadata(t.Context(), timestampKey)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}
