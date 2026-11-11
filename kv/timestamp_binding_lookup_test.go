package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/bootjp/elastickv/store/storetest"
	"github.com/stretchr/testify/require"
)

func TestLookupTimestampBinding(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			metadata, applier := testTimestampBindingStores(t, st)
			binding := testTimestampBinding()
			got, found, err := LookupTimestampBinding(t.Context(), metadata, binding.OperationID)
			require.NoError(t, err)
			require.False(t, found)
			require.Equal(t, TimestampBinding{}, got)
			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
				_, err := StageTimestampBinding(ctx, metadata, binding)
				return err
			}))
			got, found, err = LookupTimestampBinding(t.Context(), metadata, binding.OperationID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, binding, got)
		})
	}
}

func TestLookupTimestampBindingRejectsMissingReverseIndex(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, applier := testTimestampBindingStores(t, st)
	binding := testTimestampBinding()
	operationKey, _ := binding.keys()
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		return metadata.StageRaftControlMetadata(ctx, operationKey, binding.encoded())
	}))
	_, _, err := LookupTimestampBinding(t.Context(), metadata, binding.OperationID)
	require.ErrorIs(t, err, ErrTimestampBindingConflict)
}

func TestLookupTimestampBindingRejectsMalformedRecord(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, applier := testTimestampBindingStores(t, st)
	binding := testTimestampBinding()
	operationKey, _ := binding.keys()
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		return metadata.StageRaftControlMetadata(ctx, operationKey, []byte{timestampBindingVersion})
	}))
	_, _, err := LookupTimestampBinding(t.Context(), metadata, binding.OperationID)
	require.ErrorIs(t, err, ErrTimestampBindingInvalid)
}
