package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type partitionHistoryStore struct {
	store.MVCCStore
	partitions store.MigrationOwnershipPartitionState
	view       store.MigrationServingState
	err        error
}

func (s *partitionHistoryStore) MigrationOwnershipPartitions(context.Context) (store.MigrationOwnershipPartitionState, error) {
	return s.partitions, s.err
}

func (s *partitionHistoryStore) ApplyMigrationOwnershipPartition(context.Context, store.MigrationOwnershipPartitionTransition, uint64) error {
	return store.ErrNotSupported
}

func (s *partitionHistoryStore) MigrationServingState(context.Context) (store.MigrationServingState, error) {
	return s.view, s.err
}

func (s *partitionHistoryStore) AcquireMigrationOwnership(context.Context, store.MigrationOwnershipState, uint64) error {
	return store.ErrNotSupported
}

func (s *partitionHistoryStore) ClearMigrationStateWithOwnership(context.Context, store.MigrationOwnershipState, uint64) error {
	return store.ErrNotSupported
}

func TestMigrationPartitionHistoryPresent(t *testing.T) {
	for _, tc := range []struct {
		name       string
		partitions store.MigrationOwnershipPartitionState
		view       store.MigrationServingState
		want       bool
	}{
		{name: "empty"},
		{name: "active partition", want: true, partitions: store.MigrationOwnershipPartitionState{Partitions: []store.MigrationOwnershipPartition{{}}}},
		{name: "receipt only", want: true, partitions: store.MigrationOwnershipPartitionState{Receipts: []store.MigrationOwnershipPartitionReceipt{{}}}},
		{name: "legacy history", want: true, view: store.MigrationServingState{Ownership: []store.MigrationOwnershipState{{}}}},
		{name: "armed guard", want: true, view: store.MigrationServingState{Readiness: []store.TargetStagedReadinessState{{Armed: true}}}},
		{name: "unarmed guard", view: store.MigrationServingState{Readiness: []store.TargetStagedReadinessState{{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &partitionHistoryStore{MVCCStore: store.NewMVCCStore(), partitions: tc.partitions, view: tc.view}
			present, err := MigrationPartitionHistoryPresent(t.Context(), st)
			require.NoError(t, err)
			require.Equal(t, tc.want, present)
		})
	}
	st := &partitionHistoryStore{MVCCStore: store.NewMVCCStore(), err: errors.New("failed durable read")}
	_, err := MigrationPartitionHistoryPresent(t.Context(), st)
	require.ErrorIs(t, err, st.err)
	_, err = MigrationPartitionHistoryPresent(t.Context(), nil)
	require.ErrorIs(t, err, ErrInvalidRequest)
	present, err := MigrationPartitionHistoryPresent(t.Context(), store.NewMVCCStore())
	require.NoError(t, err)
	require.False(t, present)
}
