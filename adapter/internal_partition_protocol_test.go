package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type partitionProtocolStore struct{ store.MVCCStore }

type legacyPartitionProtocolStore struct{ store.MVCCStore }

func (s *partitionProtocolStore) MigrationOwnershipPartitions(context.Context) (store.MigrationOwnershipPartitionState, error) {
	return store.MigrationOwnershipPartitionState{}, nil
}

func (s *partitionProtocolStore) ApplyMigrationOwnershipPartition(context.Context, store.MigrationOwnershipPartitionTransition, uint64) error {
	return store.ErrNotSupported
}

func TestMigrationPartitionProtocolSupportsUnconfiguredCandidate(t *testing.T) {
	st := &partitionProtocolStore{MVCCStore: store.NewMVCCStore()}
	proposer := &ownershipRuntimeProposer{fsm: kv.NewKvFSMWithHLC(st, nil)}
	tx := kv.NewTransactionWithProposer(proposer, kv.WithMigrationRequestAdmission(st, 7))
	t.Cleanup(tx.Close)
	i := NewInternalWithEngine(tx, migrationConfigurationLeader{}, nil, nil,
		WithInternalStore(st), WithInternalMigrationProposer(proposer),
		WithInternalMigrationPartitionProtocolReady(func() bool { return true }))
	resp, err := i.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1,
		Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY,
		RequireConfigurationIndex: true})
	require.NoError(t, err)
	field := resp.ProtoReflect().Descriptor().Fields().ByNumber(10)
	require.NotNil(t, field, "partition protocol field allocation must stay disjoint from transfer size")
	require.Equal(t, protoreflect.Name("ownership_partition_protocol"), field.Name())
	require.Equal(t, uint64(1), resp.ProtoReflect().Get(field).Uint())
	require.False(t, resp.GetReady(), "static support is not operational admission")
	require.Zero(t, resp.GetAppliedConfigurationIndex(), "a candidate has no applied configuration yet")
	require.Zero(t, resp.GetOwnershipProtocol())
}

func TestMigrationPartitionProtocolRequiresProposalAdmission(t *testing.T) {
	st := &partitionProtocolStore{MVCCStore: store.NewMVCCStore()}
	proposer := &ownershipRuntimeProposer{fsm: kv.NewKvFSMWithHLC(st, nil)}
	tx := kv.NewTransactionWithProposer(proposer)
	t.Cleanup(tx.Close)
	i := NewInternalWithEngine(tx, migrationConfigurationLeader{}, nil, nil,
		WithInternalStore(st), WithInternalMigrationProposer(proposer),
		WithInternalMigrationPartitionProtocolReady(func() bool { return true }))
	require.Zero(t, i.implementedPartitionProtocol(), "an explicit callback cannot replace a missing proposal hook")
	readyTx := kv.NewTransactionWithProposer(proposer, kv.WithMigrationRequestAdmission(st, 7))
	t.Cleanup(readyTx.Close)
	i.transactionManager = readyTx
	i.migrationProposer = nil
	require.Zero(t, i.implementedPartitionProtocol(), "a concrete control proposer is required")
}

func TestMigrationPartitionProtocolDefaultDenyAndStorePairing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store store.MVCCStore
		ready func() bool
	}{
		{name: "missing callback", store: &partitionProtocolStore{MVCCStore: store.NewMVCCStore()}},
		{name: "not installed", store: &partitionProtocolStore{MVCCStore: store.NewMVCCStore()}, ready: func() bool { return false }},
		{name: "missing store", ready: func() bool { return true }},
		{name: "unsupported store", store: &legacyPartitionProtocolStore{MVCCStore: store.NewMVCCStore()}, ready: func() bool { return true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proposer := &ownershipRuntimeProposer{fsm: kv.NewKvFSMWithHLC(store.NewMVCCStore(), nil)}
			tx := kv.NewTransactionWithProposer(proposer, kv.WithMigrationRequestAdmission(tc.store, 7))
			t.Cleanup(tx.Close)
			i := NewInternalWithEngine(tx, migrationConfigurationLeader{index: 9}, nil, nil,
				WithInternalStore(tc.store), WithInternalMigrationProposer(proposer), WithInternalMigrationPartitionProtocolReady(tc.ready))
			resp, err := i.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1,
				Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY})
			require.NoError(t, err)
			field := resp.ProtoReflect().Descriptor().Fields().ByNumber(10)
			require.NotNil(t, field)
			require.Zero(t, resp.ProtoReflect().Get(field).Uint())
			require.False(t, resp.GetReady())
		})
	}
}
