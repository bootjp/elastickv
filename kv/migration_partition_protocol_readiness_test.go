package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type partitionReadinessNilStore struct {
	store.MVCCStore
	store.MigrationOwnershipStore
	store.MigrationOwnershipPartitionStore
	store.MigrationServingAdmissionReader
}

func TestMigrationPartitionProtocolReadinessRequiresInstalledRequestPath(t *testing.T) {
	st := store.NewMVCCStore()
	f, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
	require.True(t, ok)
	proposer := &migrationAdmissionProposer{fsm: f}
	var nilProposer *migrationAdmissionProposer
	var nilStore *partitionReadinessNilStore
	for _, tc := range []struct {
		name string
		tx   *TransactionManager
		want bool
	}{
		{name: "no proposal hook", tx: NewTransactionWithProposer(proposer)},
		{name: "no proposer", tx: NewTransactionWithProposer(nil, WithMigrationRequestAdmission(st, 7))},
		{name: "typed nil proposer", tx: NewTransactionWithProposer(nilProposer, WithMigrationRequestAdmission(st, 7))},
		{name: "typed nil store", tx: NewTransactionWithProposer(proposer, WithMigrationRequestAdmission(nilStore, 7))},
		{name: "zero group", tx: NewTransactionWithProposer(proposer, WithMigrationRequestAdmission(st, 0))},
		{name: "type-only legacy store", tx: NewTransactionWithProposer(proposer, WithMigrationRequestAdmission(&partitionHistoryStore{MVCCStore: st}, 7))},
		{name: "installed without active epoch", tx: NewTransactionWithProposer(proposer, WithMigrationRequestAdmission(st, 7)), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(tc.tx.Close)
			require.Equal(t, tc.want, MigrationPartitionRequestAdmissionReady(tc.tx))
			wrapped := &timestampAdmissionTransactional{inner: &leaseRefreshingTxn{inner: &LeaderProxy{tm: tc.tx}}}
			require.Equal(t, tc.want, MigrationPartitionRequestAdmissionReady(wrapped), "wrappers cannot invent an installed hook")
		})
	}
	var nilManager *TransactionManager
	require.False(t, MigrationPartitionRequestAdmissionReady(nilManager))
	require.False(t, MigrationPartitionRequestAdmissionReady(nil))
}

func TestMigrationPartitionProtocolReadinessRequiresCachedPublicReadPairing(t *testing.T) {
	engine := distribution.NewEngine()
	st := store.NewMVCCStore()
	var nilStore *partitionReadinessNilStore
	for _, tc := range []struct {
		name  string
		reads *ShardStore
		group uint64
		want  bool
	}{
		{name: "nil shard store", group: 7},
		{name: "no route engine", reads: NewShardStore(nil, map[uint64]*ShardGroup{7: {Store: st}}), group: 7},
		{name: "unknown group", reads: NewShardStore(engine, nil), group: 7},
		{name: "zero group", reads: NewShardStore(engine, map[uint64]*ShardGroup{0: {Store: st}})},
		{name: "typed nil store", reads: NewShardStore(engine, map[uint64]*ShardGroup{7: {Store: nilStore}}), group: 7},
		{name: "type-only legacy store", reads: NewShardStore(engine, map[uint64]*ShardGroup{7: {Store: &partitionHistoryStore{MVCCStore: st}}}), group: 7},
		{name: "installed without active epoch", reads: NewShardStore(engine, map[uint64]*ShardGroup{7: {Store: st}}), group: 7, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.reads.MigrationPartitionReadAdmissionReady(tc.group))
		})
	}
}
