package kv

import (
	"context"
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func migrationNamespaceKeys() [][]byte {
	return [][]byte{[]byte("!migstage|ready|"), []byte("!migstage|ownership"), []byte("!migstage|retired"),
		[]byte("!migstage|ack|"), []byte("!migwrite|source"), []byte("!migfence|source"),
		[]byte("!dist|migstage|data"), store.MigrationConfigurationIndexKey(),
		[]byte("!txn|lock|customer"), backupTimestampFloorKey, prepareEvidenceEnabledKey, prepareEvidenceGateKey, prepareEvidenceTimestampFloorKey}
}

func migrationNamespaceRequest(phase pb.Phase, key []byte) *pb.Request {
	return &pb.Request{IsTxn: true, Phase: phase, Ts: 10, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: 20})},
		{Op: pb.Op_PUT, Key: key, Value: []byte("not-control-metadata")},
	}}
}

func TestMigrationNamespaceRejectsAllTransactionPhasesAndAdvisory(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE, pb.Phase_COMMIT, pb.Phase_ABORT} {
		for _, key := range migrationNamespaceKeys() {
			t.Run(phase.String()+"/"+string(key), func(t *testing.T) {
				st := store.NewMVCCStore()
				f := &kvFSM{store: st, shardGroupID: 1}
				request := migrationNamespaceRequest(phase, key)
				if phase == pb.Phase_COMMIT || phase == pb.Phase_ABORT {
					seedLegacyMigrationIntent(t, st, key)
				}
				require.ErrorIs(t, CheckMigrationRequest(t.Context(), st, 1, request), ErrInvalidRequest)
				require.ErrorIs(t, applyFSMRequest(t, f, request), ErrInvalidRequest)
				_, err := st.GetAt(t.Context(), key, ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
				if phase == pb.Phase_COMMIT || phase == pb.Phase_ABORT {
					_, err = st.GetAt(t.Context(), txnIntentKey(key), ^uint64(0))
					require.NoError(t, err, "rejected resolution must not mutate the legacy intent")
				}
			})
		}
	}
}

func seedLegacyMigrationIntent(t *testing.T, st store.MVCCStore, key []byte) {
	t.Helper()
	require.NoError(t, st.PutAt(t.Context(), txnLockKey(key), encodeTxnLock(txnLock{
		StartTS: 10, CommitTS: 20, PrimaryKey: key, IsPrimaryKey: true}), 10, 0))
	require.NoError(t, st.PutAt(t.Context(), txnIntentKey(key), encodeTxnIntent(txnIntent{
		StartTS: 10, Op: txnIntentOpPut, Value: []byte("forged-before-upgrade")}), 10, 0))
}

func TestMigrationNamespacePebbleRemainsReopenableAfterRejectedWrite(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	assert.ErrorIs(t, applyFSMRequest(t, f, migrationNamespaceRequest(pb.Phase_NONE, []byte("!migstage|ready|"))), ErrInvalidRequest)
	for _, key := range []string{"_migack", "_mighlc", "_migpromote"} {
		require.NoError(t, applyFSMRequest(t, f, &pb.Request{Ts: 30, Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("ordinary")},
		}}))
	}
	control, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
		JobId: 7, MigrationJobId: 7, RouteStart: []byte("b"), RouteEnd: []byte("z"),
		ExpectedCutoverVersion: 1, MinWriteTsExclusive: 1, Armed: true})
	require.NoError(t, err)
	f.SetApplyIndex(1)
	require.Nil(t, f.Apply(control), "typed readiness controls must remain valid")
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err, "logical MVCC keys must not poison raw readiness metadata on reopen")
	reader, ok := reopened.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(t.Context())
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, uint64(7), states[0].JobID)
	require.True(t, states[0].Armed)
	for _, key := range []string{"_migack", "_mighlc", "_migpromote"} {
		value, getErr := reopened.GetAt(t.Context(), []byte(key), ^uint64(0))
		require.NoError(t, getErr)
		require.Equal(t, []byte("ordinary"), value)
	}
	require.NoError(t, reopened.Close())
}

func TestMigrationNamespacePreservesOrdinaryAliases(t *testing.T) {
	keys := append(legacyPrepareEvidenceUserKeys(), []byte("_migack"), []byte("_mighlc"), []byte("_migpromote"),
		[]byte("!migstage-user"), []byte("!migstage"), []byte("!txn|foo"), []byte("!txn|backup|customer"),
		[]byte("!hs|!migstage|ready||field"), []byte("customer"))
	for _, key := range keys {
		t.Run(string(key), func(t *testing.T) {
			st := store.NewMVCCStore()
			f := &kvFSM{store: st, shardGroupID: 1}
			request := migrationNamespaceRequest(pb.Phase_NONE, key)
			require.NoError(t, CheckMigrationRequest(context.Background(), st, 1, request))
			require.NoError(t, applyFSMRequest(t, f, request))
			value, err := st.GetAt(t.Context(), key, 20)
			require.NoError(t, err)
			require.Equal(t, []byte("not-control-metadata"), value)
			raw := &pb.Request{Ts: 30, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("raw")}}}
			require.NoError(t, CheckMigrationRequest(t.Context(), st, 1, raw))
			require.NoError(t, applyFSMRequest(t, f, raw))
		})
	}
}

func TestMigrationNamespaceRejectsPrefixDeletesAndCancelledAdvisory(t *testing.T) {
	for _, key := range [][]byte{[]byte("!migs"), []byte("!migstage|ready|"), []byte("!dist|migstage|"), []byte("!migwrite|"), []byte("!migfence|")} {
		for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE, pb.Phase_COMMIT, pb.Phase_ABORT} {
			st := store.NewMVCCStore()
			f := &kvFSM{store: st, shardGroupID: 1}
			request := migrationNamespaceRequest(phase, key)
			request.Mutations[1].Op = pb.Op_DEL_PREFIX
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, CheckMigrationRequest(ctx, st, 1, request), ErrInvalidRequest)
			require.ErrorIs(t, applyFSMRequest(t, f, request), ErrInvalidRequest)
		}
	}
}

func TestMigrationNamespacePreservesLegacyRawFlush(t *testing.T) {
	st := store.NewMVCCStore()
	f := &kvFSM{store: st, shardGroupID: 1}
	require.NoError(t, st.PutAt(t.Context(), []byte("customer"), []byte("ordinary"), 10, 0))
	configuration := binary.BigEndian.AppendUint64(nil, 5)
	require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), configuration, 10, 0))
	request := &pb.Request{Ts: 20, Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX}}}
	require.NoError(t, CheckMigrationRequest(t.Context(), st, 1, request))
	require.NoError(t, applyFSMRequest(t, f, request))
	_, err := st.GetAt(t.Context(), []byte("customer"), 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	control, err := st.GetAt(t.Context(), store.MigrationConfigurationIndexKey(), 20)
	require.NoError(t, err)
	require.Equal(t, configuration, control)
}
