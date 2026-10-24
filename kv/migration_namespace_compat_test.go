package kv

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// This test is also run unchanged at 2d1a36ac. Compare the namespace-compat
// lines: they include the verdict and every resulting MVCC version. The
// preexisting wall-clock-derived lock deadline is normalized, and only that.
func TestMigrationNamespaceCompatibilityMatrix(t *testing.T) {
	keys := []string{"!migstage|ready|customer", "!migstage|ownership", "!migstage|retired", "!migstage|ack|", "!migs", "!dist|migstage|x", "!migwrite|x", "!migfence|x", "!txn|lock|x", "!txn|meta|migration_configuration_index", "!txn|meta|prepare_evidence/enabled", "!txn|backup|timestamp_floor", "!txn|foo", "_migack", "user"}
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE, pb.Phase_COMMIT, pb.Phase_ABORT} {
		for _, key := range keys {
			for _, op := range []pb.Op{pb.Op_PUT, pb.Op_DEL, pb.Op_DEL_PREFIX} {
				for _, mixed := range []bool{false, true} {
					name := fmt.Sprintf("%s/%q/%s/mixed=%v", phase, key, op, mixed)
					st := store.NewMVCCStore()
					f := &kvFSM{store: st, shardGroupID: 1}
					if phase == pb.Phase_COMMIT || phase == pb.Phase_ABORT {
						require.NoError(t, st.PutAt(t.Context(), txnLockKey([]byte(key)), encodeTxnLock(txnLock{StartTS: 10, CommitTS: 20, PrimaryKey: []byte(key), IsPrimaryKey: true}), 10, 0))
						require.NoError(t, st.PutAt(t.Context(), txnIntentKey([]byte(key)), encodeTxnIntent(txnIntent{StartTS: 10, Op: txnIntentOpPut, Value: []byte("before-upgrade")}), 10, 0))
					}
					request := &pb.Request{IsTxn: true, Phase: phase, Ts: 10, Mutations: []*pb.Mutation{
						{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte(key), CommitTS: 20})},
						{Op: op, Key: []byte(key), Value: []byte("after-upgrade")},
					}}
					if mixed {
						request.Mutations = append(request.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte("neighbor"), Value: []byte("value")})
					}
					data, err := proto.Marshal(request)
					require.NoError(t, err)
					namespaceCompatibilityState(t, name, st, f.Apply(append([]byte{raftEncodeSingle}, data...)))
					require.NoError(t, st.Close())
				}
			}
		}
	}
	for _, reverse := range []bool{false, true} {
		st := store.NewMVCCStore()
		f := &kvFSM{store: st}
		requests := []*pb.Request{
			{Ts: 10, Mutations: []*pb.Mutation{{Key: []byte("neighbor"), Value: []byte("first")}}},
			{Ts: 20, Mutations: []*pb.Mutation{{Op: pb.Op(99), Key: []byte("!migstage|ready|x")}}},
			{Ts: 30, Mutations: []*pb.Mutation{{Key: []byte("neighbor"), Value: []byte("last")}}},
		}
		if reverse {
			requests[0], requests[2] = requests[2], requests[0]
		}
		data, err := proto.Marshal(&pb.RaftCommand{Requests: requests})
		require.NoError(t, err)
		namespaceCompatibilityState(t, fmt.Sprintf("batch/reverse=%v", reverse), st, f.Apply(append([]byte{raftEncodeBatch}, data...)))
		require.NoError(t, st.Close())
	}
}

func namespaceCompatibilityState(t *testing.T, name string, st store.MVCCStore, response any) {
	t.Helper()
	verdict := fmt.Sprint(response)
	if batch, ok := response.(*fsmApplyResponse); ok {
		verdict = fmt.Sprint(batch.results)
	}
	exported, err := st.ExportVersions(t.Context(), store.ExportVersionsOptions{MaxCommitTSInclusive: ^uint64(0), MaxVersions: 1000, MaxBytes: 1 << 20, MaxScannedBytes: 1 << 20})
	require.NoError(t, err)
	require.True(t, exported.Done)
	for i := range exported.Versions {
		version := &exported.Versions[i]
		if !version.Tombstone && bytesHasTxnLockPrefix(version.Key) {
			lock, err := decodeTxnLock(version.Value)
			require.NoError(t, err)
			lock.TTLExpireAt = 0
			version.Value = encodeTxnLock(lock)
		}
	}
	raw, err := json.Marshal(struct {
		LastCommitTS uint64
		Versions     []store.MVCCVersion
	}{st.LastCommitTS(), exported.Versions})
	require.NoError(t, err)
	fmt.Printf("namespace-compat %s verdict=%q state=%x\n", name, verdict, sha256.Sum256(raw))
}

func bytesHasTxnLockPrefix(key []byte) bool {
	return len(key) >= len(txnLockPrefix) && string(key[:len(txnLockPrefix)]) == txnLockPrefix
}
