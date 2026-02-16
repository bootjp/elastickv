package kv

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func applyFSMRequest(t *testing.T, fsm *kvFSM, req *pb.Request) error {
	t.Helper()

	data, err := proto.Marshal(req)
	require.NoError(t, err)

	resp := fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
	if resp == nil {
		return nil
	}
	if err, ok := resp.(error); ok {
		return err
	}
	return fmt.Errorf("unexpected apply response type: %T", resp)
}

func TestTxnDuplicateMutations_LastWriteWins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSM(st).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	commitTS := uint64(20)
	key := []byte("k")

	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v1")},
			{Op: pb.Op_DEL, Key: key},
		},
	}
	require.NoError(t, applyFSMRequest(t, fsm, prepare))

	commit := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: key},
		},
	}
	require.NoError(t, applyFSMRequest(t, fsm, commit))

	_, err := st.GetAt(ctx, key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestCommitRejectsMismatchedPrimaryKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSM(st).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(11)
	commitTS := uint64(21)
	key := []byte("k")
	primary := []byte("p1")
	wrongPrimary := []byte("p2")

	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primary, LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("v1")},
		},
	}
	require.NoError(t, applyFSMRequest(t, fsm, prepare))

	commit := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: wrongPrimary, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: key},
		},
	}
	err := applyFSMRequest(t, fsm, commit)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxnInvalidMeta)

	_, err = st.GetAt(ctx, key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(ctx, txnLockKey(key), ^uint64(0))
	require.NoError(t, err)
}
