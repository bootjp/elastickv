package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type recordedApply struct {
	op  pb.Op
	key string
}

type recordingApplyObserver struct {
	calls []recordedApply
}

func (o *recordingApplyObserver) OnApply(op pb.Op, key []byte) {
	o.calls = append(o.calls, recordedApply{op: op, key: string(key)})
}

func applyObserverTestRequest(t *testing.T, fsm *kvFSM, req *pb.Request) error {
	t.Helper()
	data, err := proto.Marshal(req)
	require.NoError(t, err)
	resp := fsm.Apply(data)
	if resp == nil {
		return nil
	}
	err, ok := resp.(error)
	require.True(t, ok, "unexpected Apply response type %T", resp)
	return err
}

func TestFSMApplyObserverRawMutationsAfterSuccess(t *testing.T) {
	st := store.NewMVCCStore()
	observer := &recordingApplyObserver{}
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithApplyObserver(observer)).(*kvFSM)
	require.True(t, ok)

	req := &pb.Request{
		Ts: 100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("put-key"), Value: []byte("v")},
			{Op: pb.Op_DEL, Key: []byte("del-key")},
		},
	}
	require.NoError(t, applyObserverTestRequest(t, fsm, req))
	require.Equal(t, []recordedApply{
		{op: pb.Op_PUT, key: "put-key"},
		{op: pb.Op_DEL, key: "del-key"},
	}, observer.calls)

	invalid := &pb.Request{
		Ts:        101,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Value: []byte("missing-key")}},
	}
	require.ErrorIs(t, applyObserverTestRequest(t, fsm, invalid), ErrInvalidRequest)
	require.Len(t, observer.calls, 2)

	stale := &pb.Request{
		Ts:        90,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("put-key"), Value: []byte("stale")}},
	}
	require.ErrorIs(t, applyObserverTestRequest(t, fsm, stale), store.ErrWriteConflict)
	require.Len(t, observer.calls, 2)
}

func TestFSMApplyObserverTxnVisibleMutationsOnly(t *testing.T) {
	st := store.NewMVCCStore()
	observer := &recordingApplyObserver{}
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithApplyObserver(observer)).(*kvFSM)
	require.True(t, ok)

	primary := []byte("primary")
	secondary := []byte("secondary")
	prepareTxn(t, fsm, primary, 10, [][]byte{primary, secondary}, [][]byte{[]byte("p"), []byte("s")})
	require.Empty(t, observer.calls, "PREPARE must not notify user-visible mutations")

	commit := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primary, CommitTS: 20})},
			{Op: pb.Op_PUT, Key: primary},
			{Op: pb.Op_PUT, Key: secondary},
		},
	}
	require.NoError(t, applyObserverTestRequest(t, fsm, commit))
	require.Equal(t, []recordedApply{
		{op: pb.Op_PUT, key: "primary"},
		{op: pb.Op_PUT, key: "secondary"},
	}, observer.calls)
}

func TestFSMApplyObserverOnePhaseTxnAfterSuccess(t *testing.T) {
	st := store.NewMVCCStore()
	observer := &recordingApplyObserver{}
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithApplyObserver(observer)).(*kvFSM)
	require.True(t, ok)

	req := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("stream"), CommitTS: 20})},
			{Op: pb.Op_PUT, Key: []byte("stream"), Value: []byte("entry")},
			{Op: pb.Op_DEL, Key: []byte("old-entry")},
		},
	}
	require.NoError(t, applyObserverTestRequest(t, fsm, req))
	require.Equal(t, []recordedApply{
		{op: pb.Op_PUT, key: "stream"},
		{op: pb.Op_DEL, key: "old-entry"},
	}, observer.calls)
}
