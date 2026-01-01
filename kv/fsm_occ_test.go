package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestValidateConflictsDetectsStaleWrite(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 50, 0))

	fsm, ok := NewKvFSM(st).(*kvFSM)
	require.True(t, ok)

	muts := []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v2")}}
	err := fsm.validateConflicts(ctx, muts, 40)
	require.ErrorIs(t, err, store.ErrWriteConflict)
}

func TestApplyReturnsErrorOnConflict(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSM(st).(*kvFSM)
	require.True(t, ok)

	// First write commits at ts=100.
	put := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Ts:    100,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte("k"),
			Value: []byte("v1"),
		}},
	}
	data, err := proto.Marshal(put)
	require.NoError(t, err)

	resp := fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
	require.Nil(t, resp)

	// Stale transaction attempts to commit with startTS=90.
	conflict := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    90,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte("k"),
			Value: []byte("v2"),
		}},
	}
	data, err = proto.Marshal(conflict)
	require.NoError(t, err)

	resp = fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})

	err, ok = resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	// Ensure committed value remains.
	v, err := st.GetAt(ctx, []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)
}

