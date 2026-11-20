package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTwoGroupPrepareOpcodeCannotAliasBareLegacyRequest(t *testing.T) {
	request := &pb.Request{Ts: 4000, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("legacy"), Value: []byte("value")}}}
	legacy, err := proto.Marshal(request)
	require.NoError(t, err)
	require.Equal(t, byte(0x18), legacy[0], "bare legacy requests may begin with the timestamp field")
	require.NotEqual(t, legacy[0], raftEncodeTwoGroupPrepare)
	require.Error(t, proto.Unmarshal([]byte{raftEncodeTwoGroupPrepare}, new(pb.Request)),
		"the reserved opcode must be an invalid protobuf field tag")

	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
	require.True(t, ok)
	require.Nil(t, fsm.Apply(legacy))
	value, err := st.GetAt(t.Context(), []byte("legacy"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}
