package etcd

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestCloneDispatchMessageConcurrentWALMarshal(t *testing.T) {
	entry := &raftpb.Entry{
		Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(7), Term: uint64Ptr(3),
		Data: bytes.Repeat([]byte("payload"), 128),
	}
	// Initialize the message descriptor before sharing immutable fields. Each
	// later Marshal can still update protobuf's internal size cache atomically.
	proto.Size(entry)
	start := make(chan struct{})
	finished := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	t.Cleanup(worker.Wait)
	go func() {
		defer worker.Done()
		<-start
		for range 2048 {
			if _, err := proto.Marshal(entry); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	close(start)
	for range 2048 {
		cloned := cloneDispatchMessage(raftpb.Message{Entries: []*raftpb.Entry{entry}}).Entries
		require.Len(t, cloned, 1)
		require.True(t, proto.Equal(entry, cloned[0]))
	}
	require.NoError(t, <-finished)
}

func TestCloneDispatchMessagePreservesWireFieldsAndOwnsPayload(t *testing.T) {
	entry := &raftpb.Entry{
		Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(0), Data: []byte("payload"),
	}
	entry.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	cloned := cloneDispatchMessage(raftpb.Message{Entries: []*raftpb.Entry{nil, entry, {Data: []byte{}}}}).Entries
	require.Len(t, cloned, 3)
	require.Nil(t, cloned[0])
	require.True(t, proto.Equal(entry, cloned[1]))
	require.Nil(t, cloned[1].Term, "absent optional field must remain absent")
	require.NotNil(t, cloned[1].Index, "present zero field must remain present")
	require.Equal(t, entry.ProtoReflect().GetUnknown(), cloned[1].ProtoReflect().GetUnknown())
	cloned[1].Data[0] = 'X'
	require.Equal(t, []byte("payload"), entry.Data)
	require.True(t, proto.Equal(&raftpb.Entry{Data: []byte{}}, cloned[2]))
	require.Nil(t, cloneDispatchMessage(raftpb.Message{}).Entries)
}

func TestCloneDispatchMessageOwnsOptionalHeadersAndPreservesNestedNil(t *testing.T) {
	original := raftpb.Message{Term: uint64Ptr(3), Responses: []*raftpb.Message{nil, {
		Entries: []*raftpb.Entry{nil}, Term: uint64Ptr(4),
	}}}
	cloned := cloneDispatchMessage(original)
	*original.Term = 9
	*original.Responses[1].Term = 10
	require.Equal(t, uint64(3), cloned.GetTerm())
	require.Equal(t, uint64(4), cloned.Responses[1].GetTerm())
	require.Nil(t, cloned.Responses[0])
	require.Nil(t, cloned.Responses[1].Entries[0])
}
