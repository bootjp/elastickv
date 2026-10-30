package etcd

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type asyncAppendRecordingStorage struct {
	recordingPersistStorage
	operations []string
	hardState  *raftpb.HardState
	entries    []*raftpb.Entry
	snapshot   *raftpb.Snapshot
	fail       string
	err        error
}

func (s *asyncAppendRecordingStorage) record(operation string) error {
	s.operations = append(s.operations, operation)
	if s.fail == operation {
		return s.err
	}
	return nil
}

func (s *asyncAppendRecordingStorage) Save(state *raftpb.HardState, entries []*raftpb.Entry) error {
	s.hardState, s.entries = state, entries
	return s.record("save")
}

func (s *asyncAppendRecordingStorage) SaveSnap(snapshot *raftpb.Snapshot) error {
	s.snapshot = snapshot
	return s.record("snapshot")
}

func (s *asyncAppendRecordingStorage) Sync() error { return s.record("sync") }

func asyncAppendMessage() *raftpb.Message {
	return &raftpb.Message{Type: messageTypePtr(raftpb.MsgStorageAppend), To: uint64Ptr(etcdraft.LocalAppendThread)}
}

func TestAsyncStorageAppendDurablePrefix(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"entry", "commit only", "response only", "snapshot"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			message := asyncAppendMessage()
			message.Responses = []*raftpb.Message{
				{Type: messageTypePtr(raftpb.MsgAppResp), To: uint64Ptr(2)},
				{Type: messageTypePtr(raftpb.MsgStorageAppendResp), To: uint64Ptr(1)},
			}
			want := []string{"save", "sync"}
			switch kind {
			case "entry":
				message.Entries = []*raftpb.Entry{{Index: uint64Ptr(3), Term: uint64Ptr(2), Data: []byte("payload")}}
			case "commit only":
				message.Term, message.Vote, message.Commit = uint64Ptr(2), uint64Ptr(1), uint64Ptr(3)
			case "response only":
				want = []string{"sync"}
			case "snapshot":
				message.Snapshot = &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: uint64Ptr(3), Term: uint64Ptr(2)}}
				want = []string{"snapshot", "sync"}
			}
			first, second := message.Responses[0], message.Responses[1]
			storage := &asyncAppendRecordingStorage{}
			require.NoError(t, persistAsyncStorageAppend(storage, message))
			require.Equal(t, want, storage.operations)
			require.Same(t, first, message.Responses[0])
			require.Same(t, second, message.Responses[1], "opaque response order cannot change")
			if kind == "commit only" {
				require.Equal(t, uint64(3), storage.hardState.GetCommit())
			}
		})
	}
}

func TestAsyncStorageAppendWithoutResponses(t *testing.T) {
	t.Parallel()
	message := asyncAppendMessage()
	message.Commit = uint64Ptr(3)
	storage := &asyncAppendRecordingStorage{}
	require.NoError(t, persistAsyncStorageAppend(storage, message))
	require.Equal(t, []string{"save"}, storage.operations)
	message.Commit = nil
	message.Responses = []*raftpb.Message{{Type: messageTypePtr(raftpb.MsgStorageAppendResp)}}
	require.NoError(t, persistAsyncStorageAppend(storage, message))
	require.Equal(t, []string{"save", "sync"}, storage.operations, "later response flushes the prior prefix")
}

func TestAsyncStorageAppendPreservesHardState(t *testing.T) {
	t.Parallel()
	storage := &asyncAppendRecordingStorage{}
	for _, state := range []struct {
		name               string
		term, vote, commit uint64
	}{
		{name: "complete state", term: 4, vote: 2, commit: 7},
		{name: "clear prior vote", term: 5, vote: 0, commit: 8},
	} {
		t.Run(state.name, func(t *testing.T) {
			message := asyncAppendMessage()
			message.Term, message.Vote, message.Commit = uint64Ptr(state.term), uint64Ptr(state.vote), uint64Ptr(state.commit)
			message.Responses = []*raftpb.Message{{Type: messageTypePtr(raftpb.MsgStorageAppendResp)}}
			before := proto.Clone(message)
			want := &raftpb.HardState{Term: uint64Ptr(state.term), Vote: uint64Ptr(state.vote), Commit: uint64Ptr(state.commit)}

			require.NoError(t, persistAsyncStorageAppend(storage, message))
			require.True(t, proto.Equal(want, storage.hardState), "saved state: %v", storage.hardState)
			require.True(t, proto.Equal(before, message), "input message must remain unchanged")
		})
	}
	require.Equal(t, []string{"save", "sync", "save", "sync"}, storage.operations)
}

func TestAsyncStorageAppendPreservesPayloads(t *testing.T) {
	t.Parallel()
	message := asyncAppendMessage()
	message.Entries = []*raftpb.Entry{
		{Type: raftpb.EntryNormal.Enum(), Index: uint64Ptr(7), Term: uint64Ptr(4), Data: []byte{0, 1, 255}},
		{Type: raftpb.EntryConfChangeV2.Enum(), Index: uint64Ptr(8), Term: uint64Ptr(5), Data: []byte("configuration")},
	}
	message.Snapshot = &raftpb.Snapshot{
		Data: []byte{255, 0, 2, 3},
		Metadata: &raftpb.SnapshotMetadata{
			Index: uint64Ptr(6), Term: uint64Ptr(3),
			ConfState: &raftpb.ConfState{
				Voters: []uint64{1, 2}, Learners: []uint64{4},
				VotersOutgoing: []uint64{1, 3}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true),
			},
		},
	}
	message.Responses = []*raftpb.Message{{Type: messageTypePtr(raftpb.MsgStorageAppendResp)}}
	want, ok := proto.Clone(message).(*raftpb.Message)
	require.True(t, ok)
	storage := &asyncAppendRecordingStorage{}

	require.NoError(t, persistAsyncStorageAppend(storage, message))
	require.Equal(t, []string{"snapshot", "save", "sync"}, storage.operations)
	require.Len(t, storage.entries, len(want.Entries))
	for i, entry := range want.Entries {
		require.True(t, proto.Equal(entry, storage.entries[i]), "saved entry %d: %v", i, storage.entries[i])
	}
	require.True(t, proto.Equal(want.Snapshot, storage.snapshot), "saved snapshot: %v", storage.snapshot)
	require.True(t, proto.Equal(want, message), "input message must remain unchanged")
}

func TestAsyncStorageAppendErrors(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"snapshot", "save", "sync"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			message := asyncAppendMessage()
			message.Snapshot = &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: uint64Ptr(3)}}
			message.Entries = []*raftpb.Entry{{Index: uint64Ptr(4)}}
			message.Responses = []*raftpb.Message{{Type: messageTypePtr(raftpb.MsgStorageAppendResp)}}
			failure := errors.New("disk failure")
			storage := &asyncAppendRecordingStorage{fail: stage, err: failure}
			require.ErrorIs(t, persistAsyncStorageAppend(storage, message), failure)
			want := map[string][]string{"snapshot": {"snapshot"}, "save": {"snapshot", "save"}, "sync": {"snapshot", "save", "sync"}}
			require.Equal(t, want[stage], storage.operations)
		})
	}
}

func TestAsyncStorageAppendRejectsMisroutedWork(t *testing.T) {
	t.Parallel()
	storage := &asyncAppendRecordingStorage{}
	require.ErrorIs(t, persistAsyncStorageAppend(nil, asyncAppendMessage()), errInvalidAsyncStorageAppend)
	require.ErrorIs(t, persistAsyncStorageAppend(storage, nil), errInvalidAsyncStorageAppend)
	message := asyncAppendMessage()
	message.To = uint64Ptr(2)
	require.ErrorIs(t, persistAsyncStorageAppend(storage, message), errInvalidAsyncStorageAppend)
	message.To, message.Type = uint64Ptr(etcdraft.LocalAppendThread), messageTypePtr(raftpb.MsgApp)
	require.ErrorIs(t, persistAsyncStorageAppend(storage, message), errInvalidAsyncStorageAppend)
	require.Empty(t, storage.operations)
}
