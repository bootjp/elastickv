package etcd

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type gatedAsyncAppendStorage struct {
	asyncAppendRecordingStorage
	saveEntered chan struct{}
	saveRelease chan struct{}
	syncEntered chan struct{}
	syncRelease chan struct{}
}

func (s *gatedAsyncAppendStorage) Save(state *raftpb.HardState, entries []*raftpb.Entry) error {
	close(s.saveEntered)
	<-s.saveRelease
	return s.asyncAppendRecordingStorage.Save(state, entries)
}

func (s *gatedAsyncAppendStorage) Sync() error {
	close(s.syncEntered)
	<-s.syncRelease
	return s.asyncAppendRecordingStorage.Sync()
}

func TestAsyncStorageAppendWorkerSaveAndSyncBarriers(t *testing.T) {
	t.Parallel()
	worker := testStorageWorker(t, 1)
	storage := &gatedAsyncAppendStorage{
		saveEntered: make(chan struct{}), saveRelease: make(chan struct{}),
		syncEntered: make(chan struct{}), syncRelease: make(chan struct{}),
	}
	var saveOnce, syncOnce sync.Once
	releaseSave := func() { saveOnce.Do(func() { close(storage.saveRelease) }) }
	releaseSync := func() { syncOnce.Do(func() { close(storage.syncRelease) }) }
	t.Cleanup(releaseSave)
	t.Cleanup(releaseSync)
	message := asyncAppendMessage()
	message.Entries = []*raftpb.Entry{{Index: uint64Ptr(3), Term: uint64Ptr(2), Data: []byte("payload")}}
	message.Responses = []*raftpb.Message{
		{Type: messageTypePtr(raftpb.MsgAppResp), To: uint64Ptr(2)},
		{Type: messageTypePtr(raftpb.MsgStorageAppendResp), To: uint64Ptr(1)},
	}
	work, err := asyncStorageAppendWork(storage, message)
	require.NoError(t, err)
	budget := newStorageWorkBudget(work.retainedBytes, 1)
	require.NoError(t, worker.trySubmit(work, budget))
	awaitStorageSignal(t, storage.saveEntered)
	requireNoStorageCompletion(t, worker)
	requireStorageWorkUsage(t, budget, work.retainedBytes, 1)
	releaseSave()
	awaitStorageSignal(t, storage.syncEntered)
	requireNoStorageCompletion(t, worker)
	requireStorageWorkUsage(t, budget, work.retainedBytes, 1)
	releaseSync()
	completion := <-worker.completions
	t.Cleanup(completion.Release)
	require.NoError(t, completion.err)
	require.Same(t, message, completion.token, "the owner receives all opaque responses, not a synthesized ACK")
	require.Equal(t, []string{"save", "sync"}, storage.operations)
	requireStorageWorkUsage(t, budget, work.retainedBytes, 1)
	completion.Release()
	requireStorageWorkUsage(t, budget, 0, 0)
}

func requireNoStorageCompletion(t *testing.T, worker *storageWorker) {
	t.Helper()
	select {
	case <-worker.completions:
		t.Fatal("storage response released before the required durable prefix")
	default:
	}
}

func TestAsyncStorageAppendWorkInvalidAdmission(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		message *raftpb.Message
	}{
		{name: "nil"},
		{name: "remote append", message: &raftpb.Message{Type: messageTypePtr(raftpb.MsgStorageAppend), To: uint64Ptr(2)}},
		{name: "apply", message: &raftpb.Message{Type: messageTypePtr(raftpb.MsgStorageApply)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			storage := &asyncAppendRecordingStorage{}
			work, err := asyncStorageAppendWork(storage, test.message)
			require.ErrorIs(t, err, errInvalidAsyncStorageAppend)
			require.Nil(t, work.run)
			require.Empty(t, storage.operations)
		})
	}
	work, err := asyncStorageAppendWork(nil, asyncAppendMessage())
	require.ErrorIs(t, err, errInvalidAsyncStorageAppend)
	require.Nil(t, work.run)
}

// This checks the real WAL/snapshot codecs across a clean reopen. The held
// Save/Sync test above, not Close, establishes the response durability barrier.
// Engine restore ordering and crash boundaries remain separate acceptance gates.
func TestAsyncStorageAppendWorkerNativeReopen(t *testing.T) {
	t.Parallel()
	for _, withSnapshot := range []bool{false, true} {
		name := "entries"
		if withSnapshot {
			name = "snapshot_and_entries"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			disk, err := openDiskState(OpenConfig{DataDir: dir, StateMachine: &dummyFSM{}}, nil)
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					require.NoError(t, closePersist(disk.Persist))
				}
			})
			worker := testStorageWorker(t, 3)
			first := asyncAppendMessage()
			lastIndex := uint64(1)
			if withSnapshot {
				lastIndex = 5
				first.Snapshot = &raftpb.Snapshot{Data: []byte("snapshot-prefix"), Metadata: &raftpb.SnapshotMetadata{
					Index: uint64Ptr(4), Term: uint64Ptr(1),
					// The native snapshot codec materializes the default false.
					ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}, AutoLeave: proto.Bool(false)},
				}}
			}
			first.Term, first.Vote, first.Commit = uint64Ptr(2), uint64Ptr(1), uint64Ptr(lastIndex)
			first.Entries = []*raftpb.Entry{{Index: uint64Ptr(lastIndex), Term: uint64Ptr(2), Data: []byte{0, 255, 1, 2}}}
			clearedVote := asyncAppendMessage()
			clearedVote.Term, clearedVote.Vote, clearedVote.Commit = uint64Ptr(3), uint64Ptr(0), uint64Ptr(lastIndex)
			flush := asyncAppendMessage()
			flush.Responses = []*raftpb.Message{{Type: messageTypePtr(raftpb.MsgStorageAppendResp), To: uint64Ptr(1)}}
			budget := newStorageWorkBudget(1<<20, 3)
			for _, message := range []*raftpb.Message{first, clearedVote, flush} {
				work, workErr := asyncStorageAppendWork(disk.Persist, message)
				require.NoError(t, workErr)
				require.NoError(t, worker.trySubmit(work, budget))
			}
			worker.quiesce()
			for completion := range worker.completions {
				completion.Release()
				require.NoError(t, completion.err)
			}
			awaitStorageSignal(t, worker.done)
			requireStorageWorkUsage(t, budget, 0, 0)
			require.NoError(t, closePersist(disk.Persist))
			closed = true
			restored := &dummyFSM{}
			reopened, err := openDiskState(OpenConfig{DataDir: dir, StateMachine: restored}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, closePersist(reopened.Persist)) })
			state, _, err := reopened.Storage.InitialState()
			require.NoError(t, err)
			require.Equal(t, uint64(3), state.GetTerm())
			require.Zero(t, state.GetVote(), "clearing a previous vote must survive reopen")
			require.Equal(t, lastIndex, state.GetCommit())
			entries, err := reopened.Storage.Entries(lastIndex, lastIndex+1, math.MaxUint64)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.True(t, proto.Equal(first.Entries[0], entries[0]))
			if withSnapshot {
				require.True(t, proto.Equal(first.Snapshot, &reopened.LocalSnap), "want %s got %s", first.Snapshot, &reopened.LocalSnap)
				require.Equal(t, []byte("snapshot-prefix"), restored.restored)
			}
		})
	}
}
