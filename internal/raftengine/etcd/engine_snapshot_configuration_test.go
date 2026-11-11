package etcd

import (
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestLocalSnapshotAfterDeferredConfigurationPreservesMembership(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			for _, format := range []string{"inline", "file"} {
				t.Run(format, func(t *testing.T) {
					testLocalSnapshotAfterDeferredConfiguration(t, version == "v2", format == "file")
				})
			}
		})
	}
}

func TestLocalSnapshotCaptureKeepsOlderConfigurationPrefix(t *testing.T) {
	deferCapture := true
	fsm := &configPhaseFSM{visit: func(stage string) error {
		if stage == "snapshot" && deferCapture {
			return raftengine.ErrSnapshotDeferred
		}
		return nil
	}}
	fixture := newConfigPhaseFixture(t, false, fsm, mockstorage.NewStorageRecorder(""))
	e := fixture.engine
	require.NoError(t, e.applyCommitted([]raftpb.Entry{fixture.entry}))
	fixture.requirePublished(t)
	deferCapture = false
	entry := raftpb.Entry{Index: uint64Ptr(3), Term: uint64Ptr(1)}
	require.NoError(t, e.storage.Append([]*raftpb.Entry{&entry}))
	require.NoError(t, e.applyCommitted([]raftpb.Entry{entry}))
	e.snapshotEvery = 1
	e.snapshotReqCh = make(chan snapshotRequest, 1)
	e.snapshotStopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	require.NoError(t, e.maybePersistLocalSnapshot())
	older := <-e.snapshotReqCh

	deferCapture = true
	data, err := proto.Marshal(&raftpb.ConfChange{
		Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: uint64Ptr(fixture.joining.NodeID),
	})
	require.NoError(t, err)
	remove := raftpb.Entry{Index: uint64Ptr(4), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryConfChange), Data: data}
	require.NoError(t, e.storage.Append([]*raftpb.Entry{&remove}))
	require.NoError(t, e.applyCommitted([]raftpb.Entry{remove}))
	require.Empty(t, e.isLearnerNode)
	require.NoError(t, e.persistLocalSnapshot(older))
	current, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(3), current.GetMetadata().GetIndex())
	require.Equal(t, []uint64{fixture.joining.NodeID}, current.GetMetadata().GetConfState().GetLearners(),
		"an older captured FSM must not be paired with the owner's later membership")

	deferCapture = false
	entry.Index = uint64Ptr(5)
	require.NoError(t, e.storage.Append([]*raftpb.Entry{&entry}))
	require.NoError(t, e.applyCommitted([]raftpb.Entry{entry}))
	e.snapshotInFlight = false
	require.NoError(t, e.maybePersistLocalSnapshot())
	newer := <-e.snapshotReqCh
	require.NoError(t, e.persistLocalSnapshot(newer))
	current, err = e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(5), current.GetMetadata().GetIndex())
	require.Empty(t, current.GetMetadata().GetConfState().GetLearners())
}

func testLocalSnapshotAfterDeferredConfiguration(t *testing.T, v2, file bool) {
	t.Helper()
	deferCapture := true
	fsm := &configPhaseFSM{visit: func(stage string) error {
		if stage == "snapshot" && deferCapture {
			return raftengine.ErrSnapshotDeferred
		}
		return nil
	}}
	fixture := newConfigPhaseFixture(t, v2, fsm, mockstorage.NewStorageRecorder(""))
	e := fixture.engine
	if file {
		e.fsmSnapDir = filepath.Join(e.dataDir, fsmSnapDirName)
	}
	require.NoError(t, e.applyCommitted([]raftpb.Entry{fixture.entry}))
	fixture.requirePublished(t)
	previous, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, configPhaseBaseIndex, previous.GetMetadata().GetIndex())
	require.Empty(t, previous.GetMetadata().GetConfState().GetLearners())

	deferCapture = false
	index := configPhaseIndex + 1
	entry := raftpb.Entry{Index: uint64Ptr(index), Term: uint64Ptr(1)}
	require.NoError(t, e.storage.Append([]*raftpb.Entry{&entry}))
	require.NoError(t, e.applyCommitted([]raftpb.Entry{entry}))
	e.snapshotEvery = 1
	e.snapshotReqCh = make(chan snapshotRequest, 1)
	e.snapshotStopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	require.NoError(t, e.maybePersistLocalSnapshot())
	request := <-e.snapshotReqCh
	require.Equal(t, index, request.index)
	require.NoError(t, e.persistLocalSnapshot(request))
	current, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, index, current.GetMetadata().GetIndex())
	require.Equal(t, []uint64{1}, current.GetMetadata().GetConfState().GetVoters())
	require.Equal(t, []uint64{fixture.joining.NodeID}, current.GetMetadata().GetConfState().GetLearners(),
		"a later checkpoint must contain the membership applied before its FSM prefix, even when the configuration snapshot was deferred")
}
