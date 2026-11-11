package etcd

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestEnginePipelineSnapshotFilesKeepPinnedOlderPair(t *testing.T) {
	root := t.TempDir()
	env := pipelineConfigurationEnvironment{dataDir: root, fsmSnapDir: filepath.Join(root, fsmSnapDirName)}
	snapDir := filepath.Join(root, snapDirName)
	require.NoError(t, os.MkdirAll(snapDir, defaultDirPerm))
	require.NoError(t, os.MkdirAll(env.fsmSnapDir, defaultDirPerm))
	files := &pipelineSnapshotFiles{mu: &sync.Mutex{}}
	var pinnedCRC uint32
	for index := uint64(100); index <= 500; index += 100 {
		crc, _ := writeFSMFileForTest(t, env.fsmSnapDir, index, []byte("pinned-snapshot-payload"))
		createTokenSnapFileWithTerm(t, snapDir, 1, index, crc)
		if index == 100 {
			pinnedCRC = crc
		}
	}
	files.pin(100)
	require.NoError(t, files.purge(env))
	require.NoError(t, files.prepare(env, 600))
	reader, err := files.openReceived(env, 100, pinnedCRC)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("pinned-snapshot-payload"), data)
	require.NoError(t, reader.Close())
	require.FileExists(t, filepath.Join(snapDir, "0000000000000001-0000000000000064.snap"))
	files.unpin(100)
	require.NoError(t, files.prepare(env, 600))
	require.NoFileExists(t, fsmSnapPath(env.fsmSnapDir, 100))
	require.NoFileExists(t, filepath.Join(snapDir, "0000000000000001-0000000000000064.snap"))
	require.FileExists(t, fsmSnapPath(env.fsmSnapDir, 500), "verified restorable floor survives prewrite")
}

func TestEnginePipelineSnapshotFilesValidateFullChecksum(t *testing.T) {
	files := &pipelineSnapshotFiles{mu: &sync.Mutex{}}
	env := pipelineConfigurationEnvironment{fsmSnapDir: t.TempDir()}
	crc, path := writeFSMFileForTest(t, env.fsmSnapDir, 10, []byte("original-payload"))
	reader, err := files.openReceived(env, 10, crc+1)
	require.ErrorIs(t, err, ErrFSMSnapshotTokenCRC)
	require.Nil(t, reader)
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("X"), 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	reader, err = files.openReceived(env, 10, crc)
	require.Error(t, err, "matching footer/token cannot hide changed payload bytes")
	require.Nil(t, reader)
}

func TestEnginePipelineSnapshotDispatchPinLivesUntilRequestClose(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	e.initPipelineSnapshotFiles()
	queue := make(chan dispatchRequest, 1)
	e.peerDispatchers = map[uint64]*peerQueues{1: {normal: queue}}
	snapshot := raftTestSnapshot(10, 2, []uint64{1, 2}, encodeSnapshotToken(10, 123))
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), From: proto.Uint64(2), To: proto.Uint64(1), Snapshot: &snapshot}
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.Equal(t, 1, e.storagePipeline.snapshotFiles.pins[10])
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.Equal(t, 1, e.storagePipeline.snapshotFiles.pins[10], "queue rejection cannot retain another pin")
	request := <-queue
	copyOfRequest := request
	require.NoError(t, request.Close())
	require.NoError(t, copyOfRequest.Close())
	require.Empty(t, e.storagePipeline.snapshotFiles.pins)
}

func TestEnginePipelineSnapshotFinalizationRejectsNewOwnership(t *testing.T) {
	resources := &pipelineSnapshotResources{}
	released := 0
	lease, err := resources.add(func() { released++ })
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, resources.closeAll())
	require.Equal(t, 1, released)
	late, err := resources.add(func() { t.Error("ownership was accepted after finalization") })
	require.ErrorIs(t, err, errStorageWorkerClosed)
	require.Nil(t, late)
	require.NoError(t, resources.closeAll())
	require.Equal(t, 1, released)
}
