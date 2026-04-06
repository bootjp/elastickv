package etcd

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestLoadStateFileRejectsLargeEntryCount(t *testing.T) {
	path := stateFilePath(t.TempDir())

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	require.NoError(t, writeFileHeader(writer))
	require.NoError(t, writeMessage(writer, (&raftpb.HardState{}).Marshal))
	require.NoError(t, writeMessage(writer, (&raftpb.Snapshot{}).Marshal))
	require.NoError(t, writeU32(writer, maxPersistedEntries+1))
	require.NoError(t, writer.Flush())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), defaultFilePerm))

	_, err := loadStateFile(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestReadMessageRejectsLargePayload(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeU32(&buf, maxPersistedMessage+1))

	var hardState raftpb.HardState
	err := readMessage(&buf, &hardState)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestOpenRejectsMultiNodePersistedState(t *testing.T) {
	dir := t.TempDir()
	state := persistedState{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
				Index:     1,
				Term:      1,
			},
		},
	}
	require.NoError(t, saveStateFile(stateFilePath(dir), state))

	_, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		StateMachine: &testStateMachine{},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, errSingleNodeOnly.Error())
}
