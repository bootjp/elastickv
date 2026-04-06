package etcd

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"testing"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestLoadStateFileRejectsLargeEntryCount(t *testing.T) {
	path := stateFilePath(t.TempDir())

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	require.NoError(t, writeVersionedHeader(writer, fileFormat{magic: stateFileMagic, version: stateFileVersion}))
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
	require.NoError(t, writeU32(&buf, maxPersistedEntryMessage+1))

	var entry raftpb.Entry
	err := readMessage(&buf, &entry, maxPersistedEntryMessage, "entry")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestPersistedEntryLimitExceedsCurrentTransportBudget(t *testing.T) {
	require.Greater(t, maxPersistedEntryMessage, uint32(internalutil.GRPCMaxMessageBytes))
}

func TestLoadEntriesFileRejectsLargePayload(t *testing.T) {
	path := entriesFilePath(t.TempDir())

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	require.NoError(t, writeVersionedHeader(writer, fileFormat{magic: entriesFileMagic, version: entriesFileVersion}))
	require.NoError(t, writeU32(writer, maxPersistedEntryMessage+1))
	require.NoError(t, writer.Flush())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), defaultFilePerm))

	_, err := loadEntriesFile(path)
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
	require.NoError(t, saveMetadataFile(metadataFilePath(dir), state.HardState, state.Snapshot))

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

func TestLoadOrCreateStateMigratesLegacyStateFile(t *testing.T) {
	dir := t.TempDir()
	legacy := persistedState{
		Snapshot: raftpb.Snapshot{
			Data: mustEncodeSnapshotData(t, [][]byte{[]byte("snap")}),
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
				Index:     5,
				Term:      2,
			},
		},
		Entries: []raftpb.Entry{{
			Type:  raftpb.EntryNormal,
			Index: 6,
			Term:  2,
			Data:  encodeProposalEnvelope(1, []byte("tail")),
		}},
	}
	require.NoError(t, saveStateFile(stateFilePath(dir), legacy))

	state, err := loadOrCreateState(dir, 1)
	require.NoError(t, err)
	require.Equal(t, legacy.Entries, state.Entries)
	require.Equal(t, legacy.Snapshot.Metadata, state.Snapshot.Metadata)
	require.Nil(t, state.Snapshot.Data)

	payload, err := os.ReadFile(snapshotDataFilePath(dir))
	require.NoError(t, err)
	require.Equal(t, legacy.Snapshot.Data, payload)
}
