package etcd

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
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

func TestLegacySnapshotLimitPreservesHistoricalBudget(t *testing.T) {
	require.GreaterOrEqual(t, maxPersistedLegacySnapshot, uint32(256<<20))
	require.Greater(t, maxPersistedLegacySnapshot, maxPersistedEntryMessage)
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

func TestLoadEntriesFileHandlesMoreThanEntryCapacityCap(t *testing.T) {
	path := entriesFilePath(t.TempDir())

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	require.NoError(t, writeVersionedHeader(writer, fileFormat{magic: entriesFileMagic, version: entriesFileVersion}))
	index := uint64(1)
	for i := 0; i < int(entryCapacityCap)+1; i++ {
		entry := raftpb.Entry{
			Type:  raftpb.EntryNormal,
			Index: index,
			Term:  1,
			Data:  []byte("x"),
		}
		require.NoError(t, writeMessage(writer, entry.Marshal))
		index++
	}
	require.NoError(t, writer.Flush())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), defaultFilePerm))

	entries, err := loadEntriesFile(path)
	require.NoError(t, err)
	require.Len(t, entries, int(entryCapacityCap)+1)
	require.Equal(t, uint64(int(entryCapacityCap)+1), entries[len(entries)-1].Index)
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
	require.ErrorContains(t, err, errClusterMismatch.Error())
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

func TestLoadLegacyOrSplitStateRemovesStaleReplaceTemps(t *testing.T) {
	dir := t.TempDir()
	legacy := persistedState{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
				Index:     1,
				Term:      1,
			},
		},
	}
	require.NoError(t, saveStateFile(stateFilePath(dir), legacy))

	stale := []string{
		filepath.Join(dir, stateFileName+".tmp-stale"),
		filepath.Join(dir, metadataFileName+".tmp-stale"),
		filepath.Join(dir, entriesFileName+".tmp-stale"),
		filepath.Join(dir, snapshotDataFileName+".tmp-stale"),
	}
	for _, path := range stale {
		require.NoError(t, os.WriteFile(path, []byte("stale"), defaultFilePerm))
	}

	_, err := loadLegacyOrSplitState(dir)
	require.NoError(t, err)
	for _, path := range stale {
		_, statErr := os.Stat(path)
		require.True(t, os.IsNotExist(statErr), path)
	}
}

func TestSnapshotBytesAndCloseReturnsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	snapshot := &errorSnapshot{
		data:     []byte("snap"),
		closeErr: closeErr,
	}

	_, err := snapshotBytesAndClose(snapshot, t.TempDir())
	require.Error(t, err)
	require.True(t, errors.Is(err, closeErr))
	require.Equal(t, 1, snapshot.closeCalls)
}

func TestSnapshotBytesAndCloseClosesSnapshotWhenWriteFails(t *testing.T) {
	writeErr := errors.New("write failed")
	snapshot := &errorSnapshot{
		data:     []byte("snap"),
		writeErr: writeErr,
		closeErr: errors.New("close failed"),
	}

	_, err := snapshotBytesAndClose(snapshot, t.TempDir())
	require.Error(t, err)
	require.True(t, errors.Is(err, writeErr))
	require.Equal(t, 1, snapshot.closeCalls)
}

func TestSnapshotBytesDoesNotCloseSnapshot(t *testing.T) {
	snapshot := &errorSnapshot{data: []byte("snap")}

	data, err := snapshotBytes(snapshot, t.TempDir())
	require.NoError(t, err)
	require.Equal(t, []byte("snap"), data)
	require.Zero(t, snapshot.closeCalls)
}

type errorSnapshot struct {
	data       []byte
	writeErr   error
	closeErr   error
	closeCalls int
}

func (s *errorSnapshot) WriteTo(w io.Writer) (int64, error) {
	written, err := w.Write(s.data)
	if err != nil {
		return int64(written), err
	}
	if s.writeErr != nil {
		return int64(written), s.writeErr
	}
	return int64(written), nil
}

func (s *errorSnapshot) Close() error {
	s.closeCalls++
	return s.closeErr
}
