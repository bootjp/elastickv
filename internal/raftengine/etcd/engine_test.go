package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type testStateMachine struct {
	mu      sync.Mutex
	applied [][]byte
}

func (s *testStateMachine) Apply(data []byte) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, append([]byte(nil), data...))
	return string(data)
}

func (s *testStateMachine) Applied() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.applied))
	for i, item := range s.applied {
		out[i] = append([]byte(nil), item...)
	}
	return out
}

func (s *testStateMachine) Snapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	count, err := uint32Len(len(s.applied))
	if err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, count); err != nil {
		return nil, err
	}
	for _, item := range s.applied {
		size, err := uint32Len(len(item))
		if err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, size); err != nil {
			return nil, err
		}
		if _, err := buf.Write(item); err != nil {
			return nil, err
		}
	}
	return &testSnapshot{data: buf.Bytes()}, nil
}

func (s *testStateMachine) Restore(r io.Reader) error {
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return err
	}

	applied := make([][]byte, 0, count)
	for range count {
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return err
		}
		item := make([]byte, length)
		if _, err := io.ReadFull(r, item); err != nil {
			return err
		}
		applied = append(applied, item)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = applied
	return nil
}

type testSnapshot struct {
	data []byte
}

func (s *testSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(s.data)
	return int64(n), err
}

func (s *testSnapshot) Close() error {
	return nil
}

func TestOpenSingleNodeProposeAndReadIndex(t *testing.T) {
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      t.TempDir(),
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.Equal(t, raftengine.StateLeader, engine.State())

	result, err := engine.Propose(context.Background(), []byte("alpha"))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotZero(t, result.CommitIndex)
	require.Equal(t, "alpha", result.Response)

	readIndex, err := engine.LinearizableRead(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, readIndex, result.CommitIndex)

	status := engine.Status()
	require.Equal(t, raftengine.StateLeader, status.State)
	require.Equal(t, readIndex, status.AppliedIndex)
	require.Equal(t, "n1", status.Leader.ID)

	cfg, err := engine.Configuration(context.Background())
	require.NoError(t, err)
	require.Equal(t, raftengine.Configuration{
		Servers: []raftengine.Server{{
			ID:       "n1",
			Address:  "127.0.0.1:7001",
			Suffrage: "voter",
		}},
	}, cfg)

	require.Equal(t, [][]byte{[]byte("alpha")}, fsm.Applied())
}

func TestOpenSingleNodeRestartsFromPersistedLog(t *testing.T) {
	dir := t.TempDir()

	firstFSM := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		StateMachine: firstFSM,
	})
	require.NoError(t, err)

	firstResult, err := engine.Propose(context.Background(), []byte("one"))
	require.NoError(t, err)
	_, err = engine.Propose(context.Background(), []byte("two"))
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	secondFSM := &testStateMachine{}
	restarted, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		StateMachine: secondFSM,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, restarted.Close())
	})

	require.Equal(t, [][]byte{[]byte("one"), []byte("two")}, secondFSM.Applied())

	readIndex, err := restarted.LinearizableRead(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, readIndex, firstResult.CommitIndex)

	result, err := restarted.Propose(context.Background(), []byte("three"))
	require.NoError(t, err)
	require.Equal(t, "three", result.Response)
	require.Equal(t, [][]byte{[]byte("one"), []byte("two"), []byte("three")}, secondFSM.Applied())
}

func TestOpenInitializesAppliedIndexFromPersistedSnapshot(t *testing.T) {
	dir := t.TempDir()
	state := persistedState{
		HardState: raftpb.HardState{
			Term:   1,
			Commit: 5,
		},
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
				Index:     5,
				Term:      1,
			},
		},
	}
	require.NoError(t, saveMetadataFile(metadataFilePath(dir), state.HardState, state.Snapshot))
	require.NoError(t, writeAndSyncFile(snapshotDataFilePath(dir), mustEncodeSnapshotData(t, nil)))

	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		StateMachine: &testStateMachine{},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.Equal(t, uint64(5), engine.Status().AppliedIndex)
}

func TestApplyReadySnapshotAdvancesAppliedIndex(t *testing.T) {
	engine := &Engine{
		storage: etcdraft.NewMemoryStorage(),
		fsm:     &testStateMachine{},
	}

	err := engine.applyReadySnapshot(raftpb.Snapshot{
		Data: mustEncodeSnapshotData(t, [][]byte{[]byte("snap")}),
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     7,
			Term:      2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), engine.applied)
	fsm, ok := engine.fsm.(*testStateMachine)
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("snap")}, fsm.Applied())
}

func TestOpenRestoresLegacySnapshotState(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, saveStateFile(stateFilePath(dir), persistedState{
		HardState: raftpb.HardState{
			Term:   2,
			Commit: 6,
		},
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
			Term:  2,
			Index: 6,
			Data:  encodeProposalEnvelope(1, []byte("tail")),
		}},
	}))

	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.Equal(t, [][]byte{[]byte("snap"), []byte("tail")}, fsm.Applied())
}

func mustEncodeSnapshotData(t *testing.T, applied [][]byte) []byte {
	t.Helper()

	fsm := &testStateMachine{applied: applied}
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, snapshot.Close())
	})

	var buf bytes.Buffer
	_, err = snapshot.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}
