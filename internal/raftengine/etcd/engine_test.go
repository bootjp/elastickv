package etcd

import (
	"context"
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
	require.NoError(t, saveStateFile(stateFilePath(dir), state))

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
	}

	err := engine.applyReadySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     7,
			Term:      2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), engine.applied)
}
