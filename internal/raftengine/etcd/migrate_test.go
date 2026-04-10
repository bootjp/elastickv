package etcd

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type kvFSMAdapter struct {
	fsm raft.FSM
}

func (a kvFSMAdapter) Apply(data []byte) any {
	return a.fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
}

func (a kvFSMAdapter) Snapshot() (Snapshot, error) {
	snapshot, err := a.fsm.Snapshot()
	if err != nil {
		return nil, err
	}
	return hashicorpSnapshotAdapter{snapshot: snapshot}, nil
}

func (a kvFSMAdapter) Restore(r io.Reader) error {
	return a.fsm.Restore(io.NopCloser(r))
}

type hashicorpSnapshotAdapter struct {
	snapshot raft.FSMSnapshot
}

func (a hashicorpSnapshotAdapter) WriteTo(w io.Writer) (int64, error) {
	sink := &snapshotSinkAdapter{writer: w}
	if err := a.snapshot.Persist(sink); err != nil {
		return sink.written, err
	}
	return sink.written, nil
}

func (a hashicorpSnapshotAdapter) Close() error {
	a.snapshot.Release()
	return nil
}

type snapshotSinkAdapter struct {
	writer  io.Writer
	written int64
}

func (s *snapshotSinkAdapter) ID() string    { return "migration" }
func (s *snapshotSinkAdapter) Cancel() error { return nil }
func (s *snapshotSinkAdapter) Close() error  { return nil }
func (s *snapshotSinkAdapter) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.written += int64(n)
	return n, err
}

func TestMigrateFSMStoreSeedsEtcdDataDir(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "fsm.db")
	source, err := store.NewPebbleStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, source.PutAt(context.Background(), []byte("alpha"), []byte("one"), 10, 0))
	require.NoError(t, source.Close())

	destDataDir := filepath.Join(t.TempDir(), "raft")
	peers := []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"}}
	stats, err := MigrateFSMStore(sourcePath, destDataDir, peers)
	require.NoError(t, err)
	require.Positive(t, stats.SnapshotBytes)

	destStore, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "dest-fsm.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, destStore.Close())
	})

	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      destDataDir,
		StateMachine: kvFSMAdapter{fsm: kv.NewKvFSM(destStore)},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	value, err := destStore.GetAt(context.Background(), []byte("alpha"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("one"), value)
}
