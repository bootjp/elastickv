package etcd

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
)

type readinessRestoreFSM struct {
	testStateMachine
	entered chan struct{}
	release chan struct{}
}

func (f *readinessRestoreFSM) Restore(io.Reader) error {
	close(f.entered)
	<-f.release
	return errors.New("restore rejected")
}

func TestReadinessFenceCoversRealSnapshotRestoreFailure(t *testing.T) {
	t.Parallel()
	fsm := &readinessRestoreFSM{entered: make(chan struct{}), release: make(chan struct{})}
	engine := &Engine{storage: etcdraft.NewMemoryStorage(), fsm: fsm, dataDir: t.TempDir(), startedCh: make(chan struct{}), doneCh: make(chan struct{}), peers: map[uint64]Peer{1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"}}}
	release := sync.OnceFunc(func() { close(fsm.release) })
	t.Cleanup(release)
	close(engine.startedCh)
	snapshot := raftTestSnapshot(7, 2, []uint64{1}, mustEncodeSnapshotData(t, [][]byte{[]byte("snapshot")}))
	done := make(chan error, 1)
	go func() { done <- engine.applyReadySnapshot(&snapshot) }()
	select {
	case <-fsm.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("restore was not entered")
	}
	require.True(t, engine.ReadinessState().Restoring)
	release()
	require.Error(t, <-done)
	require.True(t, engine.ReadinessState().Restoring, "failed restore must not reopen readiness before terminal error publication")
}
