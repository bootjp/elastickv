package etcd

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/runtimehealth"
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

func TestReadinessObservationCannotAdvanceWhileApplyIsBlocked(t *testing.T) {
	t.Parallel()
	fsm := &blockingApplyStateMachine{started: make(chan struct{}), release: make(chan struct{})}
	engine, err := Open(t.Context(), OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7001", DataDir: t.TempDir(), Bootstrap: true, StateMachine: fsm})
	require.NoError(t, err)
	release := sync.OnceFunc(func() { close(fsm.release) })
	t.Cleanup(func() { release(); require.NoError(t, engine.Close()) })
	probe := runtimehealth.New("n1")
	probe.SetSources([]runtimehealth.Source{{GroupID: 1, Engine: func() runtimehealth.Engine { return engine }}})
	probe.MarkServing()
	require.True(t, probe.Evaluate(t.Context()).Ready)
	proposed := make(chan error, 1)
	go func() { _, proposeErr := engine.Propose(t.Context(), []byte("blocked")); proposed <- proposeErr }()
	select {
	case <-fsm.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Apply was not entered")
	}
	// Status publication is frozen while Apply is blocked, whether or not the
	// previously published status already included this committed backlog.
	observed := engine.ReadinessObservedAt()
	require.False(t, observed.IsZero())
	// Advance the observation's age without sleeping through the production deadline.
	engine.statusObservedMonoNs.Store(monoclock.Now().Add(-runtimehealth.DefaultStallTimeout - time.Second).Nanos())
	require.Equal(t, "observation_stale", probe.Evaluate(t.Context()).Reason)
	release()
	require.NoError(t, <-proposed)
}
