package etcd

import (
	"bytes"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type shutdownLeaseCloseGate struct {
	etcdstorage.Storage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *shutdownLeaseCloseGate) Close() error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil
}

func TestEngineShutdownRevokesLeaseBeforeStorageClose(t *testing.T) {
	for _, method := range []string{"shutdown", "fail"} {
		for _, anchor := range []string{"single", "quorum"} {
			t.Run(method+"/"+anchor, func(t *testing.T) {
				testEngineShutdownRevokesLease(t, method == "fail", anchor == "single")
			})
		}
	}
}

func testEngineShutdownRevokesLease(t *testing.T, fatal, single bool) {
	t.Helper()
	gate := &shutdownLeaseCloseGate{entered: make(chan struct{}), release: make(chan struct{})}
	e := &Engine{
		persist: gate, closeCh: make(chan struct{}),
		status: raftengine.Status{State: raftengine.StateLeader},
	}
	e.isLeader.Store(true)
	if single {
		e.singleNodeLeaderAckMonoNs.Store(monoclock.Now().Nanos())
	} else {
		e.ackTracker.recordAck(2, 1)
	}
	require.False(t, e.LastQuorumAck().IsZero(), "control must start with a real lease anchor")
	var callbacks atomic.Int32
	e.RegisterLeaderLossCallback(func() { callbacks.Add(1) })
	failure := errors.New("injected engine shutdown")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if fatal {
			e.fail(failure)
		} else {
			e.shutdown()
		}
	}()
	t.Cleanup(func() {
		close(gate.release)
		testutil.Closed(t, done, time.Second, "shutdown did not join after storage Close returned")
	})
	testutil.Closed(t, gate.entered, time.Second, "shutdown did not reach storage Close")
	require.Equal(t, raftengine.StateShutdown, e.State())
	assert.True(t, e.LastQuorumAck().IsZero(), "shutdown must not retain a leader liveness anchor while Close is blocked")
	assert.Equal(t, int32(1), callbacks.Load(), "lease invalidation must precede blocking resource teardown")
	select {
	case <-done:
		t.Fatal("shutdown completed before storage Close returned")
	default:
	}
	if fatal {
		require.ErrorIs(t, e.Err(), failure)
	}
}

func TestEnginePublishShutdownRevokesAuthorityOnce(t *testing.T) {
	for _, state := range []raftengine.State{raftengine.StateLeader, raftengine.StateFollower, raftengine.StateCandidate} {
		t.Run(string(state), func(t *testing.T) {
			failure := errors.New("first shutdown failure")
			e := &Engine{status: raftengine.Status{State: state}}
			e.isLeader.Store(state == raftengine.StateLeader)
			e.singleNodeLeaderAckMonoNs.Store(monoclock.Now().Nanos())
			e.ackTracker.recordAck(2, 1)
			callbacks := 0
			e.RegisterLeaderLossCallback(func() {
				callbacks++
				assert.Equal(t, raftengine.StateShutdown, e.State())
				assert.True(t, e.LastQuorumAck().IsZero())
			})
			require.True(t, e.publishShutdown(failure))
			require.False(t, e.publishShutdown(errors.New("later shutdown failure")))
			require.ErrorIs(t, e.Err(), failure)
			require.Equal(t, raftengine.StateShutdown, e.State())
			require.False(t, e.isLeader.Load())
			require.Zero(t, e.singleNodeLeaderAckMonoNs.Load())
			require.True(t, e.ackTracker.load().IsZero())
			if state == raftengine.StateLeader {
				require.Equal(t, 1, callbacks)
			} else {
				require.Zero(t, callbacks)
			}
		})
	}
}

type shutdownSnapshotSaveGate struct {
	recordingPersistStorage
	entered chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func (s *shutdownSnapshotSaveGate) SaveSnap(_ *raftpb.Snapshot) error {
	close(s.entered)
	<-s.release
	return nil
}

func (s *shutdownSnapshotSaveGate) Close() error {
	s.closed.Store(true)
	return nil
}

func TestEngineShutdownRevokesLeaseBeforeSnapshotWorkerJoin(t *testing.T) {
	gate := &shutdownSnapshotSaveGate{entered: make(chan struct{}), release: make(chan struct{})}
	e := localSnapshotEngine(t, nil, &recordingAppliedIndexFSM{}, 2)
	e.persist = gate
	e.closeCh = make(chan struct{})
	e.snapshotReqCh = make(chan snapshotRequest, 1)
	e.snapshotResCh = make(chan snapshotResult, 1)
	e.snapshotStopCh = make(chan struct{})
	e.status.State = raftengine.StateLeader
	e.isLeader.Store(true)
	e.singleNodeLeaderAckMonoNs.Store(monoclock.Now().Nanos())
	var callbacks atomic.Int32
	e.RegisterLeaderLossCallback(func() { callbacks.Add(1) })
	e.startSnapshotWorker()
	done := make(chan struct{})
	var stopOnce, releaseOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			go func() {
				defer close(done)
				e.shutdown()
			}()
		})
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(gate.release) })
		stop()
		testutil.Closed(t, done, time.Second, "shutdown did not join the released snapshot worker")
	})
	e.snapshotReqCh <- snapshotRequest{index: 2, snapshot: &testSnapshot{data: []byte("held snapshot")}}
	testutil.Closed(t, gate.entered, time.Second, "real snapshot worker did not reach SaveSnap")
	stop()
	testutil.Closed(t, e.snapshotStopCh, time.Second, "shutdown did not start snapshot worker quiescence")
	require.Equal(t, raftengine.StateShutdown, e.State())
	require.True(t, e.LastQuorumAck().IsZero())
	require.Equal(t, int32(1), callbacks.Load())
	require.False(t, gate.closed.Load(), "storage must remain open while the snapshot worker is saving")
	select {
	case <-done:
		t.Fatal("shutdown completed before SaveSnap returned")
	default:
	}
	releaseOnce.Do(func() { close(gate.release) })
	testutil.Closed(t, done, time.Second, "shutdown did not join after SaveSnap returned")
	require.True(t, gate.closed.Load())
	require.Equal(t, int32(1), callbacks.Load())
}

type shutdownLogGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *shutdownLogGate) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("etcd raft engine shutting down due to error")) {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return len(data), nil
}

func TestEngineFatalShutdownRevokesLeaseBeforeLogging(t *testing.T) {
	testEngineFatalShutdownRevokesLeaseBeforeLogging(t)
}

func testEngineFatalShutdownRevokesLeaseBeforeLogging(t *testing.T) {
	t.Helper()
	gate := &shutdownLogGate{entered: make(chan struct{}), release: make(chan struct{})}
	previous := slog.Default()
	output, flags := log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(gate, nil)))
	e := &Engine{status: raftengine.Status{State: raftengine.StateLeader}, closeCh: make(chan struct{})}
	e.isLeader.Store(true)
	e.singleNodeLeaderAckMonoNs.Store(monoclock.Now().Nanos())
	var callbacks atomic.Int32
	e.RegisterLeaderLossCallback(func() { callbacks.Add(1) })
	failure := errors.New("held fatal log")
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.fail(failure)
	}()
	t.Cleanup(func() {
		defer func() {
			slog.SetDefault(previous)
			log.SetOutput(output)
			log.SetFlags(flags)
		}()
		close(gate.release)
		testutil.Closed(t, done, time.Second, "fatal shutdown did not join after releasing logging")
	})
	testutil.Closed(t, gate.entered, time.Second, "fatal shutdown did not reach its error log")
	assert.Equal(t, raftengine.StateShutdown, e.State())
	assert.True(t, e.LastQuorumAck().IsZero())
	assert.Equal(t, int32(1), callbacks.Load())
	assert.ErrorIs(t, e.Err(), failure)
}

func TestEngineFatalShutdownLoggingRestoresStandardLogger(t *testing.T) {
	previous, output, flags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(output)
		log.SetFlags(flags)
	})
	const exerciseFlags = log.Ldate | log.Ltime | log.Lshortfile
	log.SetFlags(exerciseFlags)
	t.Run("held-fatal-log", testEngineFatalShutdownRevokesLeaseBeforeLogging)
	require.Same(t, previous, slog.Default())
	require.Equal(t, output, log.Writer(), "slog replacement must not retain the test log gate")
	require.Equal(t, exerciseFlags, log.Flags(), "nonzero standard log flags must survive slog replacement")
}
