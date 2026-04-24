package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
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

type blockingSnapshotStateMachine struct {
	started chan struct{}
	release chan struct{}
}

type blockingSnapshot struct {
	started chan struct{}
	release chan struct{}
}

type countingSnapshotStateMachine struct {
	snapshotCalls atomic.Uint32
}

type transportTestNode struct {
	peer      Peer
	lis       net.Listener
	server    *grpc.Server
	transport *GRPCTransport
	fsm       *testStateMachine
	engine    *Engine
	dir       string
}

func (s *testSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(s.data)
	return int64(n), err
}

func (s *testSnapshot) Close() error {
	return nil
}

func (s *blockingSnapshotStateMachine) Apply(data []byte) any {
	return string(data)
}

func (s *blockingSnapshotStateMachine) Snapshot() (Snapshot, error) {
	return &blockingSnapshot{
		started: s.started,
		release: s.release,
	}, nil
}

func (s *blockingSnapshotStateMachine) Restore(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (s *blockingSnapshot) WriteTo(w io.Writer) (int64, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	<-s.release
	n, err := w.Write([]byte("snap"))
	return int64(n), err
}

func (s *blockingSnapshot) Close() error {
	return nil
}

func (s *countingSnapshotStateMachine) Apply(data []byte) any {
	return string(data)
}

func (s *countingSnapshotStateMachine) Snapshot() (Snapshot, error) {
	s.snapshotCalls.Add(1)
	return &testSnapshot{data: []byte("counting")}, nil
}

func (s *countingSnapshotStateMachine) Restore(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func TestOpenSingleNodeProposeAndReadIndex(t *testing.T) {
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      t.TempDir(),
		Bootstrap:    true,
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
		Bootstrap:    true,
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
		Bootstrap:    true,
		StateMachine: &testStateMachine{},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.GreaterOrEqual(t, engine.Status().AppliedIndex, uint64(5))
}

func TestOpenRestoresPeersFromPersistedMetadata(t *testing.T) {
	dir := t.TempDir()
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		{NodeID: 3, ID: "n3", Address: "127.0.0.1:7003"},
	}

	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		Peers:        peers,
		Bootstrap:    true,
		StateMachine: &testStateMachine{},
	})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	persisted, ok, err := LoadPersistedPeers(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, peers, persisted)

	restarted, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		StateMachine: &testStateMachine{},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, restarted.Close())
	})

	cfg, err := restarted.Configuration(context.Background())
	require.NoError(t, err)
	require.Len(t, cfg.Servers, 3)
}

func TestOpenRestoresPlaceholderPersistedPeerMetadata(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeCurrentPersistedPeers(dir, 7, []Peer{
		{NodeID: 1, ID: "1"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
	}))
	require.NoError(t, saveMetadataFile(metadataFilePath(dir), raftpb.HardState{Term: 2, Commit: 7}, raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
			Index:     7,
			Term:      2,
		},
	}))

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

	cfg, err := engine.Configuration(context.Background())
	require.NoError(t, err)
	require.Equal(t, []raftengine.Server{
		{ID: "n1", Address: "127.0.0.1:7001", Suffrage: "voter"},
		{ID: "n2", Address: "127.0.0.1:7002", Suffrage: "voter"},
	}, cfg.Servers)
}

func TestVerifyLeaderUsesReadIndexPath(t *testing.T) {
	engine := &Engine{
		readCh: make(chan readRequest, 1),
		doneCh: make(chan struct{}),
		status: raftengine.Status{State: raftengine.StateLeader},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.VerifyLeader(context.Background())
	}()

	select {
	case req := <-engine.readCh:
		require.False(t, req.waitApplied)
		req.done <- readResult{index: 9}
	case <-time.After(time.Second):
		t.Fatal("verify leader did not issue read-index request")
	}

	require.NoError(t, <-errCh)
}

func TestCancelPendingRequestsRemoveEntries(t *testing.T) {
	engine := &Engine{
		pendingProposals: map[uint64]proposalRequest{
			1: {id: 1, done: make(chan proposalResult, 1)},
		},
		pendingReads: map[uint64]readRequest{
			2: {id: 2, done: make(chan readResult, 1)},
		},
	}

	engine.cancelPendingProposal(1)
	engine.cancelPendingRead(2)

	require.Empty(t, engine.pendingProposals)
	require.Empty(t, engine.pendingReads)
}

func TestHandleTransportMessageWaitsForStartup(t *testing.T) {
	engine := &Engine{
		startedCh: make(chan struct{}),
		doneCh:    make(chan struct{}),
		stepCh:    make(chan raftpb.Message, 1),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.handleTransportMessage(context.Background(), raftpb.Message{Type: raftpb.MsgHeartbeat})
	}()

	select {
	case <-engine.stepCh:
		t.Fatal("transport message delivered before startup")
	case <-time.After(20 * time.Millisecond):
	}

	close(engine.startedCh)

	select {
	case msg := <-engine.stepCh:
		require.Equal(t, raftpb.MsgHeartbeat, msg.Type)
	case <-time.After(time.Second):
		t.Fatal("transport message was not delivered after startup")
	}

	require.NoError(t, <-errCh)
}

func TestEnqueueStepReturnsQueueFull(t *testing.T) {
	engine := &Engine{
		doneCh: make(chan struct{}),
		stepCh: make(chan raftpb.Message, 1),
	}
	engine.stepCh <- raftpb.Message{Type: raftpb.MsgHeartbeat}

	require.Equal(t, uint64(0), engine.StepQueueFullCount())

	err := engine.enqueueStep(context.Background(), raftpb.Message{Type: raftpb.MsgApp})
	require.Error(t, err)
	require.True(t, errors.Is(err, errStepQueueFull))

	// The Prometheus hot-path dashboard relies on StepQueueFullCount
	// advancing exactly once per rejected enqueue so the scraped rate
	// equals the true drop rate, not a multiple of it.
	require.Equal(t, uint64(1), engine.StepQueueFullCount())

	err = engine.enqueueStep(context.Background(), raftpb.Message{Type: raftpb.MsgApp})
	require.Error(t, err)
	require.Equal(t, uint64(2), engine.StepQueueFullCount())
}

func TestHandleStepIgnoresPeerNotFoundResponses(t *testing.T) {
	engine := &Engine{
		rawNode: mustRawNode(t, etcdraft.NewMemoryStorage(), 1),
	}
	msg := raftpb.Message{
		Type: raftpb.MsgAppResp,
		From: 2,
		To:   1,
	}

	engine.handleStep(msg)

	require.NoError(t, engine.currentError())
}

func TestApplyReadySnapshotAdvancesAppliedIndex(t *testing.T) {
	engine := &Engine{
		storage: etcdraft.NewMemoryStorage(),
		fsm:     &testStateMachine{},
	}

	payload := mustEncodeSnapshotData(t, [][]byte{[]byte("snap")})
	err := engine.applyReadySnapshot(raftpb.Snapshot{
		Data: payload,
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
	snapshot, err := engine.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, payload, snapshot.Data)
}

func TestSendMessagesDoesNotBlockWhenDispatchQueueIsFull(t *testing.T) {
	hbCh := make(chan dispatchRequest, 1)
	hbCh <- dispatchRequest{msg: raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2}}
	engine := &Engine{
		nodeID:    1,
		transport: &GRPCTransport{},
		peerDispatchers: map[uint64]*peerQueues{
			2: {normal: make(chan dispatchRequest, 1), heartbeat: hbCh},
		},
	}

	done := make(chan struct{})
	go func() {
		require.NoError(t, engine.sendMessages([]raftpb.Message{{
			Type: raftpb.MsgHeartbeat,
			To:   2,
		}}))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sendMessages blocked on a full dispatch queue")
	}
}

func TestStopDispatchWorkersCancelsInflightDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	engine := &Engine{
		nodeID: 1,
		peers: map[uint64]Peer{
			2: {NodeID: 2},
		},
		peerDispatchers:  make(map[uint64]*peerQueues),
		perPeerQueueSize: 1,
		dispatchStopCh:   make(chan struct{}),
		dispatchCtx:      ctx,
		dispatchCancel:   cancel,
	}
	started := make(chan struct{})
	engine.dispatchFn = func(ctx context.Context, _ dispatchRequest) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	engine.startDispatchWorkers()
	engine.peerDispatchers[2].heartbeat <- dispatchRequest{msg: raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2}}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatch worker did not start")
	}

	done := make(chan struct{})
	go func() {
		engine.stopDispatchWorkers()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatch workers did not stop after cancellation")
	}
}

func TestUpsertPeerStartsDispatcherAndAcceptsMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dispatched := make(chan raftpb.Message, 1)
	engine := &Engine{
		nodeID:           1,
		peerDispatchers:  make(map[uint64]*peerQueues),
		perPeerQueueSize: 4,
		dispatchStopCh:   make(chan struct{}),
		dispatchCtx:      ctx,
		dispatchCancel:   cancel,
	}
	engine.dispatchFn = func(_ context.Context, req dispatchRequest) error {
		dispatched <- req.msg
		return nil
	}

	engine.upsertPeer(Peer{NodeID: 2, ID: "peer2", Address: "localhost:2"})

	pd, ok := engine.peerDispatchers[2]
	require.True(t, ok, "dispatcher must be created on upsert")
	require.Equal(t, defaultHeartbeatBufPerPeer, cap(pd.heartbeat))
	require.Equal(t, 4, cap(pd.normal))

	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2}))

	select {
	case msg := <-dispatched:
		require.Equal(t, uint64(2), msg.To)
	case <-time.After(time.Second):
		t.Fatal("message was not dispatched to peer 2")
	}

	close(engine.dispatchStopCh)
	engine.dispatchWG.Wait()
}

func TestRemovePeerClosesDispatcherAndDropsSubsequentMessages(t *testing.T) {
	stopCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	pd := &peerQueues{
		normal:    make(chan dispatchRequest, 4),
		heartbeat: make(chan dispatchRequest, 4),
		ctx:       ctx,
		cancel:    cancel,
	}
	engine := &Engine{
		nodeID:          1,
		peers:           map[uint64]Peer{2: {NodeID: 2, ID: "peer2"}},
		peerDispatchers: map[uint64]*peerQueues{2: pd},
		dispatchStopCh:  stopCh,
	}
	engine.dispatchWG.Add(2)
	go engine.runDispatchWorker(ctx, pd.normal)
	go engine.runDispatchWorker(ctx, pd.heartbeat)

	engine.removePeer(2)

	_, ok := engine.peerDispatchers[2]
	require.False(t, ok, "dispatcher must be removed from map")

	workerDone := make(chan struct{})
	go func() {
		engine.dispatchWG.Wait()
		close(workerDone)
	}()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("dispatch workers did not exit after channel close")
	}

	// Subsequent messages to the removed peer must be dropped without panic.
	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2}))
}

func TestMaybePersistLocalSnapshotSkipsSmallAdvance(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))

	persist := mockstorage.NewStorageRecorder("")
	engine := &Engine{
		storage: storage,
		persist: persist,
		fsm:     &testStateMachine{},
		applied: 2,
	}

	require.NoError(t, engine.maybePersistLocalSnapshot())
	require.Empty(t, persist.Action())
}

func TestMaybePersistLocalSnapshotRunsInBackground(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))

	entries := make([]raftpb.Entry, defaultSnapshotEvery)
	for i := range entries {
		index, err := uint32Len(i + 2)
		require.NoError(t, err)
		entries[i] = raftpb.Entry{
			Type:  raftpb.EntryNormal,
			Term:  1,
			Index: uint64(index),
		}
	}
	require.NoError(t, storage.Append(entries))

	persist := mockstorage.NewStorageRecorder("")
	fsm := &blockingSnapshotStateMachine{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	release := sync.OnceFunc(func() {
		close(fsm.release)
	})
	engine := &Engine{
		storage:        storage,
		persist:        persist,
		fsm:            fsm,
		applied:        defaultSnapshotEvery + 1,
		snapshotReqCh:  make(chan snapshotRequest),
		snapshotResCh:  make(chan snapshotResult, 1),
		snapshotStopCh: make(chan struct{}),
	}
	engine.startSnapshotWorker()
	t.Cleanup(func() {
		release()
		engine.stopSnapshotWorker()
	})

	done := make(chan struct{})
	go func() {
		require.NoError(t, engine.maybePersistLocalSnapshot())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("maybePersistLocalSnapshot blocked on snapshot persistence")
	}

	select {
	case <-fsm.started:
	case <-time.After(time.Second):
		t.Fatal("snapshot worker did not start")
	}

	require.Empty(t, persist.Action())
	release()

	select {
	case result := <-engine.snapshotResCh:
		require.NoError(t, engine.handleSnapshotResult(result))
	case <-time.After(time.Second):
		t.Fatal("snapshot worker did not finish")
	}

	actions := persist.Action()
	require.Len(t, actions, 2)
	require.Equal(t, "SaveSnap", actions[0].Name)
	require.Equal(t, "Release", actions[1].Name)
}

func TestMaybePersistLocalSnapshotReturnsWhenWorkerIsStopped(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))

	entries := make([]raftpb.Entry, defaultSnapshotEvery)
	for i := range entries {
		index, err := uint32Len(i + 2)
		require.NoError(t, err)
		entries[i] = raftpb.Entry{
			Type:  raftpb.EntryNormal,
			Term:  1,
			Index: uint64(index),
		}
	}
	require.NoError(t, storage.Append(entries))

	engine := &Engine{
		storage:        storage,
		persist:        mockstorage.NewStorageRecorder(""),
		fsm:            &testStateMachine{},
		applied:        defaultSnapshotEvery + 1,
		snapshotReqCh:  make(chan snapshotRequest),
		snapshotStopCh: make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	close(engine.snapshotStopCh)

	done := make(chan error, 1)
	go func() {
		done <- engine.maybePersistLocalSnapshot()
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, errClosed)
		require.False(t, engine.snapshotInFlight)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("maybePersistLocalSnapshot blocked when snapshot worker was stopped")
	}
}

func TestPersistConfigSnapshotSkipsSerializationWhenNewerSnapshotExists(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Data: []byte("newer"),
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
			Index:     9,
			Term:      2,
		},
	}))

	fsm := &countingSnapshotStateMachine{}
	engine := &Engine{
		storage: storage,
		fsm:     fsm,
	}

	require.NoError(t, engine.persistConfigSnapshot(7, raftpb.ConfState{Voters: []uint64{1}}))
	require.Zero(t, fsm.snapshotCalls.Load())
}

func TestPersistConfigSnapshotDoesNotCompactLogs(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))
	require.NoError(t, storage.Append([]raftpb.Entry{
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	}))

	engine := &Engine{
		storage: storage,
		persist: mockstorage.NewStorageRecorder(""),
		fsm:     &testStateMachine{},
	}

	require.NoError(t, engine.persistConfigSnapshot(4, raftpb.ConfState{Voters: []uint64{1, 2}}))

	firstIndex, err := storage.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), firstIndex)
}

func TestPersistConfigSnapshotWaitsForDurablePublication(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))
	require.NoError(t, storage.Append([]raftpb.Entry{
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	}))

	persist := mockstorage.NewStorageRecorder("")
	fsm := &blockingSnapshotStateMachine{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	release := sync.OnceFunc(func() {
		close(fsm.release)
	})
	engine := &Engine{
		storage: storage,
		persist: persist,
		fsm:     fsm,
		dataDir: t.TempDir(),
	}
	t.Cleanup(release)

	done := make(chan error, 1)
	go func() {
		done <- engine.persistConfigSnapshot(4, raftpb.ConfState{Voters: []uint64{1, 2}})
	}()

	select {
	case <-fsm.started:
	case <-time.After(time.Second):
		t.Fatal("config snapshot serialization did not start")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
		t.Fatal("persistConfigSnapshot returned before snapshot serialization completed")
	case <-time.After(100 * time.Millisecond):
	}

	require.Empty(t, persist.Action())
	release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("persistConfigSnapshot did not finish after snapshot serialization was released")
	}

	actions := persist.Action()
	require.Len(t, actions, 2)
	require.Equal(t, "SaveSnap", actions[0].Name)
	require.Equal(t, "Release", actions[1].Name)

	snapshot, err := storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshot.Metadata.Index)
	require.Equal(t, []uint64{1, 2}, snapshot.Metadata.ConfState.Voters)
}

func TestConfigSnapshotUpToDateUsesCachedIndexFastPath(t *testing.T) {
	engine := &Engine{}
	engine.configIndex.Store(9)

	upToDate, err := engine.configSnapshotUpToDate(7, raftpb.ConfState{Voters: []uint64{1}})
	require.NoError(t, err)
	require.True(t, upToDate)
}

func TestPersistConfigStateUpdatesCachedIndex(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))
	require.NoError(t, storage.Append([]raftpb.Entry{
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	}))

	engine := &Engine{
		storage: storage,
		persist: mockstorage.NewStorageRecorder(""),
		fsm:     &testStateMachine{},
		dataDir: t.TempDir(),
	}

	require.NoError(t, engine.persistConfigState(4, raftpb.ConfState{Voters: []uint64{1, 2}}, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
	}))
	require.Equal(t, uint64(4), engine.currentConfigIndex())
}

func TestPersistConfigStateBlocksConcurrentRestore(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     1,
			Term:      1,
		},
	}))
	require.NoError(t, storage.Append([]raftpb.Entry{
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	}))

	fsm := &blockingSnapshotStateMachine{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	release := sync.OnceFunc(func() {
		close(fsm.release)
	})
	engine := &Engine{
		storage: storage,
		persist: mockstorage.NewStorageRecorder(""),
		fsm:     fsm,
		dataDir: t.TempDir(),
	}
	t.Cleanup(release)

	configDone := make(chan error, 1)
	go func() {
		configDone <- engine.persistConfigState(4, raftpb.ConfState{Voters: []uint64{1, 2}}, []Peer{
			{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
			{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		})
	}()

	select {
	case <-fsm.started:
	case <-time.After(time.Second):
		t.Fatal("config snapshot serialization did not start")
	}

	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- engine.applyReadySnapshot(raftpb.Snapshot{
			Data: mustEncodeSnapshotData(t, nil),
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
				Index:     5,
				Term:      1,
			},
		})
	}()

	select {
	case err := <-restoreDone:
		require.NoError(t, err)
		t.Fatal("snapshot restore finished before config publication released snapshotMu")
	case <-time.After(100 * time.Millisecond):
	}

	release()

	select {
	case err := <-configDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("persistConfigState did not finish after snapshot serialization was released")
	}

	select {
	case err := <-restoreDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("snapshot restore did not finish after config publication completed")
	}
}

func TestNextPeersAfterConfigChangeKeepsLearnerMetadata(t *testing.T) {
	engine := &Engine{
		peers: map[uint64]Peer{
			1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		},
	}

	learner := Peer{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"}
	context, err := encodeConfChangeContext(17, learner)
	require.NoError(t, err)
	next := engine.nextPeersAfterConfigChange(
		raftpb.ConfChangeAddLearnerNode,
		learner.NodeID,
		context,
		raftpb.ConfState{
			Voters:   []uint64{1},
			Learners: []uint64{2},
		},
	)

	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
	}, next)
}

func TestNextPeersAfterConfigChangeV2IgnoresMismatchedPeerContext(t *testing.T) {
	engine := &Engine{
		peers: map[uint64]Peer{
			1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		},
	}

	context, err := encodeConfChangeContext(17, Peer{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"})
	require.NoError(t, err)

	next := engine.nextPeersAfterConfigChangeV2(raftpb.ConfChangeV2{
		Context: context,
		Changes: []raftpb.ConfChangeSingle{
			{Type: raftpb.ConfChangeAddNode, NodeID: 2},
			{Type: raftpb.ConfChangeAddNode, NodeID: 3},
		},
	}, raftpb.ConfState{Voters: []uint64{1, 2, 3}})

	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		{NodeID: 3, ID: "3", Address: ""},
	}, next)
}

func TestNextPeersAfterConfigChangeV2PreservesExistingPeerWithoutContext(t *testing.T) {
	engine := &Engine{
		peers: map[uint64]Peer{
			1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
			2: {NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		},
	}

	next := engine.nextPeersAfterConfigChangeV2(raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{Type: raftpb.ConfChangeAddNode, NodeID: 2},
		},
	}, raftpb.ConfState{Voters: []uint64{1, 2}})

	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
	}, next)
}

func TestApplyAddedPeerWithoutContextPreservesExistingMetadata(t *testing.T) {
	engine := &Engine{
		peers: map[uint64]Peer{
			2: {NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		},
		config: raftengine.Configuration{
			Servers: []raftengine.Server{{
				ID:       "n2",
				Address:  "127.0.0.1:7002",
				Suffrage: "voter",
			}},
		},
	}

	engine.applyAddedPeer(2, Peer{}, false)

	require.Equal(t, Peer{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"}, engine.peers[2])
	require.Equal(t, []raftengine.Server{{
		ID:       "n2",
		Address:  "127.0.0.1:7002",
		Suffrage: "voter",
	}}, engine.config.Servers)
}

func TestCloneDispatchMessageDeepCopy(t *testing.T) {
	original := raftpb.Message{
		Type:    raftpb.MsgApp,
		To:      2,
		Context: []byte("ctx"),
		Entries: []raftpb.Entry{{
			Type:  raftpb.EntryNormal,
			Term:  3,
			Index: 4,
			Data:  []byte("entry"),
		}},
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snapshot"),
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{
					Voters: []uint64{1, 2},
				},
				Index: 7,
				Term:  3,
			},
		},
		Responses: []raftpb.Message{{
			Type:    raftpb.MsgHeartbeatResp,
			Context: []byte("resp"),
		}},
	}

	cloned := cloneDispatchMessage(original)

	original.Context[0] = 'X'
	original.Entries[0].Data[0] = 'X'
	original.Snapshot.Data[0] = 'X'
	original.Snapshot.Metadata.ConfState.Voters[0] = 99
	original.Responses[0].Context[0] = 'X'

	require.Equal(t, []byte("ctx"), cloned.Context)
	require.Equal(t, []byte("entry"), cloned.Entries[0].Data)
	require.Equal(t, []byte("snapshot"), cloned.Snapshot.Data)
	require.Equal(t, []uint64{1, 2}, cloned.Snapshot.Metadata.ConfState.Voters)
	require.Equal(t, []byte("resp"), cloned.Responses[0].Context)
}

func TestPrepareDispatchRequestClonesSnapshotPayload(t *testing.T) {
	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snapshot"),
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
				Index:     7,
				Term:      3,
			},
		},
	}

	req := prepareDispatchRequest(msg)
	require.NoError(t, req.Close())

	require.NotNil(t, req.msg.Snapshot)
	require.Equal(t, []byte("snapshot"), req.msg.Snapshot.Data)

	msg.Snapshot.Data[0] = 'X'
	msg.Snapshot.Metadata.ConfState.Voters[0] = 99

	require.Equal(t, []byte("snapshot"), req.msg.Snapshot.Data)
	require.Equal(t, []uint64{1, 2}, req.msg.Snapshot.Metadata.ConfState.Voters)
}

func TestMaxAppliedIndexStartsFromSnapshotIndex(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
			Index:     5,
			Term:      2,
		},
	}))
	require.NoError(t, storage.SetHardState(raftpb.HardState{
		Term:   2,
		Commit: 9,
	}))

	require.Equal(t, uint64(5), maxAppliedIndex(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 5},
	}))
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
		Bootstrap:    true,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.Equal(t, [][]byte{[]byte("snap"), []byte("tail")}, fsm.Applied())
}

func TestOpenMultiNodeReplicatesOverGRPCTransport(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 3)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNodes(ctx, nodes, peers))

	leader := waitForLeaderNode(t, nodes)
	result, err := leader.engine.Propose(context.Background(), []byte("alpha"))
	require.NoError(t, err)
	require.NotZero(t, result.CommitIndex)

	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if got := node.fsm.Applied(); len(got) != 1 || string(got[0]) != "alpha" {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
}

func TestFollowerStatusTracksLeaderContact(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 3)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNodes(ctx, nodes, peers))
	leader := waitForLeaderNode(t, nodes)

	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node == leader {
				continue
			}
			status := node.engine.Status()
			if status.State == raftengine.StateFollower && status.LastContact >= 0 {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
}

func TestAddVoterAllowsJoiningNodeToReplicate(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	index, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	require.Greater(t, index, uint64(0))

	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	// Re-confirm leadership: the config-change commit can briefly lag the
	// engine's cached State(), causing an immediate Propose to return errNotLeader.
	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)

	result, err := leader.engine.Propose(context.Background(), []byte("alpha"))
	require.NoError(t, err)
	require.NotZero(t, result.CommitIndex)

	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) == 1 && string(nodes[1].fsm.Applied()[0]) == "alpha"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestRemoveServerUpdatesConfiguration(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	addIndex, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	removeIndex, err := leader.engine.RemoveServer(ctx, nodes[1].peer.ID, addIndex)
	require.NoError(t, err)
	require.Greater(t, removeIndex, addIndex)

	waitForConfigSize(t, leader.engine, 1)
	require.Eventually(t, func() bool {
		cfg, err := nodes[1].engine.Configuration(context.Background())
		require.NoError(t, err)
		return len(cfg.Servers) == 1 && cfg.Servers[0].ID == nodes[0].peer.ID
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		return nodes[1].engine.State() == raftengine.StateShutdown
	}, 5*time.Second, 20*time.Millisecond)
}

func TestTransferLeadershipToServerMovesLeader(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	_, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	// Re-confirm leadership before proposing. waitForConfigSize only verifies
	// that the config entry is committed; the engine's cached State() (updated
	// by refreshStatus on each drainReady cycle) may briefly lag behind the
	// commit, causing an immediate Propose to return errNotLeader.
	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)

	_, err = leader.engine.Propose(context.Background(), []byte("seed"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) == 1
	}, 5*time.Second, 20*time.Millisecond)

	transferCtx, transferCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer transferCancel()
	require.NoError(t, leader.engine.TransferLeadershipToServer(transferCtx, nodes[1].peer.ID, nodes[1].peer.Address))

	require.Eventually(t, func() bool {
		return nodes[1].engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)

	result, err := nodes[1].engine.Propose(context.Background(), []byte("beta"))
	require.NoError(t, err)
	require.NotZero(t, result.CommitIndex)
	require.Eventually(t, func() bool {
		applied := nodes[0].fsm.Applied()
		return len(applied) == 2 && string(applied[1]) == "beta"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestStorePendingConfigRejectsWhenQueueIsFull(t *testing.T) {
	engine := &Engine{
		pendingConfigs: map[uint64]adminRequest{},
	}

	maxPending := uint64(defaultMaxPendingConfigs)
	for id := uint64(1); id <= maxPending; id++ {
		require.NoError(t, engine.storePendingConfig(adminRequest{id: id}))
	}

	err := engine.storePendingConfig(adminRequest{id: maxPending + 1})
	require.Error(t, err)
	require.True(t, errors.Is(err, errTooManyPendingConfigs))
}

func newTransportTestNodes(t *testing.T, count int) ([]*transportTestNode, []Peer) {
	t.Helper()

	nodes := make([]*transportTestNode, 0, count)
	peers := make([]Peer, 0, count)
	nodeID := uint64(1)
	for i := range count {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)

		peer := Peer{
			NodeID:  nodeID,
			ID:      "n" + strconv.Itoa(i+1),
			Address: lis.Addr().String(),
		}
		peers = append(peers, peer)
		nodes = append(nodes, &transportTestNode{
			peer: peer,
			lis:  lis,
			dir:  t.TempDir(),
			fsm:  &testStateMachine{},
		})
		nodeID++
	}
	return nodes, peers
}

func startTransportTestServers(nodes []*transportTestNode, peers []Peer) {
	for _, node := range nodes {
		node.transport = NewGRPCTransport(peers)
		node.server = grpc.NewServer(internalutil.GRPCServerOptions()...)
		node.transport.Register(node.server)
		go func(server *grpc.Server, lis net.Listener) {
			_ = server.Serve(lis)
		}(node.server, node.lis)
	}
}

func cleanupTransportTestNodes(t *testing.T, nodes []*transportTestNode) {
	t.Helper()
	for _, node := range nodes {
		if node.engine != nil {
			require.NoError(t, node.engine.Close())
		}
		if node.server != nil {
			node.server.Stop()
		}
		if node.lis != nil {
			_ = node.lis.Close()
		}
		if node.transport != nil {
			require.NoError(t, node.transport.Close())
		}
	}
}

func openTransportTestNode(ctx context.Context, node *transportTestNode, peers []Peer, bootstrap bool) error {
	engine, err := Open(ctx, OpenConfig{
		NodeID:       node.peer.NodeID,
		LocalID:      node.peer.ID,
		LocalAddress: node.peer.Address,
		DataDir:      node.dir,
		Peers:        peers,
		Bootstrap:    bootstrap,
		Transport:    node.transport,
		StateMachine: node.fsm,
	})
	if err != nil {
		return err
	}
	node.engine = engine
	return nil
}

func openTransportTestNodes(ctx context.Context, nodes []*transportTestNode, peers []Peer) error {
	var eg errgroup.Group
	for _, node := range nodes {
		eg.Go(func() error {
			return openTransportTestNode(ctx, node, peers, true)
		})
	}
	return eg.Wait()
}

func TestSingleNodeWALRotatesSegments(t *testing.T) {
	oldSegmentSize := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 4 * 1024
	t.Cleanup(func() {
		wal.SegmentSizeBytes = oldSegmentSize
	})

	dir := t.TempDir()
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      dir,
		Bootstrap:    true,
		StateMachine: &testStateMachine{},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	payload := bytes.Repeat([]byte("a"), 1024)
	for i := 0; i < 32; i++ {
		_, err := engine.Propose(context.Background(), payload)
		require.NoError(t, err)
	}

	files, err := filepath.Glob(filepath.Join(dir, walDirName, "*.wal"))
	require.NoError(t, err)
	require.Greater(t, len(files), 1)
}

func waitForLeaderNode(t *testing.T, nodes []*transportTestNode) *transportTestNode {
	t.Helper()
	var leader *transportTestNode
	require.Eventually(t, func() bool {
		leader = nil
		for _, node := range nodes {
			if node.engine == nil {
				return false
			}
			if node.engine.State() == raftengine.StateLeader {
				leader = node
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	return leader
}

func waitForConfigSize(t *testing.T, engine *Engine, size int) {
	t.Helper()
	require.Eventually(t, func() bool {
		cfg, err := engine.Configuration(context.Background())
		require.NoError(t, err)
		return len(cfg.Servers) == size
	}, 5*time.Second, 20*time.Millisecond)
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

func mustRawNode(t *testing.T, storage *etcdraft.MemoryStorage, nodeID uint64) *etcdraft.RawNode {
	t.Helper()
	rawNode, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID:              nodeID,
		ElectionTick:    defaultElectionTick,
		HeartbeatTick:   defaultHeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   defaultMaxSizePerMsg,
		MaxInflightMsgs: defaultMaxInflightMsg,
		CheckQuorum:     true,
		PreVote:         true,
		ReadOnlyOption:  etcdraft.ReadOnlySafe,
	})
	require.NoError(t, err)
	return rawNode
}

// TestErrNotLeaderMatchesRaftEngineSentinel pins the invariant that the
// etcd engine's internal leadership-loss errors are marked against the
// shared raftengine sentinels. The lease-read fast path in package kv
// relies on a single cross-backend errors.Is(err, raftengine.ErrNotLeader)
// check; a future refactor that forgets to mark these errors would
// silently force every read onto the slow LinearizableRead path.
func TestErrNotLeaderMatchesRaftEngineSentinel(t *testing.T) {
	t.Parallel()
	require.True(t, errors.Is(errors.WithStack(errNotLeader), raftengine.ErrNotLeader))
	require.True(t, errors.Is(errors.WithStack(errLeadershipTransferNotLeader), raftengine.ErrNotLeader))
	require.True(t, errors.Is(errors.WithStack(errLeadershipTransferInProgress), raftengine.ErrLeadershipTransferInProgress))
}

// TestSelectDispatchLane_LegacyTwoLane verifies that, when the 4-lane
// dispatcher is disabled (default), messages are routed exactly as before:
// priority control traffic → heartbeat lane, everything else → normal lane.
func TestSelectDispatchLane_LegacyTwoLane(t *testing.T) {
	t.Parallel()
	engine := &Engine{dispatcherLanesEnabled: false}
	pd := &peerQueues{
		normal:    make(chan dispatchRequest, 1),
		heartbeat: make(chan dispatchRequest, 1),
	}

	cases := map[raftpb.MessageType]chan dispatchRequest{
		raftpb.MsgHeartbeat:     pd.heartbeat,
		raftpb.MsgHeartbeatResp: pd.heartbeat,
		raftpb.MsgReadIndex:     pd.heartbeat,
		raftpb.MsgReadIndexResp: pd.heartbeat,
		raftpb.MsgVote:          pd.heartbeat,
		raftpb.MsgVoteResp:      pd.heartbeat,
		raftpb.MsgPreVote:       pd.heartbeat,
		raftpb.MsgPreVoteResp:   pd.heartbeat,
		raftpb.MsgTimeoutNow:    pd.heartbeat,
		raftpb.MsgApp:           pd.normal,
		raftpb.MsgAppResp:       pd.normal,
		raftpb.MsgSnap:          pd.normal,
	}
	for mt, want := range cases {
		got := engine.selectDispatchLane(pd, mt)
		require.Equalf(t, want, got, "legacy mode routing for %s", mt)
	}
}

// TestSelectDispatchLane_FourLane verifies that, when ELASTICKV_RAFT_DISPATCHER_LANES
// is enabled, MsgApp/MsgAppResp goes to the replication lane, MsgSnap goes to
// the snapshot lane, and heartbeats/votes/read-index share the priority lane.
func TestSelectDispatchLane_FourLane(t *testing.T) {
	t.Parallel()
	engine := &Engine{dispatcherLanesEnabled: true}
	pd := &peerQueues{
		heartbeat:   make(chan dispatchRequest, 1),
		replication: make(chan dispatchRequest, 1),
		snapshot:    make(chan dispatchRequest, 1),
		other:       make(chan dispatchRequest, 1),
	}

	cases := map[raftpb.MessageType]chan dispatchRequest{
		raftpb.MsgHeartbeat:     pd.heartbeat,
		raftpb.MsgHeartbeatResp: pd.heartbeat,
		raftpb.MsgVote:          pd.heartbeat,
		raftpb.MsgVoteResp:      pd.heartbeat,
		raftpb.MsgPreVote:       pd.heartbeat,
		raftpb.MsgPreVoteResp:   pd.heartbeat,
		raftpb.MsgReadIndex:     pd.heartbeat,
		raftpb.MsgReadIndexResp: pd.heartbeat,
		raftpb.MsgTimeoutNow:    pd.heartbeat,
		raftpb.MsgApp:           pd.replication,
		raftpb.MsgAppResp:       pd.replication,
		raftpb.MsgSnap:          pd.snapshot,
	}
	for mt, want := range cases {
		got := engine.selectDispatchLane(pd, mt)
		require.Equalf(t, want, got, "4-lane mode routing for %s", mt)
	}
}

// TestSelectDispatchLane_MsgPropReachesDefaultFallback verifies that MsgProp,
// which is unreachable in practice because DisableProposalForwarding=true
// prevents outbound proposals, is routed to the catch-all lane by the default
// fallback if it ever slips through. This guards against a regression that
// would panic or misroute MsgProp inside a raft engine goroutine.
func TestSelectDispatchLane_MsgPropReachesDefaultFallback(t *testing.T) {
	t.Parallel()
	engine := &Engine{nodeID: 1, dispatcherLanesEnabled: true}
	pd := &peerQueues{
		heartbeat:   make(chan dispatchRequest, 1),
		replication: make(chan dispatchRequest, 1),
		snapshot:    make(chan dispatchRequest, 1),
		other:       make(chan dispatchRequest, 1),
	}
	require.NotPanics(t, func() {
		got := engine.selectDispatchLane(pd, raftpb.MsgProp)
		require.Equal(t, pd.other, got)
	})
}

// TestFourLaneDispatcher_SnapshotDoesNotBlockReplication exercises the key
// correctness invariant for the 4-lane layout: a stuck MsgSnap transfer must
// not prevent MsgApp from being dispatched, because they now run on
// independent goroutines.
func TestFourLaneDispatcher_SnapshotDoesNotBlockReplication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	replicationDone := make(chan struct{}, 1)
	snapshotBlocking := make(chan struct{})
	engine := &Engine{
		nodeID:                 1,
		peerDispatchers:        make(map[uint64]*peerQueues),
		perPeerQueueSize:       4,
		dispatcherLanesEnabled: true,
		dispatchStopCh:         make(chan struct{}),
		dispatchCtx:            ctx,
		dispatchCancel:         cancel,
	}
	engine.dispatchFn = func(dctx context.Context, req dispatchRequest) error {
		switch req.msg.Type { //nolint:exhaustive // test only exercises MsgSnap and MsgApp; other types are irrelevant here
		case raftpb.MsgSnap:
			// Block the snapshot lane until the test releases it. The
			// replication lane should keep flowing in the meantime.
			select {
			case <-snapshotBlocking:
			case <-dctx.Done():
			}
		case raftpb.MsgApp:
			replicationDone <- struct{}{}
		default:
			// Other MessageType values are not exercised by this test.
		}
		return nil
	}

	engine.upsertPeer(Peer{NodeID: 2, ID: "peer2", Address: "localhost:2"})
	pd, ok := engine.peerDispatchers[2]
	require.True(t, ok)
	require.NotNil(t, pd.replication)
	require.NotNil(t, pd.snapshot)

	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: raftpb.MsgSnap, To: 2}))
	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: raftpb.MsgApp, To: 2}))

	select {
	case <-replicationDone:
	case <-time.After(time.Second):
		t.Fatal("MsgApp did not dispatch while MsgSnap was stuck — lanes are not independent")
	}

	close(snapshotBlocking)
	close(engine.dispatchStopCh)
	engine.dispatchCancel()
	engine.dispatchWG.Wait()
}

// TestFourLaneDispatcher_RemovePeerClosesAllLanes confirms removePeer closes
// every lane (not just normal/heartbeat) so no worker goroutine leaks under
// the opt-in 4-lane layout.
func TestFourLaneDispatcher_RemovePeerClosesAllLanes(t *testing.T) {
	stopCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	pd := &peerQueues{
		heartbeat:   make(chan dispatchRequest, 4),
		replication: make(chan dispatchRequest, 4),
		snapshot:    make(chan dispatchRequest, 4),
		other:       make(chan dispatchRequest, 4),
		ctx:         ctx,
		cancel:      cancel,
	}
	engine := &Engine{
		nodeID:                 1,
		peers:                  map[uint64]Peer{2: {NodeID: 2, ID: "peer2"}},
		peerDispatchers:        map[uint64]*peerQueues{2: pd},
		dispatchStopCh:         stopCh,
		dispatcherLanesEnabled: true,
	}
	engine.dispatchWG.Add(4)
	go engine.runDispatchWorker(ctx, pd.heartbeat)
	go engine.runDispatchWorker(ctx, pd.replication)
	go engine.runDispatchWorker(ctx, pd.snapshot)
	go engine.runDispatchWorker(ctx, pd.other)

	engine.removePeer(2)

	done := make(chan struct{})
	go func() {
		engine.dispatchWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("4-lane dispatch workers did not exit after peer removal")
	}

	// Subsequent sends to the removed peer must be dropped without panic.
	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: raftpb.MsgApp, To: 2}))
}

// TestDispatcherLanesEnabledFromEnv pins env-var parsing so a regression in
// the feature flag can't silently flip the default.
func TestDispatcherLanesEnabledFromEnv(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"", false},
		{"0", false},
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{"false", false},
		{"yes", false},
	}
	for _, c := range cases {
		t.Setenv(dispatcherLanesEnvVar, c.val)
		require.Equalf(t, c.want, dispatcherLanesEnabledFromEnv(), "env=%q", c.val)
	}
}

// TestMaxInflightMsgFromEnv_Unset pins the "no env var => caller wins"
// contract of maxInflightMsgFromEnv. normalizeLimitConfig relies on the
// second return to decide whether to overwrite the caller-supplied value.
func TestMaxInflightMsgFromEnv_Unset(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "")
	n, ok := maxInflightMsgFromEnv()
	require.False(t, ok)
	require.Equal(t, 0, n)
}

// TestMaxInflightMsgFromEnv_ReadsOverride pins that a valid positive
// integer is parsed and surfaced to the caller verbatim.
func TestMaxInflightMsgFromEnv_ReadsOverride(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "2048")
	n, ok := maxInflightMsgFromEnv()
	require.True(t, ok)
	require.Equal(t, 2048, n)
}

// TestMaxInflightMsgFromEnv_FallsBackOnInvalid pins the safety behaviour:
// a non-numeric, zero, or negative value is refused and the compiled-in
// default is surfaced (ok=true) so that normalizeLimitConfig actually
// applies the default the warning log promises, instead of letting a
// caller-supplied value silently win.
func TestMaxInflightMsgFromEnv_FallsBackOnInvalid(t *testing.T) {
	cases := []string{"not-a-number", "0", "-3"}
	for _, v := range cases {
		t.Setenv(maxInflightMsgEnvVar, v)
		n, ok := maxInflightMsgFromEnv()
		require.Truef(t, ok, "env=%q", v)
		require.Equalf(t, defaultMaxInflightMsg, n, "env=%q", v)
	}
}

// TestMaxSizePerMsgFromEnv_Unset pins the "no env var => caller wins"
// contract, symmetric with maxInflightMsgFromEnv above.
func TestMaxSizePerMsgFromEnv_Unset(t *testing.T) {
	t.Setenv(maxSizePerMsgEnvVar, "")
	n, ok := maxSizePerMsgFromEnv()
	require.False(t, ok)
	require.Equal(t, uint64(0), n)
}

// TestMaxSizePerMsgFromEnv_ReadsOverride pins that a valid byte count
// >= minMaxSizePerMsg is accepted and surfaced to the caller verbatim.
func TestMaxSizePerMsgFromEnv_ReadsOverride(t *testing.T) {
	t.Setenv(maxSizePerMsgEnvVar, "8388608") // 8 MiB
	n, ok := maxSizePerMsgFromEnv()
	require.True(t, ok)
	require.Equal(t, uint64(8388608), n)
}

// TestMaxSizePerMsgFromEnv_FallsBackOnInvalid covers the three failure
// modes: non-numeric, zero, and below-floor. The floor is minMaxSizePerMsg
// (1 KiB) — a smaller cap would make MsgApp batching degenerate. On
// failure the helper returns (defaultMaxSizePerMsg, true) so the caller
// actually applies the compiled-in default the log line promises, rather
// than silently letting a caller-supplied value win.
func TestMaxSizePerMsgFromEnv_FallsBackOnInvalid(t *testing.T) {
	cases := []string{"not-a-number", "0", "512"}
	for _, v := range cases {
		t.Setenv(maxSizePerMsgEnvVar, v)
		n, ok := maxSizePerMsgFromEnv()
		require.Truef(t, ok, "env=%q", v)
		require.Equalf(t, uint64(defaultMaxSizePerMsg), n, "env=%q", v)
	}
}

// TestMaxInflightMsgFromEnv_ClampsAboveCap pins the upper-bound safety
// behaviour: a value above maxMaxInflightMsg is refused (it would
// trigger multi-GB channel allocations at Open() and crash the process
// before the node becomes healthy) and the compiled-in default is
// surfaced with ok=true so normalizeLimitConfig actually applies it.
func TestMaxInflightMsgFromEnv_ClampsAboveCap(t *testing.T) {
	cases := []string{
		strconv.Itoa(maxMaxInflightMsg + 1),
		"100000000", // fat-fingered value from the Codex P2 report
	}
	for _, v := range cases {
		t.Setenv(maxInflightMsgEnvVar, v)
		n, ok := maxInflightMsgFromEnv()
		require.Truef(t, ok, "env=%q", v)
		require.Equalf(t, defaultMaxInflightMsg, n, "env=%q", v)
	}
}

// TestMaxInflightMsgFromEnv_AcceptsAtCap pins the boundary: exactly
// maxMaxInflightMsg must parse through unchanged. Catches an off-by-one
// regression that would silently clamp an operator-tuned value.
func TestMaxInflightMsgFromEnv_AcceptsAtCap(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, strconv.Itoa(maxMaxInflightMsg))
	n, ok := maxInflightMsgFromEnv()
	require.True(t, ok)
	require.Equal(t, maxMaxInflightMsg, n)
}

// TestMaxSizePerMsgFromEnv_ClampsAboveTransportBudget pins that a
// MaxSizePerMsg above the gRPC transport's message-size budget
// (GRPCMaxMessageBytes) is refused. Without this clamp the override
// would make Raft emit MsgApp frames the transport physically cannot
// carry, producing repeated send failures under large batches.
func TestMaxSizePerMsgFromEnv_ClampsAboveTransportBudget(t *testing.T) {
	over := strconv.FormatUint(maxMaxSizePerMsg+1, 10)
	t.Setenv(maxSizePerMsgEnvVar, over)
	n, ok := maxSizePerMsgFromEnv()
	require.True(t, ok)
	require.Equal(t, uint64(defaultMaxSizePerMsg), n)
}

// TestMaxSizePerMsgFromEnv_AcceptsAtCap covers the boundary symmetrically
// with the inflight test above.
func TestMaxSizePerMsgFromEnv_AcceptsAtCap(t *testing.T) {
	t.Setenv(maxSizePerMsgEnvVar, strconv.FormatUint(maxMaxSizePerMsg, 10))
	n, ok := maxSizePerMsgFromEnv()
	require.True(t, ok)
	require.Equal(t, maxMaxSizePerMsg, n)
}

// TestMaxMaxSizePerMsg_ReservesEnvelopeHeadroom pins the invariant that
// the MaxSizePerMsg upper cap is strictly less than GRPCMaxMessageBytes,
// by exactly raftMessageEnvelopeHeadroom bytes. etcd/raft's
// MaxSizePerMsg caps the entries-data size per MsgApp; the full
// serialized raftpb.Message envelope adds Term/Index/From/To plus
// per-entry framing, so a batch that exactly hits MaxSizePerMsg
// serializes to a frame a few KiB larger than MaxSizePerMsg. If the
// cap matched the transport budget exactly, a full-sized batch could
// overflow the transport and fail replication with ResourceExhausted.
// Raising GRPCMaxMessageBytes without updating this cap (or vice
// versa) MUST be a deliberate, paired action.
func TestMaxMaxSizePerMsg_ReservesEnvelopeHeadroom(t *testing.T) {
	require.Equal(t, uint64(internalutil.GRPCMaxMessageBytes)-raftMessageEnvelopeHeadroom, maxMaxSizePerMsg,
		"maxMaxSizePerMsg must be GRPCMaxMessageBytes - raftMessageEnvelopeHeadroom")
	require.Less(t, maxMaxSizePerMsg, uint64(internalutil.GRPCMaxMessageBytes),
		"maxMaxSizePerMsg must leave strict headroom below the transport budget")
}

// TestNormalizeLimitConfig_DefaultsWhenUnset pins the production defaults
// that reach raft.Config when neither the caller nor the operator has
// overridden them: 512 inflight msgs and 2 MiB per msg. The combination
// bounds worst-case per-peer buffered Raft traffic at 1 GiB (512 × 2 MiB);
// see defaultMaxInflightMsg for the memory-footprint rationale.
func TestNormalizeLimitConfig_DefaultsWhenUnset(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "")
	t.Setenv(maxSizePerMsgEnvVar, "")
	got := normalizeLimitConfig(OpenConfig{})
	require.Equal(t, defaultMaxInflightMsg, got.MaxInflightMsg)
	require.Equal(t, uint64(defaultMaxSizePerMsg), got.MaxSizePerMsg)
	require.Equal(t, 512, got.MaxInflightMsg)
	require.Equal(t, uint64(2<<20), got.MaxSizePerMsg)
}

// TestNormalizeLimitConfig_EnvOverridesCaller pins that a valid env var
// takes precedence over any caller-supplied cfg value. This is how an
// operator retunes a running deployment without a rebuild.
func TestNormalizeLimitConfig_EnvOverridesCaller(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "2048")
	t.Setenv(maxSizePerMsgEnvVar, "8388608")
	got := normalizeLimitConfig(OpenConfig{
		MaxInflightMsg: 256,
		MaxSizePerMsg:  1 << 20,
	})
	require.Equal(t, 2048, got.MaxInflightMsg)
	require.Equal(t, uint64(8388608), got.MaxSizePerMsg)
}

// TestNormalizeLimitConfig_InvalidEnvFallsBackToDefault pins that a
// malformed env override does NOT leak through to raft.Config; the
// compiled-in defaults are applied (even when the caller supplied a
// different value) so the operator-visible warning log matches reality.
func TestNormalizeLimitConfig_InvalidEnvFallsBackToDefault(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "not-a-number")
	t.Setenv(maxSizePerMsgEnvVar, "-1")
	got := normalizeLimitConfig(OpenConfig{})
	require.Equal(t, defaultMaxInflightMsg, got.MaxInflightMsg)
	require.Equal(t, uint64(defaultMaxSizePerMsg), got.MaxSizePerMsg)
}

// TestNormalizeLimitConfig_InvalidEnvOverridesCaller pins the fix for
// the "log message is a lie" gemini reviewer finding: when the env var
// is malformed, the helper warns "using default" — so the default MUST
// actually win, even if the caller supplied a non-default value. Prior
// to the fix the caller's 256 would silently survive, contradicting the
// log line.
func TestNormalizeLimitConfig_InvalidEnvOverridesCaller(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "garbage")
	t.Setenv(maxSizePerMsgEnvVar, "also-garbage")
	got := normalizeLimitConfig(OpenConfig{
		MaxInflightMsg: 256,
		MaxSizePerMsg:  1 << 20,
	})
	require.Equal(t, defaultMaxInflightMsg, got.MaxInflightMsg)
	require.Equal(t, uint64(defaultMaxSizePerMsg), got.MaxSizePerMsg)
}

// TestInboundChannelCap verifies the floor/passthrough behaviour of the
// stepCh / dispatchReportCh sizing helper: the resolved MaxInflightMsg
// drives capacity, but never below minInboundChannelCap.
func TestInboundChannelCap(t *testing.T) {
	require.Equal(t, minInboundChannelCap, inboundChannelCap(0))
	require.Equal(t, minInboundChannelCap, inboundChannelCap(1))
	require.Equal(t, minInboundChannelCap, inboundChannelCap(minInboundChannelCap-1))
	require.Equal(t, minInboundChannelCap, inboundChannelCap(minInboundChannelCap))
	require.Equal(t, 1024, inboundChannelCap(1024))
	require.Equal(t, 2048, inboundChannelCap(2048))
}

// TestOpen_InboundChannelsHonourMaxInflightEnv pins the codex P1 fix:
// when ELASTICKV_RAFT_MAX_INFLIGHT_MSGS is raised above the compiled-in
// default, the engine's inbound stepCh and dispatchReportCh must be
// sized from the override, not the default. Previously these were hard-
// wired to defaultMaxInflightMsg, so a larger override would still hit
// errStepQueueFull at the compiled-in cap, silently defeating the whole
// tuning knob.
func TestOpen_InboundChannelsHonourMaxInflightEnv(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "2048")
	// Open() normalizes BOTH raft limit env vars; leaving the size var
	// unset here would let an ambient ELASTICKV_RAFT_MAX_SIZE_PER_MSG
	// in the shell the test runs in influence config resolution and
	// confuse an unrelated failure diagnosis. Pin it to "" so the size
	// path always falls through to the caller's OpenConfig value.
	t.Setenv(maxSizePerMsgEnvVar, "")
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      t.TempDir(),
		Bootstrap:    true,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})
	require.Equal(t, 2048, cap(engine.stepCh),
		"stepCh capacity must reflect the env-overridden MaxInflightMsg")
	require.Equal(t, 2048, cap(engine.dispatchReportCh),
		"dispatchReportCh capacity must reflect the env-overridden MaxInflightMsg")
}

// TestOpen_InboundChannelsDefaultCap pins that with no env override the
// inbound channels are sized from the compiled-in default (512), the
// current production value.
func TestOpen_InboundChannelsDefaultCap(t *testing.T) {
	t.Setenv(maxInflightMsgEnvVar, "")
	// See TestOpen_InboundChannelsHonourMaxInflightEnv for why the size
	// env var is cleared here too.
	t.Setenv(maxSizePerMsgEnvVar, "")
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      t.TempDir(),
		Bootstrap:    true,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})
	require.Equal(t, defaultMaxInflightMsg, cap(engine.stepCh))
	require.Equal(t, defaultMaxInflightMsg, cap(engine.dispatchReportCh))
}
