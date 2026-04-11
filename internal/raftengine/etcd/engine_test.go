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

	err := engine.enqueueStep(context.Background(), raftpb.Message{Type: raftpb.MsgApp})
	require.Error(t, err)
	require.True(t, errors.Is(err, errStepQueueFull))
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
	engine := &Engine{
		nodeID:     1,
		transport:  &GRPCTransport{},
		dispatchCh: make(chan dispatchRequest, 1),
	}
	engine.dispatchCh <- dispatchRequest{msg: raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2}}

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
		dispatchCh:     make(chan dispatchRequest, 1),
		dispatchStopCh: make(chan struct{}),
		dispatchCtx:    ctx,
		dispatchCancel: cancel,
	}
	started := make(chan struct{})
	engine.dispatchFn = func(ctx context.Context, _ dispatchRequest) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	engine.startDispatchWorkers()
	engine.dispatchCh <- dispatchRequest{msg: raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2}}

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
