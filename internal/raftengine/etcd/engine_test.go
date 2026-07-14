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

type blockingApplyStateMachine struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

type blockingSnapshot struct {
	started chan struct{}
	release chan struct{}
}

type countingSnapshotStateMachine struct {
	snapshotCalls atomic.Uint32
}

type transportTestNode struct {
	peer          Peer
	lis           net.Listener
	server        *grpc.Server
	transport     *GRPCTransport
	fsm           *testStateMachine
	engine        *Engine
	dir           string
	joinAsLearner bool
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

func (s *blockingApplyStateMachine) Apply(data []byte) any {
	s.startOnce.Do(func() { close(s.started) })
	<-s.release
	return string(data)
}

func (s *blockingApplyStateMachine) Snapshot() (Snapshot, error) {
	return &testSnapshot{}, nil
}

func (s *blockingApplyStateMachine) Restore(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
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

// TestProposeAdmin_EquivalentToProposeToday pins the Stage 6E-2b
// invariant: ProposeAdmin and Propose commit equivalent entries on
// the current build. Both reach the FSM as the same byte sequence
// and report a non-zero CommitIndex.
//
// This is the bridge property the migration relies on. Stage 6E-2d
// will diverge the two paths — Propose gains a §7.1 quiescence
// barrier check that rejects with ErrEnvelopeCutoverInProgress
// while a cutover is being installed; ProposeAdmin remains
// admissible so the cutover entry and ConfChange-time
// RegisterEncryptionWriter proposals can still land. Until that
// behaviour change ships, callers that switched to ProposeAdmin
// in this PR must see identical results to staying on Propose,
// else the migration would visibly change semantics before
// 6E-2d's protective barrier is in place.
func TestProposeAdmin_EquivalentToProposeToday(t *testing.T) {
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7011",
		DataDir:      t.TempDir(),
		Bootstrap:    true,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.Equal(t, raftengine.StateLeader, engine.State())

	plainRes, err := engine.Propose(context.Background(), []byte("plain"))
	require.NoError(t, err)
	require.NotNil(t, plainRes)
	require.NotZero(t, plainRes.CommitIndex)
	require.Equal(t, "plain", plainRes.Response)

	adminRes, err := engine.ProposeAdmin(context.Background(), []byte("admin"))
	require.NoError(t, err)
	require.NotNil(t, adminRes)
	require.NotZero(t, adminRes.CommitIndex)
	require.Equal(t, "admin", adminRes.Response)
	require.Greater(t, adminRes.CommitIndex, plainRes.CommitIndex,
		"ProposeAdmin must advance CommitIndex past the preceding Propose, "+
			"proving its proposal reaches the engine through the same Raft path")

	applied := fsm.Applied()
	require.Equal(t, [][]byte{[]byte("plain"), []byte("admin")}, applied,
		"both Propose and ProposeAdmin must reach the FSM verbatim and in-order")
}

// Compile-time guard: *Engine must satisfy raftengine.Proposer
// (which Stage 6E-2b extended with ProposeAdmin). Declared at
// package scope so the check fires at test-package compile time
// regardless of which test is selected; wrapping it inside a
// no-op test function would defer the check to a test target
// nobody runs. Removing ProposeAdmin from the interface would
// either leave this assertion dangling (if Engine still has the
// method) or hard-fail the build (if it doesn't) — the latter is
// the failure mode this guard exists to produce.
var _ raftengine.Proposer = (*Engine)(nil)

func TestInboundQueueCapacityScalesSmallInflight(t *testing.T) {
	tests := []struct {
		name        string
		peerCount   int
		maxInflight int
		want        int
	}{
		{
			name:        "single peer small inflight uses floor",
			peerCount:   1,
			maxInflight: 17,
			want:        minInboundQueueCapacity,
		},
		{
			name:        "multi peer default is memory bounded",
			peerCount:   5,
			maxInflight: 256,
			want:        defaultMaxInflightMsg,
		},
		{
			name:        "production inflight stays at default cap",
			peerCount:   5,
			maxInflight: defaultMaxInflightMsg,
			want:        defaultMaxInflightMsg,
		},
		{
			name:        "explicit large inflight is preserved",
			peerCount:   5,
			maxInflight: 2048,
			want:        2048,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, inboundQueueCapacity(tc.peerCount, tc.maxInflight))
		})
	}
}

func TestOpenConfigMaxInflightSizesInboundQueues(t *testing.T) {
	fsm := &testStateMachine{}
	const maxInflight = 17

	engine, err := Open(context.Background(), OpenConfig{
		NodeID:         1,
		LocalID:        "n1",
		LocalAddress:   "127.0.0.1:7011",
		DataDir:        t.TempDir(),
		Bootstrap:      true,
		StateMachine:   fsm,
		MaxInflightMsg: maxInflight,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	require.Equal(t, minInboundQueueCapacity, cap(engine.stepCh))
	require.Equal(t, minInboundQueueCapacity, cap(engine.dispatchReportCh))
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
		HardState: testHardState(1, 5),
		Snapshot:  raftTestSnapshot(5, 1, []uint64{1}, nil),
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
	expected := append([]Peer(nil), peers...)
	for i := range expected {
		expected[i].Suffrage = SuffrageVoter
	}
	require.Equal(t, expected, persisted)

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
	require.NoError(t, saveMetadataFile(metadataFilePath(dir), testHardState(2, 7), raftTestSnapshot(7, 2, []uint64{1, 2}, nil)))

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
		errCh <- engine.handleTransportMessage(context.Background(), raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat)})
	}()

	select {
	case <-engine.stepCh:
		t.Fatal("transport message delivered before startup")
	case <-time.After(20 * time.Millisecond):
	}

	close(engine.startedCh)

	select {
	case msg := <-engine.stepCh:
		require.Equal(t, raftpb.MsgHeartbeat, msg.GetType())
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
	engine.stepCh <- raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat)}

	require.Equal(t, uint64(0), engine.StepQueueFullCount())

	err := engine.enqueueStep(context.Background(), raftpb.Message{Type: messageTypePtr(raftpb.MsgApp)})
	require.Error(t, err)
	require.True(t, errors.Is(err, errStepQueueFull))

	// The Prometheus hot-path dashboard relies on StepQueueFullCount
	// advancing exactly once per rejected enqueue so the scraped rate
	// equals the true drop rate, not a multiple of it.
	require.Equal(t, uint64(1), engine.StepQueueFullCount())

	err = engine.enqueueStep(context.Background(), raftpb.Message{Type: messageTypePtr(raftpb.MsgApp)})
	require.Error(t, err)
	require.Equal(t, uint64(2), engine.StepQueueFullCount())
}

func TestEnqueueStepPriorityBypassesFullBulkQueue(t *testing.T) {
	engine := &Engine{
		doneCh:         make(chan struct{}),
		stepCh:         make(chan raftpb.Message, 1),
		priorityStepCh: make(chan raftpb.Message, 1),
	}
	engine.stepCh <- raftpb.Message{Type: messageTypePtr(raftpb.MsgApp)}

	err := engine.enqueueStep(context.Background(), raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat)})
	require.NoError(t, err)
	require.Equal(t, uint64(0), engine.StepQueueFullCount())

	select {
	case msg := <-engine.priorityStepCh:
		require.Equal(t, raftpb.MsgHeartbeat, msg.GetType())
	default:
		t.Fatal("priority heartbeat was not enqueued")
	}

	err = engine.enqueueStep(context.Background(), raftpb.Message{Type: messageTypePtr(raftpb.MsgApp)})
	require.Error(t, err)
	require.True(t, errors.Is(err, errStepQueueFull))
	require.Equal(t, uint64(1), engine.StepQueueFullCount())
}

func TestHandleEventDrainsPriorityStepBeforeBulkStep(t *testing.T) {
	engine := &Engine{
		closeCh:          make(chan struct{}),
		proposeCh:        make(chan proposalRequest),
		readCh:           make(chan readRequest),
		adminCh:          make(chan adminRequest),
		stepCh:           make(chan raftpb.Message, 1),
		priorityStepCh:   make(chan raftpb.Message, 1),
		dispatchReportCh: make(chan dispatchReport),
		snapshotResCh:    make(chan snapshotResult),
	}
	engine.stepCh <- raftpb.Message{Type: messageTypePtr(raftpb.MsgApp)}
	engine.priorityStepCh <- raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat)}

	handled, err := engine.handleEvent(make(chan time.Time))
	require.NoError(t, err)
	require.True(t, handled)

	require.Empty(t, engine.priorityStepCh)
	require.Len(t, engine.stepCh, 1)
	msg := <-engine.stepCh
	require.Equal(t, raftpb.MsgApp, msg.GetType())
}

func TestTryHandlePriorityStepYieldsAfterBurstLimit(t *testing.T) {
	engine := &Engine{
		priorityStepCh:    make(chan raftpb.Message, 1),
		priorityStepBurst: priorityStepBurstLimit,
	}
	engine.priorityStepCh <- raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat)}

	require.False(t, engine.tryHandlePriorityStep())
	require.Zero(t, engine.priorityStepBurst)
	require.Len(t, engine.priorityStepCh, 1)

	require.True(t, engine.tryHandlePriorityStep())
	require.Equal(t, 1, engine.priorityStepBurst)
	require.Empty(t, engine.priorityStepCh)
}

func TestHandleStepIgnoresPeerNotFoundResponses(t *testing.T) {
	engine := &Engine{
		rawNode: mustRawNode(t, etcdraft.NewMemoryStorage(), 1),
	}
	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgAppResp),
		From: uint64Ptr(2),
		To:   uint64Ptr(1),
	}

	engine.handleStep(msg)

	require.NoError(t, engine.currentError())
}

func TestHandleStepUnprotectsSnapshotTokenWhenCommittedAlreadyCoversIt(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	snap := raftTestSnapshot(10, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	engine := &Engine{
		rawNode: mustRawNode(t, storage, 1),
		protectedReceivedFSMSnaps: map[uint64]int{
			9: 1,
		},
	}
	require.False(t, engine.rawNode.HasReady())

	engine.handleStep(raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(2),
		To:   uint64Ptr(1),
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(9, 0),
			Metadata: testSnapshotMetadata(9, 1, []uint64{1}),
		},
	})

	require.NoError(t, engine.currentError())
	require.Empty(t, engine.protectedReceivedFSMSnaps)
}

func TestHandleStepKeepsAcceptedSnapshotTokenProtectedUntilReady(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	snap := raftTestSnapshot(10, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	engine := &Engine{
		rawNode: mustRawNode(t, storage, 1),
		protectedReceivedFSMSnaps: map[uint64]int{
			11: 1,
		},
	}
	require.False(t, engine.rawNode.HasReady())

	engine.handleStep(raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(2),
		To:   uint64Ptr(1),
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(11, 0),
			Metadata: testSnapshotMetadata(11, 2, []uint64{1}),
		},
	})

	require.NoError(t, engine.currentError())
	require.True(t, engine.rawNode.HasReady())
	require.Equal(t, map[uint64]int{11: 1}, engine.protectedReceivedFSMSnaps)
}

func TestApplyReadySnapshotAdvancesAppliedIndex(t *testing.T) {
	engine := &Engine{
		storage: etcdraft.NewMemoryStorage(),
		fsm:     &testStateMachine{},
	}

	payload := mustEncodeSnapshotData(t, [][]byte{[]byte("snap")})
	snap := raftTestSnapshot(7, 2, []uint64{1}, payload)
	err := engine.applyReadySnapshot(&snap)
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
	hbCh <- dispatchRequest{msg: raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), To: uint64Ptr(2)}}
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
			Type: messageTypePtr(raftpb.MsgHeartbeat),
			To:   uint64Ptr(2),
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
	engine.peerDispatchers[2].heartbeat <- dispatchRequest{msg: raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), To: uint64Ptr(2)}}

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
	require.Equal(t, defaultHeartbeatRespBufPerPeer, cap(pd.heartbeatResp))
	require.Equal(t, 4, cap(pd.normal))

	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), To: uint64Ptr(2)}))

	select {
	case msg := <-dispatched:
		require.Equal(t, uint64(2), msg.GetTo())
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
		normal:        make(chan dispatchRequest, 4),
		heartbeat:     make(chan dispatchRequest, 4),
		heartbeatResp: make(chan dispatchRequest, 4),
		ctx:           ctx,
		cancel:        cancel,
	}
	engine := &Engine{
		nodeID:          1,
		peers:           map[uint64]Peer{2: {NodeID: 2, ID: "peer2"}},
		peerDispatchers: map[uint64]*peerQueues{2: pd},
		dispatchStopCh:  stopCh,
	}
	engine.dispatchWG.Add(3)
	go engine.runDispatchWorker(ctx, pd.normal)
	go engine.runDispatchWorker(ctx, pd.heartbeat)
	go engine.runDispatchWorker(ctx, pd.heartbeatResp)

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
	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), To: uint64Ptr(2)}))
}

func TestMaybePersistLocalSnapshotSkipsSmallAdvance(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))

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
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))

	entries := make([]raftpb.Entry, defaultSnapshotEvery)
	for i := range entries {
		index, err := uint32Len(i + 2)
		require.NoError(t, err)
		entries[i] = testEntry(uint64(index), 1, nil)
	}
	require.NoError(t, storage.Append(entryPointers(entries)))

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
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))

	entries := make([]raftpb.Entry, defaultSnapshotEvery)
	for i := range entries {
		index, err := uint32Len(i + 2)
		require.NoError(t, err)
		entries[i] = testEntry(uint64(index), 1, nil)
	}
	require.NoError(t, storage.Append(entryPointers(entries)))

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
	snap := raftTestSnapshot(9, 2, []uint64{1, 2}, []byte("newer"))
	require.NoError(t, storage.ApplySnapshot(&snap))

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
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	require.NoError(t, storage.Append(entryPointers([]raftpb.Entry{
		testEntry(2, 1, nil),
		testEntry(3, 1, nil),
		testEntry(4, 1, nil),
	})))

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
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	require.NoError(t, storage.Append(entryPointers([]raftpb.Entry{
		testEntry(2, 1, nil),
		testEntry(3, 1, nil),
		testEntry(4, 1, nil),
	})))

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
	require.Equal(t, uint64(4), snapshot.GetMetadata().GetIndex())
	require.Equal(t, []uint64{1, 2}, snapshot.GetMetadata().GetConfState().GetVoters())
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
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	require.NoError(t, storage.Append(entryPointers([]raftpb.Entry{
		testEntry(2, 1, nil),
		testEntry(3, 1, nil),
		testEntry(4, 1, nil),
	})))

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
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	require.NoError(t, storage.Append(entryPointers([]raftpb.Entry{
		testEntry(2, 1, nil),
		testEntry(3, 1, nil),
		testEntry(4, 1, nil),
	})))

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
		restoreSnap := raftTestSnapshot(5, 1, []uint64{1, 2}, mustEncodeSnapshotData(t, nil))
		restoreDone <- engine.applyReadySnapshot(&restoreSnap)
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

	// nextPeersAfterConfigChange now annotates Suffrage from the
	// post-change ConfState so persistConfigState can write a v2
	// peers file that round-trips correctly across restarts. See
	// learner design doc §4.3.
	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageLearner},
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
		Changes: []*raftpb.ConfChangeSingle{
			{Type: confChangeTypePtr(raftpb.ConfChangeAddNode), NodeId: uint64Ptr(2)},
			{Type: confChangeTypePtr(raftpb.ConfChangeAddNode), NodeId: uint64Ptr(3)},
		},
	}, raftpb.ConfState{Voters: []uint64{1, 2, 3}})

	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
		{NodeID: 3, ID: "3", Address: "", Suffrage: SuffrageVoter},
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
		Changes: []*raftpb.ConfChangeSingle{
			{Type: confChangeTypePtr(raftpb.ConfChangeAddNode), NodeId: uint64Ptr(2)},
		},
	}, raftpb.ConfState{Voters: []uint64{1, 2}})

	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
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
		Type:    messageTypePtr(raftpb.MsgApp),
		To:      uint64Ptr(2),
		Context: []byte("ctx"),
		Entries: entryPointers([]raftpb.Entry{testEntry(4, 3, []byte("entry"))}),
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snapshot"),
			Metadata: testSnapshotMetadata(7, 3, []uint64{1, 2}),
		},
		Responses: testMessagePointers([]raftpb.Message{{
			Type:    messageTypePtr(raftpb.MsgHeartbeatResp),
			Context: []byte("resp"),
		}}),
	}

	cloned := cloneDispatchMessage(original)

	original.Context[0] = 'X'
	original.Entries[0].Data[0] = 'X'
	original.Snapshot.Data[0] = 'X'
	original.Snapshot.GetMetadata().GetConfState().Voters[0] = 99
	original.Responses[0].Context[0] = 'X'

	require.Equal(t, []byte("ctx"), cloned.Context)
	require.Equal(t, []byte("entry"), cloned.Entries[0].Data)
	require.Equal(t, []byte("snapshot"), cloned.Snapshot.Data)
	require.Equal(t, []uint64{1, 2}, cloned.Snapshot.GetMetadata().GetConfState().GetVoters())
	require.Equal(t, []byte("resp"), cloned.Responses[0].Context)
}

func TestPrepareDispatchRequestClonesSnapshotPayload(t *testing.T) {
	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snapshot"),
			Metadata: testSnapshotMetadata(7, 3, []uint64{1, 2}),
		},
	}

	req := prepareDispatchRequest(msg)
	require.NoError(t, req.Close())

	require.NotNil(t, req.msg.Snapshot)
	require.Equal(t, []byte("snapshot"), req.msg.Snapshot.Data)

	msg.Snapshot.Data[0] = 'X'
	msg.Snapshot.GetMetadata().GetConfState().Voters[0] = 99

	require.Equal(t, []byte("snapshot"), req.msg.Snapshot.Data)
	require.Equal(t, []uint64{1, 2}, req.msg.Snapshot.GetMetadata().GetConfState().GetVoters())
}

func TestEnqueueDispatchMessageCoalescesPlainHeartbeatResponses(t *testing.T) {
	t.Parallel()
	pd := &peerQueues{
		heartbeat:     make(chan dispatchRequest, 1),
		heartbeatResp: make(chan dispatchRequest, 1),
	}
	engine := &Engine{
		nodeID: 1,
		peerDispatchers: map[uint64]*peerQueues{
			2: pd,
		},
	}
	pd.heartbeatResp <- prepareDispatchRequest(raftpb.Message{
		Type: messageTypePtr(raftpb.MsgHeartbeatResp),
		To:   uint64Ptr(2),
	})

	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{
		Type:    messageTypePtr(raftpb.MsgHeartbeatResp),
		To:      uint64Ptr(2),
		Context: []byte("new"),
	}))

	require.Zero(t, engine.DispatchDropCount())
	require.Len(t, pd.heartbeatResp, 1)
	req := <-pd.heartbeatResp
	require.Equal(t, raftpb.MsgHeartbeatResp, req.msg.GetType())
	require.Equal(t, []byte("new"), req.msg.Context)
}

func TestEnqueueDispatchMessagePreservesReadIndexHeartbeatResponses(t *testing.T) {
	t.Parallel()
	pd := &peerQueues{
		heartbeat:     make(chan dispatchRequest, 1),
		heartbeatResp: make(chan dispatchRequest, 1),
	}
	engine := &Engine{
		nodeID: 1,
		peerDispatchers: map[uint64]*peerQueues{
			2: pd,
		},
	}
	pd.heartbeatResp <- prepareDispatchRequest(raftpb.Message{
		Type:    messageTypePtr(raftpb.MsgHeartbeatResp),
		To:      uint64Ptr(2),
		Context: []byte("read-index"),
	})

	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{
		Type: messageTypePtr(raftpb.MsgHeartbeatResp),
		To:   uint64Ptr(2),
	}))

	require.Equal(t, uint64(1), engine.DispatchDropCount())
	require.Len(t, pd.heartbeatResp, 1)
	req := <-pd.heartbeatResp
	require.Equal(t, raftpb.MsgHeartbeatResp, req.msg.GetType())
	require.Equal(t, []byte("read-index"), req.msg.Context)
}

func TestMaxAppliedIndexStartsFromSnapshotIndex(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	snap := raftTestSnapshot(5, 2, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	hs := testHardState(2, 9)
	require.NoError(t, storage.SetHardState(&hs))

	require.Equal(t, uint64(5), maxAppliedIndex(raftTestSnapshot(5, 0, nil, nil)))
}

func TestOpenRestoresLegacySnapshotState(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, saveStateFile(stateFilePath(dir), persistedState{
		HardState: testHardState(2, 6),
		Snapshot:  raftTestSnapshot(5, 2, []uint64{1}, mustEncodeSnapshotData(t, [][]byte{[]byte("snap")})),
		Entries: []raftpb.Entry{{
			Type:  entryTypePtr(raftpb.EntryNormal),
			Term:  uint64Ptr(2),
			Index: uint64Ptr(6),
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

func TestOpenMultiNodeWaitStartedWaitsForCommittedTailDrain(t *testing.T) {
	dir := t.TempDir()
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
	}
	require.NoError(t, saveStateFile(stateFilePath(dir), persistedState{
		HardState: testHardState(2, 2),
		Snapshot:  raftTestSnapshot(1, 1, []uint64{1, 2}, mustEncodeSnapshotData(t, nil)),
		Entries: []raftpb.Entry{{
			Type:  entryTypePtr(raftpb.EntryNormal),
			Term:  uint64Ptr(2),
			Index: uint64Ptr(2),
			Data:  encodeProposalEnvelope(1, []byte("tail")),
		}},
	}))

	fsm := &blockingApplyStateMachine{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan openResult, 1)
	go func() {
		engine, err := Open(context.Background(), OpenConfig{
			NodeID:       1,
			LocalID:      "n1",
			LocalAddress: "127.0.0.1:7001",
			DataDir:      dir,
			Peers:        peers,
			StateMachine: fsm,
		})
		done <- openResult{engine: engine, err: err}
	}()

	result := requireOpenResult(t, done, time.Second, "multi-node Open did not return before committed tail drain completed")
	require.NoError(t, result.err)
	require.NotNil(t, result.engine)
	defer func() {
		require.NoError(t, result.engine.Close())
	}()

	requireSignal(t, fsm.started, time.Second, "startup committed tail was not being applied")

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- result.engine.WaitStarted(context.Background())
	}()

	requireNoStartupWaitResult(t, waitDone, 20*time.Millisecond, "WaitStarted returned before committed tail drain completed")

	close(fsm.release)

	requireStartupWaitResult(t, waitDone, time.Second, "WaitStarted did not return after committed tail drain completed")
	requireSignal(t, result.engine.startedCh, time.Second, "engine did not mark started after committed tail drain completed")
}

type openResult struct {
	engine *Engine
	err    error
}

func requireOpenResult(t *testing.T, ch <-chan openResult, timeout time.Duration, msg string) openResult {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(timeout):
		t.Fatal(msg)
		return openResult{}
	}
}

func requireSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

func requireNoStartupWaitResult(t *testing.T, ch <-chan error, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case err := <-ch:
		require.NoError(t, err)
		t.Fatal(msg)
	case <-time.After(timeout):
	}
}

func requireStartupWaitResult(t *testing.T, ch <-chan error, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case err := <-ch:
		require.NoError(t, err)
	case <-time.After(timeout):
		t.Fatal(msg)
	}
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

func TestPendingConfChangeFenceTracksUnappliedConfig(t *testing.T) {
	engine := &Engine{}
	engine.appliedIndex.Store(10)

	engine.markPendingConfChange(12)
	require.True(t, engine.hasPendingConfChange())

	engine.clearPendingConfChange(11)
	require.True(t, engine.hasPendingConfChange(), "entry below the pending config index must not clear the fence")

	engine.clearPendingConfChange(12)
	require.False(t, engine.hasPendingConfChange())
}

func TestRestorePendingConfChangeFenceFromStorage(t *testing.T) {
	storage := committedTailStorageWithEntries(t, 100, 150, map[uint64]raftpb.Entry{
		130: {
			Type: entryTypePtr(raftpb.EntryConfChange),
			Data: []byte("conf"),
		},
	})
	engine := &Engine{storage: storage}
	engine.appliedIndex.Store(129)

	require.NoError(t, engine.restorePendingConfChangeFenceFromStorage(engine.appliedIndex.Load()))
	require.True(t, engine.hasPendingConfChange())

	engine.clearPendingConfChange(130)
	require.False(t, engine.hasPendingConfChange())
}

func TestRestorePendingConfChangeFenceFromStorageUsesReplayAppliedFloor(t *testing.T) {
	storage := committedTailStorageWithEntries(t, 100, 150, map[uint64]raftpb.Entry{
		130: {
			Type: entryTypePtr(raftpb.EntryConfChange),
			Data: []byte("conf"),
		},
	})
	engine := &Engine{storage: storage}
	engine.appliedIndex.Store(150)

	require.NoError(t, engine.restorePendingConfChangeFenceFromStorage(129))
	require.True(t, engine.hasPendingConfChange(),
		"RawNode replay can start before the FSM applied index after restart; the transfer fence must cover that replay window")
}

func TestRestorePendingConfChangeFenceFromStorageIgnoresAppliedConfig(t *testing.T) {
	storage := committedTailStorageWithEntries(t, 100, 150, map[uint64]raftpb.Entry{
		130: {
			Type: entryTypePtr(raftpb.EntryConfChange),
			Data: []byte("conf"),
		},
	})
	engine := &Engine{storage: storage}
	engine.appliedIndex.Store(130)

	require.NoError(t, engine.restorePendingConfChangeFenceFromStorage(engine.appliedIndex.Load()))
	require.False(t, engine.hasPendingConfChange())
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
		NodeID:        node.peer.NodeID,
		LocalID:       node.peer.ID,
		LocalAddress:  node.peer.Address,
		DataDir:       node.dir,
		Peers:         peers,
		Bootstrap:     bootstrap,
		JoinAsLearner: node.joinAsLearner,
		Transport:     node.transport,
		StateMachine:  node.fsm,
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

func rawNodeTestConfig() OpenConfig {
	return OpenConfig{
		NodeID:         1,
		ElectionTick:   defaultElectionTick,
		HeartbeatTick:  defaultHeartbeatTick,
		MaxSizePerMsg:  defaultMaxSizePerMsg,
		MaxInflightMsg: defaultMaxInflightMsg,
	}
}

func committedTailStorage(t *testing.T, snapIndex, commit uint64) *etcdraft.MemoryStorage {
	t.Helper()
	return committedTailStorageWithEntries(t, snapIndex, commit, nil)
}

func committedTailStorageWithEntries(t *testing.T, snapIndex, commit uint64, overrides map[uint64]raftpb.Entry) *etcdraft.MemoryStorage {
	t.Helper()
	storage := etcdraft.NewMemoryStorage()
	snapshot := raftTestSnapshot(snapIndex, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snapshot))
	entries := make([]*raftpb.Entry, 0, commit-snapIndex)
	for index := snapIndex + 1; index <= commit; index++ {
		entry := testEntry(index, 1, []byte("already-applied"))
		if override, ok := overrides[index]; ok {
			entry = override
			entry.Index = uint64Ptr(index)
			if entry.GetTerm() == 0 {
				entry.Term = uint64Ptr(1)
			}
		}
		entries = append(entries, &entry)
	}
	require.NoError(t, storage.Append(entries))
	hardState := testHardState(1, commit)
	require.NoError(t, storage.SetHardState(&hardState))
	return storage
}

func TestNewRawNodeSeedsAppliedFromColdStart(t *testing.T) {
	storage := committedTailStorage(t, 100, 150)
	rawApplied, err := rawNodeAppliedForOpen(storage, 150, rawNodeTestConfig())
	require.NoError(t, err)
	require.Equal(t, uint64(150), rawApplied)

	rawNode, err := newRawNode(rawNodeTestConfig(), storage, rawApplied)
	require.NoError(t, err)
	require.False(t, rawNode.HasReady(),
		"restarting from an already-applied committed tail must not redeliver it through Ready")
}

func TestNewRawNodeWithoutAppliedRedeliversCommittedTail(t *testing.T) {
	storage := committedTailStorage(t, 100, 150)

	rawNode, err := newRawNode(rawNodeTestConfig(), storage, 0)
	require.NoError(t, err)
	require.True(t, rawNode.HasReady())
	ready := rawNode.Ready()
	require.Len(t, ready.CommittedEntries, 50)
	require.Equal(t, uint64(101), ready.CommittedEntries[0].GetIndex())
	require.Equal(t, uint64(150), ready.CommittedEntries[len(ready.CommittedEntries)-1].GetIndex())
}

func TestRawNodeAppliedForOpenClipsToCommitted(t *testing.T) {
	storage := committedTailStorage(t, 100, 150)

	rawApplied, err := rawNodeAppliedForOpen(storage, 200, rawNodeTestConfig())
	require.NoError(t, err)
	require.Equal(t, uint64(150), rawApplied,
		"etcd/raft Config.Applied cannot exceed the local committed index")
}

func TestRawNodeAppliedForOpenPreservesVolatileReplay(t *testing.T) {
	storage := committedTailStorageWithEntries(t, 100, 150, map[uint64]raftpb.Entry{
		125: {
			Type: entryTypePtr(raftpb.EntryNormal),
			Data: encodeProposalEnvelope(1, []byte{0x02, 0xaa}),
		},
	})
	cfg := rawNodeTestConfig()
	cfg.StateMachine = &volatileTagFakeFSM{}

	rawApplied, err := rawNodeAppliedForOpen(storage, 150, cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(124), rawApplied,
		"RawNode must still deliver volatile duplicate entries for in-memory replay")

	rawNode, err := newRawNode(cfg, storage, rawApplied)
	require.NoError(t, err)
	require.True(t, rawNode.HasReady())
	ready := rawNode.Ready()
	require.NotEmpty(t, ready.CommittedEntries)
	require.Equal(t, uint64(125), ready.CommittedEntries[0].GetIndex())
}

func TestRawNodeAppliedForOpenPreservesConfChangeReplay(t *testing.T) {
	storage := committedTailStorageWithEntries(t, 100, 150, map[uint64]raftpb.Entry{
		130: {
			Type: entryTypePtr(raftpb.EntryConfChange),
			Data: []byte("conf"),
		},
	})
	cfg := rawNodeTestConfig()

	rawApplied, err := rawNodeAppliedForOpen(storage, 150, cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(129), rawApplied,
		"RawNode must deliver committed config changes so ApplyConfChange rebuilds membership")
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
	require.True(t, errors.Is(errors.WithStack(errLeadershipTransferNoHealthyTarget), raftengine.ErrLeadershipTransferNoHealthyTarget))
	require.True(t, errors.Is(errors.WithStack(errLeadershipTransferTargetNotCaughtUp), raftengine.ErrLeadershipTransferTargetNotCaughtUp))
	require.True(t, errors.Is(errors.WithStack(errLeadershipTransferConfChangePending), raftengine.ErrLeadershipTransferConfChangePending))
}

// TestSelectDispatchLane_LegacyThreeLane verifies that, when the opt-in
// multi-lane dispatcher is disabled (default), priority control traffic uses
// the heartbeat lane, heartbeat responses use their coalescing response lane,
// and everything else uses the normal lane.
func TestSelectDispatchLane_LegacyThreeLane(t *testing.T) {
	t.Parallel()
	engine := &Engine{dispatcherLanesEnabled: false}
	pd := &peerQueues{
		normal:        make(chan dispatchRequest, 1),
		heartbeat:     make(chan dispatchRequest, 1),
		heartbeatResp: make(chan dispatchRequest, 1),
	}

	cases := map[raftpb.MessageType]chan dispatchRequest{
		raftpb.MsgHeartbeat:     pd.heartbeat,
		raftpb.MsgHeartbeatResp: pd.heartbeatResp,
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

// TestSelectDispatchLane_FiveLane verifies that, when ELASTICKV_RAFT_DISPATCHER_LANES
// is enabled, MsgApp/MsgAppResp goes to the replication lane, MsgSnap goes to
// the snapshot lane, heartbeat responses get their own lane, and
// heartbeats/votes/read-index share the priority lane.
func TestSelectDispatchLane_FiveLane(t *testing.T) {
	t.Parallel()
	engine := &Engine{dispatcherLanesEnabled: true}
	pd := &peerQueues{
		heartbeat:     make(chan dispatchRequest, 1),
		heartbeatResp: make(chan dispatchRequest, 1),
		replication:   make(chan dispatchRequest, 1),
		snapshot:      make(chan dispatchRequest, 1),
		other:         make(chan dispatchRequest, 1),
	}

	cases := map[raftpb.MessageType]chan dispatchRequest{
		raftpb.MsgHeartbeat:     pd.heartbeat,
		raftpb.MsgHeartbeatResp: pd.heartbeatResp,
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
		require.Equalf(t, want, got, "multi-lane mode routing for %s", mt)
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
		heartbeat:     make(chan dispatchRequest, 1),
		heartbeatResp: make(chan dispatchRequest, 1),
		replication:   make(chan dispatchRequest, 1),
		snapshot:      make(chan dispatchRequest, 1),
		other:         make(chan dispatchRequest, 1),
	}
	require.NotPanics(t, func() {
		got := engine.selectDispatchLane(pd, raftpb.MsgProp)
		require.Equal(t, pd.other, got)
	})
}

// TestMultiLaneDispatcher_SnapshotDoesNotBlockReplication exercises the key
// correctness invariant for the multi-lane layout: a stuck MsgSnap transfer must
// not prevent MsgApp from being dispatched, because they now run on
// independent goroutines.
func TestMultiLaneDispatcher_SnapshotDoesNotBlockReplication(t *testing.T) {
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
		switch req.msg.GetType() { //nolint:exhaustive // test only exercises MsgSnap and MsgApp; other types are irrelevant here
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

	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2)}))
	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(2)}))

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

// TestMultiLaneDispatcher_RemovePeerClosesAllLanes confirms removePeer closes
// every lane (not just normal/heartbeat) so no worker goroutine leaks under
// the opt-in multi-lane layout.
func TestMultiLaneDispatcher_RemovePeerClosesAllLanes(t *testing.T) {
	stopCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	pd := &peerQueues{
		heartbeat:     make(chan dispatchRequest, 4),
		heartbeatResp: make(chan dispatchRequest, 4),
		replication:   make(chan dispatchRequest, 4),
		snapshot:      make(chan dispatchRequest, 4),
		other:         make(chan dispatchRequest, 4),
		ctx:           ctx,
		cancel:        cancel,
	}
	engine := &Engine{
		nodeID:                 1,
		peers:                  map[uint64]Peer{2: {NodeID: 2, ID: "peer2"}},
		peerDispatchers:        map[uint64]*peerQueues{2: pd},
		dispatchStopCh:         stopCh,
		dispatcherLanesEnabled: true,
	}
	engine.dispatchWG.Add(5)
	go engine.runDispatchWorker(ctx, pd.heartbeat)
	go engine.runDispatchWorker(ctx, pd.heartbeatResp)
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
		t.Fatal("multi-lane dispatch workers did not exit after peer removal")
	}

	// Subsequent sends to the removed peer must be dropped without panic.
	require.NoError(t, engine.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(2)}))
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
