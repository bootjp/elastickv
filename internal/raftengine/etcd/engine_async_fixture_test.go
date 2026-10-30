package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// This gate delegates to the real WAL; only the named, later user entry stops
// before Save. Election state, the leader no-op and the baseline write pass.
type asyncStorageSaveGate struct {
	etcdstorage.Storage
	payload            []byte
	entered            chan uint64
	release            chan struct{}
	releaseOnce        sync.Once
	saved              chan struct{}
	term               atomic.Uint64
	vote               atomic.Uint64
	commit             atomic.Uint64
	lastIndex          atomic.Uint64
	syncEntered        chan uint64
	syncRelease        chan struct{}
	syncOnce           sync.Once
	synced             chan struct{}
	syncPending        uint64 // Confined to the single disk writer.
	saveActive         atomic.Bool
	closeDuringSave    atomic.Bool
	closeStarted       atomic.Bool
	closeFinished      atomic.Bool
	expectedCloseError error
}

func (s *asyncStorageSaveGate) unblock() {
	if s.release != nil {
		s.releaseOnce.Do(func() { close(s.release) })
	}
	if s.syncRelease != nil {
		s.syncOnce.Do(func() { close(s.syncRelease) })
	}
}

func (s *asyncStorageSaveGate) Save(hs *raftpb.HardState, entries []*raftpb.Entry) error {
	var gatedIndex uint64
	for _, entry := range entries {
		_, payload, ok := decodeProposalEnvelope(entry.GetData())
		if ok && bytes.Equal(payload, s.payload) {
			gatedIndex = entry.GetIndex()
		}
	}
	if gatedIndex != 0 && s.entered != nil {
		s.saveActive.Store(true)
		defer s.saveActive.Store(false)
		s.entered <- gatedIndex
		<-s.release
	}
	if err := s.Storage.Save(hs, entries); err != nil {
		return err
	}
	s.recordSaved(hs, entries, gatedIndex)
	return nil
}

func (s *asyncStorageSaveGate) recordSaved(hs *raftpb.HardState, entries []*raftpb.Entry, gatedIndex uint64) {
	if !etcdraft.IsEmptyHardState(hs) {
		s.term.Store(hs.GetTerm())
		s.vote.Store(hs.GetVote())
		s.commit.Store(hs.GetCommit())
	}
	for _, entry := range entries {
		s.lastIndex.Store(entry.GetIndex())
	}
	if gatedIndex == 0 {
		return
	}
	if s.saved != nil {
		close(s.saved)
	}
	if s.syncEntered != nil {
		s.syncPending = gatedIndex
	}
}

func (s *asyncStorageSaveGate) Sync() error {
	if s.syncEntered == nil || s.syncPending == 0 {
		return s.Storage.Sync()
	}
	s.syncEntered <- s.syncPending
	<-s.syncRelease
	if err := s.Storage.Sync(); err != nil {
		return err
	}
	s.syncPending = 0
	close(s.synced)
	return nil
}

func (s *asyncStorageSaveGate) Close() error {
	if s.saveActive.Load() {
		s.closeDuringSave.Store(true)
	}
	s.closeStarted.Store(true)
	err := s.Storage.Close()
	s.closeFinished.Store(true)
	return err
}

type asyncStorageFSM struct {
	testStateMachine
	applyIndex    atomic.Uint64
	durableIndex  atomic.Uint64
	payload       []byte
	applyEntered  chan uint64
	applyRelease  chan struct{}
	applyReturned chan struct{}
	applyOnce     sync.Once
}

func (s *asyncStorageFSM) Apply(data []byte) any {
	gated := s.applyEntered != nil && bytes.Equal(data, s.payload)
	if gated {
		s.applyEntered <- s.applyIndex.Load()
		<-s.applyRelease
	}
	result := s.testStateMachine.Apply(data)
	if gated {
		close(s.applyReturned)
	}
	return result
}

func (s *asyncStorageFSM) unblock() {
	if s.applyRelease != nil {
		s.applyOnce.Do(func() { close(s.applyRelease) })
	}
}

func (s *asyncStorageFSM) SetApplyIndex(index uint64) { s.applyIndex.Store(index) }
func (s *asyncStorageFSM) SetDurableAppliedIndex(index uint64) error {
	s.durableIndex.Store(index)
	return nil
}

type asyncStorageRuntime struct {
	engine    *Engine
	follower  *etcdraft.RawNode
	storage   *etcdraft.MemoryStorage
	outbound  chan raftpb.Message
	ctx       context.Context
	proposals sync.WaitGroup
}

// Construct with existing disk/RawNode helpers, but start the actual production
// run loop only AFTER installing Storage and dispatchFn. The go statement is the
// happens-before edge: no running loop ever observes a persist replacement.
func newNativeStorageRuntime(t *testing.T, fsm *asyncStorageFSM, gate *asyncStorageSaveGate,
	buildRawNode func(OpenConfig, *etcdraft.MemoryStorage, uint64) (*etcdraft.RawNode, error),
	configure func(*Engine),
) *asyncStorageRuntime {
	t.Helper()
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7012"},
	}
	prepared, err := prepareOpenState(OpenConfig{
		NodeID: 1, LocalID: "n1", DataDir: t.TempDir(), Peers: peers,
		Bootstrap: true, StateMachine: fsm,
	})
	require.NoError(t, err)
	// Also close disk resources if construction fails before run owns them.
	ownedByLoop := false
	t.Cleanup(func() {
		if !ownedByLoop {
			require.NoError(t, closePersist(prepared.disk.Persist))
		}
	})
	applied := coldStartApplied(prepared.disk)
	rawNode, err := buildRawNode(prepared.cfg, prepared.disk.Storage, applied)
	require.NoError(t, err)
	gate.Storage = prepared.disk.Persist
	ctx, cancel := context.WithCancel(context.Background())
	h := &asyncStorageRuntime{
		ctx: ctx, outbound: make(chan raftpb.Message, defaultMaxInflightMsg),
		storage: etcdraft.NewMemoryStorage(),
	}
	t.Cleanup(cancel)
	require.NoError(t, h.storage.ApplySnapshot(&prepared.disk.LocalSnap))
	hs, _, err := prepared.disk.Storage.InitialState()
	require.NoError(t, err)
	require.NoError(t, h.storage.SetHardState(hs))
	h.follower = mustRawNode(t, h.storage, 2)
	peerMap := map[uint64]Peer{1: peers[0], 2: peers[1]}
	e := &Engine{
		nodeID: 1, localID: "n1", localAddress: prepared.cfg.LocalAddress, dataDir: prepared.cfg.DataDir,
		fsmSnapDir:   filepath.Join(prepared.cfg.DataDir, fsmSnapDirName),
		tickInterval: prepared.cfg.TickInterval, electionTick: prepared.cfg.ElectionTick,
		storage: prepared.disk.Storage, rawNode: rawNode, persist: gate, fsm: fsm,
		peers: peerMap, transport: NewGRPCTransport(peers),
		raftCutoverIndex: orInertCutover(nil),
		proposeCh:        make(chan proposalRequest), readCh: make(chan readRequest),
		adminCh: make(chan adminRequest), stepCh: make(chan raftpb.Message, minInboundQueueCapacity),
		priorityStepCh:   make(chan raftpb.Message, priorityStepQueueCapacity),
		dispatchReportCh: make(chan dispatchReport, minInboundQueueCapacity),
		closeCh:          make(chan struct{}), doneCh: make(chan struct{}),
		startedCh: make(chan struct{}), leaderReady: make(chan struct{}),
		config:     configurationFromConfState(peerMap, confStateValue(prepared.disk.LocalSnap.GetMetadata().GetConfState())),
		voterCount: len(peers), applied: applied,
		pendingProposals: map[uint64]proposalRequest{}, pendingReads: map[uint64]readRequest{},
		pendingConfigs: map[uint64]adminRequest{}, snapshotEvery: defaultSnapshotEvery,
		dispatchCtx: ctx, dispatchCancel: cancel,
		dispatchFn: func(ctx context.Context, req dispatchRequest) error {
			select {
			case h.outbound <- req.msg:
			case <-ctx.Done():
			}
			return nil
		},
	}
	h.engine = e
	e.appliedIndex.Store(applied)
	e.configIndex.Store(maxAppliedIndex(prepared.disk.LocalSnap))
	require.NoError(t, e.recoverOpenConfiguration(prepared, applied))
	if configure != nil {
		// The explicit pipeline owns the disk writer. Starting the legacy
		// snapshot worker here would create a second concurrent writer.
		configure(e)
		if e.storagePipeline != nil {
			require.NoError(t, e.attachPipelineSnapshotView(prepared.cfg, &prepared.disk.LocalSnap))
		}
	}
	e.initTransport(prepared.cfg)
	if configure == nil {
		e.initSnapshotWorker()
	}
	e.refreshStatus()
	t.Cleanup(func() {
		gate.unblock()
		fsm.unblock()
		cancel()
		err := e.Close()
		h.proposals.Wait()
		if gate.expectedCloseError != nil {
			require.ErrorIs(t, err, gate.expectedCloseError)
			return
		}
		require.NoError(t, err)
	})
	ownedByLoop = true
	go e.run()
	return h
}

// The test goroutine is the native follower's sole owner. It persists Ready
// before returning genuine raft responses; no quorum ACK is manufactured.
func (h *asyncStorageRuntime) flushFollower(t *testing.T) {
	t.Helper()
	for h.follower.HasReady() {
		rd := h.follower.Ready()
		require.True(t, etcdraft.IsEmptySnap(rd.Snapshot), "unexpected follower snapshot")
		require.NoError(t, h.storage.Append(rd.Entries))
		if !etcdraft.IsEmptyHardState(rd.HardState) {
			require.NoError(t, h.storage.SetHardState(rd.HardState))
		}
		for _, msg := range rd.Messages {
			require.NoError(t, h.engine.handleTransportMessage(h.ctx, *msg))
		}
		h.follower.Advance(rd)
	}
}

func (h *asyncStorageRuntime) deliver(t *testing.T, msg raftpb.Message) {
	t.Helper()
	require.Equal(t, uint64(2), msg.GetTo())
	require.NoError(t, h.follower.Step(&msg))
	h.flushFollower(t)
}

func asyncStorageAwait[T any](t *testing.T, h *asyncStorageRuntime, ch <-chan T, description string) T {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case result := <-ch:
			return result
		case msg := <-h.outbound:
			h.deliver(t, msg)
		case <-h.engine.Done():
			t.Fatalf("engine stopped waiting for %s: %v", description, h.engine.Err())
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func (h *asyncStorageRuntime) propose(payload []byte) chan proposalResult {
	done := make(chan proposalResult, 1)
	h.proposals.Add(1)
	go func() {
		defer h.proposals.Done()
		for {
			result, err := h.engine.Propose(h.ctx, payload)
			if !errors.Is(err, errStorageWorkBackpressure) {
				done <- proposalResult{result: result, err: err}
				return
			}
			// This sentinel is returned only by reservation BEFORE request-ID
			// allocation/copy/handoff. Wait for that admission, never retry an
			// unknown, leadership error or possibly replicated operation.
			// Admission assertions call Engine.Propose directly instead.
			runtime.Gosched()
		}
	}()
	return done
}

type asyncStorageHeartbeatProgress struct {
	nonce     []byte
	round     uint64
	confirmed bool
	periodic  int
}

func (p *asyncStorageHeartbeatProgress) observe(msg raftpb.Message) {
	if msg.GetType() == raftpb.MsgHeartbeat {
		p.observeHeartbeat(msg.GetContext())
		return
	}
	if p.round == 0 || msg.GetType() != raftpb.MsgReadIndexResp || len(msg.GetEntries()) != 1 {
		return
	}
	if bytes.Equal(msg.GetEntries()[0].GetData(), p.nonce) {
		p.confirmed = true
	}
}

func (p *asyncStorageHeartbeatProgress) observeHeartbeat(ctx []byte) {
	if len(ctx) != 0 {
		if len(ctx) == 8 {
			p.round = max(p.round, binary.LittleEndian.Uint64(ctx))
		}
		return
	}
	if p.confirmed {
		p.periodic++
	}
}

func (h *asyncStorageRuntime) verifyBlockedHeartbeats(t *testing.T, assertBlocked func()) {
	t.Helper()
	// Correct the original nonce-in-heartbeat assumption for pinned Raft v3.7:
	// read_only.go:heartbeatCtx encodes an internal little-endian read counter;
	// raft.go:responseToReadIndexReq preserves our nonce in Entries[0].Data.
	// Require that actual heartbeat round, its native nonce response, then two
	// subsequent empty heartbeat-lane messages generated by owner Tick. Both
	// synchronous and explicit async engines use this same protocol witness.
	nonce := []byte("read-index-issued-after-user-save-entered")
	h.follower.ReadIndex(nonce)
	h.flushFollower(t)
	t.Logf("post-gate native ReadIndex challenge sent: context=%q", nonce)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	progress := asyncStorageHeartbeatProgress{nonce: nonce}
	for progress.periodic < 2 {
		select {
		case msg := <-h.outbound:
			h.deliver(t, msg)
			progress.observe(msg)
			assertBlocked()
		case <-h.engine.Done():
			t.Fatalf("engine stopped with operation blocked: %v", h.engine.Err())
		case <-deadline.C:
			t.Errorf("heartbeat progress stopped with operation blocked: round=%d fresh_nonce=%t periodic_ticks=%d want=2", progress.round, progress.confirmed, progress.periodic)
			logBlockedAsyncStorageStack(t)
			return
		}
	}
	t.Logf("blocked native progress: round=%d fresh_nonce=%t periodic_ticks=%d", progress.round, progress.confirmed, progress.periodic)
}

func logBlockedAsyncStorageStack(t *testing.T) {
	t.Helper()
	stack := make([]byte, 64<<10)
	stack = stack[:runtime.Stack(stack, true)]
	for _, goroutine := range strings.Split(string(stack), "\n\n") {
		if strings.Contains(goroutine, "(*asyncStorageSaveGate).Save") {
			t.Logf("blocked storage stack:\n%s", goroutine)
		}
	}
}

type asyncStorageRuntimeFactory func(*testing.T, *asyncStorageFSM, *asyncStorageSaveGate) *asyncStorageRuntime

func runEngineHeldUserSaveScenario(t *testing.T, factory asyncStorageRuntimeFactory) {
	t.Helper()
	baseline, payload := []byte("durable-leader-baseline"), []byte("gated-user-entry")
	gate := &asyncStorageSaveGate{
		payload: payload, entered: make(chan uint64, 1),
		release: make(chan struct{}), saved: make(chan struct{}),
	}
	fsm := &asyncStorageFSM{}
	h := factory(t, fsm, gate)
	asyncStorageAwait(t, h, h.engine.startedCh, "engine startup")
	// TimeoutNow drives a deterministic election on the OWNER, not by calling
	// Campaign or Tick on its RawNode from the test goroutine.
	require.NoError(t, h.engine.handleTransportMessage(h.ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgTimeoutNow), From: uint64Ptr(2), To: uint64Ptr(1), Term: uint64Ptr(1),
	}))
	asyncStorageAwait(t, h, h.engine.leaderReady, "leader election")
	first := asyncStorageAwait(t, h, h.propose(baseline), "durable baseline proposal")
	require.NoError(t, first.err)
	require.NotNil(t, first.result)
	require.Equal(t, string(baseline), first.result.Response)
	require.Equal(t, raftengine.StateLeader, h.engine.State())
	require.Equal(t, h.engine.Status().Term, gate.term.Load())
	require.Equal(t, uint64(1), gate.vote.Load())
	require.GreaterOrEqual(t, gate.commit.Load(), first.result.CommitIndex)
	require.GreaterOrEqual(t, gate.lastIndex.Load(), first.result.CommitIndex)
	beforeApplied := h.engine.AppliedIndex()
	beforeFSMIndex, beforeDurableIndex := fsm.applyIndex.Load(), fsm.durableIndex.Load()
	require.Equal(t, [][]byte{baseline}, fsm.Applied())
	t.Logf("durable leader: term=%d vote=%d commit=%d applied=%d", gate.term.Load(), gate.vote.Load(), gate.commit.Load(), beforeApplied)

	done := h.propose(payload)
	index := asyncStorageAwait(t, h, gate.entered, "later user-entry Save gate")
	require.Greater(t, index, beforeApplied)
	t.Logf("user Save blocked before WAL delegation: index=%d", index)
	assertBlocked := func() {
		t.Helper()
		select {
		case <-gate.saved:
			t.Fatal("gated Save completed before release")
		default:
		}
		select {
		case result := <-done:
			t.Fatalf("proposal returned before local durability: %+v", result)
		default:
		}
		require.Equal(t, beforeApplied, h.engine.AppliedIndex(), "applied index advanced before Save")
		require.Equal(t, beforeFSMIndex, fsm.applyIndex.Load(), "FSM apply started before Save")
		require.Equal(t, beforeDurableIndex, fsm.durableIndex.Load(), "durable applied-index callback advanced before Save")
		require.Equal(t, [][]byte{baseline}, fsm.Applied(), "user entry reached FSM before Save")
	}
	assertBlocked()

	h.verifyBlockedHeartbeats(t, assertBlocked)
	assertBlocked()
	t.Logf("blocked safety: no proposal ACK, FSM apply or applied-index progress; applied=%d", h.engine.AppliedIndex())

	// Continue after the diagnostic red so it cannot be a broken proposal or
	// permanently stuck fixture masquerading as a heartbeat regression.
	// Cleanup may also release the channel if an earlier assertion fails.
	gate.unblock()
	result := asyncStorageAwait(t, h, done, "real proposal after Save release")
	require.NoError(t, result.err)
	require.NotNil(t, result.result)
	require.Equal(t, index, result.result.CommitIndex)
	require.Equal(t, string(payload), result.result.Response)
	require.Equal(t, [][]byte{baseline, payload}, fsm.Applied())
	require.Equal(t, index, h.engine.AppliedIndex())
	select {
	case <-gate.saved:
	default:
		t.Fatal("proposal succeeded before gated WAL Save completed")
	}
	t.Logf("released Save: real proposal response=%q commit=%d applied=%d", result.result.Response, result.result.CommitIndex, h.engine.AppliedIndex())
}
