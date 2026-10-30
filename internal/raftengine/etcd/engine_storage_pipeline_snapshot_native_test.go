package etcd

import (
	"context"
	"io"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type nativeSnapshotGate struct {
	stage   string
	minimum atomic.Uint64
	used    atomic.Bool
	entered chan uint64
	release chan struct{}
	once    sync.Once
	failure error
}

func newNativeSnapshotGate(stage string) *nativeSnapshotGate {
	g := &nativeSnapshotGate{stage: stage, entered: make(chan uint64, 1), release: make(chan struct{})}
	g.minimum.Store(math.MaxUint64)
	return g
}

func (g *nativeSnapshotGate) visit(stage string, index uint64) error {
	if g.stage == stage && index >= g.minimum.Load() && g.used.CompareAndSwap(false, true) {
		g.entered <- index
		<-g.release
		return g.failure
	}
	return nil
}

func (g *nativeSnapshotGate) unblock() { g.once.Do(func() { close(g.release) }) }

type nativeSnapshotFSM struct {
	asyncStorageFSM
	gate         *nativeSnapshotGate
	restores     atomic.Uint64
	deferCapture atomic.Bool
}

func (f *nativeSnapshotFSM) Apply(data []byte) any {
	if err := f.gate.visit("apply", f.applyIndex.Load()); err != nil {
		panic(err)
	}
	return f.asyncStorageFSM.Apply(data)
}

func (f *nativeSnapshotFSM) Snapshot() (Snapshot, error) {
	if f.deferCapture.Load() {
		return nil, raftengine.ErrSnapshotDeferred
	}
	index := f.applyIndex.Load()
	if index >= 3 {
		f.gate.minimum.Store(index)
	}
	if err := f.gate.visit("capture", index); err != nil {
		return nil, err
	}
	snapshot, err := f.asyncStorageFSM.Snapshot()
	if err != nil {
		return nil, err
	}
	return &nativeCheckpointSnapshot{Snapshot: snapshot, gate: f.gate, index: index}, nil
}

type nativeCheckpointSnapshot struct {
	Snapshot
	gate  *nativeSnapshotGate
	index uint64
}

func (s *nativeCheckpointSnapshot) WriteTo(writer io.Writer) (int64, error) {
	if err := s.gate.visit("serialize", s.index); err != nil {
		return 0, err
	}
	return s.Snapshot.WriteTo(writer)
}

func (s *nativeCheckpointSnapshot) Close() error {
	err := s.gate.visit("snapshot-close", s.index)
	return errors.CombineErrors(err, s.Snapshot.Close())
}

func (f *nativeSnapshotFSM) Restore(reader io.Reader) error {
	if err := f.gate.visit("restore", f.gate.minimum.Load()); err != nil {
		return err
	}
	f.restores.Add(1)
	return f.asyncStorageFSM.Restore(reader)
}

func (f *nativeSnapshotFSM) SetDurableAppliedIndex(index uint64) error {
	if err := f.gate.visit("durable", index); err != nil {
		return err
	}
	return f.asyncStorageFSM.SetDurableAppliedIndex(index)
}

type nativeSnapshotStorage struct {
	etcdstorage.Storage
	gate *nativeSnapshotGate
}

func (s *nativeSnapshotStorage) SaveSnap(snapshot *raftpb.Snapshot) error {
	if err := s.gate.visit("save", snapshot.GetMetadata().GetIndex()); err != nil {
		return err
	}
	return s.Storage.SaveSnap(snapshot)
}

func (s *nativeSnapshotStorage) Release(snapshot *raftpb.Snapshot) error {
	if err := s.gate.visit("release", snapshot.GetMetadata().GetIndex()); err != nil {
		return err
	}
	return s.Storage.Release(snapshot)
}

type nativeSnapshotEnvelope struct {
	from    uint64
	message raftpb.Message
}

type nativeSnapshotCluster struct {
	nodes           []*transportTestNode
	fsms            []*nativeSnapshotFSM
	gates           []*nativeSnapshotGate
	wire            chan nativeSnapshotEnvelope
	dropThird       bool // test-owned network partition, never read by engine actors
	dropAppendThird bool
	ackThird        atomic.Uint64
	failures        map[uint64]error
}

func newNativeSnapshotCluster(t *testing.T, leaderStage, receiverStage string) *nativeSnapshotCluster {
	t.Helper()
	return newNativeSnapshotClusterWithMachines(t, leaderStage, receiverStage, nil)
}

func newNativeSnapshotClusterWithMachines(t *testing.T, leaderStage, receiverStage string, machines []pipelineNativeConfigurationStateMachine) *nativeSnapshotCluster {
	t.Helper()
	return newNativeSnapshotClusterWithHook(t, leaderStage, receiverStage, machines, func(*Engine, int) {})
}

func newNativeSnapshotClusterWithHook(t *testing.T, leaderStage, receiverStage string, machines []pipelineNativeConfigurationStateMachine, beforeRun func(*Engine, int)) *nativeSnapshotCluster {
	t.Helper()
	nodes, peers := newTransportTestNodes(t, 3)
	startTransportTestServers(nodes, peers)
	c := &nativeSnapshotCluster{nodes: nodes, wire: make(chan nativeSnapshotEnvelope, 4096), failures: map[uint64]error{}}
	t.Cleanup(func() { c.cleanup(t) })
	for i, node := range nodes {
		stage := map[int]string{0: leaderStage, 2: receiverStage}[i]
		gate := newNativeSnapshotGate(stage)
		fsm := &nativeSnapshotFSM{gate: gate}
		var machine StateMachine = fsm
		if len(machines) > 0 {
			machine = &nativeSnapshotKVFSM{pipelineNativeConfigurationStateMachine: machines[i], gate: gate}
		}
		c.gates = append(c.gates, gate)
		c.fsms = append(c.fsms, fsm)
		prepared, err := prepareOpenState(OpenConfig{NodeID: node.peer.NodeID, LocalID: node.peer.ID, LocalAddress: node.peer.Address,
			DataDir: node.dir, Peers: peers, Bootstrap: true, StateMachine: machine, Transport: node.transport})
		require.NoError(t, err)
		raw, err := newStorageRawNode(prepared.cfg, prepared.disk.Storage, coldStartApplied(prepared.disk))
		require.NoError(t, err)
		disk := &nativeSnapshotStorage{Storage: prepared.disk.Persist, gate: gate}
		pipeline, err := newEngineStoragePipeline(disk)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		peerMap := make(map[uint64]Peer)
		for _, peer := range peers {
			peerMap[peer.NodeID] = peer
		}
		e := &Engine{nodeID: node.peer.NodeID, localID: node.peer.ID, localAddress: node.peer.Address,
			dataDir: node.dir, fsmSnapDir: filepath.Join(node.dir, fsmSnapDirName), storage: prepared.disk.Storage, persist: disk,
			rawNode: raw, fsm: machine, storagePipeline: pipeline, peers: peerMap, transport: node.transport,
			tickInterval: prepared.cfg.TickInterval, electionTick: prepared.cfg.ElectionTick,
			raftCutoverIndex: orInertCutover(nil), applied: coldStartApplied(prepared.disk), voterCount: len(peers),
			config:    configurationFromConfState(peerMap, confStateValue(prepared.disk.LocalSnap.GetMetadata().GetConfState())),
			proposeCh: make(chan proposalRequest), readCh: make(chan readRequest), adminCh: make(chan adminRequest),
			stepCh: make(chan raftpb.Message, minInboundQueueCapacity), priorityStepCh: make(chan raftpb.Message, priorityStepQueueCapacity),
			dispatchReportCh: make(chan dispatchReport, minInboundQueueCapacity), closeCh: make(chan struct{}), doneCh: make(chan struct{}),
			startedCh: make(chan struct{}), leaderReady: make(chan struct{}), pendingProposals: map[uint64]proposalRequest{},
			pendingReads: map[uint64]readRequest{}, pendingConfigs: map[uint64]adminRequest{}, dispatchCtx: ctx, dispatchCancel: cancel,
			snapshotEvery: defaultSnapshotEvery}
		if i == 0 {
			e.snapshotEvery = 1
		}
		e.appliedIndex.Store(e.applied)
		e.configIndex.Store(1)
		node.engine = e
		e.dispatchFn = c.dispatcher(node)
		beforeRun(e, i)
		require.NoError(t, e.recoverOpenConfiguration(prepared, e.applied))
		e.initTransport(prepared.cfg)
		e.refreshStatus()
		go e.run()
	}
	for _, node := range nodes {
		c.pumpUntil(t, func() bool {
			select {
			case <-node.engine.startedCh:
				return true
			default:
				return false
			}
		}, "startup")
	}
	require.NoError(t, nodes[0].engine.handleTransportMessage(t.Context(), raftpb.Message{Type: messageTypePtr(raftpb.MsgTimeoutNow), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(1)}))
	c.pumpUntil(t, func() bool { return nodes[0].engine.State() == raftengine.StateLeader }, "three-voter native election")
	return c
}

func (c *nativeSnapshotCluster) cleanup(t *testing.T) {
	t.Helper()
	for _, gate := range c.gates {
		gate.unblock()
	}
	for _, node := range c.nodes {
		if failure := c.failures[node.peer.NodeID]; failure != nil {
			require.ErrorIs(t, node.engine.Close(), failure)
			node.engine = nil
		}
	}
	cleanupTransportTestNodes(t, c.nodes)
}

func TestEnginePipelineReceivedSnapshotFailuresQuiesce(t *testing.T) {
	for _, stage := range []string{"save", "restore", "durable", "release"} {
		t.Run(stage, func(t *testing.T) { testPipelineReceivedSnapshotFailure(t, stage) })
	}
}

func testPipelineReceivedSnapshotFailure(t *testing.T, stage string) {
	t.Helper()
	c := newNativeSnapshotCluster(t, "", stage)
	failure := errors.New("injected native received snapshot " + stage)
	c.failures[3] = failure
	c.gates[2].failure = failure
	c.dropThird = true
	index := c.propose(t, []byte("fault-target"))
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > index }, "fault checkpoint")
	c.gates[2].minimum.Store(index)
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "fault stage")
	c.requireHeartbeats(t, func() { require.Less(t, c.ackThird.Load(), index) })
	c.dropThird = true
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool {
		select {
		case <-c.nodes[2].engine.Done():
			return true
		default:
			return false
		}
	}, "native snapshot failure join")
	e := c.nodes[2].engine
	require.ErrorIs(t, e.Err(), failure)
	require.Less(t, c.ackThird.Load(), index)
	require.Less(t, e.AppliedIndex(), index)
	bytes, items := e.storagePipeline.snapshotBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
	require.Empty(t, e.storagePipeline.snapshotResources.leases)
	selected, err := loadWalState(zap.NewNop(), filepath.Join(e.dataDir, walDirName), filepath.Join(e.dataDir, snapDirName), e.fsmSnapDir, &testStateMachine{}, nil)
	require.NoError(t, err)
	if stage == "save" {
		require.Less(t, selected.LocalSnap.GetMetadata().GetIndex(), index)
	} else {
		require.Equal(t, index, selected.LocalSnap.GetMetadata().GetIndex(), "durable snapshot survives failure before publication")
	}
	require.NoError(t, closePersist(selected.Persist))
}

func (c *nativeSnapshotCluster) dispatcher(node *transportTestNode) func(context.Context, dispatchRequest) error {
	return func(ctx context.Context, request dispatchRequest) error {
		if request.msg.GetTo() > uint64(len(c.nodes)) {
			return nil // Explicitly offline learner; the native three-voter quorum still runs.
		}
		if node.peer.NodeID == 3 {
			c.recordPositiveSnapshotAck(&request.msg)
		}
		if request.msg.GetType() == raftpb.MsgSnap {
			return node.transport.Dispatch(ctx, request.msg)
		}
		select {
		case c.wire <- nativeSnapshotEnvelope{from: node.peer.NodeID, message: request.msg}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *nativeSnapshotCluster) recordPositiveSnapshotAck(message *raftpb.Message) {
	if message.GetType() != raftpb.MsgAppResp || message.GetReject() {
		return
	}
	for {
		prior := c.ackThird.Load()
		if message.GetIndex() <= prior || c.ackThird.CompareAndSwap(prior, message.GetIndex()) {
			return
		}
	}
}

func (c *nativeSnapshotCluster) deliver(t *testing.T, envelope nativeSnapshotEnvelope) {
	t.Helper()
	to := envelope.message.GetTo()
	if c.drops(envelope) {
		return
	}
	require.NotZero(t, to)
	require.LessOrEqual(t, to, uint64(len(c.nodes)))
	err := c.nodes[to-1].engine.handleTransportMessage(t.Context(), envelope.message)
	if failure := c.failures[to]; failure != nil && errors.Is(err, failure) {
		return
	}
	// The native transport reports backpressure to its sender; Raft retries
	// rejected appends. Preserve that lossy-network behavior in this wire.
	if !errors.Is(err, errStepQueueFull) {
		require.NoError(t, err)
	}
}

func (c *nativeSnapshotCluster) drops(envelope nativeSnapshotEnvelope) bool {
	return (c.dropThird && (envelope.message.GetTo() == 3 || envelope.from == 3)) ||
		(c.dropAppendThird && envelope.message.GetTo() == 3 && envelope.message.GetType() == raftpb.MsgApp && len(envelope.message.GetEntries()) > 0)
}

func (c *nativeSnapshotCluster) pumpUntil(t *testing.T, ready func() bool, what string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !ready() {
		select {
		case message := <-c.wire:
			c.deliver(t, message)
		case <-deadline.C:
			for _, node := range c.nodes {
				t.Logf("node=%d status=%+v error=%v", node.peer.NodeID, node.engine.Status(), node.engine.Err())
			}
			t.Fatalf("no native progress waiting for %s", what)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func (c *nativeSnapshotCluster) propose(t *testing.T, payload []byte) uint64 {
	t.Helper()
	done := make(chan proposalResult, 1)
	go func() {
		result, err := c.nodes[0].engine.Propose(t.Context(), payload)
		done <- proposalResult{result: result, err: err}
	}()
	var result proposalResult
	c.pumpUntil(t, func() bool {
		select {
		case result = <-done:
			return true
		default:
			return false
		}
	}, "real proposal")
	require.NoError(t, result.err)
	return result.result.CommitIndex
}

func (c *nativeSnapshotCluster) requireHeartbeats(t *testing.T, held func()) {
	t.Helper()
	c.requireMatchingHeartbeats(t, held, nativeSnapshotHeartbeat)
}

func (c *nativeSnapshotCluster) requireReceiverHeartbeats(t *testing.T, held func()) {
	t.Helper()
	c.requireMatchingHeartbeats(t, held, func(envelope nativeSnapshotEnvelope) bool {
		return envelope.from == 3 && envelope.message.GetType() == raftpb.MsgHeartbeatResp && len(envelope.message.GetContext()) == 0
	})
}

func (c *nativeSnapshotCluster) requireMatchingHeartbeats(t *testing.T, held func(), matches func(nativeSnapshotEnvelope) bool) {
	t.Helper()
	verified := make(chan error, 1)
	go func() { verified <- c.nodes[0].engine.VerifyLeader(t.Context()) }()
	done, ticks := false, 0
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !done || ticks < 2 {
		select {
		case err := <-verified:
			require.NoError(t, err)
			done = true
		case envelope := <-c.wire:
			c.deliver(t, envelope)
			if done && matches(envelope) {
				ticks++
			}
			held()
		case <-deadline.C:
			t.Fatal("blocked snapshot stopped native heartbeat progress")
		}
	}
}

func nativeSnapshotHeartbeat(envelope nativeSnapshotEnvelope) bool {
	return envelope.from == 1 && envelope.message.GetTo() == 2 && envelope.message.GetType() == raftpb.MsgHeartbeat && len(envelope.message.GetContext()) == 0
}

func TestEnginePipelineLocalCheckpointNativeHeldStages(t *testing.T) {
	for _, stage := range []string{"capture", "serialize", "snapshot-close", "durable", "save", "release"} {
		t.Run(stage, func(t *testing.T) {
			c := newNativeSnapshotCluster(t, stage, "")
			index := c.propose(t, []byte("checkpoint-target"))
			c.pumpUntil(t, func() bool { return c.gates[0].used.Load() }, "held local checkpoint")
			c.requireHeartbeats(t, func() {
				snapshot, err := c.nodes[0].engine.storage.Snapshot()
				require.NoError(t, err)
				if stage != "release" {
					require.Less(t, snapshot.GetMetadata().GetIndex(), index)
				}
			})
			c.gates[0].unblock()
			c.pumpUntil(t, func() bool {
				snapshot, _ := c.nodes[0].engine.storage.Snapshot()
				return snapshot.GetMetadata().GetIndex() >= index
			}, "durable local checkpoint")
			require.NoError(t, c.nodes[0].engine.Close())
			disk, err := loadWalState(zap.NewNop(), filepath.Join(c.nodes[0].dir, walDirName), filepath.Join(c.nodes[0].dir, snapDirName), filepath.Join(c.nodes[0].dir, fsmSnapDirName), &testStateMachine{}, nil)
			require.NoError(t, err)
			require.GreaterOrEqual(t, disk.LocalSnap.GetMetadata().GetIndex(), index)
			require.Equal(t, []uint64{1, 2, 3}, disk.LocalSnap.GetMetadata().GetConfState().GetVoters())
			require.NoError(t, closePersist(disk.Persist))
		})
	}
}

func TestEnginePipelineThreeVoterStreamedSnapshotCatchup(t *testing.T) {
	for _, stage := range []string{"save", "restore", "release"} {
		t.Run(stage, func(t *testing.T) { testPipelineStreamedSnapshotCatchup(t, stage) })
	}
}

func testPipelineStreamedSnapshotCatchup(t *testing.T, stage string) {
	t.Helper()
	c := newNativeSnapshotCluster(t, "", stage)
	c.dropThird = true
	index := c.propose(t, []byte("checkpoint-target"))
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > index }, "actual checkpoint compaction")
	c.gates[2].minimum.Store(index)
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "actual streamed snapshot restore")
	c.requireReceiverHeartbeats(t, func() {
		require.Less(t, c.ackThird.Load(), index, "opaque append response escaped before snapshot durability/publication")
		require.Less(t, c.nodes[2].engine.AppliedIndex(), index)
		snapshot, err := c.nodes[2].engine.storage.Snapshot()
		require.NoError(t, err)
		require.Less(t, snapshot.GetMetadata().GetIndex(), index)
	})
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool { return c.nodes[2].engine.AppliedIndex() >= index }, "published native snapshot")
	require.Equal(t, c.fsms[0].Applied(), c.fsms[2].Applied())
	require.Positive(t, c.fsms[2].restores.Load())
	require.Positive(t, c.nodes[0].engine.SnapshotStreamSendCount())
	snapshot, err := c.nodes[2].engine.storage.Snapshot()
	require.NoError(t, err)
	require.GreaterOrEqual(t, snapshot.GetMetadata().GetIndex(), index)
	require.Equal(t, []uint64{1, 2, 3}, snapshot.GetMetadata().GetConfState().GetVoters())
	for _, node := range c.nodes {
		require.NoError(t, node.engine.Close())
	}
	for _, node := range c.nodes {
		used, items := node.engine.storagePipeline.snapshotBudget.usage()
		require.Zero(t, used)
		require.Zero(t, items)
	}
}

func TestEnginePipelineReceivedSnapshotCloseJoinsActualRestore(t *testing.T) {
	c := newNativeSnapshotCluster(t, "", "restore")
	c.dropThird = true
	index := c.propose(t, []byte("close-target"))
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > index }, "checkpoint")
	c.gates[2].minimum.Store(index)
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "held restore")
	c.dropThird = true
	closed := make(chan error, 1)
	go func() { closed <- c.nodes[2].engine.Close() }()
	c.requireHeartbeats(t, func() {
		select {
		case err := <-closed:
			t.Fatalf("Close fabricated completion of a held restore: %v", err)
		default:
		}
		require.Less(t, c.ackThird.Load(), index)
	})
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool {
		select {
		case err := <-closed:
			require.NoError(t, err)
			return true
		default:
			return false
		}
	}, "real restore/Close join")
	bytes, items := c.nodes[2].engine.storagePipeline.snapshotBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}
