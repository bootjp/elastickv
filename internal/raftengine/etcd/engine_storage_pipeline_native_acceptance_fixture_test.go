package etcd

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const asyncNativeLargeValueBytes = (64 << 20) - (1 << 19)

// Gate the required Sync after a real large-entry WAL Save, never an ACK.
type asyncNativeAcceptanceDisk struct {
	etcdstorage.Storage
	armed   atomic.Bool
	target  atomic.Uint64
	synced  atomic.Uint64
	pending uint64 // disk actor only
	entered chan uint64
	release chan struct{}
	once    sync.Once
}

func (d *asyncNativeAcceptanceDisk) Save(hs *raftpb.HardState, entries []*raftpb.Entry) error {
	if err := d.Storage.Save(hs, entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if len(entry.GetData()) >= asyncNativeLargeValueBytes {
			d.pending = entry.GetIndex()
			d.target.Store(d.pending)
		}
	}
	return nil
}

func (d *asyncNativeAcceptanceDisk) Sync() error {
	if d.pending != 0 && d.armed.CompareAndSwap(true, false) {
		d.entered <- d.pending
		<-d.release
	}
	if err := d.Storage.Sync(); err != nil {
		return err
	}
	if d.pending != 0 {
		d.synced.Store(d.pending)
	}
	return nil
}

func (d *asyncNativeAcceptanceDisk) unblock() { d.once.Do(func() { close(d.release) }) }

type asyncNativeAcceptanceFSM struct {
	pipelineNativeConfigurationStateMachine
	checkpoint atomic.Bool
	restores   atomic.Uint64
}

func (f *asyncNativeAcceptanceFSM) Snapshot() (Snapshot, error) {
	if !f.checkpoint.Load() {
		return nil, raftengine.ErrSnapshotDeferred
	}
	return f.pipelineNativeConfigurationStateMachine.Snapshot()
}

func (f *asyncNativeAcceptanceFSM) Restore(reader io.Reader) error {
	if err := f.pipelineNativeConfigurationStateMachine.Restore(reader); err != nil {
		return err
	}
	f.restores.Add(1)
	return nil
}

type asyncNativeEncryptionObserver struct {
	maximum  atomic.Int64
	failures atomic.Uint64
}

func (o *asyncNativeEncryptionObserver) ObserveEncryptionDecryptFailure(string) { o.failures.Add(1) }
func (o *asyncNativeEncryptionObserver) ObserveEncryptionWrite(_ uint32, plaintextBytes, _ int) {
	for prior := o.maximum.Load(); int64(plaintextBytes) > prior; prior = o.maximum.Load() {
		if o.maximum.CompareAndSwap(prior, int64(plaintextBytes)) {
			return
		}
	}
}

func newAsyncNativeEncryptedStore(t *testing.T, dir string, node, epoch uint16, observer *asyncNativeEncryptionObserver) store.MVCCStore {
	t.Helper()
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(1, bytes.Repeat([]byte{0x51}, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	st, err := store.NewPebbleStore(dir,
		store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(node, epoch), func() (uint32, bool) { return 1, true }),
		store.WithEncryptionObserver(observer))
	require.NoError(t, err)
	return st
}

type asyncNativeAcceptanceCluster struct {
	nodes      []*transportTestNode
	peers      []Peer
	stores     []store.MVCCStore
	fsms       []*asyncNativeAcceptanceFSM
	disks      []*asyncNativeAcceptanceDisk
	observers  []*asyncNativeEncryptionObserver
	partition3 atomic.Bool
	earlyAck   atomic.Bool
	ack2       atomic.Uint64
	events     chan raftpb.Message // only small copied heartbeat/read-index fields
	dropped    atomic.Uint64
	ctx        context.Context
	cancel     context.CancelFunc
	proposals  sync.WaitGroup
}

func newAsyncNativeAcceptanceCluster(t *testing.T) *asyncNativeAcceptanceCluster {
	t.Helper()
	nodes, peers := newTransportTestNodes(t, 3)
	startTransportTestServers(nodes, peers)
	ctx, cancel := context.WithCancel(context.Background())
	c := &asyncNativeAcceptanceCluster{nodes: nodes, peers: peers, ctx: ctx, cancel: cancel, events: make(chan raftpb.Message, 4096),
		stores: make([]store.MVCCStore, len(nodes)), fsms: make([]*asyncNativeAcceptanceFSM, len(nodes)),
		disks: make([]*asyncNativeAcceptanceDisk, len(nodes)), observers: make([]*asyncNativeEncryptionObserver, len(nodes))}
	t.Cleanup(func() { c.cleanup(t) })
	for i, node := range nodes {
		observer := &asyncNativeEncryptionObserver{}
		st := newAsyncNativeEncryptedStore(t, filepath.Join(node.dir, "kv"), uint16(i+1), 1, observer)
		machine, ok := kv.NewKvFSMWithHLC(st, nil).(pipelineNativeConfigurationStateMachine)
		require.True(t, ok)
		fsm := &asyncNativeAcceptanceFSM{pipelineNativeConfigurationStateMachine: machine}
		disk := &asyncNativeAcceptanceDisk{entered: make(chan uint64, 1), release: make(chan struct{})}
		c.stores[i], c.fsms[i], c.disks[i], c.observers[i] = st, fsm, disk, observer
		node.engine = openAsyncNativeAcceptanceEngine(t, node, peers, fsm, disk, true, ctx, c.dispatch(node))
	}
	for _, node := range nodes {
		c.wait(t, func() bool {
			select {
			case <-node.engine.startedCh:
				return true
			default:
				return false
			}
		}, "explicit async startup")
		require.NotNil(t, node.engine.storagePipeline)
		require.Nil(t, node.engine.snapshotReqCh, "legacy snapshot worker must not run alongside async disk actor")
	}
	require.NoError(t, nodes[0].engine.handleTransportMessage(ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgTimeoutNow), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(1),
	}))
	c.wait(t, func() bool { return nodes[0].engine.State() == raftengine.StateLeader }, "three-voter election")
	return c
}

func (c *asyncNativeAcceptanceCluster) cleanup(t *testing.T) {
	t.Helper()
	for _, disk := range c.disks {
		if disk != nil {
			disk.unblock()
		}
	}
	for _, node := range c.nodes {
		closeAsyncNativeAcceptanceNode(t, node)
	}
	c.cancel()
	c.proposals.Wait()
	for _, st := range c.stores {
		if st != nil {
			if err := st.Close(); err != nil {
				t.Errorf("close encrypted native store: %v", err)
			}
		}
	}
}

func closeAsyncNativeAcceptanceNode(t *testing.T, node *transportTestNode) {
	t.Helper()
	if node.engine != nil {
		if err := node.engine.Close(); err != nil {
			t.Errorf("close native engine %d: %v", node.peer.NodeID, err)
		}
	}
	if node.server != nil {
		node.server.Stop()
	}
	if node.lis != nil {
		_ = node.lis.Close()
	}
	if node.transport != nil {
		if err := node.transport.Close(); err != nil {
			t.Errorf("close native transport %d: %v", node.peer.NodeID, err)
		}
	}
}

// Like the existing explicit native fixture, install the production pipeline
// before run. Never call Open (which deliberately keeps the synchronous default).
func openAsyncNativeAcceptanceEngine(t *testing.T, node *transportTestNode, peers []Peer, fsm *asyncNativeAcceptanceFSM,
	disk *asyncNativeAcceptanceDisk, bootstrap bool, parent context.Context, dispatch func(context.Context, dispatchRequest) error,
) *Engine {
	t.Helper()
	fsm.checkpoint.Store(bootstrap)
	prepared, err := prepareOpenState(OpenConfig{NodeID: node.peer.NodeID, LocalID: node.peer.ID, LocalAddress: node.peer.Address,
		DataDir: node.dir, Peers: peers, Bootstrap: bootstrap, StateMachine: fsm, Transport: node.transport})
	require.NoError(t, err)
	fsm.checkpoint.Store(false)
	raw, err := newStorageRawNode(prepared.cfg, prepared.disk.Storage, coldStartApplied(prepared.disk))
	require.NoError(t, err)
	disk.Storage = prepared.disk.Persist
	pipeline, err := newEngineStoragePipeline(disk)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(parent)
	peerMap := make(map[uint64]Peer)
	for _, peer := range prepared.peers {
		peerMap[peer.NodeID] = peer
	}
	e := &Engine{nodeID: node.peer.NodeID, localID: node.peer.ID, localAddress: node.peer.Address,
		dataDir: node.dir, fsmSnapDir: filepath.Join(node.dir, fsmSnapDirName), storage: prepared.disk.Storage,
		persist: disk, rawNode: raw, fsm: fsm, storagePipeline: pipeline, peers: peerMap, transport: node.transport,
		tickInterval: prepared.cfg.TickInterval, electionTick: prepared.cfg.ElectionTick,
		raftCutoverIndex: orInertCutover(nil), applied: coldStartApplied(prepared.disk), voterCount: len(peerMap),
		config:    configurationFromConfState(peerMap, confStateValue(prepared.disk.LocalSnap.GetMetadata().GetConfState())),
		proposeCh: make(chan proposalRequest), readCh: make(chan readRequest), adminCh: make(chan adminRequest),
		stepCh: make(chan raftpb.Message, minInboundQueueCapacity), priorityStepCh: make(chan raftpb.Message, priorityStepQueueCapacity),
		dispatchReportCh: make(chan dispatchReport, minInboundQueueCapacity), closeCh: make(chan struct{}), doneCh: make(chan struct{}),
		startedCh: make(chan struct{}), leaderReady: make(chan struct{}), pendingProposals: map[uint64]proposalRequest{},
		pendingReads: map[uint64]readRequest{}, pendingConfigs: map[uint64]adminRequest{}, dispatchCtx: ctx, dispatchCancel: cancel,
		dispatchFn: dispatch, snapshotEvery: 1}
	e.appliedIndex.Store(e.applied)
	e.configIndex.Store(maxAppliedIndex(prepared.disk.LocalSnap))
	require.NoError(t, e.recoverOpenConfiguration(prepared, e.applied))
	e.initTransport(prepared.cfg)
	e.refreshStatus()
	go e.run()
	return e
}

func (c *asyncNativeAcceptanceCluster) dispatch(node *transportTestNode) func(context.Context, dispatchRequest) error {
	return func(ctx context.Context, request dispatchRequest) error {
		m := request.msg
		if c.partition3.Load() && (node.peer.NodeID == 3 || m.GetTo() == 3) {
			return status.Error(codes.Unavailable, "test-owned node3 partition")
		}
		ack := node.peer.NodeID == 2 && m.GetType() == raftpb.MsgAppResp && !m.GetReject()
		if ack {
			c.checkAckDurability(m.GetIndex())
		}
		err := node.transport.Dispatch(ctx, m)
		if err == nil {
			c.observe(node.peer.NodeID, &m)
			if ack {
				c.recordAck(m.GetIndex())
			}
		}
		return err
	}
}

func (c *asyncNativeAcceptanceCluster) checkAckDurability(index uint64) {
	target := c.disks[1].target.Load()
	if target != 0 && index >= target && c.disks[1].synced.Load() < target {
		c.earlyAck.Store(true)
	}
}

func (c *asyncNativeAcceptanceCluster) recordAck(index uint64) {
	for prior := c.ack2.Load(); index > prior; prior = c.ack2.Load() {
		if c.ack2.CompareAndSwap(prior, index) {
			return
		}
	}
}

func (c *asyncNativeAcceptanceCluster) observe(from uint64, m *raftpb.Message) {
	if from != 1 || m.GetTo() != 2 || (m.GetType() != raftpb.MsgHeartbeat && m.GetType() != raftpb.MsgReadIndexResp) {
		return
	}
	light := raftpb.Message{Type: messageTypePtr(m.GetType()), Context: append([]byte(nil), m.GetContext()...)}
	if m.GetType() == raftpb.MsgReadIndexResp && len(m.Entries) == 1 {
		light.Entries = []*raftpb.Entry{{Data: append([]byte(nil), m.Entries[0].GetData()...)}}
	}
	select {
	case c.events <- light:
	default:
		c.dropped.Add(1)
	}
}

func (c *asyncNativeAcceptanceCluster) wait(t *testing.T, ready func() bool, what string) {
	t.Helper()
	deadline, poll := time.NewTimer(30*time.Second), time.NewTicker(10*time.Millisecond)
	defer deadline.Stop()
	defer poll.Stop()
	for !ready() {
		select {
		case <-poll.C:
		case <-deadline.C:
			for _, node := range c.nodes {
				t.Logf("node=%d status=%+v err=%v", node.peer.NodeID, node.engine.Status(), node.engine.Err())
			}
			t.Fatalf("no actual async native progress: %s", what)
		}
	}
}
