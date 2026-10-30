package etcd

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const envelopeNativeRaftKeyID uint32 = 2

// Already-active encryption, not a simulation of the cutover admin protocol.
type envelopeNativeCutover struct{}

func (envelopeNativeCutover) RaftEnvelopeCutoverIndex() uint64 { return 1 }

// Reuse the real Save/Sync gate, but detect small encrypted normal entries.
type envelopeNativeDisk struct{ *asyncNativeAcceptanceDisk }

func (d *envelopeNativeDisk) Save(hs *raftpb.HardState, entries []*raftpb.Entry) error {
	if err := d.Storage.Save(hs, entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, _, ok := decodeProposalEnvelope(entry.GetData()); ok && entry.GetType() == raftpb.EntryNormal {
			d.pending = entry.GetIndex()
			d.target.Store(d.pending)
		}
	}
	return nil
}

type envelopeNativeCluster struct {
	*asyncNativeAcceptanceCluster
	cipher *encryption.Cipher
	group  *kv.ShardGroup
	proxy  *kv.LeaderProxy
	wrap   kv.RaftPayloadWrapper
	fatal  bool
}

func newEnvelopeNativeCipher(t *testing.T) *encryption.Cipher {
	t.Helper()
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(1, bytes.Repeat([]byte{0x51}, encryption.KeySize)))
	require.NoError(t, keys.Set(envelopeNativeRaftKeyID, bytes.Repeat([]byte{0x72}, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	return cipher
}

func newEnvelopeNativeFSM(t *testing.T, cipher *encryption.Cipher, node, epoch uint16, observer *asyncNativeEncryptionObserver) (store.MVCCStore, *asyncNativeAcceptanceFSM) {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir(),
		store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(node, epoch), func() (uint32, bool) { return 1, true }),
		store.WithStorageEnvelopeGate(func() bool { return true }),
		store.WithStorageEnvelopeV2Gate(func() bool { return true }),
		store.WithEncryptionObserver(observer))
	require.NoError(t, err)
	machine, ok := kv.NewKvFSMWithHLC(st, nil, kv.WithCutoverSource(envelopeNativeCutover{})).(pipelineNativeConfigurationStateMachine)
	require.True(t, ok)
	return st, &asyncNativeAcceptanceFSM{pipelineNativeConfigurationStateMachine: machine}
}

func newEnvelopeNativeCluster(t *testing.T, missingCipher bool) *envelopeNativeCluster {
	t.Helper()
	nodes, peers := newTransportTestNodes(t, 3)
	startTransportTestServers(nodes, peers)
	ctx, cancel := context.WithCancel(context.Background())
	c := &envelopeNativeCluster{cipher: newEnvelopeNativeCipher(t), asyncNativeAcceptanceCluster: &asyncNativeAcceptanceCluster{
		nodes: nodes, peers: peers, ctx: ctx, cancel: cancel, events: make(chan raftpb.Message, 4096),
		stores: make([]store.MVCCStore, 3), fsms: make([]*asyncNativeAcceptanceFSM, 3),
		disks: make([]*asyncNativeAcceptanceDisk, 3), observers: make([]*asyncNativeEncryptionObserver, 3),
	}}
	t.Cleanup(func() { c.close(t) })
	for i, node := range nodes {
		observer := &asyncNativeEncryptionObserver{}
		st, fsm := newEnvelopeNativeFSM(t, c.cipher, uint16(i+1), 1, observer)
		disk := &asyncNativeAcceptanceDisk{entered: make(chan uint64, 1), release: make(chan struct{})}
		c.stores[i], c.fsms[i], c.disks[i], c.observers[i] = st, fsm, disk, observer
		cipher := c.cipher
		if missingCipher {
			cipher = nil
		}
		node.engine = openEnvelopeNativeEngine(t, node, peers, fsm, &envelopeNativeDisk{disk}, true, cipher, ctx, c.dispatch(node))
		testutil.Closed(t, node.engine.startedCh, 30*time.Second, "envelope-native startup")
	}
	require.NoError(t, nodes[0].engine.handleTransportMessage(ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgTimeoutNow), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(1),
	}))
	c.wait(t, func() bool { return nodes[0].engine.State() == raftengine.StateLeader }, "envelope-native voter election")
	nonces := encryption.NewDeterministicNonceFactory(1, 1)
	c.wrap = func(payload []byte) ([]byte, error) {
		nonce, err := nonces.Next()
		if err != nil {
			return nil, err
		}
		return encryption.WrapRaftPayload(c.cipher, envelopeNativeRaftKeyID, nonce[:], payload)
	}
	c.group = &kv.ShardGroup{Engine: nodes[0].engine, Store: c.stores[0]}
	c.group.SetRaftPayloadWrap(c.wrap)
	c.proxy = kv.NewLeaderProxyForShardGroup(c.group)
	return c
}

// All encryption references are installed before any owner/actor starts.
func openEnvelopeNativeEngine(t *testing.T, node *transportTestNode, peers []Peer, fsm *asyncNativeAcceptanceFSM,
	disk *envelopeNativeDisk, bootstrap bool, cipher *encryption.Cipher, parent context.Context, dispatch func(context.Context, dispatchRequest) error,
) *Engine {
	t.Helper()
	fsm.checkpoint.Store(bootstrap)
	prepared, err := prepareOpenState(OpenConfig{NodeID: node.peer.NodeID, LocalID: node.peer.ID, LocalAddress: node.peer.Address,
		DataDir: node.dir, Peers: peers, Bootstrap: bootstrap, StateMachine: fsm, Transport: node.transport,
		RaftCipher: cipher, RaftCutoverIndex: envelopeNativeCutover{}.RaftEnvelopeCutoverIndex})
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
		raftCipher: prepared.cfg.RaftCipher, raftCutoverIndex: orInertCutover(prepared.cfg.RaftCutoverIndex),
		applied: coldStartApplied(prepared.disk), voterCount: len(peerMap),
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

func closeEnvelopeNativeEngine(t *testing.T, e *Engine, fatal bool) {
	t.Helper()
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	err := testutil.Recv(t, closed, time.Second, "envelope-native engine Close did not join")
	if fatal && err != nil {
		require.True(t, errors.Is(err, ErrRaftUnwrapFailed), "unexpected close failure: %v", err)
	} else {
		require.NoError(t, err)
	}
}

func (c *envelopeNativeCluster) close(t *testing.T) {
	t.Helper()
	for _, disk := range c.disks {
		if disk != nil {
			disk.unblock()
		}
	}
	for _, node := range c.nodes {
		if node.engine != nil {
			closeEnvelopeNativeEngine(t, node.engine, c.fatal)
		}
		if node.server != nil {
			node.server.Stop()
		}
		if node.lis != nil {
			_ = node.lis.Close()
		}
	}
	c.cancel()
	joined := make(chan struct{})
	go func() { c.proposals.Wait(); close(joined) }()
	testutil.Closed(t, joined, time.Second, "envelope-native proposal callers did not join")
	for _, st := range c.stores {
		if st != nil {
			require.NoError(t, st.Close())
		}
	}
}

func (c *envelopeNativeCluster) write(key, value []byte, timestamp uint64) <-chan asyncNativeAcceptanceWrite {
	done := make(chan asyncNativeAcceptanceWrite, 1)
	c.proposals.Add(1)
	go func() {
		defer c.proposals.Done()
		ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
		defer cancel()
		response, err := c.proxy.Commit(ctx, []*pb.Request{{Ts: timestamp,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: value}},
		}})
		done <- asyncNativeAcceptanceWrite{response: response, err: err}
	}()
	return done
}
