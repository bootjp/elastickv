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
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
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

	require.Equal(t, uint64(5), engine.Status().AppliedIndex)
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

func TestApplyReadySnapshotAdvancesAppliedIndex(t *testing.T) {
	engine := &Engine{
		storage: etcdraft.NewMemoryStorage(),
		fsm:     &testStateMachine{},
	}

	err := engine.applyReadySnapshot(raftpb.Snapshot{
		Data: mustEncodeSnapshotData(t, [][]byte{[]byte("snap")}),
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
	t.Cleanup(func() {
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
	})

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

func openTransportTestNodes(ctx context.Context, nodes []*transportTestNode, peers []Peer) error {
	var eg errgroup.Group
	for _, node := range nodes {
		eg.Go(func() error {
			engine, err := Open(ctx, OpenConfig{
				NodeID:       node.peer.NodeID,
				LocalID:      node.peer.ID,
				LocalAddress: node.peer.Address,
				DataDir:      node.dir,
				Peers:        peers,
				Bootstrap:    true,
				Transport:    node.transport,
				StateMachine: node.fsm,
			})
			if err != nil {
				return err
			}
			node.engine = engine
			return nil
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
