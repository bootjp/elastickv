package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestRemoveServerDeliversCommitAfterPeerTeardown(t *testing.T) {
	for _, stream := range []string{"0", "1"} {
		for _, lanes := range []string{"0", "1"} {
			t.Run("stream="+stream+"/lanes="+lanes, func(t *testing.T) {
				t.Setenv(sendStreamEnabledEnvVar, stream)
				t.Setenv(dispatcherLanesEnvVar, lanes)
				testRemovalCommitAfterPeerTeardown(t)
			})
		}
	}
}

func testRemovalCommitAfterPeerTeardown(t *testing.T) {
	t.Helper()
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))
	addIndex, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, nodes[1].engine, 2)

	// Hold receiver admission until local removal has completed. Teardown of
	// the ordinary dispatch connection must not cancel this last notification.
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	nodes[1].transport.SetHandler(func(ctx context.Context, msg raftpb.Message) error {
		if msg.GetType() == raftpb.MsgApp && msg.GetCommit() > addIndex {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nodes[1].engine.handleTransportMessage(ctx, msg)
	})
	removeIndex, err := leader.engine.RemoveServer(ctx, nodes[1].peer.ID, addIndex)
	require.NoError(t, err)
	require.Greater(t, removeIndex, addIndex)
	waitForConfigSize(t, leader.engine, 1)
	testutil.Closed(t, entered, 5*time.Second, "final commit notification did not reach the removed peer")
	unblock()
	require.Eventually(t, func() bool {
		return nodes[1].engine.State() == raftengine.StateShutdown
	}, 5*time.Second, 10*time.Millisecond)
	cfg, err := nodes[1].engine.Configuration(context.Background())
	require.NoError(t, err)
	require.Len(t, cfg.Servers, 1)
	require.Equal(t, nodes[0].peer.ID, cfg.Servers[0].ID)
}

func TestCommittedRemovalIndexes(t *testing.T) {
	remove, err := proto.Marshal(&raftpb.ConfChange{
		Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: uint64Ptr(2),
	})
	require.NoError(t, err)
	add, err := proto.Marshal(&raftpb.ConfChange{
		Type: confChangeTypePtr(raftpb.ConfChangeAddNode), NodeId: uint64Ptr(3),
	})
	require.NoError(t, err)
	joint, err := proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{
		{Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: uint64Ptr(2)},
		{Type: confChangeTypePtr(raftpb.ConfChangeAddNode), NodeId: uint64Ptr(3)},
		{Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: uint64Ptr(4)},
	}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		typeID  raftpb.EntryType
		data    []byte
		want    map[uint64]uint64
		wantErr bool
	}{
		{name: "normal", typeID: raftpb.EntryNormal, data: remove},
		{name: "remove", typeID: raftpb.EntryConfChange, data: remove, want: map[uint64]uint64{2: 10}},
		{name: "add", typeID: raftpb.EntryConfChange, data: add, want: map[uint64]uint64{}},
		{name: "joint", typeID: raftpb.EntryConfChangeV2, data: joint, want: map[uint64]uint64{2: 10, 4: 10}},
		{name: "invalid", typeID: raftpb.EntryConfChange, data: []byte{0xff}, wantErr: true},
		{name: "invalid joint", typeID: raftpb.EntryConfChangeV2, data: []byte{0xff}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := committedRemovalIndexes([]*raftpb.Entry{{
				Type: entryTypePtr(tc.typeID), Index: uint64Ptr(10), Data: tc.data,
			}})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRemovalNotificationPreservesReplacementConnection(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	nodes[1].transport.SetHandler(func(ctx context.Context, _ raftpb.Message) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	engine := &Engine{nodeID: 1, transport: nodes[0].transport, dispatchCtx: ctx}
	engine.startRemovalNotification(removalNotification{peer: peers[1], messages: []raftpb.Message{{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2), Commit: uint64Ptr(4),
	}}})
	requireSignal(t, entered, time.Second, "notification did not reach original peer")
	nodes[0].transport.RemovePeer(2)
	replacement := Peer{NodeID: 2, ID: "replacement", Address: peers[0].Address}
	nodes[0].transport.UpsertPeer(replacement)
	nodes[0].transport.SetHandler(func(context.Context, raftpb.Message) error { return nil })
	client, err := nodes[0].transport.clientForPeer(replacement)
	require.NoError(t, err)
	unblock()
	finished := make(chan struct{})
	go func() { engine.dispatchWG.Wait(); close(finished) }()
	requireSignal(t, finished, time.Second, "notification did not finish after admission")
	peer, err := nodes[0].transport.peerFor(2)
	require.NoError(t, err)
	require.Equal(t, replacement, peer)
	after, err := nodes[0].transport.clientForPeer(replacement)
	require.NoError(t, err)
	require.Same(t, client, after)
	require.NoError(t, nodes[0].transport.Dispatch(ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgHeartbeat), From: uint64Ptr(1), To: uint64Ptr(2),
	}))
}

func TestStopDispatchWorkersCancelsRemovalNotification(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 1)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })
	entered := make(chan struct{})
	nodes[0].transport.SetHandler(func(ctx context.Context, _ raftpb.Message) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	engine := &Engine{dispatchCtx: ctx, dispatchCancel: cancel, dispatchStopCh: make(chan struct{})}
	engine.startRemovalNotification(removalNotification{peer: peers[0], messages: []raftpb.Message{{
		Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(1),
	}}})
	requireSignal(t, entered, time.Second, "notification did not reach receiver")
	finished := make(chan struct{})
	go func() { engine.stopDispatchWorkers(); close(finished) }()
	requireSignal(t, finished, time.Second, "shutdown did not cancel the blocked notification")
}

func TestRemovalNotificationHonorsDeadlineForUnreachablePeer(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 1)
	require.NoError(t, nodes[0].lis.Close())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	err := dispatchRemovalNotification(ctx, removalNotification{peer: peers[0], messages: []raftpb.Message{{
		Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(1),
	}}})
	require.Equal(t, codes.DeadlineExceeded, grpcStatusCode(err))
}
