package etcd

import (
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func peerDecoderWire(t *testing.T, kind raftpb.MessageType, from, to uint64) []byte {
	t.Helper()
	m := &raftpb.Message{Type: kind.Enum(), From: proto.Uint64(from), To: proto.Uint64(to)}
	if kind == raftpb.MsgSnap {
		m.Snapshot = &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(1)}}
	}
	raw, err := proto.Marshal(m)
	require.NoError(t, err)
	return raw
}

func TestPeerSessionDecoderAppliedRoleChanges(t *testing.T) {
	local := peersession.Node{Cluster: raftbootstrap.UUID{1}, Member: 1, Address: "local:9000", Key: raftbootstrap.Digest{1}}
	remote := peersession.Node{Cluster: local.Cluster, Member: 2, Address: "remote:9000", Key: raftbootstrap.Digest{2}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	owner, err := peersession.NewOwner(peersession.Config{Local: local, Versions: versions, Provisioned: []peersession.Provision{
		{Node: local, Groups: []uint64{1}}, {Node: remote, Groups: []uint64{1}},
	}})
	require.NoError(t, err)
	defer owner.Close()
	methods := PeerSessionMethodsForOwner(1, owner)
	req := &pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgApp, 1, 2)}
	_, err = methods[0].Request(req)
	require.ErrorIs(t, err, peersession.ErrDenied)
	g := peersession.Group{ID: 1, Generation: 1, Minimum: versions, State: peersession.Intact, Pending: peersession.PendingClear,
		Configuration: raftbootstrap.Configuration{State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
			{ID: 1, Address: local.Address, PublicKeyDigest: local.Key}, {ID: 2, Address: remote.Address, PublicKeyDigest: remote.Key},
		}}}
	require.NoError(t, owner.UpdateGroup(g))
	_, err = methods[0].Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgProp, 1, 2)})
	require.ErrorIs(t, err, peersession.ErrFormat, "applied voter roles do not authorize forwarded proposals")
	b := peersession.Binding{Group: 1, Local: local, Remote: remote}
	snapshot, err := methods[2].Stream(b)
	require.NoError(t, err)
	_, err = snapshot.Request(&pb.EtcdRaftSnapshotChunk{Metadata: peerDecoderWire(t, raftpb.MsgSnap, 1, 2)})
	require.NoError(t, err)
	for _, learner := range []bool{true, false, true, false} {
		g.Generation++
		g.Configuration.State = raftbootstrap.ConfState{Voters: []uint64{1, 2}}
		want := peersession.Replication
		if learner {
			g.Configuration.State = raftbootstrap.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
			want = peersession.Catchup
		}
		require.NoError(t, owner.UpdateGroup(g))
		m, err := methods[0].Request(req)
		require.NoError(t, err)
		require.Equal(t, want, m.Traffic)
		stream, err := methods[1].Stream(b)
		require.NoError(t, err)
		m, err = stream.Request(req)
		require.NoError(t, err)
		require.Equal(t, want, m.Traffic)
		m, err = snapshot.Request(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("continuation")})
		require.NoError(t, err)
		require.Equal(t, want, m.Traffic)
	}
	g.Generation++
	g.Pending = peersession.PendingUnknown
	require.NoError(t, owner.UpdateGroup(g))
	_, err = methods[0].Request(req)
	require.ErrorIs(t, err, peersession.ErrDenied)
	_, err = snapshot.Request(&pb.EtcdRaftSnapshotChunk{Final: true})
	require.ErrorIs(t, err, peersession.ErrDenied)
	owner.Close()
	_, err = methods[0].Request(req)
	require.ErrorIs(t, err, peersession.ErrDenied)
	_, err = PeerSessionMethodsForOwner(1, nil)[0].Request(req)
	require.ErrorIs(t, err, peersession.ErrDenied)
}

func TestPeerSessionDecoderUnary(t *testing.T) {
	learners := []uint64{3}
	methods := PeerSessionMethods(0, learners)
	learners[0] = 2
	m := methods[0]
	for _, kind := range []raftpb.MessageType{raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp, raftpb.MsgApp, raftpb.MsgAppResp, raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp, raftpb.MsgSnap, raftpb.MsgTransferLeader, raftpb.MsgTimeoutNow, raftpb.MsgReadIndex, raftpb.MsgReadIndexResp} {
		route, err := m.Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, kind, 1, 2)})
		require.NoError(t, err)
		require.Equal(t, uint64(0), route.Group)
		want, err := peerRaftTraffic(kind)
		require.NoError(t, err)
		require.Equal(t, want, route.Traffic)
		ack, err := m.Reply(route, &pb.EtcdRaftAck{})
		require.NoError(t, err)
		require.Equal(t, uint64(2), ack.From)
		require.Equal(t, uint64(1), ack.To)
	}
	route, err := m.Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgApp, 1, 3)})
	require.NoError(t, err)
	require.Equal(t, peersession.Catchup, route.Traffic)
	valid := peerDecoderWire(t, raftpb.MsgApp, 1, 2)
	bad := [][]byte{nil, {0xff}, append(append([]byte{}, valid...), 0), append(append([]byte{}, valid...), 0xf8, 0x07, 1), append(append([]byte{}, valid...), valid...), peerDecoderWire(t, raftpb.MsgHup, 1, 2), peerDecoderWire(t, raftpb.MessageType(999), 1, 2), peerDecoderWire(t, raftpb.MsgApp, 0, 2), peerDecoderWire(t, raftpb.MsgApp, 1, 1)}
	for _, raw := range bad {
		_, err := m.Request(&pb.EtcdRaftMessage{Message: raw})
		require.Error(t, err)
	}
	_, err = m.Request((*pb.EtcdRaftMessage)(nil))
	require.Error(t, err)
	ack := &pb.EtcdRaftAck{}
	ack.ProtoReflect().SetUnknown([]byte{8, 1})
	_, err = m.Reply(route, ack)
	require.Error(t, err)
}

func TestPeerSessionDecoderStreams(t *testing.T) {
	methods := PeerSessionMethods(1, nil)
	b := peersession.Binding{Group: 1, Local: peersession.Node{Member: 1}, Remote: peersession.Node{Member: 2}}
	for _, method := range methods[1:] {
		s, err := method.Stream(b)
		require.NoError(t, err)
		_, err = s.Reply(peersession.Message{}, &pb.EtcdRaftAck{})
		require.Error(t, err)
	}
	s, err := methods[1].Stream(b)
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgApp, 9, 2)})
	require.Error(t, err)
	_, err = s.Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgApp, 1, 2)})
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgApp, 2, 1)})
	require.Error(t, err)
	_, err = s.Reply(peersession.Message{}, &pb.EtcdRaftAck{})
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgApp, 1, 2)})
	require.Error(t, err)

	factory := methods[2].Stream
	s, err = factory(b)
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("no metadata")})
	require.Error(t, err)
	header := peerDecoderWire(t, raftpb.MsgSnap, 1, 2)
	route, err := s.Request(&pb.EtcdRaftSnapshotChunk{Metadata: header})
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftSnapshotChunk{Metadata: header})
	require.Error(t, err)
	_, err = s.Request(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("payload"), Final: true})
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftSnapshotChunk{Final: true})
	require.Error(t, err)
	ack, err := s.Reply(peersession.Message{}, &pb.EtcdRaftAck{})
	require.NoError(t, err)
	require.Equal(t, route.To, ack.From)
	other, err := factory(b)
	require.NoError(t, err)
	_, err = other.Request(&pb.EtcdRaftSnapshotChunk{Final: true})
	require.Error(t, err)
	_, err = other.Request(&pb.EtcdRaftSnapshotChunk{Metadata: peerDecoderWire(t, raftpb.MsgSnap, 9, 2)})
	require.Error(t, err)
	_, err = factory(peersession.Binding{Group: 2})
	require.Error(t, err)
}

func TestPeerSessionDecoderConcurrentDirections(t *testing.T) {
	b := peersession.Binding{Group: 1, Local: peersession.Node{Member: 1}, Remote: peersession.Node{Member: 2}}
	s, err := PeerSessionMethods(1, nil)[1].Stream(b)
	require.NoError(t, err)
	raw := peerDecoderWire(t, raftpb.MsgApp, 1, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = s.Request(&pb.EtcdRaftMessage{Message: raw})
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = s.Reply(peersession.Message{}, &pb.EtcdRaftAck{})
		}
	}()
	wg.Wait()
}

func TestPeerSessionDecoderSnapshotConcurrentAndMalformed(t *testing.T) {
	b := peersession.Binding{Group: 1, Local: peersession.Node{Member: 1}, Remote: peersession.Node{Member: 2}}
	factory := PeerSessionMethods(1, nil)[2].Stream
	for _, kind := range []raftpb.MessageType{raftpb.MsgApp, raftpb.MsgSnap} {
		raw := peerDecoderWire(t, kind, 1, 2)
		var header raftpb.Message
		require.NoError(t, proto.Unmarshal(raw, &header))
		if kind == raftpb.MsgSnap {
			header.Snapshot.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
		}
		raw, err := proto.Marshal(&header)
		require.NoError(t, err)
		s, err := factory(b)
		require.NoError(t, err)
		_, err = s.Request(&pb.EtcdRaftSnapshotChunk{Metadata: raw})
		require.Error(t, err)
	}
	s, err := factory(b)
	require.NoError(t, err)
	_, err = s.Request(&pb.EtcdRaftSnapshotChunk{Metadata: peerDecoderWire(t, raftpb.MsgSnap, 1, 2)})
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = s.Request(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("data")})
		}
		_, _ = s.Request(&pb.EtcdRaftSnapshotChunk{Final: true})
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = s.Reply(peersession.Message{}, &pb.EtcdRaftAck{})
		}
	}()
	wg.Wait()
}
