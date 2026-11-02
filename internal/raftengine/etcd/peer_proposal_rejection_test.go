package etcd

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPeerProposalRejectedBeforeNativeDelivery(t *testing.T) {
	transport := NewGRPCTransport(nil)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	delivered := false
	transport.SetHandler(func(context.Context, raftpb.Message) error { delivered = true; return nil })
	for _, entries := range [][]*raftpb.Entry{nil, {{Data: []byte("proposal")}}} {
		message := raftpb.Message{Type: raftpb.MsgProp.Enum(), From: proto.Uint64(1), To: proto.Uint64(2), Entries: entries}
		raw, err := proto.Marshal(&message)
		require.NoError(t, err)
		_, err = PeerSessionMethods(1, nil)[0].Request(&pb.EtcdRaftMessage{Message: raw})
		require.ErrorIs(t, err, peersession.ErrFormat)
		_, err = transport.Send(t.Context(), &pb.EtcdRaftMessage{Message: raw})
		require.ErrorIs(t, err, errPeerProposalForbidden)
		require.False(t, delivered)
		engine := &Engine{}
		require.ErrorIs(t, engine.handleTransportMessage(t.Context(), message), errPeerProposalForbidden)
	}
}
