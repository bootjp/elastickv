package peersession

import (
	"context"
	"sync"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// This test adapter models the existing first-metadata/subsequent-chunks
// envelope. Native snapshot manifests/lineage/install are the runtime owner.
func snapshotMethodSpec() Method {
	return Method{Name: pb.EtcdRaft_SendSnapshot_FullMethodName, Stream: func(b Binding) (StreamDecoders, error) {
		var mu sync.Mutex
		var route *Message
		decode := func(v any) (Message, error) {
			chunk, ok := v.(*pb.EtcdRaftSnapshotChunk)
			if !ok {
				return Message{}, ErrFormat
			}
			mu.Lock()
			defer mu.Unlock()
			if len(chunk.Metadata) != 0 {
				if route != nil {
					return Message{}, ErrFormat
				}
				var msg raftpb.Message
				if err := proto.Unmarshal(chunk.Metadata, &msg); err != nil {
					return Message{}, err
				}
				if msg.GetType() != raftpb.MsgSnap {
					return Message{}, ErrFormat
				}
				route = &Message{Group: b.Group, From: msg.GetFrom(), To: msg.GetTo(), Traffic: Replication}
			}
			if route == nil {
				return Message{}, ErrFormat
			}
			return *route, nil
		}
		return StreamDecoders{Request: decode, Reply: func(req Message, _ any) (Message, error) { req.From, req.To = req.To, req.From; return req, nil }}, nil
	}}
}

func TestNativeSnapshotFirstMetadataAndScopedReceipts(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	desc := &grpc.StreamDesc{ClientStreams: true}
	s, err := h.NewStream(ctx, cc, desc, pb.EtcdRaft_SendSnapshot_FullMethodName)
	require.NoError(t, err)
	metadata, err := proto.Marshal(&raftpb.Message{Type: raftpb.MsgSnap.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)})
	require.NoError(t, err)
	require.NoError(t, s.SendMsg(&pb.EtcdRaftSnapshotChunk{Metadata: metadata}))
	require.NoError(t, s.SendMsg(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("native chunk"), Final: true}))
	require.NoError(t, s.CloseSend())
	var ack pb.EtcdRaftAck
	require.NoError(t, s.RecvMsg(&ack))
	var receipt *Receipt
	select {
	case receipt = <-f.large.receipts:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, receipt.Check(ctx))
	require.Equal(t, Message{Group: testGroup, From: 2, To: 1, Traffic: Replication}, receipt.Message())
	// A separate stream cannot inherit the first stream's metadata/route.
	badctx, stop := context.WithCancel(ctx)
	defer stop()
	other, err := h.NewStream(badctx, cc, desc, pb.EtcdRaft_SendSnapshot_FullMethodName)
	require.NoError(t, err)
	require.Error(t, other.SendMsg(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("missing metadata")}))
	stop()
	// Bypass sender preflight too: native receiver rejects a bare first chunk.
	raw, err := cc.NewStream(ctx, desc, pb.EtcdRaft_SendSnapshot_FullMethodName, grpc.PerRPCCredentials(h))
	require.NoError(t, err)
	_ = raw.SendMsg(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("missing metadata")})
	_ = raw.CloseSend()
	require.Error(t, raw.RecvMsg(&ack))
	f.server.RevokeGroup(testGroup)
	require.Error(t, receipt.Check(ctx), "stored receipt is not permanent enqueue/install authority")
}
