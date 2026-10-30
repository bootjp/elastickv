package peersession

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const publicEchoMethod = "/test.Public/Echo"

type largeService struct {
	pb.UnimplementedEtcdRaftServer
	owner    *Owner
	receipts chan *Receipt
}

func (*largeService) Send(_ context.Context, req *pb.EtcdRaftMessage) (*pb.EtcdRaftAck, error) {
	return &pb.EtcdRaftAck{}, nil
}
func largeRaftMethod() Method {
	return Method{Name: pb.EtcdRaft_Send_FullMethodName, Request: decodeRaft, Reply: func(req Message, _ any) (Message, error) { req.From, req.To = req.To, req.From; return req, nil }}
}
func decodeRaft(v any) (Message, error) {
	req, ok := v.(*pb.EtcdRaftMessage)
	if !ok {
		return Message{}, ErrFormat
	}
	var msg raftpb.Message
	if err := proto.Unmarshal(req.Message, &msg); err != nil {
		return Message{}, err
	}
	if len(msg.ProtoReflect().GetUnknown()) > 0 {
		return Message{}, ErrFormat
	}
	traffic := Replication
	if msg.GetType() == raftpb.MsgVote || msg.GetType() == raftpb.MsgPreVote || msg.GetType() == raftpb.MsgVoteResp || msg.GetType() == raftpb.MsgPreVoteResp {
		traffic = Vote
	}
	return Message{Group: testGroup, From: msg.GetFrom(), To: msg.GetTo(), Traffic: traffic}, nil
}
func registerLargeServices(s *grpc.Server, owner *Owner) *largeService {
	service := &largeService{owner: owner, receipts: make(chan *Receipt, 16)}
	pb.RegisterEtcdRaftServer(s, service)
	s.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Public", HandlerType: (*testPeer)(nil), Methods: []grpc.MethodDesc{{MethodName: "Echo", Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := &wrapperspb.BytesValue{}
		if err := decode(req); err != nil {
			return nil, err
		}
		return interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: publicEchoMethod}, func(context.Context, any) (any, error) { return req, nil })
	}}}}, &nativeService{})
	return service
}

func (s *largeService) SendSnapshot(stream pb.EtcdRaft_SendSnapshotServer) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&pb.EtcdRaftAck{})
		}
		if err != nil {
			return err
		}
		r, err := s.owner.Received(stream.Context())
		if err != nil {
			return err
		}
		s.receipts <- r
	}
}
func TestNativePublicAndRaftKeep64MiBContract(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	require.Equal(t, 64<<20, TransportMaxMessageSize)
	// Existing public requests AND replies must remain usable above 16 MiB.
	public := &wrapperspb.BytesValue{Value: bytes.Repeat([]byte{0x4a}, 20<<20)}
	var echo wrapperspb.BytesValue
	require.NoError(t, cc.Invoke(ctx, publicEchoMethod, public, &echo, grpc.MaxCallRecvMsgSize(TransportMaxMessageSize), grpc.MaxCallSendMsgSize(TransportMaxMessageSize)))
	require.Equal(t, public.Value, echo.Value)
	// Use the EXISTING EtcdRaft protobuf service/envelope and a real decoded
	// MsgApp, not the new control Frame, for the forwarding-size regression.
	message := &raftpb.Message{Type: raftpb.MsgApp.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(4), Entries: []*raftpb.Entry{{Data: public.Value}}}
	data, err := proto.Marshal(message)
	require.NoError(t, err)
	var ack pb.EtcdRaftAck
	require.NoError(t, h.Invoke(ctx, cc, pb.EtcdRaft_Send_FullMethodName, &pb.EtcdRaftMessage{Message: data}, &ack))
	// A 64 MiB PAYLOAD plus protobuf envelope still exceeds the old 64 MiB
	// transport budget and remains rejected (the cap was not removed).
	tooLarge := &wrapperspb.BytesValue{Value: make([]byte, TransportMaxMessageSize)}
	require.Error(t, cc.Invoke(ctx, publicEchoMethod, tooLarge, &echo, grpc.MaxCallSendMsgSize(TransportMaxMessageSize+128)))
}

type rawCodec struct{ data []byte }

func (rawCodec) Name() string                    { return "proto" }
func (r rawCodec) Marshal(any) ([]byte, error)   { return r.data, nil }
func (rawCodec) Unmarshal(b []byte, v any) error { return (StrictCodec{}).Unmarshal(b, v) }

func TestNativeOversizedNewFrameRejectedBeforeRecordDecode(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	encoded := protowire.AppendBytes([]byte{0x0a}, make([]byte, MaxControlMessageSize+1))
	var reply wire.Frame
	err := cc.Invoke(ctx, wire.PeerSession_Begin_FullMethodName, &wire.Frame{}, &reply, grpc.ForceCodec(rawCodec{data: encoded}), grpc.MaxCallSendMsgSize(TransportMaxMessageSize))
	require.Error(t, err)
	require.Zero(t, f.server.Counts().Pending)
	require.Zero(t, f.server.Counts().Active)
	// The already-received gRPC wire buffer may be up to 64 MiB. Frame-specific
	// rejection occurs BEFORE frame.Reset/Body clone or nested record decoding.
	before := []byte("unchanged")
	out := &wire.Frame{Body: before}
	require.Error(t, (StrictCodec{}).Unmarshal(encoded, out))
	require.Same(t, &before[0], &out.Body[0])
	allocations := testing.AllocsPerRun(5, func() {
		if (StrictCodec{}).Unmarshal(encoded, out) == nil {
			panic("oversize accepted")
		}
	})
	require.Zero(t, allocations)
}

func TestStrictControlEnvelopeRejectsDuplicateUnknownAndNoncanonical(t *testing.T) {
	valid, err := (StrictCodec{}).Marshal(request(Probe))
	require.NoError(t, err)
	for _, bad := range [][]byte{
		append(bytes.Clone(valid), valid...),
		append(bytes.Clone(valid), 0x10, 0x01),
		{0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f},
		{0x0a, 0x81, 0x00, 0x01},
		{0x8a, 0x00, 0x01, 0x01},
		append(bytes.Clone(valid), 0x00),
	} {
		var frame wire.Frame
		require.Error(t, (StrictCodec{}).Unmarshal(bad, &frame))
	}
	for n := 0; n < len(valid); n++ {
		var frame wire.Frame
		require.Error(t, (StrictCodec{}).Unmarshal(valid[:n], &frame))
	}
}
