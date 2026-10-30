package kv

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func largeClaimRequest(size int) *pb.Request {
	group := &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("large-raw"), Value: bytes.Repeat([]byte{'v'}, size)}}}
	return rawClaimRequestForGroup(group, 1)
}

func legacyLargeClaimPut(t *testing.T, request *pb.Request) {
	t.Helper()
	request.Ts = 10
	encoded, err := marshalRaftCommand([]*pb.Request{request})
	require.NoError(t, err)
	st := store.NewMVCCStore()
	defer st.Close()
	f := NewKvFSMWithHLC(st, NewHLC())
	require.Nil(t, f.Apply(encoded))
	got, err := st.GetAt(t.Context(), request.Mutations[0].Key, request.Ts)
	require.NoError(t, err)
	require.True(t, bytes.Equal(request.Mutations[0].Value, got))
	t.Logf("legacy value=%d request=%d raft=%d", len(got), proto.Size(request), len(encoded))
	request.Ts = 0
}

func TestTSOClaimLarge33MiB(t *testing.T) {
	request := largeClaimRequest(33 << 20)
	legacyLargeClaimPut(t, request)
	runtime.GC()
	_, allocator, _ := newProductionClaimFixture(t)
	assert.NoError(t, EnsureRequestTimestampIntent(request))
	assert.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	// The cross-group builder must also hash actual bodies rather than copying
	// values into a flat root plus every participant manifest.
	first := &pb.Request{TimestampClaimGroupId: 1, Mutations: request.Mutations}
	second := &pb.Request{TimestampClaimGroupId: 2, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("other"), Value: []byte("small")}}}
	_, err := bindClaimToParticipantRequests(t.Context(), allocator, []*pb.Request{first, second}, &OperationGroup[OP]{})
	require.NoError(t, err)
	require.Less(t, proto.Size(first.TimestampIntent), 1024)
	for _, participant := range []*pb.Request{first, second} {
		require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, participant))
	}
}

type largeClaimForwardServer struct{ pb.UnimplementedInternalServer }

func (*largeClaimForwardServer) Forward(context.Context, *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	return &pb.ForwardResponse{Success: true}, nil
}

func TestTSOClaimLarge22MiB(t *testing.T) {
	request := largeClaimRequest(22 << 20)
	legacyLargeClaimPut(t, request)
	runtime.GC()
	_, allocator, _ := newProductionClaimFixture(t)
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	require.NoError(t, StampTimestampAdmission(t.Context(), allocator, nil, []*pb.Request{request}, 1))
	forward := (&Coordinate{}).toForwardRequest([]*pb.Request{request})
	t.Logf("value=%d intent=%d request=%d forward=%d", len(request.Mutations[0].Value), proto.Size(request.TimestampIntent), proto.Size(request), proto.Size(forward))
	_, marshalErr := marshalRaftCommand([]*pb.Request{request})
	assert.NoError(t, marshalErr)
	runtime.GC()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(internalutil.GRPCServerOptions()...)
	pb.RegisterInternalServer(server, &largeClaimForwardServer{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	conn, err := grpc.NewClient(listener.Addr().String(), internalutil.GRPCDialOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client := pb.NewInternalClient(conn)
	_, err = client.Forward(ctx, &pb.ForwardRequest{Requests: []*pb.Request{{Mutations: request.Mutations, Ts: request.Ts}}})
	require.NoError(t, err)
	_, err = client.Forward(ctx, forward)
	assert.NoError(t, err)
}
