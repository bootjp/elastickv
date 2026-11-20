package kv

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVersionCASForwardRejectsNilRequests(t *testing.T) {
	proxy := NewLeaderProxyWithEngine(newStubFollowerEngine(""), WithVersionCASAdmission(func(ctx context.Context) (context.Context, error) { return ctx, nil }))
	req := onePhaseReq(0, 0, 0, []byte("switch"), []byte("off"))
	req.Mutations[1].Op = pb.Op_COMPARE_AND_SWAP
	req.Mutations[1].ExpectedCommitTs = 10
	for _, requests := range [][]*pb.Request{{nil, req}, {req, nil}, {nil}} {
		_, err := proxy.Commit(t.Context(), requests)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
	require.Zero(t, proxy.forwardSeq.Load())
}

func TestVersionCASForwardToLegacyLeader(t *testing.T) {
	legacy := &fakeInternal{resp: &pb.ForwardResponse{Success: true, CommitIndex: 15, CommitTs: 20}}
	var dedicatedCalls atomic.Int32
	addr := grpctest.Serve(t, func(s *grpc.Server) {
		desc := pb.Internal_ServiceDesc
		desc.Methods = nil
		for _, method := range pb.Internal_ServiceDesc.Methods {
			if method.MethodName != "ForwardCompareAndSwap" {
				desc.Methods = append(desc.Methods, method)
			}
		}
		s.RegisterService(&desc, legacy)
	}, grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if method == pb.Internal_ForwardCompareAndSwap_FullMethodName {
			dedicatedCalls.Add(1)
		}
		return status.Error(codes.Unimplemented, "unknown method")
	}))
	proxy := NewLeaderProxyWithEngine(newStubFollowerEngine(addr), WithVersionCASAdmission(func(ctx context.Context) (context.Context, error) { return ctx, nil }))
	t.Cleanup(func() { _ = proxy.connCache.Close() })
	req := onePhaseReq(0, 0, 0, []byte("switch"), []byte("off"))
	req.Mutations[1].Op = pb.Op_COMPARE_AND_SWAP
	req.Mutations[1].ExpectedCommitTs = 10

	_, err := proxy.Commit(t.Context(), []*pb.Request{req})
	require.ErrorIs(t, err, ErrVersionCASUnavailable)
	require.EqualValues(t, 1, dedicatedCalls.Load())
	legacy.mu.Lock()
	defer legacy.mu.Unlock()
	require.Zero(t, legacy.calls, "legacy Forward would append an unframed operation")
}
