package grpctest_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	grpc_testing "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

type echoServer struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (echoServer) UnaryCall(_ context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{Payload: req.Payload}, nil
}

func TestServeDialProductionLimitsAndCleanup(t *testing.T) {
	var conn *grpc.ClientConn
	t.Run("owner", func(t *testing.T) {
		addr := grpctest.Serve(t, func(s *grpc.Server) { grpc_testing.RegisterTestServiceServer(s, echoServer{}) })
		conn = grpctest.Dial(t, addr)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Exceeds gRPC's default 4 MiB receive limit on both client and server.
		body := bytes.Repeat([]byte("x"), 5<<20)
		response, err := grpc_testing.NewTestServiceClient(conn).UnaryCall(ctx, &grpc_testing.SimpleRequest{
			Payload: &grpc_testing.Payload{Body: body},
		})
		require.NoError(t, err)
		require.Equal(t, body, response.GetPayload().GetBody())
	})
	require.Equal(t, connectivity.Shutdown, conn.GetState())
}

func TestServeExplicitOptionsOverrideDefaults(t *testing.T) {
	addr := grpctest.Serve(t, func(s *grpc.Server) { grpc_testing.RegisterTestServiceServer(s, echoServer{}) }, grpc.MaxRecvMsgSize(1))
	conn := grpctest.Dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := grpc_testing.NewTestServiceClient(conn).UnaryCall(ctx, &grpc_testing.SimpleRequest{
		Payload: &grpc_testing.Payload{Body: []byte("too large")},
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}
