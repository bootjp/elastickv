package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type testServer struct {
	pb.UnimplementedRawKVServer
	pb.UnimplementedTransactionalKVServer
	mu        sync.Mutex
	value     []byte
	writes    int
	loseReply bool
}

func (s *testServer) put(value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = bytes.Clone(value)
	s.writes++
	if s.loseReply {
		return status.Error(codes.Unavailable, "reply lost after commit")
	}
	return nil
}

func (s *testServer) get() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.value)
}

func (s *testServer) RawPut(_ context.Context, req *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	err := s.put(req.GetValue())
	return &pb.RawPutResponse{Success: err == nil}, err
}

func (s *testServer) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	err := s.put(req.GetValue())
	return &pb.PutResponse{Success: err == nil}, err
}

func (s *testServer) RawGet(context.Context, *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	v := s.get()
	return &pb.RawGetResponse{Exists: v != nil, Value: v}, nil
}

func (s *testServer) Get(context.Context, *pb.GetRequest) (*pb.GetResponse, error) {
	return &pb.GetResponse{Value: s.get()}, nil
}

func fixture(t *testing.T, api string) (rpcClient, *testServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	backend := &testServer{}
	pb.RegisterRawKVServer(server, backend)
	pb.RegisterTransactionalKVServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return rpcClient{raw: pb.NewRawKVClient(conn), txn: pb.NewTransactionalKVClient(conn), api: api}, backend
}

func TestPublicAPIsPreserveObservedOutcomes(t *testing.T) {
	for _, api := range []string{"raw", "transactional"} {
		t.Run(api, func(t *testing.T) {
			client, backend := fixture(t, api)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.Equal(t, response{Type: "ok"}, client.invoke(ctx, request{Op: "read", Key: "key"}))
			value := int64(0)
			require.Equal(t, "ok", client.invoke(ctx, request{Op: "write", Key: "key", Value: &value}).Type)
			require.Equal(t, value, client.invoke(ctx, request{Op: "read", Key: "key"}).Value)
			backend.mu.Lock()
			backend.loseReply = true
			backend.mu.Unlock()
			value = 7
			require.Equal(t, "info", client.invoke(ctx, request{Op: "write", Key: "key", Value: &value}).Type)
			require.Equal(t, value, client.invoke(ctx, request{Op: "read", Key: "key"}).Value)
			backend.mu.Lock()
			writes := backend.writes
			backend.value = []byte("corrupt")
			backend.mu.Unlock()
			require.Equal(t, 2, writes, "ambiguous writes must not be retried")
			observed := client.invoke(ctx, request{Op: "read", Key: "key"})
			require.Equal(t, "ok", observed.Type)
			require.Equal(t, "invalid integer: corrupt", observed.Value)
			backend.mu.Lock()
			backend.value = []byte{}
			backend.mu.Unlock()
			observed = client.invoke(ctx, request{Op: "read", Key: "key"})
			if api == apiRaw {
				require.Equal(t, "invalid integer: ", observed.Value, "present empty data is not a missing register")
			} else {
				// GetResponse has no exists bit; its wire contract cannot
				// distinguish an empty value from an absent key.
				require.Nil(t, observed.Value)
			}
		})
	}
}

func TestJSONStreamAndInvalidRequests(t *testing.T) {
	client, backend := fixture(t, "raw")
	var output bytes.Buffer
	err := serve(t.Context(), client, strings.NewReader("{\"op\":\"write\",\"key\":\"k\",\"value\":3}\n{\"op\":\"read\",\"key\":\"k\"}\n"), &output, time.Second)
	require.NoError(t, err)
	decoder := json.NewDecoder(&output)
	for range 2 {
		var result response
		require.NoError(t, decoder.Decode(&result))
		require.Equal(t, "ok", result.Type)
		require.Equal(t, float64(3), result.Value)
	}
	require.Equal(t, "fail", client.invoke(t.Context(), request{Op: "write", Key: "k"}).Type)
	require.Equal(t, "fail", client.invoke(t.Context(), request{Op: "unknown", Key: "k"}).Type)
	require.Error(t, serve(t.Context(), client, strings.NewReader("not json\n"), &output, time.Second))
	backend.mu.Lock()
	writes := backend.writes
	backend.mu.Unlock()
	require.Equal(t, 1, writes)
}
