package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type boundTimestampIssuer struct{ calls atomic.Int32 }

func (i *boundTimestampIssuer) NextAfter(_ context.Context, min uint64) (uint64, error) {
	i.calls.Add(1)
	return min + 1, nil
}

func (i *boundTimestampIssuer) Next(ctx context.Context) (uint64, error) {
	return i.NextAfter(ctx, 0)
}

func boundTimestampClient(t *testing.T, server *Internal) pb.InternalClient {
	t.Helper()
	fixture := httptest.NewTLSServer(nil)
	defer fixture.Close()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: fixture.TLS.Certificates,
	})))
	pb.RegisterInternalServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	roots := x509.NewCertPool()
	roots.AddCert(fixture.Certificate())
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots,
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return pb.NewInternalClient(conn)
}

func boundTimestampContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer timestamp-admission-token")
}

func TestIssueBoundTimestampRequiresTLSCredentialAndGroupZero(t *testing.T) {
	issuer := &boundTimestampIssuer{}
	leader := &prepareSafetyRPCLeader{}
	server := NewInternalWithEngine(nil, leader, nil, nil,
		WithKVForwardRejected(), WithInternalTimestampAdmission("timestamp-admission-token", issuer))
	client := boundTimestampClient(t, server)
	ctx := boundTimestampContext(t)
	unauthorized, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err := client.IssueBoundTimestamp(unauthorized, &pb.IssueBoundTimestampRequest{MinTimestamp: 10})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = server.IssueBoundTimestamp(metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer timestamp-admission-token")),
		&pb.IssueBoundTimestampRequest{MinTimestamp: 10})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "a bearer header without native TLS is not authority")
	response, err := client.IssueBoundTimestamp(ctx, &pb.IssueBoundTimestampRequest{MinTimestamp: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(11), response.GetTimestamp())
	require.Equal(t, int32(1), issuer.calls.Load())
	leader.follower.Store(true)
	_, err = client.IssueBoundTimestamp(ctx, &pb.IssueBoundTimestampRequest{MinTimestamp: 11})
	require.Error(t, err)
	require.Equal(t, int32(1), issuer.calls.Load())
	leader.follower.Store(false)
	dataClient := boundTimestampClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalTimestampAdmission("timestamp-admission-token", issuer)))
	_, err = dataClient.IssueBoundTimestamp(ctx, &pb.IssueBoundTimestampRequest{MinTimestamp: 11})
	require.Error(t, err)
	require.Equal(t, int32(1), issuer.calls.Load())
}

func TestTimestampBindingRPCsDisabledWithoutRolloutWiring(t *testing.T) {
	leader := &prepareSafetyRPCLeader{}
	client := boundTimestampClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithKVForwardRejected(), WithInternalStore(store.NewMVCCStore())))
	ctx := boundTimestampContext(t)
	_, err := client.IssueBoundTimestamp(ctx, &pb.IssueBoundTimestampRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = client.LookupTimestampBinding(ctx, &pb.LookupTimestampBindingRequest{OperationId: append([]byte{1}, make([]byte, 15)...)})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestForwardRejectsBoundCreationBeforeTimestampWork(t *testing.T) {
	issuer := &boundTimestampIssuer{}
	server := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithInternalTimestampAllocator(issuer))
	for _, timestamp := range []uint64{0, 42} {
		request := &pb.Request{Ts: timestamp, TimestampBindingOperationId: append([]byte{1}, make([]byte, 15)...)}
		_, err := server.Forward(t.Context(), &pb.ForwardRequest{Requests: []*pb.Request{request}})
		require.Error(t, err)
		require.Equal(t, int32(0), issuer.calls.Load(), "bound forward must not consume a timestamp")
		require.Equal(t, timestamp, request.GetTs(), "forward rejection must not restamp caller data")
	}
}

func TestLookupTimestampBindingRequiresAppliedLeaderBarrier(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	binding := kv.TimestampBinding{Timestamp: 11, OperationID: [16]byte{1}, ManifestDigest: [32]byte{2}}
	leader := &prepareSafetyRPCLeader{}
	client := boundTimestampClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(st), WithInternalTimestampAdmission("timestamp-admission-token", nil)))
	ctx := boundTimestampContext(t)
	request := &pb.LookupTimestampBindingRequest{OperationId: binding.OperationID[:]}
	response, err := client.LookupTimestampBinding(ctx, request)
	require.NoError(t, err)
	require.False(t, response.GetFound())
	require.Equal(t, uint64(23), response.GetAppliedIndex())
	require.Equal(t, int32(1), leader.reads.Load())
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		_, err := kv.StageTimestampBinding(ctx, metadata, binding)
		return err
	}))
	response, err = client.LookupTimestampBinding(ctx, request)
	require.NoError(t, err)
	require.True(t, response.GetFound())
	require.Equal(t, binding.Timestamp, response.GetTimestamp())
	require.Equal(t, binding.ManifestDigest[:], response.GetManifestDigest())
	require.Equal(t, int32(2), leader.reads.Load())
	leader.loseRead = true
	response, err = client.LookupTimestampBinding(ctx, request)
	require.Error(t, err)
	require.Nil(t, response, "a lost leader cannot attest even an existing binding")
}
