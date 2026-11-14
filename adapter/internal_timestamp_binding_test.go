package adapter

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
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

func (i *boundTimestampIssuer) ReservePhysicalBandAfter(ctx context.Context, n int, min uint64) (kv.TSOPhysicalBandReservation, error) {
	ts, err := i.NextAfter(ctx, min)
	if err != nil {
		return kv.TSOPhysicalBandReservation{}, err
	}
	return kv.TSOPhysicalBandReservation{Base: ts, Count: n, CommittedBandEnd: ts | 0xffff}, nil
}

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
	require.Nil(t, response)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "TLS and bearer cannot establish peer identity")
	require.Zero(t, issuer.calls.Load())
	leader.follower.Store(true)
	_, err = client.IssueBoundTimestamp(ctx, &pb.IssueBoundTimestampRequest{MinTimestamp: 11})
	require.Error(t, err)
	require.Zero(t, issuer.calls.Load())
	leader.follower.Store(false)
	dataClient := boundTimestampClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalTimestampAdmission("timestamp-admission-token", issuer)))
	_, err = dataClient.IssueBoundTimestamp(ctx, &pb.IssueBoundTimestampRequest{MinTimestamp: 11})
	require.Error(t, err)
	require.Zero(t, issuer.calls.Load())
}

func boundTimestampPeerMethod(group uint64) peersession.Method {
	return peersession.Method{Name: pb.Internal_IssueBoundTimestamp_FullMethodName,
		Request: func(v any) (peersession.Message, error) {
			if _, ok := v.(*pb.IssueBoundTimestampRequest); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: 2, To: 1, Traffic: peersession.Control}, nil
		},
		Reply: func(req peersession.Message, v any) (peersession.Message, error) {
			if _, ok := v.(*pb.IssueBoundTimestampResponse); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			req.From, req.To = req.To, req.From
			return req, nil
		}}
}

func lookupTimestampPeerMethod(group uint64) peersession.Method {
	return peersession.Method{Name: pb.Internal_LookupTimestampBinding_FullMethodName,
		Request: func(v any) (peersession.Message, error) {
			if _, ok := v.(*pb.LookupTimestampBindingRequest); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: 2, To: 1, Traffic: peersession.Control}, nil
		},
		Reply: func(req peersession.Message, v any) (peersession.Message, error) {
			if _, ok := v.(*pb.LookupTimestampBindingResponse); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			req.From, req.To = req.To, req.From
			return req, nil
		}}
}

func newTimestampNativePeer(t *testing.T, groupID uint64, method peersession.Method, service *Internal) (*peersession.Owner, *peersession.Owner, *grpc.ClientConn, peersession.Node) {
	t.Helper()
	serverCert := migrationAuthTestCertificate(t)
	clientCert := migrationAuthTestCertificate(t)
	roots := x509.NewCertPool()
	roots.AddCert(serverCert.Leaf)
	roots.AddCert(clientCert.Leaf)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cluster, err := raftbootstrap.ParseUUID("17db1fe1-80ef-40bd-8f15-48f58db66c98")
	require.NoError(t, err)
	serverNode := peersession.Node{Cluster: cluster, Member: 1, Address: listener.Addr().String(), Key: sha256.Sum256(serverCert.Leaf.RawSubjectPublicKeyInfo)}
	clientNode := peersession.Node{Cluster: cluster, Member: 2, Address: "client.test:9001", Key: sha256.Sum256(clientCert.Leaf.RawSubjectPublicKeyInfo)}
	provisioned := []peersession.Provision{{Node: serverNode, Groups: []uint64{groupID}}, {Node: clientNode, Groups: []uint64{groupID}}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	methods := []peersession.Method{method}
	serverOwner, err := peersession.NewOwner(peersession.Config{Local: serverNode, Provisioned: provisioned, Versions: versions, Methods: methods})
	require.NoError(t, err)
	clientOwner, err := peersession.NewOwner(peersession.Config{Local: clientNode, Provisioned: provisioned, Versions: versions, Methods: methods})
	require.NoError(t, err)
	t.Cleanup(func() { clientOwner.Close(); serverOwner.Close() })
	group := peersession.Group{ID: groupID, Generation: 1, Minimum: versions, State: peersession.Intact, Stage: peersession.ControlReady, Pending: peersession.PendingClear, MarkerReady: true,
		Policy: sha256.Sum256([]byte("applied tso policy")), Floor: raftbootstrap.Floor{ActivationIndex: 2, Generation: 1},
		Configuration: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
			{ID: 1, Address: serverNode.Address, PublicKeyDigest: serverNode.Key}, {ID: 2, Address: clientNode.Address, PublicKeyDigest: clientNode.Key},
		}}}
	require.NoError(t, serverOwner.UpdateGroup(group))
	require.NoError(t, clientOwner.UpdateGroup(group))
	options, err := serverOwner.ServerOptions(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: roots})
	require.NoError(t, err)
	grpcServer := grpc.NewServer(options...)
	pb.RegisterInternalServer(grpcServer, service)
	serverOwner.Register(grpcServer)
	done := make(chan struct{})
	go func() { defer close(done); _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close(); <-done })
	tc, err := clientOwner.TransportCredentials(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12}, false)
	require.NoError(t, err)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(tc), grpc.WithAuthority("migration.test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return serverOwner, clientOwner, conn, serverNode
}

func TestIssueBoundTimestampNativePeerSession(t *testing.T) {
	issuer := &boundTimestampIssuer{}
	service := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithKVForwardRejected(), WithInternalTimestampAdmission("timestamp-admission-token", issuer))
	serverOwner, clientOwner, conn, serverNode := newTimestampNativePeer(t, 0, boundTimestampPeerMethod(0), service)
	var err error
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request := &pb.IssueBoundTimestampRequest{MinTimestamp: 10, PhysicalBandProtocolVersion: kv.TSOPhysicalBandProtocolVersion}
	_, err = pb.NewInternalClient(conn).IssueBoundTimestamp(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer timestamp-admission-token"), request)
	require.Error(t, err, "mTLS plus bearer without a session is insufficient")
	require.Zero(t, issuer.calls.Load())
	handle, err := clientOwner.Handshake(ctx, conn, 0, serverNode)
	require.NoError(t, err)
	var response pb.IssueBoundTimestampResponse
	for _, token := range []string{"", "wrong"} {
		var callErr error
		if token == "" {
			callErr = handle.Invoke(ctx, conn, pb.Internal_IssueBoundTimestamp_FullMethodName, request, &response)
		} else {
			callErr = handle.Invoke(ctx, conn, pb.Internal_IssueBoundTimestamp_FullMethodName,
				request, &response, grpc.PerRPCCredentials(migrationNativeBearer{token: token}))
		}
		require.Equal(t, codes.Unauthenticated, status.Code(callErr))
		require.Zero(t, issuer.calls.Load())
	}
	require.NoError(t, handle.Invoke(ctx, conn, pb.Internal_IssueBoundTimestamp_FullMethodName,
		request, &response, grpc.PerRPCCredentials(migrationNativeBearer{token: "timestamp-admission-token"})))
	require.Equal(t, uint64(11), response.GetTimestamp())
	require.Equal(t, kv.TSOPhysicalBandProtocolVersion, response.GetPhysicalBandProtocolVersion())
	require.Equal(t, uint64(0xffff), response.GetCommittedBandEnd())
	require.Equal(t, int32(1), issuer.calls.Load())
	request.PhysicalBandProtocolVersion = 0
	require.Error(t, handle.Invoke(ctx, conn, pb.Internal_IssueBoundTimestamp_FullMethodName,
		request, &response, grpc.PerRPCCredentials(migrationNativeBearer{token: "timestamp-admission-token"})))
	require.Equal(t, int32(1), issuer.calls.Load())
	serverOwner.RevokeGroup(0)
	request.PhysicalBandProtocolVersion = kv.TSOPhysicalBandProtocolVersion
	require.Error(t, handle.Invoke(ctx, conn, pb.Internal_IssueBoundTimestamp_FullMethodName,
		&pb.IssueBoundTimestampRequest{MinTimestamp: 11}, &response, grpc.PerRPCCredentials(migrationNativeBearer{token: "timestamp-admission-token"})))
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
	controlMetadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	binding := kv.TimestampBinding{Timestamp: 11, OperationID: [16]byte{1}, ManifestDigest: [32]byte{2}}
	leader := &prepareSafetyRPCLeader{}
	service := NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(st), WithInternalTimestampAdmission("timestamp-admission-token", nil))
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 17, lookupTimestampPeerMethod(17), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request := &pb.LookupTimestampBindingRequest{OperationId: binding.OperationID[:]}
	_, err := pb.NewInternalClient(conn).LookupTimestampBinding(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer timestamp-admission-token"), request)
	require.Error(t, err, "TLS and bearer without a peer session cannot read home binding")
	require.Zero(t, leader.reads.Load())
	handle, err := clientOwner.Handshake(ctx, conn, 17, serverNode)
	require.NoError(t, err)
	lookup := func() (*pb.LookupTimestampBindingResponse, error) {
		response := new(pb.LookupTimestampBindingResponse)
		err := handle.Invoke(ctx, conn, pb.Internal_LookupTimestampBinding_FullMethodName, request, response,
			grpc.PerRPCCredentials(migrationNativeBearer{token: "timestamp-admission-token"}))
		return response, err
	}
	response, err := lookup()
	require.NoError(t, err)
	require.False(t, response.GetFound())
	require.Equal(t, uint64(23), response.GetAppliedIndex())
	require.Equal(t, int32(1), leader.reads.Load())
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		_, err := kv.StageTimestampBinding(ctx, controlMetadata, binding)
		return err
	}))
	response, err = lookup()
	require.NoError(t, err)
	require.True(t, response.GetFound())
	require.Equal(t, binding.Timestamp, response.GetTimestamp())
	require.Equal(t, binding.ManifestDigest[:], response.GetManifestDigest())
	require.Equal(t, int32(2), leader.reads.Load())
	leader.loseRead = true
	_, err = lookup()
	require.Error(t, err)
}
