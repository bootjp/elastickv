package peersession

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func mixedRequest(binding RequestBinding, payload any) (Message, error) {
	m, err := decodeMessage(payload)
	if err != nil || m.Group != binding.Group || m.From != 0 || m.To != 0 {
		return Message{}, ErrFormat
	}
	return Message{Group: binding.Group, From: binding.From.Member, To: binding.To.Member, Traffic: Control}, nil
}

func mixedReply(request Message, payload any) (Message, error) {
	m, err := decodeMessage(payload)
	if err != nil || m.Group != request.Group {
		return Message{}, ErrFormat
	}
	request.From, request.To = request.To, request.From
	return request, nil
}

func mixedMethods() []Method {
	return []Method{
		{Name: sendMethod, AllowPublic: true, BoundRequest: mixedRequest, Reply: mixedReply},
		{Name: otherMethod, Request: decodeMessage, Reply: decodeReply},
		{Name: streamMethod, AllowPublic: true, Request: decodeMessage, Reply: decodeReply},
	}
}

func setupMixed(t *testing.T, ready *atomic.Bool, stage Stage) *nativeFixture {
	t.Helper()
	pki := newPKI(t)
	serverCert, serverKey := pki.certificate(t, 2)
	clientCert, clientKey := pki.certificate(t, 3)
	cluster, err := raftbootstrap.ParseUUID("c40d2835-d4be-4b78-91d8-1528dc544415")
	require.NoError(t, err)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverNode := Node{Cluster: cluster, Member: 1, Address: lis.Addr().String(), Key: serverKey}
	clientNode := Node{Cluster: cluster, Member: 2, Address: "client.test:9001", Key: clientKey}
	provisions := []Provision{{Node: serverNode, Groups: []uint64{testGroup}}, {Node: clientNode, Groups: []uint64{testGroup}}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	client, err := NewOwner(Config{Local: clientNode, Provisioned: provisions, Versions: versions, Methods: mixedMethods()})
	require.NoError(t, err)
	server, err := NewOwner(Config{Local: serverNode, Provisioned: provisions, Versions: versions, Methods: mixedMethods()})
	require.NoError(t, err)
	group := Group{ID: testGroup, Generation: 1, Minimum: versions, State: Intact, Stage: stage, Pending: PendingClear, MarkerReady: true,
		Policy: sha256.Sum256([]byte("policy")), Floor: raftbootstrap.Floor{ActivationIndex: 6, Generation: 1},
		Configuration: raftbootstrap.Configuration{Index: 5, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}},
			Members: []raftbootstrap.Member{{ID: 1, Address: serverNode.Address, PublicKeyDigest: serverKey}, {ID: 2, Address: clientNode.Address, PublicKeyDigest: clientKey}}}}
	require.NoError(t, client.UpdateGroup(group))
	require.NoError(t, server.UpdateGroup(group))
	clientTLS := &tls.Config{RootCAs: pki.roots, Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS13}
	serverTLS := &tls.Config{ClientCAs: pki.roots, ClientAuth: tls.VerifyClientCertIfGiven, Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS13}
	opts, err := server.ServerOptions(serverTLS)
	require.NoError(t, err)
	// Model the parent's separate DataReady gate: an authenticated peer receipt
	// chooses Control; its absence never grants startup-stage public access.
	opts = append(opts, grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != sendMethod {
			return handler(ctx, req)
		}
		receipt, err := server.Received(ctx)
		if err != nil {
			if CapturedReceipt(ctx) != nil {
				return nil, denied()
			}
			if !ready.Load() {
				return nil, status.Error(codes.Unavailable, "data is not ready")
			}
			return handler(ctx, req)
		}
		if CapturedReceipt(ctx) != receipt {
			return nil, denied()
		}
		handle, err := server.Inbound(ctx)
		if err != nil {
			return nil, err
		}
		if err := handle.CheckIncoming(ctx, info.FullMethod, req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}))
	opts = append(opts, grpc.ChainStreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if CapturedReceipt(stream.Context()) != nil {
			return denied()
		}
		return handler(srv, &mixedCaptureStream{ServerStream: stream, owner: server})
	}))
	gs := grpc.NewServer(opts...)
	server.Register(gs)
	service := &nativeService{owner: server, ended: make(chan struct{}, 8)}
	registerNative(gs, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = gs.Serve(lis) }()
	t.Cleanup(func() { client.Close(); server.Close(); gs.Stop(); _ = lis.Close(); <-done })
	return &nativeFixture{client: client, server: server, clientNode: clientNode, serverNode: serverNode, group: group,
		pki: pki, clientTLS: clientTLS, serverTLS: serverTLS, service: service, address: lis.Addr().String()}
}

func mixedPayload() *wire.Frame { return frameMessage(Message{Group: testGroup}) }

type mixedCaptureStream struct {
	grpc.ServerStream
	owner *Owner
}

func (s *mixedCaptureStream) RecvMsg(payload any) error {
	if err := s.ServerStream.RecvMsg(payload); err != nil {
		return err
	}
	if CapturedReceipt(s.Context()) != nil {
		return denied()
	}
	if !hasSessionMetadata(s.Context()) {
		return nil
	}
	receipt, err := s.owner.Received(s.Context())
	if err != nil {
		return err
	}
	captured, err := s.owner.CaptureReceived(s.Context())
	if err != nil {
		return err
	}
	if CapturedReceipt(captured) != receipt {
		return denied()
	}
	return nil
}

func TestMixedMethodsTLSControlBeforePublicData(t *testing.T) {
	var ready atomic.Bool
	f := setupMixed(t, &ready, ControlReady)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	require.NoError(t, h.Invoke(ctx, cc, sendMethod, mixedPayload(), &reply))
	require.EqualValues(t, 1, f.service.delivered.Load())
	public := f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, MinVersion: tls.VersionTLS13}))
	require.Equal(t, codes.Unavailable, status.Code(public.Invoke(ctx, sendMethod, mixedPayload(), &reply)))
	ready.Store(true)
	// The public handler is deliberately not restricted to the private decoder.
	require.NoError(t, public.Invoke(ctx, sendMethod, request(Data), &reply))
	require.EqualValues(t, 2, f.service.delivered.Load())
	require.Error(t, public.Invoke(ctx, otherMethod, request(Control), &reply))
}

func TestMixedMethodsTLSMalformedMetadataNeverFallsBack(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	f := setupMixed(t, &ready, ControlReady)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	id := h.SessionID()
	wrongMethodProof := rpcProof(h.s, otherMethod)
	tests := []struct {
		name string
		md   metadata.MD
	}{
		{"session only", metadata.Pairs(sessionHeader, "bad")},
		{"proof only", metadata.Pairs(proofHeader, "bad")},
		{"empty headers", metadata.Pairs(sessionHeader, "", proofHeader, "")},
		{"forged proof", metadata.Pairs(sessionHeader, "bad", proofHeader, "bad")},
		{"duplicate session", metadata.Pairs(sessionHeader, "bad", sessionHeader, "bad", proofHeader, "bad")},
		{"wrong method proof", metadata.Pairs(sessionHeader, hex.EncodeToString(id[:]), proofHeader, hex.EncodeToString(wrongMethodProof[:]))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			call := metadata.NewOutgoingContext(ctx, tt.md)
			require.Error(t, cc.Invoke(call, sendMethod, mixedPayload(), &reply))
		})
	}
	// A valid session credential for the wrong typed payload is also not public.
	require.Error(t, cc.Invoke(ctx, sendMethod, request(Data), &reply, grpc.PerRPCCredentials(h)))
	require.EqualValues(t, 0, f.service.delivered.Load())
}

func TestMixedMethodsTLSBindingAndRevocation(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	f := setupMixed(t, &ready, ControlReady)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	wrongGroup := frameMessage(Message{Group: testGroup + 1})
	require.Error(t, h.Invoke(ctx, cc, sendMethod, wrongGroup, &reply))
	require.Error(t, cc.Invoke(ctx, sendMethod, wrongGroup, &reply, grpc.PerRPCCredentials(h)))
	// A receipt binds private endpoints and supports subsequent enqueue checks.
	b, err := privateBinding(h, true)
	require.NoError(t, err)
	require.Equal(t, RequestBinding{Group: testGroup, From: f.clientNode, To: f.serverNode}, b)
	other := f.dial(t)
	require.Error(t, other.Invoke(ctx, sendMethod, mixedPayload(), &reply, grpc.PerRPCCredentials(h)))
	f.server.RevokeGroup(testGroup)
	require.Error(t, h.Invoke(ctx, cc, sendMethod, mixedPayload(), &reply))
	require.EqualValues(t, 0, f.service.delivered.Load())
}

func TestMixedMethodsTLSIdentityStageCannotIssueControl(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	f := setupMixed(t, &ready, IdentityReady)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	require.Error(t, h.Invoke(ctx, cc, sendMethod, mixedPayload(), &reply))
	require.Error(t, cc.Invoke(ctx, sendMethod, mixedPayload(), &reply, grpc.PerRPCCredentials(h)))
	require.EqualValues(t, 0, f.service.delivered.Load())
}

func TestMixedMethodsTLSRejectsUnverifiedOrInsecurePeer(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	f := setupMixed(t, &ready, ControlReady)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	public := f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, MinVersion: tls.VersionTLS13}))
	require.Error(t, public.Invoke(ctx, sendMethod, mixedPayload(), &reply, grpc.PerRPCCredentials(h)))
	badCert, _ := newPKI(t).certificate(t, 4)
	badTLS := credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, Certificates: []tls.Certificate{badCert}, MinVersion: tls.VersionTLS13})
	bad := f.dialCredentials(t, badTLS)
	require.Error(t, bad.Invoke(ctx, sendMethod, mixedPayload(), &reply))
	require.Error(t, bad.Invoke(ctx, sendMethod, mixedPayload(), &reply, grpc.PerRPCCredentials(h)))
	untrusted := f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: newPKI(t).roots, MinVersion: tls.VersionTLS13}))
	require.Error(t, untrusted.Invoke(ctx, sendMethod, mixedPayload(), &reply))
	plain, err := grpc.NewClient("passthrough:///"+f.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	require.Error(t, plain.Invoke(ctx, sendMethod, mixedPayload(), &reply, grpc.PerRPCCredentials(h)))
	require.EqualValues(t, 0, f.service.delivered.Load())
}

func TestMixedMethodsTLSAdditionalTokenGateIsConjunctive(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	f := setupMixed(t, &ready, ControlReady)
	f.service.requireControlToken.Store(true)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	require.Error(t, h.Invoke(ctx, cc, sendMethod, mixedPayload(), &reply))
	wrong := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-test-startup-control", "wrong"))
	require.Error(t, h.Invoke(wrong, cc, sendMethod, mixedPayload(), &reply))
	require.EqualValues(t, 0, f.service.delivered.Load())
	good := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-test-startup-control", "test-only-control"))
	require.NoError(t, h.Invoke(good, cc, sendMethod, mixedPayload(), &reply))
	require.EqualValues(t, 1, f.service.delivered.Load())
}

func TestMixedMethodsStreamMetadataDoesNotDowngrade(t *testing.T) {
	var ready atomic.Bool
	f := setupMixed(t, &ready, ControlReady)
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	desc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	call := metadata.NewOutgoingContext(ctx, metadata.Pairs(proofHeader, "bad"))
	s, err := cc.NewStream(call, desc, streamMethod)
	require.NoError(t, err)
	_ = s.SendMsg(request(Control))
	var reply wire.Frame
	require.Error(t, s.RecvMsg(&reply))
	require.EqualValues(t, 0, f.service.delivered.Load())
	private, err := h.NewStream(ctx, cc, desc, streamMethod)
	require.NoError(t, err)
	require.NoError(t, private.SendMsg(request(Control)))
	require.NoError(t, private.RecvMsg(&reply))
	public, err := cc.NewStream(ctx, desc, streamMethod)
	require.NoError(t, err)
	require.NoError(t, public.SendMsg(request(Data)))
	require.NoError(t, public.RecvMsg(&reply))
	require.NoError(t, private.CloseSend())
	require.NoError(t, public.CloseSend())
}

func TestMixedMethodsDecoderConfiguration(t *testing.T) {
	method := mixedMethods()[0]
	require.True(t, validDecoders(method))
	for _, mutate := range []func(*Method){
		func(m *Method) { m.Request = decodeMessage },
		func(m *Method) { m.Reply = nil },
		func(m *Method) { m.Stream = func(Binding) (StreamDecoders, error) { return StreamDecoders{}, nil } },
	} {
		bad := method
		mutate(&bad)
		require.False(t, validDecoders(bad))
	}
	_, err := decodeRequest(&Handle{}, method, mixedPayload(), false)
	require.ErrorIs(t, err, ErrDenied)
	for _, key := range []string{sessionHeader, proofHeader} {
		ctx := metadata.NewIncomingContext(t.Context(), metadata.MD{key: nil})
		require.True(t, hasSessionMetadata(ctx))
	}
}
