package main

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const bootstrapTransportTestMethod = "/S3BlobFetch/Hold"

type bootstrapTransportFixture struct {
	gate    *startupPublicKVGate
	conn    *grpc.ClientConn
	server  *grpc.Server
	owner   *peersession.Owner
	config  *bootstrapPeerConfig
	address string
	entered chan context.Context
	ended   chan error
	sent    atomic.Uint64
}

func bootstrapTransportServer(t *testing.T, mode string) *bootstrapTransportFixture {
	t.Helper()
	f := &bootstrapTransportFixture{gate: bootstrapIngressFixture(t), entered: make(chan context.Context, 1), ended: make(chan error, 1)}
	bootstrapIngressLoadCatalog(t, f.gate)
	config, err := buildBootstrapPeerConfig(bootstrapConfigFixture(t))
	require.NoError(t, err)
	f.config = config
	owner, err := peersession.NewOwner(peersession.Config{Local: config.local, Provisioned: config.provisions,
		Versions: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, PublicMethods: []string{bootstrapTransportTestMethod}})
	require.NoError(t, err)
	f.owner = owner
	t.Cleanup(owner.Close)
	options, err := owner.ServerOptions(config.serverTLS)
	require.NoError(t, err)
	options = append(options, grpc.ChainStreamInterceptor(f.gate.streamInterceptor))
	f.server = grpc.NewServer(options...)
	descriptor := grpc.StreamDesc{StreamName: "Hold", ClientStreams: true, ServerStreams: true,
		Handler: func(_ any, stream grpc.ServerStream) (err error) {
			defer func() { f.ended <- err }()
			var initial wrapperspb.BytesValue
			if err := stream.RecvMsg(&initial); err != nil {
				return err
			}
			f.entered <- context.WithoutCancel(stream.Context())
			if mode == "normal" {
				return nil
			}
			if mode == "recv" {
				return stream.RecvMsg(&wrapperspb.BytesValue{})
			}
			payload := wrapperspb.Bytes(make([]byte, 1<<20))
			for {
				if err := stream.SendMsg(payload); err != nil {
					return err
				}
				f.sent.Add(1)
			}
		}}
	f.server.RegisterService(&grpc.ServiceDesc{ServiceName: "S3BlobFetch", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{descriptor}}, &struct{}{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f.address = listener.Addr().String()
	served := make(chan struct{})
	go func() { defer close(served); _ = f.server.Serve(listener) }()
	t.Cleanup(func() { f.server.Stop(); <-served })
	// Ordinary public TLS does not require a client certificate or peer session.
	clientTLS := config.clientTLS.Clone()
	clientTLS.Certificates = nil
	f.conn, err = grpc.NewClient(listener.Addr().String(), grpc.WithAuthority("localhost"),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
		grpc.WithInitialWindowSize(64<<10), grpc.WithInitialConnWindowSize(64<<10))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.conn.Close()) })
	return f
}

func (f *bootstrapTransportFixture) start(t *testing.T, ctx context.Context) grpc.ClientStream {
	t.Helper()
	stream, err := f.conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, bootstrapTransportTestMethod)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(wrapperspb.Bytes([]byte("start"))))
	return stream
}

func TestBootstrapIngressTLSTransportRevocationUnblocks(t *testing.T) {
	for _, mode := range []string{"recv", "send"} {
		t.Run(mode, func(t *testing.T) {
			f := bootstrapTransportServer(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			stream := f.start(t, ctx)
			var authenticated context.Context
			select {
			case authenticated = <-f.entered:
			case <-ctx.Done():
				t.Fatal("TLS stream handler did not start")
			}
			_, err := peersession.CaptureServerTransportAbort(authenticated)
			require.NoError(t, err, "actual owned server TLS transport must be capturable")
			remote, ok := peer.FromContext(authenticated)
			require.True(t, ok)
			auth, ok := remote.AuthInfo.(*peersession.ServerAuthInfo)
			require.True(t, ok)
			info, ok := auth.TransportTLSInfo()
			require.True(t, ok)
			require.Empty(t, info.State.PeerCertificates, "cleanup does not require mTLS peer authority")
			// No further send/receive/CloseSend: the client intentionally stalls.
			// A megabyte reply exceeds the fixed HTTP/2 receive window.
			if mode == "send" {
				require.Eventually(t, func() bool { return f.sent.Load() != 0 }, 3*time.Second, time.Millisecond)
			}
			previous := f.sent.Load()
			time.Sleep(150 * time.Millisecond)
			require.Equal(t, previous, f.sent.Load(), "server must be waiting on transport flow control")
			select {
			case err := <-f.ended:
				t.Fatalf("handler returned before revocation: %v", err)
			default:
			}
			bootstrapIngressObserve(t, f.gate.domain.admission, 2)
			select {
			case err := <-f.ended:
				require.Error(t, err)
			case <-ctx.Done():
				t.Fatal("revoked transport did not unblock handler")
			}
			// Finish the RPC/client without relying on its deadline for recovery.
			require.Error(t, stream.RecvMsg(&wrapperspb.BytesValue{}))
			_, err = peersession.CaptureServerTransportAbort(authenticated)
			require.ErrorIs(t, err, peersession.ErrDenied, "retired physical connection must be rejected")
			bootstrapTransportRepublish(t, f.gate)
			require.Eventually(t, func() bool { return f.gate.domain.admission.Stage(1) == peersession.DataReady }, 3*time.Second, time.Millisecond, "revoked stream must leave no draining operation")
		})
	}
}

func bootstrapTransportRepublish(t *testing.T, gate *startupPublicKVGate) {
	t.Helper()
	ticket, err := gate.domain.admission.Ticket(1)
	require.NoError(t, err)
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1}}, Members: []raftbootstrap.Member{{ID: 1, Address: "localhost:9000", PublicKeyDigest: raftbootstrap.Digest{1}}}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	require.NoError(t, gate.domain.admission.PublishRecovery(ticket, raftbootstrap.PolicyRecord{Version: 1, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1}, BaselineDigest: digest,
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1, Generation: 1, LastConfigurationIndex: 1, Configuration: configuration}, 2, raftadmission.RegistrationNotConfigured))
	bootstrapIngressLoadCatalog(t, gate)
}

func TestBootstrapIngressTLSNormalCompletionKeepsTransport(t *testing.T) {
	f := bootstrapTransportServer(t, "normal")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream := f.start(t, ctx)
	var authenticated context.Context
	select {
	case authenticated = <-f.entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	require.ErrorIs(t, stream.RecvMsg(&wrapperspb.BytesValue{}), io.EOF)
	select {
	case err := <-f.ended:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("normal handler did not finish")
	}
	_, err := peersession.CaptureServerTransportAbort(authenticated)
	require.NoError(t, err, "normal operation retirement must not close the connection")
	// A second RPC on the same HTTP/2 connection remains usable.
	stream = f.start(t, ctx)
	var second context.Context
	select {
	case second = <-f.entered:
	case <-ctx.Done():
		t.Fatal("second handler did not start")
	}
	require.ErrorIs(t, stream.RecvMsg(&wrapperspb.BytesValue{}), io.EOF)
	firstPeer, ok := peer.FromContext(authenticated)
	require.True(t, ok)
	secondPeer, ok := peer.FromContext(second)
	require.True(t, ok)
	require.Same(t, firstPeer.AuthInfo, secondPeer.AuthInfo, "reuse must not be an automatic reconnect after an accidental abort")
}

func TestBootstrapIngressRejectsUnownedTransportBeforeBegin(t *testing.T) {
	gate := bootstrapIngressFixture(t)
	bootstrapIngressLoadCatalog(t, gate)
	for _, auth := range []credentials.AuthInfo{nil, &peersession.ServerAuthInfo{}, &peersession.ClientAuthInfo{}, credentials.TLSInfo{}} {
		ctx := peer.NewContext(t.Context(), &peer.Peer{AuthInfo: auth})
		called := false
		err := gate.protectedStream(nil, &bootstrapIngressTestStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: bootstrapTransportTestMethod}, func(any, grpc.ServerStream) error { called = true; return nil })
		require.Error(t, err)
		require.False(t, called)
	}
	for _, ctx := range []context.Context{nil, context.Background()} {
		_, err := peersession.CaptureServerTransportAbort(ctx)
		require.ErrorIs(t, err, peersession.ErrDenied)
	}
	owned, op, err := gate.domain.admission.Begin(t.Context(), raftadmission.Data)
	require.NoError(t, err)
	require.NoError(t, gate.domain.admission.ValidateResult(owned))
	op.CancelAndJoin()
}

func TestBootstrapIngressRejectsGenuineClientTransportAbort(t *testing.T) {
	f := bootstrapTransportServer(t, "normal")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	creds, err := f.owner.TransportCredentials(f.config.clientTLS, false)
	require.NoError(t, err)
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", f.address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	conn, auth, err := creds.ClientHandshake(ctx, "localhost", raw)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.IsType(t, &peersession.ClientAuthInfo{}, auth)
	_, err = peersession.CaptureServerTransportAbort(peer.NewContext(ctx, &peer.Peer{AuthInfo: auth}))
	require.ErrorIs(t, err, peersession.ErrDenied)
}
