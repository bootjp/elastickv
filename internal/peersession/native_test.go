package peersession

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const sendMethod = "/test.Peer/Send"
const otherMethod = "/test.Peer/Other"
const streamMethod = "/test.Peer/Exchange"
const snapshotMethod = "/test.Peer/Snapshot"
const testGroup = 17
const testPayloadKind = 21
const testTimeout = 5 * time.Second

type testPKI struct {
	ca    *x509.Certificate
	key   ed25519.PrivateKey
	roots *x509.CertPool
}

func newPKI(t *testing.T) testPKI {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return testPKI{ca: cert, key: key, roots: roots}
}
func (p testPKI) certificate(t *testing.T, serial int64) (tls.Certificate, raftbootstrap.Digest) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"server.test", "client.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, p.ca, pub, p.key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der, p.ca.Raw}, PrivateKey: key, Leaf: leaf}, keyDigest(leaf.RawSubjectPublicKeyInfo)
}

func frameMessage(m Message) *wire.Frame {
	e := newEncoder(testPayloadKind)
	e.fixed(m)
	return &wire.Frame{Body: e.Bytes()}
}
func decodeMessage(v any) (Message, error) {
	f, ok := v.(*wire.Frame)
	if !ok {
		return Message{}, ErrFormat
	}
	d, err := newDecoder(f.Body, testPayloadKind)
	if err != nil {
		return Message{}, err
	}
	var m Message
	d.fixed(&m)
	return m, d.end()
}
func decodeReply(_ Message, reply any) (Message, error) { return decodeMessage(reply) }
func flipMessage(f *wire.Frame) *wire.Frame {
	m, _ := decodeMessage(f)
	m.From, m.To = m.To, m.From
	return frameMessage(m)
}

type testPeer interface{ testPeerMarker() }
type nativeService struct {
	owner               *Owner
	delivered           atomic.Int64
	spoolDir            string
	spools              chan string
	ended               chan struct{}
	retryClose          atomic.Bool
	spoofReply          atomic.Bool
	requireControlToken atomic.Bool
}

func (*nativeService) testPeerMarker() {}
func (s *nativeService) unary(ctx context.Context, req *wire.Frame) (*wire.Frame, error) {
	if s.requireControlToken.Load() {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("x-test-startup-control")
		if len(values) != 1 || values[0] != "test-only-control" {
			return nil, denied()
		}
	}
	s.delivered.Add(1)
	if s.retryClose.CompareAndSwap(true, false) {
		h, err := s.owner.Inbound(ctx)
		if err != nil {
			return nil, err
		}
		h.Close()
		return nil, io.ErrUnexpectedEOF
	}
	if s.spoofReply.Load() {
		m, err := decodeMessage(req)
		if err != nil {
			return nil, err
		}
		m.From = 99
		return frameMessage(m), nil
	}
	return flipMessage(req), nil
}
func (s *nativeService) exchange(stream grpc.ServerStream, spool bool) error {
	defer func() { s.ended <- struct{}{} }()
	spooled := false
	for {
		var req wire.Frame
		if err := stream.RecvMsg(&req); err != nil {
			return err
		}
		if spool && !spooled {
			file, err := os.CreateTemp(s.spoolDir, "snapshot-*")
			if err != nil {
				return err
			}
			path := file.Name()
			defer os.Remove(path)
			defer file.Close()
			s.spools <- path
			spooled = true
		}
		s.delivered.Add(1)
		if err := stream.SendMsg(flipMessage(&req)); err != nil {
			return err
		}
	}
}
func testUnary(method string) grpc.MethodDesc {
	return grpc.MethodDesc{MethodName: method, Handler: func(srv any, ctx context.Context, dec func(any) error, intercept grpc.UnaryServerInterceptor) (any, error) {
		req := &wire.Frame{}
		if err := dec(req); err != nil {
			return nil, err
		}
		service, ok := srv.(*nativeService)
		if !ok {
			return nil, ErrFormat
		}
		handler := func(ctx context.Context, v any) (any, error) {
			frame, ok := v.(*wire.Frame)
			if !ok {
				return nil, ErrFormat
			}
			return service.unary(ctx, frame)
		}
		return intercept(ctx, req, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/test.Peer/" + method}, handler)
	}}
}
func registerNative(server *grpc.Server, service *nativeService) {
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Peer", HandlerType: (*testPeer)(nil), Methods: []grpc.MethodDesc{testUnary("Send"), testUnary("Other")}, Streams: []grpc.StreamDesc{
		{StreamName: "Exchange", ServerStreams: true, ClientStreams: true, Handler: testStream(false)},
		{StreamName: "Snapshot", ServerStreams: true, ClientStreams: true, Handler: testStream(true)},
	}}, service)
}

func testStream(spool bool) grpc.StreamHandler {
	return func(srv any, s grpc.ServerStream) error {
		service, ok := srv.(*nativeService)
		if !ok {
			return ErrFormat
		}
		return service.exchange(s, spool)
	}
}

type nativeFixture struct {
	client, server         *Owner
	clientNode, serverNode Node
	group                  Group
	pki                    testPKI
	clientTLS, serverTLS   *tls.Config
	service                *nativeService
	large                  *largeService
	address                string
}
type fixtureOptions struct {
	clientState   State
	learner       bool
	authorizer    CatchupAuthorizer
	wrap          func(*Owner) wire.PeerSessionServer
	old           bool
	preactivation bool
}

func setupNative(t *testing.T, options fixtureOptions) *nativeFixture {
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
	methods := []Method{{Name: sendMethod, Request: decodeMessage, Reply: decodeReply}, {Name: otherMethod, Request: decodeMessage, Reply: decodeReply}, {Name: streamMethod, Request: decodeMessage, Reply: decodeReply}, {Name: snapshotMethod, Request: decodeMessage, Reply: decodeReply}}
	methods = append(methods, largeRaftMethod(), snapshotMethodSpec())
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	client, err := NewOwner(Config{Local: clientNode, Provisioned: provisions, Versions: versions, Methods: methods, Catchup: options.authorizer})
	require.NoError(t, err)
	server, err := NewOwner(Config{Local: serverNode, Provisioned: provisions, Versions: versions, Methods: methods, Catchup: options.authorizer, PublicMethods: []string{healthpb.Health_Check_FullMethodName, publicEchoMethod}})
	require.NoError(t, err)
	g := Group{ID: testGroup, Generation: 1, Minimum: versions, State: Intact, Stage: DataReady, Pending: PendingClear, MarkerReady: true, Policy: sha256.Sum256([]byte("policy")), Configuration: raftbootstrap.Configuration{Index: 5, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{{ID: 1, Address: serverNode.Address, PublicKeyDigest: serverKey}, {ID: 2, Address: clientNode.Address, PublicKeyDigest: clientKey}}}}
	g.Floor = raftbootstrap.Floor{ActivationIndex: 6, Generation: 1}
	if options.preactivation {
		g.Floor = raftbootstrap.Floor{}
	}
	if options.learner {
		g.Configuration.State.Voters = []uint64{1}
		g.Configuration.State.Learners = []uint64{2}
	}
	require.NoError(t, server.UpdateGroup(g))
	cg := g
	if options.clientState != 0 {
		cg.State = options.clientState
	}
	require.NoError(t, client.UpdateGroup(cg))
	clientTLS := &tls.Config{RootCAs: pki.roots, Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS13}
	serverTLS := &tls.Config{ClientCAs: pki.roots, ClientAuth: tls.VerifyClientCertIfGiven, Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12}
	var opts []grpc.ServerOption
	if options.old {
		opts = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLS))}
	} else {
		opts, err = server.ServerOptions(serverTLS)
		require.NoError(t, err)
	}
	grpcServer := grpc.NewServer(opts...)
	if !options.old {
		if options.wrap == nil {
			server.Register(grpcServer)
		} else {
			wire.RegisterPeerSessionServer(grpcServer, options.wrap(server))
		}
	}
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	service := &nativeService{owner: server, spoolDir: t.TempDir(), spools: make(chan string, 8), ended: make(chan struct{}, 8)}
	var large *largeService
	if !options.old {
		registerNative(grpcServer, service)
		large = registerLargeServices(grpcServer, server)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() {
		client.Close()
		server.Close()
		grpcServer.Stop()
		_ = lis.Close()
		<-done
		require.Equal(t, Counts{}, client.Counts())
		require.Equal(t, Counts{}, server.Counts())
	})
	return &nativeFixture{client: client, server: server, clientNode: clientNode, serverNode: serverNode, group: g, pki: pki, clientTLS: clientTLS, serverTLS: serverTLS, service: service, large: large, address: lis.Addr().String()}
}

func (f *nativeFixture) dial(t *testing.T) *grpc.ClientConn {
	t.Helper()
	tc, err := f.client.TransportCredentials(f.clientTLS, false)
	require.NoError(t, err)
	return f.dialCredentials(t, tc)
}
func (f *nativeFixture) dialCredentials(t *testing.T, tc credentials.TransportCredentials, extra ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tc), grpc.WithAuthority("server.test")}
	opts = append(opts, extra...)
	cc, err := grpc.NewClient(f.address, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}
func (f *nativeFixture) handshake(t *testing.T, cc *grpc.ClientConn) *Handle {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	h, err := f.client.Handshake(ctx, cc, testGroup, f.serverNode)
	require.NoError(t, err)
	return h
}
func request(traffic Traffic) *wire.Frame {
	return frameMessage(Message{Group: testGroup, From: 2, To: 1, Traffic: traffic})
}

func TestNativeMTLSHandshakeAndPublicRPC(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
	m, err := decodeMessage(&reply)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m.From)
	require.Equal(t, 1, f.client.Counts().Active)
	require.Equal(t, 1, f.server.Counts().Active)
	var nativePeer peer.Peer
	_, err = healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Peer(&nativePeer))
	require.NoError(t, err)
	ai, ok := nativePeer.AuthInfo.(*ClientAuthInfo)
	require.True(t, ok)
	require.Same(t, h.c, ai.connection)
	// Public callers need no client certificate on the SAME shared listener.
	public := f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, MinVersion: tls.VersionTLS13}))
	_, err = healthpb.NewHealthClient(public).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	// It is the transport's physical Close, not an address-cache eviction.
	h.Close()
	select {
	case <-h.Done():
	case <-time.After(testTimeout):
		t.Fatal("handle not revoked")
	}
	require.Eventually(t, func() bool { return f.server.Counts().Active == 0 }, testTimeout, time.Millisecond)
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
}

func TestNativeStreamChunkRevocationAndSpoolCleanup(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := h.NewStream(ctx, cc, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, snapshotMethod)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(request(Replication)))
	var reply wire.Frame
	require.NoError(t, stream.RecvMsg(&reply))
	var path string
	select {
	case path = <-f.service.spools:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.FileExists(t, path)
	g := f.group
	g.Generation++
	g.Pending = PendingTransition
	require.NoError(t, f.server.UpdateGroup(g))
	// Native server RecvMsg is blocked here; closing the owned physical TLS
	// transport must cancel it, return the handler, and discard its spool.
	select {
	case <-f.service.ended:
	case <-ctx.Done():
		t.Fatal("native stream did not terminate")
	}
	require.NoFileExists(t, path)
	require.Eventually(t, func() bool {
		select {
		case <-h.Done():
			return true
		default:
			return false
		}
	}, testTimeout, time.Millisecond)
	require.Error(t, stream.SendMsg(request(Replication)))
	require.Error(t, stream.RecvMsg(&reply))
	require.Equal(t, int64(1), f.service.delivered.Load())
}

func TestNativeOldBinaryHasNoBeginService(t *testing.T) {
	f := setupNative(t, fixtureOptions{old: true})
	cc := f.dial(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, err := f.client.Handshake(ctx, cc, testGroup, f.serverNode)
	require.Error(t, err)
	require.Zero(t, f.client.Counts().Active)
	// A CA certificate is NOT attestation of which program is running.
}

func TestFrameDecoderRejectsTrailingPayload(t *testing.T) {
	b := request(Vote)
	b.Body = append(b.Body, 0)
	_, err := decodeMessage(b)
	require.Error(t, err)
}
