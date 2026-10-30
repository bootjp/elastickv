package adapter

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type migrationNativeAuthMode uint8

const (
	migrationNativeLegacy migrationNativeAuthMode = iota
	migrationNativeWrapped
	migrationNativePeerGated
	migrationNativePlaintext
)
const migrationNativeGroup = 17
const migrationNativeToken = "migration-native-test-token"

type migrationNativeAuthFixture struct {
	address                  string
	roots                    *x509.CertPool
	clientCertificate        tls.Certificate
	server                   *DistributionServer
	coordinator              *distributionCoordinatorStub
	serverOwner, clientOwner *peersession.Owner
	serverNode               peersession.Node
	auth                     chan credentials.AuthInfo
}

func newMigrationNativeAuthFixture(t *testing.T, mode migrationNativeAuthMode) *migrationNativeAuthFixture {
	t.Helper()
	serverCert := migrationAuthTestCertificate(t)
	clientCert := migrationAuthTestCertificate(t)
	roots := x509.NewCertPool()
	roots.AddCert(serverCert.Leaf)
	roots.AddCert(clientCert.Leaf)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	business, coordinator := proofFreeSplitRangeAuthServer(t)
	f := &migrationNativeAuthFixture{address: listener.Addr().String(), roots: roots, clientCertificate: clientCert, server: business, coordinator: coordinator, auth: make(chan credentials.AuthInfo, 32)}
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: roots}
	opts := migrationNativeServerOptions(t, f, mode, serverTLS, serverCert, clientCert)
	migrationUnary, migrationStream := SplitMigrationTokenAuth(migrationNativeToken, false, true)
	capture := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		p, ok := peer.FromContext(ctx)
		if ok {
			f.auth <- p.AuthInfo
		}
		return handler(ctx, req)
	}
	opts = append(opts, grpc.ChainUnaryInterceptor(capture, migrationUnary), grpc.ChainStreamInterceptor(migrationStream))
	server := grpc.NewServer(opts...)
	pb.RegisterDistributionServer(server, business)
	exportStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, exportStore.Close()) })
	require.NoError(t, exportStore.PutAt(t.Context(), []byte("a"), []byte("exported-value"), 10, 0))
	pb.RegisterInternalServer(server, NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(exportStore)))
	if mode == migrationNativePeerGated {
		f.serverOwner.Register(server)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		require.NoError(t, <-done)
		if f.serverOwner != nil {
			f.serverOwner.Close()
			require.Equal(t, peersession.Counts{}, f.serverOwner.Counts())
		}
		if f.clientOwner != nil {
			f.clientOwner.Close()
			require.Equal(t, peersession.Counts{}, f.clientOwner.Counts())
		}
	})
	return f
}

func migrationNativeServerOptions(t *testing.T, f *migrationNativeAuthFixture, mode migrationNativeAuthMode, cfg *tls.Config, serverCert, clientCert tls.Certificate) []grpc.ServerOption {
	t.Helper()
	switch mode {
	case migrationNativePlaintext:
		return nil
	case migrationNativeLegacy:
		return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(cfg))}
	case migrationNativeWrapped, migrationNativePeerGated:
		migrationNativeOwners(t, f, mode, serverCert, clientCert)
		opts, err := f.serverOwner.ServerOptions(cfg)
		require.NoError(t, err)
		return opts
	default:
		t.Fatal("unknown fixture mode")
		return nil
	}
}

func migrationNativeOwners(t *testing.T, f *migrationNativeAuthFixture, mode migrationNativeAuthMode, serverCert, clientCert tls.Certificate) {
	t.Helper()
	cluster, err := raftbootstrap.ParseUUID("17db1fe1-80ef-40bd-8f15-48f58db66c98")
	require.NoError(t, err)
	f.serverNode = peersession.Node{Cluster: cluster, Member: 1, Address: f.address, Key: sha256.Sum256(serverCert.Leaf.RawSubjectPublicKeyInfo)}
	client := peersession.Node{Cluster: cluster, Member: 2, Address: "client.test:9001", Key: sha256.Sum256(clientCert.Leaf.RawSubjectPublicKeyInfo)}
	provisioned := []peersession.Provision{{Node: f.serverNode, Groups: []uint64{migrationNativeGroup}}, {Node: client, Groups: []uint64{migrationNativeGroup}}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	public := []string{pb.Distribution_ListRoutes_FullMethodName}
	var methods []peersession.Method
	if mode == migrationNativeWrapped {
		public = append(public, pb.Distribution_SplitRange_FullMethodName, pb.Internal_ExportRangeVersions_FullMethodName)
	} else {
		methods = []peersession.Method{migrationNativeSplitMethod(), migrationNativeExportMethod()}
	}
	f.serverOwner, err = peersession.NewOwner(peersession.Config{Local: f.serverNode, Provisioned: provisioned, Versions: versions, PublicMethods: public, Methods: methods})
	require.NoError(t, err)
	f.clientOwner, err = peersession.NewOwner(peersession.Config{Local: client, Provisioned: provisioned, Versions: versions, Methods: methods})
	require.NoError(t, err)
	// This is explicit test-owned applied-state admission, not runtime P recovery.
	group := peersession.Group{ID: migrationNativeGroup, Generation: 1, Minimum: versions, State: peersession.Intact, Stage: peersession.ControlReady, Pending: peersession.PendingClear, MarkerReady: true,
		Policy: sha256.Sum256([]byte("test applied policy")), Floor: raftbootstrap.Floor{ActivationIndex: 2, Generation: 1},
		Configuration: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
			{ID: 1, Address: f.serverNode.Address, PublicKeyDigest: f.serverNode.Key}, {ID: 2, Address: client.Address, PublicKeyDigest: client.Key},
		}}}
	require.NoError(t, f.serverOwner.UpdateGroup(group))
	require.NoError(t, f.clientOwner.UpdateGroup(group))
}

func migrationNativeExportMethod() peersession.Method {
	return peersession.Method{Name: pb.Internal_ExportRangeVersions_FullMethodName, Request: func(v any) (peersession.Message, error) {
		if _, ok := v.(*pb.ExportRangeVersionsRequest); !ok {
			return peersession.Message{}, peersession.ErrFormat
		}
		return peersession.Message{Group: migrationNativeGroup, From: 2, To: 1, Traffic: peersession.Control}, nil
	}, Reply: func(req peersession.Message, v any) (peersession.Message, error) {
		if _, ok := v.(*pb.ExportRangeVersionsResponse); !ok {
			return peersession.Message{}, peersession.ErrFormat
		}
		req.From, req.To = req.To, req.From
		return req, nil
	}}
}

func migrationNativeExportRequest() *pb.ExportRangeVersionsRequest {
	return &pb.ExportRangeVersionsRequest{MaxCommitTs: 20, RouteStart: []byte("a"), RouteEnd: []byte("b"), RangeEnd: []byte("z"), KeyFamily: distribution.MigrationFamilyUser, ExcludeKnownInternal: true, MaxScannedBytes: 1 << 20}
}

func TestSplitMigrationNativeExportStreamTLSCompatibility(t *testing.T) {
	for _, mode := range []migrationNativeAuthMode{migrationNativeLegacy, migrationNativeWrapped, migrationNativePlaintext} {
		f := newMigrationNativeAuthFixture(t, mode)
		var cc *grpc.ClientConn
		if mode == migrationNativePlaintext {
			cc = f.dial(t, insecure.NewCredentials())
		} else {
			cc = f.publicTLSClient(t)
		}
		ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
		for _, token := range []string{"", "wrong", migrationNativeToken} {
			stream, err := pb.NewInternalClient(cc).ExportRangeVersions(migrationNativeTokenContext(ctx, token), migrationNativeExportRequest())
			var reply *pb.ExportRangeVersionsResponse
			if err == nil {
				reply, err = stream.Recv()
			}
			if token == migrationNativeToken && mode != migrationNativePlaintext {
				require.NoError(t, err)
				require.True(t, reply.GetDone())
				require.Len(t, reply.GetVersions(), 1)
				require.Equal(t, []byte("exported-value"), reply.GetVersions()[0].GetValue())
				_, err = stream.Recv()
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.Equal(t, codes.Unauthenticated, status.Code(err))
			}
		}
		cancel()
	}
}

func TestSplitMigrationNativeExportStreamPeerAndTokenChecks(t *testing.T) {
	f := newMigrationNativeAuthFixture(t, migrationNativePeerGated)
	cfg := &tls.Config{RootCAs: f.roots, Certificates: []tls.Certificate{f.clientCertificate}, MinVersion: tls.VersionTLS12}
	tc, err := f.clientOwner.TransportCredentials(cfg, false)
	require.NoError(t, err)
	cc := f.dial(t, tc)
	ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
	defer cancel()
	h, err := f.clientOwner.Handshake(ctx, cc, migrationNativeGroup, f.serverNode)
	require.NoError(t, err)
	for _, token := range []string{"wrong", migrationNativeToken} {
		stream, err := h.NewStream(ctx, cc, &grpc.StreamDesc{ServerStreams: true}, pb.Internal_ExportRangeVersions_FullMethodName, grpc.PerRPCCredentials(migrationNativeBearer{token: token}))
		require.NoError(t, err)
		err = stream.SendMsg(migrationNativeExportRequest())
		if err == nil {
			err = stream.CloseSend()
		}
		var reply pb.ExportRangeVersionsResponse
		if err == nil {
			err = stream.RecvMsg(&reply)
		}
		if token == migrationNativeToken {
			require.NoError(t, err)
			require.True(t, reply.GetDone())
			require.Len(t, reply.GetVersions(), 1)
			err = stream.RecvMsg(&reply)
			require.True(t, errors.Is(err, io.EOF))
		} else {
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		}
	}
}

func migrationNativeSplitMethod() peersession.Method {
	return peersession.Method{Name: pb.Distribution_SplitRange_FullMethodName, Request: func(v any) (peersession.Message, error) {
		req, ok := v.(*pb.SplitRangeRequest)
		if !ok || req.GetRouteId() == 0 || len(req.GetSplitKey()) == 0 {
			return peersession.Message{}, peersession.ErrFormat
		}
		return peersession.Message{Group: migrationNativeGroup, From: 2, To: 1, Traffic: peersession.Control}, nil
	}, Reply: func(req peersession.Message, v any) (peersession.Message, error) {
		if _, ok := v.(*pb.SplitRangeResponse); !ok {
			return peersession.Message{}, peersession.ErrFormat
		}
		req.From, req.To = req.To, req.From
		return req, nil
	}}
}

func (f *migrationNativeAuthFixture) dial(t *testing.T, tc credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(tc), grpc.WithAuthority("migration.test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
func (f *migrationNativeAuthFixture) publicTLSClient(t *testing.T) *grpc.ClientConn {
	t.Helper()
	return f.dial(t, credentials.NewTLS(&tls.Config{RootCAs: f.roots, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}))
}
func migrationNativeTokenContext(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
func migrationNativeRequest() *pb.SplitRangeRequest {
	return &pb.SplitRangeRequest{ExpectedCatalogVersion: 1, RouteId: 1, SplitKey: []byte("m")}
}

func TestSplitMigrationNativePublicTLSCompatibility(t *testing.T) {
	for _, mode := range []migrationNativeAuthMode{migrationNativeLegacy, migrationNativeWrapped} {
		t.Run(map[migrationNativeAuthMode]string{migrationNativeLegacy: "legacy", migrationNativeWrapped: "wrapped"}[mode], func(t *testing.T) {
			f := newMigrationNativeAuthFixture(t, mode)
			cc := f.publicTLSClient(t)
			client := pb.NewDistributionClient(cc)
			ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
			defer cancel()
			_, err := client.ListRoutes(ctx, &pb.ListRoutesRequest{})
			require.NoError(t, err)
			for _, token := range []string{"", "wrong"} {
				_, err = client.SplitRange(migrationNativeTokenContext(ctx, token), migrationNativeRequest())
				require.Equal(t, codes.Unauthenticated, status.Code(err))
			}
			assertSplitRangeAuthResult(t, f.server, f.coordinator, false, err)
			bad := migrationNativeRequest()
			bad.ExpectedCatalogVersion = 99
			_, err = client.SplitRange(migrationNativeTokenContext(ctx, migrationNativeToken), bad)
			require.Equal(t, codes.Aborted, status.Code(err), "valid transport/token cannot bypass existing catalog preconditions")
			_, err = client.SplitRange(migrationNativeTokenContext(ctx, migrationNativeToken), migrationNativeRequest())
			require.NoError(t, err)
			assertSplitRangeAuthResult(t, f.server, f.coordinator, true, err)
			var info credentials.TLSInfo
			select {
			case auth := <-f.auth:
				if mode == migrationNativeLegacy {
					native, ok := auth.(credentials.TLSInfo)
					require.True(t, ok)
					info = native
				} else {
					native, ok := auth.(*peersession.ServerAuthInfo)
					require.True(t, ok)
					info = native.TLSInfo
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, uint16(tls.VersionTLS12), info.State.Version)
			require.Empty(t, info.State.PeerCertificates)
			require.True(t, splitMigrationTLSProtectedTransport(info))
		})
	}
}

func TestSplitMigrationNativePlaintextRejectsToken(t *testing.T) {
	f := newMigrationNativeAuthFixture(t, migrationNativePlaintext)
	cc := f.dial(t, insecure.NewCredentials())
	ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
	defer cancel()
	_, err := pb.NewDistributionClient(cc).SplitRange(migrationNativeTokenContext(ctx, migrationNativeToken), migrationNativeRequest())
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.ErrorContains(t, err, "requires TLS")
	assertSplitRangeAuthResult(t, f.server, f.coordinator, false, err)
}

func TestSplitMigrationNativeWrappedTransportOwnership(t *testing.T) {
	for _, closeMode := range []string{"physical disconnect", "owner shutdown"} {
		t.Run(closeMode, func(t *testing.T) {
			f := newMigrationNativeAuthFixture(t, migrationNativeWrapped)
			cc := f.publicTLSClient(t)
			ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
			defer cancel()
			_, err := pb.NewDistributionClient(cc).ListRoutes(ctx, &pb.ListRoutesRequest{})
			require.NoError(t, err)
			var native *peersession.ServerAuthInfo
			select {
			case observed := <-f.auth:
				var ok bool
				native, ok = observed.(*peersession.ServerAuthInfo)
				require.True(t, ok)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			info, current := native.TransportTLSInfo()
			require.True(t, current)
			require.Equal(t, credentials.PrivacyAndIntegrity, info.SecurityLevel)
			require.Empty(t, info.State.PeerCertificates)
			require.Zero(t, f.serverOwner.Counts().Active)
			require.Zero(t, f.serverOwner.Counts().Pending)
			require.True(t, splitMigrationTLSProtectedTransport(native), "public TLS needs neither peer certificate nor Begin/Finish")

			forged := &peersession.ServerAuthInfo{TLSInfo: info}
			_, current = forged.TransportTLSInfo()
			require.False(t, current, "copied real TLS state cannot mint the private connection")
			require.False(t, splitMigrationTLSProtectedTransport(forged))

			// Keep the genuine private connection reference, but discard all public
			// inspection fields. The getter must use only owner-owned TLS state.
			inspectionChanged := *native
			inspectionChanged.TLSInfo = credentials.TLSInfo{}
			owned, current := inspectionChanged.TransportTLSInfo()
			require.True(t, current)
			require.Equal(t, info.State.CipherSuite, owned.State.CipherSuite)
			require.True(t, splitMigrationTLSProtectedTransport(&inspectionChanged))

			if closeMode == "physical disconnect" {
				require.NoError(t, cc.Close())
			} else {
				f.serverOwner.Close()
			}
			require.Eventually(t, func() bool {
				_, live := native.TransportTLSInfo()
				return !live
			}, migrationAuthNativeTimeout, time.Millisecond)
			require.False(t, splitMigrationTLSProtectedTransport(native))
			require.False(t, splitMigrationTLSProtectedTransport(&inspectionChanged))
			require.Eventually(t, func() bool { return f.serverOwner.Counts() == (peersession.Counts{}) }, migrationAuthNativeTimeout, time.Millisecond)
		})
	}
}

func TestSplitMigrationNativeClientAuthInfoCannotBecomeServerProof(t *testing.T) {
	f := newMigrationNativeAuthFixture(t, migrationNativeWrapped)
	tc, err := f.clientOwner.TransportCredentials(&tls.Config{
		RootCAs: f.roots, Certificates: []tls.Certificate{f.clientCertificate}, MinVersion: tls.VersionTLS12,
	}, false)
	require.NoError(t, err)
	cc := f.dial(t, tc)
	ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
	defer cancel()
	var observed peer.Peer
	_, err = pb.NewDistributionClient(cc).ListRoutes(ctx, &pb.ListRoutesRequest{}, grpc.Peer(&observed))
	require.NoError(t, err)
	client, ok := observed.AuthInfo.(*peersession.ClientAuthInfo)
	require.True(t, ok)
	// The two exported structs have convertible layouts. Even retaining a real
	// private client reference must NOT manufacture a native server transport.
	converted := (*peersession.ServerAuthInfo)(client)
	_, current := converted.TransportTLSInfo()
	require.False(t, current)
	require.False(t, splitMigrationTLSProtectedTransport(converted))
}

type migrationNativeBearer struct{ token string }

func (m migrationNativeBearer) RequireTransportSecurity() bool { return true }
func (m migrationNativeBearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + m.token}, nil
}

func TestSplitMigrationNativePeerAndTokenChecksAreConjunctive(t *testing.T) {
	f := newMigrationNativeAuthFixture(t, migrationNativePeerGated)
	ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
	defer cancel()
	public := f.publicTLSClient(t)
	_, err := pb.NewDistributionClient(public).SplitRange(migrationNativeTokenContext(ctx, migrationNativeToken), migrationNativeRequest())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "token cannot replace verified peer certificate/session")
	clientTLS := &tls.Config{RootCAs: f.roots, Certificates: []tls.Certificate{f.clientCertificate}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}
	tc, err := f.clientOwner.TransportCredentials(clientTLS, false)
	require.NoError(t, err)
	cc := f.dial(t, tc)
	_, err = pb.NewDistributionClient(cc).SplitRange(migrationNativeTokenContext(ctx, migrationNativeToken), migrationNativeRequest())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "mTLS plus token cannot replace Begin/Finish")
	h, err := f.clientOwner.Handshake(ctx, cc, migrationNativeGroup, f.serverNode)
	require.NoError(t, err)
	for _, token := range []string{"", "wrong"} {
		var reply pb.SplitRangeResponse
		if token == "" {
			err = h.Invoke(ctx, cc, pb.Distribution_SplitRange_FullMethodName, migrationNativeRequest(), &reply)
		} else {
			err = h.Invoke(ctx, cc, pb.Distribution_SplitRange_FullMethodName, migrationNativeRequest(), &reply, grpc.PerRPCCredentials(migrationNativeBearer{token: token}))
		}
		require.Equal(t, codes.Unauthenticated, status.Code(err), "valid peer session does not replace migration authorization")
	}
	var reply pb.SplitRangeResponse
	require.NoError(t, h.Invoke(ctx, cc, pb.Distribution_SplitRange_FullMethodName, migrationNativeRequest(), &reply, grpc.PerRPCCredentials(migrationNativeBearer{token: migrationNativeToken})))
	assertSplitRangeAuthResult(t, f.server, f.coordinator, true, nil)
	f.serverOwner.RevokeGroup(migrationNativeGroup)
	next := &pb.SplitRangeRequest{ExpectedCatalogVersion: 2, RouteId: reply.GetLeft().GetRouteId(), SplitKey: []byte("f")}
	require.Error(t, h.Invoke(ctx, cc, pb.Distribution_SplitRange_FullMethodName, next, &reply, grpc.PerRPCCredentials(migrationNativeBearer{token: migrationNativeToken})))
	assertSplitRangeAuthResult(t, f.server, f.coordinator, true, nil)
}
