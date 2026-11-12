package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const bootstrapGateTestToken = "test-startup-control-credential"

type bootstrapGatePKI struct {
	ca    *x509.Certificate
	key   ed25519.PrivateKey
	roots *x509.CertPool
}

func newBootstrapGatePKI(t *testing.T) bootstrapGatePKI {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bootstrap test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return bootstrapGatePKI{ca: cert, key: key, roots: roots}
}

func (p bootstrapGatePKI) certificate(t *testing.T, serial int64) (tls.Certificate, raftbootstrap.Digest) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"server.test", "client.test"},
		NotBefore: p.ca.NotBefore, NotAfter: p.ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, p.ca, pub, p.key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der, p.ca.Raw}, PrivateKey: key, Leaf: leaf}, sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
}

type bootstrapGateService struct {
	pb.UnimplementedRawKVServer
	pb.UnimplementedDistributionServer
	pb.UnimplementedInternalServer
	pb.UnimplementedEncryptionAdminServer
	calls  atomic.Int64
	action func(context.Context) error
}

func (s *bootstrapGateService) RegisterEncryptionWriter(ctx context.Context, _ *pb.RegisterEncryptionWriterRequest) (*pb.RegisterEncryptionWriterResponse, error) {
	if err := s.run(ctx); err != nil {
		return nil, err
	}
	return &pb.RegisterEncryptionWriterResponse{AppliedIndex: 1}, nil
}

func (s *bootstrapGateService) run(ctx context.Context) error {
	s.calls.Add(1)
	if s.action != nil {
		return s.action(ctx)
	}
	return nil
}

func (s *bootstrapGateService) RawLatestCommitTS(ctx context.Context, request *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	if err := s.run(ctx); err != nil {
		return nil, err
	}
	return &pb.RawLatestCommitTSResponse{GroupId: request.GroupId, LeaderFenced: true}, nil
}

func (s *bootstrapGateService) GetTimestamp(ctx context.Context, _ *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	if err := s.run(ctx); err != nil {
		return nil, err
	}
	return &pb.GetTimestampResponse{Timestamp: 100, Count: 1}, nil
}

func (s *bootstrapGateService) GetPrepareEvidenceState(ctx context.Context, _ *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
	if err := s.run(ctx); err != nil {
		return nil, err
	}
	return &pb.PrepareEvidenceStateResponse{Supported: true}, nil
}

func (s *bootstrapGateService) ForwardAdminProposal(ctx context.Context, _ *pb.ForwardAdminProposalRequest) (*pb.ForwardAdminProposalResponse, error) {
	if err := s.run(ctx); err != nil {
		return nil, err
	}
	return &pb.ForwardAdminProposalResponse{CommitIndex: 1}, nil
}

type bootstrapGateFixture struct {
	a       *raftadmission.Arbiter
	server  *peersession.Owner
	client  *peersession.Owner
	cc      *grpc.ClientConn
	handle  *peersession.Handle
	service *bootstrapGateService
	handled atomic.Bool
	group   uint64
}

func newBootstrapGateFixture(t *testing.T, group uint64, stages ...peersession.Stage) *bootstrapGateFixture {
	t.Helper()
	pki := newBootstrapGatePKI(t)
	serverCert, serverKey := pki.certificate(t, 2)
	clientCert, clientKey := pki.certificate(t, 3)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cluster := raftbootstrap.UUID{1}
	local := peersession.Node{Cluster: cluster, Member: 1, Address: lis.Addr().String(), Key: serverKey}
	remote := peersession.Node{Cluster: cluster, Member: 2, Address: "client.test:9001", Key: clientKey}
	provisions := []peersession.Provision{{Node: local, Groups: []uint64{group}}, {Node: remote, Groups: []uint64{group}}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	methods := clusterbootstrap.BootstrapSharedGRPCMethods(group)
	server, err := peersession.NewOwner(peersession.Config{Local: local, Provisioned: provisions, Versions: versions, Methods: methods})
	require.NoError(t, err)
	client, err := peersession.NewOwner(peersession.Config{Local: remote, Provisioned: provisions, Versions: versions, Methods: methods})
	require.NoError(t, err)
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
		{ID: 1, Address: local.Address, PublicKeyDigest: local.Key}, {ID: 2, Address: remote.Address, PublicKeyDigest: remote.Key}}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	policy := raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: raftbootstrap.Identity{Cluster: cluster, Group: group},
		BaselineDigest: digest, Minimum: versions, ActivationIndex: 1, Generation: 1, LastConfigurationIndex: 1, Configuration: configuration}
	policyDigest, err := raftbootstrap.PolicyDigest(policy)
	require.NoError(t, err)
	stage := peersession.ControlReady
	if len(stages) != 0 {
		stage = stages[0]
	}
	projection := peersession.Group{ID: group, Generation: 1, Configuration: configuration, Policy: policyDigest,
		Floor: raftbootstrap.Floor{ActivationIndex: 1, Generation: 1}, Minimum: versions, State: peersession.Intact,
		Stage: stage, Pending: peersession.PendingClear, MarkerReady: true}
	require.NoError(t, server.UpdateGroup(projection))
	require.NoError(t, client.UpdateGroup(projection))
	a, err := raftadmission.New(raftadmission.Config{Cluster: cluster, DefaultGroup: group, Groups: []uint64{group}, ValidatePayload: clusterbootstrap.BootstrapPayloadValidator(cluster)})
	require.NoError(t, err)
	require.NoError(t, a.WithNative(t.Context(), group, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: 1, Role: raftengine.StateLeader, LeaderID: 1, ConfigurationIndex: 1})
		return nil
	}))
	ticket, err := a.Ticket(group)
	require.NoError(t, err)
	registration := raftadmission.RegistrationNotConfigured
	if stage == peersession.IdentityReady {
		registration = raftadmission.RegistrationUnknown
	}
	require.NoError(t, a.PublishRecovery(ticket, policy, 1, registration))
	f := &bootstrapGateFixture{a: a, server: server, client: client, service: &bootstrapGateService{}, group: group}
	options, err := server.ServerOptions(&tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: pki.roots,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	require.NoError(t, err)
	options = append(options, grpc.ChainUnaryInterceptor(clusterbootstrap.BootstrapRegistrationReceiverInterceptor(group), func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		reply, handled, err := clusterbootstrap.HandleBootstrapStartupControl(ctx, bootstrapGateTestToken, a, info.FullMethod, request, next)
		f.handled.Store(handled)
		if handled {
			return reply, err
		}
		// The normal/public lane is still closed; only handshake may pass.
		if info.FullMethod == "/elastickv.peersession.v1.PeerSession/Begin" || info.FullMethod == "/elastickv.peersession.v1.PeerSession/Finish" {
			return next(ctx, request)
		}
		return nil, status.Error(codes.Unavailable, "public data is not ready")
	}))
	gs := grpc.NewServer(options...)
	server.Register(gs)
	pb.RegisterRawKVServer(gs, f.service)
	pb.RegisterDistributionServer(gs, f.service)
	pb.RegisterInternalServer(gs, f.service)
	pb.RegisterEncryptionAdminServer(gs, f.service)
	done := make(chan struct{})
	go func() { defer close(done); _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
		<-done
		client.Close()
		server.Close()
		a.Close()
	})
	tc, err := client.TransportCredentials(&tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pki.roots, MinVersion: tls.VersionTLS13}, false)
	require.NoError(t, err)
	f.cc, err = grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(tc), grpc.WithAuthority("server.test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.cc.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	f.handle, err = client.Handshake(ctx, f.cc, group, local)
	require.NoError(t, err)
	return f
}

func bootstrapGateContext(t *testing.T, values ...string) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	return metadata.NewOutgoingContext(ctx, metadata.MD{clusterbootstrap.BootstrapControlHeader: values}), cancel
}

func TestBootstrapControlGateTokenExactAndDedicated(t *testing.T) {
	t.Parallel()
	for _, values := range [][]string{nil, {""}, {"wrong"}, {bootstrapGateTestToken + " "},
		{bootstrapGateTestToken, bootstrapGateTestToken}} {
		ctx := metadata.NewIncomingContext(t.Context(), metadata.MD{clusterbootstrap.BootstrapControlHeader: values})
		require.False(t, clusterbootstrap.BootstrapStartupTokenMatches(ctx, bootstrapGateTestToken))
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(clusterbootstrap.BootstrapControlHeader, bootstrapGateTestToken))
	require.True(t, clusterbootstrap.BootstrapStartupTokenMatches(ctx, bootstrapGateTestToken))
	require.False(t, clusterbootstrap.BootstrapStartupTokenMatches(ctx, ""))
	for _, header := range []string{"authorization", "x-elastickv-migration-token", "x-elastickv-s3-blob-token"} {
		ctx = metadata.NewIncomingContext(t.Context(), metadata.Pairs(header, bootstrapGateTestToken))
		require.False(t, clusterbootstrap.BootstrapStartupTokenMatches(ctx, bootstrapGateTestToken))
	}
}

func TestBootstrapControlGateHeadersCannotMintReceipt(t *testing.T) {
	t.Parallel()
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(clusterbootstrap.BootstrapControlHeader, bootstrapGateTestToken,
		"x-elastickv-peer-session", "forged", "x-elastickv-peer-proof", "forged"))
	_, handled, err := clusterbootstrap.HandleBootstrapStartupControl(ctx, bootstrapGateTestToken, nil,
		pb.Distribution_GetTimestamp_FullMethodName, &pb.GetTimestampRequest{}, nil)
	require.NoError(t, err)
	require.False(t, handled)
}

func TestBootstrapControlGateTLSBeforeDataReadyExactLanes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		method  string
		group   uint64
		request any
		reply   any
		payload []byte
	}{
		{pb.RawKV_RawLatestCommitTS_FullMethodName, 17, &pb.RawLatestCommitTSRequest{GroupId: 17}, &pb.RawLatestCommitTSResponse{}, nil},
		{pb.Distribution_GetTimestamp_FullMethodName, 0, &pb.GetTimestampRequest{Count: 1}, &pb.GetTimestampResponse{}, []byte("\x07TSOC\x01")},
		{pb.Internal_GetPrepareEvidenceState_FullMethodName, 17, &pb.PrepareEvidenceStateRequest{}, &pb.PrepareEvidenceStateResponse{}, nil},
		{pb.Internal_ForwardAdminProposal_FullMethodName, 17, &pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceFence(0)}, &pb.ForwardAdminProposalResponse{}, kv.EncodePrepareEvidenceFence(0)},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			f := newBootstrapGateFixture(t, tt.group)
			require.Equal(t, peersession.ControlReady, f.a.Stage(tt.group))
			_, _, err := f.a.Begin(t.Context(), raftadmission.Data)
			require.Error(t, err)
			f.service.action = func(ctx context.Context) error {
				if tt.payload != nil {
					_, err := f.a.BindProposal(ctx, tt.group, tt.payload)
					return err
				}
				return f.a.ValidateResult(ctx)
			}
			ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
			defer cancel()
			require.NoError(t, f.handle.Invoke(ctx, f.cc, tt.method, tt.request, tt.reply))
			require.True(t, f.handled.Load())
			require.EqualValues(t, 1, f.service.calls.Load())
		})
	}
}

func TestBootstrapControlGateTLSFailuresNeverFallBack(t *testing.T) {
	t.Parallel()
	f := newBootstrapGateFixture(t, 17)
	for _, values := range [][]string{nil, {""}, {"wrong"}, {bootstrapGateTestToken, bootstrapGateTestToken}} {
		ctx, cancel := bootstrapGateContext(t, values...)
		var reply pb.PrepareEvidenceStateResponse
		err := f.handle.Invoke(ctx, f.cc, pb.Internal_GetPrepareEvidenceState_FullMethodName, &pb.PrepareEvidenceStateRequest{}, &reply)
		cancel()
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.True(t, f.handled.Load())
	}
	require.EqualValues(t, 0, f.service.calls.Load())
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	var reply pb.PrepareEvidenceStateResponse
	// Even on the existing mTLS connection, a token alone is ordinary public.
	require.Equal(t, codes.Unavailable, status.Code(f.cc.Invoke(ctx, pb.Internal_GetPrepareEvidenceState_FullMethodName, &pb.PrepareEvidenceStateRequest{}, &reply)))
	require.False(t, f.handled.Load())
	require.EqualValues(t, 0, f.service.calls.Load())
}

func TestBootstrapControlGateTLSRevocationDiscardsSuccess(t *testing.T) {
	t.Parallel()
	f := newBootstrapGateFixture(t, 17)
	f.service.action = func(context.Context) error { f.a.Suspend(17); return nil }
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	var reply pb.ForwardAdminProposalResponse
	require.Error(t, f.handle.Invoke(ctx, f.cc, pb.Internal_ForwardAdminProposal_FullMethodName,
		&pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceSeal()}, &reply))
	require.Zero(t, reply.CommitIndex)
	require.True(t, f.handled.Load())
	require.EqualValues(t, 1, f.service.calls.Load())
}

func TestBootstrapControlGateDescriptorDoesNotExpandAuthority(t *testing.T) {
	t.Parallel()
	for _, method := range []string{pb.RawKV_RawGet_FullMethodName, pb.Internal_Forward_FullMethodName,
		pb.EncryptionAdmin_RotateDEK_FullMethodName, pb.Admin_BeginBackup_FullMethodName, "/unknown/Control"} {
		_, _, ok := clusterbootstrap.BootstrapStartupControlDescriptor(17, method)
		require.False(t, ok)
	}
}

func TestBootstrapControlGateTLSActivationUsesRecoveryBeforeControlReady(t *testing.T) {
	t.Parallel()
	f := newBootstrapGateFixture(t, 17, peersession.IdentityReady)
	require.Equal(t, peersession.IdentityReady, f.a.Stage(17))
	_, _, err := f.a.Begin(t.Context(), raftadmission.Control)
	require.Error(t, err)
	payload := bootstrapPayloadActivation(t, 17)
	descriptor := bootstrapMethodForTest(t, 17, pb.Internal_ForwardAdminProposal_FullMethodName)
	f.service.action = func(ctx context.Context) error {
		receipt := peersession.CapturedReceipt(ctx)
		if receipt == nil || receipt.Message().Traffic != peersession.PolicyControl {
			return status.Error(codes.Internal, "activation receipt must retain PolicyControl")
		}
		// Reusing a real PolicyControl receipt for a prepare Control request
		// must fail descriptor-derived traffic comparison, not gain authority.
		if err := clusterbootstrap.ValidateBootstrapStartupControl(ctx, bootstrapGateTestToken, descriptor.Name,
			&pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceSeal()}, receipt, descriptor); err == nil {
			return status.Error(codes.Internal, "policy receipt authorized a prepare control")
		}
		if _, err := f.a.BindProposal(ctx, 17, payload); err != nil {
			return err
		}
		if _, err := f.a.BindProposal(ctx, 17, kv.EncodePrepareEvidenceSeal()); err == nil {
			return status.Error(codes.Internal, "recovery lane accepted a prepare control")
		}
		return nil
	}
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	var reply pb.ForwardAdminProposalResponse
	require.NoError(t, f.handle.Invoke(ctx, f.cc, pb.Internal_ForwardAdminProposal_FullMethodName,
		&pb.ForwardAdminProposalRequest{Payload: payload}, &reply))
	require.True(t, f.handled.Load())
	require.EqualValues(t, 1, reply.CommitIndex)
	require.EqualValues(t, 1, f.service.calls.Load())
}

func TestBootstrapControlGateTLSActivationStillRequiresDedicatedToken(t *testing.T) {
	t.Parallel()
	f := newBootstrapGateFixture(t, 17, peersession.IdentityReady)
	payload := bootstrapPayloadActivation(t, 17)
	for _, values := range [][]string{nil, {""}, {"wrong"}, {bootstrapGateTestToken, bootstrapGateTestToken}} {
		ctx, cancel := bootstrapGateContext(t, values...)
		var reply pb.ForwardAdminProposalResponse
		err := f.handle.Invoke(ctx, f.cc, pb.Internal_ForwardAdminProposal_FullMethodName,
			&pb.ForwardAdminProposalRequest{Payload: payload}, &reply)
		cancel()
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.True(t, f.handled.Load())
	}
	require.EqualValues(t, 0, f.service.calls.Load())
}

func TestBootstrapControlGateTLSReceiptRevocationWaitsForNativeAction(t *testing.T) {
	t.Parallel()
	t.Run("prepare control", func(t *testing.T) {
		testBootstrapGateReceiptRevocation(t, peersession.ControlReady, kv.EncodePrepareEvidenceSeal())
	})
	t.Run("initial activation", func(t *testing.T) {
		testBootstrapGateReceiptRevocation(t, peersession.IdentityReady, bootstrapPayloadActivation(t, 17))
	})
}

func testBootstrapGateReceiptRevocation(t *testing.T, stage peersession.Stage, payload []byte) {
	t.Helper()
	f := newBootstrapGateFixture(t, 17, stage)
	entered := make(chan struct{})
	release := make(chan struct{})
	revoked := make(chan struct{})
	stale := make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f.service.action = func(ctx context.Context) error {
		bound, err := f.a.BindProposal(ctx, 17, payload)
		if err != nil {
			return err
		}
		err = f.a.WithNative(bound, 17, func(scope raftengine.NativeScope) error {
			if err := scope.Check(bound, raftengine.NativeRequest{
				Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256(payload),
			}); err != nil {
				return err
			}
			close(entered)
			<-release // Models the terminal RawNode action after Check, still under both locks.
			return nil
		})
		if err != nil {
			return err
		}
		<-revoked
		err = f.a.ValidateResult(bound)
		stale <- err
		return err
	}
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	callDone := make(chan error, 1)
	go func() {
		var reply pb.ForwardAdminProposalResponse
		callDone <- f.handle.Invoke(ctx, f.cc, pb.Internal_ForwardAdminProposal_FullMethodName,
			&pb.ForwardAdminProposalRequest{Payload: payload}, &reply)
	}()
	awaitBootstrapGateResult(t, ctx, entered, "native action did not pass Check")
	revokeStarted := make(chan struct{})
	go func() { close(revokeStarted); f.server.RevokeGroup(17); close(revoked) }()
	<-revokeStarted
	select {
	case <-revoked:
		t.Fatal("receipt revocation escaped while the checked native action was still running")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	awaitBootstrapGateResult(t, ctx, revoked, "receipt revocation did not complete after native action")
	require.Error(t, awaitBootstrapGateResult(t, ctx, stale, "old operation did not finish"), "old operation must fail after receipt revocation")
	require.Error(t, awaitBootstrapGateResult(t, ctx, callDone, "RPC did not finish after receipt revocation"))
}

func awaitBootstrapGateResult[T any](t *testing.T, ctx context.Context, result <-chan T, stage string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		t.Fatalf("%s: %v", stage, ctx.Err())
		var zero T
		return zero
	}
}
