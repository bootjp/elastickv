package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const bootstrapNetworkTestGroup = 17

type bootstrapNetworkFixture struct {
	network           *bootstrapNetwork
	sender, receiver  *bootstrapDomain
	config            *bootstrapPeerConfig
	address           string
	cache             *kv.GRPCConnCache
	service           *bootstrapGateService
	private, ordinary atomic.Int64
}

func bootstrapNetworkTestPolicy(t *testing.T, server, client peersession.Node) raftbootstrap.PolicyRecord {
	t.Helper()
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{server.Member, client.Member}}, Members: []raftbootstrap.Member{
		{ID: server.Member, Address: server.Address, PublicKeyDigest: server.Key}, {ID: client.Member, Address: client.Address, PublicKeyDigest: client.Key}}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	return raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: raftbootstrap.Identity{Cluster: server.Cluster, Group: bootstrapNetworkTestGroup},
		BaselineDigest: digest, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1, Generation: 1, LastConfigurationIndex: 1, Configuration: configuration}
}

func bootstrapNetworkTestDomain(t *testing.T, local peersession.Node, provisions []peersession.Provision, policy raftbootstrap.PolicyRecord) *bootstrapDomain {
	t.Helper()
	owner, err := peersession.NewOwner(peersession.Config{Local: local, Provisioned: provisions, Versions: policy.Minimum, Methods: bootstrapSharedGRPCMethods(bootstrapNetworkTestGroup)})
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	digest, err := raftbootstrap.PolicyDigest(policy)
	require.NoError(t, err)
	require.NoError(t, owner.UpdateGroup(peersession.Group{ID: bootstrapNetworkTestGroup, Generation: 1, Configuration: policy.Configuration, Policy: digest,
		Floor: raftbootstrap.Floor{ActivationIndex: 1, Generation: 1}, Minimum: policy.Minimum, State: peersession.Intact, Stage: peersession.ControlReady, Pending: peersession.PendingClear, MarkerReady: true}))
	a, err := raftadmission.New(raftadmission.Config{Cluster: policy.Identity.Cluster, DefaultGroup: bootstrapNetworkTestGroup, Groups: []uint64{bootstrapNetworkTestGroup}, ValidatePayload: bootstrapPayloadValidator(policy.Identity.Cluster)})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	require.NoError(t, a.WithNative(t.Context(), bootstrapNetworkTestGroup, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: 1, Role: raftengine.StateLeader, LeaderID: 1, ConfigurationIndex: 1})
		return nil
	}))
	ticket, err := a.Ticket(bootstrapNetworkTestGroup)
	require.NoError(t, err)
	require.NoError(t, a.PublishRecovery(ticket, policy, 1, raftadmission.RegistrationNotConfigured))
	return &bootstrapDomain{admission: a, owners: map[uint64]*peersession.Owner{bootstrapNetworkTestGroup: owner}, startupToken: bootstrapGateTestToken}
}

func newBootstrapNetworkFixture(t *testing.T, wrongSPKI bool) *bootstrapNetworkFixture {
	t.Helper()
	pki := newBootstrapGatePKI(t)
	serverCert, serverKey := pki.certificate(t, 2)
	clientCert, clientKey := pki.certificate(t, 3)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := peersession.Node{Cluster: raftbootstrap.UUID{1}, Member: 1, Address: listener.Addr().String(), Key: serverKey}
	client := peersession.Node{Cluster: server.Cluster, Member: 2, Address: "client.test:9001", Key: clientKey}
	provisions := []peersession.Provision{{Node: server, Groups: []uint64{bootstrapNetworkTestGroup}}, {Node: client, Groups: []uint64{bootstrapNetworkTestGroup}}}
	receiver := bootstrapNetworkTestDomain(t, server, provisions, bootstrapNetworkTestPolicy(t, server, client))
	expected := server
	if wrongSPKI {
		_, expected.Key = pki.certificate(t, 4)
	}
	clientProvisions := []peersession.Provision{{Node: expected, Groups: []uint64{bootstrapNetworkTestGroup}}, {Node: client, Groups: []uint64{bootstrapNetworkTestGroup}}}
	sender := bootstrapNetworkTestDomain(t, client, clientProvisions, bootstrapNetworkTestPolicy(t, expected, client))
	config := &bootstrapPeerConfig{local: client, provisions: clientProvisions, startupToken: bootstrapGateTestToken,
		clientTLS: &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "server.test", Certificates: []tls.Certificate{clientCert}, RootCAs: pki.roots}}
	f := &bootstrapNetworkFixture{sender: sender, receiver: receiver, config: config, address: server.Address, service: &bootstrapGateService{}}
	f.service.action = func(ctx context.Context) error {
		if peersession.CapturedReceipt(ctx) != nil {
			f.private.Add(1)
			return nil
		}
		f.ordinary.Add(1)
		md, _ := metadata.FromIncomingContext(ctx)
		for _, header := range []string{bootstrapControlHeader, "x-elastickv-peer-session", "x-elastickv-peer-proof"} {
			if len(md.Get(header)) != 0 {
				return errors.New("private metadata leaked onto ordinary transport")
			}
		}
		if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer ordinary-test-auth" {
			return errors.New("ordinary authorization was lost")
		}
		return nil
	}
	options, err := receiver.owners[bootstrapNetworkTestGroup].ServerOptions(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots})
	require.NoError(t, err)
	gate := &startupPublicKVGate{domain: receiver}
	gate.markReady()
	serverGRPC := grpc.NewServer(append(options, grpc.ChainUnaryInterceptor(bootstrapRegistrationReceiverInterceptor(bootstrapNetworkTestGroup), gate.unaryInterceptor), grpc.ChainStreamInterceptor(gate.streamInterceptor))...)
	receiver.owners[bootstrapNetworkTestGroup].Register(serverGRPC)
	pb.RegisterRawKVServer(serverGRPC, f.service)
	pb.RegisterDistributionServer(serverGRPC, f.service)
	pb.RegisterInternalServer(serverGRPC, f.service)
	pb.RegisterEncryptionAdminServer(serverGRPC, f.service)
	done := make(chan struct{})
	go func() { defer close(done); _ = serverGRPC.Serve(listener) }()
	t.Cleanup(func() { serverGRPC.Stop(); _ = listener.Close(); <-done })
	f.network, err = newBootstrapNetwork(t.Context(), sender, map[uint64]*bootstrapPeerConfig{bootstrapNetworkTestGroup: config})
	require.NoError(t, err)
	t.Cleanup(f.network.Close)
	f.cache = kv.NewGRPCConnCache(f.network.DialOptions)
	t.Cleanup(func() { require.NoError(t, f.cache.Close()) })
	return f
}

func (f *bootstrapNetworkFixture) ready(t *testing.T) {
	t.Helper()
	for _, domain := range []*bootstrapDomain{f.sender, f.receiver} {
		ticket, err := domain.admission.Ticket(bootstrapNetworkTestGroup)
		require.NoError(t, err)
		require.NoError(t, domain.catalogLoaded(ticket))
	}
	require.True(t, f.network.dataReady())
}

func bootstrapNetworkCallContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer ordinary-test-auth"), cancel
}

func TestBootstrapNetworkTLSStartupToOrdinary(t *testing.T) {
	f := newBootstrapNetworkFixture(t, false)
	ctx, cancel := bootstrapNetworkCallContext(t)
	defer cancel()
	conn, err := f.cache.ConnFor(f.address)
	require.NoError(t, err)
	request := &pb.RawLatestCommitTSRequest{GroupId: bootstrapNetworkTestGroup}
	_, err = pb.NewRawKVClient(conn).RawLatestCommitTS(ctx, request)
	require.Equal(t, codes.Unavailable, status.Code(err), "TLS alone cannot open DataReady")
	var reply pb.RawLatestCommitTSResponse
	handled, err := f.network.startup(ctx, bootstrapNetworkTestGroup, f.address, pb.RawKV_RawLatestCommitTS_FullMethodName, request, &reply)
	require.True(t, handled)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.private.Load())
	require.Zero(t, f.ordinary.Load())
	f.ready(t)
	handled, err = f.network.startup(ctx, bootstrapNetworkTestGroup, f.address, pb.RawKV_RawLatestCommitTS_FullMethodName, request, &reply)
	require.False(t, handled, "full process readiness chooses the original public path before sending")
	require.NoError(t, err)
	_, err = pb.NewRawKVClient(conn).RawLatestCommitTS(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.private.Load())
	require.EqualValues(t, 1, f.ordinary.Load())
}

func TestBootstrapNetworkTLSRejectsUnknownAndWrongSPKI(t *testing.T) {
	f := newBootstrapNetworkFixture(t, true)
	f.ready(t)
	_, err := f.cache.ConnFor("unprovisioned.test:1")
	require.ErrorIs(t, err, peersession.ErrDenied)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	conn, err := f.cache.ConnFor(f.address)
	require.NoError(t, err)
	_, err = pb.NewRawKVClient(conn).RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{GroupId: bootstrapNetworkTestGroup}, grpc.WaitForReady(false))
	require.Error(t, err, "a CA-valid wrong SPKI must not authenticate for the configured address")
	require.Zero(t, f.service.calls.Load())
}

func TestBootstrapNetworkPrivateFailureNeverFallsBack(t *testing.T) {
	f := newBootstrapNetworkFixture(t, false)
	f.sender.startupToken = "wrong-dedicated-credential"
	ctx, cancel := bootstrapNetworkCallContext(t)
	defer cancel()
	var reply pb.RawLatestCommitTSResponse
	handled, err := f.network.startup(ctx, bootstrapNetworkTestGroup, f.address, pb.RawKV_RawLatestCommitTS_FullMethodName,
		&pb.RawLatestCommitTSRequest{GroupId: bootstrapNetworkTestGroup}, &reply)
	require.True(t, handled)
	require.Error(t, err)
	require.Zero(t, f.ordinary.Load())
	require.Zero(t, f.service.calls.Load())
}

func TestBootstrapNetworkOrdinaryCallsConcurrentAndCloseJoins(t *testing.T) {
	f := newBootstrapNetworkFixture(t, false)
	f.ready(t)
	entered := make(chan struct{}, 2)
	f.service.action = func(ctx context.Context) error {
		entered <- struct{}{}
		<-ctx.Done()
		return errors.WithStack(ctx.Err())
	}
	ctx, cancel := bootstrapNetworkCallContext(t)
	defer cancel()
	conn, err := f.cache.ConnFor(f.address)
	require.NoError(t, err)
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := pb.NewRawKVClient(conn).RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{GroupId: bootstrapNetworkTestGroup})
			results <- err
		}()
	}
	for range 2 {
		awaitBootstrapGateResult(t, ctx, entered, "ordinary calls were serialized")
	}
	f.network.Close()
	for range 2 {
		require.Error(t, awaitBootstrapGateResult(t, ctx, results, "network Close did not join ordinary call"))
	}
	_, err = f.cache.ConnFor(f.address)
	require.Error(t, err)
	f.network.Close()
}

func TestBootstrapNetworkMissingInstallationFailsClosed(t *testing.T) {
	cache := newBootstrapRuntimeConnCache([]*raftGroupRuntime{{bootstrapDomain: &bootstrapDomain{}}})
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	_, err := cache.ConnFor("127.0.0.1:1")
	require.ErrorContains(t, err, "network was not installed")
}

func TestBootstrapNetworkSuccessThenRevocationIsUnknown(t *testing.T) {
	f := newBootstrapNetworkFixture(t, false)
	interceptor := f.network.unary(f.address)
	err := interceptor(t.Context(), pb.RawKV_RawLatestCommitTS_FullMethodName, nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			f.network.control.cancel()
			return nil
		})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
}

type bootstrapNetworkTestStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s bootstrapNetworkTestStream) Context() context.Context { return s.ctx }

func TestBootstrapNetworkCloseCancelsAndJoinsOrdinaryStream(t *testing.T) {
	f := newBootstrapNetworkFixture(t, false)
	stream, err := f.network.stream(f.address)(t.Context(), &grpc.StreamDesc{}, nil, "ordinary-stream",
		func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			return bootstrapNetworkTestStream{ctx: ctx}, nil
		})
	require.NoError(t, err)
	closed := make(chan struct{})
	go func() { f.network.Close(); close(closed) }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	awaitBootstrapGateResult(t, ctx, closed, "network Close did not join ordinary stream cancellation")
	require.Error(t, stream.Context().Err())
}

func TestBootstrapNetworkPrepareControlInheritsAllocatorBeforeDataReady(t *testing.T) {
	f := newBootstrapNetworkFixture(t, false)
	a := f.sender.admission
	require.False(t, f.network.dataReady())
	parent, finishParent, err := a.Enter(t.Context(), raftadmission.Allocator, bootstrapNetworkTestGroup)
	require.NoError(t, err)
	child, finishChild, err := f.sender.operationAdmission(parent, bootstrapNetworkTestGroup, kv.AdmissionControl)
	require.NoError(t, err)
	payload := kv.EncodePrepareEvidenceSeal()
	bound, err := a.BindProposal(child, bootstrapNetworkTestGroup, payload)
	require.NoError(t, err)
	require.NoError(t, a.WithNative(bound, bootstrapNetworkTestGroup, func(scope raftengine.NativeScope) error {
		return scope.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256(payload)})
	}))
	require.NoError(t, finishChild(nil))
	require.NoError(t, finishParent(nil))
}
