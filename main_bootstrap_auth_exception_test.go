package main

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/peersession/wire"
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

type bootstrapAuthService struct {
	*bootstrapGateService
	pb.UnimplementedAdminServer
}

type bootstrapAuthFixture struct {
	*bootstrapGateFixture
	authCalls atomic.Int64
	gateCalls atomic.Int64
}

type bootstrapAuthRegistrationOptions struct {
	prepared       bool
	receiverGroup  *uint64
	omitReceiver   bool
	inspectPayload func(raftadmission.Kind, uint64, []byte, raftbootstrap.Digest) (raftadmission.PayloadInspection, error)
}

func bootstrapAuthRegistrationInterceptor(group uint64, options bootstrapAuthRegistrationOptions) grpc.UnaryServerInterceptor {
	if options.omitReceiver {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return next(ctx, req)
		}
	}
	if options.receiverGroup != nil {
		group = *options.receiverGroup
	}
	return bootstrapRegistrationReceiverInterceptor(group)
}

// Exercise the production order on a real mTLS/session connection:
// owner -> wrapped original AdminTokenAuth -> startup/DataReady gate -> handler.
func newBootstrapAuthFixture(t *testing.T, group uint64, stage peersession.Stage, rewrite func(any) any, registrationOptions ...bootstrapAuthRegistrationOptions) *bootstrapAuthFixture {
	t.Helper()
	var registrationConfig bootstrapAuthRegistrationOptions
	if len(registrationOptions) != 0 {
		registrationConfig = registrationOptions[0]
	}
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
	methods := bootstrapSharedGRPCMethods(group)
	server, err := peersession.NewOwner(peersession.Config{Local: local, Provisioned: provisions, Versions: versions, Methods: methods,
		PublicMethods: []string{pb.Admin_StreamEvents_FullMethodName, pb.RawKV_RawGet_FullMethodName}})
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
	projection := peersession.Group{ID: group, Generation: 1, Configuration: configuration, Policy: policyDigest,
		Floor: raftbootstrap.Floor{ActivationIndex: 1, Generation: 1}, Minimum: versions, State: peersession.Intact,
		Stage: stage, Pending: peersession.PendingClear, MarkerReady: true}
	preactivation := registrationConfig.prepared
	if preactivation {
		projection.Policy, projection.Floor = raftbootstrap.Digest{}, raftbootstrap.Floor{}
	}
	require.NoError(t, server.UpdateGroup(projection))
	require.NoError(t, client.UpdateGroup(projection))
	a, err := raftadmission.New(raftadmission.Config{Cluster: cluster, DefaultGroup: group, Groups: []uint64{group},
		ValidatePayload: bootstrapPayloadValidator(cluster), InspectPayload: registrationConfig.inspectPayload})
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
	if preactivation {
		publishBootstrapRegistrationPreparedFixture(t, a, ticket, policy, serverCert)
	} else {
		require.NoError(t, a.PublishRecovery(ticket, policy, 1, registration))
	}
	f := &bootstrapAuthFixture{bootstrapGateFixture: &bootstrapGateFixture{a: a, server: server, client: client,
		service: &bootstrapGateService{}, group: group}}
	domain := &bootstrapDomain{admission: a, startupToken: bootstrapGateTestToken}
	options, err := server.ServerOptions(&tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: pki.roots,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	require.NoError(t, err)
	admin, adminStream := adapter.AdminTokenAuth("independent-admin-credential")
	countedAdmin := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		f.authCalls.Add(1)
		return admin(ctx, request, info, next)
	}
	options = append(options, grpc.ChainUnaryInterceptor(
		bootstrapAuthRegistrationInterceptor(group, registrationConfig), bootstrapAuthRewrite(rewrite), bootstrapStartupAuthException(domain, countedAdmin), f.publicGate,
	), grpc.ChainStreamInterceptor(adminStream))
	gs := grpc.NewServer(options...)
	server.Register(gs)
	service := &bootstrapAuthService{bootstrapGateService: f.service}
	pb.RegisterRawKVServer(gs, service)
	pb.RegisterDistributionServer(gs, service)
	pb.RegisterInternalServer(gs, service)
	pb.RegisterAdminServer(gs, service)
	pb.RegisterEncryptionAdminServer(gs, service)
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
	f.authCalls.Store(0)
	f.gateCalls.Store(0)
	return f
}

func bootstrapAuthRewrite(rewrite func(any) any) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if rewrite != nil && (info.FullMethod == pb.Internal_ForwardAdminProposal_FullMethodName || info.FullMethod == pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName) {
			request = rewrite(request)
		}
		return next(ctx, request)
	}
}

func (f *bootstrapAuthFixture) publicGate(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if info.FullMethod == wire.PeerSession_Begin_FullMethodName || info.FullMethod == wire.PeerSession_Finish_FullMethodName {
		return next(ctx, request)
	}
	f.gateCalls.Add(1)
	reply, handled, err := handleBootstrapStartupControl(ctx, bootstrapGateTestToken, f.a, info.FullMethod, request, next)
	if handled {
		return reply, err
	}
	return nil, status.Error(codes.Unavailable, "ordinary DataReady admission remains closed")
}

func TestBootstrapAuthExceptionTLSExactStartupSkipsOnlyAdmin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, method string
		group        uint64
		stage        peersession.Stage
		request      any
		reply        any
	}{
		{"activation", pb.Internal_ForwardAdminProposal_FullMethodName, 17, peersession.IdentityReady,
			&pb.ForwardAdminProposalRequest{Payload: bootstrapPayloadActivation(t, 17)}, &pb.ForwardAdminProposalResponse{}},
		{"prepare", pb.Internal_ForwardAdminProposal_FullMethodName, 17, peersession.ControlReady,
			&pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceSeal()}, &pb.ForwardAdminProposalResponse{}},
		{"prepare state", pb.Internal_GetPrepareEvidenceState_FullMethodName, 17, peersession.ControlReady,
			&pb.PrepareEvidenceStateRequest{}, &pb.PrepareEvidenceStateResponse{}},
		{"floor", pb.RawKV_RawLatestCommitTS_FullMethodName, 17, peersession.ControlReady,
			&pb.RawLatestCommitTSRequest{GroupId: 17}, &pb.RawLatestCommitTSResponse{}},
		{"allocator", pb.Distribution_GetTimestamp_FullMethodName, 0, peersession.ControlReady,
			&pb.GetTimestampRequest{Count: 1}, &pb.GetTimestampResponse{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapAuthFixture(t, tc.group, tc.stage, nil)
			ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
			defer cancel()
			require.NoError(t, f.handle.Invoke(ctx, f.cc, tc.method, tc.request, tc.reply))
			require.Zero(t, f.authCalls.Load())
			require.EqualValues(t, 1, f.gateCalls.Load())
			require.EqualValues(t, 1, f.service.calls.Load())
		})
	}
}

func TestBootstrapAuthExceptionTLSWrongCredentialUsesOriginalAuth(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.ControlReady, nil)
	for _, values := range [][]string{nil, {""}, {"wrong"}, {bootstrapGateTestToken, bootstrapGateTestToken}} {
		ctx, cancel := bootstrapGateContext(t, values...)
		var reply pb.PrepareEvidenceStateResponse
		err := f.handle.Invoke(ctx, f.cc, pb.Internal_GetPrepareEvidenceState_FullMethodName, &pb.PrepareEvidenceStateRequest{}, &reply)
		cancel()
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}
	require.EqualValues(t, 4, f.authCalls.Load())
	require.Zero(t, f.gateCalls.Load())
	require.Zero(t, f.service.calls.Load())
}

func TestBootstrapAuthExceptionTLSCapturedReceiptCannotAuthorizeDifferentPayload(t *testing.T) {
	t.Parallel()
	unknown := &pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceSeal()}
	unknown.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
	for name, invalid := range map[string]any{
		"backup":         &pb.ForwardAdminProposalRequest{Payload: kv.EncodeBackupReleaseEntry(kv.BackupReleaseEntry{})},
		"watermark":      &pb.ForwardAdminProposalRequest{Payload: kv.EncodeWatermarkAdvance(10)},
		"data":           &pb.RawPutRequest{Key: []byte("key"), Value: []byte("value")},
		"unknown fields": unknown,
		"force entry":    &pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceSeal(), ForceEntry: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newBootstrapAuthFixture(t, 17, peersession.ControlReady, func(any) any { return invalid })
			ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
			defer cancel()
			var reply pb.ForwardAdminProposalResponse
			err := f.handle.Invoke(ctx, f.cc, pb.Internal_ForwardAdminProposal_FullMethodName,
				&pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceSeal()}, &reply)
			require.Equal(t, codes.Unauthenticated, status.Code(err))
			require.EqualValues(t, 1, f.authCalls.Load())
			require.Zero(t, f.gateCalls.Load())
			require.Zero(t, f.service.calls.Load())
		})
	}
}

func TestBootstrapAuthExceptionTLSPublicTokenIsNotAdminAuthority(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.ControlReady, nil)
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	var reply pb.PrepareEvidenceStateResponse
	err := f.cc.Invoke(ctx, pb.Internal_GetPrepareEvidenceState_FullMethodName, &pb.PrepareEvidenceStateRequest{}, &reply)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.EqualValues(t, 1, f.authCalls.Load())
	require.Zero(t, f.gateCalls.Load())
	admin := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer independent-admin-credential")
	err = f.cc.Invoke(admin, pb.Internal_GetPrepareEvidenceState_FullMethodName, &pb.PrepareEvidenceStateRequest{}, &reply)
	require.Equal(t, codes.Unavailable, status.Code(err), "ordinary admin authorization does not open DataReady")
	require.EqualValues(t, 2, f.authCalls.Load())
	require.EqualValues(t, 1, f.gateCalls.Load())
	require.Zero(t, f.service.calls.Load())
}

func TestBootstrapAuthExceptionTLSAdmissionStillRequired(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.ControlReady, nil)
	f.a.Suspend(17)
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	var reply pb.PrepareEvidenceStateResponse
	require.Error(t, f.handle.Invoke(ctx, f.cc, pb.Internal_GetPrepareEvidenceState_FullMethodName, &pb.PrepareEvidenceStateRequest{}, &reply))
	require.Zero(t, f.authCalls.Load(), "the typed/token exception only removes the unrelated admin dependency")
	require.EqualValues(t, 1, f.gateCalls.Load())
	require.Zero(t, f.service.calls.Load(), "A.Enter must still refuse the suspended domain")
}

func TestBootstrapAuthExceptionTLSNoStreamException(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.ControlReady, nil)
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	stream, err := pb.NewAdminClient(f.cc).StreamEvents(ctx, &pb.StreamEventsRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Zero(t, f.service.calls.Load())
}

func TestBootstrapAuthExceptionWithoutDomainKeepsOriginalAuth(t *testing.T) {
	t.Parallel()
	called := false
	denied := status.Error(codes.Unauthenticated, "original auth")
	original := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		called = true
		return nil, denied
	}
	wrapped := bootstrapStartupAuthException(nil, original)
	_, err := wrapped(t.Context(), &pb.PrepareEvidenceStateRequest{}, &grpc.UnaryServerInfo{FullMethod: pb.Internal_GetPrepareEvidenceState_FullMethodName},
		func(context.Context, any) (any, error) {
			t.Fatal("original authentication was bypassed")
			return nil, nil
		})
	require.ErrorIs(t, err, denied)
	require.True(t, called)
}
