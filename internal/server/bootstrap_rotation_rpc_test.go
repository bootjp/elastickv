package server

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	bootstrapRotationRPCAdmin = "external-rotation-admin-credential"
	bootstrapRotationRPCNode  = uint64(1<<48 + 42)
)

type bootstrapRotationRPCFixture struct {
	connection *grpc.ClientConn
	address    string
	clientTLS  *tls.Config
}

// Keep the real owner TLS and service-auth order. No catalog is loaded: the
// frontdoor must reach the controller without minting a generic Data handle.
func newBootstrapRotationRPCFixture(t *testing.T, run clusterbootstrap.BootstrapRotationRPCHandler, credentialOverride ...string) bootstrapRotationRPCFixture {
	t.Helper()
	pki := newBootstrapGatePKI(t)
	serverCert, serverKey := pki.certificate(t, 2)
	clientCert, clientKey := pki.certificate(t, 3)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cluster := raftbootstrap.UUID{1}
	local := peersession.Node{Cluster: cluster, Member: bootstrapRotationRPCNode, Address: lis.Addr().String(), Key: serverKey}
	remote := peersession.Node{Cluster: cluster, Member: 2, Address: "client.test:9001", Key: clientKey}
	owner, err := peersession.NewOwner(peersession.Config{Local: local,
		Provisioned: []peersession.Provision{{Node: local, Groups: []uint64{1}}, {Node: remote, Groups: []uint64{1}}},
		Versions:    raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		Methods:     clusterbootstrap.BootstrapSharedGRPCMethods(1), PublicMethods: clusterbootstrap.BootstrapPublicGRPCMethods()})
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	options, err := owner.ServerOptions(&tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: pki.roots,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	require.NoError(t, err)
	gate := bootstrapIngressFixture(t)
	admin, _ := adapter.AdminTokenAuth(bootstrapRotationRPCAdmin)
	credential := bootstrapRotationRPCAdmin
	if len(credentialOverride) != 0 {
		credential = credentialOverride[0]
	}
	rotation, err := clusterbootstrap.BootstrapRotationRPCOptions(nodeauth.AdminGRPCInterceptors{Token: credential}, gate.Domain, bootstrapRotationRPCNode, run)
	require.NoError(t, err)
	options = append(options, grpc.ChainUnaryInterceptor(clusterbootstrap.BootstrapStartupAuthException(gate.Domain, admin), rotation.Unary[0], gate.UnaryInterceptor))
	server := grpc.NewServer(options...)
	owner.Register(server)
	service := &bootstrapGateService{}
	pb.RegisterEncryptionAdminServer(server, service)
	pb.RegisterRawKVServer(server, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(lis) }()
	t.Cleanup(func() { server.Stop(); _ = lis.Close(); <-done })
	f := bootstrapRotationRPCFixture{address: lis.Addr().String(), clientTLS: &tls.Config{
		Certificates: []tls.Certificate{clientCert}, RootCAs: pki.roots, ServerName: "server.test", MinVersion: tls.VersionTLS13}}
	f.connection, err = grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(f.clientTLS)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.connection.Close()) })
	return f
}

func bootstrapRotationRPCRequest() *pb.RotateDEKRequest {
	return &pb.RotateDEKRequest{Purpose: pb.RotateDEKRequest_PURPOSE_STORAGE, NewDekId: 9,
		WrappedNewDek: []byte("wrapped-key"), ProposerNodeId: bootstrapRotationRPCNode, ProposerLocalEpoch: 7}
}

func bootstrapRotationRPCContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bootstrapRotationRPCAdmin), cancel
}

func TestBootstrapRotationRPCAdminOnlyTLS(t *testing.T) {
	var calls atomic.Int64
	f := newBootstrapRotationRPCFixture(t, func(ctx context.Context, request *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		calls.Add(1)
		require.Nil(t, peersession.CapturedReceipt(ctx))
		require.Equal(t, bootstrapRotationRPCNode, request.GetProposerNodeId())
		return &pb.RotateDEKResponse{AppliedIndex: 17}, nil
	})
	client := pb.NewEncryptionAdminClient(f.connection)
	ctx, cancel := bootstrapRotationRPCContext(t)
	defer cancel()
	reply, err := client.RotateDEK(ctx, bootstrapRotationRPCRequest())
	require.NoError(t, err)
	require.Equal(t, uint64(17), reply.GetAppliedIndex())
	for _, test := range []struct {
		name string
		md   metadata.MD
	}{
		{"missing admin", metadata.MD{}},
		{"wrong admin", metadata.Pairs("authorization", "Bearer wrong")},
		{"startup bearer", metadata.Pairs("authorization", "Bearer "+bootstrapGateTestToken)},
		{"startup only", metadata.Pairs(clusterbootstrap.BootstrapControlHeader, bootstrapGateTestToken)},
		{"startup plus admin", metadata.Pairs(clusterbootstrap.BootstrapControlHeader, bootstrapGateTestToken, "authorization", "Bearer "+bootstrapRotationRPCAdmin)},
		{"empty startup header", metadata.Pairs(clusterbootstrap.BootstrapControlHeader, "", "authorization", "Bearer "+bootstrapRotationRPCAdmin)},
		{"duplicate admin", metadata.Pairs("authorization", "Bearer "+bootstrapRotationRPCAdmin, "authorization", "Bearer wrong")},
		{"forged peer", metadata.Pairs("x-elastickv-peer-session", "fake", "x-elastickv-peer-proof", "fake", "authorization", "Bearer "+bootstrapRotationRPCAdmin)},
	} {
		t.Run(test.name, func(t *testing.T) {
			requestCtx, requestCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer requestCancel()
			_, callErr := client.RotateDEK(metadata.NewOutgoingContext(requestCtx, test.md), bootstrapRotationRPCRequest())
			require.Error(t, callErr)
		})
	}
	require.Equal(t, int64(1), calls.Load())
	_, err = client.BootstrapEncryption(ctx, &pb.BootstrapEncryptionRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err), "bootstrap does not acquire rotation authority")
	_, err = client.EnableStorageEnvelope(ctx, &pb.EnableStorageEnvelopeRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = pb.NewRawKVClient(f.connection).RawGet(ctx, &pb.RawGetRequest{Key: []byte("key")})
	require.Equal(t, codes.Unavailable, status.Code(err), "ordinary Data gate remains closed")
}

func TestBootstrapRotationRPCRejectsUnverifiedTLS(t *testing.T) {
	var calls atomic.Int64
	f := newBootstrapRotationRPCFixture(t, func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		calls.Add(1)
		return &pb.RotateDEKResponse{AppliedIndex: 1}, nil
	})
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "missing client certificate", false: "untrusted client certificate"}[missing], func(t *testing.T) {
			cfg := f.clientTLS.Clone()
			cfg.Certificates = nil
			if !missing {
				cert, _ := newBootstrapGatePKI(t).certificate(t, 4)
				cfg.Certificates = []tls.Certificate{cert}
			}
			connection, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
			require.NoError(t, err)
			defer connection.Close()
			ctx, cancel := bootstrapRotationRPCContext(t)
			defer cancel()
			_, err = pb.NewEncryptionAdminClient(connection).RotateDEK(ctx, bootstrapRotationRPCRequest())
			require.Error(t, err)
		})
	}
	require.Zero(t, calls.Load())
}

func TestBootstrapRotationRPCRequestBoundary(t *testing.T) {
	var calls atomic.Int64
	f := newBootstrapRotationRPCFixture(t, func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		calls.Add(1)
		return &pb.RotateDEKResponse{AppliedIndex: 1}, nil
	})
	for _, test := range []struct {
		name string
		edit func(*pb.RotateDEKRequest)
	}{
		{"unknown purpose", func(r *pb.RotateDEKRequest) { r.Purpose = 99 }},
		{"unspecified purpose", func(r *pb.RotateDEKRequest) { r.Purpose = 0 }},
		{"reserved key", func(r *pb.RotateDEKRequest) { r.NewDekId = 0 }},
		{"missing wrapped key", func(r *pb.RotateDEKRequest) { r.WrappedNewDek = nil }},
		{"oversized wrapped key", func(r *pb.RotateDEKRequest) {
			r.WrappedNewDek = make([]byte, clusterbootstrap.BootstrapRotationWrappedKeyLimit+1)
		}},
		{"zero identity", func(r *pb.RotateDEKRequest) { r.ProposerNodeId = 0 }},
		{"truncated identity", func(r *pb.RotateDEKRequest) { r.ProposerNodeId = 42 }},
		{"other identity", func(r *pb.RotateDEKRequest) { r.ProposerNodeId++ }},
		{"overflow epoch", func(r *pb.RotateDEKRequest) { r.ProposerLocalEpoch = math.MaxUint16 + 1 }},
		{"unknown fields", func(r *pb.RotateDEKRequest) { r.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := bootstrapRotationRPCRequest()
			test.edit(request)
			_, err := clusterbootstrap.CanonicalBootstrapRotationRPCRequest(request, bootstrapRotationRPCNode)
			require.Error(t, err)
			ctx, cancel := bootstrapRotationRPCContext(t)
			defer cancel()
			_, err = pb.NewEncryptionAdminClient(f.connection).RotateDEK(ctx, request)
			require.Error(t, err)
		})
	}
	require.Zero(t, calls.Load())
	for _, bad := range []any{nil, (*pb.RotateDEKRequest)(nil), &pb.BootstrapEncryptionRequest{}} {
		_, err := clusterbootstrap.CanonicalBootstrapRotationRPCRequest(bad, bootstrapRotationRPCNode)
		require.Error(t, err)
	}
	request := bootstrapRotationRPCRequest()
	request.ProposerLocalEpoch = math.MaxUint16
	request.WrappedNewDek = make([]byte, clusterbootstrap.BootstrapRotationWrappedKeyLimit)
	canonical, err := clusterbootstrap.CanonicalBootstrapRotationRPCRequest(request, bootstrapRotationRPCNode)
	require.NoError(t, err)
	request.WrappedNewDek[0] = 1
	require.Zero(t, canonical.WrappedNewDek[0], "the controller owns an independent canonical byte copy")
}

func TestBootstrapRotationRPCRejectsCapturedStartupReceipt(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 1, peersession.ControlReady, nil)
	var checked atomic.Bool
	f.service.action = func(ctx context.Context) error {
		require.NotNil(t, peersession.CapturedReceipt(ctx), "genuine receipt from the owner transport")
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+bootstrapRotationRPCAdmin))
		require.Equal(t, codes.Unauthenticated, status.Code(clusterbootstrap.AuthenticateBootstrapRotationRPC(ctx, bootstrapRotationRPCAdmin)))
		checked.Store(true)
		return nil
	}
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	require.NoError(t, f.handle.Invoke(ctx, f.cc, pb.RawKV_RawLatestCommitTS_FullMethodName,
		&pb.RawLatestCommitTSRequest{GroupId: 1}, &pb.RawLatestCommitTSResponse{}))
	require.True(t, checked.Load())
}

func TestBootstrapRotationRPCOptionsLegacyAndIsolation(t *testing.T) {
	backing := make([]grpc.UnaryServerInterceptor, 3)
	backing[0] = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		return next(ctx, request)
	}
	options := nodeauth.AdminGRPCInterceptors{Token: bootstrapRotationRPCAdmin, Unary: backing[:1]}
	legacy, err := clusterbootstrap.BootstrapRotationRPCOptions(options, nil, 0, nil)
	require.NoError(t, err, "nil legacy profile does not require a controller or identity")
	require.Equal(t, &options.Unary[0], &legacy.Unary[0])
	configured, err := clusterbootstrap.BootstrapRotationRPCOptions(options, &clusterbootstrap.BootstrapDomain{}, bootstrapRotationRPCNode,
		func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
			return &pb.RotateDEKResponse{AppliedIndex: 1}, nil
		})
	require.NoError(t, err)
	require.Len(t, configured.Unary, 2)
	require.Len(t, options.Unary, 1)
	require.Nil(t, backing[1], "do not modify the original service-auth backing array")
	var domain *clusterbootstrap.BootstrapDomain
	require.Nil(t, domain.RotationRPCHandler())
	domain.SetProtectedRotationHandler(nil)
	domain = &clusterbootstrap.BootstrapDomain{}
	domain.SetProtectedRotationHandler(func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		return &pb.RotateDEKResponse{AppliedIndex: 1}, nil
	})
	require.NotNil(t, domain.RotationRPCHandler())
	domain.SetProtectedRotationHandler(nil)
	require.Nil(t, domain.RotationRPCHandler())
}

func TestBootstrapRotationRPCUnconfiguredDoesNotBreakStartup(t *testing.T) {
	for _, credential := range []string{"", bootstrapRotationRPCAdmin} {
		t.Run(map[bool]string{true: "missing ADMIN", false: "protected plaintext"}[credential == ""], func(t *testing.T) {
			options, err := clusterbootstrap.BootstrapRotationRPCOptions(nodeauth.AdminGRPCInterceptors{Token: credential}, &clusterbootstrap.BootstrapDomain{}, bootstrapRotationRPCNode, nil)
			require.NoError(t, err)
			require.Len(t, options.Unary, 1)
			ordinaryCalled := false
			next := func(context.Context, any) (any, error) { ordinaryCalled = true; return "ordinary", nil }
			reply, err := options.Unary[0](t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: pb.RawKV_RawGet_FullMethodName}, next)
			require.NoError(t, err)
			require.Equal(t, "ordinary", reply)
			require.True(t, ordinaryCalled)
			ordinaryCalled = false
			_, err = options.Unary[0](t.Context(), bootstrapRotationRPCRequest(), &grpc.UnaryServerInfo{FullMethod: pb.EncryptionAdmin_RotateDEK_FullMethodName}, next)
			require.Equal(t, codes.Unauthenticated, status.Code(err))
			require.False(t, ordinaryCalled, "disabled rotation cannot fall back to the adapter")
			f := newBootstrapRotationRPCFixture(t, nil, credential)
			ctx, cancel := bootstrapRotationRPCContext(t)
			defer cancel()
			_, err = pb.NewEncryptionAdminClient(f.connection).RotateDEK(ctx, bootstrapRotationRPCRequest())
			want := codes.FailedPrecondition
			if credential == "" {
				want = codes.Unauthenticated
			}
			require.Equal(t, want, status.Code(err), "authenticated unconfigured rotation fails closed, not startup")
		})
	}
}

func TestBootstrapRotationRPCBoundAndCancellation(t *testing.T) {
	entered, joined := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	f := newBootstrapRotationRPCFixture(t, func(ctx context.Context, _ *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		close(joined)
		return nil, ctx.Err()
	})
	ctx, cancel := bootstrapRotationRPCContext(t)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := pb.NewEncryptionAdminClient(f.connection).RotateDEK(ctx, bootstrapRotationRPCRequest())
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("controller not entered")
	}
	other, stopOther := bootstrapRotationRPCContext(t)
	defer stopOther()
	_, err := pb.NewEncryptionAdminClient(f.connection).RotateDEK(other, bootstrapRotationRPCRequest())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	cancel()
	select {
	case err = <-result:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-other.Done():
		t.Fatal("canceled RPC did not return")
	}
	select {
	case <-joined:
	case <-other.Done():
		t.Fatal("controller did not join caller cancellation")
	}
	require.Equal(t, int64(1), calls.Load(), "no retry or extra queued controller")
}

func TestBootstrapRotationRPCControllerResult(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply *pb.RotateDEKResponse
	}{
		{"nil", nil}, {"zero", &pb.RotateDEKResponse{}},
		{"unknown fields", func() *pb.RotateDEKResponse {
			r := &pb.RotateDEKResponse{AppliedIndex: 1}
			r.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
			return r
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			reply, err := clusterbootstrap.RunBootstrapRotationRPC(t.Context(), bootstrapRotationRPCRequest(), func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
				return test.reply, nil
			})
			require.Nil(t, reply)
			require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	reply, err := clusterbootstrap.RunBootstrapRotationRPC(ctx, bootstrapRotationRPCRequest(), func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		cancel()
		return &pb.RotateDEKResponse{AppliedIndex: 1}, nil
	})
	require.Nil(t, reply)
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, context.Canceled)
	want := errors.Join(raftadmission.ErrStale, kv.ErrTransactionOutcomeUnknown)
	reply, err = clusterbootstrap.RunBootstrapRotationRPC(t.Context(), bootstrapRotationRPCRequest(), func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		return &pb.RotateDEKResponse{AppliedIndex: 1}, want
	})
	require.Nil(t, reply)
	require.ErrorIs(t, err, want, "ambiguous controller results never trigger a retry")
	for _, bad := range []struct {
		credential string
		node       uint64
		run        clusterbootstrap.BootstrapRotationRPCHandler
	}{{}, {credential: bootstrapRotationRPCAdmin}, {credential: bootstrapRotationRPCAdmin, node: bootstrapRotationRPCNode}} {
		_, err = clusterbootstrap.NewBootstrapRotationRPC(bad.credential, bad.node, bad.run)
		require.Error(t, err)
	}
}
