package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Capture real server-side AuthInfo from native gRPC, rather than inventing TLS
// flags. The fixture connection stays alive until test cleanup. A client cert
// is intentionally absent: TLS confidentiality is not peer/session authority.
func migrationNativeTLSPeerFixture(t *testing.T) *peer.Peer {
	t.Helper()
	certPath, keyPath := writeSelfSignedCert(t)
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	pem, err := os.ReadFile(certPath)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pem))
	captured := make(chan *peer.Peer, 1)
	observer := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		native, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Internal, "native TLS peer missing")
		}
		captured <- native
		return next(ctx, req)
	}
	address := grpctest.Serve(t, func(server *grpc.Server) {
		healthpb.RegisterHealthServer(server, health.NewServer())
	}, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
	})), grpc.ChainUnaryInterceptor(observer))
	client := grpctest.Dial(t, address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs: roots, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
	})))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	select {
	case native := <-captured:
		info, ok := native.AuthInfo.(credentials.TLSInfo)
		require.True(t, ok)
		require.True(t, info.State.HandshakeComplete)
		require.Equal(t, uint16(tls.VersionTLS12), info.State.Version)
		require.Equal(t, credentials.PrivacyAndIntegrity, info.SecurityLevel)
		require.NotZero(t, info.State.CipherSuite)
		require.Empty(t, info.State.PeerCertificates)
		return native
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}
