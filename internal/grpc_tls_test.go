package internal

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestConfigureGRPCTLSValidatesFiles(t *testing.T) {
	require.ErrorContains(t, ConfigureGRPCTLS("ca.pem", "", ""), "certificate and key")
	require.ErrorContains(t, ConfigureGRPCTLS("", "missing.pem", "missing.key"), "certificate/key")
	_, err := GRPCClientTLSCredentials("missing.pem")
	require.ErrorContains(t, err, "CA file")
	invalid := filepath.Join(t.TempDir(), "invalid.pem")
	require.NoError(t, os.WriteFile(invalid, []byte("invalid"), 0o600))
	_, err = GRPCClientTLSCredentials(invalid)
	require.ErrorContains(t, err, "no certificates")
	require.NoError(t, ConfigureGRPCTLS("", "", ""))
	require.Nil(t, grpcTLS.Load())
}

func TestGRPCTLSCommonOptionsVerifyPeer(t *testing.T) {
	fixture := httptest.NewTLSServer(nil)
	fixture.Close()
	cert := fixture.Certificate()
	key, err := x509.MarshalPKCS8PrivateKey(fixture.TLS.Certificates[0].PrivateKey)
	require.NoError(t, err)
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600))
	require.NoError(t, ConfigureGRPCTLS(certPath, certPath, keyPath))
	t.Cleanup(func() { require.NoError(t, ConfigureGRPCTLS("", "", "")) })
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(GRPCServerOptions()...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
		require.NoError(t, <-serveErr)
	})
	conn, err := grpc.NewClient(lis.Addr().String(), GRPCDialOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	// System roots do not trust the test CA; never fall back to plaintext.
	creds, err := GRPCClientTLSCredentials("")
	require.NoError(t, err)
	untrusted, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, untrusted.Close()) })
	_, err = healthpb.NewHealthClient(untrusted).Check(ctx, &healthpb.HealthCheckRequest{})
	require.ErrorContains(t, err, "certificate")
	wrongName, err := grpc.NewClient(lis.Addr().String(), append(GRPCDialOptions(), grpc.WithAuthority("wrong-node.invalid"))...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wrongName.Close()) })
	_, err = healthpb.NewHealthClient(wrongName).Check(ctx, &healthpb.HealthCheckRequest{})
	require.ErrorContains(t, err, "certificate")
}
