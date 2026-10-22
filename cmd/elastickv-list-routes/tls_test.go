package main

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type routesTestServer struct {
	pb.UnimplementedDistributionServer
}

func (*routesTestServer) ListRoutes(context.Context, *pb.ListRoutesRequest) (*pb.ListRoutesResponse, error) {
	return &pb.ListRoutesResponse{CatalogVersion: 7}, nil
}

func TestRoutesDialOptions(t *testing.T) {
	oldTLS, oldCA, oldName := *tlsEnabled, *tlsCAFile, *tlsServerName
	t.Cleanup(func() { *tlsEnabled, *tlsCAFile, *tlsServerName = oldTLS, oldCA, oldName })
	fixture := httptest.NewTLSServer(nil)
	fixture.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.Certificate().Raw}), 0o600))
	for _, tc := range []struct {
		name       string
		serverTLS  bool
		clientTLS  bool
		ca         string
		serverName string
		wantError  string
	}{
		{name: "plaintext default"},
		{name: "CA implies TLS", serverTLS: true, ca: caPath},
		{name: "verified server name", serverTLS: true, ca: caPath, serverName: "example.com"},
		{name: "reject wrong name", serverTLS: true, ca: caPath, serverName: "wrong.invalid", wantError: "certificate"},
		{name: "reject untrusted CA", serverTLS: true, clientTLS: true, wantError: "certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*tlsEnabled, *tlsCAFile, *tlsServerName = tc.clientTLS, tc.ca, tc.serverName
			var lc net.ListenConfig
			listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			var serverOpts []grpc.ServerOption
			if tc.serverTLS {
				serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(&tls.Config{
					MinVersion: tls.VersionTLS12, Certificates: fixture.TLS.Certificates,
				})))
			}
			server := grpc.NewServer(serverOpts...)
			pb.RegisterDistributionServer(server, &routesTestServer{})
			serveErr := make(chan error, 1)
			go func() { serveErr <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serveErr)
			})
			opts, err := routesDialOptions()
			require.NoError(t, err)
			conn, err := grpc.NewClient(listener.Addr().String(), opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := pb.NewDistributionClient(conn).ListRoutes(ctx, &pb.ListRoutesRequest{})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(7), resp.GetCatalogVersion())
		})
	}
	*tlsCAFile = filepath.Join(t.TempDir(), "missing.pem")
	_, err := routesDialOptions()
	require.ErrorContains(t, err, "CA file")
}
