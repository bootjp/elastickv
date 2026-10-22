package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/credentials"
)

type grpcTLSCredentials struct {
	server credentials.TransportCredentials
	client credentials.TransportCredentials
}

var grpcTLS atomic.Pointer[grpcTLSCredentials]

// ConfigureGRPCTLS installs transport credentials before node services start.
// Empty paths preserve the existing plaintext peer transport.
func ConfigureGRPCTLS(caPath, certPath, keyPath string) error {
	if caPath == "" && certPath == "" && keyPath == "" {
		grpcTLS.Store(nil)
		return nil
	}
	if certPath == "" || keyPath == "" {
		return errors.New("gRPC TLS requires both certificate and key files")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return errors.Wrap(err, "load gRPC TLS certificate/key")
	}
	client, err := GRPCClientTLSCredentials(caPath)
	if err != nil {
		return err
	}
	grpcTLS.Store(&grpcTLSCredentials{
		server: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}),
		client: client,
	})
	return nil
}

// GRPCClientTLSCredentials verifies peers using the given CA or system roots.
// The peer name is taken from the dial target; verification cannot be disabled.
func GRPCClientTLSCredentials(caPath string) (credentials.TransportCredentials, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, errors.Wrap(err, "read gRPC TLS CA file")
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("gRPC TLS CA file contains no certificates")
		}
	}
	return credentials.NewTLS(cfg), nil
}

// GRPCBearerToken is never sent on an insecure gRPC connection.
type GRPCBearerToken string

func (t GRPCBearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

func (GRPCBearerToken) RequireTransportSecurity() bool { return true }
