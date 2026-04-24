package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func freshKey() string {
	raw := make([]byte, 64)
	// Deterministic seed is fine; tests only care it is the right length.
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestBuildAdminConfig_Passthrough(t *testing.T) {
	in := adminListenerConfig{
		enabled:                   true,
		listen:                    "127.0.0.1:18080",
		tlsCertFile:               "c",
		tlsKeyFile:                "k",
		allowPlaintextNonLoopback: true,
		allowInsecureDevCookie:    true,
		sessionSigningKey:         "a",
		sessionSigningKeyPrevious: "b",
		readOnlyAccessKeys:        []string{"X"},
		fullAccessKeys:            []string{"Y"},
	}
	out := buildAdminConfig(in)
	require.Equal(t, true, out.Enabled)
	require.Equal(t, in.listen, out.Listen)
	require.Equal(t, in.tlsCertFile, out.TLSCertFile)
	require.Equal(t, in.tlsKeyFile, out.TLSKeyFile)
	require.Equal(t, in.allowPlaintextNonLoopback, out.AllowPlaintextNonLoopback)
	require.Equal(t, in.allowInsecureDevCookie, out.AllowInsecureDevCookie)
	require.Equal(t, in.sessionSigningKey, out.SessionSigningKey)
	require.Equal(t, in.sessionSigningKeyPrevious, out.SessionSigningKeyPrevious)
	require.Equal(t, in.readOnlyAccessKeys, out.ReadOnlyAccessKeys)
	require.Equal(t, in.fullAccessKeys, out.FullAccessKeys)
}

func TestParseCSV(t *testing.T) {
	require.Equal(t, []string{"a", "b", "c"}, parseCSV("a,b,c"))
	require.Equal(t, []string{"a", "b"}, parseCSV("  a ,, b ,"))
	require.Equal(t, []string{}, parseCSV(""))
}

func TestStartAdminServer_DisabledNoOp(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	defer func() { _ = eg.Wait() }()
	var lc net.ListenConfig
	_, err := startAdminServer(ctx, &lc, eg, adminListenerConfig{enabled: false}, nil, nil, "")
	require.NoError(t, err)
}

func TestStartAdminServer_InvalidConfigRejected(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	defer func() { _ = eg.Wait() }()
	var lc net.ListenConfig
	cfg := adminListenerConfig{
		enabled: true,
		listen:  "127.0.0.1:0",
		// missing signing key
	}
	_, err := startAdminServer(ctx, &lc, eg, cfg, map[string]string{}, nil, "")
	require.Error(t, err)
}

func TestStartAdminServer_NonLoopbackWithoutTLSRejected(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	defer func() { _ = eg.Wait() }()
	var lc net.ListenConfig
	cfg := adminListenerConfig{
		enabled:           true,
		listen:            "0.0.0.0:0",
		sessionSigningKey: freshKey(),
	}
	_, err := startAdminServer(ctx, &lc, eg, cfg, map[string]string{}, nil, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "TLS")
}

func TestStartAdminServer_RejectsMissingClusterSource(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	defer func() { _ = eg.Wait() }()
	var lc net.ListenConfig
	cfg := adminListenerConfig{
		enabled:           true,
		listen:            "127.0.0.1:0",
		sessionSigningKey: freshKey(),
	}
	_, err := startAdminServer(ctx, &lc, eg, cfg, map[string]string{}, nil, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster info source")
}

func TestStartAdminServer_ServesHealthz(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	eg, eCtx := errgroup.WithContext(ctx)
	defer func() {
		cancel()
		_ = eg.Wait()
	}()

	var lc net.ListenConfig
	cfg := adminListenerConfig{
		enabled:                true,
		listen:                 "127.0.0.1:0",
		sessionSigningKey:      freshKey(),
		allowInsecureDevCookie: true,
	}
	cluster := admin.ClusterInfoFunc(func(_ context.Context) (admin.ClusterInfo, error) {
		return admin.ClusterInfo{NodeID: "n1", Version: "test"}, nil
	})
	addr, err := startAdminServer(eCtx, &lc, eg, cfg, map[string]string{}, cluster, "test")
	require.NoError(t, err)

	// Poll /admin/healthz until success or the test deadline.
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(3 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/admin/healthz", nil)
		require.NoError(t, reqErr)
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equal(t, "ok\n", string(body))
}

func TestStartAdminServer_ServesTLS(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, eCtx := errgroup.WithContext(ctx)
	defer func() {
		cancel()
		_ = eg.Wait()
	}()

	var lc net.ListenConfig
	cfg := adminListenerConfig{
		enabled:           true,
		listen:            "127.0.0.1:0",
		tlsCertFile:       certFile,
		tlsKeyFile:        keyFile,
		sessionSigningKey: freshKey(),
	}
	cluster := admin.ClusterInfoFunc(func(_ context.Context) (admin.ClusterInfo, error) {
		return admin.ClusterInfo{NodeID: "n-tls", Version: "test"}, nil
	})
	addr, err := startAdminServer(eCtx, &lc, eg, cfg, map[string]string{}, cluster, "test")
	require.NoError(t, err)

	transport := &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // self-signed certificate in test; server identity is not what we assert here.
		MinVersion:         tls.VersionTLS12,
	}}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	deadline := time.Now().Add(3 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+"/admin/healthz", nil)
		require.NoError(t, reqErr)
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()
}

// writeSelfSignedCert writes a short-lived self-signed certificate to a
// temp dir and returns the cert / key paths. The certificate is valid
// for 127.0.0.1 only; tests that need TLS should run with it.
func writeSelfSignedCert(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "elastickv-admin-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, certOut.Close())

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyOut, err := os.Create(keyPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}))
	require.NoError(t, keyOut.Close())
	return certPath, keyPath
}
