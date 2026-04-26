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
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestConfigureAdminServiceDisabledByDefault(t *testing.T) {
	t.Parallel()
	srv, icept, err := configureAdminService("", false, adapter.NodeIdentity{NodeID: "n1"}, nil)
	if err != nil {
		t.Fatalf("disabled-by-default should not error: %v", err)
	}
	if srv != nil || !icept.empty() {
		t.Fatalf("disabled service should return nil server and empty interceptors; got %v %+v", srv, icept)
	}
}

func TestConfigureAdminServiceRejectsMutualExclusion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokPath := filepath.Join(dir, "t")
	if err := os.WriteFile(tokPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := configureAdminService(tokPath, true, adapter.NodeIdentity{}, nil); err == nil {
		t.Fatal("expected mutual-exclusion error")
	}
}

func TestConfigureAdminServiceTokenFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokPath := filepath.Join(dir, "t")
	if err := os.WriteFile(tokPath, []byte("hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, icept, err := configureAdminService(tokPath, false, adapter.NodeIdentity{NodeID: "n1"}, nil)
	if err != nil {
		t.Fatalf("configureAdminService: %v", err)
	}
	if srv == nil {
		t.Fatal("expected an AdminServer instance")
	}
	// Expect one unary + one stream interceptor for the admin-token gate.
	if len(icept.unary) != 1 || len(icept.stream) != 1 {
		t.Fatalf("expected 1 unary + 1 stream interceptor, got %d + %d", len(icept.unary), len(icept.stream))
	}
}

func TestConfigureAdminServiceInsecureNoAuth(t *testing.T) {
	t.Parallel()
	srv, icept, err := configureAdminService("", true, adapter.NodeIdentity{NodeID: "n1"}, nil)
	if err != nil {
		t.Fatalf("insecure mode should succeed: %v", err)
	}
	if srv == nil {
		t.Fatal("expected AdminServer in insecure mode")
	}
	if !icept.empty() {
		t.Fatalf("insecure mode should not attach interceptors, got %+v", icept)
	}
}

func TestAdminMembersFromBootstrapExcludesSelf(t *testing.T) {
	t.Parallel()
	servers := []raftengine.Server{
		{ID: "n1", Address: "10.0.0.11:50051"},
		{ID: "n2", Address: "10.0.0.12:50051"},
		{ID: "n3", Address: "10.0.0.13:50051"},
	}
	got := adminMembersFromBootstrap("n1", servers)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (self excluded)", len(got))
	}
	want := map[string]string{"n2": "10.0.0.12:50051", "n3": "10.0.0.13:50051"}
	for _, m := range got {
		if want[m.NodeID] != m.GRPCAddress {
			t.Fatalf("member %+v not in expected set %v", m, want)
		}
	}
}

func TestAdminMembersFromBootstrapEmpty(t *testing.T) {
	t.Parallel()
	if got := adminMembersFromBootstrap("n1", nil); got != nil {
		t.Fatalf("empty bootstrap should produce nil, got %v", got)
	}
	single := []raftengine.Server{{ID: "n1", Address: "a:1"}}
	if got := adminMembersFromBootstrap("n1", single); len(got) != 0 {
		t.Fatalf("single-node bootstrap should yield no members, got %v", got)
	}
}

// TestCanonicalSelfAddressPicksLowestGroup pins the deterministic choice of
// Self.GRPCAddress when --raftGroups is set — the fan-out path has to dial an
// endpoint that this process actually listens on, so --address (which may be
// unrelated) must not win over the real group listeners.
func TestCanonicalSelfAddressPicksLowestGroup(t *testing.T) {
	t.Parallel()
	runtimes := []*raftGroupRuntime{
		{spec: groupSpec{id: 5, address: "10.0.0.1:50055"}},
		{spec: groupSpec{id: 2, address: "10.0.0.1:50052"}},
		{spec: groupSpec{id: 9, address: "10.0.0.1:50059"}},
	}
	got := canonicalSelfAddress("localhost:50051", runtimes)
	if got != "10.0.0.1:50052" {
		t.Fatalf("got %q, want lowest-group address 10.0.0.1:50052", got)
	}
}

func TestCanonicalSelfAddressFallsBackWithoutRuntimes(t *testing.T) {
	t.Parallel()
	got := canonicalSelfAddress("localhost:50051", nil)
	if got != "localhost:50051" {
		t.Fatalf("got %q, want fallback localhost:50051", got)
	}
}

func TestLoadAdminTokenFileRejectsOversize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", adminTokenMaxBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAdminTokenFile(path); err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected size-cap error, got %v", err)
	}
}

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
	_, err := startAdminServer(ctx, &lc, eg, adminListenerConfig{enabled: false}, nil, nil, nil, nil, "")
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
	_, err := startAdminServer(ctx, &lc, eg, cfg, map[string]string{}, nil, nil, nil, "")
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
	_, err := startAdminServer(ctx, &lc, eg, cfg, map[string]string{}, nil, nil, nil, "")
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
	_, err := startAdminServer(ctx, &lc, eg, cfg, map[string]string{}, nil, nil, nil, "")
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
	addr, err := startAdminServer(eCtx, &lc, eg, cfg, map[string]string{}, cluster, nil, nil, "test")
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
	addr, err := startAdminServer(eCtx, &lc, eg, cfg, map[string]string{}, cluster, nil, nil, "test")
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

// TestTranslateAdminTablesError_LeaderChurn verifies that the
// bridge maps transient leader-churn errors from the kv coordinator
// (which AdminCreateTable/AdminDeleteTable can return after their
// initial isVerifiedDynamoLeader check) to admin.ErrTablesNotLeader
// rather than the generic 500 fallthrough. Codex P2 on PR #634.
func TestTranslateAdminTablesError_LeaderChurn(t *testing.T) {
	cases := []struct {
		name string
		in   error
	}{
		{"kv.ErrLeaderNotFound", kv.ErrLeaderNotFound},
		{"adapter.ErrNotLeader", adapter.ErrNotLeader},
		{"adapter.ErrLeaderNotFound", adapter.ErrLeaderNotFound},
		{"wrapped not leader", errors.New("dispatch failed: not leader")},
		{"wrapped leader not found", errors.New("dispatch: leader not found")},
		{"wrapped leadership lost", errors.New("commit aborted: leadership lost")},
		{"wrapped leadership transfer", errors.New("retry exhausted: leadership transfer in progress")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := translateAdminTablesError(tc.in)
			require.ErrorIs(t, out, admin.ErrTablesNotLeader,
				"input %q must map to ErrTablesNotLeader", tc.in)
		})
	}
}

// TestTranslateAdminTablesError_LeaderPhraseInMiddleOfMessage covers
// the false-positive class Codex P2 flagged: a message that
// happens to contain a leader phrase NOT at the suffix must NOT
// be classified as leader-churn. Switching from strings.Contains
// to strings.HasSuffix is what makes this test pass.
func TestTranslateAdminTablesError_LeaderPhraseInMiddleOfMessage(t *testing.T) {
	cases := []string{
		// Phrase mid-message — kv-internal sentinels never look
		// like this; they always end with the canonical phrase.
		"not leader: actually a downstream error",
		"leader not found, but recovered automatically",
		"leadership lost mid-snapshot, retried successfully",
	}
	for _, msg := range cases {
		t.Run(msg, func(t *testing.T) {
			out := translateAdminTablesError(errors.New(msg))
			require.NotErrorIs(t, out, admin.ErrTablesNotLeader,
				"mid-message leader phrase %q must not classify as leader-churn", msg)
		})
	}
}

// TestTranslateAdminTablesError_UnrelatedErrorPassesThrough confirms
// the leader-churn detector does not swallow unrelated errors that
// happen to mention the word "leader" outside the canonical phrases.
func TestTranslateAdminTablesError_UnrelatedErrorPassesThrough(t *testing.T) {
	in := errors.New("team leader misconfigured")
	out := translateAdminTablesError(in)
	// Falls through to default — same error returned, NOT
	// ErrTablesNotLeader.
	require.NotErrorIs(t, out, admin.ErrTablesNotLeader)
	require.Equal(t, in, out)
}
