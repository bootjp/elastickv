package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestSplitNodesTrimsAndDrops(t *testing.T) {
	t.Parallel()
	got := splitNodes(" host-a:50051 ,,host-b:50051 ,")
	want := []string{"host-a:50051", "host-b:50051"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestLoadTokenRequiresFileOrInsecure(t *testing.T) {
	t.Parallel()
	if _, err := loadToken("", false); err == nil {
		t.Fatal("expected error when neither token nor insecure mode supplied")
	}
	tok, err := loadToken("", true)
	if err != nil {
		t.Fatalf("insecure-mode empty path should succeed: %v", err)
	}
	if tok != "" {
		t.Fatalf("insecure-mode token = %q, want empty", tok)
	}
}

func TestLoadTokenReadsAndTrims(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("\n  s3cret \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tok, err := loadToken(path, false)
	if err != nil {
		t.Fatalf("loadToken: %v", err)
	}
	if tok != "s3cret" {
		t.Fatalf("tok = %q, want s3cret", tok)
	}
}

func TestLoadTokenRejectsEmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")
	if err := os.WriteFile(path, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadToken(path, false)
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected empty-file error, got %v", err)
	}
}

func TestLoadTokenRejectsInsecureWithFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "tok")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadToken(path, true); err == nil {
		t.Fatal("expected mutual-exclusion error when both supplied")
	}
}

func TestLoadTransportCredentialsPrecedence(t *testing.T) {
	t.Parallel()

	if _, err := loadTransportCredentials(true, "", "", false); err != nil {
		t.Fatalf("plaintext alone should succeed: %v", err)
	}
	if _, err := loadTransportCredentials(true, "/tmp/ca.pem", "", false); err == nil {
		t.Fatal("plaintext + CA file should error")
	}
	if _, err := loadTransportCredentials(true, "", "", true); err == nil {
		t.Fatal("plaintext + skip-verify should error")
	}

	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, writePEMCert(t), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransportCredentials(false, ca, "", true); err == nil {
		t.Fatal("CA file + skip-verify should error")
	}
	creds, err := loadTransportCredentials(false, ca, "node-1", false)
	if err != nil {
		t.Fatalf("valid CA config failed: %v", err)
	}
	if creds == nil {
		t.Fatal("expected TLS creds")
	}

	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransportCredentials(false, bad, "", false); err == nil {
		t.Fatal("expected error for unparseable CA file")
	}
}

func writePEMCert(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestMembersFromDeduplicatesAndIncludesSeed(t *testing.T) {
	t.Parallel()
	resp := &pb.GetClusterOverviewResponse{
		Self:    &pb.NodeIdentity{GrpcAddress: "a:1"},
		Members: []*pb.NodeIdentity{{GrpcAddress: "a:1"}, {GrpcAddress: "b:2"}, {GrpcAddress: " "}, {GrpcAddress: "c:3"}},
	}
	got := membersFrom("seed:1", resp)
	want := []string{"seed:1", "a:1", "b:2", "c:3"}
	if len(got) != len(want) {
		t.Fatalf("len = %d (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// fakeAdminServer counts GetClusterOverview calls and returns a configurable
// member list, letting the test assert membership-cache behavior.
type fakeAdminServer struct {
	pb.UnimplementedAdminServer
	addr     string
	members  []string
	calls    atomic.Int64
	returnUn bool
}

func (f *fakeAdminServer) GetClusterOverview(
	_ context.Context,
	_ *pb.GetClusterOverviewRequest,
) (*pb.GetClusterOverviewResponse, error) {
	f.calls.Add(1)
	if f.returnUn {
		return nil, status.Error(codes.Unavailable, "node gone")
	}
	members := make([]*pb.NodeIdentity, 0, len(f.members))
	for _, m := range f.members {
		members = append(members, &pb.NodeIdentity{GrpcAddress: m})
	}
	return &pb.GetClusterOverviewResponse{
		Self:    &pb.NodeIdentity{GrpcAddress: f.addr},
		Members: members,
	}, nil
}

func startFakeAdmin(t *testing.T, srv *fakeAdminServer) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.addr = lis.Addr().String()
	gs := grpc.NewServer()
	pb.RegisterAdminServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.GracefulStop()
		_ = lis.Close()
	})
	return srv.addr
}

func TestFanoutCurrentTargetsCachesAndRefreshes(t *testing.T) {
	t.Parallel()

	peer := &fakeAdminServer{members: []string{"peer-1:1", "peer-2:2"}}
	seedAddr := startFakeAdmin(t, peer)

	f := newFanout([]string{seedAddr}, "", 50*time.Millisecond, insecure.NewCredentials())
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	first := f.currentTargets(ctx)
	if len(first) != 3 {
		t.Fatalf("first call targets = %v, want 3 (seed + 2 members)", first)
	}
	if peer.calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1 after first refresh", peer.calls.Load())
	}

	// Within the cache window, no new discovery RPC.
	_ = f.currentTargets(ctx)
	if peer.calls.Load() != 1 {
		t.Fatalf("cache window should suppress refresh, calls = %d", peer.calls.Load())
	}

	time.Sleep(70 * time.Millisecond)
	_ = f.currentTargets(ctx)
	if peer.calls.Load() != 2 {
		t.Fatalf("post-expiry refresh expected, calls = %d", peer.calls.Load())
	}
}

func TestFanoutCurrentTargetsFallsBackToSeeds(t *testing.T) {
	t.Parallel()

	peer := &fakeAdminServer{returnUn: true}
	seedAddr := startFakeAdmin(t, peer)

	f := newFanout([]string{seedAddr}, "", 50*time.Millisecond, insecure.NewCredentials())
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	targets := f.currentTargets(ctx)
	if len(targets) != 1 || targets[0] != seedAddr {
		t.Fatalf("fallback targets = %v, want [%s]", targets, seedAddr)
	}
}
