package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestValidateBindAddr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		addr    string
		allow   bool
		wantErr bool
	}{
		{"loopback ipv4", "127.0.0.1:8080", false, false},
		{"loopback ipv6", "[::1]:8080", false, false},
		{"localhost", "localhost:8080", false, false},
		{"remote bind default rejected", "0.0.0.0:8080", false, true},
		{"specific ip default rejected", "10.0.0.5:8080", false, true},
		{"empty host rejected", ":8080", false, true},
		{"allow opt-in permits remote", "0.0.0.0:8080", true, false},
		{"malformed addr", "not-an-addr", false, true},
	}
	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateBindAddr(tc.addr, tc.allow)
			if tc.wantErr && err == nil {
				t.Fatalf("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

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

func TestLoadTransportCredentialsPlaintextDefault(t *testing.T) {
	t.Parallel()
	if _, err := loadTransportCredentials("", "", false); err != nil {
		t.Fatalf("no-flags default should succeed: %v", err)
	}
	if _, err := loadTransportCredentials("", "node-1", false); err == nil {
		t.Fatal("serverName without TLS opt-in should error")
	}
}

func TestLoadTransportCredentialsTLS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, writePEMCert(t), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransportCredentials(ca, "", true); err == nil {
		t.Fatal("CA file + skip-verify should error (mutually exclusive)")
	}
	creds, err := loadTransportCredentials(ca, "node-1", false)
	if err != nil {
		t.Fatalf("valid CA config failed: %v", err)
	}
	if creds == nil {
		t.Fatal("expected TLS creds")
	}
	creds, err = loadTransportCredentials("", "", true)
	if err != nil {
		t.Fatalf("skip-verify alone should succeed: %v", err)
	}
	if creds == nil {
		t.Fatal("expected TLS creds for skip-verify")
	}
}

func TestLoadTransportCredentialsRejectsBadCA(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransportCredentials(bad, "", false); err == nil {
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

func TestLoadTokenRejectsOversizedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge")
	// One byte past the cap: exact boundary plus one.
	payload := strings.Repeat("x", maxTokenFileBytes+1)
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadToken(path, false)
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected size-cap error, got %v", err)
	}
}

func TestMembersFromCapsAtMaxDiscoveredNodes(t *testing.T) {
	t.Parallel()
	resp := &pb.GetClusterOverviewResponse{
		Self: &pb.NodeIdentity{GrpcAddress: "self:1"},
	}
	// Return way more members than the cap allows.
	for i := 0; i < maxDiscoveredNodes+50; i++ {
		resp.Members = append(resp.Members, &pb.NodeIdentity{
			GrpcAddress: "node-" + strconvItoa(i) + ":1",
		})
	}
	got := membersFrom("seed:1", resp)
	if len(got) != maxDiscoveredNodes {
		t.Fatalf("len = %d, want %d (cap)", len(got), maxDiscoveredNodes)
	}
}

// small helper to avoid pulling strconv into the test file just for one call.
func strconvItoa(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

// TestFanoutClientCacheEvictsEvenWhenAllEntriesAreSeeds asserts that when
// operators configure more seeds than maxCachedClients the cache still honors
// its cap — without the seed-fallback, the eviction loop would skip every
// entry and the cache would grow past the documented bound.
func TestFanoutClientCacheEvictsEvenWhenAllEntriesAreSeeds(t *testing.T) {
	t.Parallel()
	seeds := make([]string, 0, maxCachedClients+3)
	for i := 0; i < maxCachedClients+3; i++ {
		seeds = append(seeds, "seed-"+strconvItoa(i)+":1")
	}
	f := newFanout(seeds, "", time.Second, insecure.NewCredentials())
	defer f.Close()

	for _, s := range seeds {
		if _, release, err := f.clientFor(s); err != nil {
			t.Fatalf("clientFor(%s): %v", s, err)
		} else {
			release()
		}
	}
	f.mu.Lock()
	size := len(f.clients)
	f.mu.Unlock()
	if size > maxCachedClients {
		t.Fatalf("cache size = %d, exceeds cap %d (seed-only path)", size, maxCachedClients)
	}
}

func TestFanoutClientCacheEvictsWhenFull(t *testing.T) {
	t.Parallel()
	f := newFanout([]string{"seed:1"}, "", time.Second, insecure.NewCredentials())
	defer f.Close()

	// Fill the cache past the cap. New dials should not error out and the
	// map must stay bounded.
	for i := 0; i < maxCachedClients+5; i++ {
		_, release, err := f.clientFor("node-" + strconvItoa(i) + ":1")
		if err != nil {
			t.Fatalf("clientFor[%d]: %v", i, err)
		}
		release()
	}
	f.mu.Lock()
	size := len(f.clients)
	f.mu.Unlock()
	if size > maxCachedClients {
		t.Fatalf("cache size = %d, exceeds cap %d", size, maxCachedClients)
	}
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

// TestFanoutCurrentTargetsSingleflight asserts that concurrent refreshes after
// cache expiry collapse into one GetClusterOverview call.
func TestFanoutCurrentTargetsSingleflight(t *testing.T) {
	t.Parallel()

	peer := &fakeAdminServer{members: []string{"peer-1:1"}}
	seedAddr := startFakeAdmin(t, peer)

	f := newFanout([]string{seedAddr}, "", math.MaxInt64, insecure.NewCredentials())
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Warm: trigger first refresh serially so singleflight key exists.
	_ = f.currentTargets(ctx)
	if peer.calls.Load() != 1 {
		t.Fatalf("warm-up calls = %d, want 1", peer.calls.Load())
	}

	// Force expiry by nil-ing the cache and then fire many concurrent refresh
	// attempts. Because refreshInterval is effectively infinite, only the
	// forced clear can cause a refresh, and singleflight should collapse the
	// burst into a single RPC.
	f.mu.Lock()
	f.members = nil
	f.mu.Unlock()

	const concurrency = 20
	done := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		go func() {
			_ = f.currentTargets(ctx)
			done <- struct{}{}
		}()
	}
	for i := 0; i < concurrency; i++ {
		<-done
	}

	// Expect exactly one additional RPC for the burst.
	if got := peer.calls.Load(); got != 2 {
		t.Fatalf("singleflight failed: calls = %d, want 2", got)
	}
}

func TestHandleOverviewRejectsNonGET(t *testing.T) {
	t.Parallel()
	f := newFanout([]string{"127.0.0.1:0"}, "", time.Second, insecure.NewCredentials())
	defer f.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/cluster/overview", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	f.handleOverview(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != http.StatusMethodNotAllowed {
		t.Fatalf("body.code = %d", body.Code)
	}
}

func TestWriteJSONSurfacesEncodeFailure(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	// math.Inf(1) is not representable in JSON; encoding fails.
	writeJSON(rec, http.StatusOK, math.Inf(1))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(rec.Body.String(), "internal server error") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestWriteJSONSuccessPath(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]int{"n": 42})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	var out map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["n"] != 42 {
		t.Fatalf("body = %v", out)
	}
}

// TestFanoutEvictionDoesNotCloseInFlightConn asserts that evicting a cached
// entry while a borrower still holds the lease does NOT close the underlying
// gRPC connection — the close is deferred to the last release(), so in-flight
// RPCs on the evicted client complete successfully.
func TestFanoutEvictionDoesNotCloseInFlightConn(t *testing.T) {
	t.Parallel()

	peer := &fakeAdminServer{members: []string{"m:1"}}
	addr := startFakeAdmin(t, peer)

	f := newFanout([]string{addr}, "", time.Second, insecure.NewCredentials())
	defer f.Close()

	// Borrower 1 leases the client.
	cli, release, err := f.clientFor(addr)
	if err != nil {
		t.Fatal(err)
	}

	// Force eviction while the lease is held. invalidateClient marks
	// the entry retired+refcount>0, so the conn must stay open.
	f.invalidateClient(addr)

	// The lease should still be usable — conn.Close() has been deferred.
	if _, callErr := cli.client.GetClusterOverview(
		context.Background(), &pb.GetClusterOverviewRequest{},
	); callErr != nil {
		t.Fatalf("in-flight RPC on retired client failed (eviction raced): %v", callErr)
	}
	release() // last release closes the conn; verify no panic / double-close.
	release() // extra release must be a no-op (refcount already zero).
}

// TestFanoutClientForAfterCloseIsSafe asserts that clientFor and
// invalidateClient do not panic when invoked concurrently with Close — a
// shutdown-time race that otherwise hits a nil-map write in clientFor.
func TestFanoutClientForAfterCloseIsSafe(t *testing.T) {
	t.Parallel()
	f := newFanout([]string{"127.0.0.1:1"}, "", time.Second, insecure.NewCredentials())
	f.Close()

	if _, _, err := f.clientFor("127.0.0.1:2"); err == nil {
		t.Fatal("expected error after Close, got nil")
	}
	f.invalidateClient("127.0.0.1:2") // must be a no-op, not panic
	f.Close()                         // idempotent
}

// TestFanoutRefreshSurvivesFirstCallerCancel asserts that canceling the first
// caller's context does not kill the shared singleflight refresh — subsequent
// callers should still see a populated membership.
func TestFanoutRefreshSurvivesFirstCallerCancel(t *testing.T) {
	t.Parallel()

	peer := &fakeAdminServer{members: []string{"m:1"}}
	seedAddr := startFakeAdmin(t, peer)

	f := newFanout([]string{seedAddr}, "", 50*time.Millisecond, insecure.NewCredentials())
	defer f.Close()

	// First caller cancels before the refresh completes.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = f.currentTargets(cancelled)

	// A fresh caller a beat later must see the member list populated by the
	// still-running background refresh rather than the raw seed list.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		targets := f.currentTargets(ctx)
		cancel()
		if len(targets) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("membership never populated; peer calls=%d", peer.calls.Load())
}

// TestHandleOverviewUsesProtojson asserts that admin responses preserve the
// proto3 JSON mapping (camelCase field names, zero-valued fields emitted) so
// the browser sees stable field names regardless of encoding/json's behavior.
func TestHandleOverviewUsesProtojson(t *testing.T) {
	t.Parallel()
	peer := &fakeAdminServer{members: []string{"m:1"}}
	seedAddr := startFakeAdmin(t, peer)

	f := newFanout([]string{seedAddr}, "", time.Second, insecure.NewCredentials())
	defer f.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/cluster/overview", nil)
	rec := httptest.NewRecorder()
	f.handleOverview(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	body := rec.Body.String()
	// protojson uses camelCase by default; encoding/json would emit
	// "grpc_address" (proto name). Catch the regression explicitly.
	if !strings.Contains(body, "grpcAddress") {
		t.Fatalf("response missing protojson camelCase field; body=%q", body)
	}
}
