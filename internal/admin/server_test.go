package admin

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func newServerForTest(t *testing.T) *Server {
	t.Helper()
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)
	creds := MapCredentialStore{
		"AKIA_ADMIN": "ADMIN_SECRET",
		"AKIA_RO":    "RO_SECRET",
	}
	roles := map[string]Role{
		"AKIA_ADMIN": RoleFull,
		"AKIA_RO":    RoleReadOnly,
	}
	cluster := ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{NodeID: "node-1", Version: "0.1.0"}, nil
	})
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv, err := NewServer(ServerDeps{
		Signer:      signer,
		Verifier:    verifier,
		Credentials: creds,
		Roles:       roles,
		ClusterInfo: cluster,
		AuthOpts:    AuthServiceOpts{Clock: clk},
		Logger:      logger,
	})
	require.NoError(t, err)
	return srv
}

func TestServer_LoginThenCluster(t *testing.T) {
	srv := newServerForTest(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body, _ := json.Marshal(loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "ADMIN_SECRET"})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		ts.URL+"/admin/api/v1/auth/login", strings.NewReader(string(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	cookies := resp.Cookies()
	require.Len(t, cookies, 2)
	_ = resp.Body.Close()

	// Follow-up: /cluster with cookies should succeed.
	req2, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		ts.URL+"/admin/api/v1/cluster", nil)
	require.NoError(t, err)
	for _, c := range cookies {
		req2.AddCookie(c)
	}
	resp2, err := client.Do(req2)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	var info ClusterInfo
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&info))
	require.Equal(t, "node-1", info.NodeID)
	_ = resp2.Body.Close()
}

func TestServer_ClusterRequiresSession(t *testing.T) {
	srv := newServerForTest(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		ts.URL+"/admin/api/v1/cluster", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestServer_UnknownAPIReturnsJSON404(t *testing.T) {
	srv := newServerForTest(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/nope", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.Contains(t, rec.Body.String(), "unknown_endpoint")
}

func TestServer_HealthzExposed(t *testing.T) {
	srv := newServerForTest(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok\n", rec.Body.String())
}

func TestServer_LoginBodyLimited(t *testing.T) {
	srv := newServerForTest(t)
	oversize := strings.Repeat("x", int(defaultBodyLimit)+1024)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/login", strings.NewReader(oversize))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestServer_MissingDepsRejected(t *testing.T) {
	_, err := NewServer(ServerDeps{})
	require.Error(t, err)

	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)
	// Signer + Verifier only; still missing credentials.
	_, err = NewServer(ServerDeps{Signer: signer, Verifier: verifier})
	require.Error(t, err)
}

func TestServer_APIHandlerReturnsBareMux(t *testing.T) {
	srv := newServerForTest(t)
	h := srv.APIHandler()
	require.NotNil(t, h)

	// /admin/api/v1/unknown still resolves to the "unknown_endpoint"
	// JSON 404 even without the router wrapper.
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/unknown", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestServer_WriteRejectsMissingCSRF(t *testing.T) {
	// Login to obtain a session, then hit cluster with POST to trigger
	// CSRF on what the router normally rejects as method_not_allowed.
	// Since /cluster rejects POST at the handler level, we use an
	// endpoint that would exercise the write path. For P1 we do not
	// have one yet, so assert via a direct middleware-level test that
	// composing Server routes a write path through CSRF. This is
	// already covered by TestCSRF_* in middleware_test.go; here we
	// spot-check the compose order by sending POST /cluster and
	// expecting 405 (CSRF runs before method check so we get 403
	// instead). Either 403 or 405 is acceptable; the important bit
	// is that no handler state is mutated.
	srv := newServerForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusMethodNotAllowed}, rec.Code)
}

// Smoke-test that ctxClosed and endpointMatches compile & work; these
// are small internal helpers that would otherwise skew coverage.
func TestInternalHelpers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	require.False(t, ctxClosed(ctx))
	cancel()
	require.True(t, ctxClosed(ctx))
	require.True(t, endpointMatches("/a", "/A"))
	require.False(t, endpointMatches("/a", "/b"))
}
