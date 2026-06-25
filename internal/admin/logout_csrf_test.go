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

	"github.com/stretchr/testify/require"
)

// TestLogout_RejectsUnauthenticated ensures a cross-site caller cannot
// POST /admin/api/v1/auth/logout without a valid session, which was the
// logout-CSRF vector Codex flagged.
func TestLogout_RejectsUnauthenticated(t *testing.T) {
	srv := newServerForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	// The server must not have set any cookies on a rejected logout.
	require.Empty(t, rec.Result().Cookies())
}

// TestLogout_RequiresCSRF ensures that even with a valid session cookie,
// logout refuses to execute without a matching X-Admin-CSRF header.
// SameSite=Strict already blocks the cross-site leg, but the server-side
// CSRF check is an explicit belt-and-braces guard.
func TestLogout_RequiresCSRF(t *testing.T) {
	srv := newServerForTest(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Log in to collect cookies.
	cookies := loginForTest(t, ts)

	// POST /auth/logout with session cookie but NO X-Admin-CSRF header.
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "csrf_missing")
}

// TestLogout_AuditEmittedOnce ensures the logout response carries
// exactly one admin_audit log line — the one HandleLogout emits with
// actor decoded from the session cookie. The generic Audit middleware
// is intentionally skipped on the logout chain to avoid a duplicate
// (less informative) audit entry per request.
func TestLogout_AuditEmittedOnce(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)
	creds := MapCredentialStore{"AKIA_ADMIN": "ADMIN_SECRET"}
	roles := map[string]Role{"AKIA_ADMIN": RoleFull}
	cluster := ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{NodeID: "n"}, nil
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

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	cookies := loginForTest(t, ts)
	var csrfValue string
	for _, c := range cookies {
		if c.Name == csrfCookieName {
			csrfValue = c.Value
		}
	}
	require.NotEmpty(t, csrfValue)
	buf.Reset()

	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	req.Header.Set(csrfHeaderName, csrfValue)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	out := buf.String()
	count := strings.Count(out, `"msg":"admin_audit"`)
	require.Equalf(t, 1, count, "expected exactly one admin_audit line on logout, got %d:\n%s", count, out)
	require.Contains(t, out, `"action":"logout"`)
}

// TestLogout_HappyPath verifies that a well-formed logout (session
// cookie + matching CSRF header + cookie) succeeds and returns 204.
func TestLogout_HappyPath(t *testing.T) {
	srv := newServerForTest(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	cookies := loginForTest(t, ts)
	var csrfValue string
	for _, c := range cookies {
		if c.Name == csrfCookieName {
			csrfValue = c.Value
		}
	}
	require.NotEmpty(t, csrfValue)

	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	req.Header.Set(csrfHeaderName, csrfValue)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Expired-cookie Set-Cookie headers must still be emitted so the
	// client actually forgets the session.
	var cleared int
	for _, c := range rec.Result().Cookies() {
		if c.MaxAge == -1 {
			cleared++
		}
	}
	require.Equal(t, 2, cleared, "expected both admin_session and admin_csrf to be cleared")
}

// loginForTest POSTs a valid login against ts and returns the resulting
// cookies. It is a small helper shared by the logout tests above.
func loginForTest(t *testing.T, ts *httptest.Server) []*http.Cookie {
	t.Helper()
	body := []byte(`{"access_key":"AKIA_ADMIN","secret_key":"ADMIN_SECRET"}`)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/admin/api/v1/auth/login", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	cookies := resp.Cookies()
	require.Len(t, cookies, 2)
	return cookies
}
