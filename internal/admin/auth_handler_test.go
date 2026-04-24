package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func newAuthServiceForTest(t *testing.T) (*AuthService, *Verifier) {
	t.Helper()
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)

	creds := MapCredentialStore{
		"AKIA_ADMIN": "ADMIN_SECRET",
		"AKIA_RO":    "RO_SECRET",
		"AKIA_OTHER": "OTHER_SECRET", // present in creds but not in roles
	}
	roles := map[string]Role{
		"AKIA_ADMIN": RoleFull,
		"AKIA_RO":    RoleReadOnly,
	}
	svc := NewAuthService(signer, creds, roles, AuthServiceOpts{
		Clock: clk,
	})
	return svc, verifier
}

func postJSON(t *testing.T, body any) *http.Request {
	t.Helper()
	buf, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/login", strings.NewReader(string(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:50001"
	return req
}

func TestLogin_HappyPathFull(t *testing.T) {
	svc, verifier := newAuthServiceForTest(t)
	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "ADMIN_SECRET"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp loginResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, RoleFull, resp.Role)

	// Find both cookies and assert attributes.
	var session, csrf *http.Cookie
	for _, c := range rec.Result().Cookies() {
		switch c.Name {
		case sessionCookieName:
			session = c
		case csrfCookieName:
			csrf = c
		}
	}
	require.NotNil(t, session, "expected admin_session cookie")
	require.NotNil(t, csrf, "expected admin_csrf cookie")

	// Cookie hardening.
	require.True(t, session.HttpOnly)
	require.True(t, session.Secure)
	require.Equal(t, http.SameSiteStrictMode, session.SameSite)
	require.Equal(t, "/admin", session.Path)
	require.Equal(t, int(sessionTTL.Seconds()), session.MaxAge)

	require.False(t, csrf.HttpOnly, "CSRF cookie must be readable by SPA")
	require.True(t, csrf.Secure)
	require.Equal(t, http.SameSiteStrictMode, csrf.SameSite)
	require.Equal(t, "/admin", csrf.Path)

	// Token must verify.
	principal, err := verifier.Verify(session.Value)
	require.NoError(t, err)
	require.Equal(t, "AKIA_ADMIN", principal.AccessKey)
	require.Equal(t, RoleFull, principal.Role)
}

func TestLogin_ReadOnlyMappedToRoleReadOnly(t *testing.T) {
	svc, verifier := newAuthServiceForTest(t)
	req := postJSON(t, loginRequest{AccessKey: "AKIA_RO", SecretKey: "RO_SECRET"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var session *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			session = c
		}
	}
	require.NotNil(t, session)
	principal, err := verifier.Verify(session.Value)
	require.NoError(t, err)
	require.Equal(t, RoleReadOnly, principal.Role)
}

func TestLogin_WrongSecretRejected(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "WRONG"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Body.String(), "invalid_credentials")
	// No cookies on failure.
	require.Empty(t, rec.Result().Cookies())
}

func TestLogin_UnknownAccessKeyRejected(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	req := postJSON(t, loginRequest{AccessKey: "AKIA_NOBODY", SecretKey: "anything"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Body.String(), "invalid_credentials")
}

func TestLogin_CredentialValidButNotAdminRejected(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	// AKIA_OTHER is in creds but not in the role index.
	req := postJSON(t, loginRequest{AccessKey: "AKIA_OTHER", SecretKey: "OTHER_SECRET"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "forbidden")
}

func TestLogin_MissingFields(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	for _, body := range []loginRequest{
		{AccessKey: "", SecretKey: "x"},
		{AccessKey: "x", SecretKey: ""},
		{},
	} {
		req := postJSON(t, body)
		rec := httptest.NewRecorder()
		svc.HandleLogin(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	}
}

func TestLogin_RequiresJSON(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/login",
		strings.NewReader("access_key=x&secret_key=y"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
}

func TestLogin_OnlyPOST(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/auth/login", nil)
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestLogin_RateLimitPerIP(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	// Configure is already default 5/min.
	for i := 0; i < 5; i++ {
		req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "WRONG"})
		rec := httptest.NewRecorder()
		svc.HandleLogin(rec, req)
		require.Equalf(t, http.StatusUnauthorized, rec.Code, "attempt %d", i+1)
	}
	// 6th attempt from the same IP must be rate limited.
	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "WRONG"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "60", rec.Header().Get("Retry-After"))
}

func TestLogin_RateLimitIsPerIPNotGlobal(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	for i := 0; i < 5; i++ {
		req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "WRONG"})
		rec := httptest.NewRecorder()
		svc.HandleLogin(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	}
	// Different IP — should not be throttled.
	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "ADMIN_SECRET"})
	req.RemoteAddr = "10.0.0.1:12345"
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestLogin_InsecureCookieOptIn(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	creds := MapCredentialStore{"AKIA_ADMIN": "ADMIN_SECRET"}
	roles := map[string]Role{"AKIA_ADMIN": RoleFull}
	svc := NewAuthService(signer, creds, roles, AuthServiceOpts{Clock: clk, InsecureCookie: true})

	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "ADMIN_SECRET"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	for _, c := range rec.Result().Cookies() {
		require.Falsef(t, c.Secure, "cookie %s must not be Secure in dev mode", c.Name)
	}
}

func TestLogout_ExpiresBothCookies(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	svc.HandleLogout(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	var names []string
	for _, c := range rec.Result().Cookies() {
		require.Equal(t, -1, c.MaxAge)
		require.Equal(t, "", c.Value)
		names = append(names, c.Name)
	}
	require.ElementsMatch(t, []string{sessionCookieName, csrfCookieName}, names)
}

func TestLogout_OnlyPOST(t *testing.T) {
	svc, _ := newAuthServiceForTest(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/auth/logout", nil)
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	svc.HandleLogout(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestRateLimiter_ResetsAfterWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	clk := func() time.Time { return now }
	rl := newRateLimiter(2, time.Minute, clk)

	require.True(t, rl.allow("1.1.1.1"))
	require.True(t, rl.allow("1.1.1.1"))
	require.False(t, rl.allow("1.1.1.1"))

	now = now.Add(61 * time.Second)
	require.True(t, rl.allow("1.1.1.1"))
}

func TestClientIP(t *testing.T) {
	for _, tc := range []struct {
		remote string
		want   string
	}{
		{"127.0.0.1:12345", "127.0.0.1"},
		{"[::1]:12345", "::1"},
		{"no-port", "no-port"},
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = tc.remote
		require.Equal(t, tc.want, clientIP(req))
	}
}
