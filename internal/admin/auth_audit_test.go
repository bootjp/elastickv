package admin

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newAuthServiceWithAudit(t *testing.T) (*AuthService, *bytes.Buffer) {
	t.Helper()
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)

	creds := MapCredentialStore{
		"AKIA_ADMIN": "ADMIN_SECRET",
	}
	roles := map[string]Role{
		"AKIA_ADMIN": RoleFull,
	}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	svc := NewAuthService(signer, creds, roles, AuthServiceOpts{
		Clock:    clk,
		Verifier: verifier,
		Logger:   logger,
	})
	return svc, buf
}

func TestAudit_LoginSuccessRecordsActor(t *testing.T) {
	svc, buf := newAuthServiceWithAudit(t)
	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "ADMIN_SECRET"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	out := buf.String()
	require.Contains(t, out, `"msg":"admin_audit"`)
	require.Contains(t, out, `"action":"login"`)
	require.Contains(t, out, `"actor":"AKIA_ADMIN"`)
	require.Contains(t, out, `"claimed_actor":"AKIA_ADMIN"`)
	require.Contains(t, out, `"status":200`)
}

func TestAudit_LoginFailureRecordsClaimedActor(t *testing.T) {
	svc, buf := newAuthServiceWithAudit(t)
	req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "WRONG"})
	rec := httptest.NewRecorder()
	svc.HandleLogin(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	out := buf.String()
	require.Contains(t, out, `"action":"login"`)
	// We did NOT authenticate, so actor is empty.
	require.Contains(t, out, `"actor":""`)
	// But the claimed actor is still logged so operators can track
	// which access key was targeted by brute-force attempts.
	require.Contains(t, out, `"claimed_actor":"AKIA_ADMIN"`)
	require.Contains(t, out, `"status":401`)
}

func TestAudit_LogoutDecodesCookieForActor(t *testing.T) {
	svc, buf := newAuthServiceWithAudit(t)

	// Log in first.
	loginReq := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: "ADMIN_SECRET"})
	loginRec := httptest.NewRecorder()
	svc.HandleLogin(loginRec, loginReq)
	require.Equal(t, http.StatusOK, loginRec.Code)
	cookies := loginRec.Result().Cookies()
	buf.Reset()

	// Now log out with the session cookie — audit must record actor.
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	req.RemoteAddr = "127.0.0.1:1"
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	svc.HandleLogout(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	out := buf.String()
	require.Contains(t, out, `"action":"logout"`)
	require.Contains(t, out, `"actor":"AKIA_ADMIN"`)
}

func TestAudit_LogoutWithoutCookieEmptyActor(t *testing.T) {
	svc, buf := newAuthServiceWithAudit(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/auth/logout", nil)
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	svc.HandleLogout(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	out := buf.String()
	require.Contains(t, out, `"action":"logout"`)
	require.Contains(t, out, `"actor":""`)
}

func TestAudit_LoginLengthTimingHashed(t *testing.T) {
	// Same-length secret mismatch and different-length secret mismatch
	// must both reach the failure path without short-circuiting on
	// length. We cannot time them precisely in a unit test, but we can
	// at least verify both paths emit the same failure response.
	svc, _ := newAuthServiceWithAudit(t)
	for _, secret := range []string{"x", "much-longer-wrong-secret-value-here"} {
		req := postJSON(t, loginRequest{AccessKey: "AKIA_ADMIN", SecretKey: secret})
		rec := httptest.NewRecorder()
		svc.HandleLogin(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Contains(t, rec.Body.String(), "invalid_credentials")
	}
}
