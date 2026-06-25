package admin

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBodyLimit_Allows(t *testing.T) {
	var gotBody []byte
	h := BodyLimit(128)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = b
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/x", strings.NewReader("hello"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "hello", string(gotBody))
}

func TestBodyLimit_Exceeded(t *testing.T) {
	oversize := strings.Repeat("x", 200)
	h := BodyLimit(64)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if err != nil {
			if IsMaxBytesError(err) {
				WriteMaxBytesError(w)
				return
			}
			t.Fatalf("unexpected error: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/x", strings.NewReader(oversize))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Contains(t, rec.Body.String(), "payload_too_large")
}

func TestSessionAuth_MissingCookie(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	v := newVerifierForTest(t, []byte{1}, clk)
	h := SessionAuth(v)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Body.String(), "unauthenticated")
}

func TestSessionAuth_HappyPathPutsPrincipalOnContext(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	v := newVerifierForTest(t, []byte{1}, clk)

	principal := AuthPrincipal{AccessKey: "AKIA", Role: RoleFull}
	token, err := signer.Sign(principal)
	require.NoError(t, err)

	var gotPrincipal AuthPrincipal
	h := SessionAuth(v)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		require.True(t, ok)
		gotPrincipal = p
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/cluster", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, principal, gotPrincipal)
}

func TestSessionAuth_InvalidToken(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	v := newVerifierForTest(t, []byte{1}, clk)
	h := SessionAuth(v)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/cluster", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "garbage"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestRequireWriteRole_ReadOnlyRejected(t *testing.T) {
	h := RequireWriteRole()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	ctx := context.WithValue(context.Background(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly})
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "forbidden")
}

func TestRequireWriteRole_FullAllowed(t *testing.T) {
	h := RequireWriteRole()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	ctx := context.WithValue(context.Background(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_ADMIN", Role: RoleFull})
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestRequireWriteRole_NoPrincipal(t *testing.T) {
	h := RequireWriteRole()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestCSRF_GETPasses(t *testing.T) {
	called := false
	h := CSRFDoubleSubmit()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/dynamo/tables", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.True(t, called)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestCSRF_WriteMissingCookie(t *testing.T) {
	h := CSRFDoubleSubmit()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
	req.Header.Set(csrfHeaderName, "tok")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "csrf_missing")
}

func TestCSRF_WriteMissingHeader(t *testing.T) {
	h := CSRFDoubleSubmit()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "tok"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "csrf_missing")
}

func TestCSRF_Mismatch(t *testing.T) {
	h := CSRFDoubleSubmit()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "a"})
	req.Header.Set(csrfHeaderName, "b")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "csrf_mismatch")
}

func TestCSRF_MatchAllows(t *testing.T) {
	h := CSRFDoubleSubmit()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "same"})
	req.Header.Set(csrfHeaderName, "same")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)
}

func TestAudit_LogsWriteRequest(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.WithValue(context.Background(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_ADMIN", Role: RoleFull})
	h := Audit(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	out := buf.String()
	require.Contains(t, out, `"msg":"admin_audit"`)
	require.Contains(t, out, `"actor":"AKIA_ADMIN"`)
	require.Contains(t, out, `"method":"POST"`)
	require.Contains(t, out, `"status":204`)
}

func TestAudit_SkipsReads(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	h := Audit(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/dynamo/tables", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, buf.String())
}
