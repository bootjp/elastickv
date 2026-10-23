package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteForwardedResult(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		res        *ForwardResult
		wantStatus int
		wantBody   string
		wantRetry  string
	}{
		{
			name:       "valid JSON is relayed compacted",
			res:        &ForwardResult{StatusCode: http.StatusCreated, Payload: []byte(`{ "name" : "users" }`), ContentType: contentTypeJSONUTF8},
			wantStatus: http.StatusCreated,
			wantBody:   `{"name":"users"}`,
		},
		{
			name:       "HTML-significant characters are escaped",
			res:        &ForwardResult{StatusCode: http.StatusOK, Payload: []byte(`{"message":"<script>a&b</script>"}`), ContentType: contentTypeJSONUTF8},
			wantStatus: http.StatusOK,
			wantBody:   `{"message":"\u003cscript\u003ea\u0026b\u003c/script\u003e"}`,
		},
		{
			name:       "empty payload writes no body",
			res:        &ForwardResult{StatusCode: http.StatusNoContent, ContentType: contentTypeJSONUTF8},
			wantStatus: http.StatusNoContent,
			wantBody:   "",
		},
		{
			name:       "503 keeps Retry-After",
			res:        &ForwardResult{StatusCode: http.StatusServiceUnavailable, Payload: []byte(`{"error":"leader_unavailable"}`), ContentType: contentTypeJSONUTF8},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":"leader_unavailable"}`,
			wantRetry:  "1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
			writeForwardedResult(rec, req, nil, tc.res, "write failed")
			require.Equal(t, tc.wantStatus, rec.Code)
			require.Equal(t, tc.wantBody, rec.Body.String())
			require.Equal(t, tc.res.ContentType, rec.Header().Get("Content-Type"))
			require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
			require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			require.Equal(t, tc.wantRetry, rec.Header().Get("Retry-After"))
		})
	}
}

func TestWriteForwardedResultRejectsMalformedPayload(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/dynamo/tables", nil)
	res := &ForwardResult{StatusCode: http.StatusOK, Payload: []byte(`<html>not json</html>`), ContentType: "text/html"}
	writeForwardedResult(rec, req, nil, res, "write failed")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, contentTypeJSONUTF8, rec.Header().Get("Content-Type"))
	require.JSONEq(t, `{"error":"bad_gateway","message":"leader returned a malformed response"}`, rec.Body.String())
}

func TestAdminCookieAttributes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		secure   bool
		httpOnly bool
	}{
		{"secure session", true, true},
		{"secure csrf", true, false},
		{"insecure session", false, true},
		{"insecure csrf", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &AuthService{secureCookie: tc.secure, cookieDomain: "example.test", sessionTTL: time.Hour}
			live := s.buildCookie("n", "v", tc.httpOnly)
			require.Equal(t, tc.secure, live.Secure)
			require.Equal(t, tc.httpOnly, live.HttpOnly)
			require.Equal(t, http.SameSiteStrictMode, live.SameSite)
			require.Equal(t, pathPrefixAdmin, live.Path)
			require.Equal(t, "example.test", live.Domain)
			require.Equal(t, "v", live.Value)
			require.Equal(t, int(time.Hour.Seconds()), live.MaxAge)
			require.True(t, live.Expires.IsZero())

			expired := s.buildExpiredCookie("n", tc.httpOnly)
			require.Equal(t, tc.secure, expired.Secure)
			require.Equal(t, tc.httpOnly, expired.HttpOnly)
			require.Equal(t, http.SameSiteStrictMode, expired.SameSite)
			require.Equal(t, "", expired.Value)
			require.Equal(t, -1, expired.MaxAge)
			require.Equal(t, time.Unix(0, 0), expired.Expires)
		})
	}
}
