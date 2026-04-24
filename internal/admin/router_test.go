package admin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func newTestStatic() fstest.MapFS {
	return fstest.MapFS{
		"index.html":     {Data: []byte("<!doctype html><html>spa</html>")},
		"assets/app.js":  {Data: []byte("console.log('ok');")},
		"assets/app.css": {Data: []byte("body { color: red; }")},
	}
}

func TestRouter_APIPathIsNeverSwallowedBySPA(t *testing.T) {
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Came-From", "api")
		writeJSONError(w, http.StatusNotFound, "unknown_endpoint", "no handler")
	})
	r := NewRouter(api, newTestStatic())

	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/unknown", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "api", rec.Header().Get("X-Came-From"))
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.NotContains(t, rec.Body.String(), "<html")
}

func TestRouter_HealthzReturnsPlainText(t *testing.T) {
	r := NewRouter(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/admin/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/plain")
	require.Equal(t, "ok\n", rec.Body.String())
}

func TestRouter_HealthzHeadNoBody(t *testing.T) {
	r := NewRouter(nil, nil)
	req := httptest.NewRequest(http.MethodHead, "/admin/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "", rec.Body.String())
}

func TestRouter_HealthzRejectsPost(t *testing.T) {
	r := NewRouter(nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/admin/healthz", strings.NewReader(""))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestRouter_StaticAssetServed(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodGet, "/admin/assets/app.js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "console.log")
}

func TestRouter_StaticAssetMissingReturnsJSONNotFound(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodGet, "/admin/assets/missing.js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
}

func TestRouter_StaticTraversalRejected(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodGet, "/admin/assets/../index.html", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	// Requested path cleaned by net/http before reaching us is still
	// under /admin/ — defence in depth: we reject any ".." segment.
	require.NotEqual(t, http.StatusOK, rec.Code)
}

// Filenames containing ".." as a substring (but not as a path segment)
// are legitimate — the guard must not reject `app..js`.
func TestRouter_StaticSubstringDotDotAllowed(t *testing.T) {
	fs := fstest.MapFS{
		"index.html":     {Data: []byte("<!doctype html>")},
		"assets/app..js": {Data: []byte("console.log('double-dot');")},
	}
	r := NewRouter(nil, fs)
	req := httptest.NewRequest(http.MethodGet, "/admin/assets/app..js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "double-dot")
}

// A client-supplied double slash must resolve to a JSON 404, not a 500.
func TestRouter_StaticDoubleSlashReturnsJSON404(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodGet, "/admin/assets//app.js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
}

func TestRouter_SPAFallbackServesIndex(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	for _, p := range []string{"/admin", "/admin/", "/admin/dynamo", "/admin/s3/bucket-42"} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		require.Equalf(t, http.StatusOK, rec.Code, "path %s", p)
		require.Containsf(t, rec.Body.String(), "spa", "path %s", p)
	}
}

func TestRouter_SPAWithoutStaticReturns404(t *testing.T) {
	r := NewRouter(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/admin/dynamo", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRouter_BareAPIRootReturnsJSON404NotHTML(t *testing.T) {
	r := NewRouter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), newTestStatic())
	// /admin/api, /admin/api/v1 (bare), /admin/api/v2 (unimplemented),
	// /admin/api/v2/foo (deeper under unknown version), and the bare
	// /admin/assets directory must all resolve to a JSON 404 — never
	// the SPA HTML fallback — so API clients and probes get a
	// machine-readable answer.
	paths := []string{
		"/admin/api",
		"/admin/api/v1",
		"/admin/api/v2",
		"/admin/api/v2/tables",
		"/admin/assets",
	}
	for _, p := range paths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equalf(t, http.StatusNotFound, rec.Code, "path %s", p)
		require.Containsf(t, rec.Header().Get("Content-Type"), "application/json", "path %s", p)
		require.NotContainsf(t, rec.Body.String(), "<html", "path %s must not serve SPA HTML", p)
	}
}

func TestRouter_NonAdminPathReturns404JSON(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodGet, "/other", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
}

func TestRouter_APIHandlerReceivesFullPath(t *testing.T) {
	var received string
	api := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		received = r.URL.Path
	})
	r := NewRouter(api, newTestStatic())

	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/dynamo/tables", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, "/admin/api/v1/dynamo/tables", received)
	_ = io.Discard
}
