package admin

import (
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

// TestRouter_HealthzLeader_ReturnsOKWhenLeader locks down the
// happy path on /admin/healthz/leader: a verified leader probe
// produces 200 + "ok\n" + text/plain. Mirrors the S3 / Dynamo
// /healthz/leader contract so a multi-protocol load balancer
// sees identical semantics.
func TestRouter_HealthzLeader_ReturnsOKWhenLeader(t *testing.T) {
	r := NewRouterWithLeaderProbe(nil, nil, LeaderProbeFunc(func() bool { return true }))
	req := httptest.NewRequest(http.MethodGet, "/admin/healthz/leader", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/plain")
	require.Equal(t, "ok\n", rec.Body.String())
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
}

// TestRouter_HealthzLeader_Returns503WhenNotLeader locks down the
// non-leader contract: 503 Service Unavailable + "not leader\n".
// A load balancer probing this endpoint takes the node out of
// rotation when it loses leadership; the body string is informative
// for operators reading curl output.
func TestRouter_HealthzLeader_Returns503WhenNotLeader(t *testing.T) {
	r := NewRouterWithLeaderProbe(nil, nil, LeaderProbeFunc(func() bool { return false }))
	req := httptest.NewRequest(http.MethodGet, "/admin/healthz/leader", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/plain")
	require.Equal(t, "not leader\n", rec.Body.String())
}

// TestRouter_HealthzLeader_HeadOmitsBody mirrors the existing
// healthz HEAD test. The status code must still indicate the
// leader state; only the body is suppressed.
func TestRouter_HealthzLeader_HeadOmitsBody(t *testing.T) {
	rLeader := NewRouterWithLeaderProbe(nil, nil, LeaderProbeFunc(func() bool { return true }))
	req := httptest.NewRequest(http.MethodHead, "/admin/healthz/leader", nil)
	rec := httptest.NewRecorder()
	rLeader.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "", rec.Body.String())

	rFollower := NewRouterWithLeaderProbe(nil, nil, LeaderProbeFunc(func() bool { return false }))
	req = httptest.NewRequest(http.MethodHead, "/admin/healthz/leader", nil)
	rec = httptest.NewRecorder()
	rFollower.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "", rec.Body.String())
}

// TestRouter_HealthzLeader_RejectsPost guards the method allowlist:
// only GET / HEAD are accepted. Mirrors TestRouter_HealthzRejectsPost.
func TestRouter_HealthzLeader_RejectsPost(t *testing.T) {
	r := NewRouterWithLeaderProbe(nil, nil, LeaderProbeFunc(func() bool { return true }))
	req := httptest.NewRequest(http.MethodPost, "/admin/healthz/leader", strings.NewReader(""))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestRouter_HealthzLeader_NilProbeReturns404 locks down the
// "feature off" pattern: when the LeaderProbe is not wired, the
// router answers /admin/healthz/leader with the standard JSON 404
// — distinct from the operational 503 (which means "wired, not
// leader"). Single-node dev runs and admin builds without runtime
// access fall here.
func TestRouter_HealthzLeader_NilProbeReturns404(t *testing.T) {
	r := NewRouter(nil, newTestStatic()) // NewRouter passes nil probe to NewRouterWithLeaderProbe
	req := httptest.NewRequest(http.MethodGet, "/admin/healthz/leader", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.Contains(t, rec.Body.String(), "not_found")
}

// TestRouter_HealthzLeader_NotSwallowedBySPA locks down the
// classify ordering: /admin/healthz/leader must reach the leader
// handler (200/503) — not the SPA fallback that would otherwise
// answer with index.html. A regression here would cause a load
// balancer probing the path to see HTML 200 forever and never
// detect a leadership change.
func TestRouter_HealthzLeader_NotSwallowedBySPA(t *testing.T) {
	probe := LeaderProbeFunc(func() bool { return false })
	r := NewRouterWithLeaderProbe(nil, newTestStatic(), probe)
	req := httptest.NewRequest(http.MethodGet, "/admin/healthz/leader", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.NotContains(t, rec.Body.String(), "<html")
}

func TestRouter_StaticAssetServed(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodGet, "/admin/assets/app.js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "console.log")
}

func TestRouter_StaticAssetRejectsPost(t *testing.T) {
	r := NewRouter(nil, newTestStatic())
	req := httptest.NewRequest(http.MethodPost, "/admin/assets/app.js", strings.NewReader("payload"))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Contains(t, rec.Body.String(), "method_not_allowed")
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

	// A ".." path segment must resolve to the exact JSON 404 — any
	// other response (302/400/500) would hide a regression in the
	// validation path (e.g. the file opening and returning a 500).
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
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
}
