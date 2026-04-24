package admin

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/goccy/go-json"
)

// Constants for the admin URL namespace. Centralised here so the router,
// handlers, and tests all agree on the paths. The admin listener only
// serves URLs under /admin/*; anything else yields a 404.
//
// The "root" variants (without a trailing slash) are treated as the
// directory itself so that requests like `/admin/api/v1` or
// `/admin/assets` resolve to a JSON 404 rather than falling through to
// the SPA fallback and being answered with index.html.
//
// pathAPIRoot / pathPrefixAPI guard the whole `/admin/api*` namespace —
// not just v1 — so that requests to currently-unimplemented API
// versions (`/admin/api`, `/admin/api/v2`, ...) return a JSON 404
// instead of being silently answered with the SPA HTML.
const (
	pathPrefixAdmin   = "/admin"
	pathAPIRoot       = "/admin/api"
	pathPrefixAPI     = pathAPIRoot + "/"
	pathAPIv1Root     = "/admin/api/v1"
	pathPrefixAPIv1   = pathAPIv1Root + "/"
	pathHealthz       = "/admin/healthz"
	pathAssetsRoot    = "/admin/assets"
	pathPrefixAssets  = pathAssetsRoot + "/"
	pathRootAssetsDir = "assets"
	pathIndexHTML     = "index.html"
)

// APIHandler is the bridge between the router and all JSON API endpoints.
// Everything under /admin/api/v1/ resolves through it; individual endpoint
// routing is the handler's responsibility (see apiMux below).
type APIHandler http.Handler

// Router dispatches admin HTTP requests in the strict order mandated by
// the design doc (Section 5.3): API routes first, then healthz, then
// static assets, then SPA fallback. We do NOT use http.ServeMux because
// its LongestPrefix matching rules would let /admin/api/v1/... slip into
// the SPA catch-all when the JSON handler returns a 404.
type Router struct {
	api     http.Handler
	static  fs.FS
	notFind http.Handler
}

// NewRouter builds the admin router.
//
//   - api handles /admin/api/v1/*. It must return a JSON body itself; the
//     router never rewrites its response.
//   - static, if non-nil, backs both /admin/assets/* and the /admin/*
//     SPA catch-all (which always serves index.html). A nil static FS
//     causes 404s for asset and SPA routes, which is the expected state
//     while the SPA has not been built yet.
func NewRouter(api http.Handler, static fs.FS) *Router {
	return &Router{
		api:     api,
		static:  static,
		notFind: http.HandlerFunc(writeJSONNotFound),
	}
}

// ServeHTTP is the single entrypoint. Routing cascade in priority order:
//  1. /admin/api/v1/* → API handler
//  2. /admin/healthz → plain text
//  3. /admin/assets/* → static file
//  4. /admin/* → index.html (SPA fallback)
//  5. anything else → 404 JSON
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rt.dispatch(rt.classify(r.URL.Path)).ServeHTTP(w, r)
}

// routeKind enumerates the admin URL classes the router distinguishes.
// Splitting classify/dispatch keeps ServeHTTP under the cyclomatic
// complexity ceiling while preserving the strict evaluation order that
// API-before-SPA routing depends on.
type routeKind int

const (
	routeAPIv1 routeKind = iota
	routeAPIOther
	routeHealthz
	routeAssetsRoot
	routeAsset
	routeSPA
	routeUnknown
)

func (rt *Router) classify(p string) routeKind {
	if k, ok := classifyAPI(p); ok {
		return k
	}
	if k, ok := classifyAssets(p); ok {
		return k
	}
	if p == pathHealthz {
		return routeHealthz
	}
	if p == pathPrefixAdmin || strings.HasPrefix(p, pathPrefixAdmin+"/") {
		return routeSPA
	}
	return routeUnknown
}

func classifyAPI(p string) (routeKind, bool) {
	switch {
	case strings.HasPrefix(p, pathPrefixAPIv1):
		return routeAPIv1, true
	case p == pathAPIRoot, p == pathAPIv1Root, strings.HasPrefix(p, pathPrefixAPI):
		return routeAPIOther, true
	}
	return 0, false
}

func classifyAssets(p string) (routeKind, bool) {
	switch {
	case p == pathAssetsRoot:
		return routeAssetsRoot, true
	case strings.HasPrefix(p, pathPrefixAssets):
		return routeAsset, true
	}
	return 0, false
}

func (rt *Router) dispatch(k routeKind) http.Handler {
	switch k {
	case routeAPIv1:
		if rt.api == nil {
			return rt.notFind
		}
		return rt.api
	case routeHealthz:
		return http.HandlerFunc(rt.serveHealth)
	case routeAsset:
		return http.HandlerFunc(rt.serveAsset)
	case routeSPA:
		return http.HandlerFunc(rt.serveSPA)
	case routeAPIOther, routeAssetsRoot, routeUnknown:
		return rt.notFind
	}
	return rt.notFind
}

func (rt *Router) serveHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or HEAD supported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write([]byte("ok\n"))
	}
}

func (rt *Router) serveAsset(w http.ResponseWriter, r *http.Request) {
	if rt.static == nil {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	// Drop /admin/assets/ prefix → relative path under pathRootAssetsDir.
	rel := strings.TrimPrefix(r.URL.Path, pathPrefixAssets)
	// Defence against traversal and malformed paths: require the
	// already-normalised form that fs.ValidPath enforces (no
	// ".." segments, no "//" segments, no leading "/"). Anything
	// that does not pass validation resolves to a 404 JSON rather
	// than risking a 500 from the underlying fs.FS — legitimate
	// filenames containing ".." as a substring (e.g. "app..js")
	// still pass because ValidPath checks segments, not substrings.
	if rel == "" || !fs.ValidPath(rel) {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	name := path.Join(pathRootAssetsDir, rel)
	f, err := rt.static.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			rt.notFind.ServeHTTP(w, r)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to open asset")
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	readSeeker, ok := f.(io.ReadSeeker)
	if !ok {
		// embed.FS files implement ReadSeeker, but be defensive.
		writeJSONError(w, http.StatusInternalServerError, "internal", "asset is not seekable")
		return
	}
	http.ServeContent(w, r, name, info.ModTime(), readSeeker)
}

func (rt *Router) serveSPA(w http.ResponseWriter, r *http.Request) {
	// Reject non-GET/HEAD methods before inspecting rt.static so the
	// response is uniform across admin binaries whether or not the
	// SPA bundle happens to be configured. Without this, a POST to
	// /admin/something returned a JSON 404 with a nil static and a
	// JSON 405 with a populated static — same URL, different answer.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or HEAD supported")
		return
	}
	if rt.static == nil {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	f, err := rt.static.Open(pathIndexHTML)
	if err != nil {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	readSeeker, ok := f.(io.ReadSeeker)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "internal", "index.html is not seekable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	http.ServeContent(w, r, pathIndexHTML, info.ModTime(), readSeeker)
}

// errorResponse is the JSON shape for every admin error.
type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

func writeJSONNotFound(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusNotFound, "not_found", "")
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: code, Message: msg})
}
