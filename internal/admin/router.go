package admin

import (
	"errors"
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
const (
	pathPrefixAdmin   = "/admin"
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
	p := r.URL.Path

	switch {
	case p == pathAPIv1Root:
		// Bare /admin/api/v1 is an API root, not an SPA page: answer
		// with a JSON 404 so clients never get HTML back on an API
		// path.
		rt.notFind.ServeHTTP(w, r)
		return
	case strings.HasPrefix(p, pathPrefixAPIv1):
		if rt.api == nil {
			rt.notFind.ServeHTTP(w, r)
			return
		}
		rt.api.ServeHTTP(w, r)
		return
	case p == pathHealthz:
		rt.serveHealth(w, r)
		return
	case p == pathAssetsRoot:
		// Same reasoning as /admin/api/v1: the bare assets root is
		// a directory, not an SPA route.
		rt.notFind.ServeHTTP(w, r)
		return
	case strings.HasPrefix(p, pathPrefixAssets):
		rt.serveAsset(w, r)
		return
	case p == pathPrefixAdmin || strings.HasPrefix(p, pathPrefixAdmin+"/"):
		rt.serveSPA(w, r)
		return
	}
	rt.notFind.ServeHTTP(w, r)
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
	readSeeker, ok := f.(interface {
		Read(p []byte) (int, error)
		Seek(offset int64, whence int) (int64, error)
	})
	if !ok {
		// embed.FS files implement ReadSeeker, but be defensive.
		writeJSONError(w, http.StatusInternalServerError, "internal", "asset is not seekable")
		return
	}
	http.ServeContent(w, r, name, info.ModTime(), readSeeker)
}

func (rt *Router) serveSPA(w http.ResponseWriter, r *http.Request) {
	if rt.static == nil {
		rt.notFind.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or HEAD supported")
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
	readSeeker, ok := f.(interface {
		Read(p []byte) (int, error)
		Seek(offset int64, whence int) (int64, error)
	})
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
