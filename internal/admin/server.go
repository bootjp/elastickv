package admin

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
)

// ServerDeps bundles the collaborators the admin HTTP surface needs. All
// fields are required unless noted otherwise. Construct once at startup
// and pass to NewServer.
type ServerDeps struct {
	// Signer issues session tokens. Keyed with the primary HS256 key.
	Signer *Signer

	// Verifier validates session tokens. Configured with the primary
	// key and, optionally, the previous key for rotation support.
	Verifier *Verifier

	// Credentials is the server-side access key → secret map used by
	// the login endpoint. Sharing this with the S3/DynamoDB adapters
	// keeps authentication consistent.
	Credentials CredentialStore

	// Roles maps each admin-allowed access key to its Role. Keys not
	// present are rejected at login with 403, even if their SigV4
	// secret validates against Credentials.
	Roles map[string]Role

	// ClusterInfo describes the local node's Raft state.
	ClusterInfo ClusterInfoSource

	// StaticFS is the embed.FS (or any fs.FS) backing the SPA. May be
	// nil during early development; the router renders 404 for
	// /admin/assets/* and the SPA fallback in that case.
	StaticFS fs.FS

	// AuthOpts configures cookie attributes and rate limiting. Zero
	// values pick production-appropriate defaults.
	AuthOpts AuthServiceOpts

	// Logger is the slog destination for admin_audit entries. nil
	// falls back to slog.Default().
	Logger *slog.Logger
}

// Server is the composed admin HTTP handler. Obtain one via NewServer and
// hand its Handler() to an http.Server listening on the admin address.
type Server struct {
	deps   ServerDeps
	router *Router
	auth   *AuthService
	mux    http.Handler
}

// NewServer constructs the admin Server. It returns an error only if the
// dependencies are inconsistent enough to be unusable; otherwise it is
// total over its configuration space.
func NewServer(deps ServerDeps) (*Server, error) {
	if deps.Signer == nil {
		return nil, errMissing("Signer")
	}
	if deps.Verifier == nil {
		return nil, errMissing("Verifier")
	}
	if deps.Credentials == nil {
		return nil, errMissing("Credentials")
	}
	if deps.ClusterInfo == nil {
		return nil, errMissing("ClusterInfo")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Inject verifier + logger into AuthService so login/logout can
	// emit admin_audit entries themselves (the generic Audit
	// middleware cannot, because those endpoints do not run through
	// SessionAuth).
	authOpts := deps.AuthOpts
	if authOpts.Verifier == nil {
		authOpts.Verifier = deps.Verifier
	}
	if authOpts.Logger == nil {
		authOpts.Logger = logger
	}
	auth := NewAuthService(deps.Signer, deps.Credentials, deps.Roles, authOpts)
	cluster := NewClusterHandler(deps.ClusterInfo).WithLogger(logger)
	mux := buildAPIMux(auth, deps.Verifier, cluster, logger)
	router := NewRouter(mux, deps.StaticFS)
	return &Server{deps: deps, router: router, auth: auth, mux: mux}, nil
}

// Handler returns an http.Handler that serves the full admin surface.
func (s *Server) Handler() http.Handler {
	return s.router
}

// APIHandler returns just the /admin/api/v1/* subtree. Tests that want
// to bypass static-file routing call this to avoid building an fs.FS.
func (s *Server) APIHandler() http.Handler {
	return s.mux
}

// buildAPIMux composes the JSON API sub-handler. Kept package-private so
// external callers go through Server.
//
// Layout:
//
//	POST   /admin/api/v1/auth/login     (no auth, rate-limited)
//	POST   /admin/api/v1/auth/logout    (no auth required)
//	GET    /admin/api/v1/cluster        (auth required)
//
// Body limit applies uniformly. CSRF and Audit middleware apply to
// write-capable protected endpoints; login and logout carry their own
// audit path inside AuthService because the generic Audit middleware
// cannot see the claimed actor at that point in the chain.
func buildAPIMux(auth *AuthService, verifier *Verifier, clusterHandler http.Handler, logger *slog.Logger) http.Handler {
	loginHandler := http.HandlerFunc(auth.HandleLogin)
	logoutHandler := http.HandlerFunc(auth.HandleLogout)

	// The protected chain: body limit → session auth → CSRF → audit.
	protect := func(next http.Handler) http.Handler {
		return BodyLimit(defaultBodyLimit)(
			SessionAuth(verifier)(
				CSRFDoubleSubmit()(
					Audit(logger)(next),
				),
			),
		)
	}
	// Login / logout: body limit only. Audit is handled inside the
	// AuthService so the actor field is populated correctly.
	publicAuth := func(next http.Handler) http.Handler {
		return BodyLimit(defaultBodyLimit)(next)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/api/v1/auth/login":
			publicAuth(loginHandler).ServeHTTP(w, r)
		case "/admin/api/v1/auth/logout":
			publicAuth(logoutHandler).ServeHTTP(w, r)
		case "/admin/api/v1/cluster":
			protect(clusterHandler).ServeHTTP(w, r)
		default:
			writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
				"no admin API handler is registered for this path")
		}
	})
}

// ctxClosed is a small helper for shutdown signalling; kept here so
// tests do not need to import context directly.
func ctxClosed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func errMissing(field string) error {
	return &missingDepError{field: field}
}

type missingDepError struct{ field string }

func (e *missingDepError) Error() string {
	return "admin.NewServer: required dependency missing: " + e.field
}

// endpointMatches is a small helper kept here for readability in table
// driven path-routing tests.
func endpointMatches(path, want string) bool {
	return strings.EqualFold(path, want)
}
