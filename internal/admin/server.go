package admin

import (
	"io/fs"
	"log/slog"
	"net/http"
	"reflect"
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
	if isNilCredentialStore(deps.Credentials) {
		return nil, errMissing("Credentials")
	}
	if deps.Roles == nil {
		// A nil role index would silently 403 every login. Treat it
		// as a wiring bug rather than a valid "admin is locked down"
		// state: operators who really want zero admin access can
		// set admin.enabled=false or pass an empty (non-nil) map.
		return nil, errMissing("Roles")
	}
	if deps.ClusterInfo == nil {
		return nil, errMissing("ClusterInfo")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Inject the logger into AuthService so login/logout can emit
	// their own admin_audit entries (login runs before SessionAuth,
	// so the generic Audit middleware cannot record the claimed
	// actor on its own).
	authOpts := deps.AuthOpts
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
// We wrap the router in BodyLimit at the top level so every endpoint
// — including /admin/healthz and the static asset / SPA paths — is
// protected from oversized request bodies. The login / logout / API
// chains apply BodyLimit again internally; both layers cap to the
// same default, so the effective limit stays at defaultBodyLimit and
// the redundant wrap is a no-op rather than a smaller cap.
func (s *Server) Handler() http.Handler {
	return BodyLimit(defaultBodyLimit)(s.router)
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

	// The protected chain: body limit → session auth → audit → CSRF.
	// Audit is deliberately placed before CSRF so that CSRF-rejected
	// protected requests are still written to the audit log — the
	// actor is already known at that point because SessionAuth ran.
	// If CSRF were wrapped inside Audit, every csrf_missing /
	// csrf_mismatch rejection would silently escape auditing, which
	// is exactly the attack trace operators want to see.
	protect := func(next http.Handler) http.Handler {
		return BodyLimit(defaultBodyLimit)(
			SessionAuth(verifier)(
				Audit(logger)(
					CSRFDoubleSubmit()(next),
				),
			),
		)
	}
	// Logout shares SessionAuth + CSRF with `protect` but skips the
	// generic Audit middleware: HandleLogout emits its own
	// admin_audit entry (action=logout, actor decoded from the
	// session cookie) that carries strictly more context than the
	// generic line. Wrapping Audit too would produce two audit lines
	// per logout for no extra information.
	protectNoAudit := func(next http.Handler) http.Handler {
		return BodyLimit(defaultBodyLimit)(
			SessionAuth(verifier)(
				CSRFDoubleSubmit()(next),
			),
		)
	}
	// Login is the only endpoint that runs without a pre-existing
	// session — every other write must go through session + CSRF so
	// a cross-site page cannot force a user to perform any state
	// change against their will. Notably, /auth/logout goes through
	// the protected chain to prevent logout-CSRF: a hostile site that
	// POSTed to /auth/logout would otherwise be able to force-clear a
	// victim's cookies even with SameSite=Strict sessions, because
	// HandleLogout always emits expired Set-Cookie headers.
	publicAuth := func(next http.Handler) http.Handler {
		return BodyLimit(defaultBodyLimit)(next)
	}

	// Build each route's middleware stack exactly once at startup
	// rather than rebuilding 4–5 closures per inbound request.
	loginChain := publicAuth(loginHandler)
	logoutChain := protectNoAudit(logoutHandler)
	clusterChain := protect(clusterHandler)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/api/v1/auth/login":
			loginChain.ServeHTTP(w, r)
		case "/admin/api/v1/auth/logout":
			logoutChain.ServeHTTP(w, r)
		case "/admin/api/v1/cluster":
			clusterChain.ServeHTTP(w, r)
		default:
			writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
				"no admin API handler is registered for this path")
		}
	})
}

func errMissing(field string) error {
	return &missingDepError{field: field}
}

type missingDepError struct{ field string }

func (e *missingDepError) Error() string {
	return "admin.NewServer: required dependency missing: " + e.field
}

// isNilCredentialStore treats both untyped-nil and typed-nil reference
// values (e.g. `MapCredentialStore(nil)`) as missing. Without the typed
// check, a caller that wraps a nil map in our CredentialStore type slips
// past a plain `== nil` guard and the admin listener silently rejects
// every login with "invalid_credentials" at runtime.
func isNilCredentialStore(cs CredentialStore) bool {
	if cs == nil {
		return true
	}
	v := reflect.ValueOf(cs)
	if isNilableKind(v.Kind()) {
		return v.IsNil()
	}
	return false
}

// nilableKinds is the set of reflect.Kind values that can legitimately
// hold a nil value. Keeping it as a package-level lookup (rather than a
// switch) lets the exhaustive linter see that we are intentionally
// matching a small allow-list rather than forgetting a case.
var nilableKinds = map[reflect.Kind]struct{}{
	reflect.Map:       {},
	reflect.Ptr:       {},
	reflect.Slice:     {},
	reflect.Chan:      {},
	reflect.Func:      {},
	reflect.Interface: {},
}

func isNilableKind(k reflect.Kind) bool {
	_, ok := nilableKinds[k]
	return ok
}
