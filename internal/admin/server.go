package admin

import (
	"io/fs"
	"log/slog"
	"net/http"
	"reflect"
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

	// Tables is the DynamoDB admin source — covers list, describe,
	// create, and delete via TablesSource. Optional: a nil value
	// disables /admin/api/v1/dynamo/tables{,/{name}} (the mux
	// answers them with 404). This lets a build that ships only the
	// cluster page deploy without standing up the dynamo bridge.
	Tables TablesSource

	// Forwarder is the LeaderForwarder that the Dynamo handler hands
	// off ErrTablesNotLeader writes to (design 3.3, AdminForward).
	// Optional: a nil value disables follower→leader forwarding, in
	// which case the handler surfaces 503 + Retry-After: 1 directly.
	// Single-node and leader-only deployments leave this nil; multi-
	// node clusters wire the production gRPC client.
	Forwarder LeaderForwarder

	// Buckets is the S3 admin source — read-only in this slice
	// (list + describe). Optional: a nil value disables
	// /admin/api/v1/s3/buckets{,/{name}} (the mux answers them
	// with 404). Mirrors the Tables nil contract for cluster-only
	// builds.
	Buckets BucketsSource

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
	if err := validateServerDeps(deps); err != nil {
		return nil, err
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
	dynamo := buildDynamoHandlerForDeps(deps, logger)
	s3 := buildS3HandlerForDeps(deps, logger)
	mux := buildAPIMux(auth, deps.Verifier, cluster, dynamo, s3, logger)
	router := NewRouter(mux, deps.StaticFS)
	return &Server{deps: deps, router: router, auth: auth, mux: mux}, nil
}

// validateServerDeps centralises the wiring-bug guards that NewServer
// applies before constructing anything. Pulled out so NewServer's own
// body can stay under the cyclomatic-complexity ceiling without
// hiding the contract — every required field is enumerated here.
func validateServerDeps(deps ServerDeps) error {
	switch {
	case deps.Signer == nil:
		return errMissing("Signer")
	case deps.Verifier == nil:
		return errMissing("Verifier")
	case isNilCredentialStore(deps.Credentials):
		return errMissing("Credentials")
	case deps.Roles == nil:
		// A nil role index would silently 403 every login. Treat
		// it as a wiring bug rather than a valid "admin is locked
		// down" state: operators who really want zero admin
		// access can set admin.enabled=false or pass an empty
		// (non-nil) map.
		return errMissing("Roles")
	case deps.ClusterInfo == nil:
		return errMissing("ClusterInfo")
	}
	return nil
}

// buildDynamoHandlerForDeps assembles the Dynamo HTTP handler from
// ServerDeps when Tables is wired, threading the logger and the
// optional LeaderForwarder. Returns nil when Tables is nil so the
// router falls through to the unknown-endpoint 404.
//
// Re-evaluates the principal's role on every state-changing request
// against the live role map (Codex P1 on PR #635). MapRoleStore
// wraps the same map the auth layer uses for login, so a config
// reload that updates deps.Roles does NOT automatically propagate
// here — operators must restart the listener for revocation to take
// effect, but the JWT no longer extends a revoked key past the next
// request.
func buildDynamoHandlerForDeps(deps ServerDeps, logger *slog.Logger) http.Handler {
	if deps.Tables == nil {
		return nil
	}
	h := NewDynamoHandler(deps.Tables).
		WithLogger(logger).
		WithRoleStore(MapRoleStore(deps.Roles))
	if deps.Forwarder != nil {
		h = h.WithLeaderForwarder(deps.Forwarder)
	}
	return h
}

// buildS3HandlerForDeps is the parallel constructor for the S3
// admin handler. Slice 1 is read-only; the next slice will plumb a
// MapRoleStore and the LeaderForwarder through the same shape.
func buildS3HandlerForDeps(deps ServerDeps, logger *slog.Logger) http.Handler {
	if deps.Buckets == nil {
		return nil
	}
	return NewS3Handler(deps.Buckets).WithLogger(logger)
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
//	POST   /admin/api/v1/auth/login                 (no auth, rate-limited)
//	POST   /admin/api/v1/auth/logout                (auth required)
//	GET    /admin/api/v1/cluster                    (auth required)
//	GET    /admin/api/v1/dynamo/tables              (auth required)
//	POST   /admin/api/v1/dynamo/tables              (auth required, full role)
//	GET    /admin/api/v1/dynamo/tables/{name}       (auth required)
//	DELETE /admin/api/v1/dynamo/tables/{name}       (auth required, full role)
//	GET    /admin/api/v1/s3/buckets                 (auth required)
//	GET    /admin/api/v1/s3/buckets/{name}          (auth required)
//
// Body limit applies uniformly. CSRF and Audit middleware apply to
// write-capable protected endpoints; login and logout carry their own
// audit path inside AuthService because the generic Audit middleware
// cannot see the claimed actor at that point in the chain.
//
// dynamoHandler / s3Handler may be nil; in that case the corresponding
// paths fall through to the unknown-endpoint 404, matching the
// behaviour of any other unregistered admin path.
func buildAPIMux(auth *AuthService, verifier *Verifier, clusterHandler, dynamoHandler, s3Handler http.Handler, logger *slog.Logger) http.Handler {
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
	// Dynamo endpoints (reads and writes) share the protect chain
	// so a missing session or CSRF token 401s/403s the same way
	// regardless of method. The Audit middleware is a no-op for
	// GET (it only logs state-changing methods) so dashboard polls
	// don't flood the audit log, while POST/DELETE always do.
	var dynamoChain http.Handler
	if dynamoHandler != nil {
		dynamoChain = protect(dynamoHandler)
	}
	// S3 endpoints (read-only in this slice) share the protect chain
	// for the same reason: even GETs need a session + CSRF cookie so
	// a cross-site page cannot enumerate bucket names by tricking a
	// logged-in browser into a fetch with credentials.
	var s3Chain http.Handler
	if s3Handler != nil {
		s3Chain = protect(s3Handler)
	}

	routes := apiRouteTable{
		login:   loginChain,
		logout:  logoutChain,
		cluster: clusterChain,
		dynamo:  dynamoChain,
		s3:      s3Chain,
	}
	return http.HandlerFunc(routes.dispatch)
}

// apiRouteTable bundles the precomposed middleware chains for each
// admin API path family. Pulled into a type so the dispatch switch
// keeps buildAPIMux under the cyclop ceiling — every additional
// resource family (S3 buckets here, future SQS / queues / etc.)
// would otherwise push buildAPIMux's branch count past the limit.
type apiRouteTable struct {
	login, logout, cluster http.Handler
	dynamo, s3             http.Handler
}

// dispatch is the receiver method httpHandlerFunc adapts. Logic is
// the same path-prefix switch the call site previously inlined.
func (t apiRouteTable) dispatch(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/admin/api/v1/auth/login":
		t.login.ServeHTTP(w, r)
	case r.URL.Path == "/admin/api/v1/auth/logout":
		t.logout.ServeHTTP(w, r)
	case r.URL.Path == "/admin/api/v1/cluster":
		t.cluster.ServeHTTP(w, r)
	case t.dynamo != nil && isDynamoPath(r.URL.Path):
		t.dynamo.ServeHTTP(w, r)
	case t.s3 != nil && isS3Path(r.URL.Path):
		t.s3.ServeHTTP(w, r)
	default:
		writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
			"no admin API handler is registered for this path")
	}
}

func isDynamoPath(p string) bool {
	return p == pathDynamoTables || strings.HasPrefix(p, pathPrefixDynamoTables)
}

func isS3Path(p string) bool {
	return p == pathS3Buckets || strings.HasPrefix(p, pathPrefixS3Buckets)
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
// hold a nil value — every reference kind, plus interface. Keeping it
// as a package-level lookup (rather than a switch) makes the intent
// — an explicit allow-list of nilable kinds — easier to audit.
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
