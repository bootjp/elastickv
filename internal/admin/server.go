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

	// Queues is the SQS admin source — covers list, describe, and
	// delete via QueuesSource. Optional: a nil value disables
	// /admin/api/v1/sqs/queues{,/{name}} (the mux answers them with
	// 404). Same opt-in shape as Tables; deployments that don't run
	// the SQS adapter omit this without breaking the rest of the
	// admin surface.
	Queues QueuesSource

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
	var dynamo http.Handler
	if deps.Tables != nil {
		// Re-evaluate the principal's role on every state-
		// changing request against the live role map (Codex P1
		// on PR #635). MapRoleStore wraps the same map the auth
		// layer uses for login, so a config reload that updates
		// deps.Roles does NOT automatically propagate here —
		// operators must restart the listener for revocation to
		// take effect, but the JWT no longer extends a revoked
		// key past the next request.
		dynamo = NewDynamoHandler(deps.Tables).
			WithLogger(logger).
			WithRoleStore(MapRoleStore(deps.Roles))
	}
	var sqs http.Handler
	if deps.Queues != nil {
		// Same role-revalidation reasoning as dynamo above.
		sqs = NewSqsHandler(deps.Queues).
			WithLogger(logger).
			WithRoleStore(MapRoleStore(deps.Roles))
	}
	mux := buildAPIMux(auth, deps.Verifier, cluster, dynamo, sqs, logger)
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
//	POST   /admin/api/v1/auth/login                 (no auth, rate-limited)
//	POST   /admin/api/v1/auth/logout                (auth required)
//	GET    /admin/api/v1/cluster                    (auth required)
//	GET    /admin/api/v1/dynamo/tables              (auth required)
//	POST   /admin/api/v1/dynamo/tables              (auth required, full role)
//	GET    /admin/api/v1/dynamo/tables/{name}       (auth required)
//	DELETE /admin/api/v1/dynamo/tables/{name}       (auth required, full role)
//
// Body limit applies uniformly. CSRF and Audit middleware apply to
// write-capable protected endpoints; login and logout carry their own
// audit path inside AuthService because the generic Audit middleware
// cannot see the claimed actor at that point in the chain.
//
// dynamoHandler / sqsHandler may be nil; in that case the
// corresponding paths fall through to the unknown-endpoint 404,
// matching the behaviour of any other unregistered admin path.
func buildAPIMux(auth *AuthService, verifier *Verifier, clusterHandler, dynamoHandler, sqsHandler http.Handler, logger *slog.Logger) http.Handler {
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
	var sqsChain http.Handler
	if sqsHandler != nil {
		sqsChain = protect(sqsHandler)
	}

	routes := apiRoutes{
		login:   loginChain,
		logout:  logoutChain,
		cluster: clusterChain,
		dynamo:  dynamoChain,
		sqs:     sqsChain,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routes.dispatch(w, r)
	})
}

// apiRoutes is the per-startup chain bundle the request router walks
// in priority order. Split out of buildAPIMux so the request-time
// dispatch loop stays under the cyclop budget — every additional
// optional handler (Tables, Queues, …) would otherwise add a branch
// to the same closure and push it past the limit.
type apiRoutes struct {
	login   http.Handler
	logout  http.Handler
	cluster http.Handler
	dynamo  http.Handler
	sqs     http.Handler
}

func (r apiRoutes) dispatch(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/admin/api/v1/auth/login":
		r.login.ServeHTTP(w, req)
		return
	case "/admin/api/v1/auth/logout":
		r.logout.ServeHTTP(w, req)
		return
	case "/admin/api/v1/cluster":
		r.cluster.ServeHTTP(w, req)
		return
	}
	if r.dynamo != nil && hasResourcePrefix(req.URL.Path, pathDynamoTables, pathPrefixDynamoTables) {
		r.dynamo.ServeHTTP(w, req)
		return
	}
	if r.sqs != nil && hasResourcePrefix(req.URL.Path, pathSqsQueues, pathPrefixSqsQueues) {
		r.sqs.ServeHTTP(w, req)
		return
	}
	writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
		"no admin API handler is registered for this path")
}

// hasResourcePrefix reports whether p is exactly the collection root
// or sits under the per-resource prefix. Pulled out so both the
// dynamo and sqs branches in apiRoutes.dispatch read the same way.
func hasResourcePrefix(p, root, prefix string) bool {
	return p == root || strings.HasPrefix(p, prefix)
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
