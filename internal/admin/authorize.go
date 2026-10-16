package admin

import "net/http"

// roleAccess is the access an admin endpoint needs from the caller's role.
type roleAccess int

const (
	// accessRead is for endpoints that return stored content (item
	// scans, object bodies, queue peeks). Metadata endpoints stay on
	// the session-auth gate alone.
	accessRead roleAccess = iota
	// accessWrite is for state-changing endpoints.
	accessWrite
)

func (a roleAccess) allowedBy(role Role) bool {
	if a == accessWrite {
		return role.AllowsWrite()
	}
	return role.AllowsRead()
}

// authorizePrincipal is the one authorisation gate for admin endpoints
// that change state or return stored content:
//
//  1. No session principal on the context: 401. SessionAuth attaches
//     one before any handler runs, so this fires only if a routing
//     change bypasses the middleware chain.
//  2. The role frozen in the JWT must allow the access. A live
//     promotion never widens it; the key logs in again to pick the
//     promotion up (Codex P2 and CodeRabbit on PR #669).
//  3. When a RoleStore is configured, the access key's live role must
//     exist and allow the access, so a key revoked or downgraded since
//     login is refused before its JWT expires (Codex P1 on PR #635).
//     Revoked and downgraded keys get the same 403.
//  4. The principal is returned with the live role, which is the role
//     the adapters re-check.
//
// On rejection it writes the JSON error, using deny as the 403
// message, and returns ok=false. ForwardServer.validatePrincipal is
// the leader-side counterpart for forwarded requests, which carry no
// JWT.
func authorizePrincipal(w http.ResponseWriter, r *http.Request, roles RoleStore, access roleAccess, deny string) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no session principal")
		return AuthPrincipal{}, false
	}
	if !access.allowedBy(principal.Role) {
		writeJSONError(w, http.StatusForbidden, "forbidden", deny)
		return AuthPrincipal{}, false
	}
	if roles == nil {
		// No live role index (single-handler tests, single-tenant
		// wiring): the JWT role is authoritative. Production wiring
		// always sets one.
		return principal, true
	}
	live, exists := roles.LookupRole(principal.AccessKey)
	if !exists || !access.allowedBy(live) {
		writeJSONError(w, http.StatusForbidden, "forbidden", deny)
		return AuthPrincipal{}, false
	}
	principal.Role = live
	return principal, true
}
