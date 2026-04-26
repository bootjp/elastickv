package admin

// RoleStore is the live access-key → Role lookup the admin handlers
// re-evaluate on every state-changing request. Embedding the role
// in the JWT alone is insufficient: a token minted under role
// `full` would otherwise keep mutating tables for the rest of its
// 1-hour TTL even if an operator revoked or downgraded the access
// key in the cluster's role configuration. Codex P1 on PR #635
// flagged the gap; the leader-side ForwardServer already does this
// re-evaluation, the HTTP path now does it too so leader-direct
// writes match the forwarded path's authorisation contract.
type RoleStore interface {
	// LookupRole returns the role for an access key as understood
	// by the local node's view of cluster configuration. The bool
	// is false when the access key is not in the admin role index
	// — a session whose key has been removed must not be able to
	// perform any admin write, even if its JWT is still within
	// its issued validity window.
	LookupRole(accessKey string) (Role, bool)
}

// MapRoleStore is the trivial in-memory implementation, sufficient
// for tests and for the production wiring (which already keeps the
// role map in memory).
type MapRoleStore map[string]Role

// LookupRole implements RoleStore.
func (m MapRoleStore) LookupRole(accessKey string) (Role, bool) {
	r, ok := m[accessKey]
	return r, ok
}
