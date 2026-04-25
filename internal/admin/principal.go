package admin

// Role represents the authorization tier of an authenticated admin session.
type Role string

const (
	// RoleReadOnly permits only GET endpoints.
	RoleReadOnly Role = "read_only"
	// RoleFull permits the entire admin CRUD surface.
	RoleFull Role = "full"
)

// AllowsWrite reports whether the role may execute state-mutating operations.
func (r Role) AllowsWrite() bool {
	return r == RoleFull
}

// AuthPrincipal is the authenticated caller derived from a session cookie or,
// in the future, a follower→leader forwarded request. The admin handler and
// adapter internal entrypoints pass it around instead of a raw HTTP request
// so the authorization model stays consistent whether the request arrived
// via SigV4 or JWT.
type AuthPrincipal struct {
	// AccessKey is the caller's SigV4 access key identifier.
	AccessKey string
	// Role is the role resolved from the server-side access key table.
	Role Role
}
