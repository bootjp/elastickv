package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAuthorizePrincipal pins the admin authorisation policy every
// content-read and write endpoint shares: the JWT role must allow the
// access, the live role (when a RoleStore is configured) must exist
// and allow it too, and the principal flows downstream with the live
// role.
func TestAuthorizePrincipal(t *testing.T) {
	t.Parallel()

	const (
		noStore      = "no store"
		liveFull     = "live full"
		liveReadOnly = "live read_only"
		liveRoleless = "live role-less"
		keyMissing   = "key missing from store"
	)
	cases := []struct {
		jwt   Role
		store string
		read  int
		write int
	}{
		{jwt: RoleFull, store: noStore, read: http.StatusOK, write: http.StatusOK},
		{jwt: RoleFull, store: liveFull, read: http.StatusOK, write: http.StatusOK},
		{jwt: RoleFull, store: liveReadOnly, read: http.StatusOK, write: http.StatusForbidden},
		{jwt: RoleFull, store: liveRoleless, read: http.StatusForbidden, write: http.StatusForbidden},
		{jwt: RoleFull, store: keyMissing, read: http.StatusForbidden, write: http.StatusForbidden},

		{jwt: RoleReadOnly, store: noStore, read: http.StatusOK, write: http.StatusForbidden},
		{jwt: RoleReadOnly, store: liveFull, read: http.StatusOK, write: http.StatusForbidden},
		{jwt: RoleReadOnly, store: liveReadOnly, read: http.StatusOK, write: http.StatusForbidden},
		{jwt: RoleReadOnly, store: liveRoleless, read: http.StatusForbidden, write: http.StatusForbidden},
		{jwt: RoleReadOnly, store: keyMissing, read: http.StatusForbidden, write: http.StatusForbidden},

		{jwt: "", store: noStore, read: http.StatusForbidden, write: http.StatusForbidden},
		{jwt: "", store: liveFull, read: http.StatusForbidden, write: http.StatusForbidden},
		{jwt: "", store: liveReadOnly, read: http.StatusForbidden, write: http.StatusForbidden},
		{jwt: "", store: liveRoleless, read: http.StatusForbidden, write: http.StatusForbidden},
		{jwt: "", store: keyMissing, read: http.StatusForbidden, write: http.StatusForbidden},
	}
	for _, tc := range cases {
		var roles RoleStore
		liveRole := tc.jwt
		switch tc.store {
		case liveFull:
			liveRole = RoleFull
			roles = MapRoleStore{"AKIA": liveRole}
		case liveReadOnly:
			liveRole = RoleReadOnly
			roles = MapRoleStore{"AKIA": liveRole}
		case liveRoleless:
			liveRole = ""
			roles = MapRoleStore{"AKIA": liveRole}
		case keyMissing:
			roles = MapRoleStore{}
		}
		for access, want := range map[string]struct {
			access roleAccess
			status int
		}{
			"read":  {access: accessRead, status: tc.read},
			"write": {access: accessWrite, status: tc.write},
		} {
			t.Run("jwt="+string(tc.jwt)+"/"+tc.store+"/"+access, func(t *testing.T) {
				t.Parallel()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
					AuthPrincipal{AccessKey: "AKIA", Role: tc.jwt}))
				rec := httptest.NewRecorder()

				principal, ok := authorizePrincipal(rec, req, roles, want.access, "denied by test policy")

				if want.status == http.StatusOK {
					require.True(t, ok)
					require.Zero(t, rec.Body.Len(), "an allowed request must not get a response written")
					require.Equal(t, AuthPrincipal{AccessKey: "AKIA", Role: liveRole}, principal,
						"the principal must carry the live role downstream")
					return
				}
				require.False(t, ok)
				require.Equal(t, want.status, rec.Code)
				require.Contains(t, rec.Body.String(), "denied by test policy")
				require.Equal(t, AuthPrincipal{}, principal)
			})
		}
	}
}

func TestAuthorizePrincipal_MissingPrincipalIsUnauthenticated(t *testing.T) {
	t.Parallel()

	for _, access := range []roleAccess{accessRead, accessWrite} {
		rec := httptest.NewRecorder()
		principal, ok := authorizePrincipal(rec, httptest.NewRequest(http.MethodGet, "/", nil),
			MapRoleStore{"AKIA": RoleFull}, access, "denied by test policy")

		require.False(t, ok)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Equal(t, AuthPrincipal{}, principal)
	}
}
