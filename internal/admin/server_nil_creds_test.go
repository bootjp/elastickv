package admin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestServer_RejectsTypedNilCredentialStore ensures that a caller that
// wraps a nil map in MapCredentialStore cannot start the admin
// listener. Without this guard, `== nil` on the CredentialStore
// interface would miss a typed-nil, and every login would silently
// 401 at runtime.
func TestServer_RejectsTypedNilCredentialStore(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)
	cluster := ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{NodeID: "n"}, nil
	})

	var nilMap MapCredentialStore // typed nil
	_, err := NewServer(ServerDeps{
		Signer:      signer,
		Verifier:    verifier,
		Credentials: nilMap,
		Roles:       map[string]Role{"AKIA": RoleFull},
		ClusterInfo: cluster,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Credentials")
}

func TestIsNilCredentialStore(t *testing.T) {
	var noMap MapCredentialStore
	cases := []struct {
		name  string
		store CredentialStore
		want  bool
	}{
		{"untyped nil", nil, true},
		{"typed nil map literal", MapCredentialStore(nil), true},
		{"zero-value MapCredentialStore variable", noMap, true},
		{"non-empty map", MapCredentialStore{"AKIA": "x"}, false},
		// An empty (but non-nil) map is not considered nil — it
		// is a valid "no credentials configured, reject all
		// logins" posture; the wiring layer (main_admin.go) is
		// responsible for refusing that shape when admin is
		// enabled.
		{"empty non-nil map", MapCredentialStore{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isNilCredentialStore(tc.store))
		})
	}
}
