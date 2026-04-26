package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubQueuesSource is the in-memory test double for SqsHandler.
// Mirrors stubTablesSource in dynamo_handler_test.go: records the
// principal that flowed through to the source so tests can assert
// live-role forwarding (Codex P2 + Claude P1 on PR #670).
type stubQueuesSource struct {
	queues              []string
	describeErr         error
	deleteErr           error
	lastDeleteName      string
	lastDeletePrincipal AuthPrincipal
}

func (s *stubQueuesSource) AdminListQueues(_ context.Context) ([]string, error) {
	return s.queues, nil
}

func (s *stubQueuesSource) AdminDescribeQueue(_ context.Context, name string) (*QueueSummary, bool, error) {
	if s.describeErr != nil {
		return nil, false, s.describeErr
	}
	for _, q := range s.queues {
		if q == name {
			return &QueueSummary{Name: name}, true, nil
		}
	}
	return nil, false, nil
}

func (s *stubQueuesSource) AdminDeleteQueue(_ context.Context, principal AuthPrincipal, name string) error {
	s.lastDeleteName = name
	s.lastDeletePrincipal = principal
	if s.deleteErr != nil {
		return s.deleteErr
	}
	for i, q := range s.queues {
		if q == name {
			s.queues = append(s.queues[:i], s.queues[i+1:]...)
			return nil
		}
	}
	return ErrQueuesNotFound
}

// TestSqsHandler_DeleteQueue_LivePromotion pins the live-role
// forwarding fix for PR #670: a JWT minted while the access key was
// read_only must, after the operator promotes the key to full in
// the live RoleStore, be allowed to delete *and* the principal
// arriving at AdminDeleteQueue must carry the live role (full),
// not the JWT's stale role.
//
// The bug before this fix: principalCanWrite returned true based on
// the live role, but the unmodified principal (with JWT read_only)
// was passed to AdminDeleteQueue, which independently checked
// principal.Role.canWrite() and returned ErrAdminForbidden. The
// user saw 403 and had to log out + back in for the new role to
// take effect.
func TestSqsHandler_DeleteQueue_LivePromotion(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	roles := MapRoleStore{"AKIA_PROMOTED": RoleFull}
	h := NewSqsHandler(src).WithRoleStore(roles)

	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders", nil)
	// JWT was minted while the key was still read_only. The live
	// role store has since been updated to full.
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_PROMOTED", Role: RoleReadOnly}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code,
		"promoted key must succeed at the handler layer; body=%s", rec.Body.String())
	require.Equal(t, "orders", src.lastDeleteName)
	require.Equal(t, RoleFull, src.lastDeletePrincipal.Role,
		"principal forwarded to AdminDeleteQueue must carry the live role (RoleFull), not the JWT's stale RoleReadOnly")
	require.Equal(t, "AKIA_PROMOTED", src.lastDeletePrincipal.AccessKey)
}

// TestSqsHandler_DeleteQueue_LiveRevocation is the symmetric case
// the live-role gate exists for in the first place: a JWT minted
// while full but the key was later removed from full_access_keys
// (or downgraded to read_only) — the request must be rejected at
// the handler, never reaching the source.
func TestSqsHandler_DeleteQueue_LiveRevocation(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	roles := MapRoleStore{"AKIA_DOWNGRADED": RoleReadOnly}
	h := NewSqsHandler(src).WithRoleStore(roles)

	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_DOWNGRADED", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastDeleteName, "source must not be reached when role is revoked")
}

// TestSqsHandler_DeleteQueue_KeyRemovedFromStore covers the third
// edge of the live-role gate: a JWT minted while authorised but the
// access key was *removed entirely* from the role config (operator
// rotated credentials). The request must 403 — same shape as the
// downgrade case — and never reach the source.
func TestSqsHandler_DeleteQueue_KeyRemovedFromStore(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	roles := MapRoleStore{} // key not present at all
	h := NewSqsHandler(src).WithRoleStore(roles)

	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_GONE", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastDeleteName)
}

// TestSqsHandler_DeleteQueue_NoRoleStore covers the single-tenant
// default: when no RoleStore is wired, the handler trusts the JWT's
// embedded role. A JWT-full request succeeds; a JWT-read_only
// request is rejected at the handler.
func TestSqsHandler_DeleteQueue_NoRoleStore(t *testing.T) {
	t.Run("jwt full succeeds", func(t *testing.T) {
		src := &stubQueuesSource{queues: []string{"orders"}}
		h := NewSqsHandler(src) // no WithRoleStore
		req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, RoleFull, src.lastDeletePrincipal.Role)
	})
	t.Run("jwt read-only is forbidden", func(t *testing.T) {
		src := &stubQueuesSource{queues: []string{"orders"}}
		h := NewSqsHandler(src)
		req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code)
		require.Empty(t, src.lastDeleteName)
	})
}

// TestSqsHandler_ListQueues_EmptyArrayNotNull pins the nil→[]
// normalisation added in the Gemini PR #670 medium fix. Without it
// the SPA crashes on `queues.length` access against null.
func TestSqsHandler_ListQueues_EmptyArrayNotNull(t *testing.T) {
	src := &stubQueuesSource{queues: nil}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathSqsQueues, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"queues":[]`,
		"empty catalog must serialise as [] not null; body=%s", rec.Body.String())
}

// helper to silence the unused-import warning when errors is only
// referenced inside one of the test functions.
var _ = errors.New
