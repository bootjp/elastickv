package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubLeaderForwarder is the in-memory test double the integration
// tests use to simulate the follower→leader forwarding path. It
// records the last principal/payload it saw and returns whatever
// canned response/error the test prepared.
type stubLeaderForwarder struct {
	createRes *ForwardResult
	createErr error
	deleteRes *ForwardResult
	deleteErr error

	lastCreatePrincipal AuthPrincipal
	lastCreateInput     CreateTableRequest
	lastDeletePrincipal AuthPrincipal
	lastDeleteName      string
}

func (s *stubLeaderForwarder) ForwardCreateTable(_ context.Context, principal AuthPrincipal, in CreateTableRequest) (*ForwardResult, error) {
	s.lastCreatePrincipal = principal
	s.lastCreateInput = in
	if s.createErr != nil {
		return nil, s.createErr
	}
	return s.createRes, nil
}

func (s *stubLeaderForwarder) ForwardDeleteTable(_ context.Context, principal AuthPrincipal, name string) (*ForwardResult, error) {
	s.lastDeletePrincipal = principal
	s.lastDeleteName = name
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	return s.deleteRes, nil
}

// notLeaderSource is a TablesSource that always returns
// ErrTablesNotLeader on writes — i.e., it simulates the local
// node being a follower. Used to exercise the forwarder path
// without spinning up a real Raft cluster.
type notLeaderSource struct {
	stubTablesSource
}

func (s *notLeaderSource) AdminCreateTable(_ context.Context, _ AuthPrincipal, _ CreateTableRequest) (*DynamoTableSummary, error) {
	return nil, ErrTablesNotLeader
}

func (s *notLeaderSource) AdminDeleteTable(_ context.Context, _ AuthPrincipal, _ string) error {
	return ErrTablesNotLeader
}

func newFollowerHandler(t *testing.T, fwd LeaderForwarder) *DynamoHandler {
	t.Helper()
	src := &notLeaderSource{stubTablesSource: stubTablesSource{tables: map[string]*DynamoTableSummary{}}}
	h := NewDynamoHandler(src)
	if fwd != nil {
		h = h.WithLeaderForwarder(fwd)
	}
	return h
}

func TestDynamoHandler_FollowerForwardsCreateTable_HappyPath(t *testing.T) {
	// Acceptance criterion 2: a write hitting a follower is
	// transparently forwarded; the SPA sees the same 201 + body
	// shape it would have seen from a leader-direct call.
	fwd := &stubLeaderForwarder{createRes: &ForwardResult{
		StatusCode:  http.StatusCreated,
		Payload:     []byte(`{"name":"users","partition_key":"id","generation":1}`),
		ContentType: "application/json; charset=utf-8",
	}}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `{"name":"users","partition_key":"id","generation":1}`, rec.Body.String())
	require.Equal(t, RoleFull, fwd.lastCreatePrincipal.Role)
	require.Equal(t, "users", fwd.lastCreateInput.TableName)
}

func TestDynamoHandler_FollowerForwardsDeleteTable_HappyPath(t *testing.T) {
	fwd := &stubLeaderForwarder{deleteRes: &ForwardResult{
		StatusCode:  http.StatusNoContent,
		ContentType: "application/json; charset=utf-8",
	}}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/users", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Empty(t, rec.Body.Bytes())
	require.Equal(t, "users", fwd.lastDeleteName)
	require.Equal(t, RoleFull, fwd.lastDeletePrincipal.Role)
}

func TestDynamoHandler_NoForwarder_FallsBackTo503(t *testing.T) {
	// Without a forwarder configured, a follower handler still
	// produces the original 503 leader_unavailable + Retry-After
	// — preserves the leader-only deployment story from PR #634.
	h := newFollowerHandler(t, nil)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "leader_unavailable")
}

func TestDynamoHandler_ForwarderLeaderUnavailableReturns503(t *testing.T) {
	// Acceptance criterion 3: when the forwarder cannot find a
	// leader (election in progress), the handler must surface
	// 503 + Retry-After: 1 so the client retries instead of
	// failing hard. ErrLeaderUnavailable specifically must NOT
	// be logged as an error — that path is expected during
	// elections and would otherwise spam the audit log.
	fwd := &stubLeaderForwarder{createErr: ErrLeaderUnavailable}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "leader_unavailable")
}

func TestDynamoHandler_ForwarderTransportErrorReturns503(t *testing.T) {
	// Generic gRPC error from the forwarder also produces 503 +
	// Retry-After. The error is logged on the server (covered by
	// inspection of slog output) but never surfaces to the SPA.
	fwd := &stubLeaderForwarder{createErr: errors.New("gRPC transport sentinel TX-1")}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.NotContains(t, rec.Body.String(), "TX-1")
	require.NotContains(t, rec.Body.String(), "transport sentinel")
}

// TestDynamoHandler_ForwarderNotInvokedForNonNotLeaderError pins
// the gate in tryForwardCreate / tryForwardDelete: the forward
// path must run ONLY when the source returned ErrTablesNotLeader.
// Other source errors (already-exists, validation, generic) must
// fall through to writeTablesError and never reach the forwarder
// — otherwise a leader-direct rejection like 409 Conflict would
// be silently re-applied at the leader. Claude review on PR #644
// noted the test gap; locking it down protects against a future
// change accidentally removing the !errors.Is guard.
func TestDynamoHandler_ForwarderNotInvokedForNonNotLeaderError(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"already_exists", ErrTablesAlreadyExists, http.StatusConflict},
		{"validation", &ValidationError{Message: "bad input"}, http.StatusBadRequest},
		{"generic", errors.New("opaque storage failure"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd := &stubLeaderForwarder{}
			src := &stubTablesSource{createErr: tc.err}
			h := NewDynamoHandler(src).WithLeaderForwarder(fwd)
			req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
			req = withWritePrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tc.wantCode, rec.Code)
			require.Empty(t, fwd.lastCreateInput.TableName,
				"forwarder must not be invoked for source error: %s", tc.name)
		})
	}
}

// TestDynamoHandler_ForwarderForwardsLeaderResponseSetsNosniff
// confirms the security parity Claude flagged: writeForwardResult
// must emit X-Content-Type-Options: nosniff just like the leader-
// direct path's writeAdminJSONStatus, otherwise a SPA hitting a
// follower would silently lose MIME-sniff protection on
// forwarded responses.
func TestDynamoHandler_ForwarderForwardsLeaderResponseSetsNosniff(t *testing.T) {
	fwd := &stubLeaderForwarder{createRes: &ForwardResult{
		StatusCode:  http.StatusCreated,
		Payload:     []byte(`{"name":"users"}`),
		ContentType: "application/json; charset=utf-8",
	}}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
}

func TestDynamoHandler_ForwarderForwardsLeaderConflictResponse(t *testing.T) {
	// 409 Conflict from the leader (table already exists) must
	// be relayed verbatim to the SPA, not re-classified as 503.
	// The forwarder's transport succeeded; the leader's
	// structured response is the authoritative answer.
	fwd := &stubLeaderForwarder{createRes: &ForwardResult{
		StatusCode:  http.StatusConflict,
		Payload:     []byte(`{"error":"already_exists","message":"table already exists"}`),
		ContentType: "application/json; charset=utf-8",
	}}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "already_exists")
}

func TestDynamoHandler_ForwarderForwardsLeader503WithRetryAfter(t *testing.T) {
	// If the leader stepped down mid-request and returned 503
	// itself, the forwarder relays the body but the handler must
	// also set Retry-After: 1 so the SPA's retry policy works
	// uniformly regardless of where in the chain the 503 came
	// from.
	fwd := &stubLeaderForwarder{createRes: &ForwardResult{
		StatusCode:  http.StatusServiceUnavailable,
		Payload:     []byte(`{"error":"leader_unavailable"}`),
		ContentType: "application/json; charset=utf-8",
	}}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestDynamoHandler_ForwarderNotInvokedForReadOnlyPrincipal(t *testing.T) {
	// The role check fires before the forward path — a read-only
	// principal must NEVER reach the forwarder. Defence in depth:
	// even if a malicious follower could fabricate the principal,
	// the leader re-validates (covered by forward_server_test).
	fwd := &stubLeaderForwarder{}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, fwd.lastCreateInput.TableName, "forwarder must not be reached on role rejection")
}

func TestDynamoHandler_ForwarderNotInvokedForBadJSON(t *testing.T) {
	// Body validation runs before the source call — invalid JSON
	// must fail at the handler with 400 and never produce a
	// forward attempt.
	fwd := &stubLeaderForwarder{}
	h := newFollowerHandler(t, fwd)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader("{not json"))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Empty(t, fwd.lastCreateInput.TableName)
}
