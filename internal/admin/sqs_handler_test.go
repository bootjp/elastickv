package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stubQueuesSource is the in-memory test double for SqsHandler.
// Mirrors stubTablesSource in dynamo_handler_test.go: records the
// principal that flowed through to the source so tests can assert
// live-role forwarding.
type stubQueuesSource struct {
	queues                []string
	describeErr           error
	deleteErr             error
	setAttrsErr           error
	peekErr               error
	peekResult            PeekResult
	purgeErr              error
	purgeResult           PurgeResult
	lastDeleteName        string
	lastDeletePrincipal   AuthPrincipal
	lastSetAttrsName      string
	lastSetAttrsPrincipal AuthPrincipal
	lastSetAttrs          map[string]string
	lastPeekName          string
	lastPeekPrincipal     AuthPrincipal
	lastPeekOpts          PeekMessageOptions
	lastPurgeName         string
	lastPurgePrincipal    AuthPrincipal
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

func (s *stubQueuesSource) AdminSetQueueAttributes(_ context.Context, principal AuthPrincipal, name string, attrs map[string]string) error {
	s.lastSetAttrsName = name
	s.lastSetAttrsPrincipal = principal
	s.lastSetAttrs = attrs
	if s.setAttrsErr != nil {
		return s.setAttrsErr
	}
	return nil
}

func (s *stubQueuesSource) AdminPeekQueue(_ context.Context, principal AuthPrincipal, name string, opts PeekMessageOptions) (PeekResult, error) {
	s.lastPeekName = name
	s.lastPeekPrincipal = principal
	s.lastPeekOpts = opts
	if s.peekErr != nil {
		return PeekResult{}, s.peekErr
	}
	return s.peekResult, nil
}

func (s *stubQueuesSource) AdminPurgeQueue(_ context.Context, principal AuthPrincipal, name string) (PurgeResult, error) {
	s.lastPurgeName = name
	s.lastPurgePrincipal = principal
	if s.purgeErr != nil {
		return PurgeResult{}, s.purgeErr
	}
	return s.purgeResult, nil
}

// TestSqsHandler_DeleteQueue_LivePromotion pins the live-role
// forwarding contract: a JWT minted while the access key was
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

func TestSqsHandler_SetQueueAttributes_HappyPath(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	body := `{"attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-dlq\",\"maxReceiveCount\":5}"}}`
	req := httptest.NewRequest(http.MethodPut, pathPrefixSqsQueues+"orders/attributes", strings.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "orders", src.lastSetAttrsName)
	require.Equal(t, RoleFull, src.lastSetAttrsPrincipal.Role)
	require.Equal(t, `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:orders-dlq","maxReceiveCount":5}`, src.lastSetAttrs["RedrivePolicy"])
}

func TestSqsHandler_SetQueueAttributes_ReadOnlyForbidden(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodPut, pathPrefixSqsQueues+"orders/attributes", strings.NewReader(`{"attributes":{"RedriveAllowPolicy":"{}"}}`))
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastSetAttrsName, "read-only principal must not reach the set-attributes source")
}

// TestSqsHandler_ListQueues_EmptyArrayNotNull pins the nil→[]
// normalisation in the list handler. Without it the empty-catalog
// case serialises as `{"queues": null}` and the SPA crashes on
// `queues.length` against null.
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

// TestSqsHandler_DescribeQueue_ZeroCreatedAtIsOmittedOnTheWire pins
// the wire-level contract that a queue with no wall-clock creation
// timestamp does not surface a Go-zero time.Time on the wire.
// time.Time with `omitempty` is NOT dropped by encoding/json or
// goccy/go-json when zero — it serialises as "0001-01-01T00:00:00Z"
// and the SPA renders an ancient date instead of "—". The fix
// switched QueueSummary.CreatedAt to *time.Time and the bridge
// converts a zero time.Time to nil. This test exercises the wire
// representation, not the Go-side IsZero() check the adapter unit
// test already pins.
func TestSqsHandler_DescribeQueue_ZeroCreatedAtIsOmittedOnTheWire(t *testing.T) {
	// AdminDescribeQueue stub returns a QueueSummary with no CreatedAt
	// set (nil pointer, the post-bridge representation of an unknown
	// CreatedAtMillis).
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.NotContains(t, body, "0001-01-01T00:00:00Z",
		"a queue with no wall-clock timestamp must not surface the Go zero time on the wire; body=%s", body)
	require.NotContains(t, body, `"created_at":`,
		"created_at must be omitted entirely when the queue has no wall-clock timestamp so the SPA renders the placeholder; body=%s", body)
}

// TestSqsHandler_PeekMessages_HappyPath confirms a GET on
// /queues/{name}/messages dispatches to AdminPeekQueue, forwards
// the parsed limit / cursor / body_max_bytes query params, and
// renders the response as snake_case JSON with messages and
// next_cursor.
func TestSqsHandler_PeekMessages_HappyPath(t *testing.T) {
	src := &stubQueuesSource{
		queues: []string{"orders"},
		peekResult: PeekResult{
			Messages: []PeekedMessage{
				{MessageID: "m1", Body: "hello", BodyOriginalSize: 5, SentTimestamp: time.UnixMilli(1700000000000).UTC()},
			},
			NextCursor: "cursor-page-2",
		},
	}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/messages?limit=5&cursor=abc&body_max_bytes=1024", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "orders", src.lastPeekName)
	require.Equal(t, PeekMessageOptions{Limit: 5, Cursor: "abc", BodyMaxBytes: 1024}, src.lastPeekOpts)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Contains(t, body, "messages")
	require.Equal(t, "cursor-page-2", body["next_cursor"])
}

// TestSqsHandler_PeekMessages_EmptyArrayNotNull pins the nil to
// empty-slice normalisation: a queue with no visible messages must
// render as `{"messages":[]}` not `{"messages":null}` so the SPA's
// `messages.length` iteration does not crash.
func TestSqsHandler_PeekMessages_EmptyArrayNotNull(t *testing.T) {
	src := &stubQueuesSource{
		queues:     []string{"orders"},
		peekResult: PeekResult{Messages: nil, NextCursor: ""},
	}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"messages":[]`)
}

// TestSqsHandler_PeekMessages_NoRoleForbidden confirms a role-less
// principal (the zero AuthPrincipal) is rejected with 403 even
// without a RoleStore wired — peek's payload is sensitive enough
// that the lower-bound gate fires.
func TestSqsHandler_PeekMessages_NoRoleForbidden(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_NOROLE", Role: ""}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastPeekName, "source must not be reached for role-less principal")
}

// TestSqsHandler_PeekMessages_BadLimitQueryParam pins the query
// parsing: a non-numeric limit returns a 400 without dispatching.
func TestSqsHandler_PeekMessages_BadLimitQueryParam(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/messages?limit=NaN", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Empty(t, src.lastPeekName)
}

// TestSqsHandler_PurgeQueue_HappyPath confirms a DELETE on
// /queues/{name}/messages dispatches to AdminPurgeQueue (write role
// required) and returns 204 on success.
func TestSqsHandler_PurgeQueue_HappyPath(t *testing.T) {
	src := &stubQueuesSource{
		queues:      []string{"orders"},
		purgeResult: PurgeResult{GenerationBefore: 1, GenerationAfter: 2},
	}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "orders", src.lastPurgeName)
	require.Equal(t, RoleFull, src.lastPurgePrincipal.Role)
}

// TestSqsHandler_PurgeQueue_RateLimited429 pins the 60-second
// cooldown response shape: 429 status, Retry-After header rounded
// up to whole seconds, and retry_after_seconds JSON field. errors.As
// on the *PurgeInProgressError chain feeds the duration in.
func TestSqsHandler_PurgeQueue_RateLimited429(t *testing.T) {
	src := &stubQueuesSource{
		queues:   []string{"orders"},
		purgeErr: &PurgeInProgressError{RetryAfter: 42*time.Second + 500*time.Millisecond},
	}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	// 42.5s rounds up to 43.
	require.Equal(t, "43", rec.Header().Get("Retry-After"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	// The "error" key (not "code") matches writeJSONError's envelope
	// so apiFetch.ts can extract the AWS-style sentinel consistently
	// with every other 4xx error.
	require.Equal(t, "PurgeQueueInProgress", body["error"])
	require.EqualValues(t, 43, body["retry_after_seconds"])
}

// TestSqsHandler_PurgeQueue_ReadOnlyForbidden pins the write gate:
// a read-only principal cannot purge. The 403 body carries the
// purge-specific action verb ("purge messages"), not the
// delete-specific one — Claude r1 caught the misleading wording
// when principalForWrite was shared between handleDelete and
// handlePurge.
func TestSqsHandler_PurgeQueue_ReadOnlyForbidden(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastPurgeName, "read-only principal must not reach the purge source")
	require.Contains(t, rec.Body.String(), "purge messages",
		"purge rejection body must use the purge verb (not 'delete queues'); got=%s", rec.Body.String())
	require.NotContains(t, rec.Body.String(), "delete queues",
		"purge rejection body must not say 'delete queues' (Claude r1 fix); got=%s", rec.Body.String())
}

// TestSqsHandler_Routing_PathValidation_BadInputs is the table-driven
// pin for the 6-step routing's confused-deputy classes (design doc
// §3.4): every input must be rejected with 400 invalid_path before
// dispatch.
func TestSqsHandler_Routing_PathValidation_BadInputs(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"empty queue name segment", pathSqsQueues + "//messages"},
		{"percent-encoded slash", pathSqsQueues + "/%2F/messages"},
		{"double-encoded slash", pathSqsQueues + "/%252F/messages"},
		{"dot-segment escape", pathSqsQueues + "/orders/../messages"},
		{"leading dot", pathSqsQueues + "/./orders/messages"},
		{"empty interior segment", pathSqsQueues + "/orders//messages"},
		{"percent-encoded dot-dot", pathSqsQueues + "/orders/%2E%2E/messages"},
		{"percent-encoded dot", pathSqsQueues + "/orders/%2e/messages"},
	}
	src := &stubQueuesSource{queues: []string{"orders", "messages"}}
	h := NewSqsHandler(src)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
				AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code,
				"%s: want 400 got %d body=%s", tc.name, rec.Code, rec.Body.String())
		})
	}
}

// TestSqsHandler_Routing_TrailingSlashAccepted confirms path.Clean
// normalises /queues/orders/messages/ (legal trailing slash) into
// /queues/orders/messages — it dispatches to peek (GET) and purge
// (DELETE) just like the un-suffixed form.
func TestSqsHandler_Routing_TrailingSlashAccepted(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}, peekResult: PeekResult{}}
	h := NewSqsHandler(src)

	t.Run("GET with trailing slash dispatches peek", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/messages/", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
		require.Equal(t, "orders", src.lastPeekName)
	})

	t.Run("DELETE with trailing slash dispatches purge", func(t *testing.T) {
		src.lastPurgeName = ""
		req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages/", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "orders", src.lastPurgeName)
	})
}

// TestSqsHandler_Routing_QueueNamedMessages confirms a queue
// literally named "messages" still routes to describe / delete on
// the /queues/messages path (no /messages sub-resource segment).
// Distinguishes "queue named messages" from "missing queue name".
func TestSqsHandler_Routing_QueueNamedMessages(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"messages"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"messages", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), `"name":"messages"`)
}

// TestSqsHandler_Routing_UnknownSubResource pins the 404 fallback
// for sub-resources we don't recognise (e.g. /queues/orders/foo
// instead of /queues/orders/messages).
func TestSqsHandler_Routing_UnknownSubResource(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/notathing", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// TestSqsHandler_Routing_CollectionRoot pins the step-2 pre-split
// dispatch: bare /queues and /queues/ both route to handleList,
// not through the queue-name validator that would reject them as
// "empty queue name segment".
func TestSqsHandler_Routing_CollectionRoot(t *testing.T) {
	src := &stubQueuesSource{queues: []string{"orders"}}
	h := NewSqsHandler(src)
	for _, path := range []string{pathSqsQueues, pathPrefixSqsQueues} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
			require.Contains(t, rec.Body.String(), `"queues":["orders"]`)
		})
	}
}

// TestPurgeInProgressError_IsAndAs pins the typed-error contract:
// errors.Is matches ErrQueuesPurgeInProgress and errors.As pulls the
// typed RetryAfter duration. Without these the handler's branching
// in writeQueuesError silently falls through to 500.
func TestPurgeInProgressError_IsAndAs(t *testing.T) {
	err := &PurgeInProgressError{RetryAfter: 30 * time.Second}
	require.True(t, errors.Is(err, ErrQueuesPurgeInProgress))
	var typed *PurgeInProgressError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, 30*time.Second, typed.RetryAfter)
}

// TestRole_AllowsRead pins the new gate.
func TestRole_AllowsRead(t *testing.T) {
	require.True(t, RoleFull.AllowsRead())
	require.True(t, RoleReadOnly.AllowsRead())
	require.False(t, Role("").AllowsRead())
	require.False(t, Role("bogus").AllowsRead())
}
