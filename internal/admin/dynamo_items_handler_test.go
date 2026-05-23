package admin

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// stubItemsSource is a richer TablesSource fake that backs the
// item-level handler tests with an in-memory per-table item map.
// The table-level methods inherit zero-value behaviour from the
// embedded stubTablesSource (good enough — these tests don't
// exercise list/describe/create/delete on the table itself).
type stubItemsSource struct {
	stubTablesSource

	// items[table][primary-key-fingerprint] -> stored item
	items map[string]map[string]AdminItem

	// Injected error returned by the next Admin*Item call, then
	// cleared. Tests use this to drive the error-translation
	// branches without spinning up a real adapter.
	nextErr error

	// Last principal observed by a write call — lets tests assert
	// the live-role re-check landed.
	lastPutPrincipal    AuthPrincipal
	lastDeletePrincipal AuthPrincipal

	// Scan customisation for cursor tests.
	scanCursor map[string]AdminAttributeValue
}

func newStubItemsSource() *stubItemsSource {
	return &stubItemsSource{
		items: map[string]map[string]AdminItem{},
	}
}

// seedItem stores item under the fingerprint of `key`. The handler
// reaches AdminGetItem / AdminDeleteItem with a primary-key map
// (NOT the full attribute map), so the stub's index has to match
// that lookup shape — fingerprinting the whole Attributes map
// would yield a different key than the get/delete lookup uses.
//
// `table` is parameterised even though every current test passes
// "orders" — the stub is intentionally multi-table-aware so future
// tests covering cross-table isolation do not need a second helper.
//
//nolint:unparam // see note above; `table` is forward-looking.
func (s *stubItemsSource) seedItem(table string, key map[string]AdminAttributeValue, item AdminItem) {
	if s.items[table] == nil {
		s.items[table] = map[string]AdminItem{}
	}
	s.items[table][itemFingerprint(key)] = item
}

// itemFingerprint is a stable deterministic representation of an
// item's attribute map used as the in-memory map key. Marshals to
// the same JSON the wire shape uses so the test stub doesn't have
// to implement structural equality.
func itemFingerprint(attrs map[string]AdminAttributeValue) string {
	b, _ := json.Marshal(attrs)
	return string(b)
}

func (s *stubItemsSource) AdminScanItems(_ context.Context, _ AuthPrincipal, table string, opts AdminScanItemsOptions) (AdminScanItemsResult, error) {
	if s.nextErr != nil {
		err := s.nextErr
		s.nextErr = nil
		return AdminScanItemsResult{}, err
	}
	rows := make([]AdminItem, 0, len(s.items[table]))
	for _, it := range s.items[table] {
		rows = append(rows, it)
	}
	if opts.Limit > 0 && len(rows) > opts.Limit {
		rows = rows[:opts.Limit]
	}
	return AdminScanItemsResult{Items: rows, LastEvaluatedKey: s.scanCursor}, nil
}

func (s *stubItemsSource) AdminGetItem(_ context.Context, _ AuthPrincipal, table string, key map[string]AdminAttributeValue) (*AdminItem, bool, error) {
	if s.nextErr != nil {
		err := s.nextErr
		s.nextErr = nil
		return nil, false, err
	}
	// Stub does not know the table schema, so lookup is by
	// subset-match: for each stored item, every requested-key
	// attribute must appear with the same value in the item's
	// Attributes map. First match wins (in practice the test
	// inputs never have ambiguous matches).
	for _, it := range s.items[table] {
		if itemMatchesKey(it.Attributes, key) {
			return &it, true, nil
		}
	}
	return nil, false, nil
}

// itemMatchesKey reports whether every attribute in `key` is
// present in `attrs` with a structurally equal value. Mirrors
// the attributeValuesEqual logic from the handler — duplicated
// here so test fixtures don't depend on handler internals.
func itemMatchesKey(attrs, key map[string]AdminAttributeValue) bool {
	for k, want := range key {
		got, ok := attrs[k]
		if !ok || !attributeValuesEqual(got, want) {
			return false
		}
	}
	return true
}

func (s *stubItemsSource) AdminPutItem(_ context.Context, principal AuthPrincipal, table string, item AdminItem) error {
	s.lastPutPrincipal = principal
	if s.nextErr != nil {
		err := s.nextErr
		s.nextErr = nil
		return err
	}
	if s.items[table] == nil {
		s.items[table] = map[string]AdminItem{}
	}
	s.items[table][itemFingerprint(item.Attributes)] = item
	return nil
}

func (s *stubItemsSource) AdminDeleteItem(_ context.Context, principal AuthPrincipal, table string, key map[string]AdminAttributeValue) error {
	s.lastDeletePrincipal = principal
	if s.nextErr != nil {
		err := s.nextErr
		s.nextErr = nil
		return err
	}
	// Same subset-match strategy as AdminGetItem.
	for fp, it := range s.items[table] {
		if itemMatchesKey(it.Attributes, key) {
			delete(s.items[table], fp)
			return nil
		}
	}
	return nil
}

// encodeKey encodes a primary-key map as the URL-segment shape
// the handler expects: base64-url(JSON(map[string]AdminAttributeValue)).
// Test ergonomic: takes the key map directly so test bodies stay
// readable.
func encodeKey(t *testing.T, key map[string]AdminAttributeValue) string {
	t.Helper()
	raw, err := json.Marshal(key)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func stringAttr(s string) AdminAttributeValue {
	return AdminAttributeValue{S: &s}
}

// itemPath assembles the URL the per-key item handlers route on.
// `table` is parameterised for parity with seedItem; future
// cross-table tests will pass a non-"orders" value here without
// needing to grow a second helper.
//
//nolint:unparam // see comment above
func itemPath(table string, key map[string]AdminAttributeValue, t *testing.T) string {
	return pathDynamoTables + "/" + table + "/" + adminItemSubResource + "/" + encodeKey(t, key)
}

// ---------- routing tests ----------

func TestDynamoItems_RoutingDispatchesEachMethod(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	cases := []struct {
		name           string
		method         string
		body           string
		principalApply func(*http.Request) *http.Request
		wantCode       int
	}{
		{"scan", http.MethodGet, "", withReadOnlyPrincipal, http.StatusOK},
		{"get-missing", http.MethodGet, "", withReadOnlyPrincipal, http.StatusNotFound},
		{"put", http.MethodPut, `{"attributes":{"pk":{"S":"k1"}}}`, withWritePrincipal, http.StatusNoContent},
		{"delete", http.MethodDelete, "", withWritePrincipal, http.StatusNoContent},
	}
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var url string
			if tc.name == "scan" {
				url = pathDynamoTables + "/orders/" + adminItemSubResource
			} else {
				url = itemPath("orders", key, t)
			}
			req := httptest.NewRequest(tc.method, url, strings.NewReader(tc.body))
			req = tc.principalApply(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equalf(t, tc.wantCode, rec.Code, "body=%s", rec.Body.String())
		})
	}
}

func TestDynamoItems_RoutingRejectsUnknownSubResource(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/orders/unknown", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "unknown sub-resource")
}

func TestDynamoItems_RoutingRejectsTooManySegments(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/orders/items/k1/extra", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "too many path segments")
}

func TestDynamoItems_RoutingRejectsDotSegments(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	cases := []string{
		pathDynamoTables + "/../etc/passwd",
		pathDynamoTables + "/orders/./items",
		pathDynamoTables + "/orders/items/..",
	}
	for _, url := range cases {
		t.Run(url, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, url, nil)
			req = withReadOnlyPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestDynamoItems_RoutingRejectsPercentInSegment(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	// %2f decodes to `/` which would split the segment. The validator
	// now decodes each segment first (Codex P1 on PR #813 — allow
	// %20 / UTF-8 in table names) but still rejects decoded `/`,
	// `.`, and `..` to keep the path-traversal classes closed.
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/orders%2fother/items", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDynamoItems_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	// /items only accepts GET (scan).
	req := httptest.NewRequest(http.MethodPost,
		pathDynamoTables+"/orders/items", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// /items/{key} accepts GET / PUT / DELETE only.
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}
	req = httptest.NewRequest(http.MethodPost, itemPath("orders", key, t), nil)
	req = withWritePrincipal(req)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// ---------- key decoding tests ----------

func TestDynamoItems_RejectsMalformedKeySegment(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	cases := map[string]string{
		"not-base64":         pathDynamoTables + "/orders/items/!!!notbase64",
		"valid-b64-not-json": pathDynamoTables + "/orders/items/" + base64.RawURLEncoding.EncodeToString([]byte("not-json")),
		"empty-map":          pathDynamoTables + "/orders/items/" + base64.RawURLEncoding.EncodeToString([]byte("{}")),
	}
	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, url, nil)
			req = withReadOnlyPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code,
				"want 400 for malformed key %q; body=%s", url, rec.Body.String())
		})
	}
}

// ---------- read path: scan / get ----------

func TestDynamoItems_Scan_ReturnsItemsAndCursor(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	keyA := map[string]AdminAttributeValue{"pk": stringAttr("a")}
	keyB := map[string]AdminAttributeValue{"pk": stringAttr("b")}
	src.seedItem("orders", keyA, AdminItem{Attributes: keyA})
	src.seedItem("orders", keyB, AdminItem{Attributes: keyB})
	src.scanCursor = map[string]AdminAttributeValue{"pk": stringAttr("a")}
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/orders/items", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var got AdminScanItemsResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Items, 2)
	require.NotEmpty(t, got.LastEvaluatedKey, "cursor must be carried through")
}

func TestDynamoItems_Scan_LimitClamped(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		key := map[string]AdminAttributeValue{"pk": stringAttr(k)}
		src.seedItem("orders", key, AdminItem{Attributes: key})
	}
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/orders/items?limit=2", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var got AdminScanItemsResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Items, 2, "explicit ?limit must be honoured")
}

func TestDynamoItems_Scan_RejectsBadLimit(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/orders/items?limit=-1", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDynamoItems_Get_HappyPath(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}
	val := AdminItem{Attributes: map[string]AdminAttributeValue{
		"pk":    stringAttr("k1"),
		"value": stringAttr("hello"),
	}}
	src.seedItem("orders", key, val)
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, itemPath("orders", key, t), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var got AdminItem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "hello", *got.Attributes["value"].S)
}

func TestDynamoItems_Get_Missing404(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("ghost")}

	req := httptest.NewRequest(http.MethodGet, itemPath("orders", key, t), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// ---------- write path: put / delete ----------

func TestDynamoItems_Put_RoundTrips(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	body := `{"attributes":{"pk":{"S":"k1"},"value":{"S":"v1"}}}`
	req := httptest.NewRequest(http.MethodPut, itemPath("orders", key, t),
		bytes.NewReader([]byte(body)))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "AKIA_FULL", src.lastPutPrincipal.AccessKey)

	// Verify via GET that the item landed.
	req = httptest.NewRequest(http.MethodGet, itemPath("orders", key, t), nil)
	req = withReadOnlyPrincipal(req)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestDynamoItems_Put_RejectsBodyKeyMismatch(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	pathKey := map[string]AdminAttributeValue{"pk": stringAttr("path-k")}

	// Body claims pk=body-k; path says pk=path-k. Mismatch → 400.
	body := `{"attributes":{"pk":{"S":"body-k"}}}`
	req := httptest.NewRequest(http.MethodPut, itemPath("orders", pathKey, t),
		bytes.NewReader([]byte(body)))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "does not match")
}

func TestDynamoItems_Put_RejectsMissingPathKeyInBody(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	pathKey := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	// Body omits pk entirely → 400.
	body := `{"attributes":{"value":{"S":"v"}}}`
	req := httptest.NewRequest(http.MethodPut, itemPath("orders", pathKey, t),
		bytes.NewReader([]byte(body)))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "missing the path key attribute")
}

func TestDynamoItems_Put_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	req := httptest.NewRequest(http.MethodPut, itemPath("orders", key, t),
		bytes.NewReader([]byte(`{not json`)))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDynamoItems_Delete_HappyPath(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}
	src.seedItem("orders", key, AdminItem{Attributes: key})
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodDelete, itemPath("orders", key, t), nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// GET now 404.
	req = httptest.NewRequest(http.MethodGet, itemPath("orders", key, t), nil)
	req = withReadOnlyPrincipal(req)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// ---------- authorisation tests ----------

func TestDynamoItems_Put_RejectsReadOnly(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	body := `{"attributes":{"pk":{"S":"k1"}}}`
	req := httptest.NewRequest(http.MethodPut, itemPath("orders", key, t),
		bytes.NewReader([]byte(body)))
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestDynamoItems_Delete_RejectsReadOnly(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	req := httptest.NewRequest(http.MethodDelete, itemPath("orders", key, t), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestDynamoItems_Scan_RejectsAnonymous(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	// No principal injected — should fail 401 unauthenticated,
	// matching principalForWrite and the rest of the admin
	// surface (Claude review on PR #813 r2 caught the original
	// 500 inconsistency).
	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/orders/items", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// ---------- error translation tests ----------

func TestDynamoItems_TranslatesSentinelsToStatus(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		injected error
		wantCode int
	}{
		"forbidden":       {ErrItemsForbidden, http.StatusForbidden},
		"not-leader":      {ErrItemsNotLeader, http.StatusServiceUnavailable},
		"table-not-found": {ErrItemsTableNotFound, http.StatusNotFound},
		"validation":      {ErrItemsValidation, http.StatusBadRequest},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			src := newStubItemsSource()
			src.nextErr = tc.injected
			h := newDynamoHandlerForTest(src)

			req := httptest.NewRequest(http.MethodGet,
				pathDynamoTables+"/orders/items", nil)
			req = withReadOnlyPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDynamoItems_Scan_JWTNoneRoleRejectedWhenLiveStoreGrantsRead
// pins the principalForItemRead JWT-bypass fix from PR #813 r5.
// Pre-fix, when a RoleStore was configured (h.roles != nil), the
// JWT role check was skipped entirely and only the live store
// was consulted — so a session whose JWT carried no read role
// could pass through reads if the live store had since granted
// the access key a read-or-higher role. The fix moves the JWT
// gate ahead of the live check; both must permit the request
// (defence-in-depth matching principalForWrite's contract).
//
// Pre-fix this assertion would have failed: live store grants
// RoleReadOnly so the role-store branch accepted the request
// and never re-checked the JWT's zero-value Role; the source's
// AdminScanItems was invoked and returned 200 OK. Post-fix the
// JWT check rejects first (Role="" doesn't satisfy AllowsRead)
// and the source is never touched. Mirrors
// TestDynamoHandler_CreateTable_LiveRoleRevocation in
// dynamo_handler_test.go:448 (Claude review on PR #813 r5
// flagged the missing test).
func TestDynamoItems_Scan_JWTNoneRoleRejectedWhenLiveStoreGrantsRead(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	// Make the source return a recognisable success path so the
	// pre-fix bug would surface as 200 (source reached) rather
	// than a sentinel-induced status.
	src.seedItem("orders", map[string]AdminAttributeValue{"pk": stringAttr("k1")},
		AdminItem{Attributes: map[string]AdminAttributeValue{"pk": stringAttr("k1")}})
	// Live role map says the access key has read access — but the
	// JWT carries Role="" (zero value, neither read nor write).
	roles := MapRoleStore{"AKIA_BYPASS": RoleReadOnly}
	h := NewDynamoHandler(src).WithRoleStore(roles)

	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/orders/items", nil)
	// Inject a principal with the matching access key but no role
	// on the JWT side. withReadOnlyPrincipal can't reproduce this —
	// we need the JWT-without-read-role case the bypass relied on.
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_BYPASS", Role: ""}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code,
		"JWT role with no read permission must be rejected even when the live store grants read access")
}

// TestDynamoItems_NotLeader503HasRetryAfterAndCanonicalCode pins
// the items 503 contract to the same shape every other admin
// surface uses: status 503, Retry-After: 1, error code
// "leader_unavailable". Claude review on PR #813 r2 caught the
// drift — translateAdminItemsError's comment promised
// "503 + Retry-After: 1" but writeItemsError didn't set the
// header and the items handler shipped a non-canonical
// "not_leader" code. The SPA's retry logic branches on the code
// string; the items 503 must be indistinguishable from the table-
// tier / S3 / SQS 503 paths.
func TestDynamoItems_NotLeader503HasRetryAfterAndCanonicalCode(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	src.nextErr = ErrItemsNotLeader
	h := newDynamoHandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/orders/items", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"),
		"503 from items handler must set Retry-After: 1 so SPA retry backoff fires")
	require.Contains(t, rec.Body.String(), `"leader_unavailable"`,
		`items 503 must use the canonical "leader_unavailable" code`)
}

// ---------- body-validation defence-in-depth (Gemini medium on PR #813) ----------

// TestDynamoItems_RoutingAcceptsPercentEncodedTableName pins the
// Codex P1 on PR #813 fix: a table name containing characters that
// must be URL-escaped (spaces, UTF-8, etc.) must still be routable
// via its percent-encoded form. validateCreateTableRequest only
// blocks empty and `/`, so a table named "foo bar" is creatable; if
// the dispatcher rejected the percent-encoded segment outright, the
// table would be unreachable through describe / delete / items.
func TestDynamoItems_RoutingAcceptsPercentEncodedTableName(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	src.tables = map[string]*DynamoTableSummary{
		"foo bar": {Name: "foo bar", PartitionKey: "pk"},
	}
	h := newDynamoHandlerForTest(src)

	// GET /tables/foo%20bar must decode to "foo bar" before
	// AdminDescribeTable is called.
	req := httptest.NewRequest(http.MethodGet,
		pathDynamoTables+"/foo%20bar", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
}

// TestDynamoItems_RoutingRejectsPercentEncodedSlash keeps the
// path-traversal defence: %2f decodes to `/`, which would split the
// segment after decode. The validator must reject the decoded `/`
// regardless of whether it arrived raw or percent-encoded (Codex P1
// on PR #813 narrowed the % rejection to decoded forms).
func TestDynamoItems_RoutingRejectsPercentEncodedSlash(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)

	cases := []string{
		pathDynamoTables + "/orders%2fother",      // %2f → decoded `/`
		pathDynamoTables + "/orders%2Fother",      // %2F → decoded `/` (upper)
		pathDynamoTables + "/%2e",                 // %2e → decoded `.`  (dot-segment)
		pathDynamoTables + "/%2E%2E",              // %2E%2E → decoded `..` (dot-segment)
		pathDynamoTables + "/orders/%2e%2e/items", // %2e%2e → `..` mid-path
	}
	for _, url := range cases {
		t.Run(url, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, url, nil)
			req = withReadOnlyPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestDynamoItems_AttributeValue_EmptyContainersRoundTrip locks down
// that an item carrying an empty L or M attribute serializes with
// the type tag still present (`{"L":[]}` / `{"M":{}}`) rather than
// disappearing under omitempty. Without this the SPA cannot
// distinguish a "list of zero elements" attribute from "no L field
// at all" (a different attribute kind entirely).
func TestDynamoItems_AttributeValue_EmptyContainersRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   AdminAttributeValue
		wantSub string
	}{
		{
			name:    "empty list keeps L tag",
			input:   AdminAttributeValue{L: []AdminAttributeValue{}},
			wantSub: `"L":[]`,
		},
		{
			name:    "empty map keeps M tag",
			input:   AdminAttributeValue{M: map[string]AdminAttributeValue{}},
			wantSub: `"M":{}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := json.Marshal(tc.input)
			require.NoError(t, err)
			require.Contains(t, string(out), tc.wantSub,
				"empty container attribute must round-trip through JSON")
		})
	}
}

// TestDynamoItems_Put_RejectsNULBytePayload guards against the
// payload-smuggling vector goccy/go-json exhibits with NUL bytes —
// a `{"attributes":...}\x00{"extra":1}` body would otherwise sneak
// the second object past the decoder. Same shape as the
// decodeCreateTableRequest test on /tables (Codex P2 on PR #634).
func TestDynamoItems_Put_RejectsNULBytePayload(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	body := []byte(`{"attributes":{"pk":{"S":"k1"}}}` + "\x00" + `{"extra":1}`)
	req := httptest.NewRequest(http.MethodPut, itemPath("orders", key, t),
		bytes.NewReader(body))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "NUL")
}

// TestDynamoItems_Put_RejectsTrailingJSONData mirrors
// decodeCreateTableRequest: a body like `{"attributes":...}{"a":1}`
// must surface 400 rather than silently accepting the first object
// and dropping the second.
func TestDynamoItems_Put_RejectsTrailingJSONData(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	body := []byte(`{"attributes":{"pk":{"S":"k1"}}}{"extra":1}`)
	req := httptest.NewRequest(http.MethodPut, itemPath("orders", key, t),
		bytes.NewReader(body))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "trailing data")
}

// TestDynamoItems_Put_RejectsEmptyBody pins the existing nil-body
// rejection contract — the io.ReadAll path returns 0 bytes which
// would otherwise decode to a zero-value AdminItem (Attributes:
// nil) and confuse the body/path-key reconciler.
func TestDynamoItems_Put_RejectsEmptyBody(t *testing.T) {
	t.Parallel()
	src := newStubItemsSource()
	h := newDynamoHandlerForTest(src)
	key := map[string]AdminAttributeValue{"pk": stringAttr("k1")}

	req := httptest.NewRequest(http.MethodPut, itemPath("orders", key, t),
		bytes.NewReader(nil))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
