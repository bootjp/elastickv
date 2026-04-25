package admin

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// stubTablesSource is the in-memory test double the dynamo handler
// tests use. AdminListTables returns names in lex order, matching
// the adapter's contract.
type stubTablesSource struct {
	tables    map[string]*DynamoTableSummary
	listErr   error
	descErr   error
	createErr error
	deleteErr error

	// Last-call tracking: tests assert the principal that reached
	// the source so we can prove SessionAuth wired through
	// correctly without parsing slog audit lines.
	lastCreatePrincipal AuthPrincipal
	lastCreateInput     CreateTableRequest
	lastDeletePrincipal AuthPrincipal
	lastDeleteName      string
}

func (s *stubTablesSource) AdminListTables(_ context.Context) ([]string, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]string, 0, len(s.tables))
	for k := range s.tables {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func (s *stubTablesSource) AdminDescribeTable(_ context.Context, name string) (*DynamoTableSummary, bool, error) {
	if s.descErr != nil {
		return nil, false, s.descErr
	}
	t, ok := s.tables[name]
	if !ok {
		return nil, false, nil
	}
	return t, true, nil
}

func (s *stubTablesSource) AdminCreateTable(_ context.Context, principal AuthPrincipal, in CreateTableRequest) (*DynamoTableSummary, error) {
	s.lastCreatePrincipal = principal
	s.lastCreateInput = in
	if s.createErr != nil {
		return nil, s.createErr
	}
	if _, exists := s.tables[in.TableName]; exists {
		return nil, ErrTablesAlreadyExists
	}
	summary := &DynamoTableSummary{
		Name:         in.TableName,
		PartitionKey: in.PartitionKey.Name,
		Generation:   1,
	}
	if in.SortKey != nil {
		summary.SortKey = in.SortKey.Name
	}
	if s.tables == nil {
		s.tables = map[string]*DynamoTableSummary{}
	}
	s.tables[in.TableName] = summary
	return summary, nil
}

func (s *stubTablesSource) AdminDeleteTable(_ context.Context, principal AuthPrincipal, name string) error {
	s.lastDeletePrincipal = principal
	s.lastDeleteName = name
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if _, exists := s.tables[name]; !exists {
		return ErrTablesNotFound
	}
	delete(s.tables, name)
	return nil
}

func newDynamoHandlerForTest(src TablesSource) *DynamoHandler {
	return NewDynamoHandler(src)
}

func TestDynamoHandler_ListTables_EmptyArrayNotNull(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"tables":[]`)
	require.NotContains(t, rec.Body.String(), `"next_token"`)
}

func TestDynamoHandler_ListTables_DefaultLimitAppliesAt100(t *testing.T) {
	tables := make(map[string]*DynamoTableSummary, 250)
	for i := 0; i < 250; i++ {
		name := tableNameForIndex(i)
		tables[name] = &DynamoTableSummary{Name: name}
	}
	h := newDynamoHandlerForTest(&stubTablesSource{tables: tables})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp dynamoListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tables, 100)
	require.NotEmpty(t, resp.NextToken)

	// next_token must round-trip through base64url back to the last
	// page entry — opaque to the client but stable enough that the
	// SPA's "next page" call resumes deterministically.
	decoded, err := base64.RawURLEncoding.DecodeString(resp.NextToken)
	require.NoError(t, err)
	require.Equal(t, resp.Tables[len(resp.Tables)-1], string(decoded))
}

func TestDynamoHandler_ListTables_LimitClampedToMax(t *testing.T) {
	tables := make(map[string]*DynamoTableSummary, 1500)
	for i := 0; i < 1500; i++ {
		name := tableNameForIndex(i)
		tables[name] = &DynamoTableSummary{Name: name}
	}
	h := newDynamoHandlerForTest(&stubTablesSource{tables: tables})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?limit=99999", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp dynamoListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tables, dynamoListLimitMax)
}

func TestDynamoHandler_ListTables_PaginationResumesAfterCursor(t *testing.T) {
	tables := map[string]*DynamoTableSummary{
		"alpha":   {Name: "alpha"},
		"bravo":   {Name: "bravo"},
		"charlie": {Name: "charlie"},
		"delta":   {Name: "delta"},
		"echo":    {Name: "echo"},
	}
	h := newDynamoHandlerForTest(&stubTablesSource{tables: tables})

	// First page: limit=2 → ["alpha", "bravo"], next_token=base64("bravo").
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?limit=2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var page1 dynamoListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.Equal(t, []string{"alpha", "bravo"}, page1.Tables)
	require.NotEmpty(t, page1.NextToken)

	// Second page: forward the opaque token verbatim — contract
	// must not require the client to URL-escape it again.
	req2 := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?limit=2&next_token="+page1.NextToken, nil)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	var page2 dynamoListResponse
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Equal(t, []string{"charlie", "delta"}, page2.Tables)
	require.NotEmpty(t, page2.NextToken)

	// Third page exhausts the list and must omit next_token.
	req3 := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?limit=2&next_token="+page2.NextToken, nil)
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	var page3 dynamoListResponse
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &page3))
	require.Equal(t, []string{"echo"}, page3.Tables)
	require.Empty(t, page3.NextToken)
}

func TestDynamoHandler_ListTables_NextTokenForVanishedNameFastForwards(t *testing.T) {
	// A cursor for a name that was deleted between pages must resume
	// at the next surviving entry, not silently swallow the page.
	tables := map[string]*DynamoTableSummary{
		"alpha": {Name: "alpha"},
		"delta": {Name: "delta"},
	}
	h := newDynamoHandlerForTest(&stubTablesSource{tables: tables})

	cursor := base64.RawURLEncoding.EncodeToString([]byte("bravo"))
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?limit=10&next_token="+cursor, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var resp dynamoListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, []string{"delta"}, resp.Tables)
	require.Empty(t, resp.NextToken)
}

func TestDynamoHandler_ListTables_RejectsBadLimit(t *testing.T) {
	cases := []struct {
		raw    string
		expect string
	}{
		{"abc", "invalid_limit"},
		{"-5", "invalid_limit"},
		{"0", "invalid_limit"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
			req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?limit="+tc.raw, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), tc.expect)
		})
	}
}

func TestDynamoHandler_ListTables_RejectsBadNextToken(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"?next_token=!!!not-base64!!!", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "invalid_next_token")
}

func TestDynamoHandler_ListTables_SourceErrorIsHidden(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{listErr: errors.New("kv backing sentinel ZZZ-471")})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "dynamo_list_failed")
	require.NotContains(t, rec.Body.String(), "ZZZ-471")
	require.NotContains(t, rec.Body.String(), "kv backing sentinel")
}

func TestDynamoHandler_DescribeTable_HappyPath(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"orders": {
			Name:         "orders",
			PartitionKey: "customer",
			SortKey:      "orderID",
			Generation:   42,
			GlobalSecondaryIndexes: []DynamoGSISummary{
				{Name: "by-status", PartitionKey: "status", SortKey: "createdAt", ProjectionType: "ALL"},
			},
		},
	}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/orders", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got DynamoTableSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "orders", got.Name)
	require.Equal(t, "customer", got.PartitionKey)
	require.Equal(t, "orderID", got.SortKey)
	require.EqualValues(t, 42, got.Generation)
	require.Len(t, got.GlobalSecondaryIndexes, 1)
	require.Equal(t, "by-status", got.GlobalSecondaryIndexes[0].Name)
	require.Equal(t, "ALL", got.GlobalSecondaryIndexes[0].ProjectionType)
}

func TestDynamoHandler_DescribeTable_MissingReturns404(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/absent", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "not_found")
}

func TestDynamoHandler_DescribeTable_RejectsSlashInName(t *testing.T) {
	// /admin/api/v1/dynamo/tables/foo/bar must not let the handler
	// call AdminDescribeTable with a "/"-bearing name. Returning 404
	// is preferable to 400 here because the URL itself is the only
	// way to express the table name; an embedded "/" simply does not
	// route to a real table.
	src := &stubTablesSource{
		descErr: errors.New("must not be invoked with slash-bearing name"),
	}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/foo/bar", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDynamoHandler_DescribeTable_SourceErrorIsHidden(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{descErr: errors.New("storage sentinel QQ-808")})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/orders", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "dynamo_describe_failed")
	require.NotContains(t, rec.Body.String(), "QQ-808")
}

func TestDynamoHandler_RejectsUnsupportedMethods(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	// /tables accepts GET + POST; PUT/PATCH/DELETE on the
	// collection root are 405. Wrapping the principal lets the
	// handler reach the method-dispatch arm rather than 401-ing
	// on missing principal first.
	for _, m := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(m, pathDynamoTables, nil)
		req = withWritePrincipal(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "collection method %s", m)
	}
	// /tables/{name} accepts GET + DELETE; POST/PUT/PATCH are 405.
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
		req := httptest.NewRequest(m, pathDynamoTables+"/x", nil)
		req = withWritePrincipal(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "item method %s", m)
	}
}

func TestDynamoHandler_UnknownSubpathReturns404(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	// /admin/api/v1/dynamo/something-else falls outside the prefix the
	// handler owns; the handler must answer 404 so the admin router's
	// "API took it" prefix routing stays correct.
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/dynamo/things", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDynamoHandler_DescribeTable_TrailingSlashIsRejected(t *testing.T) {
	// /admin/api/v1/dynamo/tables/ with an empty trailing component
	// would otherwise pass an empty name down to the source.
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathDynamoTables+"/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// withWritePrincipal injects a full-access principal into the
// request context so handler tests bypass the SessionAuth middleware
// while keeping the role check live. Mirrors how SessionAuth wires
// the value in production.
func withWritePrincipal(req *http.Request) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
}

func withReadOnlyPrincipal(req *http.Request) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
}

// validCreateBody returns a minimal-but-valid POST body the
// happy-path tests share.
func validCreateBody() string {
	return `{"table_name":"users","partition_key":{"name":"id","type":"S"}}`
}

// TestDynamoHandler_CreateTable_TrimsWhitespaceFromTableName covers
// the Claude-review finding on PR #634: a name like "  users  "
// must be trimmed before reaching the source so that subsequent
// describe/delete URL segments (which are matched literally)
// resolve the same table.
func TestDynamoHandler_CreateTable_TrimsWhitespaceFromTableName(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	body := `{"table_name":"  users  ","partition_key":{"name":"id","type":"S"}}`
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(body))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "users", src.lastCreateInput.TableName,
		"name reaching the source must be the trimmed canonical form")
}

// TestDynamoHandler_CreateTable_WhitespaceOnlyNameRejected ensures
// that a name consisting solely of whitespace still fails after
// the trim — i.e., trimming does not weaken the empty-name guard.
func TestDynamoHandler_CreateTable_WhitespaceOnlyNameRejected(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	body := `{"table_name":"   ","partition_key":{"name":"id","type":"S"}}`
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(body))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "table_name is required")
}

// TestDynamoHandler_CreateTable_LiveRoleRevocation covers Codex P1
// on PR #635: a session JWT with role=full from before a config
// reload must NOT keep mutating after the access key is revoked
// or downgraded. The handler re-validates against the live
// RoleStore on every write.
func TestDynamoHandler_CreateTable_LiveRoleRevocation(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	// Live role map says the access key is read-only — even though
	// the JWT in withWritePrincipal carries role=full.
	roles := MapRoleStore{"AKIA_FULL": RoleReadOnly}
	h := NewDynamoHandler(src).WithRoleStore(roles)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code,
		"a downgraded access key must be rejected even with a still-valid full-role JWT")
	require.Empty(t, src.lastCreateInput.TableName,
		"source must not be touched on revocation")
}

// TestDynamoHandler_CreateTable_LiveRoleAccessKeyRemoved covers
// the harder case: the access key was deleted entirely from the
// role index. Same 403, same defence-in-depth.
func TestDynamoHandler_CreateTable_LiveRoleAccessKeyRemoved(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	roles := MapRoleStore{} // AKIA_FULL is absent
	h := NewDynamoHandler(src).WithRoleStore(roles)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastCreateInput.TableName)
}

// TestDynamoHandler_DeleteTable_LiveRoleRevocation mirrors the
// create-side coverage on the delete path.
func TestDynamoHandler_DeleteTable_LiveRoleRevocation(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"users": {Name: "users"},
	}}
	roles := MapRoleStore{"AKIA_FULL": RoleReadOnly}
	h := NewDynamoHandler(src).WithRoleStore(roles)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/users", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastDeleteName)
}

func TestDynamoHandler_CreateTable_HappyPath(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}, src.lastCreatePrincipal)
	require.Equal(t, "users", src.lastCreateInput.TableName)
	var got DynamoTableSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "users", got.Name)
	require.Equal(t, "id", got.PartitionKey)
}

// TestDynamoHandler_CreateTable_CanonicalisesProjectionType makes
// sure validateGSI normalises a lowercase "include" value back to
// the uppercase form the adapter expects. Without normalisation
// the request would pass handler validation only to fail at the
// adapter as ValidationException ("invalid projection") — exactly
// the boundary mismatch Codex P2 flagged on PR #635.
func TestDynamoHandler_CreateTable_CanonicalisesProjectionType(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	body := `{
		"table_name":"t",
		"partition_key":{"name":"id","type":"S"},
		"gsi":[{
			"name":"by-status",
			"partition_key":{"name":"status","type":"S"},
			"projection":{"type":"include","non_key_attributes":["author"]}
		}]
	}`
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(body))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Len(t, src.lastCreateInput.GSI, 1)
	require.Equal(t, "INCLUDE", src.lastCreateInput.GSI[0].Projection.Type,
		"projection type must reach the source canonicalised so the adapter does not re-reject it")
}

// TestDynamoHandler_CreateTable_AcceptsMixedCaseProjection covers
// the same canonicalisation for KEYS_ONLY / Keys_Only / keys_only.
func TestDynamoHandler_CreateTable_AcceptsMixedCaseProjection(t *testing.T) {
	cases := []struct{ in, want string }{
		{"all", "ALL"},
		{"All", "ALL"},
		{"KEYS_ONLY", "KEYS_ONLY"},
		{"keys_only", "KEYS_ONLY"},
		{"Include", "INCLUDE"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
			h := newDynamoHandlerForTest(src)
			body := `{
				"table_name":"t",
				"partition_key":{"name":"id","type":"S"},
				"gsi":[{
					"name":"i",
					"partition_key":{"name":"k","type":"S"},
					"projection":{"type":"` + tc.in + `"}
				}]
			}`
			req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(body))
			req = withWritePrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			require.Equal(t, tc.want, src.lastCreateInput.GSI[0].Projection.Type)
		})
	}
}

func TestDynamoHandler_CreateTable_RejectsReadOnlyPrincipal(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	// The source must not be touched at all when the role check
	// fires — leaking a read-only call into the source layer would
	// be a defence-in-depth regression.
	require.Empty(t, src.lastCreateInput.TableName)
}

func TestDynamoHandler_CreateTable_RejectsMissingPrincipal(t *testing.T) {
	// Without a principal in context (SessionAuth would normally
	// reject earlier, but defence-in-depth here matters), the
	// handler must answer 401 rather than crashing on a zero
	// AuthPrincipal.
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestDynamoHandler_CreateTable_OversizedBodyReturns413 confirms a
// body that trips BodyLimit's MaxBytesReader surfaces as 413
// payload_too_large rather than the generic 400 invalid_body
// (Codex P2 on PR #634). The middleware contract in
// internal/admin/middleware.go is that oversized bodies map to
// 413; the previous wholesale "decode failure → 400" path
// silently broke that for this endpoint.
func TestDynamoHandler_CreateTable_OversizedBodyReturns413(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	// Build a body just over the limit. Padding a real-shape
	// JSON object with whitespace keeps the structure valid up
	// to the cap so the test isolates the size-rejection path.
	oversize := `{"table_name":"u","partition_key":{"name":"id","type":"S"}` +
		strings.Repeat(" ", int(defaultBodyLimit)+1) + "}"
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(oversize))
	req = withWritePrincipal(req)
	// The router applies BodyLimit at the outer wrap; emulate
	// that here so MaxBytesReader is in play during ReadAll.
	req.Body = http.MaxBytesReader(httptest.NewRecorder(), req.Body, defaultBodyLimit)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Contains(t, rec.Body.String(), "payload_too_large")
	require.Empty(t, src.lastCreateInput.TableName, "source must not be touched on oversized body")
}

func TestDynamoHandler_CreateTable_RejectsBadJSON(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	cases := []string{
		``,
		`{`,
		`{"table_name":""}`,
		`{"table_name":"u"}`, // missing partition_key
		`{"table_name":"u","partition_key":{"name":"id","type":"X"}}`,                                   // bad type
		`{"table_name":"u","partition_key":{"name":"id","type":"S"},"sort_key":{"name":"","type":"S"}}`, // bad sort key
		`{"table_name":"u","partition_key":{"name":"id","type":"S"},"unknown_field":1}`,                 // strict decode
		`{"table_name":"u","partition_key":{"name":"id","type":"S"}}{"second":"object"}`,                // trailing JSON
		`{"table_name":"u","partition_key":{"name":"id","type":"S"}} 42`,                                // trailing scalar
		`{"table_name":"foo/bar","partition_key":{"name":"id","type":"S"}}`,                             // slash in name
		`{"table_name":"a/b/c","partition_key":{"name":"id","type":"S"}}`,                               // multiple slashes
		// NUL-delimited smuggling vector (Codex P2). goccy/go-json
		// treats raw NUL as end-of-input so dec.More() would
		// otherwise return false; the explicit NUL scan catches it.
		"{\"table_name\":\"u\",\"partition_key\":{\"name\":\"id\",\"type\":\"S\"}}\x00{\"extra\":1}",
		// Lone NUL inside the JSON object — same vector, less obvious.
		"{\"table_name\":\"u\",\"partition_key\":{\"name\":\"id\",\"type\":\"S\"}}\x00",
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(body))
			req = withWritePrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%q", body)
			require.Contains(t, rec.Body.String(), "invalid_body")
		})
	}
}

func TestDynamoHandler_CreateTable_AlreadyExistsReturns409(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"users": {Name: "users", PartitionKey: "id"},
	}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "already_exists")
}

func TestDynamoHandler_CreateTable_NotLeaderReturns503WithRetryAfter(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{},
		createErr: ErrTablesNotLeader,
	}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "leader_unavailable")
}

func TestDynamoHandler_CreateTable_ForbiddenFromSourceMaps403(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{},
		createErr: ErrTablesForbidden,
	}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestDynamoHandler_CreateTable_ValidationErrorMaps400(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{},
		createErr: &ValidationError{Message: "conflicting attribute type for id"},
	}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "conflicting attribute type for id")
}

func TestDynamoHandler_CreateTable_GenericErrorIsHidden(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{},
		createErr: errors.New("storage backing sentinel ZQ-993"),
	}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathDynamoTables, strings.NewReader(validCreateBody()))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.NotContains(t, rec.Body.String(), "ZQ-993")
	require.NotContains(t, rec.Body.String(), "storage backing sentinel")
}

func TestDynamoHandler_DeleteTable_HappyPath(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"users": {Name: "users", PartitionKey: "id"},
	}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/users", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Empty(t, rec.Body.Bytes())
	require.Equal(t, "users", src.lastDeleteName)
	require.Equal(t, AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}, src.lastDeletePrincipal)
}

func TestDynamoHandler_DeleteTable_ReadOnlyPrincipalRejected(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"users": {Name: "users", PartitionKey: "id"},
	}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/users", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastDeleteName, "source must not be reached when role check fails")
}

func TestDynamoHandler_DeleteTable_MissingReturns404(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/absent", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "not_found")
}

func TestDynamoHandler_DeleteTable_NotLeaderReturns503WithRetryAfter(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{"users": {Name: "users"}},
		deleteErr: ErrTablesNotLeader,
	}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/users", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestDynamoHandler_DeleteTable_RejectsTrailingSlash(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	h := newDynamoHandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathDynamoTables+"/", nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// tableNameForIndex generates lex-sortable monotonically increasing
// names so list-pagination tests can assert deterministic ordering.
func tableNameForIndex(i int) string {
	const width = 4
	digits := []byte("0000")
	for k := width - 1; k >= 0 && i > 0; k-- {
		digits[k] = byte('0' + i%10)
		i /= 10
	}
	return "tbl-" + string(digits)
}

// Sanity check on the helper itself so the pagination assertions
// have a stable backing dataset.
func TestTableNameForIndex_LexSortable(t *testing.T) {
	prev := ""
	for i := 0; i < 20; i++ {
		cur := tableNameForIndex(i)
		if prev != "" {
			require.True(t, strings.Compare(prev, cur) < 0, "non-monotonic at %d: %s !< %s", i, prev, cur)
		}
		prev = cur
	}
}
