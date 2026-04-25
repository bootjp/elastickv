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
	tables  map[string]*DynamoTableSummary
	listErr error
	descErr error
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

func TestDynamoHandler_OnlyGET(t *testing.T) {
	h := newDynamoHandlerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}})
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(m, pathDynamoTables, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "method %s", m)
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
