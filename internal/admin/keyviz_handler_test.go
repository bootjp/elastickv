package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

// keyVizGet performs a GET against the test server with a request
// context (so the noctx linter is satisfied) and returns the parsed
// response body for the caller to inspect.
func keyVizGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// fakeKeyVizSource is a deterministic in-memory KeyVizSource so the
// handler tests don't need to drive a real *keyviz.MemSampler with
// goroutines and time.
type fakeKeyVizSource struct {
	cols []keyviz.MatrixColumn
}

func (f *fakeKeyVizSource) Snapshot(_, _ time.Time) []keyviz.MatrixColumn {
	out := make([]keyviz.MatrixColumn, len(f.cols))
	for i, c := range f.cols {
		rows := make([]keyviz.MatrixRow, len(c.Rows))
		for j, r := range c.Rows {
			rows[j] = r
			rows[j].Start = append([]byte(nil), r.Start...)
			rows[j].End = append([]byte(nil), r.End...)
			if len(r.MemberRoutes) > 0 {
				rows[j].MemberRoutes = append([]uint64(nil), r.MemberRoutes...)
			}
		}
		out[i] = keyviz.MatrixColumn{At: c.At, Rows: rows}
	}
	return out
}

func newKeyVizTestServer(t *testing.T, source KeyVizSource) *httptest.Server {
	t.Helper()
	h := NewKeyVizHandler(source).WithClock(func() time.Time {
		return time.Unix(1_700_000_000, 0).UTC()
	})
	return httptest.NewServer(h)
}

// TestKeyVizHandlerReturnsServiceUnavailableWhenNoSource pins the
// "keyviz disabled" signal so the SPA can render a clear feature-off
// state instead of an empty matrix indistinguishable from "no
// activity yet."
func TestKeyVizHandlerReturnsServiceUnavailableWhenNoSource(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, nil)
	defer srv.Close()

	resp := keyVizGet(t, srv.URL)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "keyviz_disabled", body["error"])
}

// TestKeyVizHandlerRejectsNonGet pins the method allow-list so a
// stray POST from a misbehaving client doesn't surface as 200 with
// an empty matrix.
func TestKeyVizHandlerRejectsNonGet(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{})
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

// TestKeyVizHandlerPivotsMatrix pins the JSON wire shape: row-major
// layout (one KeyVizRow per RouteID, values aligned to the parallel
// column_unix_ms slice), the requested series counter (default writes),
// and Start-order sort.
func TestKeyVizHandlerPivotsMatrix(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_700_000_000, 0)
	t1 := t0.Add(time.Minute)
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{
			At: t0,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte("a"), End: []byte("m"), Writes: 4, Reads: 1},
				{RouteID: 2, Start: []byte("m"), End: []byte("z"), Writes: 7, Reads: 0},
			},
		},
		{
			At: t1,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte("a"), End: []byte("m"), Writes: 9, Reads: 3},
			},
		},
	}})
	defer srv.Close()

	resp := keyVizGet(t, srv.URL)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var matrix KeyVizMatrix
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
	require.Equal(t, KeyVizSeries("writes"), matrix.Series, "default series must be writes")
	require.Equal(t, []int64{t0.UnixMilli(), t1.UnixMilli()}, matrix.ColumnUnixMs)
	require.Len(t, matrix.Rows, 2)

	r1, r2 := matrix.Rows[0], matrix.Rows[1]
	require.Equal(t, "route:1", r1.BucketID)
	require.Equal(t, "route:2", r2.BucketID)
	require.Equal(t, []uint64{4, 9}, r1.Values)
	// Route 2 absent in column 1 → zero by default.
	require.Equal(t, []uint64{7, 0}, r2.Values)
}

// TestKeyVizHandlerSeriesParam pins the ?series=... query parameter
// dispatching across all four enum values.
func TestKeyVizHandlerSeriesParam(t *testing.T) {
	t.Parallel()
	row := keyviz.MatrixRow{
		RouteID: 1, Start: []byte("a"), End: []byte("z"),
		Reads: 11, Writes: 22, ReadBytes: 333, WriteBytes: 4444,
	}
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{At: time.Unix(1_700_000_000, 0), Rows: []keyviz.MatrixRow{row}},
	}})
	defer srv.Close()

	for _, tc := range []struct {
		series string
		want   uint64
	}{
		{"reads", 11},
		{"writes", 22},
		{"read_bytes", 333},
		{"write_bytes", 4444},
	} {
		t.Run(tc.series, func(t *testing.T) {
			// Sequential: the parent's `defer srv.Close()` would fire
			// before parallel subtests get to dial.
			resp := keyVizGet(t, srv.URL+"?series="+tc.series)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var matrix KeyVizMatrix
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
			require.Equal(t, KeyVizSeries(tc.series), matrix.Series)
			require.Equal(t, []uint64{tc.want}, matrix.Rows[0].Values)
		})
	}
}

// TestKeyVizHandlerSeriesParamRejectsUnknown pins input validation:
// an unknown series surfaces as 400 invalid_query so the SPA gets a
// crisp error rather than silently degrading to the default.
func TestKeyVizHandlerSeriesParamRejectsUnknown(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{At: time.Unix(1_700_000_000, 0), Rows: []keyviz.MatrixRow{
			{RouteID: 1, Start: []byte("a"), End: []byte("z"), Writes: 1},
		}},
	}})
	defer srv.Close()

	resp := keyVizGet(t, srv.URL+"?series=bogus")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "invalid_query", body["error"])
}

// TestKeyVizHandlerHonorsRowsBudget pins the rows cap: the request
// truncates to top-N rows by activity (then sorts by Start), matching
// the gRPC handler's Phase-1 simplification.
func TestKeyVizHandlerHonorsRowsBudget(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte("a"), End: []byte("b"), Writes: 1},
				{RouteID: 2, Start: []byte("b"), End: []byte("c"), Writes: 100},
				{RouteID: 3, Start: []byte("c"), End: []byte("d"), Writes: 5},
				{RouteID: 4, Start: []byte("d"), End: []byte("e"), Writes: 50},
			},
		},
	}})
	defer srv.Close()

	resp := keyVizGet(t, srv.URL+"?rows=2")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var matrix KeyVizMatrix
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
	require.Len(t, matrix.Rows, 2)
	// Top-2 by Writes activity = routes 2 (100) and 4 (50).
	// Then sorted by Start: "b" before "d".
	require.Equal(t, "route:2", matrix.Rows[0].BucketID)
	require.Equal(t, "route:4", matrix.Rows[1].BucketID)
}

// TestKeyVizHandlerEncodesAggregateBucket pins the aggregate-row
// proto-equivalent layout: bucket_id prefixed "virtual:", aggregate
// flag, route_count from MemberRoutesTotal (not len(MemberRoutes)),
// and route_ids_truncated when the cap was exceeded.
func TestKeyVizHandlerEncodesAggregateBucket(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{
					RouteID:           ^uint64(0),
					Start:             []byte("c"),
					End:               []byte("d"),
					Aggregate:         true,
					MemberRoutes:      []uint64{2, 3},
					MemberRoutesTotal: 9,
					Writes:            100,
				},
			},
		},
	}})
	defer srv.Close()

	resp := keyVizGet(t, srv.URL)
	defer resp.Body.Close()

	var matrix KeyVizMatrix
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
	require.Len(t, matrix.Rows, 1)
	r := matrix.Rows[0]
	require.True(t, r.Aggregate)
	require.Equal(t, "virtual:18446744073709551615", r.BucketID)
	require.Equal(t, uint64(9), r.RouteCount)
	require.True(t, r.RouteIDsTruncated)
	require.Equal(t, []uint64{2, 3}, r.RouteIDs)
}

// TestKeyVizHandlerRowsBudgetTieBreakDeterministic pins Gemini round-1
// nit: when two rows tie on activity total, the rows-budget truncation
// must pick the same set every refresh. Tie-break is BucketID
// ascending so a re-poll on identical data yields identical rows.
func TestKeyVizHandlerRowsBudgetTieBreakDeterministic(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{RouteID: 3, Start: []byte("c"), End: []byte("d"), Writes: 10},
				{RouteID: 1, Start: []byte("a"), End: []byte("b"), Writes: 10},
				{RouteID: 2, Start: []byte("b"), End: []byte("c"), Writes: 10},
			},
		},
	}})
	defer srv.Close()

	for i := 0; i < 3; i++ {
		resp := keyVizGet(t, srv.URL+"?rows=2")
		var matrix KeyVizMatrix
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
		resp.Body.Close()
		require.Len(t, matrix.Rows, 2, "iteration %d", i)
		// BucketID tie-break: route:1, route:2 win over route:3.
		// After the budget cap they sort by Start, giving "a" then "b".
		require.Equal(t, "route:1", matrix.Rows[0].BucketID, "iteration %d", i)
		require.Equal(t, "route:2", matrix.Rows[1].BucketID, "iteration %d", i)
	}
}

// TestKeyVizHandlerOmittedRowsAppliesDefaultCap pins Codex round-3 P1
// on PR #660: when the SPA polls without ?rows=, the handler must
// still apply the keyVizRowBudgetCap default — leaving p.rows at
// zero would let applyKeyVizRowBudget fall through to "no cap" and
// return every tracked route in one payload.
func TestKeyVizHandlerOmittedRowsAppliesDefaultCap(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{At: time.Unix(1_700_000_000, 0), Rows: stagedRowsForBudgetTest()},
	}})
	defer srv.Close()

	for _, query := range []string{"", "?rows=0", "?rows=-1"} {
		resp := keyVizGet(t, srv.URL+query)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var matrix KeyVizMatrix
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
		require.NoError(t, resp.Body.Close())
		require.Len(t, matrix.Rows, keyVizRowBudgetCap,
			"omitted/0/negative rows must apply the default cap (query=%q)", query)
	}
}

// TestKeyVizHandlerClampsRowsBudgetToCap pins the above-cap branch of
// setKeyVizRowsParam: an explicit rows= value greater than
// keyVizRowBudgetCap must be silently clamped down so callers cannot
// bypass the resource guard by asking for more rows than the cap.
func TestKeyVizHandlerClampsRowsBudgetToCap(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{At: time.Unix(1_700_000_000, 0), Rows: stagedRowsForBudgetTest()},
	}})
	defer srv.Close()

	resp := keyVizGet(t, srv.URL+"?rows=9999")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var matrix KeyVizMatrix
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
	require.NoError(t, resp.Body.Close())
	require.Len(t, matrix.Rows, keyVizRowBudgetCap,
		"rows=9999 must clamp down to keyVizRowBudgetCap")
}

// stagedRowsForBudgetTest builds keyVizRowBudgetCap+5 distinct rows so
// any test that exercises the budget cap can confirm truncation
// occurred. The loop counter is uint64 to avoid an int→uint64
// conversion that would need a gosec suppression; Start / End encode
// the index as a 2-byte big-endian key.
func stagedRowsForBudgetTest() []keyviz.MatrixRow {
	const total uint64 = keyVizRowBudgetCap + 5
	rows := make([]keyviz.MatrixRow, total)
	for i := uint64(0); i < total; i++ {
		n := i + 1
		rows[i] = keyviz.MatrixRow{
			RouteID: n,
			Start:   []byte{byte(i >> 8), byte(i)},
			End:     []byte{byte(n >> 8), byte(n)},
			Writes:  n,
		}
	}
	return rows
}

// TestKeyVizHandlerTimeBoundsParam exercises the from_unix_ms /
// to_unix_ms query parameters: a non-zero pair filters columns to the
// requested half-open window, while 0 means "unbounded on that side"
// (NOT the Unix epoch). The fakeKeyVizSource here does not actually
// honour the bounds (its Snapshot ignores them) — what we're pinning
// is the parse/dispatch contract: a parsable pair must yield 200,
// 0 must reach the source as the zero Time, and a non-numeric value
// must surface as 400 invalid_query.
func TestKeyVizHandlerTimeBoundsParam(t *testing.T) {
	t.Parallel()
	captured := &capturingKeyVizSource{}
	h := NewKeyVizHandler(captured).WithClock(func() time.Time {
		return time.Unix(1_700_000_000, 0).UTC()
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	from := time.Unix(1_699_000_000, 0)
	to := time.Unix(1_700_500_000, 0)
	u := srv.URL + "?from_unix_ms=" + strconv.FormatInt(from.UnixMilli(), 10) +
		"&to_unix_ms=" + strconv.FormatInt(to.UnixMilli(), 10)
	resp := keyVizGet(t, u)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, captured.from.Equal(from.UTC()), "from = %v, want %v", captured.from, from.UTC())
	require.True(t, captured.to.Equal(to.UTC()), "to = %v, want %v", captured.to, to.UTC())

	// 0 → unbounded (zero Time), not Unix epoch.
	captured.reset()
	resp = keyVizGet(t, srv.URL+"?from_unix_ms=0&to_unix_ms=0")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, captured.from.IsZero(), "from = %v, want zero", captured.from)
	require.True(t, captured.to.IsZero(), "to = %v, want zero", captured.to)

	// Non-numeric → 400 invalid_query.
	resp = keyVizGet(t, srv.URL+"?from_unix_ms=notanumber")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "invalid_query", body["error"])
}

// capturingKeyVizSource records the from/to bounds the handler
// forwarded so tests can assert on the parse step without rendering
// any matrix data.
type capturingKeyVizSource struct {
	from time.Time
	to   time.Time
}

func (c *capturingKeyVizSource) Snapshot(from, to time.Time) []keyviz.MatrixColumn {
	c.from = from
	c.to = to
	return nil
}

func (c *capturingKeyVizSource) reset() {
	c.from = time.Time{}
	c.to = time.Time{}
}

// TestKeyVizHandlerAggregateFallbackWhenTotalZero pins the defensive
// fallback for the unlikely case where the sampler emits an aggregate
// row with MemberRoutesTotal == 0: the handler must not serialise
// route_count = 0 (which the SPA would render as "0 routes" — nonsense
// for a virtual bucket). Instead it falls back to len(MemberRoutes).
func TestKeyVizHandlerAggregateFallbackWhenTotalZero(t *testing.T) {
	t.Parallel()
	srv := newKeyVizTestServer(t, &fakeKeyVizSource{cols: []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{
					RouteID:           ^uint64(0),
					Start:             []byte("c"),
					End:               []byte("d"),
					Aggregate:         true,
					MemberRoutes:      []uint64{2, 3},
					MemberRoutesTotal: 0, // pathological zero
					Writes:            5,
				},
			},
		},
	}})
	defer srv.Close()

	resp := keyVizGet(t, srv.URL)
	defer resp.Body.Close()
	var matrix KeyVizMatrix
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&matrix))
	require.Len(t, matrix.Rows, 1)
	require.True(t, matrix.Rows[0].Aggregate)
	require.Equal(t, uint64(2), matrix.Rows[0].RouteCount, "fallback to len(MemberRoutes)")
}
