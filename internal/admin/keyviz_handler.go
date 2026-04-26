package admin

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/goccy/go-json"
)

// KeyVizSource is the small contract the keyviz handler depends on.
// Production wires this to a real *keyviz.MemSampler; tests use a
// stub that returns canned columns.
//
// Snapshot returns the matrix columns within [from, to). Either bound
// may be the zero Time meaning unbounded on that side. Implementations
// MUST return rows the caller can mutate freely (a deep copy) — see
// keyviz.MemSampler.Snapshot.
type KeyVizSource interface {
	Snapshot(from, to time.Time) []keyviz.MatrixColumn
}

// KeyVizSeries selects which counter on a MatrixRow the response
// surfaces in `Values`. Wire form mirrors the proto enum but uses
// lowercase strings so the SPA can pass `?series=writes` directly
// without an extra encoding round-trip.
type KeyVizSeries string

const (
	keyVizSeriesReads      KeyVizSeries = "reads"
	keyVizSeriesWrites     KeyVizSeries = "writes"
	keyVizSeriesReadBytes  KeyVizSeries = "read_bytes"
	keyVizSeriesWriteBytes KeyVizSeries = "write_bytes"
)

// keyVizDefaultSeries matches the design doc §4.1 default. Writes is
// the primary signal the heatmap is built around; reads will land in
// a follow-up phase (read sampling not yet wired).
const keyVizDefaultSeries = keyVizSeriesWrites

// keyVizRowBudgetCap is the upper bound on the per-request row
// budget. Mirrors the same cap on the gRPC GetKeyVizMatrix RPC so
// the SPA cannot force unbounded payloads via the JSON path. Design
// doc §4.1.
const keyVizRowBudgetCap = 1024

// KeyVizMatrix is the row-major JSON wire form returned by
// /admin/api/v1/keyviz/matrix. Mirrors the proto GetKeyVizMatrixResponse
// shape so a future refactor can share a single pivot helper across
// the adapter (gRPC) and admin (JSON) paths.
type KeyVizMatrix struct {
	ColumnUnixMs []int64      `json:"column_unix_ms"`
	Rows         []KeyVizRow  `json:"rows"`
	Series       KeyVizSeries `json:"series"`
	GeneratedAt  time.Time    `json:"generated_at"`
}

// KeyVizRow is one route's worth of activity across the column window,
// matching the proto KeyVizRow layout. Values is parallel to
// KeyVizMatrix.ColumnUnixMs — Values[j] is the counter for that route
// at column j.
type KeyVizRow struct {
	BucketID          string   `json:"bucket_id"`
	Start             []byte   `json:"start"`
	End               []byte   `json:"end"`
	Aggregate         bool     `json:"aggregate"`
	RouteIDs          []uint64 `json:"route_ids,omitempty"`
	RouteIDsTruncated bool     `json:"route_ids_truncated,omitempty"`
	RouteCount        uint64   `json:"route_count"`
	Values            []uint64 `json:"values"`
}

// KeyVizHandler serves GET /admin/api/v1/keyviz/matrix.
//
// Query parameters (all optional):
//
//	series      - reads | writes | read_bytes | write_bytes (default: writes)
//	from_unix_ms - lower bound in unix ms (default: unbounded)
//	to_unix_ms   - upper bound in unix ms (default: unbounded)
//	rows         - row budget; 0 means no cap, capped at 1024 (default: 0)
//
// Returns 503 codes.Unavailable when no sampler is configured so the
// SPA can distinguish "keyviz disabled" from "no data yet" (the
// latter is a successful empty matrix).
type KeyVizHandler struct {
	source KeyVizSource
	now    func() time.Time
	logger *slog.Logger
}

// NewKeyVizHandler wires a KeyVizSource into the HTTP handler.
// source may be nil; calls to ServeHTTP will then return 503 with
// code "keyviz_disabled".
func NewKeyVizHandler(source KeyVizSource) *KeyVizHandler {
	return &KeyVizHandler{
		source: source,
		now:    func() time.Time { return time.Now().UTC() },
		logger: slog.Default(),
	}
}

// WithLogger overrides the slog destination so main.go can attach a
// component tag without changing the constructor signature.
func (h *KeyVizHandler) WithLogger(l *slog.Logger) *KeyVizHandler {
	if l == nil {
		return h
	}
	h.logger = l
	return h
}

// WithClock lets tests inject a deterministic GeneratedAt.
func (h *KeyVizHandler) WithClock(now func() time.Time) *KeyVizHandler {
	if now == nil {
		return h
	}
	h.now = now
	return h
}

func (h *KeyVizHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
		return
	}
	if h.source == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "keyviz_disabled",
			"key visualizer sampler is not configured on this node")
		return
	}
	params, err := parseKeyVizParams(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	cols := h.source.Snapshot(params.from, params.to)
	matrix := pivotKeyVizColumns(cols, params.series, params.rows)
	matrix.GeneratedAt = h.now()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(matrix); err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin keyviz response encode failed",
			slog.String("error", err.Error()),
		)
	}
}

// keyVizParams is the parsed query-string form of a matrix request.
type keyVizParams struct {
	series KeyVizSeries
	from   time.Time
	to     time.Time
	rows   int
}

func parseKeyVizParams(r *http.Request) (keyVizParams, error) {
	p := keyVizParams{series: keyVizDefaultSeries}
	q := r.URL.Query()
	if err := setKeyVizSeriesParam(&p, q.Get("series")); err != nil {
		return keyVizParams{}, err
	}
	if err := setKeyVizTimeParam(&p.from, "from_unix_ms", q.Get("from_unix_ms")); err != nil {
		return keyVizParams{}, err
	}
	if err := setKeyVizTimeParam(&p.to, "to_unix_ms", q.Get("to_unix_ms")); err != nil {
		return keyVizParams{}, err
	}
	if err := setKeyVizRowsParam(&p.rows, q.Get("rows")); err != nil {
		return keyVizParams{}, err
	}
	return p, nil
}

func setKeyVizSeriesParam(p *keyVizParams, raw string) error {
	if raw == "" {
		return nil
	}
	series, err := parseKeyVizSeries(raw)
	if err != nil {
		return err
	}
	p.series = series
	return nil
}

func setKeyVizTimeParam(dst *time.Time, name, raw string) error {
	if raw == "" {
		return nil
	}
	t, err := parseUnixMs(name, raw)
	if err != nil {
		return err
	}
	*dst = t
	return nil
}

func setKeyVizRowsParam(dst *int, raw string) error {
	if raw == "" {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return errors.New("rows must be a non-negative integer")
	}
	if n > keyVizRowBudgetCap {
		n = keyVizRowBudgetCap
	}
	*dst = n
	return nil
}

func parseKeyVizSeries(s string) (KeyVizSeries, error) {
	switch KeyVizSeries(s) {
	case keyVizSeriesReads, keyVizSeriesWrites, keyVizSeriesReadBytes, keyVizSeriesWriteBytes:
		return KeyVizSeries(s), nil
	default:
		return "", errors.New("series must be one of: reads, writes, read_bytes, write_bytes")
	}
}

func parseUnixMs(name, raw string) (time.Time, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, errors.New(name + " must be an integer (unix milliseconds)")
	}
	if n == 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(n).UTC(), nil
}

// pivotKeyVizColumns flips the column-major MatrixColumn slice into
// the row-major JSON shape, picks the requested series counter from
// each MatrixRow, and applies the rowBudget cap (top-N by total
// activity) before sorting back into Start order.
//
// Mirrors adapter.matrixToProto exactly — the duplication is
// intentional for now to keep the gRPC and JSON paths independent;
// extracting a shared helper into the keyviz package is a future
// cleanup.
func pivotKeyVizColumns(cols []keyviz.MatrixColumn, series KeyVizSeries, rowBudget int) KeyVizMatrix {
	pick := keyVizSeriesPicker(series)
	matrix := KeyVizMatrix{
		Series:       series,
		ColumnUnixMs: make([]int64, len(cols)),
	}
	rowsByID := make(map[uint64]*KeyVizRow)
	order := make([]uint64, 0)
	for j, col := range cols {
		matrix.ColumnUnixMs[j] = col.At.UnixMilli()
		for _, mr := range col.Rows {
			row, ok := rowsByID[mr.RouteID]
			if !ok {
				row = newKeyVizRowFrom(mr, len(cols))
				rowsByID[mr.RouteID] = row
				order = append(order, mr.RouteID)
			}
			row.Values[j] = pick(mr)
		}
	}
	matrix.Rows = make([]KeyVizRow, len(order))
	for i, id := range order {
		matrix.Rows[i] = *rowsByID[id]
	}
	matrix.Rows = applyKeyVizRowBudget(matrix.Rows, rowBudget)
	sortKeyVizRowsByStart(matrix.Rows)
	return matrix
}

func keyVizSeriesPicker(series KeyVizSeries) func(keyviz.MatrixRow) uint64 {
	switch series {
	case keyVizSeriesReads:
		return func(r keyviz.MatrixRow) uint64 { return r.Reads }
	case keyVizSeriesReadBytes:
		return func(r keyviz.MatrixRow) uint64 { return r.ReadBytes }
	case keyVizSeriesWriteBytes:
		return func(r keyviz.MatrixRow) uint64 { return r.WriteBytes }
	case keyVizSeriesWrites:
		return func(r keyviz.MatrixRow) uint64 { return r.Writes }
	default:
		return func(r keyviz.MatrixRow) uint64 { return r.Writes }
	}
}

func newKeyVizRowFrom(mr keyviz.MatrixRow, numCols int) *KeyVizRow {
	total := mr.MemberRoutesTotal
	if !mr.Aggregate && total == 0 {
		total = 1
	}
	row := &KeyVizRow{
		BucketID:          bucketIDFor(mr),
		Start:             append([]byte(nil), mr.Start...),
		End:               append([]byte(nil), mr.End...),
		Aggregate:         mr.Aggregate,
		RouteCount:        total,
		RouteIDsTruncated: mr.Aggregate && total > uint64(len(mr.MemberRoutes)),
		Values:            make([]uint64, numCols),
	}
	if mr.Aggregate {
		row.RouteIDs = append([]uint64(nil), mr.MemberRoutes...)
	}
	return row
}

func bucketIDFor(mr keyviz.MatrixRow) string {
	if mr.Aggregate {
		return "virtual:" + strconv.FormatUint(mr.RouteID, 10)
	}
	return "route:" + strconv.FormatUint(mr.RouteID, 10)
}

// applyKeyVizRowBudget mirrors the adapter Phase-1 simplification:
// activity-descending truncation rather than design §5.5's lexicographic
// merge. Future work should swap in the spec'd merge once the
// virtual-bucket plumbing supports synthesis at the response layer.
func applyKeyVizRowBudget(rows []KeyVizRow, budget int) []KeyVizRow {
	if budget <= 0 || len(rows) <= budget {
		return rows
	}
	sort.Slice(rows, func(i, j int) bool {
		return rowActivityTotal(rows[i]) > rowActivityTotal(rows[j])
	})
	return rows[:budget]
}

func rowActivityTotal(r KeyVizRow) uint64 {
	var sum uint64
	for _, v := range r.Values {
		sum += v
	}
	return sum
}

func sortKeyVizRowsByStart(rows []KeyVizRow) {
	sort.Slice(rows, func(i, j int) bool {
		if c := bytes.Compare(rows[i].Start, rows[j].Start); c != 0 {
			return c < 0
		}
		return rows[i].BucketID < rows[j].BucketID
	})
}
