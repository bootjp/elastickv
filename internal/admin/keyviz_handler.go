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
//
// Fanout is non-nil when the handler is configured for cluster-wide
// fan-out (Phase 2-C): it carries per-node status so the SPA can
// surface degraded responses inline (see design 2026_04_27_proposed_keyviz_cluster_fanout.md).
// The field is omitted from the wire form when fan-out is disabled
// so old clients keep working unchanged.
type KeyVizMatrix struct {
	ColumnUnixMs []int64       `json:"column_unix_ms"`
	Rows         []KeyVizRow   `json:"rows"`
	Series       KeyVizSeries  `json:"series"`
	GeneratedAt  time.Time     `json:"generated_at"`
	Fanout       *FanoutResult `json:"fanout,omitempty"`
}

// KeyVizRow is one route's worth of activity across the column window,
// matching the proto KeyVizRow layout. Values is parallel to
// KeyVizMatrix.ColumnUnixMs — Values[j] is the counter for that route
// at column j.
//
// Conflict is true when the Phase 2-C max-merge collapsed disagreeing
// values from multiple nodes for the same row (see fan-out design 4.2);
// the SPA hatches such rows so operators know the displayed total may
// understate the true per-window count during a leadership flip. The
// flag is row-level for now and will move to per-cell when the proto
// extension lands in Phase 2-C+.
type KeyVizRow struct {
	BucketID          string   `json:"bucket_id"`
	Start             []byte   `json:"start"`
	End               []byte   `json:"end"`
	Aggregate         bool     `json:"aggregate"`
	RouteIDs          []uint64 `json:"route_ids,omitempty"`
	RouteIDsTruncated bool     `json:"route_ids_truncated,omitempty"`
	RouteCount        uint64   `json:"route_count"`
	Values            []uint64 `json:"values"`
	Conflict          bool     `json:"conflict,omitempty"`
	// total accumulates the sum of Values during pivot so the
	// rowBudget sort is O(N log N) on a precomputed key rather
	// than O(N log N × M) recomputing the sum per comparison.
	// Not on the wire — clients read activity off Values directly.
	total uint64
}

// KeyVizHandler serves GET /admin/api/v1/keyviz/matrix.
//
// Query parameters (all optional):
//
//	series      - reads | writes | read_bytes | write_bytes (default: writes)
//	from_unix_ms - lower bound in unix ms; 0 or omitted means unbounded
//	               on that side (NOT the Unix epoch)
//	to_unix_ms   - upper bound in unix ms; same 0 = unbounded contract
//	rows         - row budget; default and maximum is 1024 (design §4.1).
//	               Omitted / 0 / negative all yield the cap; explicit
//	               values above the cap are silently clamped down.
//
// Returns 503 codes.Unavailable when no sampler is configured so the
// SPA can distinguish "keyviz disabled" from "no data yet" (the
// latter is a successful empty matrix).
type KeyVizHandler struct {
	source KeyVizSource
	now    func() time.Time
	logger *slog.Logger
	// fanout is non-nil when the operator configured
	// --keyvizFanoutNodes. When set, ServeHTTP merges the local
	// matrix with peer responses before encoding the JSON body.
	// nil keeps the legacy single-node behaviour.
	fanout *KeyVizFanout
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

// WithFanout enables cluster-wide fan-out aggregation. Pass nil to
// disable; passing a configured aggregator switches the handler to
// merge the local matrix with peer responses on every request.
func (h *KeyVizHandler) WithFanout(f *KeyVizFanout) *KeyVizHandler {
	h.fanout = f
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
	if h.fanout != nil {
		// Forward the inbound user's cookies so the peer's SessionAuth
		// middleware sees a valid principal. Cluster nodes share
		// --adminSessionSigningKey for HA, so a cookie minted on this
		// node verifies on any peer. nil cookies make peers 401,
		// which surfaces as ok=false in the per-node status.
		matrix = h.fanout.Run(r.Context(), params, matrix, r.Cookies())
	}
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
	// rows defaults to the cap so a normal SPA poll without the
	// query parameter still respects the budget — leaving it at the
	// zero value would let applyKeyVizRowBudget fall through to "no
	// cap" and return every tracked route in one payload (Codex
	// round-3 P1 on PR #660).
	p := keyVizParams{series: keyVizDefaultSeries, rows: keyVizRowBudgetCap}
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
		// Caller pre-set dst to the default cap; preserve it.
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return errors.New("rows must be an integer")
	}
	if n <= 0 || n > keyVizRowBudgetCap {
		// Explicit 0 / negative / above-cap all collapse to the cap
		// (same as omitting the param) so callers can't disable the
		// budget by passing pathological values.
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
			v := pick(mr)
			row.Values[j] = v
			row.total += v
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
	switch {
	case !mr.Aggregate && total == 0:
		// Individual slots with the field zero-initialised — every
		// real route contributes exactly one member to itself.
		total = 1
	case mr.Aggregate && total == 0:
		// Defensive fallback: a virtual bucket should always carry a
		// non-zero MemberRoutesTotal once foldIntoBucket has run, but
		// if a sampler ever serialises a just-coalesced bucket before
		// the count is set the SPA would render "0 routes" — which is
		// nonsense for an aggregate row. Fall back to the visible
		// MemberRoutes length so route_count stays meaningful.
		total = uint64(len(mr.MemberRoutes))
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
//
// Sort uses the precomputed row.total (accumulated during pivot) so
// the comparator is O(1), not O(M). BucketID breaks activity ties
// deterministically — the SPA refresh on the same data must yield the
// same row set.
func applyKeyVizRowBudget(rows []KeyVizRow, budget int) []KeyVizRow {
	if budget <= 0 || len(rows) <= budget {
		return rows
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].total != rows[j].total {
			return rows[i].total > rows[j].total
		}
		return rows[i].BucketID < rows[j].BucketID
	})
	return rows[:budget]
}

func sortKeyVizRowsByStart(rows []KeyVizRow) {
	sort.Slice(rows, func(i, j int) bool {
		if c := bytes.Compare(rows[i].Start, rows[j].Start); c != 0 {
			return c < 0
		}
		return rows[i].BucketID < rows[j].BucketID
	})
}
