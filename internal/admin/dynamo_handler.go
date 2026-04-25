package admin

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
)

// Pagination knobs for the read-only Dynamo table list endpoint.
//
// defaultDynamoListLimit matches the design doc Section 4.3 default
// (100). dynamoListLimitMax is the hard ceiling; oversized client
// requests are clamped silently rather than rejected so the SPA can
// pass through an opaque "max" without a round-trip on validation.
const (
	defaultDynamoListLimit = 100
	dynamoListLimitMax     = 1000
)

// pathPrefixDynamoTables is the URL prefix the dynamo handler owns.
// "" + suffix "/tables" produces /admin/api/v1/dynamo/tables; the
// trailing slash variant routes to the per-table sub-handler.
const (
	pathDynamoTables       = "/admin/api/v1/dynamo/tables"
	pathPrefixDynamoTables = pathDynamoTables + "/"
)

// DynamoTableSummary is the JSON shape the admin dashboard consumes.
// Defined in the admin package — rather than reusing the adapter's
// AdminTableSummary directly — so the admin HTTP layer does not pull
// in the heavyweight adapter dependency tree (gRPC, Raft, etc.) and
// remains testable in isolation. main_admin.go translates between
// adapter.AdminTableSummary and this type.
type DynamoTableSummary struct {
	Name                   string             `json:"name"`
	PartitionKey           string             `json:"partition_key"`
	SortKey                string             `json:"sort_key,omitempty"`
	Generation             uint64             `json:"generation"`
	GlobalSecondaryIndexes []DynamoGSISummary `json:"global_secondary_indexes,omitempty"`
}

// DynamoGSISummary mirrors DynamoTableSummary for a single GSI.
type DynamoGSISummary struct {
	Name           string `json:"name"`
	PartitionKey   string `json:"partition_key"`
	SortKey        string `json:"sort_key,omitempty"`
	ProjectionType string `json:"projection_type"`
}

// TablesSource is the contract the dynamo handler depends on. Wired in
// production to *adapter.DynamoDBServer via a small bridge in
// main_admin.go; tests use a stub.
//
// AdminDescribeTable returns (nil, false, nil) for a missing table so
// callers can distinguish "not found" from a storage error without
// sniffing sentinels. This mirrors the adapter signature exactly so
// the bridge remains a thin pass-through.
type TablesSource interface {
	AdminListTables(ctx context.Context) ([]string, error)
	AdminDescribeTable(ctx context.Context, name string) (*DynamoTableSummary, bool, error)
}

// DynamoHandler serves /admin/api/v1/dynamo/tables and
// /admin/api/v1/dynamo/tables/{name}. Only GET is supported here —
// table creation and deletion live behind the protected write chain
// in a follow-up handler.
type DynamoHandler struct {
	source TablesSource
	logger *slog.Logger
}

// NewDynamoHandler binds the source and seeds logging with
// slog.Default(). Use WithLogger to attach a tagged logger.
func NewDynamoHandler(source TablesSource) *DynamoHandler {
	return &DynamoHandler{source: source, logger: slog.Default()}
}

// WithLogger overrides the default slog destination.
func (h *DynamoHandler) WithLogger(l *slog.Logger) *DynamoHandler {
	if l == nil {
		return h
	}
	h.logger = l
	return h
}

// ServeHTTP routes /tables and /tables/{name}. We do not use
// http.ServeMux because the admin router already guards the
// /admin/api/v1/* prefix — adding another mux here would just
// duplicate the path-parsing logic.
func (h *DynamoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET is implemented")
		return
	}
	switch {
	case r.URL.Path == pathDynamoTables:
		h.handleList(w, r)
	case strings.HasPrefix(r.URL.Path, pathPrefixDynamoTables):
		name := strings.TrimPrefix(r.URL.Path, pathPrefixDynamoTables)
		h.handleDescribe(w, r, name)
	default:
		writeJSONError(w, http.StatusNotFound, "not_found", "")
	}
}

// dynamoListResponse is the JSON shape returned by GET /tables.
// NextToken is omitted when there is no further page so the client
// can use a presence check rather than parsing an empty string.
type dynamoListResponse struct {
	Tables    []string `json:"tables"`
	NextToken string   `json:"next_token,omitempty"`
}

func (h *DynamoHandler) handleList(w http.ResponseWriter, r *http.Request) {
	limit, err := parseDynamoListLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}
	startAfter, err := decodeDynamoNextToken(r.URL.Query().Get("next_token"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_next_token", err.Error())
		return
	}

	// AdminListTables returns the full lex-sorted name list that
	// the adapter's metadata prefix scan produces; we then slice
	// to the requested page. The adapter's listTableNames already
	// materialises the same list for the SigV4 listTables path
	// (adapter/dynamodb.go:1146), which has been in production
	// since DynamoDB-compat shipped — admin's memory profile is
	// strictly the SigV4 path's, not a regression on top of it.
	//
	// Worst-case bound: a Dynamo table name caps at 255 bytes, so
	// 1k tables ≈ 256 KiB and 10k tables ≈ 2.5 MiB of name
	// strings on the heap during a single list call. That is well
	// inside the per-request budget the admin listener targets.
	// Beyond that scale the right fix is to teach the adapter to
	// stream the metadata scan via a callback (and plumb it
	// through here), not to bolt a streaming layer on top of the
	// already-materialised slice. Tracked separately; this
	// endpoint is not the limiting factor.
	names, err := h.source.AdminListTables(r.Context())
	if err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin dynamo list tables failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "dynamo_list_failed",
			"failed to list tables; see server logs")
		return
	}

	page, next := paginateDynamoTableNames(names, startAfter, limit)
	resp := dynamoListResponse{Tables: page}
	if next != "" {
		resp.NextToken = encodeDynamoNextToken(next)
	}
	// Tables is an array contract; mint an empty slice rather than
	// emitting JSON `null` when the cluster has no tables yet.
	if resp.Tables == nil {
		resp.Tables = []string{}
	}
	writeAdminJSON(w, r.Context(), h.logger, resp)
}

func (h *DynamoHandler) handleDescribe(w http.ResponseWriter, r *http.Request, name string) {
	if name == "" || strings.ContainsRune(name, '/') {
		writeJSONError(w, http.StatusNotFound, "not_found", "")
		return
	}
	summary, exists, err := h.source.AdminDescribeTable(r.Context(), name)
	if err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin dynamo describe table failed",
			slog.String("table", name),
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "dynamo_describe_failed",
			"failed to describe table; see server logs")
		return
	}
	if !exists {
		writeJSONError(w, http.StatusNotFound, "not_found", "table does not exist")
		return
	}
	writeAdminJSON(w, r.Context(), h.logger, summary)
}

// parseDynamoListLimit translates the ?limit= query parameter into a
// concrete page size. Empty falls back to the design-doc default;
// negatives or non-numerics are an outright client error; values past
// the ceiling are silently clamped (not an error) so the SPA's
// "request the maximum" pattern works without a probe round-trip.
func parseDynamoListLimit(raw string) (int, error) {
	if raw == "" {
		return defaultDynamoListLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	if n <= 0 {
		return 0, errors.New("limit must be positive")
	}
	if n > dynamoListLimitMax {
		return dynamoListLimitMax, nil
	}
	return n, nil
}

// decodeDynamoNextToken reverses encodeDynamoNextToken. We base64-wrap
// the raw last-table-name so the wire token is opaque from the
// client's perspective and we can change the cursor representation
// later without breaking the API contract.
func decodeDynamoNextToken(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return "", errors.New("next_token is not valid base64url")
	}
	return string(decoded), nil
}

func encodeDynamoNextToken(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

// paginateDynamoTableNames slices `names` (already lex-sorted by the
// adapter) into a single page starting strictly after `startAfter`.
// The second return is the opaque cursor the client should pass back
// for the next call, or "" if this is the last page.
func paginateDynamoTableNames(names []string, startAfter string, limit int) ([]string, string) {
	start := 0
	if startAfter != "" {
		// sort.SearchStrings returns the first index >= startAfter;
		// adding 1 only when the entry equals startAfter gives us
		// "strictly after" semantics. A startAfter that no longer
		// exists in the sorted list still produces a sane resume
		// (we pick up at the first name greater than the cursor).
		idx := sort.SearchStrings(names, startAfter)
		switch {
		case idx >= len(names):
			return []string{}, ""
		case names[idx] == startAfter:
			start = idx + 1
		default:
			start = idx
		}
	}
	end := start + limit
	if end > len(names) {
		end = len(names)
	}
	page := names[start:end]
	if end < len(names) && len(page) > 0 {
		return page, page[len(page)-1]
	}
	return page, ""
}

// writeAdminJSON marshals `body` to a buffer first, *then* writes
// status + body — never streaming an encoder directly to the
// ResponseWriter. The streaming form would commit a 200 header and
// then truncate mid-body if json.Marshal failed on a value deep in
// the struct (an unsupported type, a Marshaler returning an error,
// etc.), leaving a malformed JSON object on the wire that the SPA
// has no way to recover from. Marshalling first lets us upgrade the
// encode failure to a 500 with a well-formed error envelope.
func writeAdminJSON(w http.ResponseWriter, ctx context.Context, logger *slog.Logger, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		if logger == nil {
			logger = slog.Default()
		}
		logger.LogAttrs(ctx, slog.LevelError, "admin response marshal failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to encode response")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, werr := w.Write(payload); werr != nil {
		// Status is already on the wire, so we can only log. Write
		// failures here usually mean the client closed the connection.
		if logger == nil {
			logger = slog.Default()
		}
		logger.LogAttrs(ctx, slog.LevelWarn, "admin response write failed",
			slog.String("error", werr.Error()),
		)
	}
}
