package admin

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
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
// sniffing sentinels. The write entrypoints return the structured
// errors below (ErrTablesForbidden / ErrTablesNotLeader / ...) so
// the handler can map them to HTTP statuses without leaking the
// adapter's internal error shape into the admin package.
type TablesSource interface {
	AdminListTables(ctx context.Context) ([]string, error)
	AdminDescribeTable(ctx context.Context, name string) (*DynamoTableSummary, bool, error)
	AdminCreateTable(ctx context.Context, principal AuthPrincipal, in CreateTableRequest) (*DynamoTableSummary, error)
	AdminDeleteTable(ctx context.Context, principal AuthPrincipal, name string) error
}

// CreateTableRequest is the JSON body shape for POST /tables per
// design Section 4.2. The handler validates each field before
// passing the request to the source.
type CreateTableRequest struct {
	TableName    string                `json:"table_name"`
	PartitionKey CreateTableAttribute  `json:"partition_key"`
	SortKey      *CreateTableAttribute `json:"sort_key,omitempty"`
	GSI          []CreateTableGSI      `json:"gsi,omitempty"`
}

// CreateTableAttribute names a single primary-key or GSI key
// column. Type must be one of "S", "N", "B".
type CreateTableAttribute struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// CreateTableGSI describes a single global secondary index in a
// CreateTableRequest. SortKey is optional (hash-only GSI). When
// Projection.Type is "INCLUDE", Projection.NonKeyAttributes lists
// the projected attribute names; otherwise it is ignored.
type CreateTableGSI struct {
	Name         string                `json:"name"`
	PartitionKey CreateTableAttribute  `json:"partition_key"`
	SortKey      *CreateTableAttribute `json:"sort_key,omitempty"`
	Projection   CreateTableProjection `json:"projection"`
}

// CreateTableProjection mirrors the DynamoDB Projection sub-struct
// in admin-friendly snake_case. Type defaults to "ALL" when omitted.
type CreateTableProjection struct {
	Type             string   `json:"type,omitempty"`
	NonKeyAttributes []string `json:"non_key_attributes,omitempty"`
}

// Errors the source layer may return to signal a structured
// failure mode the handler maps to a specific HTTP response.
//
// They are sentinel values so a bridge implementation can map any
// adapter-internal error onto exactly one of these without the
// admin package importing the adapter package's private types.
var (
	// ErrTablesForbidden is returned when the principal lacks the
	// role required for the operation. Maps to 403.
	ErrTablesForbidden = errors.New("admin tables: principal lacks required role")
	// ErrTablesNotLeader is returned when the local node is not the
	// Raft leader. Maps to 503 + Retry-After: 1 today; the future
	// AdminForward RPC catches this as the trigger to forward.
	ErrTablesNotLeader = errors.New("admin tables: local node is not the raft leader")
	// ErrTablesNotFound is returned when DELETE / DESCRIBE / a
	// follow-up read targets a table that does not exist. Maps to
	// 404. AdminDescribeTable's (nil, false, nil) tuple is the
	// preferred signal for the read path; this sentinel covers the
	// write paths only.
	ErrTablesNotFound = errors.New("admin tables: table not found")
	// ErrTablesAlreadyExists is returned when CreateTable hits a
	// pre-existing table with the same name. Maps to 409.
	ErrTablesAlreadyExists = errors.New("admin tables: table already exists")
)

// ValidationError is what the source returns when the input fails
// adapter-side validation. Surfaces a sanitised message back to the
// SPA — adapter-internal err.Error() output is never sent verbatim.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string {
	if e == nil || e.Message == "" {
		return "admin tables: validation failed"
	}
	return e.Message
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
	switch {
	case r.URL.Path == pathDynamoTables:
		switch r.Method {
		case http.MethodGet:
			h.handleList(w, r)
		case http.MethodPost:
			h.handleCreate(w, r)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or POST")
		}
	case strings.HasPrefix(r.URL.Path, pathPrefixDynamoTables):
		name := strings.TrimPrefix(r.URL.Path, pathPrefixDynamoTables)
		switch r.Method {
		case http.MethodGet:
			h.handleDescribe(w, r, name)
		case http.MethodDelete:
			h.handleDelete(w, r, name)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or DELETE")
		}
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
	// paginateDynamoTableNames is total over its input — it always
	// returns a non-nil slice (an empty []string{} on the
	// "cursor past end" branch, a real sub-slice otherwise) so the
	// JSON shape is always `"tables": []` rather than `null` even
	// without an explicit nil-check here. The Tables array
	// contract is enforced at the producer.
	writeAdminJSON(w, r.Context(), h.logger, resp)
}

// handleCreate is the POST /tables handler. It validates the body
// up front, requires a write-capable principal, and translates any
// structured error from the source into the appropriate HTTP status.
// Success response is 201 Created with the freshly-stored table
// summary in the body — same shape as a GET /tables/{name} call.
func (h *DynamoHandler) handleCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		// Should be unreachable — SessionAuth runs before this
		// handler and rejects any request without a principal — but
		// failing closed here is the right defence-in-depth posture.
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no session principal")
		return
	}
	if !principal.Role.AllowsWrite() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
		return
	}
	body, err := decodeCreateTableRequest(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	summary, err := h.source.AdminCreateTable(r.Context(), principal, body)
	if err != nil {
		h.writeTablesError(w, r, "create", err)
		return
	}
	writeAdminJSONStatus(w, r.Context(), h.logger, http.StatusCreated, summary)
}

// handleDelete is the DELETE /tables/{name} handler. Success is
// 204 No Content; the body is intentionally empty so the SPA can
// treat both 200 and 204 as success without parsing.
func (h *DynamoHandler) handleDelete(w http.ResponseWriter, r *http.Request, name string) {
	if name == "" || strings.ContainsRune(name, '/') {
		writeJSONError(w, http.StatusNotFound, "not_found", "")
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no session principal")
		return
	}
	if !principal.Role.AllowsWrite() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
		return
	}
	if err := h.source.AdminDeleteTable(r.Context(), principal, name); err != nil {
		h.writeTablesError(w, r, "delete", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// writeTablesError translates a TablesSource error into the
// appropriate HTTP response. Internal-server-error fallthrough logs
// the raw err.Error() but never sends it to the client, matching
// the read-path policy.
func (h *DynamoHandler) writeTablesError(w http.ResponseWriter, r *http.Request, op string, err error) {
	switch {
	case errors.Is(err, ErrTablesForbidden):
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
	case errors.Is(err, ErrTablesNotLeader):
		// The follower→leader forwarding RPC (design 3.3) will
		// catch this case in a follow-up PR. Until then, surface
		// 503 + Retry-After: 1 so the SPA / curl can re-issue.
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusServiceUnavailable, "leader_unavailable",
			"this admin node is not the raft leader")
	case errors.Is(err, ErrTablesNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", "table does not exist")
	case errors.Is(err, ErrTablesAlreadyExists):
		writeJSONError(w, http.StatusConflict, "already_exists", "table already exists")
	default:
		var verr *ValidationError
		if errors.As(err, &verr) {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", verr.Error())
			return
		}
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin dynamo "+op+" table failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "dynamo_"+op+"_failed",
			"failed to "+op+" table; see server logs")
	}
}

// decodeCreateTableRequest parses + validates the JSON body. Each
// failure mode maps to a specific human-readable message so the SPA
// can show a useful error without the user having to look at the
// network tab.
func decodeCreateTableRequest(body io.Reader) (CreateTableRequest, error) {
	if body == nil {
		return CreateTableRequest{}, errors.New("request body is empty")
	}
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	var out CreateTableRequest
	if err := dec.Decode(&out); err != nil {
		if IsMaxBytesError(err) {
			return CreateTableRequest{}, errors.New("request body exceeds the 64 KiB admin limit")
		}
		return CreateTableRequest{}, errors.New("request body is not valid JSON")
	}
	// Reject trailing JSON tokens — `{"table_name":"a", ...}{...}`
	// must surface as 400, not silently accept the first object and
	// drop the rest. dec.More() returns true when there is at least
	// one more JSON value in the stream beyond the one we just
	// decoded.
	if dec.More() {
		return CreateTableRequest{}, errors.New("request body has trailing data after the JSON object")
	}
	if err := validateCreateTableRequest(&out); err != nil {
		return CreateTableRequest{}, err
	}
	return out, nil
}

// validateCreateTableRequest is the field-level validation pass
// kept separate from the JSON decoding so each function stays under
// the project's cyclomatic-complexity ceiling and the decoder is
// trivially auditable on its own.
func validateCreateTableRequest(in *CreateTableRequest) error {
	if strings.TrimSpace(in.TableName) == "" {
		return errors.New("table_name is required")
	}
	// Reject slash-bearing names symmetrically with handleDescribe
	// and handleDelete, which already 404 on `/`. Without this
	// guard a user could create `foo/bar` and then never be able
	// to describe or delete it through the same admin surface —
	// the orphaned table would be reachable only through the SigV4
	// path. Blocking the asymmetric edge case at create time is
	// strictly better than discovering it later.
	if strings.ContainsRune(in.TableName, '/') {
		return errors.New("table_name must not contain '/'")
	}
	if err := validateAttribute(in.PartitionKey, "partition_key"); err != nil {
		return err
	}
	if in.SortKey != nil {
		if err := validateAttribute(*in.SortKey, "sort_key"); err != nil {
			return err
		}
	}
	for i := range in.GSI {
		if err := validateGSI(&in.GSI[i], i); err != nil {
			return err
		}
	}
	return nil
}

// validateAttribute enforces the "S | N | B" rule for primary-key
// and GSI key columns. We deliberately do not silently accept
// lower-case or whitespace-padded variants — Dynamo's wire format
// requires the exact upper-case letter.
func validateAttribute(attr CreateTableAttribute, field string) error {
	if strings.TrimSpace(attr.Name) == "" {
		return errors.New(field + ".name is required")
	}
	switch attr.Type {
	case "S", "N", "B":
		return nil
	default:
		return errors.New(field + `.type must be one of "S", "N", "B"`)
	}
}

func validateGSI(gsi *CreateTableGSI, index int) error {
	prefix := "gsi[" + strconv.Itoa(index) + "]"
	if strings.TrimSpace(gsi.Name) == "" {
		return errors.New(prefix + ".name is required")
	}
	if err := validateAttribute(gsi.PartitionKey, prefix+".partition_key"); err != nil {
		return err
	}
	if gsi.SortKey != nil {
		if err := validateAttribute(*gsi.SortKey, prefix+".sort_key"); err != nil {
			return err
		}
	}
	switch strings.TrimSpace(strings.ToUpper(gsi.Projection.Type)) {
	case "", "ALL", "KEYS_ONLY", "INCLUDE":
		return nil
	default:
		return errors.New(prefix + `.projection.type must be one of "ALL", "KEYS_ONLY", "INCLUDE"`)
	}
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

// writeAdminJSON is the 200-OK convenience wrapper around
// writeAdminJSONStatus. It exists only so the read-path call sites
// stay compact; both routes share the same marshal-then-write
// safety guarantee.
func writeAdminJSON(w http.ResponseWriter, ctx context.Context, logger *slog.Logger, body any) {
	writeAdminJSONStatus(w, ctx, logger, http.StatusOK, body)
}

// writeAdminJSONStatus marshals `body` to a buffer first, *then*
// writes status + body — never streaming an encoder directly to the
// ResponseWriter. The streaming form would commit the status header
// and then truncate mid-body if json.Marshal failed on a value deep
// in the struct (an unsupported type, a Marshaler returning an
// error), leaving a malformed JSON object on the wire that the SPA
// has no way to recover from. Marshalling first lets us upgrade an
// encode failure to a clean 500 with a well-formed error envelope.
func writeAdminJSONStatus(w http.ResponseWriter, ctx context.Context, logger *slog.Logger, status int, body any) {
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
	// Defence-in-depth: tell the browser not to MIME-sniff the
	// response body. The admin surface is JSON-only, so a sniffed
	// "this might be HTML" guess is never useful and could enable
	// XSS-via-sniffing on a hostile payload that somehow reached
	// here. Cookie-gated admin endpoints + a single static
	// Content-Type make this cheap and standard.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
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
