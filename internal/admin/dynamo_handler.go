package admin

import (
	"bytes"
	"context"
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
	// Raft leader. When a LeaderForwarder is configured,
	// tryForwardCreate / tryForwardDelete catch this before it reaches
	// writeTablesError and forward the request to the leader
	// transparently. Without a forwarder, maps to 503 + Retry-After: 1.
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

// errCreateBodyTooLarge is returned by decodeCreateTableRequest
// when the request body trips the BodyLimit middleware's
// MaxBytesReader. The handler matches this sentinel to map the
// failure to 413 payload_too_large rather than the generic 400
// invalid_body — the BodyLimit/middleware contract documented in
// internal/admin/middleware.go (Codex P2 on PR #634 flagged the
// previous always-400 behaviour as a regression).
var errCreateBodyTooLarge = errors.New("request body exceeds the 64 KiB admin limit")

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
// /admin/api/v1/dynamo/tables/{name}. The collection root accepts
// GET (list) and POST (create); the per-table route accepts GET
// (describe) and DELETE. Writes go through the same protected
// chain as reads (BodyLimit -> SessionAuth -> Audit -> CSRF) plus
// an in-handler RoleFull check so a read-only key cannot mutate
// even with a valid CSRF token.
//
// Writes additionally re-resolve the principal's access key
// against a live RoleStore (when configured) so that a downgraded
// or revoked key cannot continue mutating with a still-valid JWT
// — the JWT freezes the role at login time, and tokens last one
// hour. Codex P1 on PR #635 flagged the gap on the HTTP path;
// the forward server already does this re-evaluation on its side.
//
// When the source returns ErrTablesNotLeader and a LeaderForwarder
// is configured, write requests are forwarded to the leader
// transparently — the SPA sees a leader-direct response shape
// regardless of which node it hit (design Section 3.3 criterion 2).
type DynamoHandler struct {
	source    TablesSource
	roles     RoleStore
	forwarder LeaderForwarder
	logger    *slog.Logger
}

// NewDynamoHandler binds the source and seeds logging with
// slog.Default(). Use WithLogger to attach a tagged logger,
// WithRoleStore to plug in the live access-key role lookup, and
// WithLeaderForwarder to plug in the follower→leader forwarder.
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

// WithRoleStore enables per-request role revalidation on write
// endpoints. Without it, the handler trusts whatever role is
// embedded in the session JWT — which is fine for single-tenant
// deployments where the role config never changes, but
// problematic when an operator revokes or downgrades a key. The
// production wiring in main_admin.go always sets this.
func (h *DynamoHandler) WithRoleStore(r RoleStore) *DynamoHandler {
	h.roles = r
	return h
}

// WithLeaderForwarder enables transparent follower→leader
// forwarding. Without it, write requests on a follower fall back
// to the standard 503 leader_unavailable response. Production
// wires this to the gRPCForwardClient in main_admin.go; tests
// inject a stub.
//
// Asymmetric vs WithLogger by design: WithLogger no-ops on nil to
// preserve the slog.Default() seeded by NewDynamoHandler, but a
// nil forwarder here is a meaningful "disable forwarding" state
// (the gate in tryForwardCreate / tryForwardDelete checks for
// nil and falls back to the leader-only 503 path).
func (h *DynamoHandler) WithLeaderForwarder(f LeaderForwarder) *DynamoHandler {
	h.forwarder = f
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
	limit, startAfter, ok := parseListPaginationParams(w, r, defaultDynamoListLimit, dynamoListLimitMax)
	if !ok {
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
		resp.NextToken = encodeListNextToken(next)
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
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	body, err := decodeCreateTableRequest(r.Body)
	if err != nil {
		if errors.Is(err, errCreateBodyTooLarge) {
			WriteMaxBytesError(w)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	summary, err := h.source.AdminCreateTable(r.Context(), principal, body)
	if err != nil {
		// On a follower, the source returns ErrTablesNotLeader. If
		// a forwarder is wired, dispatch to the leader and re-emit
		// the leader's response verbatim — the SPA cannot tell the
		// difference between a leader-direct call and a forwarded
		// one. Without a forwarder, fall through to the standard
		// 503 leader_unavailable response.
		if h.tryForwardCreate(w, r, principal, body, err) {
			return
		}
		h.writeTablesError(w, r, "create", err)
		return
	}
	writeAdminJSONStatus(w, r.Context(), h.logger, http.StatusCreated, summary)
}

// tryForwardCreate handles the follower→leader forwarding path
// for POST /tables. Returns true when the response has been
// written (regardless of forward success/failure); the caller
// should then return without further processing.
//
// The "fall through to 503" path runs only when:
//   - the source error is something other than ErrTablesNotLeader,
//   - or no LeaderForwarder was configured,
//   - or the forwarder itself returned ErrLeaderUnavailable
//     (election in progress on the leader, criterion 3).
//
// Any other forwarder failure (gRPC transport error, etc.) is
// also surfaced as 503 + Retry-After: 1 so the SPA can re-issue.
// We log the raw error for operators and never echo it to clients.
func (h *DynamoHandler) tryForwardCreate(w http.ResponseWriter, r *http.Request, principal AuthPrincipal, body CreateTableRequest, sourceErr error) bool {
	if !errors.Is(sourceErr, ErrTablesNotLeader) || h.forwarder == nil {
		return false
	}
	res, err := h.forwarder.ForwardCreateTable(r.Context(), principal, body)
	if err != nil {
		h.writeForwardFailure(w, r, "create", err)
		return true
	}
	h.writeForwardResult(w, r, res)
	return true
}

// handleDelete is the DELETE /tables/{name} handler. Success is
// 204 No Content; the body is intentionally empty so the SPA can
// treat both 200 and 204 as success without parsing.
func (h *DynamoHandler) handleDelete(w http.ResponseWriter, r *http.Request, name string) {
	if name == "" || strings.ContainsRune(name, '/') {
		writeJSONError(w, http.StatusNotFound, "not_found", "")
		return
	}
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	if err := h.source.AdminDeleteTable(r.Context(), principal, name); err != nil {
		if h.tryForwardDelete(w, r, principal, name, err) {
			return
		}
		h.writeTablesError(w, r, "delete", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// principalForWrite is the centralised authorisation gate for
// state-changing endpoints. It pulls the principal out of the
// request context (failing closed if SessionAuth somehow did not
// attach one), enforces RoleFull on the JWT-embedded role, and —
// when a RoleStore is configured — re-validates the access key
// against the live cluster role index so a downgraded or revoked
// key cannot continue mutating with a still-valid JWT (Codex P1
// on PR #635).
//
// On any rejection the helper writes the appropriate HTTP error
// directly and returns ok=false so callers can early-exit with no
// further work. The forward server applies the same re-validation
// on its side, so leader-direct and forwarded write requests have
// matching authorisation contracts.
func (h *DynamoHandler) principalForWrite(w http.ResponseWriter, r *http.Request) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		// Should be unreachable — SessionAuth runs before this
		// handler and rejects any request without a principal —
		// but failing closed here is the right defence-in-depth
		// posture for any future routing change that might bypass
		// the middleware chain.
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no session principal")
		return AuthPrincipal{}, false
	}
	if !principal.Role.AllowsWrite() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
		return AuthPrincipal{}, false
	}
	// Live re-validation against the current role map. Skip when
	// no RoleStore is configured (single-tenant deployments where
	// the JWT-embedded role is authoritative); production wiring
	// always sets one.
	if h.roles != nil {
		liveRole, exists := h.roles.LookupRole(principal.AccessKey)
		if !exists || !liveRole.AllowsWrite() {
			// Don't surface "your key was revoked" vs "your key
			// was downgraded" — both are 403 forbidden, and the
			// distinction is operator-visible only.
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this endpoint requires a full-access role")
			return AuthPrincipal{}, false
		}
		// Use the live role downstream; the JWT may carry a
		// stale value but the live one is authoritative.
		principal.Role = liveRole
	}
	return principal, true
}

// tryForwardDelete is the DELETE counterpart of tryForwardCreate.
// Same semantics: only the ErrTablesNotLeader source error
// triggers forwarding, and only when a forwarder is configured.
func (h *DynamoHandler) tryForwardDelete(w http.ResponseWriter, r *http.Request, principal AuthPrincipal, name string, sourceErr error) bool {
	if !errors.Is(sourceErr, ErrTablesNotLeader) || h.forwarder == nil {
		return false
	}
	res, err := h.forwarder.ForwardDeleteTable(r.Context(), principal, name)
	if err != nil {
		h.writeForwardFailure(w, r, "delete", err)
		return true
	}
	h.writeForwardResult(w, r, res)
	return true
}

// writeForwardResult re-emits the leader's structured response
// verbatim. Status, payload, and content-type all come from the
// gRPC response so a forwarded request looks identical to a
// leader-direct call from the SPA's point of view.
func (h *DynamoHandler) writeForwardResult(w http.ResponseWriter, r *http.Request, res *ForwardResult) {
	w.Header().Set("Content-Type", res.ContentType)
	// Match writeAdminJSONStatus's hardening on the leader-direct
	// path: forwarded responses must also carry nosniff so a SPA
	// request that happened to traverse a follower does not
	// silently lose the MIME-sniff protection. Claude review on
	// PR #644 caught the parity gap.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	// 503 from the leader (e.g. it stepped down mid-request) must
	// carry Retry-After so the client retries; preserve the
	// criterion-3 contract on the wire whether the 503 originated
	// here or at the leader.
	if res.StatusCode == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(res.StatusCode)
	if len(res.Payload) > 0 {
		if _, err := w.Write(res.Payload); err != nil {
			h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin forward response write failed",
				slog.String("error", err.Error()),
			)
		}
	}
}

// writeForwardFailure handles forwarder errors that did not
// produce a structured leader response: ErrLeaderUnavailable
// (election in flight) and gRPC transport errors. Both surface as
// 503 + Retry-After: 1 — the SPA's retry contract is identical
// regardless of whether the leader is briefly absent or the
// network hiccupped.
func (h *DynamoHandler) writeForwardFailure(w http.ResponseWriter, r *http.Request, op string, err error) {
	if !errors.Is(err, ErrLeaderUnavailable) {
		// Not the "no leader known" case — log the raw error so
		// operators can investigate. Client still sees the same
		// 503 + Retry-After so they retry uniformly.
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin dynamo "+op+" forward failed",
			slog.String("error", err.Error()),
		)
	}
	w.Header().Set("Retry-After", "1")
	writeJSONError(w, http.StatusServiceUnavailable, "leader_unavailable",
		"raft leader currently unavailable; retry shortly")
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
		// Reached only when no LeaderForwarder is configured (single-
		// node or leader-only deployments). When a forwarder is wired,
		// tryForwardCreate / tryForwardDelete intercept ErrTablesNotLeader
		// before writeTablesError is called.
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
	raw, err := io.ReadAll(body)
	if err != nil {
		if IsMaxBytesError(err) {
			// Sentinel so handleCreate can map to 413 rather than
			// the generic 400 invalid_body.
			return CreateTableRequest{}, errCreateBodyTooLarge
		}
		return CreateTableRequest{}, errors.New("request body could not be read")
	}
	// Reject any NUL byte in the body. JSON has no need for a
	// raw NUL (control characters must be \u-escaped), and at
	// least one of our decoders (goccy/go-json) treats a raw
	// NUL as end-of-input, so a body like
	// `{"table_name":...}\x00{"extra":1}` would otherwise sneak
	// past dec.More(). Codex P2 on PR #634 flagged this as a
	// payload-smuggling vector.
	if bytes.IndexByte(raw, 0) >= 0 {
		return CreateTableRequest{}, errors.New("request body contains a NUL byte")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var out CreateTableRequest
	if err := dec.Decode(&out); err != nil {
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
	// Trim whitespace in place so the canonical name flows through
	// the rest of the pipeline. Without this, a name like " foo "
	// passes the empty-after-trim check, propagates to the adapter
	// (whose own TrimSpace check on creation also passes), and
	// gets stored verbatim — leaving a table that the URL-based
	// describe/delete routes cannot address because they trim the
	// segment literally. Claude's review on PR #634 flagged the
	// drift; trimming once at this boundary fixes it.
	in.TableName = strings.TrimSpace(in.TableName)
	if in.TableName == "" {
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
	// Canonicalise the projection type in-place. The handler
	// accepts case-insensitive input ("include" / "ALL") for SPA
	// ergonomics, but the adapter's buildCreateTableProjection
	// only matches exact uppercase. Normalising once at the
	// boundary keeps that mismatch from surfacing as a confusing
	// post-validation 500 — the bridge and the AdminForward server
	// both forward whatever ends up in this field, so writing back
	// the canonical form here means every downstream consumer sees
	// the same shape. The empty string keeps its meaning ("default
	// to ALL") on both sides.
	canonical := strings.TrimSpace(strings.ToUpper(gsi.Projection.Type))
	switch canonical {
	case "", "ALL", "KEYS_ONLY", "INCLUDE":
		gsi.Projection.Type = canonical
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
// concrete page size. The shared parseListLimit lives in
// list_pagination.go; this comment is preserved here only because
// it documents the historical rationale for the default / clamp
// policy that the shared helper inherited.
//
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
