package admin

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
)

// Phase 3a — HTTP handlers for the item-level DynamoDB admin
// surface. Driven by §3.3 of
// docs/design/2026_05_22_implemented_admin_data_browser.md.
//
// Routes owned by this file (the parent DynamoHandler dispatches
// after parsing /tables/{name}/...):
//
//	GET    /admin/api/v1/dynamo/tables/{name}/items                — scan
//	GET    /admin/api/v1/dynamo/tables/{name}/items/{key-b64url}   — get
//	PUT    /admin/api/v1/dynamo/tables/{name}/items/{key-b64url}   — put
//	DELETE /admin/api/v1/dynamo/tables/{name}/items/{key-b64url}   — delete
//
// {key-b64url} carries a base64-url-encoded JSON object of the
// AdminAttributeValue map for the item's primary key. Base64-url
// is the same encoding the rest of the admin API uses for
// arbitrary-bytes path segments, so the path validator's `%`-ban
// passes cleanly.

// adminItemSubResource is the literal second segment after the
// table name that activates the items routes. Named so the
// dispatcher stays self-documenting.
const adminItemSubResource = "items"

// Scan-page knobs. The default page size matches what
// AdminScanTable on the adapter side defaults to (25 items per
// page); the SPA overrides via ?limit=<n>. The cap mirrors the
// adapter's adminItemScanMaxLimit (100) so an HTTP-supplied
// limit beyond the cap silently clamps at the server-side limit
// rather than producing a 400 — keeps the SPA pagination
// stateless.
const (
	defaultAdminItemScanLimit = 25
	adminItemScanLimitMax     = 100
)

// AdminAttributeValue mirrors the adapter package's wire shape
// (S/N/B/BOOL/NULL scalars, SS/NS/BS sets, L/M containers).
// Defined in this package so the admin HTTP layer stays free of
// the adapter dependency; the main_admin.go bridge converts
// between this type and adapter.AdminAttributeValue field-for-
// field.
// Struct tags here are the wire-shape contract. MarshalJSON
// overrides them to preserve the empty-but-present L/M distinction
// (`{"L":[]}` and `{"M":{}}` are valid, type-bearing AttributeValue
// shapes in DynamoDB; the SPA needs both shapes to render correctly).
// UnmarshalJSON uses the default behaviour — a JSON `"L":[]` parses
// to a non-nil zero-length slice via the tag.
type AdminAttributeValue struct {
	S    *string                        `json:"S,omitempty"`
	N    *string                        `json:"N,omitempty"`
	B    []byte                         `json:"B,omitempty"`
	BOOL *bool                          `json:"BOOL,omitempty"`
	NULL *bool                          `json:"NULL,omitempty"`
	SS   []string                       `json:"SS,omitempty"`
	NS   []string                       `json:"NS,omitempty"`
	BS   [][]byte                       `json:"BS,omitempty"`
	L    []AdminAttributeValue          `json:"L,omitempty"`
	M    map[string]AdminAttributeValue `json:"M,omitempty"`
}

// MarshalJSON preserves the kind distinction between "no L/M field"
// (nil slice/map) and "empty L/M field" (non-nil zero-length slice/map).
// A stock json.Marshal with omitempty would collapse both into the
// same wire shape, losing the type tag for an explicitly-empty list
// or map — and an explicitly-empty list is a legitimate DynamoDB
// AttributeValue ({"L":[]}). Gemini medium on PR #813.
//
// Mirrors the adapter package's AdminAttributeValue.MarshalJSON
// invariant in the same way, except this side does not enforce the
// "exactly-one-field" kind check (the bridge in main_admin.go calls
// into the adapter, which performs that validation as part of its
// own marshal-time depth/kind audit). Imposing the kind check here
// too would reject legitimate responses constructed by tests where
// a zero-value attribute is intentionally produced.
//
//nolint:cyclop // ten attribute kinds × per-kind nil-presence check
func (a AdminAttributeValue) MarshalJSON() ([]byte, error) {
	obj := make(map[string]any, 1)
	if a.S != nil {
		obj["S"] = *a.S
	}
	if a.N != nil {
		obj["N"] = *a.N
	}
	if a.B != nil {
		obj["B"] = a.B
	}
	if a.BOOL != nil {
		obj["BOOL"] = *a.BOOL
	}
	if a.NULL != nil {
		obj["NULL"] = *a.NULL
	}
	if a.SS != nil {
		obj["SS"] = a.SS
	}
	if a.NS != nil {
		obj["NS"] = a.NS
	}
	if a.BS != nil {
		obj["BS"] = a.BS
	}
	if a.L != nil {
		obj["L"] = a.L
	}
	if a.M != nil {
		obj["M"] = a.M
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, errors.New("marshal AdminAttributeValue: " + err.Error())
	}
	return out, nil
}

// AdminItem is one row as the SPA sees it.
type AdminItem struct {
	Attributes map[string]AdminAttributeValue `json:"attributes"`
}

// AdminScanItemsOptions / Result mirror the adapter package's
// shapes for the scan / continuation flow. ExclusiveStart is the
// continuation token a previous page surfaced as LastEvaluatedKey;
// the SPA passes it back verbatim via base64-url in the next
// request's ?next_cursor query.
type AdminScanItemsOptions struct {
	Limit          int                            `json:"limit,omitempty"`
	ExclusiveStart map[string]AdminAttributeValue `json:"exclusive_start,omitempty"`
}

type AdminScanItemsResult struct {
	Items            []AdminItem                    `json:"items"`
	LastEvaluatedKey map[string]AdminAttributeValue `json:"last_evaluated_key,omitempty"`
}

// Sentinel errors the bridge translates Admin*Item adapter
// errors into, so the handler can map them to HTTP status codes
// without sniffing the adapter's internal error vocabulary.

// ErrItemsForbidden — principal lacks the required role (RoleReadOnly
// for scan/get, RoleFull for put/delete). Maps to 403.
var ErrItemsForbidden = errors.New("admin dynamo items: forbidden")

// ErrItemsNotLeader — this node is not the Raft leader; SPA should
// retry against the leader returned by the leader-probe endpoint.
// Maps to 503.
var ErrItemsNotLeader = errors.New("admin dynamo items: not leader")

// ErrItemsTableNotFound — the named table does not exist. Maps to 404.
var ErrItemsTableNotFound = errors.New("admin dynamo items: table not found")

// ErrItemsValidation — empty / malformed key, body shape mismatch,
// nesting depth exceeded, unknown kind tag. Maps to 400.
var ErrItemsValidation = errors.New("admin dynamo items: invalid request")

// ---------- handler entry points (called from DynamoHandler.ServeHTTP) ----------

// handleItemsCollection serves GET /tables/{name}/items.
func (h *DynamoHandler) handleItemsCollection(w http.ResponseWriter, r *http.Request, table string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
		return
	}
	principal, ok := h.principalForItemRead(w, r)
	if !ok {
		return
	}
	opts, ok := parseAdminScanItemsQuery(w, r)
	if !ok {
		return
	}
	out, err := h.source.AdminScanItems(r.Context(), principal, table, opts)
	if err != nil {
		h.writeItemsError(w, r, "scan", table, err)
		return
	}
	writeAdminJSONStatus(w, r.Context(), h.logger, http.StatusOK, out)
}

// handleItemResource serves /tables/{name}/items/{key-b64url}
// (GET / PUT / DELETE).
func (h *DynamoHandler) handleItemResource(w http.ResponseWriter, r *http.Request, table, keySegment string) {
	key, ok := decodeAdminItemKeySegment(w, keySegment)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleItemGet(w, r, table, key)
	case http.MethodPut:
		h.handleItemPut(w, r, table, key)
	case http.MethodDelete:
		h.handleItemDelete(w, r, table, key)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET, PUT, or DELETE")
	}
}

func (h *DynamoHandler) handleItemGet(w http.ResponseWriter, r *http.Request, table string, key map[string]AdminAttributeValue) {
	principal, ok := h.principalForItemRead(w, r)
	if !ok {
		return
	}
	item, found, err := h.source.AdminGetItem(r.Context(), principal, table, key)
	if err != nil {
		h.writeItemsError(w, r, "get", table, err)
		return
	}
	if !found {
		writeJSONError(w, http.StatusNotFound, "not_found", "item not found")
		return
	}
	writeAdminJSONStatus(w, r.Context(), h.logger, http.StatusOK, item)
}

func (h *DynamoHandler) handleItemPut(w http.ResponseWriter, r *http.Request, table string, key map[string]AdminAttributeValue) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	item, err := decodeAdminItemBody(r.Body)
	if err != nil {
		if errors.Is(err, errCreateBodyTooLarge) {
			WriteMaxBytesError(w)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := assertItemBodyMatchesPathKey(item, key); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.source.AdminPutItem(r.Context(), principal, table, item); err != nil {
		h.writeItemsError(w, r, "put", table, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// decodeAdminItemBody is the PUT body parser. Same defence-in-depth
// shape as decodeCreateTableRequest in dynamo_handler.go: read the
// full body (BodyLimit middleware caps it), reject NUL bytes
// (goccy/go-json treats raw NUL as end-of-input, which would
// otherwise allow `{"attributes":...}\x00{"extra":1}` smuggling),
// decode strictly with DisallowUnknownFields, then reject any
// trailing JSON tokens. Codex P2 on PR #634 / Gemini medium on PR
// #813.
func decodeAdminItemBody(body io.Reader) (AdminItem, error) {
	if body == nil {
		return AdminItem{}, errors.New("request body is empty")
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		if IsMaxBytesError(err) {
			return AdminItem{}, errCreateBodyTooLarge
		}
		return AdminItem{}, errors.New("request body could not be read")
	}
	if len(raw) == 0 {
		return AdminItem{}, errors.New("request body is empty")
	}
	if bytes.IndexByte(raw, 0) >= 0 {
		return AdminItem{}, errors.New("request body contains a NUL byte")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var out AdminItem
	if err := dec.Decode(&out); err != nil {
		return AdminItem{}, errors.New("request body is not a valid AdminItem JSON object")
	}
	if dec.More() {
		return AdminItem{}, errors.New("request body has trailing data after the JSON object")
	}
	return out, nil
}

func (h *DynamoHandler) handleItemDelete(w http.ResponseWriter, r *http.Request, table string, key map[string]AdminAttributeValue) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	if err := h.source.AdminDeleteItem(r.Context(), principal, table, key); err != nil {
		h.writeItemsError(w, r, "delete", table, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------- helpers ----------

// principalForItemRead is the read-side companion to principalForWrite
// (live-role re-check + RoleReadOnly OR RoleFull). The payload
// returned by scan/get is item content — `principalForReadSensitive`
// equivalent on the Dynamo side. Defined locally here rather than as
// a method on DynamoHandler because it's only used by item-level
// handlers; the existing list/describe paths use the looser session-
// auth gate (their output is metadata, not content).
//
// JWT role gate fires unconditionally before the optional live-store
// re-check (matches principalForWrite's defence-in-depth pattern from
// PR #669). A token minted with role=none cannot slip through reads
// via a later live-role promotion; the live re-check can only further
// constrain. Claude review on PR #814 r2 caught the parallel S3
// asymmetry; this fix lands on PR #813 to keep the items handler
// consistent with the contract documented in principalForWrite.
func (h *DynamoHandler) principalForItemRead(w http.ResponseWriter, r *http.Request) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		// 401 matches principalForWrite (s3_handler.go,
		// dynamo_handler.go.principalForWrite) — semantically,
		// "no session principal" is unauthenticated, not internal
		// failure. The path is unreachable in production
		// (SessionAuth always attaches a principal upstream); the
		// 401 keeps the contract symmetric with the rest of the
		// admin surface (Claude review on PR #813 r2).
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no session principal")
		return AuthPrincipal{}, false
	}
	if !principal.Role.AllowsRead() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this access key is not authorised to read table contents")
		return AuthPrincipal{}, false
	}
	if h.roles != nil {
		live, exists := h.roles.LookupRole(principal.AccessKey)
		if !exists || !live.AllowsRead() {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this access key is not authorised to read table contents")
			return AuthPrincipal{}, false
		}
		// Use the live role downstream — the JWT may carry a stale
		// value but the live one is authoritative once both gates
		// agree the request is allowed.
		principal.Role = live
	}
	return principal, true
}

// parseAdminScanItemsQuery extracts ?limit and ?next_cursor from
// the URL. next_cursor is the base64-url-encoded JSON of the
// previous page's LastEvaluatedKey; absent or empty means "first
// page". limit clamps to [1, adminItemScanLimitMax].
func parseAdminScanItemsQuery(w http.ResponseWriter, r *http.Request) (AdminScanItemsOptions, bool) {
	q := r.URL.Query()
	opts := AdminScanItemsOptions{Limit: defaultAdminItemScanLimit}
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request",
				"limit must be a positive integer")
			return AdminScanItemsOptions{}, false
		}
		if n > adminItemScanLimitMax {
			n = adminItemScanLimitMax
		}
		opts.Limit = n
	}
	if raw := strings.TrimSpace(q.Get("next_cursor")); raw != "" {
		cursor, err := decodeAdminItemKeySegment2(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request",
				"next_cursor is not a valid base64-url encoded key map")
			return AdminScanItemsOptions{}, false
		}
		opts.ExclusiveStart = cursor
	}
	return opts, true
}

// decodeAdminItemKeySegment decodes the `{key}` URL segment into
// a primary-key attribute map. Two-step decode: base64-url →
// JSON object → map[string]AdminAttributeValue. Failures surface
// as 400 with a body that names the failure mode so SPA debugging
// is straightforward.
func decodeAdminItemKeySegment(w http.ResponseWriter, segment string) (map[string]AdminAttributeValue, bool) {
	out, err := decodeAdminItemKeySegment2(segment)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request",
			"key segment is not a valid base64-url encoded JSON attribute map: "+err.Error())
		return nil, false
	}
	if len(out) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid_request",
			"key segment decodes to an empty attribute map")
		return nil, false
	}
	return out, true
}

// decodeAdminItemKeySegment2 is the non-HTTP form used by both
// the key-segment decode and the next-cursor decode (both shapes
// are base64-url(JSON(map[string]AdminAttributeValue))).
func decodeAdminItemKeySegment2(segment string) (map[string]AdminAttributeValue, error) {
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		// Tolerate padded base64 as a fallback so a hand-crafted
		// curl with trailing `=` still works.
		raw2, err2 := base64.URLEncoding.DecodeString(segment)
		if err2 != nil {
			return nil, errors.New("invalid base64-url encoding: " + err.Error())
		}
		raw = raw2
	}
	var out map[string]AdminAttributeValue
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, errors.New("invalid JSON in decoded segment: " + err.Error())
	}
	return out, nil
}

// assertItemBodyMatchesPathKey rejects requests where the JSON
// body's primary-key attributes disagree with the {key} URL
// segment. The path segment is authoritative (S3-style: the URL
// names the resource), so a mismatch is a client bug we surface
// rather than silently letting the body win.
//
// The implementation compares only the key attributes named in
// the path segment — extra non-key attributes in the body are
// fine (they're the item's payload). A path key attribute that
// is missing from the body is the rejection case.
//
// Schema-awareness limit (Codex P2 on PR #813): this check cannot
// reject a body that declares MORE primary-key columns than the
// URL key, because the HTTP layer has no schema. On a hash+range
// table where the URL key omits the sort key, a PUT carrying a
// full (pk, sk) body succeeds — the item lands at (pk, sk), which
// is a different resource from the URL's `pk`-only identifier.
// The adapter's existing schema-aware key validation ensures the
// stored item's primary key is well-formed; only the URL contract
// is loose. The principled fix is to plumb the URL key into
// AdminPutItem as a separate argument so the adapter can compare
// body's schema-derived primary key against the URL key. That
// change crosses the Phase-2a (adapter) boundary and is tracked
// separately; the HTTP-only check here stays the URL ⊆ body
// subset rule.
func assertItemBodyMatchesPathKey(item AdminItem, pathKey map[string]AdminAttributeValue) error {
	if item.Attributes == nil {
		return errors.New("item body is missing the attributes field")
	}
	for k, want := range pathKey {
		got, ok := item.Attributes[k]
		if !ok {
			return errors.New("item body is missing the path key attribute " + strconv.Quote(k))
		}
		if !attributeValuesEqual(got, want) {
			return errors.New("item body's " + strconv.Quote(k) +
				" attribute does not match the value encoded in the path key segment")
		}
	}
	return nil
}

// attributeValuesEqual is a structural-equality check on
// AdminAttributeValue. Used only by the path-key vs body-key
// reconciliation above — pointers compare by dereferenced
// value, byte slices and string slices compare by element,
// recursive containers compare structurally.
//
// one branch per kind; collapsing further with reflection would
// be slower and obscure the AWS-wire-shape coupling.
//
//nolint:cyclop // ten attribute kinds × per-kind equality means
func attributeValuesEqual(a, b AdminAttributeValue) bool {
	if !stringPtrEqual(a.S, b.S) || !stringPtrEqual(a.N, b.N) {
		return false
	}
	if !boolPtrEqual(a.BOOL, b.BOOL) || !boolPtrEqual(a.NULL, b.NULL) {
		return false
	}
	if !bytes.Equal(a.B, b.B) {
		return false
	}
	if !slices.Equal(a.SS, b.SS) || !slices.Equal(a.NS, b.NS) {
		return false
	}
	if !slices.EqualFunc(a.BS, b.BS, bytes.Equal) {
		return false
	}
	if !slices.EqualFunc(a.L, b.L, attributeValuesEqual) {
		return false
	}
	if len(a.M) != len(b.M) {
		return false
	}
	for k, av := range a.M {
		bv, ok := b.M[k]
		if !ok || !attributeValuesEqual(av, bv) {
			return false
		}
	}
	return true
}

func stringPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// writeItemsError translates the sentinel errors the bridge
// surfaces into the canonical HTTP status + body shape.
//
// The 503 / 403 / 400 cases use fixed strings rather than
// err.Error() — the sentinels (e.g. "admin dynamo items:
// forbidden") are internal vocabulary and leaking them to clients
// (a) sticks the SPA with a confusing message and (b) drifts the
// items surface away from writeTablesError / writeBucketsError /
// the SQS handler which all use fixed strings. Claude review on
// PR #813 r2 caught both this and the Retry-After / canonical
// code drift on the 503 case.
func (h *DynamoHandler) writeItemsError(w http.ResponseWriter, r *http.Request, op, table string, err error) {
	switch {
	case errors.Is(err, ErrItemsForbidden):
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
	case errors.Is(err, ErrItemsNotLeader):
		// Retry-After: 1 + "leader_unavailable" matches every
		// other admin 503 (writeTablesError, writeBucketsError,
		// sqs_handler, forward_server). Without these the SPA's
		// shared retry helper does not back off correctly and
		// the error branches on the wrong code string.
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusServiceUnavailable, "leader_unavailable",
			"this admin node is not the raft leader")
	case errors.Is(err, ErrItemsTableNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", "table not found")
	case errors.Is(err, ErrItemsValidation):
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin dynamo "+op+" item failed",
			slog.String("table", table),
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "internal_error",
			"failed to "+op+" item; see server logs")
	}
}
