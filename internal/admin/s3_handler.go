package admin

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// Pagination knobs for the read-only S3 bucket list endpoint.
// Values mirror the Dynamo side (defaultDynamoListLimit /
// dynamoListLimitMax) so a SPA component that reuses the same
// "page size" preset behaves identically across both resources.
const (
	defaultS3ListLimit = 100
	s3ListLimitMax     = 1000
)

const (
	pathS3Buckets       = "/admin/api/v1/s3/buckets"
	pathPrefixS3Buckets = pathS3Buckets + "/"
)

// hlcPhysicalShift is the bit position at which the 48-bit physical
// half of the HLC timestamp lives. Mirrors kv.HLC's wire format
// (upper 48 bits = Unix ms, lower 16 bits = logical counter). Pulled
// out here so the formatter is self-contained — admin must not
// import kv to read the field.
const hlcPhysicalShift = 16

// pathSuffixACL is the trailing segment of /s3/buckets/{name}/acl.
// Pulled out as a constant so the route switch matches a typed
// suffix rather than an inline literal — drop the helper if the
// per-bucket sub-resources ever grow beyond this single member.
const pathSuffixACL = "/acl"

// S3Handler serves /admin/api/v1/s3/buckets and the
// /admin/api/v1/s3/buckets/{name}{,/acl} sub-tree. Construct via
// NewS3Handler and hand to the admin router.
//
// The handler depends on a BucketsSource for in-process dispatch.
// When source is nil the constructor returns nil, which is the
// well-known "S3 admin disabled" signal the router keys off of
// (the routes fall through to the unknown-endpoint 404).
//
// Writes (POST / PUT / DELETE) re-validate the principal against a
// live RoleStore on every request — the JWT freezes the role at
// login but a downgraded or revoked key must not be allowed to
// continue mutating until the token expires (Codex P1 on PR #635
// applied the same fix on the Dynamo side).
type S3Handler struct {
	source BucketsSource
	logger *slog.Logger
	roles  RoleStore
}

// NewS3Handler wires a BucketsSource into the HTTP handler. Returns
// nil when source is nil so a build that ships without the S3
// adapter can pass the zero value to ServerDeps and have the routes
// silently disappear from the wire — matching the Tables nil
// contract on the Dynamo side.
func NewS3Handler(source BucketsSource) *S3Handler {
	if source == nil {
		return nil
	}
	return &S3Handler{source: source, logger: slog.Default()}
}

// WithLogger swaps the slog destination. Returns the receiver so
// option calls chain at construction sites
// (NewS3Handler(...).WithLogger(...).WithRoleStore(...)).
func (h *S3Handler) WithLogger(logger *slog.Logger) *S3Handler {
	if logger != nil {
		h.logger = logger
	}
	return h
}

// WithRoleStore wires the live access-key → role lookup. Required
// for the write endpoints' role re-validation; safe to omit on
// builds that disable writes (NewServer ensures it is always set
// when ServerDeps.Buckets is wired).
func (h *S3Handler) WithRoleStore(roles RoleStore) *S3Handler {
	h.roles = roles
	return h
}

// ServeHTTP routes /buckets, /buckets/{name}, and /buckets/{name}/acl.
func (h *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == pathS3Buckets:
		h.serveCollection(w, r)
	case strings.HasPrefix(r.URL.Path, pathPrefixS3Buckets):
		h.servePerBucket(w, r)
	default:
		writeJSONError(w, http.StatusNotFound, "not_found", "")
	}
}

func (h *S3Handler) serveCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleList(w, r)
	case http.MethodPost:
		h.handleCreate(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed",
			"only GET or POST is allowed on /s3/buckets")
	}
}

// servePerBucket dispatches /s3/buckets/{name} and the single
// sub-resource /s3/buckets/{name}/acl. Any other sub-path 404s so a
// SPA bug pointed at a hypothetical /buckets/{name}/policy or
// similar sees an unambiguous "no handler" rather than mistakenly
// hitting the describe path with a "{name}/policy" string. The
// pinned test is TestS3Handler_DescribeBucket_SubpathReturns404
// (CodeRabbit minor on PR #658 caught a prior version of this
// comment that mistakenly said "405").
func (h *S3Handler) servePerBucket(w http.ResponseWriter, r *http.Request) {
	tail := strings.TrimPrefix(r.URL.Path, pathPrefixS3Buckets)
	if tail == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_bucket_name",
			"bucket name is empty")
		return
	}
	if strings.HasSuffix(tail, pathSuffixACL) {
		name := strings.TrimSuffix(tail, pathSuffixACL)
		if name == "" || strings.Contains(name, "/") {
			writeJSONError(w, http.StatusNotFound, "not_found",
				"no admin S3 handler is registered for this path")
			return
		}
		if r.Method != http.MethodPut {
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				"only PUT is allowed on /s3/buckets/{name}/acl")
			return
		}
		h.handlePutACL(w, r, name)
		return
	}
	if strings.Contains(tail, "/") {
		writeJSONError(w, http.StatusNotFound, "not_found",
			"no admin S3 handler is registered for this path")
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleDescribe(w, r, tail)
	case http.MethodDelete:
		h.handleDelete(w, r, tail)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed",
			"only GET or DELETE is allowed on /s3/buckets/{name}")
	}
}

// s3ListResponse is the JSON shape returned by GET /s3/buckets.
// Buckets is always emitted as `[]` even when empty so the SPA
// can use a presence check rather than guard against null.
type s3ListResponse struct {
	Buckets   []BucketSummary `json:"buckets"`
	NextToken string          `json:"next_token,omitempty"`
}

func (h *S3Handler) handleList(w http.ResponseWriter, r *http.Request) {
	limit, startAfter, ok := parseListPaginationParams(w, r, defaultS3ListLimit, s3ListLimitMax)
	if !ok {
		return
	}
	buckets, err := h.source.AdminListBuckets(r.Context())
	if err != nil {
		// Map ErrBucketsForbidden to 403 here too so the contract
		// stays symmetric with handleDescribe — when slice 2 wires
		// a role gate on the source, the SPA gets the same 403 it
		// would on the describe path rather than a generic 500
		// (Gemini medium on PR #658).
		if errors.Is(err, ErrBucketsForbidden) {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this endpoint requires a full-access role")
			return
		}
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin s3 list buckets failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "s3_list_failed",
			"failed to list buckets; see server logs")
		return
	}
	page, next := paginateBuckets(buckets, startAfter, limit)
	resp := s3ListResponse{Buckets: page}
	if next != "" {
		resp.NextToken = encodeListNextToken(next)
	}
	writeAdminJSON(w, r.Context(), h.logger, resp)
}

func (h *S3Handler) handleDescribe(w http.ResponseWriter, r *http.Request, name string) {
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_bucket_name", "bucket name is empty")
		return
	}
	summary, exists, err := h.source.AdminDescribeBucket(r.Context(), name)
	if err != nil {
		// Differentiate the two structured failures we expect:
		//   - ErrBucketsForbidden: the bridge translated an adapter-
		//     side authorization rejection. 403.
		//   - everything else: a real storage failure. 500 + log.
		if errors.Is(err, ErrBucketsForbidden) {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this endpoint requires a full-access role")
			return
		}
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin s3 describe bucket failed",
			slog.String("bucket", name),
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "s3_describe_failed",
			"failed to describe bucket; see server logs")
		return
	}
	if !exists {
		writeJSONError(w, http.StatusNotFound, "not_found", "bucket does not exist")
		return
	}
	writeAdminJSON(w, r.Context(), h.logger, summary)
}

// FormatBucketCreatedAt converts an HLC timestamp into the ISO-8601
// string the SPA expects. Exposed (rather than kept package-private)
// so the bridge in main_admin.go can call it from the BucketsSource
// implementation — both the handler's response and any future
// audit-log enrichment land on identical formatting.
func FormatBucketCreatedAt(hlc uint64) string {
	if hlc == 0 {
		return ""
	}
	ms := int64(hlc >> hlcPhysicalShift) //nolint:gosec // 48-bit physical half always fits in int64.
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

// adminS3CreateBodyLimit is the per-request body cap for POST
// /s3/buckets and PUT /s3/buckets/{name}/acl. Matches design 4.4
// (64 KiB hard cap on every admin POST/PUT). The wrapping BodyLimit
// middleware also enforces a 64 KiB cap; this constant is the
// in-handler MaxBytesReader limit so an oversized body produces
// errCreateBodyTooLarge before json.Decode bails on the truncation.
const adminS3CreateBodyLimit = 64 << 10

// errAdminS3BodyTooLarge is the sentinel decodeAdminS3JSONBody
// returns when the request body trips MaxBytesReader. The handler
// matches it to write 413 + the standard payload_too_large code,
// distinct from the generic 400 invalid_body that other decode
// failures produce.
var errAdminS3BodyTooLarge = errors.New("request body exceeds the 64 KiB admin limit")

func (h *S3Handler) handleCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	body, err := decodeAdminS3JSONBody[CreateBucketRequest](r.Body)
	if err != nil {
		if errors.Is(err, errAdminS3BodyTooLarge) {
			WriteMaxBytesError(w)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if err := validateCreateBucketRequest(body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	summary, err := h.source.AdminCreateBucket(r.Context(), principal, body)
	if err != nil {
		h.writeBucketsError(w, r, "create", err)
		return
	}
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("operation", "create_bucket"),
		slog.String("bucket", body.BucketName),
	)
	writeAdminJSONStatus(w, r.Context(), h.logger, http.StatusCreated, summary)
}

func (h *S3Handler) handlePutACL(w http.ResponseWriter, r *http.Request, name string) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	body, err := decodeAdminS3JSONBody[PutBucketACLRequest](r.Body)
	if err != nil {
		if errors.Is(err, errAdminS3BodyTooLarge) {
			WriteMaxBytesError(w)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if strings.TrimSpace(body.ACL) == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_body", "acl is required")
		return
	}
	if err := h.source.AdminPutBucketAcl(r.Context(), principal, name, body.ACL); err != nil {
		h.writeBucketsError(w, r, "put_acl", err)
		return
	}
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("operation", "put_bucket_acl"),
		slog.String("bucket", name),
		slog.String("acl", body.ACL),
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleDelete(w http.ResponseWriter, r *http.Request, name string) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	if err := h.source.AdminDeleteBucket(r.Context(), principal, name); err != nil {
		h.writeBucketsError(w, r, "delete", err)
		return
	}
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("operation", "delete_bucket"),
		slog.String("bucket", name),
	)
	w.WriteHeader(http.StatusNoContent)
}

// principalForWrite mirrors DynamoHandler.principalForWrite: pull
// the JWT principal out of the request context, re-resolve the role
// against the live RoleStore, and reject anything below Full. Even
// a still-valid JWT with role=full does not get a free pass —
// operators who revoke an access key get the change picked up on
// the next admin write rather than waiting for the JWT to expire.
func (h *S3Handler) principalForWrite(w http.ResponseWriter, r *http.Request) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated",
			"no session principal")
		return AuthPrincipal{}, false
	}
	if h.roles == nil {
		// Production wiring always sets a RoleStore; nil is a test
		// fallthrough we accept so single-handler unit tests can
		// reach the source without standing up the auth chain.
		if !principal.Role.AllowsWrite() {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this endpoint requires a full-access role")
			return AuthPrincipal{}, false
		}
		return principal, true
	}
	role, found := h.roles.LookupRole(principal.AccessKey)
	if !found || !role.AllowsWrite() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
		return AuthPrincipal{}, false
	}
	return AuthPrincipal{AccessKey: principal.AccessKey, Role: role}, true
}

// writeBucketsError translates a BucketsSource error into the
// appropriate HTTP response. Internal-server-error fallthrough logs
// the raw err.Error() but never sends it to the client, matching
// the Dynamo side's writeTablesError policy.
func (h *S3Handler) writeBucketsError(w http.ResponseWriter, r *http.Request, op string, err error) {
	switch {
	case errors.Is(err, ErrBucketsForbidden):
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
	case errors.Is(err, ErrBucketsNotLeader):
		// Reached only when no LeaderForwarder is configured (single-
		// node or leader-only deployments). When the next slice's
		// AdminForward integration ships, the forwarder will catch
		// this before writeBucketsError is called.
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusServiceUnavailable, "leader_unavailable",
			"this admin node is not the raft leader")
	case errors.Is(err, ErrBucketsNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", "bucket does not exist")
	case errors.Is(err, ErrBucketsAlreadyExists):
		writeJSONError(w, http.StatusConflict, "already_exists", "bucket already exists")
	case errors.Is(err, ErrBucketsNotEmpty):
		writeJSONError(w, http.StatusConflict, "bucket_not_empty",
			"bucket still has objects; remove them and retry")
	default:
		var verr *ValidationError
		if errors.As(err, &verr) {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", verr.Error())
			return
		}
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin s3 "+op+" bucket failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "s3_"+op+"_failed",
			"failed to "+op+" bucket; see server logs")
	}
}

// validateCreateBucketRequest applies the lightweight client-side
// guard rails. Bucket-name format checks happen in the adapter
// (validateS3BucketName) — we only catch the obvious mistakes
// here so the SPA gets a typed 400 with a clear message rather
// than the more generic adapter-level wrapping.
func validateCreateBucketRequest(in CreateBucketRequest) error {
	if strings.TrimSpace(in.BucketName) == "" {
		return errors.New("bucket_name is required")
	}
	if in.BucketName != strings.TrimSpace(in.BucketName) {
		return errors.New("bucket_name must not have leading or trailing whitespace")
	}
	return nil
}

// decodeAdminS3JSONBody is the shared decoder for POST /s3/buckets
// and PUT /s3/buckets/{name}/acl. Strict (DisallowUnknownFields,
// trailing-token rejection, NUL-byte rejection) so a future
// schema change does not silently accept extra keys.
func decodeAdminS3JSONBody[T any](body io.Reader) (T, error) {
	var zero T
	if body == nil {
		return zero, errors.New("request body is empty")
	}
	limited := http.MaxBytesReader(nil, io.NopCloser(body), adminS3CreateBodyLimit)
	raw, err := io.ReadAll(limited)
	if err != nil {
		var me *http.MaxBytesError
		if errors.As(err, &me) {
			return zero, errAdminS3BodyTooLarge
		}
		return zero, errors.New("failed to read request body")
	}
	if len(raw) == 0 {
		// httptest passes http.NoBody for a nil-body request, which
		// reads as an empty byte slice rather than an error. The
		// caller's contract is "missing body → 400 invalid_body /
		// 'empty'", so surface that explicitly here rather than
		// letting the JSON decoder bubble up a generic
		// "not valid JSON" error.
		return zero, errors.New("request body is empty")
	}
	if bytes.IndexByte(raw, 0) >= 0 {
		// goccy/go-json treats raw NUL as end-of-input; reject so a
		// payload like `{"acl":"private"}\x00{"x":1}` cannot smuggle
		// a second JSON object past dec.More(). Codex P2 on PR #635
		// caught the same vector on the leader-side decoder.
		return zero, errors.New("request body contains a NUL byte")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var out T
	if err := dec.Decode(&out); err != nil {
		return zero, errors.New("request body is not valid JSON")
	}
	if dec.More() {
		return zero, errors.New("request body has trailing data after the first JSON value")
	}
	return out, nil
}

// paginateBuckets slices `buckets` (already lex-sorted by the
// adapter) into a single page starting strictly after `startAfter`.
// Returns the page plus the opaque cursor for the next call ("" if
// this was the last page).
//
// Mirrors paginateDynamoTableNames but operates on []BucketSummary
// rather than []string. A generic helper would force callers to
// write a key-extractor closure on every call site, which obscures
// the resume contract more than the four-line copy clarifies it.
func paginateBuckets(buckets []BucketSummary, startAfter string, limit int) ([]BucketSummary, string) {
	start := 0
	if startAfter != "" {
		idx := sort.Search(len(buckets), func(i int) bool { return buckets[i].Name >= startAfter })
		switch {
		case idx >= len(buckets):
			return []BucketSummary{}, ""
		case buckets[idx].Name == startAfter:
			start = idx + 1
		default:
			start = idx
		}
	}
	end := start + limit
	if end > len(buckets) {
		end = len(buckets)
	}
	// A slice expression on a non-nil slice is itself non-nil even
	// when its length is zero, so the result already produces the
	// `"buckets":[]` JSON shape the SPA expects without an extra
	// nil-guard (Claude Issue 2 on PR #658).
	page := buckets[start:end]
	if end < len(buckets) && len(page) > 0 {
		return page, page[len(page)-1].Name
	}
	return page, ""
}
