package admin

import (
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"
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

// S3Handler serves /admin/api/v1/s3/buckets and the
// /admin/api/v1/s3/buckets/{name} sub-tree. Construct via
// NewS3Handler and hand to the admin router.
//
// The handler depends on a BucketsSource for in-process dispatch.
// When source is nil the constructor returns nil, which is the
// well-known "S3 admin disabled" signal the router keys off of
// (the routes fall through to the unknown-endpoint 404).
//
// Slice 1 ships only the read-only paths (list + describe). The
// next slice will add a RoleStore for live role re-validation on
// the write endpoints (mirrors DynamoHandler.WithRoleStore).
type S3Handler struct {
	source BucketsSource
	logger *slog.Logger
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
// (NewS3Handler(...).WithLogger(...)).
func (h *S3Handler) WithLogger(logger *slog.Logger) *S3Handler {
	if logger != nil {
		h.logger = logger
	}
	return h
}

// ServeHTTP routes /buckets and /buckets/{name}. The next slice
// wires POST/PUT/DELETE; for now those return 405 so the SPA can
// distinguish "endpoint not configured" (404) from "method not
// implemented yet" (405).
func (h *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == pathS3Buckets:
		switch r.Method {
		case http.MethodGet:
			h.handleList(w, r)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET is implemented for /s3/buckets in this build")
		}
	case strings.HasPrefix(r.URL.Path, pathPrefixS3Buckets):
		name := strings.TrimPrefix(r.URL.Path, pathPrefixS3Buckets)
		// /buckets/{name}/acl is reserved for the next slice. Reject
		// it with 405 here so a SPA bug that calls PUT /acl on this
		// build sees a sensible error instead of mistakenly hitting
		// the describe path.
		if strings.Contains(name, "/") {
			writeJSONError(w, http.StatusNotFound, "not_found", "")
			return
		}
		switch r.Method {
		case http.MethodGet:
			h.handleDescribe(w, r, name)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET is implemented for /s3/buckets/{name} in this build")
		}
	default:
		writeJSONError(w, http.StatusNotFound, "not_found", "")
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
	page := buckets[start:end]
	if end < len(buckets) && len(page) > 0 {
		return page, page[len(page)-1].Name
	}
	if page == nil {
		return []BucketSummary{}, ""
	}
	return page, ""
}
