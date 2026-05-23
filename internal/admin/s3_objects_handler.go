package admin

import (
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Phase 3b — HTTP handlers for the object-level S3 admin surface.
// Driven by §3.3 / §3.1.2 of
// docs/design/2026_05_22_proposed_admin_data_browser.md.
//
// Routes owned by this file (dispatched from S3Handler.ServeHTTP
// once the /buckets/{name}/objects sub-tree is detected):
//
//	GET    /admin/api/v1/s3/buckets/{name}/objects                  — list
//	GET    /admin/api/v1/s3/buckets/{name}/objects/{key-b64url}     — download
//	PUT    /admin/api/v1/s3/buckets/{name}/objects/{key-b64url}     — upload
//	DELETE /admin/api/v1/s3/buckets/{name}/objects/{key-b64url}     — delete
//
// {key-b64url} is the base64-url-encoded raw object key. The list /
// describe / delete routes for buckets stay JSON; the per-object
// GET / PUT carry the object body as application/octet-stream (no
// JSON wrapper) so an arbitrary blob round-trips without an extra
// encode/decode pass.

// adminObjectSubResource is the literal second segment after the
// bucket name that activates the objects routes.
const adminObjectSubResource = "objects"

// adminObjectUploadCap is the per-request PUT body cap. Mirrors
// the adapter's per-object admin upload limit. Bodies exceeding
// this surface as 413 payload_too_large via http.MaxBytesReader.
const adminObjectUploadCap = 100 << 20

// List-page knobs. Default 100 mirrors the adapter's
// adminListObjectsDefaultMaxKeys; the cap of 1000 mirrors
// adminListObjectsMaxKeysCap. The SPA can override via ?max_keys.
const (
	defaultAdminObjectListMaxKeys = 100
	adminObjectListMaxKeysCap     = 1000
)

// AdminObject is the metadata projection the SPA receives on list
// pages and on the GET-object header set. The wire shape mirrors
// adapter.AdminObject field-for-field; the bridge in main_admin.go
// converts between the two so internal/admin stays adapter-free.
type AdminObject struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ContentType  string    `json:"content_type"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	StorageClass string    `json:"storage_class"`
}

// AdminListObjectsOptions controls one AdminListObjects call. Empty
// values pick the adapter-side defaults (MaxKeys clamps to
// [1, adminObjectListMaxKeysCap]).
type AdminListObjectsOptions struct {
	Prefix            string `json:"prefix,omitempty"`
	Delimiter         string `json:"delimiter,omitempty"`
	ContinuationToken string `json:"continuation_token,omitempty"`
	MaxKeys           int    `json:"max_keys,omitempty"`
}

// AdminObjectListing is the JSON response shape for the list page.
// CommonPrefixes is omitempty so a flat listing (no delimiter)
// returns just `{"objects":[...]}`. NextContinuationToken is
// non-empty only when the underlying scan stopped early.
type AdminObjectListing struct {
	Objects               []AdminObject `json:"objects"`
	CommonPrefixes        []string      `json:"common_prefixes,omitempty"`
	NextContinuationToken string        `json:"next_continuation_token,omitempty"`
}

// Sentinel errors the bridge translates Admin*Object adapter
// errors into, so the handler maps them to HTTP status without
// sniffing adapter-internal error vocabulary.

// ErrObjectsForbidden — principal lacks the required role. 403.
var ErrObjectsForbidden = errors.New("admin s3 objects: forbidden")

// ErrObjectsNotLeader — this node is not the Raft leader for the
// S3 group. 503 + Retry-After: 1; AdminForward will catch this once
// the object surface is wired through it.
var ErrObjectsNotLeader = errors.New("admin s3 objects: not leader")

// ErrObjectsBucketNotFound — the named bucket does not exist. 404.
var ErrObjectsBucketNotFound = errors.New("admin s3 objects: bucket not found")

// ErrObjectsNotFound — the named object does not exist. 404.
var ErrObjectsNotFound = errors.New("admin s3 objects: object not found")

// ErrObjectsValidation — empty / malformed key, malformed bucket
// name, oversized prefix/delimiter, invalid continuation token. 400.
var ErrObjectsValidation = errors.New("admin s3 objects: invalid request")

// ErrObjectsUploadTooLarge — PUT body exceeded the per-object cap.
// Mapped to 413 payload_too_large; the bridge translates the
// adapter's ErrAdminUploadTooLarge to this.
var ErrObjectsUploadTooLarge = errors.New("admin s3 objects: upload exceeds cap")

// ---------- handler entry points (called from S3Handler.ServeHTTP) ----------

// handleObjectsCollection serves GET /buckets/{name}/objects.
func (h *S3Handler) handleObjectsCollection(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
		return
	}
	principal, ok := h.principalForObjectRead(w, r)
	if !ok {
		return
	}
	opts, ok := parseAdminListObjectsQuery(w, r)
	if !ok {
		return
	}
	listing, err := h.source.AdminListObjects(r.Context(), principal, bucket, opts)
	if err != nil {
		h.writeObjectsError(w, r, "list", bucket, err)
		return
	}
	if listing.Objects == nil {
		listing.Objects = []AdminObject{}
	}
	writeAdminJSONStatus(w, r.Context(), h.logger, http.StatusOK, listing)
}

// handleObjectResource serves /buckets/{name}/objects/{key-b64url}
// (GET / PUT / DELETE). Body content type:
//   - GET response: application/octet-stream (raw object bytes)
//   - PUT request:  application/octet-stream (raw object bytes)
//   - DELETE: no body
func (h *S3Handler) handleObjectResource(w http.ResponseWriter, r *http.Request, bucket, keySegment string) {
	key, ok := decodeAdminObjectKeySegment(w, keySegment)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleObjectGet(w, r, bucket, key)
	case http.MethodPut:
		h.handleObjectPut(w, r, bucket, key)
	case http.MethodDelete:
		h.handleObjectDelete(w, r, bucket, key)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed",
			"only GET, PUT, or DELETE")
	}
}

func (h *S3Handler) handleObjectGet(w http.ResponseWriter, r *http.Request, bucket, key string) {
	principal, ok := h.principalForObjectRead(w, r)
	if !ok {
		return
	}
	body, meta, err := h.source.AdminGetObject(r.Context(), principal, bucket, key)
	if err != nil {
		h.writeObjectsError(w, r, "get", bucket, err)
		return
	}
	defer func() {
		if cerr := body.Close(); cerr != nil {
			h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin s3 get object body close failed",
				slog.String("bucket", bucket),
				slog.String("key", key),
				slog.String("error", cerr.Error()),
			)
		}
	}()
	writeAdminObjectGetHeaders(w, meta)
	if _, err := io.Copy(w, body); err != nil {
		// Connection-reset / client-disconnect mid-stream is common
		// (downloads cancelled in the browser). Log at warn rather
		// than error so an operator can still spot a sustained
		// pattern without alerts firing on every cancelled tab.
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin s3 get object body copy failed",
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
	}
}

func (h *S3Handler) handleObjectPut(w http.ResponseWriter, r *http.Request, bucket, key string) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	// Bound the body before any read; an oversized upload aborts
	// without buffering the whole payload. The 413 path is the
	// canonical admin payload_too_large response shape.
	limited := http.MaxBytesReader(w, r.Body, adminObjectUploadCap)
	defer func() {
		if cerr := limited.Close(); cerr != nil {
			h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin s3 put object body close failed",
				slog.String("bucket", bucket),
				slog.String("key", key),
				slog.String("error", cerr.Error()),
			)
		}
	}()
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if err := h.source.AdminPutObject(r.Context(), principal, bucket, key, limited, contentType); err != nil {
		// MaxBytesReader surfaces *http.MaxBytesError on the read,
		// which the adapter forwards as ErrAdminUploadTooLarge —
		// the bridge translates that to ErrObjectsUploadTooLarge.
		// Any other read-side error (client disconnect, etc.) flows
		// through the generic writeObjectsError path.
		var mbErr *http.MaxBytesError
		if errors.As(err, &mbErr) {
			WriteMaxBytesError(w)
			return
		}
		h.writeObjectsError(w, r, "put", bucket, err)
		return
	}
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("operation", "put_object"),
		slog.String("bucket", bucket),
		slog.String("key", key),
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleObjectDelete(w http.ResponseWriter, r *http.Request, bucket, key string) {
	principal, ok := h.principalForWrite(w, r)
	if !ok {
		return
	}
	if err := h.source.AdminDeleteObject(r.Context(), principal, bucket, key); err != nil {
		h.writeObjectsError(w, r, "delete", bucket, err)
		return
	}
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("operation", "delete_object"),
		slog.String("bucket", bucket),
		slog.String("key", key),
	)
	w.WriteHeader(http.StatusNoContent)
}

// ---------- response helpers ----------

// writeAdminObjectGetHeaders sets the security headers and the
// standard S3-style Content-* metadata for an object download.
//
// Security headers:
//   - X-Content-Type-Options: nosniff — prevents the browser from
//     overriding the Content-Type. An admin who downloads an HTML
//     file uploaded by a less-privileged operator must not have it
//     render in an admin-origin tab.
//   - Content-Security-Policy: sandbox — strips script execution,
//     plugins, and same-origin form posts from the response. Same
//     defence-in-depth angle as nosniff; covers UAs that miss
//     nosniff.
//   - Content-Disposition: attachment; filename="..."  — forces a
//     download dialog rather than inline rendering. The filename is
//     derived from the basename of the object key (slash-split,
//     last segment) and quoted-escaped.
//   - Cache-Control: no-store — admin downloads must not be cached
//     by intermediaries; revisiting a key after rotation should
//     fetch the current bytes.
func writeAdminObjectGetHeaders(w http.ResponseWriter, meta AdminObject) {
	ct := meta.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition",
		`attachment; filename="`+sanitizeContentDispositionFilename(objectKeyBasename(meta.Key))+`"`)
	if meta.ETag != "" {
		w.Header().Set("ETag", meta.ETag)
	}
	if !meta.LastModified.IsZero() {
		w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
	}
}

// objectKeyBasename returns the trailing slash-separated segment
// of key. An empty trailing segment (the key ends in `/`) folds to
// "object" so the Content-Disposition filename is never empty.
func objectKeyBasename(key string) string {
	idx := strings.LastIndex(key, "/")
	if idx >= 0 {
		key = key[idx+1:]
	}
	if key == "" {
		return "object"
	}
	return key
}

// sanitizeContentDispositionFilename strips characters that break
// the quoted-string form of the Content-Disposition value. We
// substitute control characters and the literal quote / backslash
// with `_` rather than percent-encoding because that round-trips
// cleanly through every browser's download dialog. Full RFC 5987
// `filename*=UTF-8”...` is out of scope; the SPA uses the
// download dialog as a sane default.
func sanitizeContentDispositionFilename(in string) string {
	if in == "" {
		return "object"
	}
	b := strings.Builder{}
	b.Grow(len(in))
	for _, r := range in {
		switch {
		case r < asciiSpace, r == asciiDelete, r == '"', r == '\\':
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ASCII control-character boundaries used by the filename sanitiser.
// Pulled out as named constants so mnd does not flag the inline
// literals.
const (
	asciiSpace  = 0x20
	asciiDelete = 0x7f
)

// ---------- principal helpers ----------

// principalForObjectRead is the read-side companion to principalForWrite
// for the object-content paths. Same live-role re-check as the SPA-
// facing endpoints — payload is object bytes, so AllowsRead (RoleReadOnly
// OR RoleFull) is the right gate.
func (h *S3Handler) principalForObjectRead(w http.ResponseWriter, r *http.Request) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no session principal")
		return AuthPrincipal{}, false
	}
	if h.roles != nil {
		live, exists := h.roles.LookupRole(principal.AccessKey)
		if !exists || !live.AllowsRead() {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this access key is not authorised to read object contents")
			return AuthPrincipal{}, false
		}
		principal.Role = live
	} else if !principal.Role.AllowsRead() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this access key is not authorised to read object contents")
		return AuthPrincipal{}, false
	}
	return principal, true
}

// ---------- query / key parsing ----------

// parseAdminListObjectsQuery pulls the read-only list knobs off the
// URL. Continuation tokens are opaque base64url blobs the adapter
// emits — we forward them verbatim without trying to parse the
// inner structure (rotating the adapter's token shape must not
// require an admin-handler update).
func parseAdminListObjectsQuery(w http.ResponseWriter, r *http.Request) (AdminListObjectsOptions, bool) {
	q := r.URL.Query()
	opts := AdminListObjectsOptions{
		Prefix:            q.Get("prefix"),
		Delimiter:         q.Get("delimiter"),
		ContinuationToken: q.Get("continuation_token"),
		MaxKeys:           defaultAdminObjectListMaxKeys,
	}
	if raw := strings.TrimSpace(q.Get("max_keys")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request",
				"max_keys must be a positive integer")
			return AdminListObjectsOptions{}, false
		}
		if n > adminObjectListMaxKeysCap {
			n = adminObjectListMaxKeysCap
		}
		opts.MaxKeys = n
	}
	return opts, true
}

// decodeAdminObjectKeySegment decodes the `{key}` URL segment into
// the raw object key. Base64-url is the wire shape; the encoded
// segment is what the route validator stamps as legal (no `%`, no
// dot-segments). An empty decoded key is treated as invalid since
// S3 itself rejects zero-length keys.
func decodeAdminObjectKeySegment(w http.ResponseWriter, segment string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		// Tolerate padded base64 as a fallback so a hand-crafted
		// curl with trailing `=` still works.
		raw2, err2 := base64.URLEncoding.DecodeString(segment)
		if err2 != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request",
				"key segment is not valid base64-url: "+err.Error())
			return "", false
		}
		raw = raw2
	}
	if len(raw) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid_request",
			"key segment decodes to an empty string")
		return "", false
	}
	return string(raw), true
}

// ---------- error translation ----------

// writeObjectsError translates the sentinel errors the bridge
// surfaces into the canonical HTTP status + body shape. Mirrors
// writeBucketsError but with the object-tier sentinels.
func (h *S3Handler) writeObjectsError(w http.ResponseWriter, r *http.Request, op, bucket string, err error) {
	switch {
	case errors.Is(err, ErrObjectsForbidden):
		writeJSONError(w, http.StatusForbidden, "forbidden", err.Error())
	case errors.Is(err, ErrObjectsNotLeader):
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusServiceUnavailable, "leader_unavailable",
			"this admin node is not the raft leader")
	case errors.Is(err, ErrObjectsBucketNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", "bucket does not exist")
	case errors.Is(err, ErrObjectsNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", "object does not exist")
	case errors.Is(err, ErrObjectsValidation):
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, ErrObjectsUploadTooLarge):
		WriteMaxBytesError(w)
	default:
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin s3 "+op+" object failed",
			slog.String("bucket", bucket),
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "s3_"+op+"_failed",
			"failed to "+op+" object; see server logs")
	}
}
