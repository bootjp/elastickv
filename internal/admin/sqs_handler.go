package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// pathSqsQueues is the URL prefix the SQS handler owns. The "" suffix
// produces the collection root /admin/api/v1/sqs/queues; the
// pathPrefixSqsQueues form is used for the per-queue routes.
const (
	pathSqsQueues       = "/admin/api/v1/sqs/queues"
	pathPrefixSqsQueues = pathSqsQueues + "/"
)

// QueueSummary is the SPA-facing projection of a single SQS queue.
// Mirrors adapter.AdminQueueSummary 1:1; the bridge in main_admin.go
// translates between the two so the admin package stays free of the
// adapter dependency tree.
//
// CreatedAt is a pointer so omitempty actually drops the field when
// the underlying queue has no wall-clock creation timestamp. Both
// encoding/json and goccy/go-json serialise a zero time.Time value
// as "0001-01-01T00:00:00Z" rather than dropping it, so the SPA
// would render an ancient date instead of the "—" placeholder its
// `created_at ? formatted : "—"` guard implies. The pointer makes
// the absent-vs-zero distinction explicit on the wire.
type QueueSummary struct {
	Name       string            `json:"name"`
	IsFIFO     bool              `json:"is_fifo"`
	Generation uint64            `json:"generation"`
	CreatedAt  *time.Time        `json:"created_at,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Counters   QueueCounters     `json:"counters"`
	// IsDLQ is true when at least one other queue's RedrivePolicy
	// resolves its deadLetterTargetArn to this queue. The SPA uses
	// the flag to switch the Messages-tab framing and the Purge
	// button label between "Purge messages" and "Purge DLQ".
	IsDLQ bool `json:"is_dlq"`
	// DLQSources lists the names of queues whose RedrivePolicy
	// points at this queue, sorted lexicographically. Empty when
	// IsDLQ is false; the SPA renders these as chips on the queue
	// detail page so the operator confirms what feeds the DLQ
	// before purging.
	DLQSources []string `json:"dlq_sources,omitempty"`
}

// QueueCounters mirrors the three Approximate* counters AWS exposes
// on GetQueueAttributes. Definitions follow §16.1 of the SQS design
// doc.
type QueueCounters struct {
	Visible    int64 `json:"visible"`
	NotVisible int64 `json:"not_visible"`
	Delayed    int64 `json:"delayed"`
}

// QueuesSource is the contract the SQS handler depends on. Wired in
// production to *adapter.SQSServer via a small bridge in main_admin.go;
// tests use a stub.
//
// AdminDescribeQueue returns (nil, false, nil) for a missing queue so
// callers can distinguish "not found" from a storage error without
// sniffing sentinels. AdminDeleteQueue / AdminPeekQueue / AdminPurgeQueue
// return the structured sentinels below so the handler can map them
// to HTTP statuses without leaking the adapter's error vocabulary.
type QueuesSource interface {
	AdminListQueues(ctx context.Context) ([]string, error)
	AdminDescribeQueue(ctx context.Context, name string) (*QueueSummary, bool, error)
	AdminDeleteQueue(ctx context.Context, principal AuthPrincipal, name string) error
	// AdminPeekQueue returns a non-destructive sample of currently-
	// visible messages on the queue (read role required). Wired only
	// when the source supports it; the bridge in main_admin.go
	// translates between adapter and admin types so the admin package
	// stays free of the adapter dependency tree.
	AdminPeekQueue(ctx context.Context, principal AuthPrincipal, name string, opts PeekMessageOptions) (PeekResult, error)
	// AdminPurgeQueue is the SigV4-bypass purge counterpart to
	// AdminDeleteQueue: bumps the queue's generation so every
	// message becomes unreachable, leaving the queue itself in place.
	// Returns the committed generation pair so the audit log records
	// the value that actually landed.
	AdminPurgeQueue(ctx context.Context, principal AuthPrincipal, name string) (PurgeResult, error)
}

// PeekMessageOptions controls a peek call. Field defaults match the
// adapter's documented bounds (Limit clamped to [1, 100] with 0
// meaning "default of 20"; BodyMaxBytes clamped to [256, 262144]
// with 0 meaning "default of 4096"; Cursor empty means "start from
// the front").
type PeekMessageOptions struct {
	Limit        int
	Cursor       string
	BodyMaxBytes int
}

// PeekResult is the admin-package projection of the adapter's
// AdminPeekQueue 3-tuple return (rows, nextCursor, error) bundled
// into one value so QueuesSource's method signatures stay regular.
// The handler renders this directly as the wire JSON.
type PeekResult struct {
	Messages   []PeekedMessage `json:"messages"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

// PeekedMessage is one row in the peek response. JSON tags pin the
// snake_case wire shape the design doc §3.5 specifies.
type PeekedMessage struct {
	MessageID        string                     `json:"message_id"`
	Body             string                     `json:"body"`
	BodyTruncated    bool                       `json:"body_truncated"`
	BodyOriginalSize int64                      `json:"body_original_size"`
	SentTimestamp    time.Time                  `json:"sent_timestamp"`
	ReceiveCount     int32                      `json:"receive_count"`
	GroupID          string                     `json:"group_id,omitempty"`
	DeduplicationID  string                     `json:"deduplication_id,omitempty"`
	Attributes       map[string]PeekedAttribute `json:"attributes,omitempty"`
}

// PeekedAttribute mirrors the typed SQS MessageAttribute shape so
// binary payloads and the DataType discriminator survive the round
// trip to the SPA.
type PeekedAttribute struct {
	DataType    string `json:"data_type"`
	StringValue string `json:"string_value,omitempty"`
	BinaryValue []byte `json:"binary_value,omitempty"`
}

// PurgeResult carries the committed-OCC generation pair so the
// admin handler's audit line records the value that actually landed
// (separate before/after meta reads would race a concurrent purge).
// JSON tags are pinned even though the current Phase 4 handler
// returns 204 with no body — the Phase 5 audit log will record the
// generation pair and a future wire encoder needs the snake_case
// shape (Claude r1 on PR #797 flagged the pre-emptive fix).
type PurgeResult struct {
	GenerationBefore uint64 `json:"generation_before"`
	GenerationAfter  uint64 `json:"generation_after"`
}

// Errors the QueuesSource may return for the handler to map onto a
// specific HTTP response. Sentinels rather than typed errors so the
// bridge can map any adapter-internal failure onto exactly one of
// these without the admin package importing adapter-private types.
var (
	// ErrQueuesForbidden — principal lacks the role required (403).
	ErrQueuesForbidden = errors.New("admin sqs: principal lacks required role")
	// ErrQueuesNotLeader — local node is not the verified Raft
	// leader. Without follower-forwarding wired (out of scope for
	// the SPA's read+delete surface), maps to 503 + Retry-After: 1.
	ErrQueuesNotLeader = errors.New("admin sqs: local node is not the raft leader")
	// ErrQueuesNotFound — DELETE / DESCRIBE targets a queue that
	// does not exist (404). The describe path uses (nil, false, nil)
	// instead of this sentinel for the not-found signal.
	ErrQueuesNotFound = errors.New("admin sqs: queue not found")
	// ErrQueuesValidation — request shape is bad (400).
	ErrQueuesValidation = errors.New("admin sqs: validation failed")
	// ErrQueuesPurgeInProgress — the queue's 60-second
	// PurgeQueue cooldown is active (429). The handler matches
	// against this sentinel and pulls RetryAfter from a typed
	// PurgeInProgressError wrapping it, so callers can branch via
	// errors.Is while extracting the duration via errors.As.
	ErrQueuesPurgeInProgress = errors.New("admin sqs: purge in progress")
)

// PurgeInProgressError is the typed admin error returned when the
// queue's 60-second PurgeQueue rate limit is active. RetryAfter
// carries the remaining wall-clock duration the caller should wait,
// derived from the same LastPurgedAtMillis value the adapter's
// rate-limit check observed inside its OCC read.
type PurgeInProgressError struct {
	RetryAfter time.Duration
}

func (e *PurgeInProgressError) Error() string {
	return "admin sqs: purge already in progress; retry after " + e.RetryAfter.String()
}

// Is lets errors.Is(err, ErrQueuesPurgeInProgress) match any
// *PurgeInProgressError so the handler can branch on the sentinel
// while errors.As pulls the typed duration.
func (e *PurgeInProgressError) Is(target error) bool {
	return target == ErrQueuesPurgeInProgress
}

// SqsHandler serves /admin/api/v1/sqs/queues and
// /admin/api/v1/sqs/queues/{name}. Reads (list, describe) accept GET;
// delete accepts DELETE and goes through the same protected
// middleware chain (BodyLimit -> SessionAuth -> Audit -> CSRF) as
// every other write surface, with an in-handler RoleFull gate so a
// read-only key cannot delete even with a valid CSRF token.
type SqsHandler struct {
	source QueuesSource
	roles  RoleStore
	logger *slog.Logger
}

// NewSqsHandler binds the source and seeds logging with
// slog.Default(). Use WithLogger to attach a tagged logger and
// WithRoleStore to plug in the live access-key role lookup so a
// downgraded key cannot continue mutating with a still-valid JWT.
func NewSqsHandler(source QueuesSource) *SqsHandler {
	return &SqsHandler{source: source, logger: slog.Default()}
}

// WithLogger overrides the default slog destination. No-ops on nil to
// preserve the constructor-seeded slog.Default().
func (h *SqsHandler) WithLogger(l *slog.Logger) *SqsHandler {
	if l == nil {
		return h
	}
	h.logger = l
	return h
}

// WithRoleStore enables per-request role revalidation on the delete
// endpoint. Without it, the handler trusts whatever role is embedded
// in the session JWT — which is fine for single-tenant deployments
// where the role config never changes, but problematic when an
// operator revokes or downgrades a key. Production wiring in
// main_admin.go always sets this.
func (h *SqsHandler) WithRoleStore(r RoleStore) *SqsHandler {
	h.roles = r
	return h
}

// ServeHTTP routes the SQS admin paths:
//
//	GET    /admin/api/v1/sqs/queues                       — list
//	GET    /admin/api/v1/sqs/queues/{name}                — describe
//	DELETE /admin/api/v1/sqs/queues/{name}                — delete
//	GET    /admin/api/v1/sqs/queues/{name}/messages       — peek
//	DELETE /admin/api/v1/sqs/queues/{name}/messages       — purge
//
// Routing follows the 6-step ordered procedure documented in
// docs/design/2026_05_16_..._admin_purge_queue.md §3.4. The procedure
// closes confused-deputy classes (empty / %2F / %252F / dot-segment)
// that would otherwise let a crafted URL route to the wrong queue or
// the wrong sub-resource.
// sqsSubResourceMessages is the recognised sub-resource segment on
// /queues/{name}/{sub}. Anything else is a 404. Lifted to a constant
// so the dispatcher and any future routing layer stay aligned.
const sqsSubResourceMessages = "messages"

// sqsRouteSegmentsQueue / sqsRouteSegmentsMessages are the segment
// counts the dispatcher recognises after the prefix trim. Named to
// keep the dispatch switch self-documenting.
const (
	sqsRouteSegmentsQueue    = 1
	sqsRouteSegmentsMessages = 2
)

func (h *SqsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	escaped := r.URL.EscapedPath()
	if h.serveCollectionRoot(w, r, escaped) {
		return
	}
	if !strings.HasPrefix(escaped, pathPrefixSqsQueues) {
		writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
			"no admin SQS handler is registered for this path")
		return
	}
	segments, ok := h.parseSqsRouteSegments(w, escaped)
	if !ok {
		return
	}
	h.dispatchSqsRoute(w, r, segments)
}

// serveCollectionRoot handles the .../queues and .../queues/ paths
// before any splitting (Step 2 of the routing procedure). Returns
// true when the request was handled (the caller short-circuits).
// strings.TrimPrefix returns its input unchanged when the prefix
// does not match and strings.Split("", "/") returns a one-element
// slice — handling the collection root pre-split sidesteps both
// corner cases.
func (h *SqsHandler) serveCollectionRoot(w http.ResponseWriter, r *http.Request, escaped string) bool {
	if escaped != pathSqsQueues && escaped != pathPrefixSqsQueues {
		return false
	}
	switch r.Method {
	case http.MethodGet:
		h.handleList(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
	}
	return true
}

// parseSqsRouteSegments runs Steps 3 to 5 of the routing procedure
// — trim the prefix, split, drop the single trailing empty, validate
// each segment (rejecting empty / % / dot-segments), then run
// path.Clean for trailing-slash normalisation. Returns the cleaned
// segment slice or false after writing a 400 invalid_path response.
func (h *SqsHandler) parseSqsRouteSegments(w http.ResponseWriter, escaped string) ([]string, bool) {
	rest := strings.TrimPrefix(escaped, pathPrefixSqsQueues)
	segments := dropSingleTrailingEmpty(strings.Split(rest, "/"))
	for _, seg := range segments {
		if !isValidSqsPathSegment(seg) {
			writeJSONError(w, http.StatusBadRequest, "invalid_path",
				"path segment is empty, contains %, or is a dot-segment")
			return nil, false
		}
	}
	cleaned := strings.TrimPrefix(path.Clean(pathPrefixSqsQueues+rest), pathPrefixSqsQueues)
	return dropSingleTrailingEmpty(strings.Split(cleaned, "/")), true
}

// dropSingleTrailingEmpty removes one trailing empty segment so a
// legal trailing slash (/queues/orders/messages/) does not collide
// with the empty-segment rejection in step 4. Never drops an
// interior empty: /queues/orders//messages still fails validation.
func dropSingleTrailingEmpty(segments []string) []string {
	if len(segments) > 1 && segments[len(segments)-1] == "" {
		return segments[:len(segments)-1]
	}
	return segments
}

// dispatchSqsRoute is the post-validation router. Segment count tells
// us which resource family the request targets; the per-resource
// dispatcher checks the HTTP method.
func (h *SqsHandler) dispatchSqsRoute(w http.ResponseWriter, r *http.Request, segments []string) {
	name, err := decodeSqsPathSegment(segments[0])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_path",
			"queue name segment is not valid percent-encoding")
		return
	}
	switch len(segments) {
	case sqsRouteSegmentsQueue:
		h.dispatchQueueResource(w, r, name)
	case sqsRouteSegmentsMessages:
		sub, err := decodeSqsPathSegment(segments[1])
		if err != nil || sub != sqsSubResourceMessages {
			writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
				"unknown sub-resource on this queue")
			return
		}
		h.dispatchMessagesResource(w, r, name)
	default:
		writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
			"too many path segments")
	}
}

// dispatchQueueResource handles the .../queues/{name} endpoint:
// GET = describe, DELETE = delete.
func (h *SqsHandler) dispatchQueueResource(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		h.handleDescribe(w, r, name)
	case http.MethodDelete:
		h.handleDelete(w, r, name)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or DELETE")
	}
}

// dispatchMessagesResource handles the .../queues/{name}/messages
// endpoint: GET = peek, DELETE = purge.
func (h *SqsHandler) dispatchMessagesResource(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		h.handlePeek(w, r, name)
	case http.MethodDelete:
		h.handlePurge(w, r, name)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or DELETE")
	}
}

// isValidSqsPathSegment enforces the step-4 rules. Every segment is
// rejected if it is empty, contains a percent sign (closes the
// %2F / %252F / %2e / %2E / %2E%2E percent-encoded slash and
// dot-segment classes structurally without enumerating depths), or
// is one of the literal dot-segment strings . / .. (closes the raw
// dot-segment traversal class — path.Clean would otherwise collapse
// .. against the preceding segment and dispatch on the wrong path).
func isValidSqsPathSegment(seg string) bool {
	if seg == "" {
		return false
	}
	if strings.ContainsRune(seg, '%') {
		return false
	}
	if seg == "." || seg == ".." {
		return false
	}
	return true
}

// decodeSqsPathSegment is a small wrapper over url.PathUnescape. The
// step-4 validation rejects every % in raw segments, so by the time
// this runs the segment is guaranteed to be percent-free and the
// decoder is a no-op. Kept as a function so a future relaxation can
// turn it back on without touching the routing logic.
func decodeSqsPathSegment(seg string) (string, error) {
	return seg, nil
}

type listQueuesResponse struct {
	Queues []string `json:"queues"`
}

func (h *SqsHandler) handleList(w http.ResponseWriter, r *http.Request) {
	names, err := h.source.AdminListQueues(r.Context())
	if err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin sqs list queues failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "list_failed",
			"failed to list queues; see server logs")
		return
	}
	// Force the empty-result case to render as `{"queues": []}` rather
	// than `{"queues": null}`. The SPA iterates the array directly and
	// would crash on null. AdminListQueues returns nil when no queues
	// exist, so the normalisation has to happen here before encoding.
	if names == nil {
		names = []string{}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(listQueuesResponse{Queues: names}); err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin sqs list response encode failed",
			slog.String("error", err.Error()),
		)
	}
}

func (h *SqsHandler) handleDescribe(w http.ResponseWriter, r *http.Request, name string) {
	if strings.TrimSpace(name) == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_queue_name", "queue name is required")
		return
	}
	summary, exists, err := h.source.AdminDescribeQueue(r.Context(), name)
	if err != nil {
		writeQueuesError(w, err, h.logger, r)
		return
	}
	if !exists {
		writeJSONError(w, http.StatusNotFound, "queue_not_found",
			"no queue is registered with that name")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(summary); err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin sqs describe response encode failed",
			slog.String("error", err.Error()),
		)
	}
}

func (h *SqsHandler) handleDelete(w http.ResponseWriter, r *http.Request, name string) {
	principal, ok := h.principalForWrite(w, r, "delete queues")
	if !ok {
		return
	}
	if strings.TrimSpace(name) == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_queue_name", "queue name is required")
		return
	}
	if err := h.source.AdminDeleteQueue(r.Context(), principal, name); err != nil {
		writeQueuesError(w, err, h.logger, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// handlePeek serves GET /admin/api/v1/sqs/queues/{name}/messages.
// Read role required (RoleReadOnly or RoleFull). Query parameters
// follow snake_case (limit / cursor / body_max_bytes); the SPA's
// TypeScript client adapter does the case translation at the
// request boundary.
func (h *SqsHandler) handlePeek(w http.ResponseWriter, r *http.Request, name string) {
	principal, ok := h.principalForReadSensitive(w, r)
	if !ok {
		return
	}
	opts, ok := parsePeekQueryParams(w, r)
	if !ok {
		return
	}
	result, err := h.source.AdminPeekQueue(r.Context(), principal, name, opts)
	if err != nil {
		writeQueuesError(w, err, h.logger, r)
		return
	}
	if result.Messages == nil {
		// Force the empty-result case to render as `{"messages": []}`
		// rather than `{"messages": null}`. The SPA iterates the
		// array directly and would crash on null.
		result.Messages = []PeekedMessage{}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin sqs peek response encode failed",
			slog.String("error", err.Error()),
		)
	}
}

// parsePeekQueryParams folds the snake_case query string into a
// typed PeekMessageOptions. Non-numeric limit / body_max_bytes
// surface as ErrQueuesValidation-shaped 400s; the adapter does the
// clamp itself, so we forward any in-range integer untouched.
func parsePeekQueryParams(w http.ResponseWriter, r *http.Request) (PeekMessageOptions, bool) {
	opts := PeekMessageOptions{Cursor: r.URL.Query().Get("cursor")}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "limit must be an integer")
			return PeekMessageOptions{}, false
		}
		opts.Limit = n
	}
	if raw := r.URL.Query().Get("body_max_bytes"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "body_max_bytes must be an integer")
			return PeekMessageOptions{}, false
		}
		opts.BodyMaxBytes = n
	}
	return opts, true
}

// handlePurge serves DELETE /admin/api/v1/sqs/queues/{name}/messages.
// Write role required (RoleFull only). Success returns 204; the
// 60-second rate limit surfaces as 429 with the Retry-After header
// and a retry_after_seconds JSON body via writeQueuesError.
func (h *SqsHandler) handlePurge(w http.ResponseWriter, r *http.Request, name string) {
	principal, ok := h.principalForWriteOnPurge(w, r)
	if !ok {
		return
	}
	if strings.TrimSpace(name) == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_queue_name", "queue name is required")
		return
	}
	if _, err := h.source.AdminPurgeQueue(r.Context(), principal, name); err != nil {
		writeQueuesError(w, err, h.logger, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// principalForReadSensitive gates peek (and any future read endpoint
// that surfaces stored payload content). Mirrors principalForWrite's
// live-role re-check pattern but accepts the lower RoleReadOnly
// tier. List / Describe themselves stay on the looser session-auth
// gate because their output is queue metadata already shown on the
// SPA's queue list page; peek diverges because the payload is the
// stored message bodies themselves (Codex r9 P1 on the design doc
// flagged the security-class distinction).
func (h *SqsHandler) principalForReadSensitive(w http.ResponseWriter, r *http.Request) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "internal", "missing session principal")
		return AuthPrincipal{}, false
	}
	if h.roles != nil {
		live, exists := h.roles.LookupRole(principal.AccessKey)
		if !exists {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this access key is not authorised to read queue contents")
			return AuthPrincipal{}, false
		}
		if !live.AllowsRead() {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this access key is not authorised to read queue contents")
			return AuthPrincipal{}, false
		}
		principal.Role = live
	} else if !principal.Role.AllowsRead() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this access key is not authorised to read queue contents")
		return AuthPrincipal{}, false
	}
	return principal, true
}

// principalForWriteOnPurge wraps principalForWrite with the verb the
// purge handler wants on rejection messages. Without this, an
// operator clicking Purge sees a 403 body saying "not authorised to
// delete queues" (the SqsHandler's principalForWrite was authored
// before the purge handler existed). Claude r1 caught the misleading
// wording.
func (h *SqsHandler) principalForWriteOnPurge(w http.ResponseWriter, r *http.Request) (AuthPrincipal, bool) {
	return h.principalForWrite(w, r, "purge messages")
}

// principalForWrite resolves the live role from the RoleStore (when
// configured), gates the request, and returns the principal with the
// **live** role overridden in place — so the role that flows downstream
// to the adapter is the one the operator currently has, not whatever
// the JWT happens to remember. Mirrors DynamoHandler.principalForWrite.
// Without the role override, a JWT-read_only / store-full promoted key
// passes the handler-side check but the adapter rejects with
// ErrAdminForbidden, forcing the user to log out and back in for a
// delete to work.
//
// The action verb (e.g. "delete queues", "purge messages") is woven
// into the 403 rejection body so an operator sees the right
// operation name. Without this, a read-only principal clicking
// Purge would be told they "lack the role to delete queues" — a
// confusing message about the wrong operation (Claude r1 caught
// this on PR #797).
//
// Failure paths write the response and return ok=false; callers
// short-circuit on the bool. Logged-out / wrong-role callers never
// reach the source layer, so the leader's identity is not leaked
// by indirection (forbidden response is the same shape regardless
// of leadership state).
func (h *SqsHandler) principalForWrite(w http.ResponseWriter, r *http.Request, action string) (AuthPrincipal, bool) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		// SessionAuth runs before this handler, so a missing
		// principal is a wiring bug. 500 rather than 401 since
		// 401 would be misleading — the request was authenticated.
		writeJSONError(w, http.StatusInternalServerError, "internal", "missing session principal")
		return AuthPrincipal{}, false
	}
	if h.roles != nil {
		live, exists := h.roles.LookupRole(principal.AccessKey)
		if !exists {
			// Key has been removed from the role config since
			// login. Treat it as no-access regardless of what
			// the JWT claimed.
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this access key is not authorised to "+action)
			return AuthPrincipal{}, false
		}
		if !live.AllowsWrite() {
			writeJSONError(w, http.StatusForbidden, "forbidden",
				"this access key is not authorised to "+action)
			return AuthPrincipal{}, false
		}
		// Forward the live role downstream so the adapter
		// re-check sees the same role the handler gated on.
		// Without this, a key promoted from read_only → full
		// after login still hits the adapter with the JWT's
		// stale read_only and gets a confusing 403.
		principal.Role = live
	} else if !principal.Role.AllowsWrite() {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this access key is not authorised to "+action)
		return AuthPrincipal{}, false
	}
	return principal, true
}

// writeQueuesError translates a QueuesSource error onto an HTTP
// response. Unrecognised errors map to 500 with a sanitised message
// — the raw err.Error() may include adapter internals (Pebble paths,
// raft peer ids) that should not flow to the SPA.
func writeQueuesError(w http.ResponseWriter, err error, logger *slog.Logger, r *http.Request) {
	// errors.As lets us pull the typed RetryAfter duration from a
	// PurgeInProgressError without losing the errors.Is sentinel
	// match below. The typed-error path is the only race-free way
	// to surface the duration: a second loadQueueMetaAt call would
	// race a concurrent purge resetting LastPurgedAtMillis in the
	// 60-second window.
	var purgeErr *PurgeInProgressError
	switch {
	case errors.As(err, &purgeErr):
		writePurgeInProgress(w, purgeErr)
	case errors.Is(err, ErrQueuesForbidden):
		writeJSONError(w, http.StatusForbidden, "forbidden", "principal lacks required role")
	case errors.Is(err, ErrQueuesNotLeader):
		w.Header().Set("Retry-After", strconv.Itoa(1))
		writeJSONError(w, http.StatusServiceUnavailable, "leader_unavailable",
			"local node is not the leader; retry shortly")
	case errors.Is(err, ErrQueuesNotFound):
		writeJSONError(w, http.StatusNotFound, "queue_not_found", "no queue with that name")
	case errors.Is(err, ErrQueuesValidation):
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		logger.LogAttrs(r.Context(), slog.LevelError, "admin sqs operation failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "internal",
			"queue operation failed; see server logs")
	}
}

// writePurgeInProgress emits the 429 response shape the design doc
// §3.4 specifies: Retry-After header (rounded up to whole seconds so
// a client retrying exactly at the deadline is guaranteed to clear)
// + JSON body { error, message, retry_after_seconds }.
//
// The `error` key (not `code`) matches writeJSONError's envelope so
// the SPA's apiFetch wrapper extracts the AWS-style sentinel into
// ApiError.code consistently with every other 4xx the admin surface
// returns. The retry_after_seconds field is in addition to the
// canonical Retry-After header so the SPA does not have to plumb
// response headers through its error path.
func writePurgeInProgress(w http.ResponseWriter, err *PurgeInProgressError) {
	secs := int64(err.RetryAfter / time.Second)
	if err.RetryAfter%time.Second != 0 {
		secs++
	}
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":               "PurgeQueueInProgress",
		"message":             "only one PurgeQueue operation on each queue is allowed every 60 seconds",
		"retry_after_seconds": secs,
	})
}
