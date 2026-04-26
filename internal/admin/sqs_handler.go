package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
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
type QueueSummary struct {
	Name              string            `json:"name"`
	IsFIFO            bool              `json:"is_fifo"`
	Generation        uint64            `json:"generation"`
	CreatedAt         time.Time         `json:"created_at,omitempty"`
	Attributes        map[string]string `json:"attributes,omitempty"`
	Counters          QueueCounters     `json:"counters"`
	CountersTruncated bool              `json:"counters_truncated,omitempty"`
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
// sniffing sentinels. AdminDeleteQueue returns the structured
// sentinels below so the handler can map them to HTTP statuses
// without leaking the adapter's error vocabulary.
type QueuesSource interface {
	AdminListQueues(ctx context.Context) ([]string, error)
	AdminDescribeQueue(ctx context.Context, name string) (*QueueSummary, bool, error)
	AdminDeleteQueue(ctx context.Context, principal AuthPrincipal, name string) error
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
)

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

// ServeHTTP routes /queues and /queues/{name}. Method handling
// mirrors DynamoHandler — keep the two parallel so an operator
// reading one understands the other for free.
func (h *SqsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == pathSqsQueues:
		switch r.Method {
		case http.MethodGet:
			h.handleList(w, r)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
		}
	case strings.HasPrefix(r.URL.Path, pathPrefixSqsQueues):
		name := strings.TrimPrefix(r.URL.Path, pathPrefixSqsQueues)
		switch r.Method {
		case http.MethodGet:
			h.handleDescribe(w, r, name)
		case http.MethodDelete:
			h.handleDelete(w, r, name)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET or DELETE")
		}
	default:
		writeJSONError(w, http.StatusNotFound, "unknown_endpoint",
			"no admin SQS handler is registered for this path")
	}
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
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		// SessionAuth runs before this handler, so a missing
		// principal is a wiring bug. 500 rather than 401 since
		// 401 would be misleading — the request was authenticated.
		writeJSONError(w, http.StatusInternalServerError, "internal", "missing session principal")
		return
	}
	// Re-evaluate the role against the live store so a downgraded
	// key cannot keep deleting with a still-valid JWT. The check is
	// before the leader check so a forbidden read-only caller never
	// learns the leader's identity by indirection.
	if !h.principalCanWrite(principal) {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"this access key is not authorised to delete queues")
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

// principalCanWrite re-resolves the access key against the live
// RoleStore (when configured) so a downgrade or revoke applies to
// the next request, not just to new logins. Falls back to the JWT's
// embedded role when no role store is wired (single-tenant default).
func (h *SqsHandler) principalCanWrite(p AuthPrincipal) bool {
	role := p.Role
	if h.roles != nil {
		if live, ok := h.roles.LookupRole(p.AccessKey); ok {
			role = live
		} else {
			// Key has been removed from the role config since
			// login. Treat it as no-access regardless of what
			// the JWT claimed.
			return false
		}
	}
	return role.AllowsWrite()
}

// writeQueuesError translates a QueuesSource error onto an HTTP
// response. Unrecognised errors map to 500 with a sanitised message
// — the raw err.Error() may include adapter internals (Pebble paths,
// raft peer ids) that should not flow to the SPA.
func writeQueuesError(w http.ResponseWriter, err error, logger *slog.Logger, r *http.Request) {
	switch {
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
