package admin

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/goccy/go-json"
)

// ClusterInfo is the lightweight snapshot the admin dashboard displays on
// its landing page. Everything here is cheap to assemble; we deliberately
// do not include per-shard key counts or byte statistics to keep the
// endpoint safe to poll.
type ClusterInfo struct {
	NodeID    string      `json:"node_id"`
	Version   string      `json:"version"`
	Timestamp time.Time   `json:"timestamp"`
	Groups    []GroupInfo `json:"groups"`
}

// GroupInfo describes a single Raft group from the local node's point of
// view. LeaderID is the empty string during an election or when the node
// has not yet discovered the leader.
type GroupInfo struct {
	GroupID  uint64   `json:"group_id"`
	LeaderID string   `json:"leader_id"`
	Members  []string `json:"members"`
	IsLeader bool     `json:"is_leader"`
}

// ClusterInfoSource is the small contract the cluster handler calls out
// to. Production wires this to a real Raft/engine view; tests use a stub.
type ClusterInfoSource interface {
	Describe(ctx context.Context) (ClusterInfo, error)
}

// ClusterInfoFunc is a convenience adapter for wiring a plain function
// without defining an interface implementation.
type ClusterInfoFunc func(ctx context.Context) (ClusterInfo, error)

// Describe implements ClusterInfoSource.
func (f ClusterInfoFunc) Describe(ctx context.Context) (ClusterInfo, error) {
	return f(ctx)
}

// ClusterHandler serves GET /admin/api/v1/cluster.
type ClusterHandler struct {
	source ClusterInfoSource
	logger *slog.Logger
}

// NewClusterHandler wires a source into the HTTP handler and seeds
// logging with slog.Default(). Callers that want a tagged logger can
// chain WithLogger(...) on the returned handler.
func NewClusterHandler(source ClusterInfoSource) *ClusterHandler {
	return &ClusterHandler{source: source, logger: slog.Default()}
}

// WithLogger overrides the default slog destination. Kept as an option
// so main.go can attach a component tag without changing the
// constructor signature.
func (h *ClusterHandler) WithLogger(l *slog.Logger) *ClusterHandler {
	if l == nil {
		return h
	}
	h.logger = l
	return h
}

// ServeHTTP renders the cluster snapshot as JSON. Errors from the source
// are logged on the server with full detail and surfaced to the client as
// a generic "cluster_describe_failed" code. Leaking err.Error() to
// unauthenticated-ish clients would reveal raft/store internals.
func (h *ClusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
		return
	}
	info, err := h.source.Describe(r.Context())
	if err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin cluster describe failed",
			slog.String("error", err.Error()),
		)
		writeJSONError(w, http.StatusInternalServerError, "cluster_describe_failed",
			"failed to describe cluster state; see server logs")
		return
	}
	if info.Timestamp.IsZero() {
		info.Timestamp = time.Now().UTC()
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(info); err != nil {
		// The 200 header is already on the wire, so we cannot
		// change the status — but a truncated JSON body is hard
		// to diagnose without a breadcrumb.
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin cluster response encode failed",
			slog.String("error", err.Error()),
		)
	}
}
