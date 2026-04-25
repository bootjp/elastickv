package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/goccy/go-json"
)

// ForwardServer is the leader-side gRPC handler for the AdminForward
// RPC (design Section 3.3). The follower's admin HTTP layer calls it
// when the local node is not the Raft leader; this server then
// re-validates the principal, dispatches the operation against the
// local TablesSource, and serialises the result back to the
// follower in the same JSON shape the SPA would have received from a
// leader-direct call.
//
// The server is deliberately kept independent of the dynamo HTTP
// handler: it runs in the gRPC server's goroutine pool, not in the
// HTTP server's, and shares only the TablesSource interface (which
// the bridge in main_admin.go already implements for the local
// adapter).
type ForwardServer struct {
	pb.UnimplementedAdminForwardServer

	source TablesSource
	roles  RoleStore
	logger *slog.Logger
}

// RoleStore is the access-key → role lookup the leader uses to
// re-validate the inbound principal. Implementations should mirror
// the admin server's `Roles` map; production passes a typed wrapper
// around that map so tests can swap in an in-memory stub.
type RoleStore interface {
	// LookupRole returns the role for an access key as understood
	// by the leader's view of cluster configuration. The bool is
	// false when the access key is not in the admin role index — a
	// follower that forwarded the principal should not be able to
	// "make up" an admin identity.
	LookupRole(accessKey string) (Role, bool)
}

// MapRoleStore is the trivial in-memory implementation, sufficient
// for tests and for the production wiring (which already keeps the
// role map in memory).
type MapRoleStore map[string]Role

// LookupRole implements RoleStore.
func (m MapRoleStore) LookupRole(accessKey string) (Role, bool) {
	r, ok := m[accessKey]
	return r, ok
}

// NewForwardServer wires a TablesSource and a RoleStore behind the
// gRPC AdminForward service. logger may be nil; defaults to
// slog.Default().
func NewForwardServer(source TablesSource, roles RoleStore, logger *slog.Logger) *ForwardServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ForwardServer{source: source, roles: roles, logger: logger}
}

// Forward is the gRPC entrypoint. It performs the principal
// re-evaluation the design mandates, then dispatches by operation.
// Errors that the SPA can act on are returned as a structured
// AdminForwardResponse with status_code + JSON payload; only fatal
// gRPC-layer errors (decode failure, unknown operation) come back as
// status.Errorf to the follower.
func (s *ForwardServer) Forward(ctx context.Context, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	if req == nil || req.GetPrincipal() == nil {
		return rejectForward(http.StatusBadRequest, "invalid_request", "missing principal")
	}
	principal, ok := s.validatePrincipal(req.GetPrincipal())
	if !ok {
		// Don't leak why the principal failed — the follower may
		// have a different view of the cluster's role config and
		// we want operators to investigate from the audit log on
		// the leader, not the follower's response body.
		s.logger.LogAttrs(ctx, slog.LevelWarn, "admin_forward_principal_rejected",
			slog.String("forwarded_from", req.GetForwardedFrom()),
			slog.String("claimed_access_key", req.GetPrincipal().GetAccessKey()),
			slog.String("claimed_role", req.GetPrincipal().GetRole()),
		)
		return rejectForward(http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
	}
	switch req.GetOperation() {
	case pb.AdminOperation_ADMIN_OP_CREATE_TABLE:
		return s.handleCreate(ctx, principal, req)
	case pb.AdminOperation_ADMIN_OP_DELETE_TABLE:
		return s.handleDelete(ctx, principal, req)
	case pb.AdminOperation_ADMIN_OP_UNSPECIFIED:
		return rejectForward(http.StatusBadRequest, "invalid_request", "unknown admin operation")
	default:
		return rejectForward(http.StatusBadRequest, "invalid_request", "unknown admin operation")
	}
}

func (s *ForwardServer) validatePrincipal(p *pb.AdminPrincipal) (AuthPrincipal, bool) {
	accessKey := p.GetAccessKey()
	if accessKey == "" {
		return AuthPrincipal{}, false
	}
	role, ok := s.roles.LookupRole(accessKey)
	if !ok {
		return AuthPrincipal{}, false
	}
	// Critical re-evaluation: if the leader sees this access key as
	// read-only, the operation is forbidden even if the follower
	// thought it was full. The reverse — leader sees full, follower
	// sees read-only — would have been short-circuited at the
	// follower already, so we do not need to check it here.
	if !role.AllowsWrite() {
		return AuthPrincipal{}, false
	}
	return AuthPrincipal{AccessKey: accessKey, Role: role}, true
}

func (s *ForwardServer) handleCreate(ctx context.Context, principal AuthPrincipal, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	var body CreateTableRequest
	if err := json.Unmarshal(req.GetPayload(), &body); err != nil {
		return rejectForward(http.StatusBadRequest, "invalid_body", "request body is not valid JSON")
	}
	summary, err := s.source.AdminCreateTable(ctx, principal, body)
	if err != nil {
		return forwardErrorResponse(err), nil
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", req.GetForwardedFrom()),
		slog.String("operation", "create_table"),
		slog.String("table", body.TableName),
	)
	return jsonForwardResponse(http.StatusCreated, summary)
}

func (s *ForwardServer) handleDelete(ctx context.Context, principal AuthPrincipal, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	// Delete carries the table name in the payload as JSON so the
	// proto stays operation-agnostic — there is no operation-specific
	// field in AdminForwardRequest, by design (adding one per op
	// would couple every new admin endpoint to the proto schema).
	var body struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(req.GetPayload(), &body); err != nil || body.Name == "" {
		return rejectForward(http.StatusBadRequest, "invalid_body", "delete payload missing name")
	}
	if err := s.source.AdminDeleteTable(ctx, principal, body.Name); err != nil {
		return forwardErrorResponse(err), nil
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", req.GetForwardedFrom()),
		slog.String("operation", "delete_table"),
		slog.String("table", body.Name),
	)
	return &pb.AdminForwardResponse{StatusCode: http.StatusNoContent}, nil
}

// forwardErrorResponse re-encodes a TablesSource error in the
// structured shape the follower's handler can re-emit verbatim. This
// is the leader-side counterpart of writeTablesError: every status /
// JSON code the HTTP handler chooses is mirrored here so a forwarded
// call is indistinguishable to the SPA from a leader-direct call.
func forwardErrorResponse(err error) *pb.AdminForwardResponse {
	switch {
	case errors.Is(err, ErrTablesForbidden):
		return mustForwardJSON(http.StatusForbidden, errorBody{Error: "forbidden", Message: "this endpoint requires a full-access role"})
	case errors.Is(err, ErrTablesNotLeader):
		// Should never happen on the leader path — the leader
		// just verified itself — but if a leadership transfer
		// races with the dispatch, surface it consistently.
		return mustForwardJSON(http.StatusServiceUnavailable, errorBody{Error: "leader_unavailable", Message: "leader stepped down mid-request"})
	case errors.Is(err, ErrTablesNotFound):
		return mustForwardJSON(http.StatusNotFound, errorBody{Error: "not_found", Message: "table does not exist"})
	case errors.Is(err, ErrTablesAlreadyExists):
		return mustForwardJSON(http.StatusConflict, errorBody{Error: "already_exists", Message: "table already exists"})
	}
	var verr *ValidationError
	if errors.As(err, &verr) {
		return mustForwardJSON(http.StatusBadRequest, errorBody{Error: "invalid_request", Message: verr.Error()})
	}
	return mustForwardJSON(http.StatusInternalServerError, errorBody{Error: "internal", Message: "internal error; see leader logs"})
}

// errorBody is the shared JSON shape for both the HTTP handler's
// writeJSONError and the forward server's encoded responses.
type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

func rejectForward(status int, code, msg string) (*pb.AdminForwardResponse, error) {
	return mustForwardJSON(status, errorBody{Error: code, Message: msg}), nil
}

func mustForwardJSON(status int, body any) *pb.AdminForwardResponse {
	payload, err := json.Marshal(body)
	if err != nil {
		// json.Marshal on a struct of strings cannot fail in
		// practice; a 500 with a bare string body is the safest
		// fallback if it ever does.
		return &pb.AdminForwardResponse{
			StatusCode:  http.StatusInternalServerError,
			Payload:     []byte(`{"error":"internal","message":"failed to encode response"}`),
			ContentType: "application/json; charset=utf-8",
		}
	}
	return &pb.AdminForwardResponse{
		StatusCode:  int32(status), //nolint:gosec // status fits in int32; net/http codes are 100-599.
		Payload:     payload,
		ContentType: "application/json; charset=utf-8",
	}
}

func jsonForwardResponse(status int, body any) (*pb.AdminForwardResponse, error) {
	return mustForwardJSON(status, body), nil
}
