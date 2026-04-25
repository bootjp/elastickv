package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/goccy/go-json"
)

// adminForwardPayloadLimit caps the JSON payload the leader will
// decode for any Forward operation. Mirrors defaultBodyLimit on the
// HTTP path (64 KiB) so a single Forward call cannot consume more
// memory than the same operation would over /admin/api/v1/dynamo/.
// gRPC has its own 4 MiB max-message default, but that is way too
// permissive for admin: a follower-forwarded request must obey the
// same 64 KiB ceiling we promise on the public API surface.
const adminForwardPayloadLimit = 64 << 10

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
	// Sanitise forwarded_from before it ever reaches a slog
	// handler. With JSON output the encoder escapes newlines on
	// our behalf, but with a text-format handler an attacker who
	// controlled the follower side could embed `\n` in the value
	// and split a single audit line into two — defeating
	// log-aggregation parsing or spoofing a synthetic entry.
	// Replacing CR/LF with spaces at the entry point keeps every
	// downstream call site on the leader trivially safe (Claude
	// review on PR #635).
	forwardedFrom := sanitiseForwardedFrom(req.GetForwardedFrom())
	principal, ok := s.validatePrincipal(req.GetPrincipal())
	if !ok {
		// Don't leak why the principal failed — the follower may
		// have a different view of the cluster's role config and
		// we want operators to investigate from the audit log on
		// the leader, not the follower's response body.
		s.logger.LogAttrs(ctx, slog.LevelWarn, "admin_forward_principal_rejected",
			slog.String("forwarded_from", forwardedFrom),
			slog.String("claimed_access_key", req.GetPrincipal().GetAccessKey()),
			slog.String("claimed_role", req.GetPrincipal().GetRole()),
		)
		return rejectForward(http.StatusForbidden, "forbidden",
			"this endpoint requires a full-access role")
	}
	switch req.GetOperation() {
	case pb.AdminOperation_ADMIN_OP_CREATE_TABLE:
		return s.handleCreate(ctx, principal, forwardedFrom, req)
	case pb.AdminOperation_ADMIN_OP_DELETE_TABLE:
		return s.handleDelete(ctx, principal, forwardedFrom, req)
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

func (s *ForwardServer) handleCreate(ctx context.Context, principal AuthPrincipal, forwardedFrom string, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	payload := req.GetPayload()
	if len(payload) > adminForwardPayloadLimit {
		return rejectForward(http.StatusRequestEntityTooLarge, "payload_too_large",
			"forwarded payload exceeds the 64 KiB admin limit")
	}
	// Reuse the HTTP handler's strict decoder so the forwarded
	// path enforces the same shape contract — DisallowUnknownFields,
	// trailing-token rejection, slash-in-name rejection, and the
	// rest of validateCreateTableRequest. Bypassing it here would
	// let a hostile follower (or a misbehaving SPA on the follower
	// side) sneak past validations the leader-direct path enforces.
	body, err := decodeCreateTableRequest(bytes.NewReader(payload))
	if err != nil {
		return rejectForward(http.StatusBadRequest, "invalid_body", err.Error())
	}
	summary, err := s.source.AdminCreateTable(ctx, principal, body)
	if err != nil {
		s.logUnexpectedSourceError(ctx, "create_table", body.TableName, forwardedFrom, err)
		return forwardErrorResponse("create", err), nil
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", forwardedFrom),
		slog.String("operation", "create_table"),
		slog.String("table", body.TableName),
	)
	return jsonForwardResponse(http.StatusCreated, summary)
}

func (s *ForwardServer) handleDelete(ctx context.Context, principal AuthPrincipal, forwardedFrom string, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	// Delete carries the table name in the payload as JSON so the
	// proto stays operation-agnostic — there is no operation-specific
	// field in AdminForwardRequest, by design (adding one per op
	// would couple every new admin endpoint to the proto schema).
	payload := req.GetPayload()
	if len(payload) > adminForwardPayloadLimit {
		return rejectForward(http.StatusRequestEntityTooLarge, "payload_too_large",
			"forwarded payload exceeds the 64 KiB admin limit")
	}
	// Mirror decodeCreateTableRequest's NUL-byte guard: goccy/go-json
	// treats raw NUL as end-of-input so dec.More() would otherwise
	// miss `{"name":"users"}\x00{"extra":1}` payloads. Codex P2 on
	// PR #635 flagged this as the same smuggling vector that the
	// HTTP create path already covers.
	if bytes.IndexByte(payload, 0) >= 0 {
		return rejectForward(http.StatusBadRequest, "invalid_body", "delete payload contains a NUL byte")
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var body struct {
		Name string `json:"name"`
	}
	if err := dec.Decode(&body); err != nil {
		return rejectForward(http.StatusBadRequest, "invalid_body", "delete payload is not valid JSON")
	}
	if dec.More() {
		return rejectForward(http.StatusBadRequest, "invalid_body", "delete payload has trailing data")
	}
	if body.Name == "" {
		return rejectForward(http.StatusBadRequest, "invalid_body", "delete payload missing name")
	}
	// Reject slash-bearing names symmetrically with the HTTP
	// handleDelete and handleDescribe paths. Without this, a
	// forwarded call could act on `foo/bar` while a leader-direct
	// call would 404 — divergent behaviour Codex P2 flagged on
	// PR #635.
	if strings.ContainsRune(body.Name, '/') {
		return rejectForward(http.StatusBadRequest, "invalid_body", "delete payload name must not contain '/'")
	}
	if err := s.source.AdminDeleteTable(ctx, principal, body.Name); err != nil {
		s.logUnexpectedSourceError(ctx, "delete_table", body.Name, forwardedFrom, err)
		return forwardErrorResponse("delete", err), nil
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", forwardedFrom),
		slog.String("operation", "delete_table"),
		slog.String("table", body.Name),
	)
	return &pb.AdminForwardResponse{StatusCode: http.StatusNoContent}, nil
}

// sanitiseForwardedFrom strips CR/LF from a follower-supplied
// node id so a malicious value cannot split a single audit log
// line into two when slog is using a text-format handler. JSON
// handlers escape these characters automatically; this is a
// defence-in-depth pass for handler-format-agnostic safety.
// Other control characters are deliberately preserved — only the
// line-splitting characters matter for log spoofing.
func sanitiseForwardedFrom(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, s)
}

// forwardErrorResponse re-encodes a TablesSource error in the
// structured shape the follower's handler can re-emit verbatim. This
// is the leader-side counterpart of writeTablesError: every status /
// JSON code the HTTP handler chooses is mirrored here so a forwarded
// call is indistinguishable to the SPA from a leader-direct call.
//
// op is "create" or "delete" so the unmapped 500 fallthrough emits
// dynamo_create_failed / dynamo_delete_failed — the same
// operation-specific codes the leader-direct HTTP path produces in
// writeTablesError. Without this, forwarded write failures showed
// up to clients as a generic "internal" code, breaking parity with
// the leader-direct path (Codex P2 on PR #635).
func forwardErrorResponse(op string, err error) *pb.AdminForwardResponse {
	switch {
	case errors.Is(err, ErrTablesForbidden):
		return mustForwardJSON(http.StatusForbidden, errorResponse{Error: "forbidden", Message: "this endpoint requires a full-access role"})
	case errors.Is(err, ErrTablesNotLeader):
		// Should never happen on the leader path — the leader
		// just verified itself — but if a leadership transfer
		// races with the dispatch, surface it consistently.
		// Carry retry_after_seconds=1 so the follower's bridge
		// translates it back into the same HTTP Retry-After
		// header the leader-direct path emits (Codex P2 on
		// PR #635 — without this the forwarded 503 would lose
		// its retry timing).
		resp := mustForwardJSON(http.StatusServiceUnavailable, errorResponse{Error: "leader_unavailable", Message: "leader stepped down mid-request"})
		resp.RetryAfterSeconds = 1
		return resp
	case errors.Is(err, ErrTablesNotFound):
		return mustForwardJSON(http.StatusNotFound, errorResponse{Error: "not_found", Message: "table does not exist"})
	case errors.Is(err, ErrTablesAlreadyExists):
		return mustForwardJSON(http.StatusConflict, errorResponse{Error: "already_exists", Message: "table already exists"})
	}
	var verr *ValidationError
	if errors.As(err, &verr) {
		return mustForwardJSON(http.StatusBadRequest, errorResponse{Error: "invalid_request", Message: verr.Error()})
	}
	return mustForwardJSON(http.StatusInternalServerError, errorResponse{
		Error:   "dynamo_" + op + "_failed",
		Message: "failed to " + op + " table; see leader logs",
	})
}

// logUnexpectedSourceError emits an error log for non-sentinel
// source failures so operators have a breadcrumb when forwarded
// writes 500. Sentinel errors that map to specific HTTP statuses
// (forbidden, not-found, validation, ...) are deliberately
// silent: those are routine client-side failures, not server
// regressions, and logging them at LevelError would drown the
// operational signal. The HTTP path's writeTablesError applies
// the same policy (Codex P2 on PR #635 flagged the silent path).
func (s *ForwardServer) logUnexpectedSourceError(ctx context.Context, op, table, forwardedFrom string, err error) {
	if isStructuredSourceError(err) {
		return
	}
	s.logger.LogAttrs(ctx, slog.LevelError, "admin_forward_"+op+"_failed",
		slog.String("table", table),
		slog.String("forwarded_from", forwardedFrom),
		slog.String("error", err.Error()),
	)
}

// isStructuredSourceError reports whether err is one of the
// admin-package sentinels or a ValidationError — i.e., a known
// failure mode the handler maps to a non-500 status. These are
// expected and not log-worthy.
func isStructuredSourceError(err error) bool {
	switch {
	case errors.Is(err, ErrTablesForbidden),
		errors.Is(err, ErrTablesNotLeader),
		errors.Is(err, ErrTablesNotFound),
		errors.Is(err, ErrTablesAlreadyExists):
		return true
	}
	var verr *ValidationError
	return errors.As(err, &verr)
}

func rejectForward(status int, code, msg string) (*pb.AdminForwardResponse, error) {
	return mustForwardJSON(status, errorResponse{Error: code, Message: msg}), nil
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
