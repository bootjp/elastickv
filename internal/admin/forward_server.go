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

	source  TablesSource
	buckets BucketsSource
	roles   RoleStore
	logger  *slog.Logger
}

// NewForwardServer wires a TablesSource and a RoleStore behind the
// gRPC AdminForward service. logger may be nil; defaults to
// slog.Default(). The S3 BucketsSource is plumbed via WithBucketsSource
// so deployments that ship without the S3 adapter can still register
// the gRPC service for Dynamo forwarding.
func NewForwardServer(source TablesSource, roles RoleStore, logger *slog.Logger) *ForwardServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ForwardServer{source: source, roles: roles, logger: logger}
}

// WithBucketsSource enables S3 admin operation forwarding. Returns
// the receiver so wiring code can chain the call:
// `NewForwardServer(...).WithBucketsSource(...)`. A nil BucketsSource
// leaves S3 forwarding disabled — the Forward dispatcher rejects
// CREATE_BUCKET / DELETE_BUCKET / PUT_BUCKET_ACL with 501 in that
// case so a follower can detect the missing capability.
func (s *ForwardServer) WithBucketsSource(b BucketsSource) *ForwardServer {
	s.buckets = b
	return s
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
	return s.dispatchForward(ctx, principal, forwardedFrom, req)
}

// dispatchForward routes the validated request to the per-operation
// handler. Pulled out so Forward stays under the cyclomatic ceiling
// as the operation enum grows; the principal-validation +
// forwarded_from sanitisation logic stays in Forward where it belongs.
//
// Source-availability checks live in checkOpAvailability rather than
// in each handler: a Dynamo-only build has s.source != nil but
// s.buckets == nil, and an S3-only build (Codex P1 on PR #673) has
// the inverse. Centralising the check means every operation gets a
// consistent 501 error shape and a future op cannot ship without the
// operator-visible "not configured" message that the existing ops
// promise.
func (s *ForwardServer) dispatchForward(ctx context.Context, principal AuthPrincipal, forwardedFrom string, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	op := req.GetOperation()
	if resp, err, ok := s.checkOpAvailability(op); !ok {
		return resp, err
	}
	switch op {
	case pb.AdminOperation_ADMIN_OP_CREATE_TABLE:
		return s.handleCreate(ctx, principal, forwardedFrom, req)
	case pb.AdminOperation_ADMIN_OP_DELETE_TABLE:
		return s.handleDelete(ctx, principal, forwardedFrom, req)
	case pb.AdminOperation_ADMIN_OP_CREATE_BUCKET:
		return s.handleCreateBucket(ctx, principal, forwardedFrom, req)
	case pb.AdminOperation_ADMIN_OP_DELETE_BUCKET:
		return s.handleDeleteBucket(ctx, principal, forwardedFrom, req)
	case pb.AdminOperation_ADMIN_OP_PUT_BUCKET_ACL:
		return s.handlePutBucketAcl(ctx, principal, forwardedFrom, req)
	case pb.AdminOperation_ADMIN_OP_UNSPECIFIED:
		return rejectForward(http.StatusBadRequest, "invalid_request", "unknown admin operation")
	default:
		return rejectForward(http.StatusBadRequest, "invalid_request", "unknown admin operation")
	}
}

// checkOpAvailability returns (resp, err, true) when dispatchForward
// should continue to the per-op handler, or (resp, err, false) when
// the leader's build does not include the source the requested
// operation needs (S3-only deployment served a Dynamo op, or vice
// versa). Pulling the per-op switch out keeps dispatchForward's
// cyclomatic count under the linter ceiling as the enum grows.
func (s *ForwardServer) checkOpAvailability(op pb.AdminOperation) (*pb.AdminForwardResponse, error, bool) {
	switch op {
	case pb.AdminOperation_ADMIN_OP_CREATE_TABLE, pb.AdminOperation_ADMIN_OP_DELETE_TABLE:
		if s.source == nil {
			resp, err := notImplementedForwardResponse("DynamoDB")
			return resp, err, false
		}
	case pb.AdminOperation_ADMIN_OP_CREATE_BUCKET,
		pb.AdminOperation_ADMIN_OP_DELETE_BUCKET,
		pb.AdminOperation_ADMIN_OP_PUT_BUCKET_ACL:
		if s.buckets == nil {
			resp, err := notImplementedForwardResponse("S3")
			return resp, err, false
		}
	case pb.AdminOperation_ADMIN_OP_UNSPECIFIED:
		// Unknown-op rejection is dispatchForward's responsibility,
		// not this gate's. Falling through to ok=true lets the main
		// switch's default branch produce the canonical 400 message.
	}
	return nil, nil, true
}

// notImplementedForwardResponse produces the 501 response a follower
// receives when the leader is built without the source for this
// operation's surface. surface is the human-facing label that ends
// up in the error message ("DynamoDB" or "S3").
func notImplementedForwardResponse(surface string) (*pb.AdminForwardResponse, error) {
	return rejectForward(http.StatusNotImplemented, "not_implemented",
		surface+" admin forwarding is not configured on this leader")
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
	name, rejection, err := decodeNamedPayload(req.GetPayload(), "delete")
	if rejection != nil || err != nil {
		return rejection, err
	}
	if err := s.source.AdminDeleteTable(ctx, principal, name); err != nil {
		s.logUnexpectedSourceError(ctx, "delete_table", name, forwardedFrom, err)
		return forwardErrorResponse("delete", err), nil
	}
	s.auditDeleteSuccess(ctx, principal, forwardedFrom, "delete_table", "table", name)
	return &pb.AdminForwardResponse{StatusCode: http.StatusNoContent}, nil
}

// handleCreateBucket dispatches a forwarded POST /s3/buckets call.
// Mirrors handleCreate (Dynamo) but decodes a CreateBucketRequest
// and routes through BucketsSource. dispatchForward gates this on
// s.buckets != nil so callers reach here only when S3 forwarding is
// configured.
func (s *ForwardServer) handleCreateBucket(ctx context.Context, principal AuthPrincipal, forwardedFrom string, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	payload := req.GetPayload()
	if len(payload) > adminForwardPayloadLimit {
		return rejectForward(http.StatusRequestEntityTooLarge, "payload_too_large",
			"forwarded payload exceeds the 64 KiB admin limit")
	}
	if bytes.IndexByte(payload, 0) >= 0 {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"create-bucket payload contains a NUL byte")
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var body CreateBucketRequest
	if err := dec.Decode(&body); err != nil {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"create-bucket payload is not valid JSON")
	}
	if dec.More() {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"create-bucket payload has trailing data")
	}
	// Reuse the HTTP handler's validateCreateBucketRequest so the
	// forwarded path enforces identical rules — empty / whitespace-
	// padded bucket_name produces the same 400 message a leader-
	// direct call would, instead of slipping through here and
	// hitting the adapter's lower-level validateS3BucketName with
	// a less actionable error (Gemini security-high + Claude #2 on
	// PR #673).
	if err := validateCreateBucketRequest(body); err != nil {
		return rejectForward(http.StatusBadRequest, "invalid_body", err.Error())
	}
	summary, err := s.buckets.AdminCreateBucket(ctx, principal, body)
	if err != nil {
		s.logUnexpectedSourceError(ctx, "create_bucket", body.BucketName, forwardedFrom, err)
		return forwardBucketsErrorResponse("create", err), nil
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", forwardedFrom),
		slog.String("operation", "create_bucket"),
		slog.String("bucket", body.BucketName),
	)
	return jsonForwardResponse(http.StatusCreated, summary)
}

// handleDeleteBucket dispatches a forwarded DELETE
// /s3/buckets/{name} call. Same payload shape as the Dynamo delete:
// a JSON object with a single "name" field, which the bridge
// generates from the URL path.
func (s *ForwardServer) handleDeleteBucket(ctx context.Context, principal AuthPrincipal, forwardedFrom string, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	name, rejection, err := decodeNamedPayload(req.GetPayload(), "delete-bucket")
	if rejection != nil || err != nil {
		return rejection, err
	}
	if err := s.buckets.AdminDeleteBucket(ctx, principal, name); err != nil {
		s.logUnexpectedSourceError(ctx, "delete_bucket", name, forwardedFrom, err)
		return forwardBucketsErrorResponse("delete", err), nil
	}
	s.auditDeleteSuccess(ctx, principal, forwardedFrom, "delete_bucket", "bucket", name)
	return &pb.AdminForwardResponse{StatusCode: http.StatusNoContent}, nil
}

// auditDeleteSuccess emits the admin_audit slog line both Dynamo and
// S3 forwarded delete handlers need. Centralised so handleDelete and
// handleDeleteBucket do not diverge on the field set, and so a future
// handler that mirrors the same delete shape (e.g. delete-namespace)
// keeps the audit-log contract by reusing this helper rather than
// re-emitting a hand-rolled subset. resourceField is "table" or
// "bucket"; opLabel is the audit "operation" value.
func (s *ForwardServer) auditDeleteSuccess(ctx context.Context, principal AuthPrincipal, forwardedFrom, opLabel, resourceField, name string) {
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", forwardedFrom),
		slog.String("operation", opLabel),
		slog.String(resourceField, name),
	)
}

// handlePutBucketAcl dispatches a forwarded PUT
// /s3/buckets/{name}/acl call. The bridge encodes both the bucket
// name and the new ACL into the payload so the proto stays
// operation-agnostic — same approach handleDeleteBucket takes for
// the bucket name.
func (s *ForwardServer) handlePutBucketAcl(ctx context.Context, principal AuthPrincipal, forwardedFrom string, req *pb.AdminForwardRequest) (*pb.AdminForwardResponse, error) {
	payload := req.GetPayload()
	if len(payload) > adminForwardPayloadLimit {
		return rejectForward(http.StatusRequestEntityTooLarge, "payload_too_large",
			"forwarded payload exceeds the 64 KiB admin limit")
	}
	if bytes.IndexByte(payload, 0) >= 0 {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"put-bucket-acl payload contains a NUL byte")
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var body struct {
		Name string `json:"name"`
		ACL  string `json:"acl"`
	}
	if err := dec.Decode(&body); err != nil {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"put-bucket-acl payload is not valid JSON")
	}
	if dec.More() {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"put-bucket-acl payload has trailing data")
	}
	if body.Name == "" {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"put-bucket-acl payload missing name")
	}
	if strings.ContainsRune(body.Name, '/') {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"put-bucket-acl payload name must not contain '/'")
	}
	if strings.TrimSpace(body.ACL) == "" {
		return rejectForward(http.StatusBadRequest, "invalid_body",
			"put-bucket-acl payload missing acl")
	}
	if err := s.buckets.AdminPutBucketAcl(ctx, principal, body.Name, body.ACL); err != nil {
		s.logUnexpectedSourceError(ctx, "put_bucket_acl", body.Name, forwardedFrom, err)
		return forwardBucketsErrorResponse("put_acl", err), nil
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "admin_audit",
		slog.String("actor", principal.AccessKey),
		slog.String("role", string(principal.Role)),
		slog.String("forwarded_from", forwardedFrom),
		slog.String("operation", "put_bucket_acl"),
		slog.String("bucket", body.Name),
		slog.String("acl", body.ACL),
	)
	return &pb.AdminForwardResponse{StatusCode: http.StatusNoContent}, nil
}

// forwardBucketsErrorResponse re-encodes a BucketsSource error into
// the structured shape the follower's bridge can re-emit verbatim.
// Mirrors forwardErrorResponse on the Dynamo side: the same status
// codes and JSON envelopes the leader-direct HTTP path produces in
// writeBucketsError.
func forwardBucketsErrorResponse(op string, err error) *pb.AdminForwardResponse {
	switch {
	case errors.Is(err, ErrBucketsForbidden):
		return mustForwardJSON(http.StatusForbidden,
			errorResponse{Error: "forbidden", Message: "this endpoint requires a full-access role"})
	case errors.Is(err, ErrBucketsNotLeader):
		// Should never happen on the leader path — the leader just
		// verified itself — but a leadership transfer racing with
		// the dispatch makes this theoretically reachable. Carry
		// retry_after_seconds=1 so the follower's bridge translates
		// it back into an HTTP Retry-After header.
		resp := mustForwardJSON(http.StatusServiceUnavailable,
			errorResponse{Error: "leader_unavailable", Message: "leader stepped down mid-request"})
		resp.RetryAfterSeconds = 1
		return resp
	case errors.Is(err, ErrBucketsNotFound):
		return mustForwardJSON(http.StatusNotFound,
			errorResponse{Error: "not_found", Message: "bucket does not exist"})
	case errors.Is(err, ErrBucketsAlreadyExists):
		return mustForwardJSON(http.StatusConflict,
			errorResponse{Error: "already_exists", Message: "bucket already exists"})
	case errors.Is(err, ErrBucketsNotEmpty):
		return mustForwardJSON(http.StatusConflict,
			errorResponse{Error: "bucket_not_empty",
				Message: "bucket still has objects; remove them and retry"})
	}
	var verr *ValidationError
	if errors.As(err, &verr) {
		return mustForwardJSON(http.StatusBadRequest,
			errorResponse{Error: "invalid_request", Message: verr.Error()})
	}
	return mustForwardJSON(http.StatusInternalServerError,
		errorResponse{
			Error:   "s3_" + op + "_failed",
			Message: "failed to " + op + " bucket; see leader logs",
		})
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

// decodeNamedPayload validates and decodes the {"name": "..."} JSON
// shape both the Dynamo and S3 delete forwarders accept. Returns the
// decoded name on success, or a populated rejection response (and
// nil name) on a 400 / 413. opLabel is the human-facing prefix that
// goes into the rejection messages ("delete" for Dynamo,
// "delete-bucket" for S3) so the response identifies which path
// rejected — and so a future op (e.g. "describe-bucket") that
// reuses this helper still produces an actionable error.
//
// All four guards mirror the leader-direct HTTP path:
//   - 64 KiB payload cap (matches adminForwardPayloadLimit elsewhere)
//   - NUL-byte rejection (goccy/go-json treats raw NUL as end-of-
//     input; without this guard `{"name":"x"}\x00{"extra":1}`
//     payloads slip past dec.More(); Codex P2 on PR #635)
//   - DisallowUnknownFields + dec.More() trailing-token rejection
//   - empty + slash-bearing name rejection (the HTTP handlers
//     already 404 slash-bearing names; the forwarded path has to
//     reject symmetrically or a hostile follower could act on
//     `foo/bar` while a leader-direct call would not).
func decodeNamedPayload(payload []byte, opLabel string) (string, *pb.AdminForwardResponse, error) {
	if len(payload) > adminForwardPayloadLimit {
		resp, err := rejectForward(http.StatusRequestEntityTooLarge, "payload_too_large",
			"forwarded payload exceeds the 64 KiB admin limit")
		return "", resp, err
	}
	if bytes.IndexByte(payload, 0) >= 0 {
		resp, err := rejectForward(http.StatusBadRequest, "invalid_body",
			opLabel+" payload contains a NUL byte")
		return "", resp, err
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var body struct {
		Name string `json:"name"`
	}
	if err := dec.Decode(&body); err != nil {
		resp, rerr := rejectForward(http.StatusBadRequest, "invalid_body",
			opLabel+" payload is not valid JSON")
		return "", resp, rerr
	}
	if dec.More() {
		resp, err := rejectForward(http.StatusBadRequest, "invalid_body",
			opLabel+" payload has trailing data")
		return "", resp, err
	}
	if body.Name == "" {
		resp, err := rejectForward(http.StatusBadRequest, "invalid_body",
			opLabel+" payload missing name")
		return "", resp, err
	}
	if strings.ContainsRune(body.Name, '/') {
		resp, err := rejectForward(http.StatusBadRequest, "invalid_body",
			opLabel+" payload name must not contain '/'")
		return "", resp, err
	}
	return body.Name, nil, nil
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
//
// The resource argument carries either a Dynamo table name or an
// S3 bucket name, depending on op. The slog key is "resource" so
// log queries do not have to know which resource family produced a
// given line — Claude review on PR #673 caught the prior "table"
// key, which made bucket-error queries miss the audit entries.
func (s *ForwardServer) logUnexpectedSourceError(ctx context.Context, op, resource, forwardedFrom string, err error) {
	if isStructuredSourceError(err) {
		return
	}
	s.logger.LogAttrs(ctx, slog.LevelError, "admin_forward_"+op+"_failed",
		slog.String("resource", resource),
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
		errors.Is(err, ErrTablesAlreadyExists),
		errors.Is(err, ErrBucketsForbidden),
		errors.Is(err, ErrBucketsNotLeader),
		errors.Is(err, ErrBucketsNotFound),
		errors.Is(err, ErrBucketsAlreadyExists),
		errors.Is(err, ErrBucketsNotEmpty):
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
