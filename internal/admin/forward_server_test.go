package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// newForwardServerForTest wires a stub source and an in-memory role
// store into a ForwardServer so each test can mutate the inputs
// without re-building the role-lookup boilerplate.
func newForwardServerForTest(src TablesSource, roles MapRoleStore) *ForwardServer {
	return NewForwardServer(src, roles, nil)
}

// fullPrincipalRoleStore returns a role store where AKIA_FULL is a
// full-access admin and AKIA_RO is read-only — the canonical
// fixture for "leader resolves the principal correctly" tests.
func fullPrincipalRoleStore() MapRoleStore {
	return MapRoleStore{
		"AKIA_FULL": RoleFull,
		"AKIA_RO":   RoleReadOnly,
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestForwardServer_RejectsMissingPrincipal(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "missing principal")
}

func TestForwardServer_RejectsUnknownAccessKey(t *testing.T) {
	// A follower could forward a principal that the leader's
	// role-store does not know about (rolling-update window). The
	// leader must reject without ever touching the source.
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_GHOST", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   mustJSON(t, CreateTableRequest{TableName: "t", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusForbidden), resp.GetStatusCode())
	require.Empty(t, src.lastCreateInput.TableName, "source must not be touched on principal rejection")
}

func TestForwardServer_DemotesReadOnlyEvenIfFollowerSaidFull(t *testing.T) {
	// Design Section 3.3.2 acceptance criterion 4: follower ships a
	// "full" role but the leader's config is read-only. Result: 403.
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_RO", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   mustJSON(t, CreateTableRequest{TableName: "t", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusForbidden), resp.GetStatusCode())
}

func TestForwardServer_CreateTable_HappyPath(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	body := CreateTableRequest{TableName: "users", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal:     &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation:     pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:       mustJSON(t, body),
		ForwardedFrom: "node-2",
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusCreated), resp.GetStatusCode())
	require.Equal(t, "users", src.lastCreateInput.TableName)
	require.Equal(t, RoleFull, src.lastCreatePrincipal.Role)

	var summary DynamoTableSummary
	require.NoError(t, json.Unmarshal(resp.GetPayload(), &summary))
	require.Equal(t, "users", summary.Name)
}

func TestForwardServer_CreateTable_BadJSONReturns400(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   []byte("{not json"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "invalid_body")
}

func TestForwardServer_CreateTable_AlreadyExistsReturns409(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"users": {Name: "users", PartitionKey: "id"},
	}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   mustJSON(t, CreateTableRequest{TableName: "users", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusConflict), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "already_exists")
}

func TestForwardServer_DeleteTable_HappyPath(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{
		"users": {Name: "users"},
	}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal:     &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation:     pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:       []byte(`{"name":"users"}`),
		ForwardedFrom: "node-2",
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNoContent), resp.GetStatusCode())
	require.Empty(t, resp.GetPayload())
	require.Equal(t, "users", src.lastDeleteName)
}

func TestForwardServer_DeleteTable_MissingReturns404(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:   []byte(`{"name":"absent"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNotFound), resp.GetStatusCode())
}

// TestForwardServer_DeleteTable_RejectsNULBytePayload exercises
// the same Codex P2 vector that the create-table path covers:
// goccy/go-json treats raw NUL as end-of-input, so a body like
// `{"name":"users"}\x00{"extra":1}` would otherwise pass dec.More()
// undetected. The handler now scans for NUL before decoding.
func TestForwardServer_DeleteTable_RejectsNULBytePayload(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{"users": {Name: "users"}}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:   []byte("{\"name\":\"users\"}\x00{\"extra\":1}"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "NUL byte")
	// Source must not be reached — the table still exists in the
	// stub map after the rejection.
	require.Contains(t, src.tables, "users")
}

// TestForwardServer_DeleteTable_RejectsSlashInName mirrors the
// HTTP path's slash rejection on forwarded delete requests so the
// two surfaces cannot diverge (Codex P2).
func TestForwardServer_DeleteTable_RejectsSlashInName(t *testing.T) {
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:   []byte(`{"name":"foo/bar"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "must not contain")
}

func TestForwardServer_UnknownOperationRejected(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_UNSPECIFIED,
		Payload:   []byte(`{}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "unknown admin operation")
}

// TestForwardServer_CreateTable_PayloadTooLargeReturns413 exercises
// the size cap added in response to the Gemini security-medium
// finding. The leader must refuse to decode payloads bigger than
// the HTTP path's 64 KiB limit; gRPC's own 4 MiB default is too
// permissive for the admin surface.
func TestForwardServer_CreateTable_PayloadTooLargeReturns413(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	oversize := make([]byte, adminForwardPayloadLimit+1)
	for i := range oversize {
		oversize[i] = 'x'
	}
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   oversize,
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusRequestEntityTooLarge), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "payload_too_large")
}

// TestForwardServer_CreateTable_RejectsUnknownFields confirms the
// decode path now reuses decodeCreateTableRequest's
// DisallowUnknownFields setting (Codex P1). Without this, a
// follower could smuggle silently-ignored fields the leader-direct
// HTTP path would have rejected.
func TestForwardServer_CreateTable_RejectsUnknownFields(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   []byte(`{"table_name":"u","partition_key":{"name":"id","type":"S"},"unknown_field":1}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "invalid_body")
}

// TestForwardServer_CreateTable_RejectsSlashInName confirms the
// strict validation pulled in via decodeCreateTableRequest also
// catches the slash-rejection rule the HTTP path enforces.
func TestForwardServer_CreateTable_RejectsSlashInName(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   []byte(`{"table_name":"foo/bar","partition_key":{"name":"id","type":"S"}}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "must not contain")
}

// TestForwardServer_DeleteTable_PayloadTooLargeReturns413 mirrors
// the create-side cap: an oversized delete payload must be refused
// before the JSON decoder runs, regardless of how the gRPC layer
// configures its own message limits.
func TestForwardServer_DeleteTable_PayloadTooLargeReturns413(t *testing.T) {
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	oversize := make([]byte, adminForwardPayloadLimit+1)
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:   oversize,
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusRequestEntityTooLarge), resp.GetStatusCode())
}

func TestForwardServer_CreateTable_GenericErrorReturns500(t *testing.T) {
	// A non-sentinel error from the source must surface as 500 with
	// a sanitised message. The leader logs the raw error; nothing
	// reaches the follower except the structured response.
	src := &stubTablesSource{
		createErr: errors.New("storage backing sentinel ZQ-993"),
	}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   mustJSON(t, CreateTableRequest{TableName: "users", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusInternalServerError), resp.GetStatusCode())
	require.NotContains(t, string(resp.GetPayload()), "ZQ-993")
	// Error code parity with leader-direct path: must use
	// dynamo_create_failed, not the previous generic "internal"
	// (Codex P2 on PR #635).
	require.Contains(t, string(resp.GetPayload()), `"error":"dynamo_create_failed"`)
}

// TestForwardServer_DeleteTable_GenericErrorUsesOperationSpecificCode
// is the delete-side counterpart: 500 fallthrough must use
// dynamo_delete_failed so client retry/branching logic that
// already handles the leader-direct path works unchanged.
func TestForwardServer_DeleteTable_GenericErrorUsesOperationSpecificCode(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{"users": {Name: "users"}},
		deleteErr: errors.New("storage backing sentinel DEL-1"),
	}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:   []byte(`{"name":"users"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusInternalServerError), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), `"error":"dynamo_delete_failed"`)
	require.NotContains(t, string(resp.GetPayload()), "DEL-1")
}

// TestForwardServer_CreateTable_LeaderSteppedDownReturns503 covers
// the race-window comment in forwardErrorResponse: even though the
// leader passed isVerifiedDynamoLeader at the top of
// AdminCreateTable, leadership can still drop mid-dispatch and the
// adapter then returns ErrTablesNotLeader. The forward server must
// surface that as a 503 leader_unavailable response so the
// follower's bridge can re-emit the exact 503 contract clients
// already know — Claude review on PR #635 flagged the gap (no test
// exercised this code path despite the comment).
func TestForwardServer_CreateTable_LeaderSteppedDownReturns503(t *testing.T) {
	src := &stubTablesSource{createErr: ErrTablesNotLeader}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:   mustJSON(t, CreateTableRequest{TableName: "users", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusServiceUnavailable), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "leader_unavailable")
	// Codex P2 on PR #635: the leader's 503 must carry the
	// retry-after hint over the wire so the follower's bridge can
	// rebuild the same HTTP Retry-After header a leader-direct
	// 503 would have produced. Without this the forwarded 503
	// silently strips the retry timing.
	require.Equal(t, int32(1), resp.GetRetryAfterSeconds())
}

// TestForwardServer_DeleteTable_LeaderSteppedDownReturns503 mirrors
// the create-side coverage on the delete path.
func TestForwardServer_DeleteTable_LeaderSteppedDownReturns503(t *testing.T) {
	src := &stubTablesSource{
		tables:    map[string]*DynamoTableSummary{"users": {Name: "users"}},
		deleteErr: ErrTablesNotLeader,
	}
	srv := newForwardServerForTest(src, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_TABLE,
		Payload:   []byte(`{"name":"users"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusServiceUnavailable), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "leader_unavailable")
}

// TestForwardServer_SanitisesForwardedFromInLog confirms the
// CR/LF stripping pass at the RPC entry point: a malicious
// follower-supplied node id with embedded newlines must not be
// able to split a single audit/error line into two when slog is
// using a text-format handler. Claude review on PR #635.
func TestForwardServer_SanitisesForwardedFromInLog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	src := &stubTablesSource{tables: map[string]*DynamoTableSummary{}}
	srv := NewForwardServer(src, fullPrincipalRoleStore(), logger)

	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal:     &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation:     pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:       mustJSON(t, CreateTableRequest{TableName: "users", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
		ForwardedFrom: "follower\nfake_actor=evil\nrole=full",
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusCreated), resp.GetStatusCode())
	logged := buf.String()
	// Sanitised value (newlines replaced with spaces) must be
	// present.
	require.Contains(t, logged, "follower fake_actor=evil role=full")
	// Raw newline-bearing value must NOT be in the log — that
	// would mean the sanitisation did not run.
	require.NotContains(t, logged, "follower\nfake_actor=evil")
}

// TestForwardServer_LogsUnexpectedSourceError confirms the leader
// emits an error log line for non-sentinel source failures so
// operators can investigate forwarded write 500s without
// reverse-engineering the response body. The HTTP path's
// writeTablesError already logs; without the symmetric emit on
// the forward path, forwarded failures were operationally invisible
// (Codex P2 on PR #635).
func TestForwardServer_LogsUnexpectedSourceError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	src := &stubTablesSource{createErr: errors.New("storage sentinel SENT-42")}
	srv := NewForwardServer(src, fullPrincipalRoleStore(), logger)

	_, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal:     &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation:     pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
		Payload:       mustJSON(t, CreateTableRequest{TableName: "u", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
		ForwardedFrom: "follower-9",
	})
	require.NoError(t, err)
	logged := buf.String()
	require.Contains(t, logged, "admin_forward_create_table_failed")
	require.Contains(t, logged, "SENT-42")
	require.Contains(t, logged, "follower-9")
}

// TestForwardServer_DoesNotLogStructuredSourceErrors confirms the
// "expected" sentinels (forbidden, not-found, already-exists,
// validation) do NOT emit error-level logs. They are routine
// client-side outcomes, not server regressions, so logging them
// at LevelError would drown the operational signal.
func TestForwardServer_DoesNotLogStructuredSourceErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"forbidden", ErrTablesForbidden},
		{"not_found", ErrTablesNotFound},
		{"already_exists", ErrTablesAlreadyExists},
		{"validation", &ValidationError{Message: "field foo invalid"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
			src := &stubTablesSource{createErr: tc.err}
			srv := NewForwardServer(src, fullPrincipalRoleStore(), logger)
			_, _ = srv.Forward(context.Background(), &pb.AdminForwardRequest{
				Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
				Operation: pb.AdminOperation_ADMIN_OP_CREATE_TABLE,
				Payload:   mustJSON(t, CreateTableRequest{TableName: "u", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}),
			})
			require.NotContains(t, buf.String(), "admin_forward_create_table_failed",
				"sentinel error %q must not produce an error log", tc.name)
		})
	}
}

// newForwardServerWithBucketsForTest is the slice 2b counterpart of
// newForwardServerForTest: wires both a TablesSource (zero-value) and
// a BucketsSource so the bucket-side dispatch tests can mutate the
// inputs without rebuilding the role-lookup boilerplate.
func newForwardServerWithBucketsForTest(buckets BucketsSource, roles MapRoleStore) *ForwardServer {
	return NewForwardServer(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, roles, nil).
		WithBucketsSource(buckets)
}

func TestForwardServer_CreateBucket_HappyPath(t *testing.T) {
	buckets := &stubBucketsSource{}
	srv := newForwardServerWithBucketsForTest(buckets, fullPrincipalRoleStore())
	body := CreateBucketRequest{BucketName: "public-assets", ACL: "public-read"}
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal:     &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation:     pb.AdminOperation_ADMIN_OP_CREATE_BUCKET,
		Payload:       mustJSON(t, body),
		ForwardedFrom: "node-2",
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusCreated), resp.GetStatusCode())
	require.Equal(t, "public-assets", buckets.lastCreateInput.BucketName)
	require.Equal(t, RoleFull, buckets.lastCreatePrincipal.Role)
	var summary BucketSummary
	require.NoError(t, json.Unmarshal(resp.GetPayload(), &summary))
	require.Equal(t, "public-assets", summary.Name)
}

func TestForwardServer_CreateBucket_NoBucketsSourceReturns501(t *testing.T) {
	// Builds without S3 do not call WithBucketsSource; the leader
	// must still reject CREATE_BUCKET cleanly with 501 instead of
	// reaching for a nil receiver and panicking.
	srv := newForwardServerForTest(&stubTablesSource{tables: map[string]*DynamoTableSummary{}}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_BUCKET,
		Payload:   mustJSON(t, CreateBucketRequest{BucketName: "x"}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNotImplemented), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "not_implemented")
}

func TestForwardServer_CreateBucket_BadJSONReturns400(t *testing.T) {
	srv := newForwardServerWithBucketsForTest(&stubBucketsSource{}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_BUCKET,
		Payload:   []byte("{not json"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "invalid_body")
}

func TestForwardServer_CreateBucket_AlreadyExistsReturns409(t *testing.T) {
	buckets := &stubBucketsSource{
		buckets:   map[string]BucketSummary{"existing": {Name: "existing"}},
		createErr: ErrBucketsAlreadyExists,
	}
	srv := newForwardServerWithBucketsForTest(buckets, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_CREATE_BUCKET,
		Payload:   mustJSON(t, CreateBucketRequest{BucketName: "existing"}),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusConflict), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "already_exists")
}

func TestForwardServer_DeleteBucket_HappyPath(t *testing.T) {
	buckets := &stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders"}},
	}
	srv := newForwardServerWithBucketsForTest(buckets, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_BUCKET,
		Payload:   []byte(`{"name":"orders"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNoContent), resp.GetStatusCode())
	require.Equal(t, "orders", buckets.lastDeleteName)
}

func TestForwardServer_DeleteBucket_NotEmptyReturns409(t *testing.T) {
	buckets := &stubBucketsSource{deleteErr: ErrBucketsNotEmpty}
	srv := newForwardServerWithBucketsForTest(buckets, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_BUCKET,
		Payload:   []byte(`{"name":"orders"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusConflict), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "bucket_not_empty")
}

func TestForwardServer_DeleteBucket_RejectsSlashInName(t *testing.T) {
	srv := newForwardServerWithBucketsForTest(&stubBucketsSource{}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_DELETE_BUCKET,
		Payload:   []byte(`{"name":"foo/bar"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "must not contain")
}

func TestForwardServer_PutBucketAcl_HappyPath(t *testing.T) {
	buckets := &stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders", ACL: "private"}},
	}
	srv := newForwardServerWithBucketsForTest(buckets, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_PUT_BUCKET_ACL,
		Payload:   []byte(`{"name":"orders","acl":"public-read"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNoContent), resp.GetStatusCode())
	require.Equal(t, "orders", buckets.lastPutACLBucket)
	require.Equal(t, "public-read", buckets.lastPutACLValue)
}

func TestForwardServer_PutBucketAcl_RejectsMissingACL(t *testing.T) {
	srv := newForwardServerWithBucketsForTest(&stubBucketsSource{}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_PUT_BUCKET_ACL,
		Payload:   []byte(`{"name":"orders"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
	require.Contains(t, string(resp.GetPayload()), "missing acl")
}

func TestForwardServer_PutBucketAcl_RejectsSlashInName(t *testing.T) {
	srv := newForwardServerWithBucketsForTest(&stubBucketsSource{}, fullPrincipalRoleStore())
	resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
		Operation: pb.AdminOperation_ADMIN_OP_PUT_BUCKET_ACL,
		Payload:   []byte(`{"name":"foo/bar","acl":"private"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), resp.GetStatusCode())
}

func TestForwardServer_BucketOps_PayloadTooLargeReturns413(t *testing.T) {
	cases := []pb.AdminOperation{
		pb.AdminOperation_ADMIN_OP_CREATE_BUCKET,
		pb.AdminOperation_ADMIN_OP_DELETE_BUCKET,
		pb.AdminOperation_ADMIN_OP_PUT_BUCKET_ACL,
	}
	for _, op := range cases {
		t.Run(op.String(), func(t *testing.T) {
			srv := newForwardServerWithBucketsForTest(&stubBucketsSource{}, fullPrincipalRoleStore())
			big := make([]byte, adminForwardPayloadLimit+1)
			resp, err := srv.Forward(context.Background(), &pb.AdminForwardRequest{
				Principal: &pb.AdminPrincipal{AccessKey: "AKIA_FULL", Role: "full"},
				Operation: op,
				Payload:   big,
			})
			require.NoError(t, err)
			require.Equal(t, int32(http.StatusRequestEntityTooLarge), resp.GetStatusCode())
		})
	}
}
