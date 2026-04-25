package admin

import (
	"context"
	"errors"
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
}
