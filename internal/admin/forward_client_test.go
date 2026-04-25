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

// stubForwardConn is the in-memory PBAdminForwardClient the
// follower-side tests use. It captures the last request and
// returns whatever response/error the test prepared.
type stubForwardConn struct {
	resp    *pb.AdminForwardResponse
	err     error
	lastReq *pb.AdminForwardRequest
}

func (s *stubForwardConn) Forward(_ context.Context, in *pb.AdminForwardRequest, _ ...grpcCallOption) (*pb.AdminForwardResponse, error) {
	s.lastReq = in
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

// stubConnFactory wraps stubForwardConn so the gRPCForwardClient
// can resolve an address to a client. The captured `addr` lets
// tests prove the resolver round-tripped correctly.
type stubConnFactory struct {
	conn     *stubForwardConn
	dialErr  error
	lastAddr string
}

func (s *stubConnFactory) ConnFor(addr string) (PBAdminForwardClient, error) {
	s.lastAddr = addr
	if s.dialErr != nil {
		return nil, s.dialErr
	}
	return s.conn, nil
}

// fixedResolver returns a closure that always resolves to addr.
// Pulling this into a helper keeps the per-test setup compact.
func fixedResolver(addr string) LeaderAddressResolver {
	return func() string { return addr }
}

func TestNewGRPCForwardClient_RejectsMissingDeps(t *testing.T) {
	cases := []struct {
		name     string
		resolver LeaderAddressResolver
		dial     GRPCConnFactory
		nodeID   string
		expect   string
	}{
		{"nil resolver", nil, &stubConnFactory{}, "n1", "leader address resolver"},
		{"nil dial", fixedResolver(""), nil, "n1", "gRPC connection factory"},
		{"empty node id", fixedResolver(""), &stubConnFactory{}, "", "node id is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd, err := NewGRPCForwardClient(tc.resolver, tc.dial, tc.nodeID)
			require.Error(t, err)
			require.Nil(t, fwd)
			require.Contains(t, err.Error(), tc.expect)
		})
	}
}

func TestGRPCForwardClient_ForwardCreateTable_HappyPath(t *testing.T) {
	conn := &stubForwardConn{resp: &pb.AdminForwardResponse{
		StatusCode:  http.StatusCreated,
		Payload:     []byte(`{"name":"users"}`),
		ContentType: "application/json; charset=utf-8",
	}}
	dial := &stubConnFactory{conn: conn}
	fwd, err := NewGRPCForwardClient(fixedResolver("leader.local:7000"), dial, "follower-2")
	require.NoError(t, err)

	in := CreateTableRequest{TableName: "users", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}}
	res, err := fwd.ForwardCreateTable(context.Background(), AuthPrincipal{AccessKey: "AKIA_F", Role: RoleFull}, in)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, http.StatusCreated, res.StatusCode)
	require.JSONEq(t, `{"name":"users"}`, string(res.Payload))
	require.Equal(t, "application/json; charset=utf-8", res.ContentType)

	// The captured request must carry the principal, the canonical
	// op enum, the JSON-encoded body, and the follower node id.
	require.Equal(t, "leader.local:7000", dial.lastAddr)
	require.NotNil(t, conn.lastReq)
	require.Equal(t, pb.AdminOperation_ADMIN_OP_CREATE_TABLE, conn.lastReq.GetOperation())
	require.Equal(t, "AKIA_F", conn.lastReq.GetPrincipal().GetAccessKey())
	require.Equal(t, "full", conn.lastReq.GetPrincipal().GetRole())
	require.Equal(t, "follower-2", conn.lastReq.GetForwardedFrom())
	var roundtripped CreateTableRequest
	require.NoError(t, json.Unmarshal(conn.lastReq.GetPayload(), &roundtripped))
	require.Equal(t, in, roundtripped)
}

func TestGRPCForwardClient_ForwardDeleteTable_HappyPath(t *testing.T) {
	conn := &stubForwardConn{resp: &pb.AdminForwardResponse{
		StatusCode:  http.StatusNoContent,
		ContentType: "application/json; charset=utf-8",
	}}
	dial := &stubConnFactory{conn: conn}
	fwd, _ := NewGRPCForwardClient(fixedResolver("leader.local:7000"), dial, "follower-3")

	res, err := fwd.ForwardDeleteTable(context.Background(), AuthPrincipal{AccessKey: "AKIA_F", Role: RoleFull}, "users")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, res.StatusCode)
	require.Empty(t, res.Payload)
	require.Equal(t, pb.AdminOperation_ADMIN_OP_DELETE_TABLE, conn.lastReq.GetOperation())
	require.JSONEq(t, `{"name":"users"}`, string(conn.lastReq.GetPayload()))
	require.Equal(t, "follower-3", conn.lastReq.GetForwardedFrom())
}

func TestGRPCForwardClient_NoLeaderReturnsErrLeaderUnavailable(t *testing.T) {
	dial := &stubConnFactory{conn: &stubForwardConn{}}
	fwd, _ := NewGRPCForwardClient(fixedResolver(""), dial, "f")

	_, err := fwd.ForwardCreateTable(context.Background(), AuthPrincipal{Role: RoleFull},
		CreateTableRequest{TableName: "t", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}})
	require.ErrorIs(t, err, ErrLeaderUnavailable)
	// Connection must not have been dialled when no leader is known.
	require.Empty(t, dial.lastAddr)
}

func TestGRPCForwardClient_DialErrorPropagated(t *testing.T) {
	dial := &stubConnFactory{dialErr: errors.New("network unreachable")}
	fwd, _ := NewGRPCForwardClient(fixedResolver("leader:1"), dial, "f")

	_, err := fwd.ForwardCreateTable(context.Background(), AuthPrincipal{Role: RoleFull},
		CreateTableRequest{TableName: "t", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "network unreachable")
}

func TestGRPCForwardClient_GRPCErrorPropagated(t *testing.T) {
	conn := &stubForwardConn{err: errors.New("rpc deadline exceeded")}
	dial := &stubConnFactory{conn: conn}
	fwd, _ := NewGRPCForwardClient(fixedResolver("leader:1"), dial, "f")

	_, err := fwd.ForwardDeleteTable(context.Background(), AuthPrincipal{Role: RoleFull}, "x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rpc deadline exceeded")
}

func TestGRPCForwardClient_ZeroStatusUpgradesTo502(t *testing.T) {
	// status_code 0 is the proto zero value — never emitted
	// intentionally by the leader. The forwarder must not treat
	// that as 200; mapping it to 502 surfaces the transport bug.
	conn := &stubForwardConn{resp: &pb.AdminForwardResponse{StatusCode: 0}}
	dial := &stubConnFactory{conn: conn}
	fwd, _ := NewGRPCForwardClient(fixedResolver("leader:1"), dial, "f")

	res, err := fwd.ForwardCreateTable(context.Background(), AuthPrincipal{Role: RoleFull},
		CreateTableRequest{TableName: "t", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, res.StatusCode)
}

func TestGRPCForwardClient_FillsMissingContentType(t *testing.T) {
	// A future leader-server change that omits ContentType must
	// still produce a SPA-readable response. The forwarder fills
	// in the default JSON content type defensively.
	conn := &stubForwardConn{resp: &pb.AdminForwardResponse{
		StatusCode: http.StatusCreated,
		Payload:    []byte(`{"name":"u"}`),
	}}
	dial := &stubConnFactory{conn: conn}
	fwd, _ := NewGRPCForwardClient(fixedResolver("leader:1"), dial, "f")

	res, _ := fwd.ForwardCreateTable(context.Background(), AuthPrincipal{Role: RoleFull},
		CreateTableRequest{TableName: "t", PartitionKey: CreateTableAttribute{Name: "id", Type: "S"}})
	require.Equal(t, "application/json; charset=utf-8", res.ContentType)
}
