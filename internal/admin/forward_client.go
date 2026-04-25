package admin

import (
	"context"
	"errors"
	"net/http"

	pb "github.com/bootjp/elastickv/proto"
	pkgerrors "github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
)

// LeaderForwarder is the contract the admin HTTP handler invokes
// when the local node is a follower (the source returned
// ErrTablesNotLeader). Implementations dial the current leader
// over the AdminForward gRPC service and return the leader's
// response in a transport-neutral shape so the handler can re-emit
// it verbatim.
//
// Defining this interface in the admin package — rather than wiring
// pb.AdminForwardClient directly into the handler — keeps the
// admin HTTP layer free of any proto-level coupling and lets tests
// substitute a deterministic stub. The bridge in main_admin.go
// provides the production implementation that uses
// kv.GRPCConnCache + the raft engine's leader address.
type LeaderForwarder interface {
	// ForwardCreateTable issues a forwarded CreateTable on the
	// leader's behalf. The response is the leader's structured
	// AdminForwardResponse re-shaped into ForwardResult so the
	// handler does not need to import proto.
	ForwardCreateTable(ctx context.Context, principal AuthPrincipal, in CreateTableRequest) (*ForwardResult, error)
	// ForwardDeleteTable is the delete-side counterpart.
	ForwardDeleteTable(ctx context.Context, principal AuthPrincipal, name string) (*ForwardResult, error)
}

// ForwardResult is the leader's response replayed for the SPA. The
// handler writes Payload verbatim with the given status code and
// content type, so a forwarded request is indistinguishable from a
// leader-direct call.
type ForwardResult struct {
	StatusCode  int
	Payload     []byte
	ContentType string
}

// ErrLeaderUnavailable is returned when the forwarder cannot reach
// any leader — typically during a Raft election or a cluster split.
// The handler maps it to 503 + Retry-After: 1 so the SPA / client
// re-issues the request after a short delay (acceptance criterion 3).
var ErrLeaderUnavailable = errors.New("admin: raft leader currently unavailable")

// LeaderAddressResolver returns the current Raft leader's address
// for the local node's group, or "" if no leader is known. The
// production wiring uses raftengine.Engine.LeaderAddr / the
// cluster's address map; tests inject a fixed string.
type LeaderAddressResolver func() string

// GRPCConnFactory is the small surface AdminForwardClient needs
// from kv.GRPCConnCache. Pulling out an interface lets tests
// substitute an in-memory dialer without spinning up a TCP
// listener and lets the bridge use the existing connection cache
// without copy-paste.
type GRPCConnFactory interface {
	// ConnFor returns a gRPC client connection to addr, reusing
	// the cached entry if one exists. addr "" is a programming
	// error and may panic; callers must check leader-empty before
	// dialling.
	ConnFor(addr string) (PBAdminForwardClient, error)
}

// PBAdminForwardClient narrows pb.AdminForwardClient to just the
// methods this package uses. The narrower interface keeps the test
// stub implementation small.
type PBAdminForwardClient interface {
	Forward(ctx context.Context, in *pb.AdminForwardRequest, opts ...grpcCallOption) (*pb.AdminForwardResponse, error)
}

// grpcCallOption is a re-export of google.golang.org/grpc.CallOption
// kept private so callers do not import proto's grpc dep directly.
type grpcCallOption = interface{}

// gRPCForwardClient is the production LeaderForwarder. Construct
// one with NewGRPCForwardClient. Two collaborators are required:
//   - resolver: returns the current leader address, or "" if absent
//   - dial:     turns an address into a PBAdminForwardClient (the
//     bridge wraps kv.GRPCConnCache to satisfy this)
//
// nodeID is echoed into the leader's audit log via
// AdminForwardRequest.forwarded_from (acceptance criterion 6).
type gRPCForwardClient struct {
	resolver LeaderAddressResolver
	dial     GRPCConnFactory
	nodeID   string
}

// NewGRPCForwardClient constructs the production LeaderForwarder.
// All three parameters must be non-nil / non-empty; otherwise the
// constructor returns nil and a wiring-error so a misconfigured
// build refuses to start rather than producing 500s at runtime.
func NewGRPCForwardClient(resolver LeaderAddressResolver, dial GRPCConnFactory, nodeID string) (LeaderForwarder, error) {
	if resolver == nil {
		return nil, errors.New("admin forwarder: leader address resolver is required")
	}
	if dial == nil {
		return nil, errors.New("admin forwarder: gRPC connection factory is required")
	}
	if nodeID == "" {
		return nil, errors.New("admin forwarder: node id is required for audit log enrichment")
	}
	return &gRPCForwardClient{resolver: resolver, dial: dial, nodeID: nodeID}, nil
}

// ForwardCreateTable serialises `in` as JSON and dispatches the
// CreateTable operation to the leader. Returns ErrLeaderUnavailable
// when no leader address is known; gRPC-level errors are wrapped
// and surfaced unchanged so the handler can decide whether to
// retry, log, or 500.
func (c *gRPCForwardClient) ForwardCreateTable(ctx context.Context, principal AuthPrincipal, in CreateTableRequest) (*ForwardResult, error) {
	payload, err := json.Marshal(in)
	if err != nil {
		// CreateTableRequest is plain string fields; Marshal
		// cannot fail in practice. Surface the error rather than
		// silently dropping the request.
		return nil, pkgerrors.Wrap(err, "admin forward: marshal create-table request")
	}
	return c.forward(ctx, pb.AdminOperation_ADMIN_OP_CREATE_TABLE, principal, payload)
}

// ForwardDeleteTable serialises the table name as `{"name":"..."}`
// to match the leader-side handleDelete contract.
func (c *gRPCForwardClient) ForwardDeleteTable(ctx context.Context, principal AuthPrincipal, name string) (*ForwardResult, error) {
	payload, err := json.Marshal(struct {
		Name string `json:"name"`
	}{Name: name})
	if err != nil {
		return nil, pkgerrors.Wrap(err, "admin forward: marshal delete-table request")
	}
	return c.forward(ctx, pb.AdminOperation_ADMIN_OP_DELETE_TABLE, principal, payload)
}

func (c *gRPCForwardClient) forward(ctx context.Context, op pb.AdminOperation, principal AuthPrincipal, payload []byte) (*ForwardResult, error) {
	addr := c.resolver()
	if addr == "" {
		return nil, ErrLeaderUnavailable
	}
	cli, err := c.dial.ConnFor(addr)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "admin forward: dial leader")
	}
	resp, err := cli.Forward(ctx, &pb.AdminForwardRequest{
		Principal: &pb.AdminPrincipal{
			AccessKey: principal.AccessKey,
			Role:      string(principal.Role),
		},
		Operation:     op,
		Payload:       payload,
		ForwardedFrom: c.nodeID,
	})
	if err != nil {
		return nil, pkgerrors.Wrap(err, "admin forward: rpc")
	}
	out := &ForwardResult{
		StatusCode:  int(resp.GetStatusCode()),
		Payload:     resp.GetPayload(),
		ContentType: resp.GetContentType(),
	}
	if out.ContentType == "" {
		// The leader server always sets a content type, but be
		// defensive: a future change that drops it must not
		// produce a SPA response with no Content-Type header.
		out.ContentType = "application/json; charset=utf-8"
	}
	if out.StatusCode == 0 {
		// status_code 0 is the proto's zero value, which the
		// leader server never emits intentionally. Treat it as a
		// transport bug rather than a 200, since 0 is not a valid
		// HTTP status.
		out.StatusCode = http.StatusBadGateway
	}
	return out, nil
}
