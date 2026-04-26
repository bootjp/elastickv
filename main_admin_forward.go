package main

import (
	"log/slog"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// adminForwardConnFactory bridges kv.GRPCConnCache to the
// admin.GRPCConnFactory interface. The cache hands back
// *grpc.ClientConn; the admin layer wants a typed
// PBAdminForwardClient — pb.NewAdminForwardClient(conn) is the
// generated constructor that adapts one to the other. Defining the
// bridge here (rather than in internal/admin) keeps the admin package
// free of any kv-package import.
type adminForwardConnFactory struct {
	cache *kv.GRPCConnCache
}

// ConnFor satisfies admin.GRPCConnFactory. addr "" is rejected at the
// LeaderForwarder layer (ErrLeaderUnavailable) before reaching this
// method, so we surface the conn-cache's own error vocabulary
// unchanged when the dial fails.
func (f *adminForwardConnFactory) ConnFor(addr string) (admin.PBAdminForwardClient, error) {
	conn, err := f.cache.ConnFor(addr)
	if err != nil {
		return nil, errors.Wrap(err, "admin forward: dial leader")
	}
	return pb.NewAdminForwardClient(conn), nil
}

// buildLeaderForwarder is the production constructor for the
// follower-side LeaderForwarder. It resolves the current leader's
// gRPC address through the local Coordinator (which queries the
// default group's raft engine), reuses one cached gRPC connection per
// address, and stamps the local nodeID onto every forwarded request
// so the leader's audit log carries the trace.
//
// All three inputs are required; a nil coordinate, a nil connCache,
// or an empty nodeID is a wiring bug and surfaces as a startup error
// rather than a runtime 500.
func buildLeaderForwarder(coordinate kv.Coordinator, connCache *kv.GRPCConnCache, nodeID string) (admin.LeaderForwarder, error) {
	if coordinate == nil {
		return nil, errors.New("admin forward bridge: coordinator is required")
	}
	if connCache == nil {
		return nil, errors.New("admin forward bridge: gRPC connection cache is required")
	}
	resolver := func() string { return coordinate.RaftLeader() }
	factory := &adminForwardConnFactory{cache: connCache}
	fwd, err := admin.NewGRPCForwardClient(resolver, factory, nodeID)
	if err != nil {
		return nil, errors.Wrap(err, "build leader forwarder")
	}
	return fwd, nil
}

// adminForwardServerDeps is the small bundle the gRPC ForwardServer
// needs to be reachable from a follower's bridge. Collecting them in
// a struct keeps startRaftServers' signature tractable as the wiring
// surface grows. tables + roles are required; buckets is optional
// (a build that ships without the S3 adapter sets it to nil and the
// ForwardServer's S3 dispatch returns 501 for those operations).
type adminForwardServerDeps struct {
	tables  admin.TablesSource
	buckets admin.BucketsSource
	roles   admin.RoleStore
}

// readyForRegistration reports whether the bundle has enough
// collaborators to construct + register a ForwardServer.
// RoleStore is always required (the principal re-evaluation step
// has no fallback). At least one of TablesSource or BucketsSource
// must be present — registering with neither would respond 501 to
// every operation, which is functionally identical to not
// registering at all. The dispatcher emits 501 for whichever
// source is nil so an S3-only or Dynamo-only build still serves
// its half of the admin surface (Codex P1 on PR #673: an S3-only
// cluster previously skipped registration entirely and surfaced
// follower forwards as gRPC Unimplemented / 503).
func (d adminForwardServerDeps) readyForRegistration() bool {
	return d.roles != nil && (d.tables != nil || d.buckets != nil)
}

// registerAdminForwardServer attaches the leader-side gRPC
// AdminForward service to gs when the bundle is ready (TablesSource +
// RoleStore both present). Centralising the call here keeps the
// proto-level Register* import out of main.go's startRaftServers and
// lets the readyForRegistration gate decide silently whether this
// build serves forwarded admin writes at all.
func registerAdminForwardServer(gs *grpc.Server, deps adminForwardServerDeps, logger *slog.Logger) {
	if !deps.readyForRegistration() {
		return
	}
	srv := admin.NewForwardServer(deps.tables, deps.roles, logger)
	if deps.buckets != nil {
		srv = srv.WithBucketsSource(deps.buckets)
	}
	pb.RegisterAdminForwardServer(gs, srv)
}

// roleStoreFromFlags builds the same access-key → role map that
// admin.Config.RoleIndex produces, but from the raw flag strings so
// the gRPC ForwardServer registration in startRaftServers does not
// need to wait for startAdminFromFlags to parse the admin config.
// Returns nil when no keys are configured at all — that shape is the
// "admin auth disabled" signal adminForwardServerDeps consumes to
// skip registration.
func roleStoreFromFlags(fullKeys, readOnlyKeys []string) admin.RoleStore {
	if len(fullKeys) == 0 && len(readOnlyKeys) == 0 {
		return nil
	}
	idx := make(map[string]admin.Role, len(fullKeys)+len(readOnlyKeys))
	for _, k := range fullKeys {
		idx[k] = admin.RoleFull
	}
	for _, k := range readOnlyKeys {
		// Overlap with FullAccessKeys is rejected at admin.Config.Validate
		// time during startAdminFromFlags. We can't replicate that here
		// without parsing the full config, so the ReadOnlyAccessKeys loop
		// runs second to mirror RoleIndex's "last-write-wins-but-only-for-
		// non-overlapping-keys" semantics — overlap is a startup error
		// that the HTTP path will surface.
		idx[k] = admin.RoleReadOnly
	}
	return admin.MapRoleStore(idx)
}
