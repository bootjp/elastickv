package main

import (
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
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
// surface grows. All fields are required; a missing one means the
// ForwardServer is not registered (single-node / leader-only build).
type adminForwardServerDeps struct {
	tables admin.TablesSource
	roles  admin.RoleStore
}

// readyForRegistration reports whether the bundle has enough
// collaborators to construct + register a ForwardServer. Both fields
// must be non-nil; a nil TablesSource means the build ships without
// the dynamo adapter, and a nil RoleStore means admin auth is not
// configured. Either way, registering the gRPC service would 500
// every forwarded call, so we silently skip registration instead.
func (d adminForwardServerDeps) readyForRegistration() bool {
	return d.tables != nil && d.roles != nil
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
