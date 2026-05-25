package main

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// capabilityFanoutTimeout bounds the whole §4 GetCapability fan-out the
// EnableStorageEnvelope cutover runs before proposing. The helper
// caps the entire fan-out (not per-probe) by this value regardless of
// member count, so a slow/unreachable peer cannot stall the operator's
// cutover RPC indefinitely — it surfaces as a Reachable=false verdict
// and the cutover fails closed. 5s is generous for a small-cluster
// GetCapability round-trip.
const capabilityFanoutTimeout = 5 * time.Second

// buildEncryptionCapabilityFanout builds the §4 capability fan-out
// closure shared across every shard's EncryptionAdmin server, or nil
// when encryption mutators are disabled (the cutover RPC is then
// unreachable, so the fan-out would never run). It owns a dedicated
// kv.GRPCConnCache — NOT the --adminEnabled-gated admin-forward cache —
// so the fan-out dials whenever encryption mutators are enabled,
// independent of the admin HTTP surface; the cache is drained on
// context cancellation via eg.
func buildEncryptionCapabilityFanout(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, enableMutators bool) adapter.CapabilityFanoutFn {
	if !enableMutators {
		return nil
	}
	fanoutConnCache := &kv.GRPCConnCache{}
	eg.Go(func() error {
		<-ctx.Done()
		if err := fanoutConnCache.Close(); err != nil {
			return errors.Wrap(err, "close encryption capability fan-out gRPC connection cache")
		}
		return nil
	})
	return buildCapabilityFanoutFn(runtimes, fanoutConnCache, capabilityFanoutTimeout)
}

// buildCapabilityFanoutFn assembles the adapter.CapabilityFanoutFn the
// EnableStorageEnvelope server invokes for its §4 pre-flight check. The
// returned closure snapshots the live membership of every Raft group
// (the §4.1 "voter ∪ learner of every group" contract) and fans
// GetCapability out to each unique node.
//
// dial reuses connCache so repeated cutover attempts share one
// *grpc.ClientConn per peer; the cleanup closure is a no-op because the
// cache owns the connection lifecycle (closed on shutdown by the
// caller, not per-probe). A snapshot-build error fails the closure so
// the cutover refuses rather than proposing against a membership view
// it could not fully enumerate (fail-closed, §4.3 no-partial-success).
func buildCapabilityFanoutFn(runtimes []*raftGroupRuntime, connCache *kv.GRPCConnCache, timeout time.Duration) adapter.CapabilityFanoutFn {
	dial := func(_ context.Context, address string) (pb.EncryptionAdminClient, func(), error) {
		conn, err := connCache.ConnFor(address)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "capability fan-out: dial %s", address)
		}
		return pb.NewEncryptionAdminClient(conn), func() {}, nil
	}
	return func(ctx context.Context) (admin.CapabilityFanoutResult, error) {
		snapshot, err := capabilityRouteSnapshot(ctx, runtimes)
		if err != nil {
			return admin.CapabilityFanoutResult{}, err
		}
		return admin.CapabilityFanout(ctx, snapshot, dial, timeout)
	}
}

// configReader is the narrow membership-view surface
// capabilityRouteSnapshot needs from a Raft engine. raftengine.Engine
// satisfies it structurally; defining it here lets the snapshot
// builder be unit-tested with a stub that returns a Configuration
// error (the fail-closed path) without standing up a full engine.
type configReader interface {
	Configuration(ctx context.Context) (raftengine.Configuration, error)
}

// groupConfigSource pairs a group id with its membership reader.
type groupConfigSource struct {
	groupID uint64
	engine  configReader
}

// capabilityRouteSnapshot builds the all-groups admin.RouteSnapshot
// from each runtime's live Raft Configuration. It reads each engine
// via snapshotEngine() (engineMu.RLock) because the fan-out closure
// runs during live EnableStorageEnvelope RPCs, concurrently with a
// possible Close() that clears the engine field — a direct field read
// would be a data race. A nil runtime/engine is skipped.
func capabilityRouteSnapshot(ctx context.Context, runtimes []*raftGroupRuntime) (admin.RouteSnapshot, error) {
	sources := make([]groupConfigSource, 0, len(runtimes))
	for _, rt := range runtimes {
		eng := rt.snapshotEngine()
		if eng == nil {
			continue
		}
		sources = append(sources, groupConfigSource{groupID: rt.spec.id, engine: eng})
	}
	return routeSnapshotFromSources(ctx, sources)
}

// routeSnapshotFromSources maps each group's Configuration into the
// admin.RouteSnapshot. Any Configuration error aborts the whole
// snapshot so the caller fails closed — the cutover must never
// proceed against a membership view it could not fully enumerate.
func routeSnapshotFromSources(ctx context.Context, sources []groupConfigSource) (admin.RouteSnapshot, error) {
	groups := make([]admin.RouteGroup, 0, len(sources))
	for _, s := range sources {
		cfg, err := s.engine.Configuration(ctx)
		if err != nil {
			return admin.RouteSnapshot{}, errors.Wrapf(err, "capability fan-out: configuration for group %d", s.groupID)
		}
		groups = append(groups, routeGroupFromConfiguration(s.groupID, cfg))
	}
	return admin.RouteSnapshot{Groups: groups}, nil
}

// routeGroupFromConfiguration maps one Raft group's live Configuration
// to an admin.RouteGroup: each server becomes a RouteMember keyed by
// etcd.DeriveNodeID(srv.ID) (the node identity the rest of the
// encryption stack uses), split into Voters / Learners on Suffrage.
// Empty suffrage counts as voter, matching raftengine's convention
// that an unannotated peer (e.g. mid-WAL-replay) is a voter.
func routeGroupFromConfiguration(groupID uint64, cfg raftengine.Configuration) admin.RouteGroup {
	group := admin.RouteGroup{GroupID: groupID}
	for _, srv := range cfg.Servers {
		member := admin.RouteMember{
			FullNodeID: etcdraftengine.DeriveNodeID(srv.ID),
			Address:    srv.Address,
		}
		if srv.Suffrage == etcdraftengine.SuffrageLearner {
			group.Learners = append(group.Learners, member)
		} else {
			group.Voters = append(group.Voters, member)
		}
	}
	return group
}
