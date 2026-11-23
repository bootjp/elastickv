package kv

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

type applyRouteHistoryKey struct{}

type applyRouteHistory struct {
	history           RouteHistory
	migrationEvidence bool
}

// withApplyRouteHistory scopes a caller-supplied immutable history to an apply
// request. The caller owns proof validation and must not mutate the history or
// its snapshots while the context is in use. This helper does not infer routes.
func withApplyRouteHistory(ctx context.Context, history RouteHistory) context.Context {
	return context.WithValue(ctx, applyRouteHistoryKey{}, applyRouteHistory{history: history, migrationEvidence: true})
}

func (f *kvFSM) routeHistoryForContext(ctx context.Context) RouteHistory {
	if routes, ok := ctx.Value(applyRouteHistoryKey{}).(applyRouteHistory); ok {
		return routes.history
	}
	return f.routes
}

var errAppliedRouteCatalog = errors.New("could not read the applied route catalog")

// A restarting catalog voter can replay a split and a subsequent pinned write
// before its asynchronous watcher delivers the split. The catalog rows in this
// group's own log already describe the write's version. Use that exact snapshot
// for this apply only, preserving ownership, fence and timestamp-floor checks.
func (f *kvFSM) withAppliedCatalogForReplay(ctx context.Context, req *pb.Request) (context.Context, error) {
	version, pinned := DecodeObservedRouteVersion(req.GetObservedRouteVersion())
	if !pinned || f.routes == nil || req.GetPhase() == pb.Phase_ABORT {
		return ctx, nil
	}
	if !f.catalogWatcherBehind(version) {
		return ctx, nil
	}
	catalog := distribution.NewCatalogStore(f.store)
	snapshot, err := catalog.Snapshot(ctx)
	if err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errAppliedRouteCatalog))
	}
	if snapshot.Version != version {
		return ctx, nil
	}
	engine := distribution.NewEngine()
	if err := engine.ApplySnapshot(snapshot); err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errAppliedRouteCatalog))
	}
	return context.WithValue(ctx, applyRouteHistoryKey{}, applyRouteHistory{history: WrapDistributionEngine(engine)}), nil
}

func (f *kvFSM) catalogWatcherBehind(version uint64) bool {
	if _, ok := f.routes.SnapshotAt(version); ok {
		return false
	}
	current, ok := f.routes.Current()
	if !ok || current.Version() >= version {
		return false
	}
	owner, ok := current.OwnerOf(distribution.CatalogVersionKey())
	return ok && owner != 0 && owner == f.shardGroupID
}
