package distribution

import (
	"context"

	"github.com/cockroachdb/errors"
)

var ErrEngineRequired = errors.New("engine is required")

// EnsureCatalogSnapshot makes engine and durable catalog consistent at startup.
//
// If the durable catalog is empty (version 0, no routes), the current in-memory
// engine routes are persisted as the initial catalog snapshot with generated
// non-zero RouteIDs. Then the resolved snapshot is applied back to engine.
func EnsureCatalogSnapshot(ctx context.Context, catalog *CatalogStore, engine *Engine) (CatalogSnapshot, error) {
	if err := ensureCatalogStore(catalog); err != nil {
		return CatalogSnapshot{}, err
	}
	if engine == nil {
		return CatalogSnapshot{}, errors.WithStack(ErrEngineRequired)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	snapshot, err := catalog.Snapshot(ctx)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if snapshot.Version == 0 && len(snapshot.Routes) == 0 {
		seedRoutes := routeDescriptorsFromEngine(engine.Stats())
		snapshot, err = catalog.Save(ctx, 0, seedRoutes)
		if err != nil {
			return CatalogSnapshot{}, err
		}
	}
	if err := engine.ApplySnapshot(snapshot); err != nil {
		return CatalogSnapshot{}, err
	}
	return snapshot, nil
}

func routeDescriptorsFromEngine(routes []Route) []RouteDescriptor {
	out := make([]RouteDescriptor, len(routes))
	routeID := uint64(1)
	for i, route := range routes {
		out[i] = RouteDescriptor{
			RouteID:       routeID,
			Start:         cloneBytes(route.Start),
			End:           cloneBytes(route.End),
			GroupID:       route.GroupID,
			State:         route.State,
			ParentRouteID: 0,
		}
		routeID++
	}
	return out
}
