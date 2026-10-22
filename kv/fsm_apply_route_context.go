package kv

import "context"

type applyRouteHistoryKey struct{}

type applyRouteHistory struct {
	history RouteHistory
}

// withApplyRouteHistory scopes a caller-supplied immutable history to an apply
// request. The caller owns proof validation and must not mutate the history or
// its snapshots while the context is in use. This helper does not infer routes.
func withApplyRouteHistory(ctx context.Context, history RouteHistory) context.Context {
	return context.WithValue(ctx, applyRouteHistoryKey{}, applyRouteHistory{history: history})
}

func (f *kvFSM) routeHistoryForContext(ctx context.Context) RouteHistory {
	if routes, ok := ctx.Value(applyRouteHistoryKey{}).(applyRouteHistory); ok {
		return routes.history
	}
	return f.routes
}
