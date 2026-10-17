package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

var _ serverwiring.Leader = (*kv.ShardedCoordinator)(nil)

// setupDistributionWatcherAndAutoSplit starts the catalog watcher, keeps the
// KeyViz sampler's routes reconciled with the catalog, and starts the
// auto-split scheduler when --autoSplit is set. It returns the runtime switch
// the admin API toggles, or nil when auto-split is disabled.
func setupDistributionWatcherAndAutoSplit(
	ctx context.Context,
	eg *errgroup.Group,
	catalog *distribution.CatalogStore,
	engine *distribution.Engine,
	distServer *adapter.DistributionServer,
	coordinate *kv.ShardedCoordinator,
	sampler *keyviz.MemSampler,
	observer autosplit.Observer,
) (adapter.AutoSplitRuntime, error) {
	cfg, err := autoSplitFlags.SchedulerConfig(coordinate, keyVizFlags)
	if err != nil {
		return nil, errors.Wrap(err, "configure auto-split")
	}
	cfg.Observer = observer
	var runtime adapter.AutoSplitRuntime
	if cfg.Enabled {
		switchRuntime := autosplit.NewRuntimeSwitch(true)
		runtime = switchRuntime
		cfg.KillSwitch = switchRuntime.KillSwitch
	}
	var reconciler *autosplit.RouteReconciler
	if sampler != nil {
		reconciler = autosplit.NewRouteReconciler(sampler)
		initialSnapshot, snapshotErr := catalog.Snapshot(ctx)
		if snapshotErr != nil {
			return nil, errors.Wrap(snapshotErr, "load initial catalog for keyviz reconciliation")
		}
		reconciler.Reconcile(initialSnapshot.Routes)
		cfg.Reconciler = reconciler
	}
	eg.Go(func() error {
		return runDistributionCatalogWatcher(ctx, catalog, engine, func(snapshot distribution.CatalogSnapshot) {
			if reconciler != nil {
				reconciler.Reconcile(snapshot.Routes)
			}
		})
	})
	serverwiring.StartScheduler(ctx, eg, catalog, distServer, coordinate, sampler, cfg)
	return runtime, nil
}
