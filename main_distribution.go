package main

import (
	"context"
	"log/slog"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func newStartupShardStore(engine *distribution.Engine, groups map[uint64]*kv.ShardGroup, advanceReadWatermark bool) *kv.ShardStore {
	shardStore := kv.NewShardStore(engine, groups)
	if advanceReadWatermark {
		shardStore.WithReadWatermarkAdvance()
	}
	return shardStore
}

type distributionStartupInput struct {
	ctx             context.Context
	eg              *errgroup.Group
	cancel          context.CancelFunc
	cleanup         *internalutil.CleanupStack
	runtimes        []*raftGroupRuntime
	cfg             runtimeConfig
	coordinate      *kv.ShardedCoordinator
	shardStore      *kv.ShardStore
	defaultGroup    *kv.ShardGroup
	encWiring       encryptionWriteWiring
	raftID          string
	sidecarPath     string
	sampler         *keyviz.MemSampler
	readTracker     *kv.ActiveTimestampTracker
	metricsRegistry *monitoring.Registry
	clock           *kv.HLC
	tsoWiring       coordinatorTSOWiring
	// shardGroups feeds the split-migration promotion/voter factories, which
	// resolve per-group leaders rather than going through the coordinator.
	shardGroups     map[uint64]*kv.ShardGroup
	snapshotOffload snapshotOffloadFlags
	keyViz          *serverwiring.KeyVizFlags
	autoSplit       *serverwiring.AutoSplitFlags
	nodeAuth        nodeAuthFlags
}

type distributionStartup struct {
	defaultRuntime     *raftGroupRuntime
	distServer         *adapter.DistributionServer
	registrationCtx    context.Context
	rotationSupervisor *bootstrapRotationRuntime
	// distCatalog is the durable route catalog the admin backup store reads
	// to capture a route snapshot at a pinned timestamp. It is built here, so
	// it has to travel back out rather than be rebuilt at the call site.
	distCatalog      *distribution.CatalogStore
	autoSplitRuntime adapter.AutoSplitRuntime
}

func startDistributionStartup(in distributionStartupInput) (distributionStartup, error) {
	auth, err := in.nodeAuth.loadGRPCAuth()
	if err != nil {
		return distributionStartup{}, err
	}
	internalutil.ConfigureGRPCForwardToken(auth.migrationToken)
	distCatalog, catalogRuntime, defaultRuntime, err := setupDistributionRuntimeDependencies(
		in.ctx, in.eg, in.runtimes, in.cfg.engine,
		in.coordinate, in.defaultGroup, in.cfg.defaultGroup,
		in.encWiring, in.raftID, in.sidecarPath)
	if err != nil {
		in.cancel()
		return distributionStartup{}, err
	}
	// Seed AFTER setupDistributionCatalog so the sampler picks up the
	// catalog-assigned RouteIDs. EnsureCatalogSnapshot inside
	// setupDistributionCatalog applies a snapshot back into the engine
	// with durable non-zero RouteIDs; seeding earlier would register
	// the placeholder zero IDs from buildEngine and Observe would miss
	// every dispatched mutation.
	serverwiring.SeedRoutes(in.sampler, in.cfg.engine)

	catalogWatchConns := newBootstrapRuntimeConnCache(in.runtimes)
	in.cleanup.Add(func() { _ = catalogWatchConns.Close() })
	in.eg.Go(func() error {
		return runDistributionCatalogStream(in.ctx, catalogRuntime, catalogWatchConns, in.cfg.engine)
	})
	serverwiring.StartFlusher(in.ctx, in.eg, in.sampler)
	startKeyVizLeaderTermPublisher(in.ctx, in.eg, in.sampler, in.runtimes)
	startMemoryWatchdog(in.ctx, in.eg, in.cancel)
	splitMigrationConnCache := newBootstrapRuntimeConnCache(in.runtimes)
	in.cleanup.Add(func() { _ = splitMigrationConnCache.Close() })
	distServer := adapter.NewDistributionServer(
		in.cfg.engine,
		distCatalog,
		adapter.WithDistributionCoordinator(in.coordinate),
		adapter.WithDistributionTimestampAllocator(in.tsoWiring.serverAllocator),
		adapter.WithDistributionTSOActivationGate(in.tsoWiring.authorizeActivation),
		adapter.WithDistributionActiveTimestampTracker(in.readTracker),
		adapter.WithCatalogWatchLeaderCheck(func() bool {
			engine := catalogRuntime.snapshotEngine()
			return engine != nil && engine.State() == raftengine.StateLeader
		}),
		adapter.WithDistributionFilesystemObserver(in.metricsRegistry.FileSystemObserver()),
		adapter.WithDistributionKnownRaftGroups(shardGroupIDs(in.shardGroups)...),
		adapter.WithSplitMigrationCapabilityGate(newSplitMigrationCapabilityGate(
			splitMigrationCapabilityPeerSourceForRuntimes(in.runtimes),
			splitMigrationCapabilityProbeTimeout,
			bootstrapSplitMigrationProbe(in.runtimes),
			in.nodeAuth.splitMigrationLocalReadinessGate,
		)),
		adapter.WithSplitPromotionClientFactory(splitPromotionClientFactory(
			splitPromotionTargetLeaderResolver(in.shardGroups),
			splitMigrationConnCache,
			auth.migrationToken,
		)),
		adapter.WithSplitMigrationClientFactory(splitMigrationClientFactory(
			splitMigrationGroupLeaderResolver(in.shardGroups),
			splitMigrationConnCache,
			auth.migrationToken,
		)),
		adapter.WithSplitMigrationVoterFactory(splitMigrationVoterFactory(
			in.shardGroups,
			splitMigrationConnCache,
			auth.migrationToken,
		)),
		adapter.WithSplitMigrationApplyMemberSnapshotFactory(splitMigrationApplyMemberSnapshotFactory(
			in.shardGroups,
			splitMigrationConnCache,
			auth.migrationToken,
		)),
		adapter.WithSplitMigrationGroupClientFactory(splitMigrationGroupClientFactory(
			splitMigrationGroupLeaderResolver(in.shardGroups),
			splitMigrationConnCache,
			auth.migrationToken,
		)),
		// Do not advertise cap_migration_v2 before protocol coverage is accepted.
		adapter.WithSplitJobRunnerReadinessGate(in.nodeAuth.splitMigrationLocalReadinessGate),
		migrationOwnershipRefreshOption(in.shardGroups, catalogRuntime, splitMigrationConnCache, auth.migrationToken, in.nodeAuth.splitMigrationPeerAuthEnforced),
		migrationPartitionRuntimeOption(in.shardStore, in.shardGroups, splitMigrationConnCache, auth.migrationToken, in.nodeAuth.splitMigrationPeerAuthEnforced),
	)
	autoSplitRuntime, err := setupDistributionWatcherAndAutoSplit(
		in.ctx,
		in.eg,
		distCatalog,
		in.cfg.engine,
		distServer,
		in.coordinate,
		in.sampler,
		autosplit.NewPrometheusObserver(in.metricsRegistry.Registerer()),
		in.keyViz,
		in.autoSplit,
	)
	if err != nil {
		return distributionStartup{}, err
	}
	startMonitoringCollectors(in.ctx, in.metricsRegistry, in.runtimes, in.clock, in.encWiring.cache)
	startFSMCompactorIfEnabled(in.ctx, in.eg, in.runtimes, in.readTracker)
	// §4 physical snapshot offload. Opt-in, and a hard error when
	// configured-but-unbuildable: an operator who set a backup
	// destination and silently got no backups is worse off than one
	// whose node refused to start.
	if err := startSnapshotOffload(
		in.ctx, in.eg, in.snapshotOffload, in.runtimes, *raftDir, in.raftID, in.cfg.multi,
		in.metricsRegistry.SnapshotOffloadObserver(), slog.Default(),
	); err != nil {
		in.cancel()
		return distributionStartup{}, err
	}
	return distributionStartup{
		defaultRuntime:   defaultRuntime,
		distServer:       distServer,
		distCatalog:      distCatalog,
		autoSplitRuntime: autoSplitRuntime,
	}, nil
}

func startDistributionSplitJobRunner(ctx context.Context, eg *errgroup.Group, server *adapter.DistributionServer, ready migrationReadinessGate) {
	if eg == nil || server == nil {
		return
	}
	if ready.check(ctx) != nil {
		return
	}
	eg.Go(func() error {
		return server.RunSplitJobRunner(ctx)
	})
}

func distributionCatalogStoreForGroup(runtimes []*raftGroupRuntime, groupID uint64) *distribution.CatalogStore {
	for _, rt := range runtimes {
		if rt == nil || rt.store == nil {
			continue
		}
		if rt.spec.id == groupID {
			return distribution.NewCatalogStore(rt.store,
				distribution.WithCatalogRouteDescriptorV2Writes(true),
				distribution.WithCatalogMigrationStoreResolver(catalogMigrationStoreResolver(runtimes)),
				distribution.WithCatalogMigrationRetireResolver(catalogMigrationRetireResolver(runtimes)),
			)
		}
	}
	return nil
}

func catalogMigrationStoreResolver(runtimes []*raftGroupRuntime) distribution.CatalogMigrationStoreResolver {
	return func(groupID uint64) (store.MVCCStore, error) {
		for _, rt := range runtimes {
			if rt == nil || rt.store == nil {
				continue
			}
			if rt.spec.id == groupID {
				return rt.store, nil
			}
		}
		return nil, errors.Newf("migration target store is not available for group %d", groupID)
	}
}

func catalogMigrationRetireResolver(runtimes []*raftGroupRuntime) distribution.CatalogMigrationRetireResolver {
	return func(ctx context.Context, groupID, jobID uint64) error {
		for _, rt := range runtimes {
			if rt == nil || rt.spec.id != groupID {
				continue
			}
			engine := rt.snapshotEngine()
			if engine == nil {
				break
			}
			cmd, err := kv.MarshalMigrationRetireCommand(jobID)
			if err != nil {
				return errors.WithStack(err)
			}
			result, err := engine.Propose(ctx, cmd)
			if err != nil {
				return errors.WithStack(err)
			}
			if result == nil {
				return errors.Newf("migration retire proposal returned nil result for group %d job %d", groupID, jobID)
			}
			if err, ok := result.Response.(error); ok {
				return errors.WithStack(err)
			}
			return nil
		}
		return errors.Newf("migration target engine is not available for group %d", groupID)
	}
}

func setupDistributionCatalog(
	ctx context.Context,
	runtimes []*raftGroupRuntime,
	engine *distribution.Engine,
) (*distribution.CatalogStore, error) {
	if err := validateNoExistingCommitTSFenceActivation(ctx, runtimes, bootstrapDomainForRuntimes(runtimes) != nil); err != nil {
		return nil, err
	}
	catalogGroupID, err := distributionCatalogGroupID(engine)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve distribution catalog group")
	}
	distCatalog := distributionCatalogStoreForGroup(runtimes, catalogGroupID)
	if distCatalog == nil {
		return nil, errors.WithStack(errors.Newf("distribution catalog store is not available for group %d", catalogGroupID))
	}
	if handled, err := loadProtectedStartupCatalog(ctx, runtimes, catalogGroupID, distCatalog, engine); handled {
		return distCatalog, err
	}
	// EnsureCatalogSnapshot may Save through the direct (non-raft) write
	// path. When the §7.1 storage envelope is active and this load's
	// writer registration has not yet committed, that Save fails closed
	// with store.ErrWriterNotRegistered (Stage 7a-2). retryUntilRegistered
	// retries the bootstrap until the registration goroutine — armed
	// before this call in setupDistributionAndRegistration — commits and
	// the gate clears. The common cases (populated catalog → no-op Save,
	// or pre-cutover → cleartext Save) never hit the gate and return on
	// the first attempt.
	//
	// Idempotency requirement: the retry re-invokes EnsureCatalogSnapshot
	// from scratch on each ErrWriterNotRegistered, so it MUST be
	// re-entrant — on the populated-catalog path it is a version-unchanged
	// no-op Save (no mutation, no nonce), so re-running it is safe.
	if err := retryUntilRegistered(ctx, "distribution catalog bootstrap", func() error {
		_, e := distribution.EnsureCatalogSnapshot(ctx, distCatalog, engine)
		return errors.Wrap(e, "ensure catalog snapshot")
	}); err != nil {
		return nil, errors.Wrapf(err, "initialize distribution catalog")
	}
	return distCatalog, nil
}

func distributionCatalogGroupID(engine *distribution.Engine) (uint64, error) {
	if engine == nil {
		return 0, errors.New("distribution engine is required")
	}
	route, ok := engine.GetRoute(distribution.CatalogVersionKey())
	if !ok {
		return 0, errors.New("no shard route for distribution catalog key")
	}
	if route.GroupID == 0 {
		return 0, errors.New("invalid shard route for distribution catalog key")
	}
	return route.GroupID, nil
}

func runDistributionCatalogWatcher(
	ctx context.Context,
	catalog *distribution.CatalogStore,
	engine *distribution.Engine,
	observers ...distribution.CatalogSnapshotObserver,
) error {
	var opts []distribution.CatalogWatcherOption
	if len(observers) > 0 {
		opts = append(opts, distribution.WithCatalogWatcherSnapshotObserver(func(snapshot distribution.CatalogSnapshot) {
			for _, observer := range observers {
				if observer != nil {
					observer(snapshot)
				}
			}
		}))
	}
	if err := distribution.RunCatalogWatcher(ctx, catalog, engine, nil, opts...); err != nil {
		return errors.Wrapf(err, "catalog watcher failed")
	}
	return nil
}

func setupDistributionRuntimeDependencies(
	runCtx context.Context,
	eg *errgroup.Group,
	runtimes []*raftGroupRuntime,
	engine *distribution.Engine,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	defaultGroupID uint64,
	encWiring encryptionWriteWiring,
	raftID string,
	sidecarPath string,
) (*distribution.CatalogStore, *raftGroupRuntime, *raftGroupRuntime, error) {
	catalog, err := setupDistributionAndRegistration(
		runCtx, eg, runtimes, engine, coordinate, defaultGroup, encWiring, raftID, sidecarPath)
	if err != nil {
		return nil, nil, nil, err
	}
	catalogGroupID, err := distributionCatalogGroupID(engine)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "resolve distribution catalog group runtime")
	}
	catalogRuntime := findDefaultGroupRuntime(runtimes, catalogGroupID)
	if catalogRuntime == nil {
		return nil, nil, nil, errors.WithStack(errors.Newf(
			"distribution catalog raft group %d runtime is unavailable",
			catalogGroupID,
		))
	}
	defaultRuntime := findDefaultGroupRuntime(runtimes, defaultGroupID)
	if defaultRuntime == nil {
		return nil, nil, nil, errors.WithStack(errors.Newf(
			"default raft group %d runtime is unavailable",
			defaultGroupID,
		))
	}
	return catalog, catalogRuntime, defaultRuntime, nil
}

func runDistributionCatalogStream(
	ctx context.Context,
	runtime *raftGroupRuntime,
	conns *kv.GRPCConnCache,
	engine *distribution.Engine,
) error {
	watcher := distribution.NewResolvingGRPCCatalogWatcher(
		func(context.Context) (pb.DistributionClient, error) {
			raftEngine := runtime.snapshotEngine()
			if raftEngine == nil {
				return nil, errors.New("default raft group engine is unavailable")
			}
			leader := raftEngine.Leader().Address
			if leader == "" {
				return nil, errors.New("default raft group leader is unavailable")
			}
			conn, err := conns.ConnFor(leader)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return pb.NewDistributionClient(conn), nil
		},
		engine,
	)
	if err := watcher.Run(ctx); err != nil {
		return errors.Wrap(err, "catalog stream watcher failed")
	}
	return nil
}
