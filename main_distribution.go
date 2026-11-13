package main

import (
	"context"
	"log/slog"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/bootjp/elastickv/internal/snapshotoffloadwiring"
	"github.com/bootjp/elastickv/internal/tsowiring"
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
	runtimes        []*clusterbootstrap.GroupRuntime
	cfg             noderuntime.RuntimeConfig
	raftDir         string
	coordinate      *kv.ShardedCoordinator
	shardStore      *kv.ShardStore
	defaultGroup    *kv.ShardGroup
	encWiring       encryptionwiring.WriteWiring
	raftID          string
	sidecarPath     string
	sampler         *keyviz.MemSampler
	readTracker     *kv.ActiveTimestampTracker
	metricsRegistry *monitoring.Registry
	clock           *kv.HLC
	tsoWiring       tsowiring.Wiring
	// shardGroups feeds the split-migration promotion/voter factories, which
	// resolve per-group leaders rather than going through the coordinator.
	shardGroups     map[uint64]*kv.ShardGroup
	snapshotOffload snapshotoffloadwiring.Flags
	keyViz          *serverwiring.KeyVizFlags
	autoSplit       *serverwiring.AutoSplitFlags
	nodeAuth        nodeauth.Flags
}

type distributionStartup struct {
	defaultRuntime     *clusterbootstrap.GroupRuntime
	distServer         *adapter.DistributionServer
	registrationCtx    context.Context
	rotationSupervisor *clusterbootstrap.BootstrapRotationRuntime
	// distCatalog is the durable route catalog the admin backup store reads
	// to capture a route snapshot at a pinned timestamp. It is built here, so
	// it has to travel back out rather than be rebuilt at the call site.
	distCatalog      *distribution.CatalogStore
	autoSplitRuntime adapter.AutoSplitRuntime
}

func startDistributionStartup(in distributionStartupInput) (distributionStartup, error) {
	auth, err := in.nodeAuth.LoadGRPCAuth()
	if err != nil {
		return distributionStartup{}, errors.WithStack(err)
	}
	internalutil.ConfigureGRPCForwardToken(auth.MigrationToken)
	distCatalog, catalogRuntime, defaultRuntime, err := setupDistributionRuntimeDependencies(
		in.ctx, in.eg, in.runtimes, in.cfg.Engine,
		in.coordinate, in.defaultGroup, in.cfg.DefaultGroup,
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
	serverwiring.SeedRoutes(in.sampler, in.cfg.Engine)

	catalogWatchConns := clusterbootstrap.NewBootstrapRuntimeConnCache(in.runtimes)
	in.cleanup.Add(func() { _ = catalogWatchConns.Close() })
	in.eg.Go(func() error {
		return runDistributionCatalogStream(in.ctx, catalogRuntime, catalogWatchConns, in.cfg.Engine)
	})
	serverwiring.StartFlusher(in.ctx, in.eg, in.sampler)
	startKeyVizLeaderTermPublisher(in.ctx, in.eg, in.sampler, in.runtimes)
	startMemoryWatchdog(in.ctx, in.eg, in.cancel)
	splitMigrationConnCache := clusterbootstrap.NewBootstrapRuntimeConnCache(in.runtimes)
	in.cleanup.Add(func() { _ = splitMigrationConnCache.Close() })
	distServer := adapter.NewDistributionServer(
		in.cfg.Engine,
		distCatalog,
		adapter.WithDistributionCoordinator(in.coordinate),
		adapter.WithDistributionTimestampAllocator(in.tsoWiring.ServerAllocator),
		adapter.WithDistributionTSOActivationGate(in.tsoWiring.AuthorizeActivation),
		adapter.WithDistributionActiveTimestampTracker(in.readTracker),
		adapter.WithCatalogWatchLeaderCheck(func() bool {
			engine := clusterbootstrap.RuntimeCore(catalogRuntime).SnapshotEngine()
			return engine != nil && engine.State() == raftengine.StateLeader
		}),
		adapter.WithDistributionFilesystemObserver(in.metricsRegistry.FileSystemObserver()),
		adapter.WithDistributionKnownRaftGroups(noderuntime.ShardGroupIDs(in.shardGroups)...),
		adapter.WithSplitMigrationCapabilityGate(adapter.NewSplitMigrationCapabilityGate(
			splitMigrationCapabilityPeerSourceForRuntimes(in.runtimes),
			splitMigrationCapabilityProbeTimeout,
			clusterbootstrap.BootstrapSplitMigrationProbe(in.runtimes),
			in.nodeAuth.SplitMigrationLocalReadinessGate,
		)),
		adapter.WithSplitPromotionClientFactory(adapter.NewSplitPromotionGRPCClientFactory(
			adapter.SplitPromotionTargetLeaderResolver(in.shardGroups),
			splitMigrationConnCache,
			auth.MigrationToken,
		)),
		adapter.WithSplitMigrationClientFactory(adapter.NewSplitMigrationGRPCClientFactory(
			adapter.SplitMigrationGroupLeaderResolver(in.shardGroups),
			splitMigrationConnCache,
			auth.MigrationToken,
		)),
		adapter.WithSplitMigrationVoterFactory(adapter.NewSplitMigrationGRPCVoterFactory(
			in.shardGroups,
			splitMigrationConnCache,
			auth.MigrationToken,
		)),
		adapter.WithSplitMigrationApplyMemberSnapshotFactory(adapter.NewSplitMigrationApplyMemberSnapshotGRPCFactory(
			in.shardGroups,
			splitMigrationConnCache,
			auth.MigrationToken,
		)),
		adapter.WithSplitMigrationGroupClientFactory(adapter.NewSplitMigrationGroupGRPCClientFactory(
			adapter.SplitMigrationGroupLeaderResolver(in.shardGroups),
			splitMigrationConnCache,
			auth.MigrationToken,
		)),
		// Do not advertise cap_migration_v2 before protocol coverage is accepted.
		adapter.WithSplitJobRunnerReadinessGate(in.nodeAuth.SplitMigrationLocalReadinessGate),
		migrationOwnershipRefreshOption(in.shardGroups, catalogRuntime, splitMigrationConnCache, auth.MigrationToken, in.nodeAuth.SplitMigrationPeerAuthEnforced),
		adapter.MigrationPartitionRuntimeOption(in.shardStore, in.shardGroups, splitMigrationConnCache, auth.MigrationToken, in.nodeAuth.SplitMigrationPeerAuthEnforced),
	)
	autoSplitRuntime, err := setupDistributionWatcherAndAutoSplit(
		in.ctx,
		in.eg,
		distCatalog,
		in.cfg.Engine,
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
	startMonitoringCollectors(in.ctx, in.metricsRegistry, in.runtimes, in.clock, in.encWiring.Cache)
	startFSMCompactorIfEnabled(in.ctx, in.eg, in.runtimes, in.readTracker)
	// §4 physical snapshot offload. Opt-in, and a hard error when
	// configured-but-unbuildable: an operator who set a backup
	// destination and silently got no backups is worse off than one
	// whose node refused to start.
	if err := startSnapshotOffload(
		in.ctx, in.eg, in.snapshotOffload, in.runtimes, in.raftDir, in.raftID, in.cfg.Multi,
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

func startDistributionSplitJobRunner(ctx context.Context, eg *errgroup.Group, server *adapter.DistributionServer, ready adapter.MigrationReadinessGate) {
	if eg == nil || server == nil {
		return
	}
	if ready.Check(ctx) != nil {
		return
	}
	eg.Go(func() error {
		return server.RunSplitJobRunner(ctx)
	})
}

func distributionCatalogStoreForGroup(runtimes []*clusterbootstrap.GroupRuntime, groupID uint64) *distribution.CatalogStore {
	for _, rt := range runtimes {
		if rt == nil || rt.Store == nil {
			continue
		}
		if rt.Spec.ID == groupID {
			return distribution.NewCatalogStore(rt.Store,
				distribution.WithCatalogRouteDescriptorV2Writes(true),
				distribution.WithCatalogMigrationStoreResolver(catalogMigrationStoreResolver(runtimes)),
				distribution.WithCatalogMigrationRetireResolver(catalogMigrationRetireResolver(runtimes)),
			)
		}
	}
	return nil
}

func catalogMigrationStoreResolver(runtimes []*clusterbootstrap.GroupRuntime) distribution.CatalogMigrationStoreResolver {
	return func(groupID uint64) (store.MVCCStore, error) {
		for _, rt := range runtimes {
			if rt == nil || rt.Store == nil {
				continue
			}
			if rt.Spec.ID == groupID {
				return rt.Store, nil
			}
		}
		return nil, errors.Newf("migration target store is not available for group %d", groupID)
	}
}

func catalogMigrationRetireResolver(runtimes []*clusterbootstrap.GroupRuntime) distribution.CatalogMigrationRetireResolver {
	return func(ctx context.Context, groupID, jobID uint64) error {
		for _, rt := range runtimes {
			if rt == nil || rt.Spec.ID != groupID {
				continue
			}
			engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
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
	runtimes []*clusterbootstrap.GroupRuntime,
	engine *distribution.Engine,
) (*distribution.CatalogStore, error) {
	if err := validateNoExistingCommitTSFenceActivation(ctx, runtimes, clusterbootstrap.BootstrapDomainForRuntimes(runtimes) != nil); err != nil {
		return nil, err
	}
	catalogGroupID, err := distribution.CatalogGroupID(engine)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve distribution catalog group")
	}
	distCatalog := distributionCatalogStoreForGroup(runtimes, catalogGroupID)
	if distCatalog == nil {
		return nil, errors.WithStack(errors.Newf("distribution catalog store is not available for group %d", catalogGroupID))
	}
	if handled, err := clusterbootstrap.LoadProtectedStartupCatalog(ctx, runtimes, catalogGroupID, distCatalog, engine); handled {
		return distCatalog, errors.WithStack(err)
	}
	// EnsureCatalogSnapshot may Save through the direct (non-raft) write
	// path. When the §7.1 storage envelope is active and this load's
	// writer registration has not yet committed, that Save fails closed
	// with store.ErrWriterNotRegistered (Stage 7a-2). encryptionwiring.RetryUntilRegistered
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
	if err := encryptionwiring.RetryUntilRegistered(ctx, "distribution catalog bootstrap", func() error {
		_, e := distribution.EnsureCatalogSnapshot(ctx, distCatalog, engine)
		return errors.Wrap(e, "ensure catalog snapshot")
	}); err != nil {
		return nil, errors.Wrapf(err, "initialize distribution catalog")
	}
	return distCatalog, nil
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
	runtimes []*clusterbootstrap.GroupRuntime,
	engine *distribution.Engine,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	defaultGroupID uint64,
	encWiring encryptionwiring.WriteWiring,
	raftID string,
	sidecarPath string,
) (*distribution.CatalogStore, *clusterbootstrap.GroupRuntime, *clusterbootstrap.GroupRuntime, error) {
	catalog, err := setupDistributionAndRegistration(
		runCtx, eg, runtimes, engine, coordinate, defaultGroup, encWiring, raftID, sidecarPath)
	if err != nil {
		return nil, nil, nil, err
	}
	catalogGroupID, err := distribution.CatalogGroupID(engine)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "resolve distribution catalog group runtime")
	}
	catalogRuntime := clusterbootstrap.FindDefaultGroupRuntime(runtimes, catalogGroupID)
	if catalogRuntime == nil {
		return nil, nil, nil, errors.WithStack(errors.Newf(
			"distribution catalog raft group %d runtime is unavailable",
			catalogGroupID,
		))
	}
	defaultRuntime := clusterbootstrap.FindDefaultGroupRuntime(runtimes, defaultGroupID)
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
	runtime *clusterbootstrap.GroupRuntime,
	conns *kv.GRPCConnCache,
	engine *distribution.Engine,
) error {
	watcher := distribution.NewResolvingGRPCCatalogWatcher(
		func(context.Context) (pb.DistributionClient, error) {
			raftEngine := clusterbootstrap.RuntimeCore(runtime).SnapshotEngine()
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

// setupDistributionAndRegistration installs the Stage 7a process-start
// registration gate and then sets up the distribution catalog,
// returning the catalog store. Bundling the two lets run() handle both
// startup faults through a single error path (keeping run() within the
// cyclop budget) while preserving the fail-synchronously-before-serving
// guarantee for the registration gate.
//
// Ordering (Stage 7a-2 §2.3): the registration gate is armed FIRST,
// then EnsureCatalogSnapshot runs. The catalog bootstrap Save is a
// direct encrypted write that 7a-2 gates on Registered(); arming the
// gate (which starts the registration goroutine) before the bootstrap
// lets the empty-catalog + active-envelope edge clear once registration
// commits, via the bounded encryptionwiring.RetryUntilRegistered wrapper inside
// setupDistributionCatalog. Registration has no dependency on the route
// catalog (the OpRegistration apply writes a writer-registry row, not a
// route), so arm-before-bootstrap cannot deadlock.
func setupDistributionAndRegistration(
	runCtx context.Context,
	eg *errgroup.Group,
	runtimes []*clusterbootstrap.GroupRuntime,
	engine *distribution.Engine,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionwiring.WriteWiring,
	raftID string,
	sidecarPath string,
) (*distribution.CatalogStore, error) {
	if err := encryptionwiring.ValidateRaftRegistrationStartupEpoch(defaultGroup, w, raftID, sidecarPath); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := encryptionwiring.InstallProcessStartRegistrationGate(runCtx, eg, coordinate, defaultGroup, w, raftID); err != nil {
		return nil, errors.WithStack(err)
	}
	encryptionwiring.InstallRaftRegistrationVerifier(defaultGroup, w, raftID)
	encryptionwiring.InstallRuntimeRaftRegistrationWatcher(runCtx, eg, coordinate, defaultGroup, w, raftID, sidecarPath)
	// Stage 7b/7b': arm the runtime watcher. Handles all three runtime
	// registration triggers — cutover (Phase-0 → EnableStorageEnvelope),
	// pre-bootstrap (no DEK → runtime Bootstrap), and rotation (RotateDEK
	// from a non-proposer's perspective). All three collapse to the same
	// in-scope propose under the cache.Registered() gate in
	// runtimeRegistrationInScope. Must run AFTER
	// encryptionwiring.InstallProcessStartRegistrationGate so the gate is in place before
	// any runtime trigger can fire.
	encryptionwiring.InstallRuntimeRegistrationWatcher(runCtx, eg, coordinate, defaultGroup, w, raftID)
	// Bootstrap + registration both run under runCtx so a shutdown
	// cancels the bounded retry rather than hanging.
	distCatalog, err := setupDistributionCatalog(runCtx, runtimes, engine)
	if err != nil {
		return nil, err
	}
	return distCatalog, nil
}
