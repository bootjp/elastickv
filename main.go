package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/noderuntime"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/runtimehealth"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/bootjp/elastickv/internal/tsowiring"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// exitCodeMemoryPressure is reported by main when memwatch triggered the
// shutdown. It is non-zero so supervisors see a non-success exit, but
// distinct from log.Fatalf's 1 and from os.Exit(1) in the other binaries
// so log scraping can tell them apart.
const exitCodeMemoryPressure = 2

func main() {
	if handled, err := dispatchBootstrapOperator(os.Args[1:], os.Stdout); handled {
		if err != nil {
			log.Fatalf("%v", err)
		}
		return
	}
	flag.Parse()
	cliFlags.keyViz.RecordExplicit(flag.CommandLine)

	err := run(cliFlags)
	if memoryPressureExit.Load() {
		// memwatch fired: surface exit code 2 regardless of whether run()
		// returned a nil or an error (cancel() can cause in-flight
		// listeners to return spurious errors during shutdown). Still
		// log any residual error so a secondary failure during the
		// graceful shutdown is visible in logs rather than swallowed.
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("shutdown error after memory pressure", "error", err)
		}
		os.Exit(exitCodeMemoryPressure)
	}
	if err := ignoreCanceledAfterSignal(err); err != nil {
		log.Fatalf("%v", err)
	}
}

// ignoreCanceledAfterSignal maps the context.Canceled an errgroup member may
// surface while unwinding after SIGTERM/SIGINT to a clean exit. The
// cancellation is the shutdown the operator asked for, not a failure; any
// other error (or a cancellation nobody requested) is reported unchanged.
// A joined error (waitErrgroupAfterStartupFailure combines the startup error
// with the drain error) is only clean when every branch is a cancellation.
func ignoreCanceledAfterSignal(err error) error {
	if err == nil || !signalShutdown.Load() || !onlyContextCanceled(err) {
		return err
	}
	slog.Info("graceful shutdown after signal completed")
	return nil
}

// onlyContextCanceled reports whether err carries nothing but
// context.Canceled: a plain or wrapped cancellation, or a joined error whose
// every branch is one. errors.Is alone would accept a join that also holds a
// real failure, hiding it behind exit status 0.
func onlyContextCanceled(err error) bool {
	if err == nil {
		return false
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		branches := joined.Unwrap()
		if len(branches) == 0 {
			return false
		}
		for _, branch := range branches {
			if !onlyContextCanceled(branch) {
				return false
			}
		}
		return true
	}
	return errors.Is(err, context.Canceled)
}

func run(flags *serverFlags) error {
	cfg, engineType, bootstrapCfg, bootstrap, err := resolveNetworkRuntimeInputs(flags)
	if err != nil {
		return err
	}
	bootstrapCfg.Encryption = flags.encryption.bootstrapRequest()

	var lc net.ListenConfig

	metricsRegistry := monitoring.NewRegistry(flags.raft.id, flags.raft.address)

	// Factory needs the cold-start observer from the registry so the
	// engine's restoreSnapshotState path can emit
	// elastickv_fsm_cold_start_restore_total / _applied_index_gap
	// (PR #934 round-1 codex P2 closed this plumbing gap — the
	// observer was previously unused because Factory.Create did not
	// carry it through to OpenConfig).
	factory, err := newRaftFactory(engineType, metricsRegistry.ColdStartObserver())
	if err != nil {
		return err
	}

	// Create the shared HLC before building shard groups so every FSM can update
	// physicalCeiling when HLC lease entries are applied to the Raft log.
	clock := kv.NewHLC()

	// Stage 6B-2: construct the shared KEK wrapper + in-memory
	// Keystore once at startup, before any FSM is built. Both are
	// process-wide singletons:
	//
	//   - KEK is exercised at DEK bootstrap (§5.6) and rotation
	//     (§5.2) by every shard's applier; one wrapper per process
	//     is what the file-mode check + kek.FileWrapper invariants
	//     assume.
	//   - Keystore is the in-memory map of (key_id → DEK bytes) the
	//     storage cipher (§6.2, wired in Stage 6D) and the
	//     EncryptionApplier (Stage 6A/6B) both read from. Sharing
	//     one instance across shards keeps post-bootstrap DEKs
	//     visible to every shard's storage cipher.
	//
	// Both are nil-safe in the applier path: WithKEK / WithKeystore
	// are only attached to the applier when a KEK source is loaded
	// (else the applier stays in the Stage 6A posture where
	// ApplyBootstrap / ApplyRotation return ErrKEKNotConfigured).
	// The KEK source is decorated for §9.2
	// elastickv_encryption_kek_unwrap_seconds inside this call, before
	// the startup guards run their own unwraps.
	kekUnwrapper, err := loadKEKAfterPreNonceStartupGuards(cfg, flags.raft, flags.encryption, metricsRegistry.KEKUnwrapObserver())
	if err != nil {
		return err
	}
	keystore := encryption.NewKeystore()
	redisApplyObserver := adapter.NewRedisApplyObserver()
	readTracker := kv.NewActiveTimestampTracker(kv.WithActiveTimestampTrackerMaxBackupPins(flags.backup.maxActivePins))
	s3BlobBackfiller := adapter.NewS3BlobBackfiller(cfg.S3BlobBackfillConfig)

	cleanup := internalutil.CleanupStack{}
	defer cleanup.Run()
	// The root context and the SIGTERM/SIGINT handler are installed before
	// the Raft/Pebble runtimes are opened, so a stop requested during a slow
	// open or snapshot restore is recorded (ctx is cancelled) instead of
	// killing the process mid-open through the default disposition; startup
	// then unwinds through the normal cancelled-context path once the
	// runtimes exist. The handler is registered before the store/runtime
	// closers so it is still swallowing repeat signals while CleanupStack
	// syncs and closes them.
	ctx, cancel := context.WithCancel(context.Background())
	handoff := newShutdownHandoff(cancel)
	cleanup.Add(installShutdownSignalHandler(handoff.requestShutdown))
	health, stopHealth, err := startRuntimeHealth(ctx, &lc, cancel, flags.healthAddress, flags.raft.id)
	if err != nil {
		return err
	}
	cleanup.Add(stopHealth)

	// Stage 6D-6c: buildShardGroupsWithEncryptionWiring assembles the
	// storage-envelope write-path wiring (cipher + deterministic nonce
	// factory + the process-shared StateCache) before opening any
	// shard store, then constructs the shard groups with it. The
	// wiring hydrates the keystore and bumps the §4.1 local_epoch when
	// a storage DEK is already active on disk (restart path); on a
	// pre-bootstrap binary the per-Put gate stays cleartext until a
	// runtime Bootstrap + EnableStorageEnvelope flips it.
	runtimes, shardGroups, encWiring, err := buildShardGroupsWithEncryptionWiring(
		flags.raft.id,
		flags.raft.dataDir,
		cfg.Groups,
		cfg.DefaultGroup,
		cfg.Multi,
		bootstrap,
		bootstrapCfg,
		factory,
		func(groupID uint64) kv.ProposalObserver {
			return metricsRegistry.RaftProposalObserver(groupID)
		},
		clock,
		kekUnwrapper,
		keystore,
		flags.encryption.sidecarPath,
		flags.encryption.enabled,
		readTracker,
		cfg.Engine,
		metricsRegistry.EncryptionObserver(),
		redisApplyObserver,
		s3BlobBackfiller,
	)
	if err = chainEncryptionStartupGuard(
		err,
		runtimes,
		cfg.DefaultGroup,
		flags.encryption.sidecarPath,
		flags.encryption.enabled,
	); err != nil {
		return err
	}
	// Startup has hydrated every keystore, so the KEK unwrap cache has
	// served its purpose. Seal it: the same wrapper is retained by
	// every applier for the process lifetime, and an unsealed cache
	// would keep a plaintext copy of every DEK a later rotation
	// unwraps.
	encryption.SealStartupUnwrapCache(kekUnwrapper)

	// Record the active FSM apply sync mode so operators can see on the
	// /metrics endpoint which durability posture this node is running in.
	// The label is resolved per-pebbleStore from ELASTICKV_FSM_SYNC_MODE
	// in NewPebbleStore; read it off the first constructed store (all
	// shards share the same env and therefore the same label).
	publishRuntimeHealth(health, runtimes)
	recordFSMApplySyncMode(metricsRegistry, runtimes)

	cleanup.Add(readTracker.Close)
	shardStore := newStartupShardStore(cfg.Engine, shardGroups, flags.consistency.readWatermarkAdvance)
	cleanup.Add(func() {
		_ = shardStore.Close()
		for _, rt := range runtimes {
			rt.Close()
		}
	})
	bootstrapDomain, resolverOptions := configureBootstrapOperations(ctx, runtimes, shardStore, &cleanup)
	bootstrapNetwork, err := configureBootstrapNetwork(ctx, runtimes, shardStore, shardGroups, &cleanup)
	if err != nil {
		return err
	}
	cleanup.Add(cancel)
	startLockResolverIfEnabled(shardStore, shardGroups, &cleanup, resolverOptions...)
	sampler := serverwiring.BuildSampler(flags.keyViz, flags.autoSplit)
	sqsPartitionResolver := noderuntime.BuildSQSPartitionResolver(cfg.SQSFifoPartitionMap)
	shardStore.WithPartitionResolver(sqsPartitionResolver)
	coordinate := kv.NewShardedCoordinator(cfg.Engine, shardGroups, cfg.DefaultGroup, clock, shardStore).
		WithLeaseReadObserver(metricsRegistry.LeaseReadObserver()).
		WithSampler(keyVizSamplerForCoordinator(sampler)).
		WithKeyVizLabelsEnabled(*flags.keyViz.LabelsEnabled).
		WithAllShardGroups(noderuntime.DataGroupIDs(cfg.Groups)...).
		WithPartitionResolver(sqsPartitionResolver)
	configureCommitTSFenceActivation(coordinate, flags.consistency.commitTSFence, runtimes)
	installBootstrapCoordinator(bootstrapDomain, coordinate, &cleanup)
	tsoWiring, err := configureCoordinatorTSOWithObserver(
		flags.tso,
		coordinate,
		shardGroups,
		shardStore,
		metricsRegistry.TSOObserver(),
		bootstrapNetwork,
	)
	if err != nil {
		return err
	}
	cleanup.Add(tsoWiring.CloseWithLog)
	readTracker.SetBackupTimestampFloorObserver(coordinate.ObserveTimestampFloor)

	// SQS HT-FIFO §8 leadership-refusal: install per-group
	// observers that step the local node down via
	// TransferLeadership when it acquires (or already holds)
	// leadership of a Raft group hosting a partitioned FIFO
	// queue while the binary lacks the htfifo capability. The
	// composite deregister flows through cleanup; it's a no-op
	// when no group hosts a partitioned queue or when the
	// binary advertises htfifo (the steady-state production
	// case post-PR-4-B-3b).
	leadershipRefusalDeregister := installSQSLeadershipRefusalAcrossGroups(
		ctx, runtimes, cfg.SQSFifoPartitionMap,
		sqsAdvertisesHTFIFO(), slog.Default())
	cleanup.Add(leadershipRefusalDeregister)
	eg, runCtx := errgroup.WithContext(ctx)
	tsowiring.StartModeReloader(runCtx, eg, flags.tso, tsoWiring)
	startRaftEngineLifecycleWatchers(runCtx, eg, runtimes)
	// setupDistributionCatalog + the Stage 7a process-start registration
	// gate are bundled so run() has a single startup-fault path: a
	// registry-read / behind-epoch failure fails the process
	// synchronously here, BEFORE the gRPC servers serve, so writes never
	// run with no registration gate installed.
	encryptionStartup := bootstrapEncryptionStartupConfig{
		runtimes: runtimes, groups: shardGroups, defaultGroup: cfg.DefaultGroup,
		network: bootstrapNetwork, wiring: encWiring, sidecarPath: flags.encryption.sidecarPath,
		wrapper: kekUnwrapper, nodeID: etcdraftengine.DeriveNodeID(flags.raft.id), requested: flags.encryption.rotateOnStartup,
		eg: eg, cleanup: &cleanup,
	}
	distStartup, err := startDistributionWithBootstrapEncryption(distributionStartupInput{
		ctx:             runCtx,
		eg:              eg,
		cancel:          cancel,
		cleanup:         &cleanup,
		runtimes:        runtimes,
		cfg:             cfg,
		coordinate:      coordinate,
		shardStore:      shardStore,
		defaultGroup:    shardGroups[cfg.DefaultGroup],
		encWiring:       encWiring,
		raftID:          flags.raft.id,
		raftDir:         flags.raft.dataDir,
		sidecarPath:     flags.encryption.sidecarPath,
		sampler:         sampler,
		readTracker:     readTracker,
		metricsRegistry: metricsRegistry,
		clock:           clock,
		tsoWiring:       tsoWiring,
		shardGroups:     shardGroups,
		snapshotOffload: flags.snapshotOffload,
		keyViz:          flags.keyViz,
		autoSplit:       flags.autoSplit,
		nodeAuth:        flags.nodeAuth,
	}, encryptionStartup)
	if err != nil {
		// Members already run on eg (TSO reloader, raft lifecycle
		// watchers, the registration gate); drain them before the deferred
		// CleanupStack closes the stores they still use. A SIGTERM during
		// the registration retry surfaces here as context.Canceled.
		return waitErrgroupAfterStartupFailure(cancel, eg, err)
	}
	runCtx = distStartup.registrationCtx
	distServer := distStartup.distServer
	distCatalog := distStartup.distCatalog
	autoSplitRuntime := distStartup.autoSplitRuntime

	// Stage 7c §3.1: build the encryption-aware
	// MembershipChangeInterceptor here where the concrete
	// *kv.ShardedCoordinator and *kv.ShardGroup are available. Returns
	// nil when encryption is not wired (no StateCache or no default
	// group), in which case raftadmin.Server skips the pre-step.
	encryptionConfChangeInterceptor := newEncryptionConfChangeInterceptor(
		encWiring.encryptionConfigured(),
		coordinate, shardGroups[cfg.DefaultGroup], encWiring.cache, flags.encryption.sidecarPath, etcdraftengine.DeriveNodeID)
	rotateOnStartupDeregister, waitRotateOnStartup := installBootstrapStartupRotation(distStartup.rotationSupervisor, runCtx, encryptionStartup)
	cleanup.Add(rotateOnStartupDeregister)
	return serveStartedRuntime(waitRotateOnStartup, serversInput{
		ctx: runCtx, eg: eg, cancel: cancel, lc: &lc,
		runtimes: runtimes, shardGroups: shardGroups, bootstrapServers: bootstrapCfg.AdminSeed(cfg.DefaultGroup),
		shardStore: shardStore, coordinate: coordinate,
		distServer: distServer, distCatalog: distCatalog, readTracker: readTracker,
		metricsRegistry: metricsRegistry, cfg: cfg,
		redisApplyObserver:              redisApplyObserver,
		cleanup:                         &cleanup,
		s3BlobBackfiller:                s3BlobBackfiller,
		encWiring:                       encWiring,
		encryption:                      flags.encryption,
		kekConfigured:                   kekUnwrapper != nil,
		keyvizSampler:                   sampler,
		autoSplitRuntime:                autoSplitRuntime,
		encryptionConfChangeInterceptor: encryptionConfChangeInterceptor,
		raftID:                          flags.raft.id,
		nodeAddress:                     flags.raft.address,
		consistency:                     flags.consistency,
		raftJoining:                     flags.bootstrap.joining(),
		filesystem:                      flags.filesystem,
		leaderBalance:                   flags.leaderBalance,
		adminHTTP:                       flags.adminHTTP,
		backup:                          flags.backup,
		nodeAuth:                        flags.nodeAuth,
		listeners:                       flags.listeners,
	}, health, handoff)
}

func serveStartedRuntime(waitRotateOnStartup startupRotationWaiter, in serversInput, health *runtimehealth.Probe, handoff *shutdownHandoff) error {
	if err := startServersAfterStartupRotation(waitRotateOnStartup, in); err != nil {
		return err
	}
	health.MarkServing()
	balanceCtx, stopBalance := context.WithCancel(in.ctx)
	defer stopBalance()
	balanceDone := make(chan struct{})
	handoff.arm(in.ctx, in.eg, in.runtimes, func(ctx context.Context) error {
		return stopLeaderBalanceForShutdown(ctx, stopBalance, balanceDone)
	})
	startLeaderBalance(
		balanceCtx,
		in.eg,
		in.runtimes,
		leaderBalanceOptions(in.leaderBalance, in.raftID, in.cfg.DefaultGroup, in.cfg.SQSFifoPartitionMap, in.metricsRegistry.Registerer()),
		balanceDone,
	)

	return waitRunGroup(in.eg)
}

func waitRunGroup(eg *errgroup.Group) error {
	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "failed to serve")
	}
	return nil
}

func waitErrgroupAfterStartupFailure(cancel context.CancelFunc, eg *errgroup.Group, startupErr error) error {
	cancel()
	if err := eg.Wait(); err != nil {
		joined := errors.Join(
			startupErr,
			errors.Wrap(err, "shutdown failed after startup error"),
		)
		return errors.Wrap(joined, "startup failed")
	}
	return startupErr
}
