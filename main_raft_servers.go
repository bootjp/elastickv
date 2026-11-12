package main

import (
	"context"
	"log/slog"
	"net"
	"slices"
	"sync"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func registerS3BlobFetchServer(
	registrar grpc.ServiceRegistrar,
	enabled bool,
	st store.MVCCStore,
	observer adapter.S3BlobOffloadObserver,
	clock *kv.HLC,
	pushBlocked func() bool,
) []string {
	if !enabled {
		return nil
	}
	pb.RegisterS3BlobFetchServer(registrar, adapter.NewS3BlobFetchServer(
		st,
		observer,
		adapter.WithS3BlobFetchClock(clock),
		adapter.WithS3BlobFetchPushBlocked(pushBlocked),
	))
	return []string{"S3BlobFetch"}
}

func raftGRPCServerOptions(adminGRPCOpts adminGRPCInterceptors) []grpc.ServerOption {
	baseOpts := internalutil.GRPCServerOptions()
	// extraOptsCap reserves slots for the unary + stream admin
	// interceptor options appended below. Sized as a constant so the
	// magic-number linter does not complain.
	const extraOptsCap = 2
	opts := make([]grpc.ServerOption, 0, len(baseOpts)+extraOptsCap)
	opts = append(opts, baseOpts...)
	// Collapse all interceptors into a single ChainUnaryInterceptor /
	// ChainStreamInterceptor call so a future grpc.UnaryInterceptor
	// (single-interceptor) option added anywhere in this chain cannot
	// silently overwrite the admin auth gate — gRPC-Go keeps only the
	// last option of the same type.
	unary := append([]grpc.UnaryServerInterceptor{}, adminGRPCOpts.unary...)
	unary = append(unary, newMigrationCoverageUnaryGate(adminGRPCOpts.migrationReady))
	opts = append(opts, grpc.ChainUnaryInterceptor(unary...))
	if len(adminGRPCOpts.stream) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(adminGRPCOpts.stream...))
	}
	return opts
}

// internalOptionsForGroup builds the Internal server options for one runtime.
//
// Group 0 runs TSOStateMachine, whose Apply halts on the KV tags
// TransactionManager.Commit emits, so a forwarded PUT reaching its Internal
// server would stop group-0 apply and all centralized timestamp issuance.
func startRaftServers(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	runtimes []*raftGroupRuntime,
	shardGroups map[uint64]*kv.ShardGroup,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	distServer *adapter.DistributionServer,
	relay *adapter.RedisPubSubRelay,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	adminServer *adapter.AdminServer,
	adminGRPCOpts adminGRPCInterceptors,
	forwardDeps adminForwardServerDeps,
	confChangeInterceptor internalraftadmin.MembershipChangeInterceptor,
	encWiring encryptionWriteWiring,
	sqsPartitionResolver kv.PartitionResolver,
	routeEngine *distribution.Engine,
	readTracker *kv.ActiveTimestampTracker,
	encryptionCfg encryptionFlags,
	kekConfigured bool,
	defaultGroupID uint64,
	raftID string,
	readWatermarkAdvance bool,
	s3BlobObserver adapter.S3BlobOffloadObserver,
	s3BlobPushBlocked func() bool,
) error {
	forwardLogger := slog.Default().With(slog.String("component", "admin"))
	enableMutators := encryptionMutatorsEnabled(encryptionCfg, kekConfigured)
	encryptionCapabilityFanout := buildEncryptionCapabilityFanout(ctx, eg, runtimes, enableMutators)
	monitorFanout, releaseMonitorConns := buildStorageEnvelopeV2MonitorFanout(runtimes, enableMutators)
	startStorageEnvelopeV2CapabilityMonitor(ctx, eg, monitorFanout, releaseMonitorConns, encWiring)
	writerRegistry, err := encryptionAdminDefaultWriterRegistry(encryptionCfg.sidecarPath, shardGroups, defaultGroupID)
	if err != nil {
		return err
	}
	defaultAdmin, err := encryptionAdminDefaultRuntimeOptions(runtimes, shardGroups, defaultGroupID, enableMutators || writerRegistry != nil)
	if err != nil {
		return err
	}
	encryptionAdminServer := newEncryptionAdminServer(
		etcdraftengine.DeriveNodeID(raftID),
		encryptionCfg.sidecarPath,
		enableMutators,
		defaultAdmin.engine,
		encryptionCapabilityFanout,
		writerRegistry,
		defaultAdmin.latestAppliedIndex,
		adapter.WithEncryptionAdminPostCutoverProposer(defaultAdmin.postCutoverProposer),
		adapter.WithEncryptionAdminCutoverBarrier(encWiring.raftEnvelope.barrier()),
	)
	for _, rt := range runtimes {
		serverOptions := append(slices.Clone(rt.transportServerOptions), raftGRPCServerOptions(adminGRPCOpts)...)
		gs := grpc.NewServer(serverOptions...)
		trx := newInternalMigrationTransaction(proposerForGroup(rt, shardGroups), observerForGroup(proposalObserverForGroup, rt.spec.id), rt.store, rt.spec.id)
		grpcSvc := adapter.NewGRPCServer(shardStore, coordinate)
		pb.RegisterRawKVServer(gs, grpcSvc)
		pb.RegisterTransactionalKVServer(gs, grpcSvc)
		internalOpts := internalServerOptions(coordinate, adminServer, proposerForGroup(rt, shardGroups), shardGroups[rt.spec.id], rt.spec.id)
		internalOpts = append(internalOpts, bootstrapInternalPolicyOptions(rt, proposerForGroup(rt, shardGroups))...)
		internalOpts = append(internalOpts, dedicatedTimestampCutoverOptions(shardGroups)...)
		internalOpts = append(internalOpts, adapter.WithInternalMigrationOwnershipReady(func() bool {
			return migrationOwnershipRuntimeReady(trx, shardStore, distServer, rt.spec.id, rt.snapshotEngine(), adminGRPCOpts.migrationReady)
		}))
		internalOpts = append(internalOpts, migrationInternalProtocolOptions(trx, shardStore, distServer, rt)...)
		internalOpts = append(internalOpts, internalReadWatermarkOptions(readWatermarkAdvance, rt, shardGroups)...)
		staticServingServices := registerS3BlobFetchServer(
			gs,
			adminGRPCOpts.s3BlobFetchEnabled,
			rt.store,
			s3BlobObserver,
			coordinate.Clock(),
			s3BlobPushBlocked,
		)
		pb.RegisterInternalServer(gs, adapter.NewInternalWithEngine(
			trx,
			rt.engine,
			coordinate.Clock(),
			relay,
			append(internalOpts,
				adapter.WithInternalPrepareEvidenceToken(adminGRPCOpts.token),
				adapter.WithInternalStore(rt.store),
				adapter.WithInternalPrimaryCommitEvidence(shardStore),
				adapter.WithInternalMigrationProposer(proposerForGroup(rt, shardGroups)),
				adapter.WithInternalRouteEngine(routeEngine),
				adapter.WithInternalActiveTimestampTracker(readTracker),
				adapter.WithInternalMigrationExportRouting(rt.spec.id, sqsPartitionResolver),
			)...,
		))
		pb.RegisterDistributionServer(gs, distServer)
		registerAdminServerIfPresent(gs, adminServer)
		// full_node_id MUST be per-node-stable, not per-shard.
		// rt.spec.id is the Raft group id which every replica of
		// the same group shares; using it as full_node_id makes
		// every node return the same CapabilityReport value and
		// BootstrapEncryption's writer-batch uniqueness validation
		// (adapter/encryption_admin.go validateWriterBatchUniqueness)
		// rejects the bootstrap with "duplicate full_node_id".
		// Derive a per-node uint64 from --raftId via the canonical
		// FNV-1a hash already used by raftengine for peer ids
		// (etcd.DeriveNodeID), so every node in the cluster reports
		// a stable, distinct value. Codex r1 P1 on PR #760.
		// Stage 6B-2 mutator gate is resolved once above the
		// per-shard loop. The admin control-plane itself is scoped to
		// the default group: writer-registry rows are written there,
		// ResyncSidecar's freshness gate must verify that group's
		// leader, and all mutating admin RPCs must commit into the
		// same FSM whose registry GetSidecarState/ResyncSidecar read.
		registerEncryptionAdminServer(gs, encryptionAdminServer)
		registerAdminForwardServer(gs, forwardDeps, forwardLogger)
		rt.registerGRPC(gs)
		// Stage 7c §3.1: pass the encryption-aware pre-register hook
		// (nil when encryption is not wired); raftadmin.Server invokes
		// it before AddVoter/AddLearner propose the conf-change.
		internalraftadmin.RegisterOperationalServicesWithInterceptorAndStaticServing(
			ctx,
			gs,
			rt.engine,
			[]string{"RawKV"},
			staticServingServices,
			migrationMembershipInterceptor(rt, shardStore, proposerForGroup(rt, shardGroups), adminGRPCOpts.migrationToken, confChangeInterceptor),
		)
		reflection.Register(gs)

		grpcSock, err := lc.Listen(ctx, "tcp", rt.spec.address)
		if err != nil {
			return errors.Wrapf(err, "failed to listen on %s", rt.spec.address)
		}
		srv := gs
		lis := grpcSock
		grpcService := grpcSvc
		eg.Go(func() error {
			var closeOnce sync.Once
			closeService := func() {
				closeOnce.Do(func() { _ = grpcService.Close() })
			}
			stop := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					stopGRPCServerBounded(ctx, srv, rt.spec.address)
					_ = lis.Close()
					closeService()
				case <-stop:
				}
			}()
			err := srv.Serve(lis)
			close(stop)
			closeService()
			if errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return errors.WithStack(err)
		})
	}
	return nil
}

func internalReadWatermarkOptions(enabled bool, rt *raftGroupRuntime, groups map[uint64]*kv.ShardGroup) []adapter.InternalOption {
	if !enabled {
		return nil
	}
	return []adapter.InternalOption{adapter.WithInternalReadWatermarkProposer(proposerForGroup(rt, groups))}
}

func internalServerOptions(
	coordinate kv.Coordinator,
	adminServer *adapter.AdminServer,
	proposer raftengine.Proposer,
	group *kv.ShardGroup,
	groupID uint64,
) []adapter.InternalOption {
	opts := internalTimestampOptions(coordinate)
	if adminServer != nil {
		opts = append(opts, adapter.WithInternalAdminProposer(proposer))
	}
	if group != nil && group.Store != nil {
		opts = append(opts, adapter.WithInternalLastCommitTimestamp(group.Store.LastCommitTS))
	}
	if groupID == dedicatedTSORaftGroupID {
		opts = append(opts, adapter.WithKVForwardRejected())
	}
	return opts
}

func registerAdminServerIfPresent(gs grpc.ServiceRegistrar, adminServer *adapter.AdminServer) {
	if adminServer == nil {
		return
	}
	pb.RegisterAdminServer(gs, adminServer)
}

func internalTimestampOptions(coordinate kv.Coordinator) []adapter.InternalOption {
	var opts []adapter.InternalOption
	if alloc, ok := kv.ConfiguredTimestampAllocatorThrough(coordinate); ok {
		opts = append(opts, adapter.WithInternalTimestampAllocator(alloc))
	}
	// Sharded deployments own a route table with migration write floors; the
	// single-group coordinator has none and its gate allows every write. The
	// value probed here is whatever main.go handed the adapters, which in the
	// production path is startupGatedCoordinator -- see the forwards on that
	// type, without which this probe silently finds nothing.
	if gate, ok := coordinate.(kv.MutationWriteGate); ok {
		opts = append(opts, adapter.WithInternalWriteGate(gate))
	}
	if observer, ok := coordinate.(interface{ ObserveForwardedRequests([]*pb.Request) }); ok {
		opts = append(opts, adapter.WithInternalForwardWriteObserver(observer.ObserveForwardedRequests))
	}
	return opts
}
