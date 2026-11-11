package main

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/admin"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// setupAdminService is a thin wrapper around configureAdminService that also
// binds each Raft runtime to the server and logs an operator warning when
// running without authentication. Keeping this out of run() preserves run's
// cyclomatic-complexity budget. Members are seeded from the bootstrap
// configuration so GetClusterOverview advertises peer node addresses to the
// admin binary's fan-out discovery path.
// serversInput bundles the values run() passes to startServers so the
// signature stays compact and run() stays under the cyclop budget.
type serversInput struct {
	ctx                context.Context
	eg                 *errgroup.Group
	cancel             context.CancelFunc
	lc                 *net.ListenConfig
	runtimes           []*raftGroupRuntime
	shardGroups        map[uint64]*kv.ShardGroup
	bootstrapServers   []raftengine.Server
	shardStore         *kv.ShardStore
	coordinate         kv.Coordinator
	distServer         *adapter.DistributionServer
	distCatalog        *distribution.CatalogStore
	readTracker        *kv.ActiveTimestampTracker
	metricsRegistry    *monitoring.Registry
	cfg                runtimeConfig
	encWiring          encryptionWriteWiring
	kekConfigured      bool
	redisApplyObserver *adapter.RedisApplyObserver
	cleanup            *internalutil.CleanupStack
	s3BlobBackfiller   *adapter.S3BlobBackfiller
	// keyvizSampler is the in-memory key visualizer sampler, or nil
	// when --keyvizEnabled is false. Threaded into setupAdminService
	// so AdminServer.GetKeyVizMatrix can serve snapshots; the
	// coordinator already has its own copy from
	// `WithSampler(...)` higher up in run().
	keyvizSampler *keyviz.MemSampler
	// autoSplitRuntime is registered on the authenticated Admin gRPC service
	// when automatic splitting was configured at process startup.
	autoSplitRuntime adapter.AutoSplitRuntime
	// encryptionConfChangeInterceptor is the Stage 7c §3.1
	// pre-register hook for raftadmin AddVoter/AddLearner.
	// Constructed in run() where concrete *kv.ShardedCoordinator and
	// *kv.ShardGroup are still in scope; nil when encryption is not
	// wired or the cluster is not bootstrapped, in which case
	// raftadmin.Server skips the pre-step.
	encryptionConfChangeInterceptor internalraftadmin.MembershipChangeInterceptor
}

// startServersAfterStartupRotation wires up the AdminServer, starts the
// per-group Raft listeners needed for quorum traffic, waits for a fresh join
// to catch up, prepares the public listeners, waits for any requested startup
// rotation, then starts serving public traffic.
func startServersAfterStartupRotation(waitRotateOnStartup startupRotationWaiter, in serversInput) error {
	adminGRPCEnabled := *adminTokenFile != "" || *adminInsecureNoAuth
	// roleStore is gated on *adminEnabled. With admin HTTP disabled, building
	// it is wasted work AND a security regression risk: a non-empty
	// -adminFullAccessKeys flag would otherwise still flip
	// forwardDeps.readyForRegistration() to true, registering the leader-side
	// gRPC AdminForward service and re-exposing the table-write surface a
	// follower-direct admin call could reach (P1/Major review on #648).
	//
	// connCache is also used by the gRPC Admin GetNodeVersion probe, so create
	// it when either admin HTTP or gRPC Admin is enabled.
	connCache := prepareAdminConnCache(in.ctx, in.eg, *adminEnabled || adminGRPCEnabled, in.runtimes...)
	adminServer, adminGRPCOpts, err := setupAdminService(
		*raftId, *myAddr, in.runtimes, in.shardGroups, in.bootstrapServers,
		in.shardStore, in.distCatalog, in.coordinate, in.readTracker, in.keyvizSampler, connCache,
		in.autoSplitRuntime,
	)
	if err != nil {
		return err
	}
	roleStore := startupRoleStoreFromFlags(*adminEnabled)
	publicKVGate := &startupPublicKVGate{blockRuntime: func() bool { return protectedRuntimesBlocked(in.runtimes) }}
	publicKVGate.domain = bootstrapDomainForRuntimes(in.runtimes)
	startBootstrapReadinessReconciler(in.ctx, in.eg, in.runtimes, in.distCatalog, in.cfg.engine)
	if in.distServer != nil {
		in.distServer.SetReadGate(publicKVGate.blocked)
	}
	installHLCLeaseRenewalBlocker(in.coordinate, waitRotateOnStartup.BlockMutators)
	adapterCoordinate := newStartupGatedCoordinator(in.coordinate, publicKVGate)
	runner := runtimeServerRunner{
		ctx:                in.ctx,
		lc:                 in.lc,
		eg:                 in.eg,
		cancel:             in.cancel,
		runtimes:           in.runtimes,
		shardGroups:        in.shardGroups,
		shardStore:         in.shardStore,
		coordinate:         adapterCoordinate,
		distServer:         in.distServer,
		routeEngine:        in.cfg.engine,
		adminServer:        adminServer,
		adminGRPCOpts:      adminGRPCOpts,
		redisAddress:       *redisAddr,
		leaderRedis:        in.cfg.leaderRedis,
		pubsubRelay:        adapter.NewRedisPubSubRelay(),
		readTracker:        in.readTracker,
		encWiring:          in.encWiring,
		kekConfigured:      in.kekConfigured,
		defaultGroupID:     in.cfg.defaultGroup,
		redisApplyObserver: in.redisApplyObserver,
		s3BlobBackfiller:   in.s3BlobBackfiller,
		dynamoAddress:      *dynamoAddr,
		leaderDynamo:       in.cfg.leaderDynamo,
		s3Address:          *s3Addr,
		leaderS3:           in.cfg.leaderS3,
		s3Region:           *s3Region,
		s3CredsFile:        *s3CredsFile,
		s3PathStyleOnly:    *s3PathStyleOnly,
		sqsAddress:         *sqsAddr,
		leaderSQS:          in.cfg.leaderSQS,
		sqsRegion:          *sqsRegion,
		sqsCredsFile:       *sqsCredsFile,
		sqsPublicEndpoint:  *sqsPublicEndpoint,
		// sqsPartitionResolver is rebuilt from the same config map
		// the coordinator's WithPartitionResolver consumes (line
		// ~328) so the SQS server's CreateQueue capability gate
		// sees exactly the routes the coordinator will use to
		// resolve SendMessage / ReceiveMessage / DeleteMessage
		// dispatch. Returns nil on a non-partitioned cluster, in
		// which case validateHTFIFOCapability skips the routing-
		// coverage check (Codex P1 review on PR #734, round 2).
		sqsPartitionResolver: buildSQSPartitionResolverConcrete(in.cfg.sqsFifoPartitionMap),
		// sqsPartitionObserver: the metrics registry's HT-FIFO
		// partition counter observer. nil when --metricsAddress is
		// empty (the adapter then no-ops the observe call).
		sqsPartitionObserver:            in.metricsRegistry.SQSPartitionObserver(),
		metricsAddress:                  *metricsAddr,
		metricsToken:                    *metricsToken,
		pprofAddress:                    *pprofAddr,
		pprofToken:                      *pprofToken,
		metricsRegistry:                 in.metricsRegistry,
		roleStore:                       roleStore,
		encryptionConfChangeInterceptor: in.encryptionConfChangeInterceptor,
		publicKVGate:                    publicKVGate,
	}
	if err := runner.startRaftTransport(); err != nil {
		return err
	}
	startBootstrapActivationWorkers(in.ctx, in.eg, in.runtimes, in.shardGroups)
	if err := preparePublicServicesAfterRaftJoinReady(
		in.ctx,
		in.runtimes,
		*raftId,
		strings.TrimSpace(*raftJoinMembers) != "",
		runner.preparePublicServices,
	); err != nil {
		return runner.startupFailure(err)
	}
	// runner.startRaftTransport() has populated runner.dynamoServer for the
	// admin listener's SigV4-bypass entrypoints (see adapter/dynamodb_admin.go).
	// Passing nil here would leave the admin dashboard with no
	// access to table metadata; the admin handler answers
	// /admin/api/v1/dynamo/* with 404 in that case.
	//
	// runner.coordinate + connCache are forwarded so the admin HTTP
	// dynamo handler can construct its production LeaderForwarder
	// (Phase 3 of design 3.3): when the local node is a follower,
	// the handler hands ErrTablesNotLeader writes to the forwarder
	// which dials the leader over the cached gRPC pool. Without these
	// the handler falls back to 503 + Retry-After:1.
	fanoutCfg := keyVizFanoutConfig{
		Nodes:   parseCSV(*keyvizFanoutNodes),
		Timeout: *keyvizFanoutTimeout,
	}
	adminHTTP, err := prepareAdminFromFlags(in.ctx, in.lc, in.runtimes, runner.dynamoServer, runner.s3Server, runner.sqsServer, runner.coordinate, connCache, in.keyvizSampler, fanoutCfg)
	if err != nil {
		return runner.startupFailure(err)
	}
	runner.adminHTTP = adminHTTP
	publicKVGate.blockMutator = waitRotateOnStartup.BlockMutators
	if err := waitRotateOnStartup.Wait(in.ctx); err != nil {
		return runner.startupFailure(errors.Wrap(err, "encryption rotate-on-startup: wait before serving"))
	}
	return startPublicServicesAfterCommitTSFence(in, &runner, publicKVGate, *commitTSFence)
}

func startPublicServicesAfterCommitTSFence(in serversInput, runner *runtimeServerRunner, gate *startupPublicKVGate, fenceEnabled bool) error {
	startHLCLeaseRenewal(in.ctx, in.eg, in.coordinate)
	if err := markPublicGateReadyAfterCommitTSFence(in.ctx, gate, in.runtimes, fenceEnabled); err != nil {
		return runner.startupFailure(errors.Wrap(err, "wait for commit-ts fence activation before public service"))
	}
	startDistributionSplitJobRunner(in.ctx, in.eg, in.distServer)
	startMigrationOwnershipRefresh(in.ctx, in.eg, in.distServer)
	if err := runner.startPublicServices(); err != nil {
		return err
	}
	runner.startAdminHTTP()
	return startFilesystemAfterServing(in)
}

type runtimeServerRunner struct {
	ctx                             context.Context
	lc                              *net.ListenConfig
	eg                              *errgroup.Group
	cancel                          context.CancelFunc
	runtimes                        []*raftGroupRuntime
	shardGroups                     map[uint64]*kv.ShardGroup
	shardStore                      *kv.ShardStore
	coordinate                      kv.Coordinator
	distServer                      *adapter.DistributionServer
	routeEngine                     *distribution.Engine
	adminServer                     *adapter.AdminServer
	adminGRPCOpts                   adminGRPCInterceptors
	redisAddress                    string
	leaderRedis                     map[string]string
	pubsubRelay                     *adapter.RedisPubSubRelay
	readTracker                     *kv.ActiveTimestampTracker
	redisApplyObserver              *adapter.RedisApplyObserver
	s3BlobBackfiller                *adapter.S3BlobBackfiller
	encWiring                       encryptionWriteWiring
	kekConfigured                   bool
	defaultGroupID                  uint64
	dynamoAddress                   string
	leaderDynamo                    map[string]string
	s3Address                       string
	leaderS3                        map[string]string
	s3Region                        string
	s3CredsFile                     string
	s3PathStyleOnly                 bool
	sqsAddress                      string
	leaderSQS                       map[string]string
	sqsRegion                       string
	sqsCredsFile                    string
	sqsPublicEndpoint               string
	metricsAddress                  string
	metricsToken                    string
	pprofAddress                    string
	pprofToken                      string
	metricsRegistry                 *monitoring.Registry
	encryptionConfChangeInterceptor internalraftadmin.MembershipChangeInterceptor

	// dynamoServer is populated by start() and made available to
	// prepareAdminFromFlags in this package so the admin listener can
	// call SigV4-bypass admin entrypoints (see
	// adapter/dynamodb_admin.go) without going through HTTP. The
	// field is unexported on purpose — it is package-private state,
	// not a public API. Nil until start() reaches the dynamo step.
	dynamoServer   *adapter.DynamoDBServer
	dynamoListener net.Listener
	redisServer    *adapter.RedisServer
	redisCompactor *adapter.DeltaCompactor
	redisListener  net.Listener

	// s3Server is the parallel field for the S3 admin endpoints
	// (read-only in this slice). Nil when --s3Address is empty,
	// in which case the admin handler answers /s3/buckets* with
	// 404, mirroring the dynamoServer == nil contract.
	s3Server   *adapter.S3Server
	s3Listener net.Listener

	// sqsServer plays the same role for the SQS admin entrypoints
	// (adapter/sqs_admin.go). Always non-nil after startup —
	// prepareSQSServer constructs a listenless SQSServer when
	// --sqsAddress is empty (the public SigV4 listener is
	// suppressed but the admin bridge stays wired since the admin
	// handlers only need the coordinator/store, not the listener).
	sqsServer   *adapter.SQSServer
	sqsListener net.Listener

	metricsServer   *http.Server
	metricsListener net.Listener
	metricsBindAddr string
	pprofServer     *http.Server
	pprofListener   net.Listener
	pprofBindAddr   string
	adminHTTP       *preparedAdminServer

	// sqsPartitionResolver is the concrete pointer to the same
	// resolver installed on the coordinator (line ~322). prepareSQSServer
	// hands this through WithSQSPartitionResolver so the CreateQueue
	// capability gate can verify routing coverage on partitioned
	// creates without re-parsing --sqsFifoPartitionMap (P1 review on
	// PR #734, round 2). Nil on single-shard / no-flag
	// deployments — the gate's resolver==nil branch then skips
	// the coverage check.
	sqsPartitionResolver *adapter.SQSPartitionResolver

	// sqsPartitionObserver records the
	// elastickv_sqs_partition_messages_total counter (PR 7a) for
	// HT-FIFO send / receive / delete operations. Sourced from
	// the monitoring registry; nil-receiver-safe on the adapter
	// side so a test fixture without a registry can omit it.
	sqsPartitionObserver adapter.SQSPartitionObserver

	// roleStore is the access-key → role index the leader-side
	// gRPC AdminForward service uses to re-validate the principal
	// on every forwarded write. Mirrors what admin.Config.RoleIndex
	// produces inside prepareAdminFromFlags; built up-front in
	// startServers so registerAdminForwardServer in startRaftServers
	// does not need to wait for the (later) admin-config parse.
	// Nil when no admin access keys are configured.
	roleStore admin.RoleStore

	publicKVGate *startupPublicKVGate
}

func (r *runtimeServerRunner) startRaftTransport() error {
	if err := r.prepareAdminForwardServers(); err != nil {
		return r.startupFailure(err)
	}
	domain := bootstrapDomainForRuntimes(r.runtimes)
	adminGRPCOpts := bootstrapServiceAuthOptions(r.adminGRPCOpts, domain)
	if domain != nil {
		adminGRPCOpts.unary = append([]grpc.UnaryServerInterceptor{bootstrapRegistrationReceiverInterceptor(r.defaultGroupID)}, adminGRPCOpts.unary...)
	}
	adminGRPCOpts, err := bootstrapRotationRPCOptions(adminGRPCOpts, domain, etcdraftengine.DeriveNodeID(*raftId), domain.rotationRPCHandler())
	if err != nil {
		return r.startupFailure(err)
	}
	if r.publicKVGate != nil {
		adminGRPCOpts.unary = append(adminGRPCOpts.unary, r.publicKVGate.unaryInterceptor)
		adminGRPCOpts.stream = append(adminGRPCOpts.stream, r.publicKVGate.streamInterceptor)
	}
	var s3BlobPushBlocked func() bool
	if r.publicKVGate != nil {
		s3BlobPushBlocked = r.publicKVGate.blocked
	}
	forwardDeps := adminForwardServerDeps{
		tables:  newDynamoTablesSource(r.dynamoServer),
		buckets: newBucketsSource(r.s3Server),
		roles:   r.roleStore,
	}
	var sqsPartitionResolver kv.PartitionResolver
	if r.sqsPartitionResolver != nil {
		sqsPartitionResolver = r.sqsPartitionResolver
	}
	if err := startRaftServers(
		r.ctx,
		r.lc,
		r.eg,
		r.runtimes,
		r.shardGroups,
		r.shardStore,
		r.coordinate,
		r.distServer,
		r.pubsubRelay,
		func(groupID uint64) kv.ProposalObserver {
			return r.metricsRegistry.RaftProposalObserver(groupID)
		},
		r.adminServer,
		adminGRPCOpts,
		forwardDeps,
		r.encryptionConfChangeInterceptor,
		r.encWiring,
		sqsPartitionResolver,
		r.routeEngine,
		r.readTracker,
		r.kekConfigured,
		r.defaultGroupID,
		r.metricsRegistry.S3BlobOffloadObserver(),
		s3BlobPushBlocked,
	); err != nil {
		return r.startupFailure(err)
	}
	return nil
}

func prepareAdminConnCache(ctx context.Context, eg *errgroup.Group, enabled bool, runtimes ...*raftGroupRuntime) *kv.GRPCConnCache {
	if !enabled {
		return nil
	}
	connCache := newBootstrapRuntimeConnCache(runtimes)
	eg.Go(func() error {
		<-ctx.Done()
		if err := connCache.Close(); err != nil {
			return errors.Wrap(err, "close admin gRPC connection cache")
		}
		return nil
	})
	return connCache
}

func (r *runtimeServerRunner) prepareAdminForwardServers() error {
	r.dynamoServer = newDynamoDBServer(r.shardStore, r.coordinate, r.leaderDynamo, r.metricsRegistry, r.readTracker)
	blobCluster, err := r.newS3BlobCluster()
	if err != nil {
		return err
	}
	s3Server, err := newS3Server(
		r.s3Address, r.shardStore, r.coordinate, r.leaderS3, r.s3Region,
		r.s3CredsFile, r.s3PathStyleOnly, r.readTracker,
		r.metricsRegistry.S3PutAdmissionObserver(),
		r.metricsRegistry.S3BlobOffloadObserver(),
		blobCluster,
		r.publicKVGate.blocked,
		r.s3BlobBackfiller,
	)
	if err != nil {
		if blobCluster != nil {
			_ = blobCluster.Close()
		}
		return err
	}
	r.s3Server = s3Server
	return nil
}

func (r *runtimeServerRunner) newS3BlobCluster() (adapter.S3BlobCluster, error) {
	if r == nil || !s3BlobNodeEnabled(r.s3Address, *s3BlobPeerTokenFile) {
		return nil, nil
	}
	members, ok := r.coordinate.(kv.RaftMembershipCoordinator)
	if !ok {
		return nil, errors.New("S3 blob offload requires raft membership discovery")
	}
	adminToken := ""
	if strings.TrimSpace(*adminTokenFile) != "" {
		loaded, err := loadAdminTokenFile(*adminTokenFile)
		if err != nil {
			return nil, err
		}
		adminToken = loaded
	}
	peerToken := ""
	if strings.TrimSpace(*s3BlobPeerTokenFile) != "" {
		loaded, err := loadS3BlobPeerTokenFile(*s3BlobPeerTokenFile)
		if err != nil {
			return nil, err
		}
		peerToken = loaded
	}
	return adapter.NewGRPCS3BlobCluster(*raftId, members, adminToken, peerToken, bootstrapRuntimeDialOptions(r.runtimes)), nil
}

func (r *runtimeServerRunner) preparePublicServices() error {
	redisServer, redisCompactor, redisListener, err := prepareRedisServer(
		r.ctx, r.lc, r.redisAddress, r.shardStore, r.coordinate,
		r.leaderRedis, r.pubsubRelay, r.metricsRegistry, r.readTracker,
		r.redisApplyObserver,
	)
	if err != nil {
		return err
	}
	r.redisServer = redisServer
	r.redisCompactor = redisCompactor
	r.redisListener = redisListener

	dynamoListener, err := bindDynamoDBServer(r.ctx, r.lc, r.dynamoAddress, r.dynamoServer)
	if err != nil {
		return err
	}
	r.dynamoListener = dynamoListener

	s3Listener, err := bindS3Server(r.ctx, r.lc, r.s3Address, r.s3Server)
	if err != nil {
		return err
	}
	r.s3Listener = s3Listener

	sqsServer, sqsListener, err := prepareSQSServer(
		r.ctx, r.lc, r.sqsAddress, r.shardStore, r.coordinate,
		r.leaderSQS, r.sqsRegion, r.sqsCredsFile, r.sqsPublicEndpoint,
		r.sqsPartitionResolver, r.sqsPartitionObserver,
	)
	if err != nil {
		return err
	}
	r.sqsServer = sqsServer
	r.sqsListener = sqsListener

	metricsServer, metricsListener, metricsAddr, err := serverwiring.PrepareMetricsServer(
		r.ctx, r.lc, r.metricsAddress, r.metricsToken, r.metricsRegistry.Handler(),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	r.metricsServer = metricsServer
	r.metricsListener = metricsListener
	r.metricsBindAddr = metricsAddr

	pprofServer, pprofListener, pprofAddr, err := serverwiring.PreparePprofServer(r.ctx, r.lc, r.pprofAddress, r.pprofToken)
	if err != nil {
		return errors.WithStack(err)
	}
	r.pprofServer = pprofServer
	r.pprofListener = pprofListener
	r.pprofBindAddr = pprofAddr
	return nil
}

func (r *runtimeServerRunner) closePreparedExternalListeners() {
	if r.redisListener != nil {
		_ = r.redisListener.Close()
		r.redisListener = nil
	}
	if r.dynamoListener != nil {
		_ = r.dynamoListener.Close()
		r.dynamoListener = nil
	}
	if r.s3Listener != nil {
		_ = r.s3Listener.Close()
		r.s3Listener = nil
	}
	if r.sqsListener != nil {
		_ = r.sqsListener.Close()
		r.sqsListener = nil
	}
	if r.metricsListener != nil {
		_ = r.metricsListener.Close()
		r.metricsListener = nil
	}
	if r.pprofListener != nil {
		_ = r.pprofListener.Close()
		r.pprofListener = nil
	}
	if r.adminHTTP != nil {
		r.adminHTTP.close()
	}
}

func (r *runtimeServerRunner) startupFailure(err error) error {
	r.closePreparedExternalListeners()
	return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
}

func (r *runtimeServerRunner) startPublicServices() error {
	if r.redisServer == nil || r.redisListener == nil {
		return r.startupFailure(errors.New("redis server is not prepared"))
	}
	if r.dynamoServer == nil || r.dynamoListener == nil {
		return r.startupFailure(errors.New("dynamodb server is not prepared"))
	}
	if r.sqsServer == nil {
		return r.startupFailure(errors.New("sqs server is not prepared"))
	}
	runRedisServer(r.ctx, r.eg, r.redisServer, r.redisCompactor)
	r.redisListener = nil
	runDynamoDBServer(r.ctx, r.eg, r.dynamoServer)
	r.dynamoListener = nil
	if r.s3Listener != nil {
		runS3Server(r.ctx, r.eg, r.s3Server)
		r.s3Listener = nil
	} else {
		runS3BlobBackfillOnly(r.ctx, r.eg, r.s3Server)
	}
	runSQSServer(r.ctx, r.eg, r.sqsServer)
	r.sqsListener = nil
	// Plug the SQS adapter into the monitoring registry's depth
	// observer (see startSQSDepthObserver). nil sqsServer (e.g.
	// --sqsAddress empty on this node) is a no-op so single-binary
	// tests don't need to construct a fake source.
	if r.sqsServer != nil {
		startSQSDepthObserver(r.ctx, r.metricsRegistry, r.sqsServer)
	}
	runPreparedMetricsServer(r.ctx, r.eg, r.metricsServer, r.metricsListener, r.metricsBindAddr)
	r.metricsListener = nil
	runPreparedPprofServer(r.ctx, r.eg, r.pprofServer, r.pprofListener, r.pprofBindAddr)
	r.pprofListener = nil
	return nil
}

func (r *runtimeServerRunner) startAdminHTTP() {
	if r.adminHTTP == nil {
		return
	}
	r.adminHTTP.start(r.ctx, r.eg)
}
