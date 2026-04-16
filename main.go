package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	heartbeatTimeout           = 200 * time.Millisecond
	electionTimeout            = 2000 * time.Millisecond
	leaderLease                = 100 * time.Millisecond
	raftCommitTimeout          = 50 * time.Millisecond
	raftMetricsObserveInterval = 5 * time.Second
	dirPerm                    = raftDirPerm

	etcdTickInterval      = 10 * time.Millisecond
	etcdHeartbeatMinTicks = 1
	etcdElectionMinTicks  = 2
	etcdMaxSizePerMsg     = 1 << 20
	etcdMaxInflightMsg    = 256
)

const snapshotRetainCount = 3

func newRaftFactory(engineType raftEngineType) (raftengine.Factory, error) {
	switch engineType {
	case raftEngineHashicorp:
		return hashicorpraftengine.NewFactory(hashicorpraftengine.FactoryConfig{
			CommitTimeout:       raftCommitTimeout,
			HeartbeatTimeout:    heartbeatTimeout,
			ElectionTimeout:     electionTimeout,
			LeaderLeaseTimeout:  leaderLease,
			SnapshotRetainCount: snapshotRetainCount,
		}), nil
	case raftEngineEtcd:
		return etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
			TickInterval:   etcdTickInterval,
			HeartbeatTick:  durationToTicks(heartbeatTimeout, etcdTickInterval, etcdHeartbeatMinTicks),
			ElectionTick:   durationToTicks(electionTimeout, etcdTickInterval, etcdElectionMinTicks),
			MaxSizePerMsg:  etcdMaxSizePerMsg,
			MaxInflightMsg: etcdMaxInflightMsg,
		}), nil
	default:
		return nil, errors.Wrapf(ErrUnsupportedRaftEngine, "%q", engineType)
	}
}

func durationToTicks(timeout time.Duration, tick time.Duration, min int) int {
	if tick <= 0 {
		return min
	}
	ticks := int(timeout / tick)
	if timeout%tick != 0 {
		ticks++
	}
	if ticks < min {
		return min
	}
	return ticks
}

var (
	myAddr               = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr            = flag.String("redisAddress", "localhost:6379", "TCP host+port for redis")
	dynamoAddr           = flag.String("dynamoAddress", "localhost:8000", "TCP host+port for DynamoDB-compatible API")
	s3Addr               = flag.String("s3Address", "", "TCP host+port for S3-compatible API; empty to disable")
	s3Region             = flag.String("s3Region", "us-east-1", "S3 signing region")
	s3CredsFile          = flag.String("s3CredentialsFile", "", "Path to a JSON file containing static S3 credentials")
	s3PathStyleOnly      = flag.Bool("s3PathStyleOnly", true, "Only accept path-style S3 requests")
	metricsAddr          = flag.String("metricsAddress", "localhost:9090", "TCP host+port for Prometheus metrics")
	metricsToken         = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	pprofAddr            = flag.String("pprofAddress", "localhost:6060", "TCP host+port for pprof debug endpoints; empty to disable")
	pprofToken           = flag.String("pprofToken", "", "Bearer token for pprof; required for non-loopback pprofAddress")
	raftId               = flag.String("raftId", "", "Node id used by Raft")
	raftEngineName       = flag.String("raftEngine", string(raftEngineEtcd), "Raft engine implementation (etcd|hashicorp)")
	raftDir              = flag.String("raftDataDir", "data/", "Raft data dir")
	raftBootstrap        = flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	raftBootstrapMembers = flag.String("raftBootstrapMembers", "", "Comma-separated bootstrap raft members (raftID=host:port,...)")
	raftGroups           = flag.String("raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	shardRanges          = flag.String("shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	raftRedisMap         = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
	raftS3Map            = flag.String("raftS3Map", "", "Map of Raft address to S3 address (raftAddr=s3Addr,...)")
	raftDynamoMap        = flag.String("raftDynamoMap", "", "Map of Raft address to DynamoDB address (raftAddr=dynamoAddr,...)")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("%v", err)
	}
}

func run() error {
	cfg, engineType, bootstrapServers, bootstrap, err := resolveRuntimeInputs()
	if err != nil {
		return err
	}

	factory, err := newRaftFactory(engineType)
	if err != nil {
		return err
	}

	var lc net.ListenConfig

	metricsRegistry := monitoring.NewRegistry(*raftId, *myAddr)

	// Create the shared HLC before building shard groups so every FSM can update
	// physicalCeiling when HLC lease entries are applied to the Raft log.
	clock := kv.NewHLC()

	runtimes, shardGroups, err := buildShardGroups(
		*raftId,
		*raftDir,
		cfg.groups,
		cfg.multi,
		bootstrap,
		bootstrapServers,
		factory,
		func(groupID uint64) kv.ProposalObserver {
			return metricsRegistry.RaftProposalObserver(groupID)
		},
		clock,
	)
	if err != nil {
		return err
	}

	cleanup := internalutil.CleanupStack{}
	defer cleanup.Run()

	ctx, cancel := context.WithCancel(context.Background())
	readTracker := kv.NewActiveTimestampTracker()
	shardStore := kv.NewShardStore(cfg.engine, shardGroups)
	cleanup.Add(func() {
		_ = shardStore.Close()
		for _, rt := range runtimes {
			rt.Close()
		}
	})
	cleanup.Add(cancel)
	lockResolver := kv.NewLockResolver(shardStore, shardGroups, nil)
	cleanup.Add(func() { lockResolver.Close() })
	coordinate := kv.NewShardedCoordinator(cfg.engine, shardGroups, cfg.defaultGroup, clock, shardStore)
	distCatalog, err := setupDistributionCatalog(ctx, runtimes, cfg.engine)
	if err != nil {
		return err
	}
	eg, runCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return runDistributionCatalogWatcher(runCtx, distCatalog, cfg.engine)
	})
	distServer := adapter.NewDistributionServer(
		cfg.engine,
		distCatalog,
		adapter.WithDistributionCoordinator(coordinate),
		adapter.WithDistributionActiveTimestampTracker(readTracker),
	)
	metricsRegistry.RaftObserver().Start(runCtx, raftMonitorRuntimes(runtimes), raftMetricsObserveInterval)
	compactor := kv.NewFSMCompactor(
		fsmCompactionRuntimes(runtimes),
		kv.WithFSMCompactorActiveTimestampTracker(readTracker),
	)
	eg.Go(func() error {
		return compactor.Run(runCtx)
	})
	eg.Go(func() error {
		coordinate.RunHLCLeaseRenewal(runCtx)
		return nil
	})

	runner := runtimeServerRunner{
		ctx:             runCtx,
		lc:              &lc,
		eg:              eg,
		cancel:          cancel,
		runtimes:        runtimes,
		shardStore:      shardStore,
		coordinate:      coordinate,
		distServer:      distServer,
		redisAddress:    *redisAddr,
		leaderRedis:     cfg.leaderRedis,
		pubsubRelay:     adapter.NewRedisPubSubRelay(),
		readTracker:     readTracker,
		dynamoAddress:   *dynamoAddr,
		leaderDynamo:    cfg.leaderDynamo,
		s3Address:       *s3Addr,
		leaderS3:        cfg.leaderS3,
		s3Region:        *s3Region,
		s3CredsFile:     *s3CredsFile,
		s3PathStyleOnly: *s3PathStyleOnly,
		metricsAddress:  *metricsAddr,
		metricsToken:    *metricsToken,
		pprofAddress:    *pprofAddr,
		pprofToken:      *pprofToken,
		metricsRegistry: metricsRegistry,
	}
	if err := runner.start(); err != nil {
		return err
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "failed to serve")
	}
	return nil
}

func resolveRuntimeInputs() (runtimeConfig, raftEngineType, []raft.Server, bool, error) {
	if *raftId == "" {
		return runtimeConfig{}, "", nil, false, errors.New("flag --raftId is required")
	}

	engineType, err := parseRaftEngineType(*raftEngineName)
	if err != nil {
		return runtimeConfig{}, "", nil, false, err
	}

	cfg, err := parseRuntimeConfig(*myAddr, *redisAddr, *s3Addr, *dynamoAddr, *raftGroups, *shardRanges, *raftRedisMap, *raftS3Map, *raftDynamoMap)
	if err != nil {
		return runtimeConfig{}, "", nil, false, err
	}

	bootstrapServers, err := resolveBootstrapServers(*raftId, cfg.groups, *raftBootstrapMembers)
	if err != nil {
		return runtimeConfig{}, "", nil, false, err
	}

	return cfg, engineType, bootstrapServers, *raftBootstrap || len(bootstrapServers) > 0, nil
}

type runtimeConfig struct {
	groups       []groupSpec
	defaultGroup uint64
	engine       *distribution.Engine
	leaderRedis  map[raft.ServerAddress]string
	leaderS3     map[raft.ServerAddress]string
	leaderDynamo map[raft.ServerAddress]string
	multi        bool
}

func parseRuntimeConfig(myAddr, redisAddr, s3Addr, dynamoAddr, raftGroups, shardRanges, raftRedisMap, raftS3Map, raftDynamoMap string) (runtimeConfig, error) {
	groups, err := parseRaftGroups(raftGroups, myAddr)
	if err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "failed to parse raft groups")
	}
	defaultGroup := defaultGroupID(groups)
	ranges, err := parseShardRanges(shardRanges, defaultGroup)
	if err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "failed to parse shard ranges")
	}
	if err := validateShardRanges(ranges, groups); err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "invalid shard ranges")
	}

	engine := buildEngine(ranges)
	leaderRedis, err := buildLeaderRedis(groups, redisAddr, raftRedisMap)
	if err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "failed to parse raft redis map")
	}
	leaderS3, err := buildLeaderS3(groups, s3Addr, raftS3Map)
	if err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "failed to parse raft s3 map")
	}
	leaderDynamo, err := buildLeaderDynamo(groups, dynamoAddr, raftDynamoMap)
	if err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "failed to parse raft dynamo map")
	}

	return runtimeConfig{
		groups:       groups,
		defaultGroup: defaultGroup,
		engine:       engine,
		leaderRedis:  leaderRedis,
		leaderS3:     leaderS3,
		leaderDynamo: leaderDynamo,
		multi:        len(groups) > 1,
	}, nil
}

func buildEngine(ranges []rangeSpec) *distribution.Engine {
	engine := distribution.NewEngine()
	for _, r := range ranges {
		engine.UpdateRoute(r.start, r.end, r.groupID)
	}
	return engine
}

func buildLeaderRedis(groups []groupSpec, redisAddr string, raftRedisMap string) (map[raft.ServerAddress]string, error) {
	return buildLeaderAddrMap(groups, redisAddr, raftRedisMap, parseRaftRedisMap)
}

func buildLeaderS3(groups []groupSpec, s3Addr string, raftS3Map string) (map[raft.ServerAddress]string, error) {
	return buildLeaderAddrMap(groups, s3Addr, raftS3Map, parseRaftS3Map)
}

func buildLeaderDynamo(groups []groupSpec, dynamoAddr string, raftDynamoMap string) (map[raft.ServerAddress]string, error) {
	return buildLeaderAddrMap(groups, dynamoAddr, raftDynamoMap, parseRaftDynamoMap)
}

func buildLeaderAddrMap(
	groups []groupSpec,
	defaultAddr string,
	rawMap string,
	parse func(string) (map[raft.ServerAddress]string, error),
) (map[raft.ServerAddress]string, error) {
	leaderAddrMap, err := parse(rawMap)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		addr := raft.ServerAddress(g.address)
		if _, ok := leaderAddrMap[addr]; !ok {
			leaderAddrMap[addr] = defaultAddr
		}
	}
	return leaderAddrMap, nil
}

var (
	ErrBootstrapMembersRequireSingleGroup = errors.New("flag --raftBootstrapMembers requires exactly one raft group")
	ErrBootstrapMembersMissingLocalNode   = errors.New("flag --raftBootstrapMembers must include local --raftId")
	ErrBootstrapMembersLocalAddrMismatch  = errors.New("flag --raftBootstrapMembers local address must match local raft group address")
	ErrNoBootstrapMembersConfigured       = errors.New("no bootstrap members configured")
)

func resolveBootstrapServers(raftID string, groups []groupSpec, bootstrapMembers string) ([]raft.Server, error) {
	if strings.TrimSpace(bootstrapMembers) == "" {
		return nil, nil
	}
	if len(groups) != 1 {
		return nil, errors.WithStack(ErrBootstrapMembersRequireSingleGroup)
	}

	servers, err := parseRaftBootstrapMembers(bootstrapMembers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse raft bootstrap members")
	}
	if len(servers) == 0 {
		return nil, errors.WithStack(ErrNoBootstrapMembersConfigured)
	}

	localAddr := groups[0].address
	for _, s := range servers {
		if string(s.ID) != raftID {
			continue
		}
		if string(s.Address) != localAddr {
			return nil, errors.Wrapf(ErrBootstrapMembersLocalAddrMismatch, "expected %q got %q", localAddr, s.Address)
		}
		return servers, nil
	}
	return nil, errors.Wrapf(ErrBootstrapMembersMissingLocalNode, "raftId=%q", raftID)
}

func buildShardGroups(
	raftID string,
	raftDir string,
	groups []groupSpec,
	multi bool,
	bootstrap bool,
	bootstrapServers []raft.Server,
	factory raftengine.Factory,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	clock *kv.HLC,
) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, error) {
	runtimes := make([]*raftGroupRuntime, 0, len(groups))
	shardGroups := make(map[uint64]*kv.ShardGroup, len(groups))
	for _, g := range groups {
		dir := groupDataDir(raftDir, raftID, g.id, multi)
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return nil, nil, errors.Wrapf(err, "failed to create fsm store dir for group %d", g.id)
		}
		st, err := store.NewPebbleStore(filepath.Join(dir, "fsm.db"))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to open pebble fsm store for group %d", g.id)
		}
		// Each shard FSM shares the same HLC so any shard's lease renewal advances
		// the global physicalCeiling. The logical counter remains in-memory only.
		sm := etcdraftengine.AdaptHashicorpFSM(kv.NewKvFSMWithHLC(st, clock))
		runtime, err := buildRuntimeForGroup(raftID, g, raftDir, multi, bootstrap, bootstrapServers, st, sm, factory)
		if err != nil {
			for _, rt := range runtimes {
				rt.Close()
			}
			_ = st.Close()
			return nil, nil, errors.Wrapf(err, "failed to start raft group %d", g.id)
		}
		runtimes = append(runtimes, runtime)
		shardGroups[g.id] = &kv.ShardGroup{
			Engine: runtime.engine,
			Store:  st,
			Txn:    kv.NewLeaderProxyWithEngine(runtime.engine, kv.WithProposalObserver(observerForGroup(proposalObserverForGroup, g.id))),
		}
	}
	return runtimes, shardGroups, nil
}

func observerForGroup(factory func(uint64) kv.ProposalObserver, groupID uint64) kv.ProposalObserver {
	if factory == nil {
		return nil
	}
	return factory(groupID)
}

func raftMonitorRuntimes(runtimes []*raftGroupRuntime) []monitoring.RaftRuntime {
	out := make([]monitoring.RaftRuntime, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.engine == nil {
			continue
		}
		out = append(out, monitoring.RaftRuntime{
			GroupID:      runtime.spec.id,
			StatusReader: runtime.engine,
			ConfigReader: runtime.engine,
		})
	}
	return out
}

func fsmCompactionRuntimes(runtimes []*raftGroupRuntime) []kv.FSMCompactRuntime {
	out := make([]kv.FSMCompactRuntime, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.engine == nil || runtime.store == nil {
			continue
		}
		out = append(out, kv.FSMCompactRuntime{
			GroupID:      runtime.spec.id,
			StatusReader: runtime.engine,
			Store:        runtime.store,
		})
	}
	return out
}

func startRaftServers(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	runtimes []*raftGroupRuntime,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	distServer *adapter.DistributionServer,
	relay *adapter.RedisPubSubRelay,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
) error {
	for _, rt := range runtimes {
		gs := grpc.NewServer(internalutil.GRPCServerOptions()...)
		trx := kv.NewTransactionWithProposer(rt.engine, kv.WithProposalObserver(observerForGroup(proposalObserverForGroup, rt.spec.id)))
		grpcSvc := adapter.NewGRPCServer(shardStore, coordinate)
		pb.RegisterRawKVServer(gs, grpcSvc)
		pb.RegisterTransactionalKVServer(gs, grpcSvc)
		pb.RegisterInternalServer(gs, adapter.NewInternalWithEngine(trx, rt.engine, coordinate.Clock(), relay))
		pb.RegisterDistributionServer(gs, distServer)
		rt.registerGRPC(gs)
		internalraftadmin.RegisterOperationalServices(ctx, gs, rt.engine, []string{"RawKV"})
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
					srv.GracefulStop()
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

func startRedisServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, redisAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderRedis map[raft.ServerAddress]string, relay *adapter.RedisPubSubRelay, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker) error {
	redisL, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", redisAddr)
	}
	deltaCompactor := adapter.NewDeltaCompactor(shardStore, coordinate)
	eg.Go(func() error { return deltaCompactor.Run(ctx) })
	redisServer := adapter.NewRedisServer(redisL, redisAddr, shardStore, coordinate, leaderRedis, relay,
		adapter.WithRedisActiveTimestampTracker(readTracker),
		adapter.WithRedisRequestObserver(metricsRegistry.RedisObserver()),
		adapter.WithRedisCompactor(deltaCompactor),
	)
	eg.Go(func() error {
		defer redisServer.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				redisServer.Stop()
			case <-stop:
			}
		}()
		err := redisServer.Run()
		close(stop)
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
	return nil
}

func startDynamoDBServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, dynamoAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderDynamo map[raft.ServerAddress]string, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker) error {
	dynamoL, err := lc.Listen(ctx, "tcp", dynamoAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", dynamoAddr)
	}
	dynamoServer := adapter.NewDynamoDBServer(
		dynamoL,
		shardStore,
		coordinate,
		adapter.WithDynamoDBActiveTimestampTracker(readTracker),
		adapter.WithDynamoDBRequestObserver(metricsRegistry.DynamoDBObserver()),
		adapter.WithDynamoDBLeaderMap(leaderDynamo),
	)
	eg.Go(func() error {
		defer dynamoServer.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				dynamoServer.Stop()
			case <-stop:
			}
		}()
		err := dynamoServer.Run()
		close(stop)
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
	return nil
}

func startPprofServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, pprofAddr string, pprofToken string) error {
	pprofAddr = strings.TrimSpace(pprofAddr)
	if pprofAddr == "" {
		return nil
	}
	if _, _, err := net.SplitHostPort(pprofAddr); err != nil {
		return errors.Wrapf(err, "invalid pprofAddress %q; expected host:port", pprofAddr)
	}
	if monitoring.AddressRequiresToken(pprofAddr) && strings.TrimSpace(pprofToken) == "" {
		return errors.New("pprofToken is required when pprofAddress is not loopback")
	}
	pprofL, err := lc.Listen(ctx, "tcp", pprofAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", pprofAddr)
	}
	pprofServer := monitoring.NewPprofServer(pprofToken)
	eg.Go(monitoring.PprofShutdownTask(ctx, pprofServer, pprofAddr))
	eg.Go(monitoring.PprofServeTask(pprofServer, pprofL, pprofAddr))
	return nil
}

func startMetricsServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, metricsAddr string, metricsToken string, handler http.Handler) error {
	metricsAddr = strings.TrimSpace(metricsAddr)
	if metricsAddr == "" || handler == nil {
		return nil
	}
	if _, _, err := net.SplitHostPort(metricsAddr); err != nil {
		return errors.Wrapf(err, "invalid metricsAddress %q; expected host:port", metricsAddr)
	}
	if monitoring.AddressRequiresToken(metricsAddr) && strings.TrimSpace(metricsToken) == "" {
		return errors.New("metricsToken is required when metricsAddress is not loopback")
	}
	metricsL, err := lc.Listen(ctx, "tcp", metricsAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", metricsAddr)
	}
	metricsServer := monitoring.NewMetricsServer(handler, metricsToken)
	eg.Go(monitoring.MetricsShutdownTask(ctx, metricsServer, metricsAddr))
	eg.Go(monitoring.MetricsServeTask(metricsServer, metricsL, metricsAddr))
	return nil
}

func distributionCatalogStoreForGroup(runtimes []*raftGroupRuntime, groupID uint64) *distribution.CatalogStore {
	for _, rt := range runtimes {
		if rt == nil || rt.store == nil {
			continue
		}
		if rt.spec.id == groupID {
			return distribution.NewCatalogStore(rt.store)
		}
	}
	return nil
}

func setupDistributionCatalog(
	ctx context.Context,
	runtimes []*raftGroupRuntime,
	engine *distribution.Engine,
) (*distribution.CatalogStore, error) {
	catalogGroupID, err := distributionCatalogGroupID(engine)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve distribution catalog group")
	}
	distCatalog := distributionCatalogStoreForGroup(runtimes, catalogGroupID)
	if distCatalog == nil {
		return nil, errors.WithStack(errors.Newf("distribution catalog store is not available for group %d", catalogGroupID))
	}
	if _, err := distribution.EnsureCatalogSnapshot(ctx, distCatalog, engine); err != nil {
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

func runDistributionCatalogWatcher(ctx context.Context, catalog *distribution.CatalogStore, engine *distribution.Engine) error {
	if err := distribution.RunCatalogWatcher(ctx, catalog, engine, nil); err != nil {
		return errors.Wrapf(err, "catalog watcher failed")
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

type runtimeServerRunner struct {
	ctx             context.Context
	lc              *net.ListenConfig
	eg              *errgroup.Group
	cancel          context.CancelFunc
	runtimes        []*raftGroupRuntime
	shardStore      *kv.ShardStore
	coordinate      kv.Coordinator
	distServer      *adapter.DistributionServer
	redisAddress    string
	leaderRedis     map[raft.ServerAddress]string
	pubsubRelay     *adapter.RedisPubSubRelay
	readTracker     *kv.ActiveTimestampTracker
	dynamoAddress   string
	leaderDynamo    map[raft.ServerAddress]string
	s3Address       string
	leaderS3        map[raft.ServerAddress]string
	s3Region        string
	s3CredsFile     string
	s3PathStyleOnly bool
	metricsAddress  string
	metricsToken    string
	pprofAddress    string
	pprofToken      string
	metricsRegistry *monitoring.Registry
}

func (r runtimeServerRunner) start() error {
	if err := startRedisServer(r.ctx, r.lc, r.eg, r.redisAddress, r.shardStore, r.coordinate, r.leaderRedis, r.pubsubRelay, r.metricsRegistry, r.readTracker); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startRaftServers(
		r.ctx,
		r.lc,
		r.eg,
		r.runtimes,
		r.shardStore,
		r.coordinate,
		r.distServer,
		r.pubsubRelay,
		func(groupID uint64) kv.ProposalObserver {
			return r.metricsRegistry.RaftProposalObserver(groupID)
		},
	); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startDynamoDBServer(r.ctx, r.lc, r.eg, r.dynamoAddress, r.shardStore, r.coordinate, r.leaderDynamo, r.metricsRegistry, r.readTracker); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startS3Server(r.ctx, r.lc, r.eg, r.s3Address, r.shardStore, r.coordinate, r.leaderS3, r.s3Region, r.s3CredsFile, r.s3PathStyleOnly, r.readTracker); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startMetricsServer(r.ctx, r.lc, r.eg, r.metricsAddress, r.metricsToken, r.metricsRegistry.Handler()); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startPprofServer(r.ctx, r.lc, r.eg, r.pprofAddress, r.pprofToken); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	return nil
}
