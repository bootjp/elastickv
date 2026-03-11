package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
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
	raftMetricsObserveInterval = 5 * time.Second
)

var (
	myAddr               = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr            = flag.String("redisAddress", "localhost:6379", "TCP host+port for redis")
	dynamoAddr           = flag.String("dynamoAddress", "localhost:8000", "TCP host+port for DynamoDB-compatible API")
	metricsAddr          = flag.String("metricsAddress", "localhost:9090", "TCP host+port for Prometheus metrics")
	metricsToken         = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	raftId               = flag.String("raftId", "", "Node id used by Raft")
	raftDir              = flag.String("raftDataDir", "data/", "Raft data dir")
	raftBootstrap        = flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	raftBootstrapMembers = flag.String("raftBootstrapMembers", "", "Comma-separated bootstrap raft members (raftID=host:port,...)")
	raftGroups           = flag.String("raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	shardRanges          = flag.String("shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	raftRedisMap         = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("%v", err)
	}
}

func run() error {
	if *raftId == "" {
		return errors.New("flag --raftId is required")
	}

	var lc net.ListenConfig

	cfg, err := parseRuntimeConfig(*myAddr, *redisAddr, *raftGroups, *shardRanges, *raftRedisMap)
	if err != nil {
		return err
	}
	bootstrapServers, err := resolveBootstrapServers(*raftId, cfg.groups, *raftBootstrapMembers)
	if err != nil {
		return err
	}
	bootstrap := *raftBootstrap || len(bootstrapServers) > 0

	runtimes, shardGroups, err := buildShardGroups(*raftId, *raftDir, cfg.groups, cfg.multi, bootstrap, bootstrapServers)
	if err != nil {
		return err
	}

	cleanup := internalutil.CleanupStack{}
	defer cleanup.Run()

	ctx, cancel := context.WithCancel(context.Background())
	clock := kv.NewHLC()
	shardStore := kv.NewShardStore(cfg.engine, shardGroups)
	cleanup.Add(func() {
		_ = shardStore.Close()
		for _, rt := range runtimes {
			rt.Close()
		}
	})
	cleanup.Add(cancel)
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
	)
	metricsRegistry := monitoring.NewRegistry(*raftId, *myAddr)
	metricsRegistry.RaftObserver().Start(runCtx, raftMonitorRuntimes(runtimes), raftMetricsObserveInterval)

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
		dynamoAddress:   *dynamoAddr,
		metricsAddress:  *metricsAddr,
		metricsToken:    *metricsToken,
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

const snapshotRetainCount = 3

type runtimeConfig struct {
	groups       []groupSpec
	defaultGroup uint64
	engine       *distribution.Engine
	leaderRedis  map[raft.ServerAddress]string
	multi        bool
}

func parseRuntimeConfig(myAddr, redisAddr, raftGroups, shardRanges, raftRedisMap string) (runtimeConfig, error) {
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

	return runtimeConfig{
		groups:       groups,
		defaultGroup: defaultGroup,
		engine:       engine,
		leaderRedis:  leaderRedis,
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
	leaderRedis, err := parseRaftRedisMap(raftRedisMap)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		addr := raft.ServerAddress(g.address)
		if _, ok := leaderRedis[addr]; !ok {
			leaderRedis[addr] = redisAddr
		}
	}
	return leaderRedis, nil
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

func buildShardGroups(raftID string, raftDir string, groups []groupSpec, multi bool, bootstrap bool, bootstrapServers []raft.Server) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, error) {
	runtimes := make([]*raftGroupRuntime, 0, len(groups))
	shardGroups := make(map[uint64]*kv.ShardGroup, len(groups))
	for _, g := range groups {
		st := store.NewMVCCStore()
		fsm := kv.NewKvFSM(st)
		r, tm, closeStores, err := newRaftGroup(raftID, g, raftDir, multi, bootstrap, bootstrapServers, fsm)
		if err != nil {
			for _, rt := range runtimes {
				rt.Close()
			}
			if r != nil {
				_ = r.Shutdown().Error()
			}
			if tm != nil {
				_ = tm.Close()
			}
			_ = st.Close()
			if closeStores != nil {
				closeStores()
			}
			return nil, nil, errors.Wrapf(err, "failed to start raft group %d", g.id)
		}
		runtimes = append(runtimes, &raftGroupRuntime{
			spec:        g,
			raft:        r,
			tm:          tm,
			store:       st,
			closeStores: closeStores,
		})
		shardGroups[g.id] = &kv.ShardGroup{
			Raft:  r,
			Store: st,
			Txn:   kv.NewLeaderProxy(r),
		}
	}
	return runtimes, shardGroups, nil
}

func raftMonitorRuntimes(runtimes []*raftGroupRuntime) []monitoring.RaftRuntime {
	out := make([]monitoring.RaftRuntime, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.raft == nil {
			continue
		}
		out = append(out, monitoring.RaftRuntime{
			GroupID: runtime.spec.id,
			Raft:    runtime.raft,
		})
	}
	return out
}

func startRaftServers(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, runtimes []*raftGroupRuntime, shardStore *kv.ShardStore, coordinate kv.Coordinator, distServer *adapter.DistributionServer) error {
	for _, rt := range runtimes {
		gs := grpc.NewServer()
		trx := kv.NewTransaction(rt.raft)
		grpcSvc := adapter.NewGRPCServer(shardStore, coordinate)
		pb.RegisterRawKVServer(gs, grpcSvc)
		pb.RegisterTransactionalKVServer(gs, grpcSvc)
		pb.RegisterInternalServer(gs, adapter.NewInternal(trx, rt.raft, coordinate.Clock()))
		pb.RegisterDistributionServer(gs, distServer)
		rt.tm.Register(gs)
		leaderhealth.Setup(rt.raft, gs, []string{"RawKV"})
		raftadmin.Register(gs, rt.raft)
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

func startRedisServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, redisAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderRedis map[raft.ServerAddress]string) error {
	redisL, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", redisAddr)
	}
	redisServer := adapter.NewRedisServer(redisL, shardStore, coordinate, leaderRedis)
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

func startDynamoDBServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, dynamoAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, metricsRegistry *monitoring.Registry) error {
	dynamoL, err := lc.Listen(ctx, "tcp", dynamoAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", dynamoAddr)
	}
	dynamoServer := adapter.NewDynamoDBServer(
		dynamoL,
		shardStore,
		coordinate,
		adapter.WithDynamoDBRequestObserver(metricsRegistry.DynamoDBObserver()),
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

func startMetricsServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, metricsAddr string, metricsToken string, handler http.Handler) error {
	if strings.TrimSpace(metricsAddr) == "" || handler == nil {
		return nil
	}
	if monitoring.MetricsAddressRequiresToken(metricsAddr) && strings.TrimSpace(metricsToken) == "" {
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
	dynamoAddress   string
	metricsAddress  string
	metricsToken    string
	metricsRegistry *monitoring.Registry
}

func (r runtimeServerRunner) start() error {
	if err := startRaftServers(r.ctx, r.lc, r.eg, r.runtimes, r.shardStore, r.coordinate, r.distServer); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startRedisServer(r.ctx, r.lc, r.eg, r.redisAddress, r.shardStore, r.coordinate, r.leaderRedis); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startDynamoDBServer(r.ctx, r.lc, r.eg, r.dynamoAddress, r.shardStore, r.coordinate, r.metricsRegistry); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startMetricsServer(r.ctx, r.lc, r.eg, r.metricsAddress, r.metricsToken, r.metricsRegistry.Handler()); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	return nil
}
