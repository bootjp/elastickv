package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	heartbeatTimeout = 200 * time.Millisecond
	electionTimeout  = 2000 * time.Millisecond
	leaderLease      = 100 * time.Millisecond
)

var (
	myAddr        = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr     = flag.String("redisAddress", "localhost:6379", "TCP host+port for redis")
	raftId        = flag.String("raftId", "", "Node id used by Raft")
	raftDir       = flag.String("raftDataDir", "data/", "Raft data dir")
	raftBootstrap = flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	raftGroups    = flag.String("raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	shardRanges   = flag.String("shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	raftRedisMap  = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
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

	ctx := context.Background()
	var lc net.ListenConfig

	cfg, err := parseRuntimeConfig(*myAddr, *redisAddr, *raftGroups, *shardRanges, *raftRedisMap)
	if err != nil {
		return err
	}

	runtimes, shardGroups, err := buildShardGroups(*raftId, *raftDir, cfg.groups, cfg.multi, *raftBootstrap)
	if err != nil {
		return err
	}

	clock := kv.NewHLC()
	shardStore := kv.NewShardStore(cfg.engine, shardGroups)
	coordinate := kv.NewShardedCoordinator(cfg.engine, shardGroups, cfg.defaultGroup, clock, shardStore)
	distServer := adapter.NewDistributionServer(cfg.engine)

	eg := errgroup.Group{}
	if err := startRaftServers(ctx, &lc, &eg, runtimes, shardStore, coordinate, distServer); err != nil {
		return err
	}
	if err := startRedisServer(ctx, &lc, &eg, *redisAddr, shardStore, coordinate, cfg.leaderRedis); err != nil {
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
		multi:        len(groups) > 1 || raftGroups != "",
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

func buildShardGroups(raftID string, raftDir string, groups []groupSpec, multi bool, bootstrap bool) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, error) {
	runtimes := make([]*raftGroupRuntime, 0, len(groups))
	shardGroups := make(map[uint64]*kv.ShardGroup, len(groups))
	for _, g := range groups {
		st := store.NewMVCCStore()
		fsm := kv.NewKvFSM(st)
		r, tm, err := newRaftGroup(raftID, g, raftDir, multi, bootstrap, fsm)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to start raft group %d", g.id)
		}
		runtimes = append(runtimes, &raftGroupRuntime{
			spec:  g,
			raft:  r,
			tm:    tm,
			store: st,
		})
		shardGroups[g.id] = &kv.ShardGroup{
			Raft:  r,
			Store: st,
			Txn:   kv.NewLeaderProxy(r),
		}
	}
	return runtimes, shardGroups, nil
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
			defer func() { _ = grpcService.Close() }()
			return errors.WithStack(srv.Serve(lis))
		})
	}
	return nil
}

func startRedisServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, redisAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderRedis map[raft.ServerAddress]string) error {
	redisL, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", redisAddr)
	}
	eg.Go(func() error {
		return errors.WithStack(adapter.NewRedisServer(redisL, shardStore, coordinate, leaderRedis).Run())
	})
	return nil
}
