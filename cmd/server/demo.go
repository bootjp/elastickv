package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	address       = flag.String("address", ":50051", "gRPC/Raft address")
	redisAddress  = flag.String("redis_address", ":6379", "Redis address")
	raftID        = flag.String("raft_id", "", "Raft ID")
	raftDataDir   = flag.String("raft_data_dir", "/var/lib/elastickv", "Raft data directory")
	raftBootstrap = flag.Bool("raft_bootstrap", false, "Bootstrap cluster")
	raftRedisMap  = flag.String("raft_redis_map", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
)

const (
	raftSnapshotsRetain = 2
	kvParts             = 2
	defaultFileMode     = 0755
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

func main() {
	flag.Parse()
	if *raftID == "" {
		slog.Error("raft_id is required")
		os.Exit(1)
	}

	eg := &errgroup.Group{}
	if err := run(eg); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	if err := eg.Wait(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func setupStorage(dir string) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	if dir == "" {
		return raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), nil
	}
	if err := os.MkdirAll(dir, defaultFileMode); err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	ldb, err := raftboltdb.NewBoltStore(filepath.Join(dir, "logs.dat"))
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	sdb, err := raftboltdb.NewBoltStore(filepath.Join(dir, "stable.dat"))
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	fss, err := raft.NewFileSnapshotStore(dir, raftSnapshotsRetain, os.Stdout)
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	return ldb, sdb, fss, nil
}

func setupGRPC(r *raft.Raft, st store.MVCCStore, tm *transport.Manager, coordinator *kv.Coordinate) *grpc.Server {
	s := grpc.NewServer()
	trx := kv.NewTransaction(r)
	gs := adapter.NewGRPCServer(st, coordinator)
	tm.Register(s)
	pb.RegisterRawKVServer(s, gs)
	pb.RegisterTransactionalKVServer(s, gs)
	pb.RegisterInternalServer(s, adapter.NewInternal(trx, r, coordinator.Clock()))
	leaderhealth.Setup(r, s, []string{"RawKV"})
	raftadmin.Register(s, r)
	return s
}

func setupRedis(ctx context.Context, lc net.ListenConfig, st store.MVCCStore, coordinator *kv.Coordinate, addr string) (*adapter.RedisServer, error) {
	leaderRedis := make(map[raft.ServerAddress]string)
	if *raftRedisMap != "" {
		parts := strings.Split(*raftRedisMap, ",")
		for _, part := range parts {
			kv := strings.Split(part, "=")
			if len(kv) == kvParts {
				leaderRedis[raft.ServerAddress(kv[0])] = kv[1]
			}
		}
	}
	// Ensure self is in map (override if present)
	leaderRedis[raft.ServerAddress(addr)] = *redisAddress

	l, err := lc.Listen(ctx, "tcp", *redisAddress)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return adapter.NewRedisServer(l, st, coordinator, leaderRedis), nil
}

func run(eg *errgroup.Group) error {
	ctx := context.Background()
	var lc net.ListenConfig

	ldb, sdb, fss, err := setupStorage(*raftDataDir)
	if err != nil {
		return err
	}

	st := store.NewMVCCStore()
	fsm := kv.NewKvFSM(st)

	// Config
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(*raftID)
	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:       "raft-" + *raftID,
		JSONFormat: true,
		Level:      hclog.Info,
	})

	// Transport
	tm := transport.New(raft.ServerAddress(*address), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return errors.WithStack(err)
	}

	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(*raftID),
					Address:  raft.ServerAddress(*address),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return errors.WithStack(err)
		}
	}

	trx := kv.NewTransaction(r)
	coordinator := kv.NewCoordinator(trx, r)

	s := setupGRPC(r, st, tm, coordinator)

	grpcSock, err := lc.Listen(ctx, "tcp", *address)
	if err != nil {
		return errors.WithStack(err)
	}

	eg.Go(func() error {
		slog.Info("Starting gRPC server", "address", *address)
		return errors.WithStack(s.Serve(grpcSock))
	})

	rd, err := setupRedis(ctx, lc, st, coordinator, *address)
	if err != nil {
		return err
	}

	eg.Go(func() error {
		slog.Info("Starting Redis server", "address", *redisAddress)
		return errors.WithStack(rd.Run())
	})

	return nil
}