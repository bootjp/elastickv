package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	raftadminpb "github.com/Jille/raftadmin/proto"
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
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
	redisAddress  = flag.String("redisAddress", ":6379", "Redis address")
	raftID        = flag.String("raftId", "", "Raft ID")
	raftDataDir   = flag.String("raftDataDir", "/var/lib/elastickv", "Raft data directory")
	raftBootstrap = flag.Bool("raftBootstrap", false, "Bootstrap cluster")
	raftRedisMap  = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
)

const (
	raftSnapshotsRetain = 2
	kvParts             = 2
	defaultFileMode     = 0755
	joinRetries         = 20
	joinWait            = 3 * time.Second
	joinRetryInterval   = 1 * time.Second
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

type config struct {
	address       string
	redisAddress  string
	raftID        string
	raftDataDir   string
	raftBootstrap bool
	raftRedisMap  string
}

func main() {
	flag.Parse()

	eg := &errgroup.Group{}

	if *raftID != "" {
		// Single node mode
		cfg := config{
			address:       *address,
			redisAddress:  *redisAddress,
			raftID:        *raftID,
			raftDataDir:   *raftDataDir,
			raftBootstrap: *raftBootstrap,
			raftRedisMap:  *raftRedisMap,
		}
		if err := run(eg, cfg); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	} else {
		// Demo cluster mode (3 nodes)
		slog.Info("Starting demo cluster with 3 nodes...")
		nodes := []config{
			{
				address:       "127.0.0.1:50051",
				redisAddress:  "127.0.0.1:63791",
				raftID:        "n1",
				raftDataDir:   "", // In-memory
				raftBootstrap: true,
			},
			{
				address:       "127.0.0.1:50052",
				redisAddress:  "127.0.0.1:63792",
				raftID:        "n2",
				raftDataDir:   "",
				raftBootstrap: false,
			},
			{
				address:       "127.0.0.1:50053",
				redisAddress:  "127.0.0.1:63793",
				raftID:        "n3",
				raftDataDir:   "",
				raftBootstrap: false,
			},
		}

		// Build raftRedisMap string
		var mapParts []string
		for _, n := range nodes {
			mapParts = append(mapParts, n.address+"="+n.redisAddress)
		}
		raftRedisMapStr := strings.Join(mapParts, ",")

		for _, n := range nodes {
			n.raftRedisMap = raftRedisMapStr
			cfg := n // capture loop variable
			if err := run(eg, cfg); err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}

		// Wait for n1 to be ready then join others?
		// Actually, standard bootstrap expects a configuration.
		// If we only bootstrap n1, we need to join n2 and n3.
		// For simplicity in this demo, let's bootstrap n1 with just n1, and have n2/n3 join.
		// Or better: bootstrap n1 with {n1, n2, n3}.
		// But run() logic for bootstrap only adds *raftID to configuration.

		// Let's modify bootstrapping logic in run() slightly or just rely on manual join?
		// The original demo likely used raftadmin to join or predefined bootstrap.
		// Since we can't easily change run() logic too much without breaking Jepsen,
		// let's use a separate goroutine to join n2/n3 to n1 after a delay.

		eg.Go(func() error {
			// Wait a bit for n1 to start
			// This is hacky but sufficient for a demo
			// Better would be to wait for gRPC readiness
			// But standard 'sleep' is unavailable here without import time
			// We can use a simple retry loop to join.

			// Actually, let's keep it simple: just start them.
			// If n1 bootstraps as a single node cluster, n2 and n3 won't be part of it automatically.
			// We need to issue 'add_voter' commands.
			// Let's rely on an external script or add a helper here?

			// For this specific demo restoration, we'll assume the external script might handle joins
			// OR we check if the CI script does it.
			// The CI script just waits for ports. It runs `lein run ...` which assumes a cluster.
			// If the cluster isn't formed, the tests might fail.
			// BUT, looking at the previous demo.go (if I could), it probably did the joins.

			// Let's add a joiner goroutine.
			return joinCluster(nodes)
		})
	}

	if err := eg.Wait(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func joinCluster(nodes []config) error {
	leader := nodes[0]
	// Give servers some time to start
	time.Sleep(joinWait)

	// Connect to leader
	conn, err := grpc.NewClient(leader.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial leader: %w", err)
	}
	defer conn.Close()
	client := raftadminpb.NewRaftAdminClient(conn)

	ctx := context.Background()
	for _, n := range nodes[1:] {
		var joined bool
		for i := 0; i < joinRetries; i++ {
			slog.Info("Attempting to join node", "id", n.raftID, "address", n.address)
			_, err := client.AddVoter(ctx, &raftadminpb.AddVoterRequest{
				Id:            n.raftID,
				Address:       n.address,
				PreviousIndex: 0,
			})
			if err == nil {
				slog.Info("Successfully joined node", "id", n.raftID)
				joined = true
				break
			}
			slog.Warn("Failed to join node, retrying...", "id", n.raftID, "err", err)
			time.Sleep(joinRetryInterval)
		}
		if !joined {
			return fmt.Errorf("failed to join node %s after retries", n.raftID)
		}
	}
	return nil
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

func setupGRPC(r *raft.Raft, st store.MVCCStore, tm *transport.Manager, coordinator *kv.Coordinate, distServer *adapter.DistributionServer) *grpc.Server {
	s := grpc.NewServer()
	trx := kv.NewTransaction(r)
	gs := adapter.NewGRPCServer(st, coordinator)
	tm.Register(s)
	pb.RegisterRawKVServer(s, gs)
	pb.RegisterTransactionalKVServer(s, gs)
	pb.RegisterInternalServer(s, adapter.NewInternal(trx, r, coordinator.Clock()))
	pb.RegisterDistributionServer(s, distServer)
	leaderhealth.Setup(r, s, []string{"RawKV"})
	raftadmin.Register(s, r)
	return s
}

func setupRedis(ctx context.Context, lc net.ListenConfig, st store.MVCCStore, coordinator *kv.Coordinate, addr, redisAddr, raftRedisMapStr string) (*adapter.RedisServer, error) {
	leaderRedis := make(map[raft.ServerAddress]string)
	if raftRedisMapStr != "" {
		parts := strings.Split(raftRedisMapStr, ",")
		for _, part := range parts {
			kv := strings.Split(part, "=")
			if len(kv) == kvParts {
				leaderRedis[raft.ServerAddress(kv[0])] = kv[1]
			}
		}
	}
	// Ensure self is in map (override if present)
	leaderRedis[raft.ServerAddress(addr)] = redisAddr

	l, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return adapter.NewRedisServer(l, st, coordinator, leaderRedis), nil
}

func run(eg *errgroup.Group, cfg config) error {
	ctx := context.Background()
	var lc net.ListenConfig

	ldb, sdb, fss, err := setupStorage(cfg.raftDataDir)
	if err != nil {
		return err
	}

	st := store.NewMVCCStore()
	fsm := kv.NewKvFSM(st)

	// Config
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(cfg.raftID)
	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:       "raft-" + cfg.raftID,
		JSONFormat: true,
		Level:      hclog.Info,
	})

	// Transport
	tm := transport.New(raft.ServerAddress(cfg.address), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return errors.WithStack(err)
	}

	if cfg.raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(cfg.raftID),
					Address:  raft.ServerAddress(cfg.address),
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
	distEngine := distribution.NewEngineWithDefaultRoute()
	distServer := adapter.NewDistributionServer(distEngine)

	s := setupGRPC(r, st, tm, coordinator, distServer)

	grpcSock, err := lc.Listen(ctx, "tcp", cfg.address)
	if err != nil {
		return errors.WithStack(err)
	}

	eg.Go(func() error {
		slog.Info("Starting gRPC server", "address", cfg.address)
		return errors.WithStack(s.Serve(grpcSock))
	})

	rd, err := setupRedis(ctx, lc, st, coordinator, cfg.address, cfg.redisAddress, cfg.raftRedisMap)
	if err != nil {
		return err
	}

	eg.Go(func() error {
		slog.Info("Starting Redis server", "address", cfg.redisAddress)
		return errors.WithStack(rd.Run())
	})

	return nil
}
