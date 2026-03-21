package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
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
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftstore"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	address        = flag.String("address", ":50051", "gRPC/Raft address")
	redisAddress   = flag.String("redisAddress", ":6379", "Redis address")
	dynamoAddress  = flag.String("dynamoAddress", ":8000", "DynamoDB-compatible API address")
	metricsAddress = flag.String("metricsAddress", "127.0.0.1:9090", "Prometheus metrics address")
	metricsToken   = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	raftID         = flag.String("raftId", "", "Raft ID")
	raftDataDir    = flag.String("raftDataDir", "/var/lib/elastickv", "Raft data directory")
	raftBootstrap  = flag.Bool("raftBootstrap", false, "Bootstrap cluster")
	raftRedisMap   = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
)

const (
	raftSnapshotsRetain = 2
	kvParts             = 2
	defaultFileMode     = 0755
	joinRetries         = 20
	joinWait            = 3 * time.Second
	joinRetryInterval   = 1 * time.Second
	joinRPCTimeout      = 3 * time.Second
	raftObserveInterval = 5 * time.Second
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

type config struct {
	address        string
	redisAddress   string
	dynamoAddress  string
	metricsAddress string
	metricsToken   string
	raftID         string
	raftDataDir    string
	raftBootstrap  bool
	raftRedisMap   string
}

func main() {
	flag.Parse()

	eg, runCtx := errgroup.WithContext(context.Background())

	if *raftID != "" {
		// Single node mode
		cfg := config{
			address:        *address,
			redisAddress:   *redisAddress,
			dynamoAddress:  *dynamoAddress,
			metricsAddress: *metricsAddress,
			metricsToken:   *metricsToken,
			raftID:         *raftID,
			raftDataDir:    *raftDataDir,
			raftBootstrap:  *raftBootstrap,
			raftRedisMap:   *raftRedisMap,
		}
		if err := run(runCtx, eg, cfg); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	} else {
		// Demo cluster mode (3 nodes)
		slog.Info("Starting demo cluster with 3 nodes...")
		demoMetricsToken := effectiveDemoMetricsToken(*metricsToken)
		nodes := []config{
			{
				address:        "127.0.0.1:50051",
				redisAddress:   "127.0.0.1:63791",
				dynamoAddress:  "127.0.0.1:63801",
				metricsAddress: "0.0.0.0:9091",
				metricsToken:   demoMetricsToken,
				raftID:         "n1",
				raftDataDir:    "", // In-memory
				raftBootstrap:  true,
			},
			{
				address:        "127.0.0.1:50052",
				redisAddress:   "127.0.0.1:63792",
				dynamoAddress:  "127.0.0.1:63802",
				metricsAddress: "0.0.0.0:9092",
				metricsToken:   demoMetricsToken,
				raftID:         "n2",
				raftDataDir:    "",
				raftBootstrap:  false,
			},
			{
				address:        "127.0.0.1:50053",
				redisAddress:   "127.0.0.1:63793",
				dynamoAddress:  "127.0.0.1:63803",
				metricsAddress: "0.0.0.0:9093",
				metricsToken:   demoMetricsToken,
				raftID:         "n3",
				raftDataDir:    "",
				raftBootstrap:  false,
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
			if err := run(runCtx, eg, cfg); err != nil {
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
			return joinCluster(runCtx, nodes)
		})
	}

	if err := eg.Wait(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func effectiveDemoMetricsToken(token string) string {
	token = strings.TrimSpace(token)
	if token != "" {
		return token
	}
	return "demo-metrics-token"
}

func joinCluster(ctx context.Context, nodes []config) error {
	leader := nodes[0]
	// Give servers some time to start
	if err := waitForJoinRetry(ctx, joinWait); err != nil {
		return joinClusterWaitError(err)
	}

	// Connect to leader
	conn, err := grpc.NewClient(leader.address, internalutil.GRPCDialOptions()...)
	if err != nil {
		return fmt.Errorf("failed to dial leader: %w", err)
	}
	defer conn.Close()
	client := raftadminpb.NewRaftAdminClient(conn)

	for _, n := range nodes[1:] {
		if err := joinNodeWithRetry(ctx, client, n); err != nil {
			return err
		}
	}
	return nil
}

func joinNodeWithRetry(ctx context.Context, client raftadminpb.RaftAdminClient, n config) error {
	for i := range joinRetries {
		if err := tryJoinNode(ctx, client, n); err == nil {
			return nil
		} else {
			if ctx.Err() != nil {
				// Retry loop should stop immediately once the parent context is canceled.
				return joinRetryCancelResult(ctx)
			}
			slog.Warn("Failed to join node, retrying...", "id", n.raftID, "err", err)
		}
		if i == joinRetries-1 {
			break
		}
		if err := waitForJoinRetry(ctx, joinRetryInterval); err != nil {
			return joinRetryCancelResult(ctx)
		}
	}
	if ctx.Err() != nil {
		return joinRetryCancelResult(ctx)
	}
	return fmt.Errorf("failed to join node %s after retries", n.raftID)
}

func joinRetryCancelResult(ctx context.Context) error {
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	return joinClusterWaitError(errors.WithStack(ctx.Err()))
}

func tryJoinNode(ctx context.Context, client raftadminpb.RaftAdminClient, n config) error {
	slog.Info("Attempting to join node", "id", n.raftID, "address", n.address)
	addCtx, cancelAdd := context.WithTimeout(ctx, joinRPCTimeout)
	defer cancelAdd()
	future, err := client.AddVoter(addCtx, &raftadminpb.AddVoterRequest{
		Id:            n.raftID,
		Address:       n.address,
		PreviousIndex: 0,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	awaitCtx, cancelAwait := context.WithTimeout(ctx, joinRPCTimeout)
	defer cancelAwait()
	await, err := client.Await(awaitCtx, future)
	if err != nil {
		return errors.WithStack(err)
	}
	if await.GetError() != "" {
		return errors.New(await.GetError())
	}
	slog.Info("Successfully joined node", "id", n.raftID)
	return nil
}

func waitForJoinRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func joinClusterWaitError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Do not override the original errgroup cause with cancellation.
		return nil
	}
	return err
}

func setupStorage(dir string) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	if dir == "" {
		return raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), nil
	}
	for _, legacy := range []string{"logs.dat", "stable.dat"} {
		if _, err := os.Stat(filepath.Join(dir, legacy)); err == nil {
			return nil, nil, nil, errors.WithStack(errors.Newf(
				"legacy boltdb Raft storage %q found in %s; manual migration required before using Pebble-backed storage",
				legacy, dir,
			))
		}
	}
	raftStore, err := raftstore.NewPebbleStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	fss, err := raft.NewFileSnapshotStore(dir, raftSnapshotsRetain, os.Stdout)
	if err != nil {
		_ = raftStore.Close()
		return nil, nil, nil, errors.WithStack(err)
	}
	return raftStore, raftStore, fss, nil
}

func setupGRPC(r *raft.Raft, st store.MVCCStore, tm *transport.Manager, coordinator *kv.Coordinate, distServer *adapter.DistributionServer, relay *adapter.RedisPubSubRelay) (*grpc.Server, *adapter.GRPCServer) {
	s := grpc.NewServer(internalutil.GRPCServerOptions()...)
	trx := kv.NewTransaction(r)
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	gs := adapter.NewGRPCServer(routedStore, coordinator, adapter.WithCloseStore())
	tm.Register(s)
	pb.RegisterRawKVServer(s, gs)
	pb.RegisterTransactionalKVServer(s, gs)
	pb.RegisterInternalServer(s, adapter.NewInternal(trx, r, coordinator.Clock(), relay))
	pb.RegisterDistributionServer(s, distServer)
	leaderhealth.Setup(r, s, []string{"RawKV"})
	raftadmin.Register(s, r)
	return s, gs
}

func setupRedis(ctx context.Context, lc net.ListenConfig, st store.MVCCStore, coordinator *kv.Coordinate, addr, redisAddr, raftRedisMapStr string, relay *adapter.RedisPubSubRelay, readTracker *kv.ActiveTimestampTracker) (*adapter.RedisServer, error) {
	leaderRedis := make(map[raft.ServerAddress]string)
	if raftRedisMapStr != "" {
		parts := strings.SplitSeq(raftRedisMapStr, ",")
		for part := range parts {
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
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	return adapter.NewRedisServer(l, redisAddr, routedStore, coordinator, leaderRedis, relay, adapter.WithRedisActiveTimestampTracker(readTracker)), nil
}

func run(ctx context.Context, eg *errgroup.Group, cfg config) error {
	var lc net.ListenConfig
	cleanup := internalutil.CleanupStack{}
	defer cleanup.Run()

	ldb, sdb, fss, err := setupStorage(cfg.raftDataDir)
	if err != nil {
		return err
	}

	st := store.NewMVCCStore()
	fsm := kv.NewKvFSM(st)
	readTracker := kv.NewActiveTimestampTracker()

	// Config
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(cfg.raftID)
	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:       "raft-" + cfg.raftID,
		JSONFormat: true,
		Level:      hclog.Info,
	})

	// Transport
	tm := transport.New(raft.ServerAddress(cfg.address), internalutil.GRPCDialOptions())

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return errors.WithStack(err)
	}

	if err := bootstrapClusterIfNeeded(r, cfg); err != nil {
		return err
	}

	trx := kv.NewTransaction(r)
	coordinator := kv.NewCoordinator(trx, r)
	distEngine := distribution.NewEngineWithDefaultRoute()
	distCatalog := distribution.NewCatalogStore(st)
	if _, err := distribution.EnsureCatalogSnapshot(ctx, distCatalog, distEngine); err != nil {
		return errors.WithStack(err)
	}
	distServer := adapter.NewDistributionServer(
		distEngine,
		distCatalog,
		adapter.WithDistributionCoordinator(coordinator),
		adapter.WithDistributionActiveTimestampTracker(readTracker),
	)
	metricsRegistry := monitoring.NewRegistry(cfg.raftID, cfg.address)
	metricsRegistry.RaftObserver().Start(ctx, []monitoring.RaftRuntime{{GroupID: 1, Raft: r}}, raftObserveInterval)
	compactor := kv.NewFSMCompactor(
		[]kv.FSMCompactRuntime{{
			GroupID: 1,
			Raft:    r,
			Store:   st,
		}},
		kv.WithFSMCompactorActiveTimestampTracker(readTracker),
	)
	relay := adapter.NewRedisPubSubRelay()

	s, grpcSvc := setupGRPC(r, st, tm, coordinator, distServer, relay)

	grpcSock, err := lc.Listen(ctx, "tcp", cfg.address)
	if err != nil {
		return errors.WithStack(err)
	}
	cleanup.Add(func() {
		_ = grpcSock.Close()
	})

	rd, err := setupRedis(ctx, lc, st, coordinator, cfg.address, cfg.redisAddress, cfg.raftRedisMap, relay, readTracker)
	if err != nil {
		return err
	}
	cleanup.Add(rd.Stop)
	dynamoL, err := lc.Listen(ctx, "tcp", cfg.dynamoAddress)
	if err != nil {
		return errors.WithStack(err)
	}
	ds := adapter.NewDynamoDBServer(
		dynamoL,
		st,
		coordinator,
		adapter.WithDynamoDBRequestObserver(metricsRegistry.DynamoDBObserver()),
	)
	cleanup.Add(ds.Stop)
	metricsL, ms, err := setupMetricsHTTPServer(ctx, lc, cfg.metricsAddress, cfg.metricsToken, metricsRegistry.Handler())
	if err != nil {
		return err
	}
	if metricsL != nil {
		cleanup.Add(func() {
			_ = metricsL.Close()
		})
	}

	eg.Go(catalogWatcherTask(ctx, distCatalog, distEngine))
	eg.Go(func() error { return compactor.Run(ctx) })
	eg.Go(grpcShutdownTask(ctx, s, grpcSock, cfg.address, grpcSvc))
	eg.Go(grpcServeTask(s, grpcSock, cfg.address))
	eg.Go(redisShutdownTask(ctx, rd, cfg.redisAddress))
	eg.Go(redisServeTask(rd, cfg.redisAddress))
	eg.Go(dynamoShutdownTask(ctx, ds, cfg.dynamoAddress))
	eg.Go(dynamoServeTask(ds, cfg.dynamoAddress))
	eg.Go(monitoring.MetricsShutdownTask(ctx, ms, cfg.metricsAddress))
	eg.Go(monitoring.MetricsServeTask(ms, metricsL, cfg.metricsAddress))

	cleanup.Release()
	return nil
}

func setupMetricsHTTPServer(ctx context.Context, lc net.ListenConfig, metricsAddress string, metricsToken string, handler http.Handler) (net.Listener, *http.Server, error) {
	metricsAddress = strings.TrimSpace(metricsAddress)
	if metricsAddress == "" || handler == nil {
		return nil, nil, nil
	}
	if _, _, err := net.SplitHostPort(metricsAddress); err != nil {
		return nil, nil, errors.Wrapf(err, "invalid metricsAddress %q", metricsAddress)
	}
	if monitoring.MetricsAddressRequiresToken(metricsAddress) && strings.TrimSpace(metricsToken) == "" {
		return nil, nil, errors.New("metricsToken is required when metricsAddress is not loopback")
	}
	metricsL, err := lc.Listen(ctx, "tcp", metricsAddress)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	ms := monitoring.NewMetricsServer(handler, metricsToken)
	return metricsL, ms, nil
}

func bootstrapClusterIfNeeded(r *raft.Raft, cfg config) error {
	if !cfg.raftBootstrap {
		return nil
	}
	bootstrapCfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(cfg.raftID),
				Address:  raft.ServerAddress(cfg.address),
			},
		},
	}
	if err := r.BootstrapCluster(bootstrapCfg).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return errors.WithStack(err)
	}
	return nil
}

func catalogWatcherTask(ctx context.Context, distCatalog *distribution.CatalogStore, distEngine *distribution.Engine) func() error {
	return func() error {
		if err := distribution.RunCatalogWatcher(ctx, distCatalog, distEngine, slog.Default()); err != nil {
			return errors.Wrapf(err, "catalog watcher failed")
		}
		return nil
	}
}

func grpcShutdownTask(ctx context.Context, server *grpc.Server, listener net.Listener, address string, closer io.Closer) func() error {
	return func() error {
		<-ctx.Done()
		slog.Info("Shutting down gRPC server", "address", address, "reason", ctx.Err())
		server.GracefulStop()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("Failed to close gRPC listener", "address", address, "err", err)
		}
		if closer != nil {
			if err := closer.Close(); err != nil {
				slog.Error("Failed to close gRPC service", "address", address, "err", err)
			}
		}
		return nil
	}
}

func grpcServeTask(server *grpc.Server, listener net.Listener, address string) func() error {
	return func() error {
		slog.Info("Starting gRPC server", "address", address)
		err := server.Serve(listener)
		if err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}

func redisShutdownTask(ctx context.Context, redisServer *adapter.RedisServer, address string) func() error {
	return func() error {
		<-ctx.Done()
		slog.Info("Shutting down Redis server", "address", address, "reason", ctx.Err())
		redisServer.Stop()
		return nil
	}
}

func redisServeTask(redisServer *adapter.RedisServer, address string) func() error {
	return func() error {
		slog.Info("Starting Redis server", "address", address)
		err := redisServer.Run()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}

func dynamoShutdownTask(ctx context.Context, dynamoServer *adapter.DynamoDBServer, address string) func() error {
	return func() error {
		<-ctx.Done()
		slog.Info("Shutting down DynamoDB server", "address", address, "reason", ctx.Err())
		dynamoServer.Stop()
		return nil
	}
}

func dynamoServeTask(dynamoServer *adapter.DynamoDBServer, address string) func() error {
	return func() error {
		slog.Info("Starting DynamoDB server", "address", address)
		err := dynamoServer.Run()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}
