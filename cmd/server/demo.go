package main

import (
	"context"
	"encoding/json"
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

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	address        = flag.String("address", ":50051", "gRPC/Raft address")
	redisAddress   = flag.String("redisAddress", ":6379", "Redis address")
	dynamoAddress  = flag.String("dynamoAddress", ":8000", "DynamoDB-compatible API address")
	s3Address      = flag.String("s3Address", ":9000", "S3-compatible API address")
	s3Region       = flag.String("s3Region", "us-east-1", "S3 signing region")
	s3CredsFile    = flag.String("s3CredentialsFile", "", "Path to a JSON file containing static S3 credentials")
	s3PathStyle    = flag.Bool("s3PathStyleOnly", true, "Only accept path-style S3 requests")
	metricsAddress = flag.String("metricsAddress", "127.0.0.1:9090", "Prometheus metrics address")
	metricsToken   = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	pprofAddress   = flag.String("pprofAddress", "localhost:6060", "TCP host+port for pprof debug endpoints; empty to disable")
	pprofToken     = flag.String("pprofToken", "", "Bearer token for pprof; required for non-loopback pprofAddress")
	raftID         = flag.String("raftId", "", "Raft ID")
	raftDataDir    = flag.String("raftDataDir", "/var/lib/elastickv", "Raft data directory")
	raftBootstrap  = flag.Bool("raftBootstrap", false, "Bootstrap cluster")
	raftRedisMap   = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
	raftS3Map      = flag.String("raftS3Map", "", "Map of Raft address to S3 address (raftAddr=s3Addr,...)")
	raftDynamoMap  = flag.String("raftDynamoMap", "", "Map of Raft address to DynamoDB address (raftAddr=dynamoAddr,...)")
)

const (
	kvParts               = 2
	defaultFileMode       = 0755
	joinRetries           = 20
	joinWait              = 3 * time.Second
	joinRetryInterval     = 1 * time.Second
	joinRPCTimeout        = 3 * time.Second
	raftObserveInterval   = 5 * time.Second
	demoTickInterval      = 10 * time.Millisecond
	demoHeartbeatTick     = 1
	demoElectionTick      = 10
	demoMaxSizePerMsg     = 1 << 20
	demoMaxInflightMsg    = 256
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
	s3Address      string
	s3Region       string
	s3CredsFile    string
	s3PathStyle    bool
	metricsAddress string
	metricsToken   string
	pprofAddress   string
	pprofToken     string
	raftID         string
	raftDataDir    string
	raftBootstrap  bool
	raftRedisMap   string
	raftS3Map      string
	raftDynamoMap  string
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
			s3Address:      *s3Address,
			s3Region:       *s3Region,
			s3CredsFile:    *s3CredsFile,
			s3PathStyle:    *s3PathStyle,
			metricsAddress: *metricsAddress,
			metricsToken:   *metricsToken,
			pprofAddress:   *pprofAddress,
			pprofToken:     *pprofToken,
			raftID:         *raftID,
			raftDataDir:    *raftDataDir,
			raftBootstrap:  *raftBootstrap,
			raftRedisMap:   *raftRedisMap,
			raftS3Map:      *raftS3Map,
			raftDynamoMap:  *raftDynamoMap,
		}
		if err := run(runCtx, eg, cfg); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	} else {
		// Demo cluster mode (3 nodes)
		slog.Info("Starting demo cluster with 3 nodes...")
		demoMetricsToken := effectiveDemoMetricsToken(*metricsToken)
		demoPprofAddresses := []string{"127.0.0.1:6061", "127.0.0.1:6062", "127.0.0.1:6063"}
		if strings.TrimSpace(*pprofAddress) == "" {
			demoPprofAddresses = []string{"", "", ""}
		}
		nodes := []config{
			{
				address:        "127.0.0.1:50051",
				redisAddress:   "127.0.0.1:63791",
				dynamoAddress:  "127.0.0.1:63801",
				s3Address:      "127.0.0.1:63901",
				s3Region:       "us-east-1",
				s3PathStyle:    true,
				metricsAddress: "0.0.0.0:9091",
				metricsToken:   demoMetricsToken,
				pprofAddress:   demoPprofAddresses[0],
				raftID:         "n1",
				raftDataDir:    "", // In-memory
				raftBootstrap:  true,
			},
			{
				address:        "127.0.0.1:50052",
				redisAddress:   "127.0.0.1:63792",
				dynamoAddress:  "127.0.0.1:63802",
				s3Address:      "127.0.0.1:63902",
				s3Region:       "us-east-1",
				s3PathStyle:    true,
				metricsAddress: "0.0.0.0:9092",
				metricsToken:   demoMetricsToken,
				pprofAddress:   demoPprofAddresses[1],
				raftID:         "n2",
				raftDataDir:    "",
				raftBootstrap:  false,
			},
			{
				address:        "127.0.0.1:50053",
				redisAddress:   "127.0.0.1:63793",
				dynamoAddress:  "127.0.0.1:63803",
				s3Address:      "127.0.0.1:63903",
				s3Region:       "us-east-1",
				s3PathStyle:    true,
				metricsAddress: "0.0.0.0:9093",
				metricsToken:   demoMetricsToken,
				pprofAddress:   demoPprofAddresses[2],
				raftID:         "n3",
				raftDataDir:    "",
				raftBootstrap:  false,
			},
		}

		// Build raftRedisMap/raftS3Map/raftDynamoMap strings.
		var redisMapParts []string
		var s3MapParts []string
		var dynamoMapParts []string
		for _, n := range nodes {
			redisMapParts = append(redisMapParts, n.address+"="+n.redisAddress)
			s3MapParts = append(s3MapParts, n.address+"="+n.s3Address)
			dynamoMapParts = append(dynamoMapParts, n.address+"="+n.dynamoAddress)
		}
		raftRedisMapStr := strings.Join(redisMapParts, ",")
		raftS3MapStr := strings.Join(s3MapParts, ",")
		raftDynamoMapStr := strings.Join(dynamoMapParts, ",")

		for _, n := range nodes {
			n.raftRedisMap = raftRedisMapStr
			n.raftS3Map = raftS3MapStr
			n.raftDynamoMap = raftDynamoMapStr
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
	client := pb.NewRaftAdminClient(conn)

	for _, n := range nodes[1:] {
		if err := joinNodeWithRetry(ctx, client, n); err != nil {
			return err
		}
	}
	return nil
}

func joinNodeWithRetry(ctx context.Context, client pb.RaftAdminClient, n config) error {
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

func tryJoinNode(ctx context.Context, client pb.RaftAdminClient, n config) error {
	slog.Info("Attempting to join node", "id", n.raftID, "address", n.address)
	addCtx, cancelAdd := context.WithTimeout(ctx, joinRPCTimeout)
	defer cancelAdd()
	_, err := client.AddVoter(addCtx, &pb.RaftAdminAddVoterRequest{
		Id:            n.raftID,
		Address:       n.address,
		PreviousIndex: 0,
	})
	if err != nil {
		return errors.WithStack(err)
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

// setupFSMStore creates and returns the MVCCStore for the Raft FSM.
// When raftDataDir is non-empty the store is persisted under that directory;
// otherwise a temporary directory is used and registered for cleanup on exit.
func setupFSMStore(raftDataDir string, cleanup *internalutil.CleanupStack) (store.MVCCStore, error) {
	if raftDataDir != "" {
		if err := os.MkdirAll(raftDataDir, defaultFileMode); err != nil {
			return nil, errors.WithStack(err)
		}
		st, err := store.NewPebbleStore(filepath.Join(raftDataDir, "fsm.db"))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return st, nil
	}
	fsmDir, err := os.MkdirTemp("", "elastickv-fsm-*")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cleanup.Add(func() { os.RemoveAll(fsmDir) })
	st, err := store.NewPebbleStore(fsmDir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return st, nil
}

func setupGRPC(ctx context.Context, engine raftengine.Engine, registerTransport func(grpc.ServiceRegistrar), st store.MVCCStore, coordinator *kv.Coordinate, distServer *adapter.DistributionServer, relay *adapter.RedisPubSubRelay, proposalObserver kv.ProposalObserver) (*grpc.Server, *adapter.GRPCServer) {
	s := grpc.NewServer(internalutil.GRPCServerOptions()...)
	trx := kv.NewTransactionWithProposer(engine, kv.WithProposalObserver(proposalObserver))
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	gs := adapter.NewGRPCServer(routedStore, coordinator, adapter.WithCloseStore())
	if registerTransport != nil {
		registerTransport(s)
	}
	pb.RegisterRawKVServer(s, gs)
	pb.RegisterTransactionalKVServer(s, gs)
	pb.RegisterInternalServer(s, adapter.NewInternalWithEngine(trx, engine, coordinator.Clock(), relay))
	pb.RegisterDistributionServer(s, distServer)
	internalraftadmin.RegisterOperationalServices(ctx, s, engine, []string{"RawKV"})
	return s, gs
}

func setupRedis(ctx context.Context, lc net.ListenConfig, st store.MVCCStore, coordinator *kv.Coordinate, addr, redisAddr, raftRedisMapStr string, relay *adapter.RedisPubSubRelay, readTracker *kv.ActiveTimestampTracker, deltaCompactor *adapter.DeltaCompactor) (*adapter.RedisServer, error) {
	leaderRedis := make(map[string]string)
	if raftRedisMapStr != "" {
		parts := strings.SplitSeq(raftRedisMapStr, ",")
		for part := range parts {
			kv := strings.Split(part, "=")
			if len(kv) == kvParts {
				leaderRedis[kv[0]] = kv[1]
			}
		}
	}
	// Ensure self is in map (override if present)
	leaderRedis[addr] = redisAddr

	l, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	return adapter.NewRedisServer(l, redisAddr, routedStore, coordinator, leaderRedis, relay,
		adapter.WithRedisActiveTimestampTracker(readTracker),
		adapter.WithRedisCompactor(deltaCompactor),
	), nil
}

func setupS3(
	ctx context.Context,
	lc net.ListenConfig,
	st store.MVCCStore,
	coordinator *kv.Coordinate,
	addr string,
	s3Addr string,
	raftS3MapStr string,
	region string,
	credentialsFile string,
	pathStyleOnly bool,
	readTracker *kv.ActiveTimestampTracker,
) (*adapter.S3Server, error) {
	if !pathStyleOnly {
		return nil, errors.New("virtual-hosted style S3 requests are not implemented")
	}
	if coordinator == nil {
		return nil, errors.New("coordinator must not be nil")
	}
	leaderS3 := make(map[string]string)
	if raftS3MapStr != "" {
		parts := strings.SplitSeq(raftS3MapStr, ",")
		for part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			kv := strings.SplitN(part, "=", kvParts)
			if len(kv) != kvParts {
				slog.Warn("ignoring invalid raft-s3 map entry; expected format addr=s3addr", "entry", part)
				continue
			}
			leaderS3[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	leaderS3[addr] = s3Addr

	l, err := lc.Listen(ctx, "tcp", s3Addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	staticCreds, err := loadS3StaticCredentials(credentialsFile)
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	return adapter.NewS3Server(
		l,
		s3Addr,
		routedStore,
		coordinator,
		leaderS3,
		adapter.WithS3Region(region),
		adapter.WithS3StaticCredentials(staticCreds),
		adapter.WithS3ActiveTimestampTracker(readTracker),
	), nil
}

func setupDynamo(ctx context.Context, lc net.ListenConfig, st store.MVCCStore, coordinator *kv.Coordinate, addr, dynamoAddr, raftDynamoMapStr string, observer monitoring.DynamoDBRequestObserver) (*adapter.DynamoDBServer, error) {
	leaderDynamo := make(map[string]string)
	if raftDynamoMapStr != "" {
		for part := range strings.SplitSeq(raftDynamoMapStr, ",") {
			pair := strings.SplitN(part, "=", kvParts)
			if len(pair) == kvParts {
				leaderDynamo[pair[0]] = pair[1]
			}
		}
	}
	leaderDynamo[addr] = dynamoAddr
	l, err := lc.Listen(ctx, "tcp", dynamoAddr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	return adapter.NewDynamoDBServer(l, routedStore, coordinator,
		adapter.WithDynamoDBRequestObserver(observer),
		adapter.WithDynamoDBLeaderMap(leaderDynamo),
	), nil
}

func run(ctx context.Context, eg *errgroup.Group, cfg config) error {
	var lc net.ListenConfig
	cleanup := internalutil.CleanupStack{}
	defer cleanup.Run()

	st, err := setupFSMStore(cfg.raftDataDir, &cleanup)
	if err != nil {
		return err
	}
	cleanup.Add(func() { st.Close() })
	hlc := kv.NewHLC()
	fsm := kv.NewKvFSMWithHLC(st, hlc)
	readTracker := kv.NewActiveTimestampTracker()

	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval:   demoTickInterval,
		HeartbeatTick:  demoHeartbeatTick,
		ElectionTick:   demoElectionTick,
		MaxSizePerMsg:  demoMaxSizePerMsg,
		MaxInflightMsg: demoMaxInflightMsg,
	})

	raftDir := cfg.raftDataDir
	if raftDir == "" {
		tmp, err := os.MkdirTemp("", "elastickv-raft-*")
		if err != nil {
			return errors.WithStack(err)
		}
		cleanup.Add(func() { os.RemoveAll(tmp) })
		raftDir = tmp
	} else if err := os.MkdirAll(raftDir, defaultFileMode); err != nil {
		return errors.WithStack(err)
	}

	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      cfg.raftID,
		LocalAddress: cfg.address,
		DataDir:      raftDir,
		Bootstrap:    cfg.raftBootstrap,
		StateMachine: fsm,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	cleanup.Add(func() {
		_ = result.Engine.Close()
		if result.Close != nil {
			_ = result.Close()
		}
	})

	metricsRegistry := monitoring.NewRegistry(cfg.raftID, cfg.address)
	proposalObserver := metricsRegistry.RaftProposalObserver(1)
	engine := result.Engine
	trx := kv.NewTransactionWithProposer(engine, kv.WithProposalObserver(proposalObserver))
	coordinator := kv.NewCoordinatorWithEngine(trx, engine, kv.WithHLC(hlc))
	defer func() {
		// Release the leader-loss callback slot on the engine before
		// the process exits. The engine itself is closed elsewhere in
		// the shutdown path; both orderings are safe, but releasing
		// the closure here matches the symmetric construction order.
		_ = coordinator.Close()
	}()
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
	metricsRegistry.RaftObserver().Start(ctx, []monitoring.RaftRuntime{{
		GroupID:      1,
		StatusReader: engine,
		ConfigReader: engine,
	}}, raftObserveInterval)
	compactor := kv.NewFSMCompactor(
		[]kv.FSMCompactRuntime{{
			GroupID:      1,
			StatusReader: engine,
			Store:        st,
		}},
		kv.WithFSMCompactorActiveTimestampTracker(readTracker),
	)
	relay := adapter.NewRedisPubSubRelay()

	s, grpcSvc := setupGRPC(ctx, engine, result.RegisterTransport, st, coordinator, distServer, relay, proposalObserver)

	grpcSock, err := lc.Listen(ctx, "tcp", cfg.address)
	if err != nil {
		return errors.WithStack(err)
	}
	cleanup.Add(func() {
		_ = grpcSock.Close()
	})

	deltaCompactor := adapter.NewDeltaCompactor(st, coordinator)

	rd, err := setupRedis(ctx, lc, st, coordinator, cfg.address, cfg.redisAddress, cfg.raftRedisMap, relay, readTracker, deltaCompactor)
	if err != nil {
		return err
	}
	cleanup.Add(rd.Stop)
	s3s, err := setupS3(ctx, lc, st, coordinator, cfg.address, cfg.s3Address, cfg.raftS3Map, cfg.s3Region, cfg.s3CredsFile, cfg.s3PathStyle, readTracker)
	if err != nil {
		return err
	}
	cleanup.Add(s3s.Stop)
	ds, err := setupDynamo(ctx, lc, st, coordinator, cfg.address, cfg.dynamoAddress, cfg.raftDynamoMap, metricsRegistry.DynamoDBObserver())
	if err != nil {
		return err
	}
	cleanup.Add(ds.Stop)
	metricsL, ms, pprofL, ps, err := setupObservabilityServers(ctx, lc, &cleanup, cfg, metricsRegistry.Handler())
	if err != nil {
		return err
	}

	eg.Go(func() error { coordinator.RunHLCLeaseRenewal(ctx); return nil })
	eg.Go(catalogWatcherTask(ctx, distCatalog, distEngine))
	eg.Go(func() error { return compactor.Run(ctx) })
	eg.Go(func() error { return deltaCompactor.Run(ctx) })
	eg.Go(grpcShutdownTask(ctx, s, grpcSock, cfg.address, grpcSvc))
	eg.Go(grpcServeTask(s, grpcSock, cfg.address))
	eg.Go(redisShutdownTask(ctx, rd, cfg.redisAddress))
	eg.Go(redisServeTask(rd, cfg.redisAddress))
	eg.Go(s3ShutdownTask(ctx, s3s, cfg.s3Address))
	eg.Go(s3ServeTask(s3s, cfg.s3Address))
	eg.Go(dynamoShutdownTask(ctx, ds, cfg.dynamoAddress))
	eg.Go(dynamoServeTask(ds, cfg.dynamoAddress))
	eg.Go(monitoring.MetricsShutdownTask(ctx, ms, cfg.metricsAddress))
	eg.Go(monitoring.MetricsServeTask(ms, metricsL, cfg.metricsAddress))
	eg.Go(monitoring.PprofShutdownTask(ctx, ps, cfg.pprofAddress))
	eg.Go(monitoring.PprofServeTask(ps, pprofL, cfg.pprofAddress))

	cleanup.Release()
	return nil
}

func setupObservabilityServers(ctx context.Context, lc net.ListenConfig, cleanup *internalutil.CleanupStack, cfg config, metricsHandler http.Handler) (metricsL net.Listener, ms *http.Server, pprofL net.Listener, ps *http.Server, err error) {
	metricsL, ms, err = setupMetricsHTTPServer(ctx, lc, cfg.metricsAddress, cfg.metricsToken, metricsHandler)
	if err != nil {
		return
	}
	if metricsL != nil {
		cleanup.Add(func() { _ = metricsL.Close() })
	}
	pprofL, ps, err = setupPprofHTTPServer(ctx, lc, cfg.pprofAddress, cfg.pprofToken)
	if err != nil {
		return
	}
	if pprofL != nil {
		cleanup.Add(func() { _ = pprofL.Close() })
	}
	return
}

func setupMetricsHTTPServer(ctx context.Context, lc net.ListenConfig, metricsAddress string, metricsToken string, handler http.Handler) (net.Listener, *http.Server, error) {
	metricsAddress = strings.TrimSpace(metricsAddress)
	if metricsAddress == "" || handler == nil {
		return nil, nil, nil
	}
	if _, _, err := net.SplitHostPort(metricsAddress); err != nil {
		return nil, nil, errors.Wrapf(err, "invalid metricsAddress %q", metricsAddress)
	}
	if monitoring.AddressRequiresToken(metricsAddress) && strings.TrimSpace(metricsToken) == "" {
		return nil, nil, errors.New("metricsToken is required when metricsAddress is not loopback")
	}
	metricsL, err := lc.Listen(ctx, "tcp", metricsAddress)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	ms := monitoring.NewMetricsServer(handler, metricsToken)
	return metricsL, ms, nil
}

func setupPprofHTTPServer(ctx context.Context, lc net.ListenConfig, pprofAddress string, pprofToken string) (net.Listener, *http.Server, error) {
	pprofAddress = strings.TrimSpace(pprofAddress)
	if pprofAddress == "" {
		return nil, nil, nil
	}
	if _, _, err := net.SplitHostPort(pprofAddress); err != nil {
		return nil, nil, errors.Wrapf(err, "invalid pprofAddress %q", pprofAddress)
	}
	if monitoring.AddressRequiresToken(pprofAddress) && strings.TrimSpace(pprofToken) == "" {
		return nil, nil, errors.New("pprofToken is required when pprofAddress is not loopback")
	}
	pprofL, err := lc.Listen(ctx, "tcp", pprofAddress)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	ps := monitoring.NewPprofServer(pprofToken)
	return pprofL, ps, nil
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

func s3ShutdownTask(ctx context.Context, s3Server *adapter.S3Server, address string) func() error {
	return func() error {
		<-ctx.Done()
		slog.Info("Shutting down S3 server", "address", address, "reason", ctx.Err())
		s3Server.Stop()
		return nil
	}
}

func s3ServeTask(s3Server *adapter.S3Server, address string) func() error {
	return func() error {
		slog.Info("Starting S3 server", "address", address)
		err := s3Server.Run()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}

type s3CredentialFile struct {
	Credentials []s3CredentialEntry `json:"credentials"`
}

type s3CredentialEntry struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func loadS3StaticCredentials(path string) (map[string]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	file := s3CredentialFile{}
	if err := json.Unmarshal(body, &file); err != nil {
		return nil, errors.WithStack(err)
	}
	out := make(map[string]string, len(file.Credentials))
	for _, cred := range file.Credentials {
		accessKeyID := strings.TrimSpace(cred.AccessKeyID)
		secretAccessKey := strings.TrimSpace(cred.SecretAccessKey)
		if accessKeyID == "" || secretAccessKey == "" {
			return nil, errors.New("s3 credentials file contains an empty access key or secret key")
		}
		out[accessKeyID] = secretAccessKey
	}
	return out, nil
}
