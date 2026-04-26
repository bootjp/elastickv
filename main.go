package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/memwatch"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	heartbeatTimeout           = 200 * time.Millisecond
	electionTimeout            = 2000 * time.Millisecond
	raftMetricsObserveInterval = 5 * time.Second
	dirPerm                    = raftDirPerm

	etcdTickInterval      = 10 * time.Millisecond
	etcdHeartbeatMinTicks = 1
	etcdElectionMinTicks  = 2
	etcdMaxSizePerMsg     = 1 << 20
	etcdMaxInflightMsg    = 256
)

func newRaftFactory(engineType raftEngineType) (raftengine.Factory, error) {
	switch engineType {
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
	sqsAddr              = flag.String("sqsAddress", "", "TCP host+port for SQS-compatible API; empty to disable")
	sqsRegion            = flag.String("sqsRegion", "us-east-1", "SQS signing region")
	sqsCredsFile         = flag.String("sqsCredentialsFile", "", "Path to a JSON file containing static SQS credentials")
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
	raftSqsMap           = flag.String("raftSqsMap", "", "Map of Raft address to SQS address (raftAddr=sqsAddr,...)")
	// Admin gRPC service flags (this PR — wired into the per-group raft
	// listeners; consumed by cmd/elastickv-admin via the bearer-token
	// gateway). These are independent of the admin HTTP listener flags
	// below — both can be enabled simultaneously, and operators can pick
	// whichever auth path they need (gRPC bearer token vs. HTTP cookies +
	// SigV4 access keys).
	adminTokenFile      = flag.String("adminTokenFile", "", "Path to a file containing the read-only bearer token required on the Admin gRPC service (leave blank with --adminInsecureNoAuth off to disable the Admin service)")
	adminInsecureNoAuth = flag.Bool("adminInsecureNoAuth", false, "Register the Admin gRPC service without bearer-token authentication; development only")

	// Admin HTTP listener flags (PR #545's parallel work merged into
	// main; serves the cookie/SigV4-authenticated admin dashboard).
	adminEnabled                       = flag.Bool("adminEnabled", false, "Enable the admin HTTP listener")
	adminListen                        = flag.String("adminListen", "127.0.0.1:8080", "host:port for the admin HTTP listener (loopback by default)")
	adminTLSCertFile                   = flag.String("adminTLSCertFile", "", "PEM-encoded TLS certificate for the admin listener")
	adminTLSKeyFile                    = flag.String("adminTLSKeyFile", "", "PEM-encoded TLS private key for the admin listener")
	adminAllowPlaintextNonLoopback     = flag.Bool("adminAllowPlaintextNonLoopback", false, "Allow the admin listener to bind a non-loopback address without TLS (strongly discouraged)")
	adminAllowInsecureDevCookie        = flag.Bool("adminAllowInsecureDevCookie", false, "Mint admin cookies without the Secure attribute (local plaintext dev only)")
	adminSessionSigningKey             = flag.String("adminSessionSigningKey", "", "Cluster-shared base64 HS256 key (64 bytes decoded); prefer -adminSessionSigningKeyFile / ELASTICKV_ADMIN_SESSION_SIGNING_KEY so the value does not appear in /proc/<pid>/cmdline")
	adminSessionSigningKeyFile         = flag.String("adminSessionSigningKeyFile", "", "Path to a file containing the base64-encoded primary admin HS256 key; avoids leaking the secret via argv")
	adminSessionSigningKeyPrevious     = flag.String("adminSessionSigningKeyPrevious", "", "Optional previous admin HS256 key accepted only for verification during rotation; prefer -adminSessionSigningKeyPreviousFile")
	adminSessionSigningKeyPreviousFile = flag.String("adminSessionSigningKeyPreviousFile", "", "Path to a file containing the base64-encoded previous admin HS256 key used for rotation")
	adminReadOnlyAccessKeys            = flag.String("adminReadOnlyAccessKeys", "", "Comma-separated SigV4 access keys granted read-only admin access")
	adminFullAccessKeys                = flag.String("adminFullAccessKeys", "", "Comma-separated SigV4 access keys granted full-access admin role")

	// Key visualizer sampler flags. The sampler runs entirely in-memory
	// on each node, feeds AdminServer.GetKeyVizMatrix, and is disabled
	// by default — opt in with --keyvizEnabled. The other flags are
	// no-ops when the sampler is disabled.
	keyvizEnabled                = flag.Bool("keyvizEnabled", false, "Enable the in-memory key visualizer sampler that feeds AdminServer.GetKeyVizMatrix")
	keyvizStep                   = flag.Duration("keyvizStep", keyviz.DefaultStep, "Flush interval / matrix-column resolution for the keyviz sampler")
	keyvizMaxTrackedRoutes       = flag.Int("keyvizMaxTrackedRoutes", keyviz.DefaultMaxTrackedRoutes, "Maximum routes tracked individually before excess routes coarsen into virtual buckets")
	keyvizMaxMemberRoutesPerSlot = flag.Int("keyvizMaxMemberRoutesPerSlot", keyviz.DefaultMaxMemberRoutesPerSlot, "Maximum members listed on a virtual bucket; excess routes still drive the bucket counters")
	keyvizHistoryColumns         = flag.Int("keyvizHistoryColumns", keyviz.DefaultHistoryColumns, "Maximum matrix columns retained in the keyviz ring buffer (each column = one Step)")
)

const adminTokenMaxBytes = 4 << 10

// memoryPressureExit is set to true by the memwatch OnExceed callback to
// signal that the subsequent graceful shutdown was triggered by user-space
// OOM avoidance rather than an ordinary SIGTERM. The process exits with a
// distinct non-zero code (exitCodeMemoryPressure) so operators reading
// logs can distinguish this case from a crash or an ordinary stop.
var memoryPressureExit atomic.Bool

// exitCodeMemoryPressure is reported by main when memwatch triggered the
// shutdown. It is non-zero so supervisors see a non-success exit, but
// distinct from log.Fatalf's 1 and from os.Exit(1) in the other binaries
// so log scraping can tell them apart.
const exitCodeMemoryPressure = 2

// memoryShutdownThresholdEnvVar configures the heap-inuse ceiling at
// which memwatch triggers a graceful shutdown. Empty or "0" disables the
// watchdog (the default; existing operators see no behaviour change).
const memoryShutdownThresholdEnvVar = "ELASTICKV_MEMORY_SHUTDOWN_THRESHOLD_MB"

// memoryShutdownPollIntervalEnvVar overrides memwatch's default poll
// cadence. Accepts any time.ParseDuration string. Invalid values log a
// warning and fall through to the default.
const memoryShutdownPollIntervalEnvVar = "ELASTICKV_MEMORY_SHUTDOWN_POLL_INTERVAL"

const bytesPerMiB = 1024 * 1024

func main() {
	flag.Parse()

	err := run()
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
	if err != nil {
		log.Fatalf("%v", err)
	}
}

// memwatchConfigFromEnv resolves the memwatch Config from environment
// variables. It returns (cfg, true) when the watcher should run, or
// (_, false) when the operator has not opted in (the default). Errors in
// the optional poll-interval override are logged and ignored so a typo
// cannot take the process down.
func memwatchConfigFromEnv() (memwatch.Config, bool) {
	raw := strings.TrimSpace(os.Getenv(memoryShutdownThresholdEnvVar))
	if raw == "" {
		return memwatch.Config{}, false
	}
	mb, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		slog.Warn("invalid "+memoryShutdownThresholdEnvVar+"; watcher disabled",
			"value", raw, "error", err)
		return memwatch.Config{}, false
	}
	if mb == 0 {
		return memwatch.Config{}, false
	}
	// Guard against mb * bytesPerMiB wrapping past math.MaxUint64. The
	// value has no real use above this ceiling (the host does not have
	// exabytes of RAM), and a wrapped value would set an absurdly low
	// threshold that fires immediately.
	if mb > math.MaxUint64/bytesPerMiB {
		slog.Warn("value for "+memoryShutdownThresholdEnvVar+" would overflow uint64; watcher disabled",
			"value_mb", mb)
		return memwatch.Config{}, false
	}

	cfg := memwatch.Config{
		ThresholdBytes: mb * bytesPerMiB,
	}
	cfg.PollInterval = memwatch.DefaultPollInterval
	if rawInterval := strings.TrimSpace(os.Getenv(memoryShutdownPollIntervalEnvVar)); rawInterval != "" {
		d, err := time.ParseDuration(rawInterval)
		if err != nil || d <= 0 {
			slog.Warn("invalid "+memoryShutdownPollIntervalEnvVar+"; using default",
				"value", rawInterval, "error", err)
		} else {
			cfg.PollInterval = d
		}
	}
	return cfg, true
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

	// Record the active FSM apply sync mode so operators can see on the
	// /metrics endpoint which durability posture this node is running in.
	// The label is resolved per-pebbleStore from ELASTICKV_FSM_SYNC_MODE
	// in NewPebbleStore; read it off the first constructed store (all
	// shards share the same env and therefore the same label).
	if label := fsmApplySyncModeLabelFromRuntimes(runtimes); label != "" {
		metricsRegistry.SetFSMApplySyncMode(label)
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
	sampler := buildKeyVizSampler()
	coordinate := kv.NewShardedCoordinator(cfg.engine, shardGroups, cfg.defaultGroup, clock, shardStore).
		WithLeaseReadObserver(metricsRegistry.LeaseReadObserver()).
		WithSampler(keyVizSamplerForCoordinator(sampler))
	distCatalog, err := setupDistributionCatalog(ctx, runtimes, cfg.engine)
	if err != nil {
		return err
	}
	// Seed AFTER setupDistributionCatalog so the sampler picks up the
	// catalog-assigned RouteIDs. EnsureCatalogSnapshot inside
	// setupDistributionCatalog applies a snapshot back into the engine
	// with durable non-zero RouteIDs; seeding earlier would register
	// the placeholder zero IDs from buildEngine and Observe would miss
	// every dispatched mutation.
	seedKeyVizRoutes(sampler, cfg.engine)
	eg, runCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return runDistributionCatalogWatcher(runCtx, distCatalog, cfg.engine)
	})
	startKeyVizFlusher(runCtx, eg, sampler)
	startMemoryWatchdog(runCtx, eg, cancel)
	distServer := adapter.NewDistributionServer(
		cfg.engine,
		distCatalog,
		adapter.WithDistributionCoordinator(coordinate),
		adapter.WithDistributionActiveTimestampTracker(readTracker),
	)
	startMonitoringCollectors(runCtx, metricsRegistry, runtimes)
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

	if err := startServers(serversInput{
		ctx: runCtx, eg: eg, cancel: cancel, lc: &lc,
		runtimes: runtimes, bootstrapServers: bootstrapServers,
		shardStore: shardStore, coordinate: coordinate,
		distServer: distServer, readTracker: readTracker,
		metricsRegistry: metricsRegistry, cfg: cfg,
		keyvizSampler: sampler,
	}); err != nil {
		return err
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "failed to serve")
	}
	return nil
}

func resolveRuntimeInputs() (runtimeConfig, raftEngineType, []raftengine.Server, bool, error) {
	if *raftId == "" {
		return runtimeConfig{}, "", nil, false, errors.New("flag --raftId is required")
	}

	engineType, err := parseRaftEngineType(*raftEngineName)
	if err != nil {
		return runtimeConfig{}, "", nil, false, err
	}

	cfg, err := parseRuntimeConfig(*myAddr, *redisAddr, *s3Addr, *dynamoAddr, *sqsAddr, *raftGroups, *shardRanges, *raftRedisMap, *raftS3Map, *raftDynamoMap, *raftSqsMap)
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
	leaderRedis  map[string]string
	leaderS3     map[string]string
	leaderDynamo map[string]string
	leaderSQS    map[string]string
	multi        bool
}

func parseRuntimeConfig(myAddr, redisAddr, s3Addr, dynamoAddr, sqsAddr, raftGroups, shardRanges, raftRedisMap, raftS3Map, raftDynamoMap, raftSqsMap string) (runtimeConfig, error) {
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
	leaderSQS, err := buildLeaderSQS(groups, sqsAddr, raftSqsMap)
	if err != nil {
		return runtimeConfig{}, errors.Wrapf(err, "failed to parse raft sqs map")
	}

	return runtimeConfig{
		groups:       groups,
		defaultGroup: defaultGroup,
		engine:       engine,
		leaderRedis:  leaderRedis,
		leaderS3:     leaderS3,
		leaderDynamo: leaderDynamo,
		leaderSQS:    leaderSQS,
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

func buildLeaderRedis(groups []groupSpec, redisAddr string, raftRedisMap string) (map[string]string, error) {
	return buildLeaderAddrMap(groups, redisAddr, raftRedisMap, parseRaftRedisMap)
}

func buildLeaderS3(groups []groupSpec, s3Addr string, raftS3Map string) (map[string]string, error) {
	return buildLeaderAddrMap(groups, s3Addr, raftS3Map, parseRaftS3Map)
}

func buildLeaderSQS(groups []groupSpec, sqsAddr string, raftSqsMap string) (map[string]string, error) {
	return buildLeaderAddrMap(groups, sqsAddr, raftSqsMap, parseRaftSQSMap)
}

func buildLeaderDynamo(groups []groupSpec, dynamoAddr string, raftDynamoMap string) (map[string]string, error) {
	return buildLeaderAddrMap(groups, dynamoAddr, raftDynamoMap, parseRaftDynamoMap)
}

func buildLeaderAddrMap(
	groups []groupSpec,
	defaultAddr string,
	rawMap string,
	parse func(string) (map[string]string, error),
) (map[string]string, error) {
	leaderAddrMap, err := parse(rawMap)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		if _, ok := leaderAddrMap[g.address]; !ok {
			leaderAddrMap[g.address] = defaultAddr
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

func resolveBootstrapServers(raftID string, groups []groupSpec, bootstrapMembers string) ([]raftengine.Server, error) {
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
		if s.ID != raftID {
			continue
		}
		if s.Address != localAddr {
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
	bootstrapServers []raftengine.Server,
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
		sm := kv.NewKvFSMWithHLC(st, clock)
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

// fsmApplySyncModeLabeler narrows an MVCCStore to those implementations
// that can report the resolved ELASTICKV_FSM_SYNC_MODE label. The
// pebble-backed store satisfies this today; alternate backends (none
// yet) would either implement it or be skipped.
type fsmApplySyncModeLabeler interface {
	FSMApplySyncModeLabel() string
}

// fsmApplySyncModeLabelFromRuntimes returns the FSM apply sync-mode
// label resolved by the first shard store that exposes it. All shards
// on a node read the same ELASTICKV_FSM_SYNC_MODE env var at
// construction time so the label is uniform across the runtimes;
// returning the first one suffices. Returns "" when no runtime
// exposes the accessor, in which case the caller skips emitting the
// gauge to avoid publishing a misleading default.
func fsmApplySyncModeLabelFromRuntimes(runtimes []*raftGroupRuntime) string {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.store == nil {
			continue
		}
		src, ok := runtime.store.(fsmApplySyncModeLabeler)
		if !ok {
			continue
		}
		return src.FSMApplySyncModeLabel()
	}
	return ""
}

// pebbleMonitorSources extracts the MVCC stores that expose
// *pebble.DB.Metrics() so monitoring can poll LSM internals (L0
// sublevels, compaction debt, memtable, block cache) for the
// elastickv_pebble_* metrics family. Stores that do not satisfy the
// interface (non-Pebble backends, if any are added later) are skipped
// silently.
func pebbleMonitorSources(runtimes []*raftGroupRuntime) []monitoring.PebbleSource {
	out := make([]monitoring.PebbleSource, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.store == nil {
			continue
		}
		src, ok := runtime.store.(monitoring.PebbleMetricsSource)
		if !ok {
			continue
		}
		out = append(out, monitoring.PebbleSource{
			GroupID:    runtime.spec.id,
			GroupIDStr: strconv.FormatUint(runtime.spec.id, 10),
			Source:     src,
		})
	}
	return out
}

// dispatchMonitorSources extracts the raft engines that expose etcd
// dispatch counters so monitoring can poll them for the hot-path
// dashboard. Engines that do not satisfy the interface (hashicorp
// backend today) are skipped silently; their groups simply won't
// contribute to elastickv_raft_dispatch_* metrics.
func dispatchMonitorSources(runtimes []*raftGroupRuntime) []monitoring.DispatchSource {
	out := make([]monitoring.DispatchSource, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.engine == nil {
			continue
		}
		src, ok := runtime.engine.(monitoring.DispatchCounterSource)
		if !ok {
			continue
		}
		out = append(out, monitoring.DispatchSource{
			GroupID: runtime.spec.id,
			Source:  src,
		})
	}
	return out
}

// setupAdminService is a thin wrapper around configureAdminService that also
// binds each Raft runtime to the server and logs an operator warning when
// running without authentication. Keeping this out of run() preserves run's
// cyclomatic-complexity budget. Members are seeded from the bootstrap
// configuration so GetClusterOverview advertises peer node addresses to the
// admin binary's fan-out discovery path.
// serversInput bundles the values run() passes to startServers so the
// signature stays compact and run() stays under the cyclop budget.
type serversInput struct {
	ctx              context.Context
	eg               *errgroup.Group
	cancel           context.CancelFunc
	lc               *net.ListenConfig
	runtimes         []*raftGroupRuntime
	bootstrapServers []raftengine.Server
	shardStore       *kv.ShardStore
	coordinate       kv.Coordinator
	distServer       *adapter.DistributionServer
	readTracker      *kv.ActiveTimestampTracker
	metricsRegistry  *monitoring.Registry
	cfg              runtimeConfig
	// keyvizSampler is the in-memory key visualizer sampler, or nil
	// when --keyvizEnabled is false. Threaded into setupAdminService
	// so AdminServer.GetKeyVizMatrix can serve snapshots; the
	// coordinator already has its own copy from
	// `WithSampler(...)` higher up in run().
	keyvizSampler *keyviz.MemSampler
}

// startServers wires up the AdminServer, builds the runtime runner, and
// kicks off both the per-group raft listeners and the admin HTTP listener.
// Extracted from run() to keep cyclomatic complexity within budget.
func startServers(in serversInput) error {
	adminServer, adminGRPCOpts, err := setupAdminService(*raftId, *myAddr, in.runtimes, in.bootstrapServers, in.keyvizSampler)
	if err != nil {
		return err
	}
	runner := runtimeServerRunner{
		ctx:             in.ctx,
		lc:              in.lc,
		eg:              in.eg,
		cancel:          in.cancel,
		runtimes:        in.runtimes,
		shardStore:      in.shardStore,
		coordinate:      in.coordinate,
		distServer:      in.distServer,
		adminServer:     adminServer,
		adminGRPCOpts:   adminGRPCOpts,
		redisAddress:    *redisAddr,
		leaderRedis:     in.cfg.leaderRedis,
		pubsubRelay:     adapter.NewRedisPubSubRelay(),
		readTracker:     in.readTracker,
		dynamoAddress:   *dynamoAddr,
		leaderDynamo:    in.cfg.leaderDynamo,
		s3Address:       *s3Addr,
		leaderS3:        in.cfg.leaderS3,
		s3Region:        *s3Region,
		s3CredsFile:     *s3CredsFile,
		s3PathStyleOnly: *s3PathStyleOnly,
		sqsAddress:      *sqsAddr,
		leaderSQS:       in.cfg.leaderSQS,
		sqsRegion:       *sqsRegion,
		sqsCredsFile:    *sqsCredsFile,
		metricsAddress:  *metricsAddr,
		metricsToken:    *metricsToken,
		pprofAddress:    *pprofAddr,
		pprofToken:      *pprofToken,
		metricsRegistry: in.metricsRegistry,
	}
	if err := runner.start(); err != nil {
		return err
	}
	// runner.start() populates runner.dynamoServer for the admin
	// listener's SigV4-bypass entrypoints (see adapter/dynamodb_admin.go).
	// Passing nil here would leave the admin dashboard with no
	// access to table metadata; the admin handler answers
	// /admin/api/v1/dynamo/* with 404 in that case.
	if err := startAdminFromFlags(in.ctx, in.lc, in.eg, in.runtimes, runner.dynamoServer); err != nil {
		return waitErrgroupAfterStartupFailure(in.cancel, in.eg, err)
	}
	return nil
}

func setupAdminService(
	nodeID, grpcAddress string,
	runtimes []*raftGroupRuntime,
	bootstrapServers []raftengine.Server,
	keyvizSampler *keyviz.MemSampler,
) (*adapter.AdminServer, adminGRPCInterceptors, error) {
	members := adminMembersFromBootstrap(nodeID, bootstrapServers)
	// In multi-group mode the process does not listen on *myAddr — each group
	// has its own rt.spec.address. Use the lowest-group-ID listener as the
	// canonical self address so GetClusterOverview.Self advertises an
	// endpoint the fan-out can actually dial. Falls back to the flag value
	// when no runtimes are registered (single-node dev runs).
	selfAddr := canonicalSelfAddress(grpcAddress, runtimes)
	srv, icept, err := configureAdminService(
		*adminTokenFile,
		*adminInsecureNoAuth,
		adapter.NodeIdentity{NodeID: nodeID, GRPCAddress: selfAddr},
		members,
	)
	if err != nil {
		return nil, adminGRPCInterceptors{}, err
	}
	if srv == nil {
		return nil, adminGRPCInterceptors{}, nil
	}
	for _, rt := range runtimes {
		srv.RegisterGroup(rt.spec.id, rt.engine)
	}
	// Only register a real sampler. Passing a typed-nil *MemSampler
	// would store a non-nil interface and make GetKeyVizMatrix
	// return a successful empty response instead of Unavailable —
	// operators want the explicit "keyviz disabled" signal.
	if keyvizSampler != nil {
		srv.RegisterSampler(keyvizSampler)
	}
	if *adminInsecureNoAuth {
		log.Printf("WARNING: --adminInsecureNoAuth is set; Admin gRPC service exposed without authentication")
	}
	return srv, icept, nil
}

// canonicalSelfAddress picks the listener address AdminServer should advertise
// as Self.GRPCAddress. The Admin gRPC service is registered on every Raft
// group's listener in startRaftServers, so any runtime's address is reachable;
// we pick the lowest group ID to make the choice deterministic across
// restarts. Returns the supplied fallback when no runtimes exist (e.g., a
// single-node dev invocation without --raftGroups).
func canonicalSelfAddress(fallback string, runtimes []*raftGroupRuntime) string {
	var (
		bestID   uint64
		bestAddr string
		found    bool
	)
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		if !found || rt.spec.id < bestID {
			bestID, bestAddr, found = rt.spec.id, rt.spec.address, true
		}
	}
	if !found {
		return fallback
	}
	return bestAddr
}

// adminMembersFromBootstrap extracts the peer list (everyone except self) from
// the Raft bootstrap configuration so GetClusterOverview returns a populated
// members list. Without this the admin binary's membersFrom cache collapses to
// only the responding seed and stops fanning out across the cluster.
func adminMembersFromBootstrap(selfID string, servers []raftengine.Server) []adapter.NodeIdentity {
	if len(servers) == 0 {
		return nil
	}
	out := make([]adapter.NodeIdentity, 0, len(servers))
	for _, s := range servers {
		if s.ID == selfID {
			continue
		}
		out = append(out, adapter.NodeIdentity{
			NodeID:      s.ID,
			GRPCAddress: s.Address,
		})
	}
	return out
}

// adminGRPCInterceptors bundles the unary+stream interceptors that enforce the
// Admin bearer token. Returning the raw interceptor functions (rather than
// pre-wrapped grpc.ServerOption values via grpc.ChainUnaryInterceptor) lets
// the registration site combine them with any other interceptors in a single
// ChainUnaryInterceptor call, so using grpc.UnaryInterceptor alongside risks
// silent overwrites (gRPC-Go: last option of the same type wins).
type adminGRPCInterceptors struct {
	unary  []grpc.UnaryServerInterceptor
	stream []grpc.StreamServerInterceptor
}

func (a adminGRPCInterceptors) empty() bool {
	return len(a.unary) == 0 && len(a.stream) == 0
}

// configureAdminService builds the node-side AdminServer plus the interceptor
// set that enforces its bearer token, or returns (nil, {}, nil) when the
// service is intentionally disabled. It is mutually exclusive with
// --adminInsecureNoAuth so operators have to opt into the unauthenticated
// mode explicitly.
func configureAdminService(
	tokenPath string,
	insecureNoAuth bool,
	self adapter.NodeIdentity,
	members []adapter.NodeIdentity,
) (*adapter.AdminServer, adminGRPCInterceptors, error) {
	if tokenPath == "" && !insecureNoAuth {
		return nil, adminGRPCInterceptors{}, nil
	}
	if tokenPath != "" && insecureNoAuth {
		return nil, adminGRPCInterceptors{}, errors.New("--adminInsecureNoAuth and --adminTokenFile are mutually exclusive")
	}
	token := ""
	if tokenPath != "" {
		loaded, err := loadAdminTokenFile(tokenPath)
		if err != nil {
			return nil, adminGRPCInterceptors{}, err
		}
		token = loaded
	}
	srv := adapter.NewAdminServer(self, members)
	unary, stream := adapter.AdminTokenAuth(token)
	var icept adminGRPCInterceptors
	if unary != nil {
		icept.unary = append(icept.unary, unary)
	}
	if stream != nil {
		icept.stream = append(icept.stream, stream)
	}
	return srv, icept, nil
}

// loadAdminTokenFile materialises --adminTokenFile with a strict upper bound
// so a misconfigured path (for example a log file) cannot force an arbitrary
// allocation before the bearer-token check. Delegates to the shared helper in
// internal/ so the admin binary and the node process read tokens identically.
func loadAdminTokenFile(path string) (string, error) {
	tok, err := internalutil.LoadBearerTokenFile(path, adminTokenMaxBytes, "admin token")
	if err != nil {
		return "", errors.Wrap(err, "load admin token")
	}
	return tok, nil
}

// startMemoryWatchdog optionally starts the memwatch goroutine. The
// watcher is off by default; it is enabled only when the operator sets
// ELASTICKV_MEMORY_SHUTDOWN_THRESHOLD_MB. On threshold crossing the
// callback flips the memoryPressureExit sentinel and cancels the root
// context, routing through the exact same shutdown path SIGTERM would
// use (errgroup unwinds, CleanupStack runs, WAL is synced). We do NOT
// send a signal, call os.Exit, or touch the raft engine directly here.
func startMemoryWatchdog(ctx context.Context, eg *errgroup.Group, cancel context.CancelFunc) {
	cfg, enabled := memwatchConfigFromEnv()
	if !enabled {
		return
	}
	cfg.OnExceed = func() {
		memoryPressureExit.Store(true)
		cancel()
	}
	w := memwatch.New(cfg)
	slog.Info("memory watchdog enabled",
		"threshold_bytes", cfg.ThresholdBytes,
		"poll_interval", cfg.PollInterval,
	)
	eg.Go(func() error {
		w.Start(ctx)
		return nil
	})
}

// startMonitoringCollectors wires up the per-tick Prometheus
// collectors (raft dispatch, Pebble LSM, store-layer OCC conflicts)
// on top of the running raft runtimes. Kept separate from run() so
// the latter stays under the cyclop complexity budget and so new
// collectors can be added without widening run() further.
func startMonitoringCollectors(ctx context.Context, reg *monitoring.Registry, runtimes []*raftGroupRuntime) {
	reg.RaftObserver().Start(ctx, raftMonitorRuntimes(runtimes), raftMetricsObserveInterval)
	if collector := reg.DispatchCollector(); collector != nil {
		collector.Start(ctx, dispatchMonitorSources(runtimes), raftMetricsObserveInterval)
	}
	if collector := reg.PebbleCollector(); collector != nil {
		collector.Start(ctx, pebbleMonitorSources(runtimes), raftMetricsObserveInterval)
	}
	if collector := reg.WriteConflictCollector(); collector != nil {
		collector.Start(ctx, writeConflictMonitorSources(runtimes), raftMetricsObserveInterval)
	}
}

// writeConflictMonitorSources extracts the MVCC stores that expose
// per-(kind, key_prefix) OCC conflict counters so monitoring can poll
// them for the elastickv_store_write_conflict_total metric. Every
// store.MVCCStore implements WriteConflictCountsByPrefix(); stores
// that do not track conflicts return an empty map and simply do not
// contribute series.
func writeConflictMonitorSources(runtimes []*raftGroupRuntime) []monitoring.WriteConflictSource {
	out := make([]monitoring.WriteConflictSource, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.store == nil {
			continue
		}
		src, ok := runtime.store.(monitoring.WriteConflictCounterSource)
		if !ok {
			continue
		}
		out = append(out, monitoring.WriteConflictSource{
			GroupID:    runtime.spec.id,
			GroupIDStr: strconv.FormatUint(runtime.spec.id, 10),
			Source:     src,
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
	adminServer *adapter.AdminServer,
	adminGRPCOpts adminGRPCInterceptors,
) error {
	// extraOptsCap reserves slots for the unary + stream admin interceptor
	// options appended below. Sized as a constant so the magic-number
	// linter does not complain.
	const extraOptsCap = 2
	for _, rt := range runtimes {
		baseOpts := internalutil.GRPCServerOptions()
		opts := make([]grpc.ServerOption, 0, len(baseOpts)+extraOptsCap)
		opts = append(opts, baseOpts...)
		// Collapse all interceptors into a single ChainUnaryInterceptor /
		// ChainStreamInterceptor call so a future grpc.UnaryInterceptor
		// (single-interceptor) option added anywhere in this chain cannot
		// silently overwrite the admin auth gate — gRPC-Go keeps only the
		// last option of the same type.
		if len(adminGRPCOpts.unary) > 0 {
			opts = append(opts, grpc.ChainUnaryInterceptor(adminGRPCOpts.unary...))
		}
		if len(adminGRPCOpts.stream) > 0 {
			opts = append(opts, grpc.ChainStreamInterceptor(adminGRPCOpts.stream...))
		}
		gs := grpc.NewServer(opts...)
		trx := kv.NewTransactionWithProposer(rt.engine, kv.WithProposalObserver(observerForGroup(proposalObserverForGroup, rt.spec.id)))
		grpcSvc := adapter.NewGRPCServer(shardStore, coordinate)
		pb.RegisterRawKVServer(gs, grpcSvc)
		pb.RegisterTransactionalKVServer(gs, grpcSvc)
		pb.RegisterInternalServer(gs, adapter.NewInternalWithEngine(trx, rt.engine, coordinate.Clock(), relay))
		pb.RegisterDistributionServer(gs, distServer)
		if adminServer != nil {
			pb.RegisterAdminServer(gs, adminServer)
		}
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

func startRedisServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, redisAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderRedis map[string]string, relay *adapter.RedisPubSubRelay, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker) error {
	redisL, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", redisAddr)
	}
	deltaCompactor := adapter.NewDeltaCompactor(shardStore, coordinate)
	eg.Go(func() error { return deltaCompactor.Run(ctx) })
	redisServer := adapter.NewRedisServer(redisL, redisAddr, shardStore, coordinate, leaderRedis, relay,
		adapter.WithRedisActiveTimestampTracker(readTracker),
		adapter.WithRedisRequestObserver(metricsRegistry.RedisObserver()),
		adapter.WithLuaObserver(metricsRegistry.LuaObserver()),
		adapter.WithLuaFastPathObserver(metricsRegistry.LuaFastPathObserver()),
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

func startDynamoDBServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, dynamoAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderDynamo map[string]string, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker) (*adapter.DynamoDBServer, error) {
	dynamoL, err := lc.Listen(ctx, "tcp", dynamoAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", dynamoAddr)
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
	return dynamoServer, nil
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
	adminServer     *adapter.AdminServer
	adminGRPCOpts   adminGRPCInterceptors
	redisAddress    string
	leaderRedis     map[string]string
	pubsubRelay     *adapter.RedisPubSubRelay
	readTracker     *kv.ActiveTimestampTracker
	dynamoAddress   string
	leaderDynamo    map[string]string
	s3Address       string
	leaderS3        map[string]string
	s3Region        string
	s3CredsFile     string
	s3PathStyleOnly bool
	sqsAddress      string
	leaderSQS       map[string]string
	sqsRegion       string
	sqsCredsFile    string
	metricsAddress  string
	metricsToken    string
	pprofAddress    string
	pprofToken      string
	metricsRegistry *monitoring.Registry

	// dynamoServer is populated by start() and made available to
	// startAdminFromFlags in this package so the admin listener can
	// call SigV4-bypass admin entrypoints (see
	// adapter/dynamodb_admin.go) without going through HTTP. The
	// field is unexported on purpose — it is package-private state,
	// not a public API. Nil until start() reaches the dynamo step.
	dynamoServer *adapter.DynamoDBServer
}

func (r *runtimeServerRunner) start() error {
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
		r.adminServer,
		r.adminGRPCOpts,
	); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	dynamoServer, err := startDynamoDBServer(r.ctx, r.lc, r.eg, r.dynamoAddress, r.shardStore, r.coordinate, r.leaderDynamo, r.metricsRegistry, r.readTracker)
	if err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	r.dynamoServer = dynamoServer
	if err := startS3Server(r.ctx, r.lc, r.eg, r.s3Address, r.shardStore, r.coordinate, r.leaderS3, r.s3Region, r.s3CredsFile, r.s3PathStyleOnly, r.readTracker); err != nil {
		return waitErrgroupAfterStartupFailure(r.cancel, r.eg, err)
	}
	if err := startSQSServer(r.ctx, r.lc, r.eg, r.sqsAddress, r.shardStore, r.coordinate, r.leaderSQS, r.sqsRegion, r.sqsCredsFile); err != nil {
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

// buildKeyVizSampler constructs the in-memory keyviz sampler from
// flag-supplied options, or returns nil when --keyvizEnabled is
// false. The coordinator's WithSampler and AdminServer's
// RegisterSampler both treat a nil receiver as "keyviz disabled," so
// this is the single decision point.
func buildKeyVizSampler() *keyviz.MemSampler {
	if !*keyvizEnabled {
		return nil
	}
	return keyviz.NewMemSampler(keyviz.MemSamplerOptions{
		Step:                   *keyvizStep,
		HistoryColumns:         *keyvizHistoryColumns,
		MaxTrackedRoutes:       *keyvizMaxTrackedRoutes,
		MaxMemberRoutesPerSlot: *keyvizMaxMemberRoutesPerSlot,
	})
}

// keyVizSamplerForCoordinator wraps a *MemSampler in the
// keyviz.Sampler interface understood by ShardedCoordinator. A nil
// sampler returns a typed-nil interface value, so the coordinator's
// `if c.sampler == nil` guard fires and the dispatch hot path skips
// Observe with a single branch.
func keyVizSamplerForCoordinator(s *keyviz.MemSampler) keyviz.Sampler {
	if s == nil {
		return nil
	}
	return s
}

// seedKeyVizRoutes copies the engine's current route catalogue into
// the sampler so the first matrix snapshots have non-empty metadata.
// No-op when the sampler is disabled. The coordinator's
// distribution.Engine handles route mutations after this point;
// route-watch propagation into the sampler is a follow-up (the
// design's Phase 3 persistence work).
func seedKeyVizRoutes(s *keyviz.MemSampler, engine *distribution.Engine) {
	if s == nil || engine == nil {
		return
	}
	for _, r := range engine.Stats() {
		s.RegisterRoute(r.RouteID, r.Start, r.End)
	}
}

// startKeyVizFlusher launches RunFlusher in the supplied errgroup
// and harvests the in-progress step with a final Flush after the
// goroutine returns, so a graceful shutdown does not lose the most
// recent partial column. Skip the goroutine entirely when the
// sampler is disabled — RunFlusher would just park on ctx.Done with
// no work to do, which is a free goroutine but adds no signal.
func startKeyVizFlusher(ctx context.Context, eg *errgroup.Group, s *keyviz.MemSampler) {
	if s == nil {
		return
	}
	eg.Go(func() error {
		keyviz.RunFlusher(ctx, s, s.Step())
		s.Flush()
		return nil
	})
}
