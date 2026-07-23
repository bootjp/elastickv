package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
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

	keyvizEnabled                = flag.Bool("keyvizEnabled", false, "Enable the in-memory key visualizer sampler")
	keyvizStep                   = flag.Duration("keyvizStep", keyviz.DefaultStep, "Flush interval / matrix-column resolution for the keyviz sampler")
	keyvizMaxTrackedRoutes       = flag.Int("keyvizMaxTrackedRoutes", keyviz.DefaultMaxTrackedRoutes, "Maximum routes tracked individually before excess routes coarsen into virtual buckets")
	keyvizMaxMemberRoutesPerSlot = flag.Int("keyvizMaxMemberRoutesPerSlot", keyviz.DefaultMaxMemberRoutesPerSlot, "Maximum members listed on a virtual bucket")
	keyvizHistoryColumns         = flag.Int("keyvizHistoryColumns", keyviz.DefaultHistoryColumns, "Maximum matrix columns retained in the keyviz ring buffer")
	keyvizKeyBucketsPerRoute     = flag.Int("keyvizKeyBucketsPerRoute", keyviz.DefaultKeyBucketsPerRoute, "Order-preserving sub-range buckets per individual route")
	keyvizHotKeysEnabled         = flag.Bool("keyvizHotKeysEnabled", false, "Enable per-route Top-K hot-key sampling")
	keyvizHotKeysPerRoute        = flag.Int("keyvizHotKeysPerRoute", keyviz.DefaultHotKeysPerRoute, "Space-Saving sketch capacity per route")
	keyvizHotKeysSampleRate      = flag.Int("keyvizHotKeysSampleRate", keyviz.DefaultHotKeysSampleRate, "Hot-key observation sample rate")
	keyvizHotKeysQueueSize       = flag.Int("keyvizHotKeysQueueSize", keyviz.DefaultHotKeysQueueSize, "Hot-key aggregator queue capacity")
	keyvizHotKeysMaxKeyLen       = flag.Int("keyvizHotKeysMaxKeyLen", keyviz.DefaultHotKeysMaxKeyLen, "Maximum key length retained by hot-key sampling")

	autoSplit                    = flag.Bool("autoSplit", false, "Enable automatic same-group hotspot range splits on the catalog leader")
	autoSplitEvalInterval        = flag.Duration("autoSplitEvalInterval", autosplit.DefaultEvalInterval, "Interval between automatic hotspot split evaluations")
	autoSplitSplitCooldown       = flag.Duration("autoSplitCooldown", autosplit.DefaultSplitCooldown, "Minimum cooldown before a route created by SplitRange can be auto-split again")
	autoSplitSplitTimeout        = flag.Duration("autoSplitSplitTimeout", autosplit.DefaultSplitTimeout, "Timeout for each automatic SplitRange RPC")
	autoSplitKillSwitchFile      = flag.String("autoSplitKillSwitchFile", "", "If non-empty and the file exists, the auto-split scheduler observes but skips new splits")
	autoSplitDefaultBuckets      = flag.Int("autoSplitDefaultBuckets", autosplit.DefaultSamplerBuckets, "KeyViz buckets per route implied by --autoSplit when --keyvizKeyBucketsPerRoute was not explicitly set")
	autoSplitWriteWeight         = flag.Float64("autoSplitWriteWeight", autosplit.DefaultConfig().WriteWeight, "Weighted ops/min multiplier for writes in automatic hotspot detection")
	autoSplitReadWeight          = flag.Float64("autoSplitReadWeight", autosplit.DefaultConfig().ReadWeight, "Weighted ops/min multiplier for reads in automatic hotspot detection")
	autoSplitThresholdOpsMin     = flag.Float64("autoSplitThreshold", autosplit.DefaultConfig().ThresholdOpsMin, "Weighted ops/min threshold per committed KeyViz column for automatic hotspot detection")
	autoSplitCandidateWindows    = flag.Int("autoSplitCandidateWindows", autosplit.DefaultConfig().CandidateWindows, "Consecutive committed KeyViz columns over threshold required before an automatic split")
	autoSplitMaxRoutes           = flag.Int("autoSplitMaxRoutes", autosplit.DefaultConfig().MaxRoutes, "Maximum live routes allowed after automatic splits")
	autoSplitMaxSplitsPerCycle   = flag.Int("autoSplitMaxPerCycle", autosplit.DefaultConfig().MaxSplitsPerCycle, "Maximum automatic split decisions scheduled per evaluation cycle")
	autoSplitTopKeyShare         = flag.Float64("autoSplitTopKeyShare", autosplit.DefaultConfig().TopKeyShare, "Minimum lower-bound share of route writes required for hot-key isolation")
	autoSplitTopKeyAbsoluteFloor = flag.Float64("autoSplitTopKeyAbsoluteFloor", 0, "Minimum lower-bound hot-key weighted ops/min; zero uses half --autoSplitThreshold")

	keyvizKeyBucketsPerRouteExplicit bool
)

const (
	kvParts             = 2
	defaultFileMode     = 0755
	raftObserveInterval = 5 * time.Second
	demoTickInterval    = 10 * time.Millisecond
	demoHeartbeatTick   = 1
	demoElectionTick    = 10
	demoMaxSizePerMsg   = 1 << 20
	demoMaxInflightMsg  = 256
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
	// raftPeers, when non-empty, seeds the etcd raft factory with the
	// full cluster membership. Demo mode populates it for every node so
	// non-bootstrap nodes start with a known peer list instead of
	// failing with "no persisted peers and no peer list was supplied".
	raftPeers []raftengine.Server
}

func main() {
	flag.Parse()
	recordExplicitDemoFlags(flag.CommandLine)

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
		peers := make([]raftengine.Server, 0, len(nodes))
		for _, n := range nodes {
			redisMapParts = append(redisMapParts, n.address+"="+n.redisAddress)
			s3MapParts = append(s3MapParts, n.address+"="+n.s3Address)
			dynamoMapParts = append(dynamoMapParts, n.address+"="+n.dynamoAddress)
			peers = append(peers, raftengine.Server{
				Suffrage: "voter",
				ID:       n.raftID,
				Address:  n.address,
			})
		}
		raftRedisMapStr := strings.Join(redisMapParts, ",")
		raftS3MapStr := strings.Join(s3MapParts, ",")
		raftDynamoMapStr := strings.Join(dynamoMapParts, ",")

		for _, n := range nodes {
			n.raftRedisMap = raftRedisMapStr
			n.raftS3Map = raftS3MapStr
			n.raftDynamoMap = raftDynamoMapStr
			n.raftPeers = peers
			// etcd raft requires every member of a fresh cluster to
			// bootstrap with the same peer list. Override the per-node
			// raftBootstrap flag in demo mode so n2/n3 don't fail with
			// "no persisted peers and no peer list was supplied".
			n.raftBootstrap = true
			cfg := n // capture loop variable
			if err := run(runCtx, eg, cfg); err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}

		// All three nodes were started with the same raftPeers list and
		// raftBootstrap=true above, so the etcd cluster is fully formed
		// at startup. joinCluster (AddVoter via raftadmin against
		// nodes[0]) is no longer needed and would actively misbehave
		// under etcd's randomised elections — nodes[0] is not guaranteed
		// to be leader, so the AddVoter RPC would either hit a follower
		// and be rejected, or add duplicates to an already-complete
		// configuration.
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

func recordExplicitDemoFlags(fs *flag.FlagSet) {
	if fs == nil {
		return
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "keyvizKeyBucketsPerRoute" {
			keyvizKeyBucketsPerRouteExplicit = true
		}
	})
}

func buildDemoKeyVizSampler() *keyviz.MemSampler {
	if !*keyvizEnabled && !*autoSplit {
		return nil
	}
	keyBucketsPerRoute := *keyvizKeyBucketsPerRoute
	if *autoSplit && !keyvizKeyBucketsPerRouteExplicit && keyBucketsPerRoute <= keyviz.DefaultKeyBucketsPerRoute {
		keyBucketsPerRoute = *autoSplitDefaultBuckets
	}
	return keyviz.NewMemSampler(keyviz.MemSamplerOptions{
		Step:                   *keyvizStep,
		HistoryColumns:         *keyvizHistoryColumns,
		MaxTrackedRoutes:       *keyvizMaxTrackedRoutes,
		MaxMemberRoutesPerSlot: *keyvizMaxMemberRoutesPerSlot,
		KeyBucketsPerRoute:     keyBucketsPerRoute,
		HotKeysEnabled:         *keyvizHotKeysEnabled,
		HotKeysPerRoute:        *keyvizHotKeysPerRoute,
		HotKeysSampleRate:      *keyvizHotKeysSampleRate,
		HotKeysQueueSize:       *keyvizHotKeysQueueSize,
		HotKeysMaxKeyLen:       *keyvizHotKeysMaxKeyLen,
	})
}

func seedDemoKeyVizRoutes(s *keyviz.MemSampler, engine *distribution.Engine) {
	if s == nil || engine == nil {
		return
	}
	for _, route := range engine.Stats() {
		s.RegisterRoute(route.RouteID, route.Start, route.End, route.GroupID)
	}
}

func startDemoKeyVizFlusher(ctx context.Context, eg *errgroup.Group, s *keyviz.MemSampler) {
	if eg == nil || s == nil {
		return
	}
	eg.Go(func() error {
		keyviz.RunFlusher(ctx, s, s.Step())
		s.Flush()
		return nil
	})
	eg.Go(func() error {
		keyviz.RunHotKeysAggregator(ctx, s)
		return nil
	})
}

func demoAutoSplitConfigFromFlags(coordinator *kv.Coordinate) (autosplit.SchedulerConfig, error) {
	cfg := autosplit.SchedulerConfig{
		Enabled:        *autoSplit,
		EvalInterval:   *autoSplitEvalInterval,
		SplitCooldown:  *autoSplitSplitCooldown,
		SplitTimeout:   *autoSplitSplitTimeout,
		KillSwitchFile: strings.TrimSpace(*autoSplitKillSwitchFile),
		Logger:         slog.Default(),
		Detector: autosplit.Config{
			WriteWeight:         *autoSplitWriteWeight,
			ReadWeight:          *autoSplitReadWeight,
			ThresholdOpsMin:     *autoSplitThresholdOpsMin,
			CandidateWindows:    *autoSplitCandidateWindows,
			MaxRoutes:           *autoSplitMaxRoutes,
			MaxSplitsPerCycle:   *autoSplitMaxSplitsPerCycle,
			TopKeyShare:         *autoSplitTopKeyShare,
			TopKeyAbsoluteFloor: *autoSplitTopKeyAbsoluteFloor,
		},
	}
	if coordinator != nil {
		cfg.IsLeader = demoAutoSplitLeaderGate(coordinator)
		cfg.Leadership = func() (bool, uint64) {
			return coordinator.LeadershipForKey(distribution.CatalogVersionKey())
		}
		cfg.GroupLeadership = coordinator.GroupLeadership
	}
	if err := validateDemoAutoSplitConfig(cfg); err != nil {
		return autosplit.SchedulerConfig{}, err
	}
	return cfg, nil
}

func validateDemoAutoSplitConfig(cfg autosplit.SchedulerConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if err := validateDemoAutoSplitSamplerConfig(cfg.Detector); err != nil {
		return err
	}
	if err := validateDemoAutoSplitDetectorConfig(cfg.Detector); err != nil {
		return err
	}
	return validateDemoAutoSplitSchedulerConfig(cfg)
}

func validateDemoAutoSplitSamplerConfig(cfg autosplit.Config) error {
	if demoAutoSplitUsesDefaultBuckets() {
		if *autoSplitDefaultBuckets <= keyviz.DefaultKeyBucketsPerRoute {
			return errors.New("--autoSplitDefaultBuckets must be greater than 1")
		}
		if *autoSplitDefaultBuckets > keyviz.MaxKeyBucketsPerRoute {
			return errors.Errorf("--autoSplitDefaultBuckets (%d) must be <= %d", *autoSplitDefaultBuckets, keyviz.MaxKeyBucketsPerRoute)
		}
	}
	if cfg.MaxRoutes > *keyvizMaxTrackedRoutes {
		return errors.Errorf("--autoSplitMaxRoutes (%d) must be <= --keyvizMaxTrackedRoutes (%d); raise keyviz capacity or lower the auto-split route cap", cfg.MaxRoutes, *keyvizMaxTrackedRoutes)
	}
	return nil
}

func demoAutoSplitUsesDefaultBuckets() bool {
	return *autoSplit &&
		!keyvizKeyBucketsPerRouteExplicit &&
		*keyvizKeyBucketsPerRoute <= keyviz.DefaultKeyBucketsPerRoute
}

func validateDemoAutoSplitDetectorConfig(cfg autosplit.Config) error {
	if err := validateDemoAutoSplitWeights(cfg.WriteWeight, cfg.ReadWeight); err != nil {
		return err
	}
	if err := validateDemoAutoSplitDetectorLimits(cfg); err != nil {
		return err
	}
	return validateDemoAutoSplitHotKeyThresholds(cfg)
}

func validateDemoAutoSplitDetectorLimits(cfg autosplit.Config) error {
	if !isFiniteDemoAutoSplitFloat(cfg.ThresholdOpsMin) || cfg.ThresholdOpsMin <= 0 {
		return errors.New("--autoSplitThreshold must be positive")
	}
	if cfg.CandidateWindows <= 0 {
		return errors.New("--autoSplitCandidateWindows must be positive")
	}
	if cfg.MaxRoutes <= 0 {
		return errors.New("--autoSplitMaxRoutes must be positive")
	}
	if cfg.MaxSplitsPerCycle <= 0 {
		return errors.New("--autoSplitMaxPerCycle must be positive")
	}
	return nil
}

func validateDemoAutoSplitHotKeyThresholds(cfg autosplit.Config) error {
	if !isFiniteDemoAutoSplitFloat(cfg.TopKeyShare) || cfg.TopKeyShare <= 0 || cfg.TopKeyShare > 1 {
		return errors.New("--autoSplitTopKeyShare must be in (0, 1]")
	}
	if !isFiniteDemoAutoSplitFloat(cfg.TopKeyAbsoluteFloor) || cfg.TopKeyAbsoluteFloor < 0 {
		return errors.New("--autoSplitTopKeyAbsoluteFloor must be non-negative")
	}
	return nil
}

func validateDemoAutoSplitWeights(writeWeight, readWeight float64) error {
	if !isFiniteDemoAutoSplitFloat(writeWeight) || !isFiniteDemoAutoSplitFloat(readWeight) ||
		writeWeight < 0 || readWeight < 0 || (writeWeight == 0 && readWeight == 0) {
		return errors.New("--autoSplitWriteWeight and --autoSplitReadWeight must be non-negative and at least one must be positive")
	}
	return nil
}

func isFiniteDemoAutoSplitFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func validateDemoAutoSplitSchedulerConfig(cfg autosplit.SchedulerConfig) error {
	if cfg.EvalInterval <= 0 {
		return errors.New("--autoSplitEvalInterval must be positive")
	}
	if cfg.SplitCooldown <= 0 {
		return errors.New("--autoSplitCooldown must be positive")
	}
	if cfg.SplitTimeout <= 0 {
		return errors.New("--autoSplitSplitTimeout must be positive")
	}
	return nil
}

func startDemoAutoSplitScheduler(
	ctx context.Context,
	eg *errgroup.Group,
	catalog *distribution.CatalogStore,
	distServer *adapter.DistributionServer,
	coordinator *kv.Coordinate,
	sampler *keyviz.MemSampler,
	cfg autosplit.SchedulerConfig,
) {
	if eg == nil || !cfg.Enabled {
		return
	}
	if sampler == nil {
		cfg.Logger.Warn("autosplit: sampler not configured; scheduler disabled")
		return
	}
	if coordinator != nil {
		cfg.IsLeader = demoAutoSplitLeaderGate(coordinator)
		cfg.Leadership = func() (bool, uint64) {
			return coordinator.LeadershipForKey(distribution.CatalogVersionKey())
		}
		cfg.GroupLeadership = coordinator.GroupLeadership
	}
	scheduler := autosplit.NewScheduler(
		cfg,
		catalog,
		demoAutoSplitDistributionSplitter{server: distServer},
		sampler,
		sampler,
	)
	eg.Go(func() error {
		return scheduler.Run(ctx)
	})
}

type demoAutoSplitDistributionSplitter struct {
	server *adapter.DistributionServer
}

func demoAutoSplitLeaderGate(coordinator *kv.Coordinate) func() bool {
	if coordinator == nil {
		return nil
	}
	return func() bool {
		return coordinator.IsLeaderForKey(distribution.CatalogVersionKey())
	}
}

func (s demoAutoSplitDistributionSplitter) SplitRange(ctx context.Context, req autosplit.SplitRequest) (autosplit.SplitResult, error) {
	if s.server == nil {
		return autosplit.SplitResult{}, errors.New("autosplit: distribution server is not configured")
	}
	if req.TargetGroupID != 0 {
		return autosplit.SplitResult{}, errors.New("autosplit: cross-group target selection is not enabled")
	}
	resp, err := s.server.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: req.ExpectedCatalogVersion,
		RouteId:                req.RouteID,
		SplitKey:               distribution.CloneBytes(req.SplitKey),
	})
	if err != nil {
		return autosplit.SplitResult{}, errors.Wrap(err, "autosplit: split range")
	}
	return autosplit.SplitResult{
		CatalogVersion: resp.GetCatalogVersion(),
		Left:           demoAutoSplitRouteFromProto(resp.GetLeft()),
		Right:          demoAutoSplitRouteFromProto(resp.GetRight()),
	}, nil
}

func demoAutoSplitRouteFromProto(route *pb.RouteDescriptor) distribution.RouteDescriptor {
	if route == nil {
		return distribution.RouteDescriptor{}
	}
	state := distribution.RouteStateWriteFenced
	switch route.GetState() {
	case pb.RouteState_ROUTE_STATE_UNSPECIFIED,
		pb.RouteState_ROUTE_STATE_WRITE_FENCED:
		state = distribution.RouteStateWriteFenced
	case pb.RouteState_ROUTE_STATE_ACTIVE:
		state = distribution.RouteStateActive
	case pb.RouteState_ROUTE_STATE_MIGRATING_SOURCE:
		state = distribution.RouteStateMigratingSource
	case pb.RouteState_ROUTE_STATE_MIGRATING_TARGET:
		state = distribution.RouteStateMigratingTarget
	}
	return distribution.RouteDescriptor{
		RouteID:       route.GetRouteId(),
		Start:         distribution.CloneBytes(route.GetStart()),
		End:           distribution.CloneBytes(route.GetEnd()),
		GroupID:       route.GetRaftGroupId(),
		State:         state,
		ParentRouteID: route.GetParentRouteId(),
		SplitAtHLC:    route.GetSplitAtHlc(),
	}
}

type demoDistributionRuntime struct {
	engine       *distribution.Engine
	catalog      *distribution.CatalogStore
	server       *adapter.DistributionServer
	autoSplitCfg autosplit.SchedulerConfig
	reconciler   *autosplit.RouteReconciler
	sampler      *keyviz.MemSampler
}

func setupDemoDistributionRuntime(
	ctx context.Context,
	st store.MVCCStore,
	coordinator *kv.Coordinate,
	readTracker *kv.ActiveTimestampTracker,
	metricsRegistry *monitoring.Registry,
) (demoDistributionRuntime, error) {
	runtime := demoDistributionRuntime{
		engine:  distribution.NewEngineWithDefaultRoute(),
		catalog: distribution.NewCatalogStore(st),
	}
	if _, err := distribution.EnsureCatalogSnapshot(ctx, runtime.catalog, runtime.engine); err != nil {
		return demoDistributionRuntime{}, errors.WithStack(err)
	}
	runtime.server = adapter.NewDistributionServer(
		runtime.engine,
		runtime.catalog,
		adapter.WithDistributionCoordinator(coordinator),
		adapter.WithDistributionActiveTimestampTracker(readTracker),
	)
	var err error
	runtime.autoSplitCfg, err = demoAutoSplitConfigFromFlags(coordinator)
	if err != nil {
		return demoDistributionRuntime{}, err
	}
	runtime.autoSplitCfg.Observer = autosplit.NewPrometheusObserver(metricsRegistry.Registerer())
	runtime.sampler = buildDemoKeyVizSampler()
	seedDemoKeyVizRoutes(runtime.sampler, runtime.engine)
	if runtime.sampler != nil {
		runtime.reconciler = autosplit.NewRouteReconciler(runtime.sampler)
		initialSnapshot, snapshotErr := runtime.catalog.Snapshot(ctx)
		if snapshotErr != nil {
			return demoDistributionRuntime{}, errors.Wrap(snapshotErr, "load initial catalog for keyviz reconciliation")
		}
		runtime.reconciler.Reconcile(initialSnapshot.Routes)
		runtime.autoSplitCfg.Reconciler = runtime.reconciler
	}
	coordinator.WithSamplerRouteResolver(runtime.sampler, demoSamplerRouteResolver(runtime.engine))
	return runtime, nil
}

func demoSamplerRouteResolver(engine *distribution.Engine) func([]byte) (uint64, bool) {
	return func(key []byte) (uint64, bool) {
		route, ok := engine.GetRoute(kv.RouteKey(key))
		return route.RouteID, ok && route.State == distribution.RouteStateActive
	}
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

func setupRedis(ctx context.Context, lc net.ListenConfig, st store.MVCCStore, coordinator *kv.Coordinate, addr, redisAddr, raftRedisMapStr string, relay *adapter.RedisPubSubRelay, readTracker *kv.ActiveTimestampTracker, deltaCompactor *adapter.DeltaCompactor, applyObserver *adapter.RedisApplyObserver) (*adapter.RedisServer, error) {
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
		adapter.WithRedisApplyObserver(applyObserver),
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

// openRaftEngine creates the etcd-backed raft engine for a demo node. It
// resolves the data dir (using a temp dir when cfg.raftDataDir is empty),
// refuses to start on top of legacy hashicorp/raft state, and registers
// cleanup callbacks for the data dir, engine and factory resources.
func openRaftEngine(cfg config, fsm raftengine.StateMachine, cleanup *internalutil.CleanupStack) (*raftengine.FactoryResult, error) {
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
			return nil, errors.WithStack(err)
		}
		cleanup.Add(func() { os.RemoveAll(tmp) })
		raftDir = tmp
	} else if err := os.MkdirAll(raftDir, defaultFileMode); err != nil {
		return nil, errors.WithStack(err)
	}

	// Refuse to start on top of hashicorp/raft artifacts from a previous
	// deployment. The backend has been removed and silently overwriting
	// its state with etcd markers could commit to an incompatible engine
	// over committed data. Fail fast so operators have to migrate
	// explicitly.
	for _, legacy := range []string{"raft.db", "logs.dat", "stable.dat"} {
		if _, err := os.Stat(filepath.Join(raftDir, legacy)); err == nil {
			return nil, errors.WithStack(errors.Newf("legacy hashicorp/raft artifact %q found in %s; hashicorp backend has been removed, manual migration required", legacy, raftDir))
		} else if !os.IsNotExist(err) {
			return nil, errors.WithStack(err)
		}
	}

	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      cfg.raftID,
		LocalAddress: cfg.address,
		DataDir:      raftDir,
		Peers:        cfg.raftPeers,
		Bootstrap:    cfg.raftBootstrap,
		StateMachine: fsm,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cleanup.Add(func() {
		_ = result.Engine.Close()
		if result.Close != nil {
			_ = result.Close()
		}
	})
	return result, nil
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
	redisApplyObserver := adapter.NewRedisApplyObserver()
	fsm := kv.NewKvFSMWithHLC(st, hlc, kv.WithApplyObserver(redisApplyObserver))
	readTracker := kv.NewActiveTimestampTracker()

	result, err := openRaftEngine(cfg, fsm, &cleanup)
	if err != nil {
		return err
	}

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
	distributionRuntime, err := setupDemoDistributionRuntime(ctx, st, coordinator, readTracker, metricsRegistry)
	if err != nil {
		return err
	}
	distEngine := distributionRuntime.engine
	distCatalog := distributionRuntime.catalog
	distServer := distributionRuntime.server
	autoSplitCfg := distributionRuntime.autoSplitCfg
	autoSplitReconciler := distributionRuntime.reconciler
	sampler := distributionRuntime.sampler
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

	rd, err := setupRedis(ctx, lc, st, coordinator, cfg.address, cfg.redisAddress, cfg.raftRedisMap, relay, readTracker, deltaCompactor, redisApplyObserver)
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
	eg.Go(catalogWatcherTask(ctx, distCatalog, distEngine, func(snapshot distribution.CatalogSnapshot) {
		autoSplitReconciler.Reconcile(snapshot.Routes)
	}))
	startDemoKeyVizFlusher(ctx, eg, sampler)
	startDemoAutoSplitScheduler(ctx, eg, distCatalog, distServer, coordinator, sampler, autoSplitCfg)
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

func catalogWatcherTask(
	ctx context.Context,
	distCatalog *distribution.CatalogStore,
	distEngine *distribution.Engine,
	observers ...distribution.CatalogSnapshotObserver,
) func() error {
	return func() error {
		var opts []distribution.CatalogWatcherOption
		if len(observers) > 0 {
			opts = append(opts, distribution.WithCatalogWatcherSnapshotObserver(func(snapshot distribution.CatalogSnapshot) {
				for _, observer := range observers {
					if observer != nil {
						observer(snapshot)
					}
				}
			}))
		}
		if err := distribution.RunCatalogWatcher(ctx, distCatalog, distEngine, slog.Default(), opts...); err != nil {
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
