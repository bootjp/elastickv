package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
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
	etcdMaxInflightMsg    = 1024
	defaultTSOBatchSize   = 256

	defaultFilesystemRootMode              = 0o755
	defaultFilesystemPlacementScanInterval = 30 * time.Second
	defaultFilesystemLeaseReapInterval     = 30 * time.Second
)

func newRaftFactory(engineType raftEngineType, coldStartObs raftengine.ColdStartObserver) (raftengine.Factory, error) {
	switch engineType {
	case raftEngineEtcd:
		return etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
			TickInterval:      etcdTickInterval,
			HeartbeatTick:     durationToTicks(heartbeatTimeout, etcdTickInterval, etcdHeartbeatMinTicks),
			ElectionTick:      durationToTicks(electionTimeout, etcdTickInterval, etcdElectionMinTicks),
			MaxSizePerMsg:     etcdMaxSizePerMsg,
			MaxInflightMsg:    etcdMaxInflightMsg,
			ColdStartObserver: coldStartObs,
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
	myAddr                          = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr                       = flag.String("redisAddress", "localhost:6379", "TCP host+port for redis")
	dynamoAddr                      = flag.String("dynamoAddress", "localhost:8000", "TCP host+port for DynamoDB-compatible API")
	s3Addr                          = flag.String("s3Address", "", "TCP host+port for S3-compatible API; empty to disable")
	s3Region                        = flag.String("s3Region", "us-east-1", "S3 signing region")
	s3CredsFile                     = flag.String("s3CredentialsFile", "", "Path to a JSON file containing static S3 credentials")
	s3PathStyleOnly                 = flag.Bool("s3PathStyleOnly", true, "Only accept path-style S3 requests")
	sqsAddr                         = flag.String("sqsAddress", "", "TCP host+port for SQS-compatible API; empty to disable")
	sqsRegion                       = flag.String("sqsRegion", "us-east-1", "SQS signing region")
	sqsCredsFile                    = flag.String("sqsCredentialsFile", "", "Path to a JSON file containing static SQS credentials")
	metricsAddr                     = flag.String("metricsAddress", "localhost:9090", "TCP host+port for Prometheus metrics")
	metricsToken                    = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	pprofAddr                       = flag.String("pprofAddress", "localhost:6060", "TCP host+port for pprof debug endpoints; empty to disable")
	pprofToken                      = flag.String("pprofToken", "", "Bearer token for pprof; required for non-loopback pprofAddress")
	filesystemMount                 = flag.String("filesystemMount", "", "FUSE mount point for the Elastickv filesystem; empty to disable")
	filesystemClientID              = flag.String("filesystemClientID", "", "Stable open-handle lease client ID for the FUSE mount; empty uses raftId")
	filesystemCapacity              = flag.Uint64("filesystemCapacity", 0, "Filesystem byte capacity reported by statfs; zero reports unlimited")
	filesystemMaxFiles              = flag.Uint64("filesystemMaxFiles", 0, "Filesystem inode capacity reported by statfs; zero reports unlimited")
	filesystemRootMode              = flag.Uint("filesystemRootMode", defaultFilesystemRootMode, "Root directory permission mode for first initialization")
	filesystemRootUID               = flag.Uint("filesystemRootUID", 0, "Root directory owner UID for first initialization")
	filesystemRootGID               = flag.Uint("filesystemRootGID", 0, "Root directory owner GID for first initialization")
	filesystemPlacementScanInterval = flag.Duration("filesystemPlacementScanInterval", defaultFilesystemPlacementScanInterval, "Interval for filesystem placement and recovery-state metrics; non-positive disables periodic scans")
	filesystemLeaseReapInterval     = flag.Duration("filesystemLeaseReapInterval", defaultFilesystemLeaseReapInterval, "Interval for reclaiming expired filesystem open-handle leases; non-positive disables periodic reaping")
	raftId                          = flag.String("raftId", "", "Node id used by Raft")
	raftEngineName                  = flag.String("raftEngine", string(raftEngineEtcd), "Raft engine implementation (etcd)")
	raftDir                         = flag.String("raftDataDir", "data/", "Raft data dir")
	redisLuaMaxIdleStates           = flag.Int("redisLuaMaxIdleStates", adapter.DefaultLuaPoolMaxIdle, "Maximum number of idle *lua.LState instances retained by the Redis Lua VM pool. Each state holds ~200 KiB; lower values reduce steady-state memory at the cost of more allocations under burst, higher values absorb bursts at the cost of memory floor. Non-positive values clamp to the default.")
	raftBootstrap                   = flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	raftBootstrapMembers            = flag.String("raftBootstrapMembers", "", "Comma-separated bootstrap raft members (raftID=host:port,...)")
	raftGroupPeers                  = flag.String("raftGroupPeers", "", "Semicolon-separated per-group bootstrap members (groupID=raftID@host:port,...)")
	raftJoinMembers                 = flag.String("raftJoinMembers", "", "Comma-separated raft members used only for transport discovery while this fresh node joins an existing single-group cluster (raftID=host:port,...); requires --raftJoinAsLearner")
	raftJoinAsLearner               = flag.Bool("raftJoinAsLearner", false, "Local node expects to join an existing cluster as a learner; if a post-apply ConfState lists this node as a voter instead, an ERROR-level alarm fires (the node keeps running -- the flag is an operator alarm, not a consensus veto). See docs/design/2026_04_26_implemented_raft_learner.md §4.5.")
	tsoEnabled                      = flag.Bool("tsoEnabled", false, "Issue coordinator-owned persistence timestamps through the local TSO batch allocator instead of direct HLC calls")
	tsoBatchSize                    = flag.Int("tsoBatchSize", defaultTSOBatchSize, "Timestamp batch size used when --tsoEnabled is true")
	leaderBalance                   = flag.Bool("leaderBalance", false, "Enable automatic count-based Raft-group leader balancing on the default-group leader")
	leaderBalanceInterval           = flag.Duration("leaderBalanceInterval", defaultLeaderBalanceInterval, "Interval between leader-balance scheduler evaluations")
	leaderBalanceGroupCooldown      = flag.Duration("leaderBalanceGroupCooldown", defaultLeaderBalanceGroupCooldown, "Minimum time before the scheduler can move the same raft group again")
	leaderBalanceGlobalCooldown     = flag.Duration("leaderBalanceGlobalCooldown", defaultLeaderBalanceGlobalCooldown, "Minimum time between any two automatic leadership transfers")
	leaderBalanceStartupGrace       = flag.Duration("leaderBalanceStartupGrace", 0, "Grace period after acquiring default-group leadership before issuing transfers; 0 uses max(interval, global cooldown)")
	leaderBalanceImbalanceThreshold = flag.Int("leaderBalanceImbalanceThreshold", defaultLeaderBalanceImbalanceThreshold, "Minimum leader-count spread required before balancing")
	leaderBalanceMaxTargetLag       = flag.Uint64("leaderBalanceMaxTargetLag", defaultLeaderBalanceMaxTargetLag, "Maximum target lag in raft log entries for gated automatic leadership transfers; 0 requires the target to match the leader's last log index")
	leaderBalancePinGroups          = flag.String("leaderBalancePinGroups", "", "Comma-separated raft group IDs excluded from automatic leader balancing")
	leaderBalanceKillSwitchFile     = flag.String("leaderBalanceKillSwitchFile", "", "If non-empty and the file exists, the leader-balance scheduler observes but skips transfers")
	raftGroups                      = flag.String("raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	shardRanges                     = flag.String("shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	raftRedisMap                    = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
	raftS3Map                       = flag.String("raftS3Map", "", "Map of Raft address to S3 address (raftAddr=s3Addr,...)")
	raftDynamoMap                   = flag.String("raftDynamoMap", "", "Map of Raft address to DynamoDB address (raftAddr=dynamoAddr,...)")
	raftSqsMap                      = flag.String("raftSqsMap", "", "Map of Raft address to SQS address (raftAddr=sqsAddr,...)")
	// HT-FIFO partition assignment (Phase 3.D §5). Distinct from
	// --raftSqsMap (which maps raftAddr=sqsAddr for the
	// proxyToLeader endpoint resolution). The grammar is
	// `queue.fifo:N=group_0,...,group_{N-1}` with multiple queues
	// separated by `;`. Empty by default — leaving the flag empty
	// means no FIFO queue is partitioned and the legacy
	// single-partition layout applies to every queue. PR 5 of the
	// rollout plan consumes this map to dispatch SendMessage and
	// fan out ReceiveMessage; the §11 PR 2 dormancy gate currently
	// rejects PartitionCount > 1 on CreateQueue regardless of this
	// flag, so populating it has no effect on production traffic
	// until PR 5 lands.
	sqsFifoPartitionMap = flag.String("sqsFifoPartitionMap", "", "HT-FIFO partition map (queue.fifo:N=group_0,...,group_{N-1};...)")
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

	// Data-at-rest encryption admin RPC wiring (Stage 5D). The
	// EncryptionAdmin gRPC service is reachable on every shard's
	// gRPC listener so the §7.1 Phase-0 GetCapability fan-out can
	// poll any member.
	//
	// This flag gates ONLY the read-only capability surface:
	// empty → GetCapability reports encryption_capable=false (the
	// §7.1 cutover refuses with ErrCapabilityCheckFailed);
	// set   → capability probing reads the §5.1 keys.json and
	// reports encryption_capable=true.
	//
	// Mutating RPCs (BootstrapEncryption / RotateDEK /
	// RegisterEncryptionWriter) are gated by Stage 6B-2 on the
	// AND of --encryption-enabled and --kekFile being non-empty.
	// Setting --encryptionSidecarPath ALONE no longer enables
	// mutators; the operator must explicitly opt in to encryption
	// AND supply a KEK source. With either gate condition false,
	// registerEncryptionAdminServer omits the Proposer + LeaderView
	// options and every mutator short-circuits at the gRPC boundary
	// with FailedPrecondition before any Raft proposal is created.
	encryptionSidecarPath = flag.String("encryptionSidecarPath", "", "§5.1 keys.json path; enables read-only EncryptionAdmin capability probing. Mutating RPCs (Bootstrap / RotateDEK / RegisterEncryptionWriter) are additionally gated on this flag being non-empty AND --encryption-enabled AND --kekFile being non-empty (all three required so the applier's WithKEK + WithKeystore + WithSidecarPath options are all wired before mutators can commit).")

	// Stage 6B-2: cluster-wide encryption opt-in flag. The mutating
	// EncryptionAdmin RPCs (BootstrapEncryption, RotateDEK,
	// RegisterEncryptionWriter) become reachable only when this
	// flag is set AND --kekFile points at a valid KEK source.
	// Default off; pre-Stage-6 clusters and operators who have
	// not yet committed to encryption are unaffected.
	encryptionEnabled = flag.Bool("encryption-enabled", false, "§6.5 opt-in to encryption-mutating EncryptionAdmin RPCs. Requires --kekFile to be set; without that, mutators still refuse with FailedPrecondition. Default off.")

	// Stage 6F: operator-requested DEK rotation at boot. The flag is
	// intentionally a request, not a guarantee: only the leader of the
	// default encryption Raft group proposes the rotation; followers
	// keep the request in memory and fire it only if they acquire
	// leadership during this process uptime.
	encryptionRotateOnStartup = flag.Bool("encryption-rotate-on-startup", false, "§6.5 request a one-shot DEK rotation after this node becomes leader of the default Raft group. Safe for rolling restarts: followers keep the request in memory and only fire if they acquire leadership during this process uptime.")

	// Stage 6B-2: KEK source. The KEK never appears in elastickv's
	// data dir; it is held externally and exercised only at process
	// boot and at DEK bootstrap/rotation per §5.1. Stage 6B-2 ships
	// only the file-backed wrapper (kek.FileWrapper); KMS providers
	// (--kekUri) land in Stage 9. Empty disables KEK loading; the
	// applier's ApplyBootstrap and ApplyRotation paths then return
	// ErrKEKNotConfigured at apply time, which is masked at the
	// RPC boundary by the mutator gate documented above.
	kekFile = flag.String("kekFile", "", "§5.1 KEK file path (32 raw bytes, owner-only mode). When set, the file-backed kek.Wrapper is constructed at startup and threaded into the §6.3 EncryptionApplier so ApplyBootstrap and ApplyRotation can KEK-unwrap.")

	// Key visualizer sampler flags. The sampler runs entirely in-memory
	// on each node, feeds AdminServer.GetKeyVizMatrix, and is disabled
	// by default — opt in with --keyvizEnabled. The other flags are
	// no-ops when the sampler is disabled.
	keyvizEnabled                = flag.Bool("keyvizEnabled", false, "Enable the in-memory key visualizer sampler that feeds AdminServer.GetKeyVizMatrix")
	keyvizStep                   = flag.Duration("keyvizStep", keyviz.DefaultStep, "Flush interval / matrix-column resolution for the keyviz sampler")
	keyvizMaxTrackedRoutes       = flag.Int("keyvizMaxTrackedRoutes", keyviz.DefaultMaxTrackedRoutes, "Maximum routes tracked individually before excess routes coarsen into virtual buckets")
	keyvizMaxMemberRoutesPerSlot = flag.Int("keyvizMaxMemberRoutesPerSlot", keyviz.DefaultMaxMemberRoutesPerSlot, "Maximum members listed on a virtual bucket; excess routes still drive the bucket counters")
	keyvizHistoryColumns         = flag.Int("keyvizHistoryColumns", keyviz.DefaultHistoryColumns, "Maximum matrix columns retained in the keyviz ring buffer (each column = one Step)")
	keyvizKeyBucketsPerRoute     = flag.Int("keyvizKeyBucketsPerRoute", keyviz.DefaultKeyBucketsPerRoute, "Order-preserving sub-range buckets per individual route for the hot-key heatmap; 1 disables sub-bucketing (route-granular, today's behaviour). Capped at 256; memory is ~K*32 bytes/route, so K_max ~= memBudget/(32*keyvizMaxTrackedRoutes)")
	keyvizLabelsEnabled          = flag.Bool("keyvizLabelsEnabled", false, "Enable per-adapter KeyViz row labels. Default false keeps legacy route-only rows during rolling upgrades")

	// Hot-key drill-down (Phase 2-A++; design 2026_05_28_implemented_keyviz_hot_key_topk).
	// Off by default — the disabled-case adds one early-return branch
	// to Observe and retains zero real key bytes. When enabled, the
	// sampler retains actual hot key bytes in memory and exposes them
	// via the admin /keyviz/hotkeys drill-down (gated behind admin
	// auth + audit).
	keyvizHotKeysEnabled    = flag.Bool("keyvizHotKeysEnabled", false, "Enable per-route Top-K hot-key drill-down (retains actual key bytes; admin auth + keyviz flag both required)")
	keyvizHotKeysPerRoute   = flag.Int("keyvizHotKeysPerRoute", keyviz.DefaultHotKeysPerRoute, "Space-Saving sketch capacity m per route (default 64, cap 256). Larger m tightens the error bound N_total/m at the cost of memory")
	keyvizHotKeysSampleRate = flag.Int("keyvizHotKeysSampleRate", keyviz.DefaultHotKeysSampleRate, "Hot-keys hot-path sample rate R: 1-in-R observes are enqueued (default 16, cap 1024). Higher R reduces hot-path cost; raises Chernoff miss probability over the sampled stream")
	keyvizHotKeysQueueSize  = flag.Int("keyvizHotKeysQueueSize", keyviz.DefaultHotKeysQueueSize, "Bounded channel size between Observe and the hot-keys aggregator. Default 8192, cap 65536. Drops past this are counted (dropped_samples -> degraded)")
	keyvizHotKeysMaxKeyLen  = flag.Int("keyvizHotKeysMaxKeyLen", keyviz.DefaultHotKeysMaxKeyLen, "Maximum key length sampled into the hot-keys sketch (default 1024 B, cap 4096). Longer keys bump skipped_long_keys -> degraded, never truncated")
	// Phase 2-C cluster fan-out: comma-separated list of admin
	// HTTP endpoints (host:port or scheme://host:port). When set,
	// the admin keyviz handler aggregates the local matrix with
	// peer responses; when empty, behaviour is unchanged
	// (single-node view). See docs/design/2026_04_27_implemented_keyviz_cluster_fanout.md.
	keyvizFanoutNodes   = flag.String("keyvizFanoutNodes", "", "Comma-separated peer admin endpoints (host:port) for keyviz cluster-wide fan-out; empty disables")
	keyvizFanoutTimeout = flag.Duration("keyvizFanoutTimeout", keyvizFanoutDefaultTimeout, "Per-peer timeout for keyviz fan-out HTTP calls")
)

// keyvizFanoutDefaultTimeout matches design 9 open-question 2: 2 s
// per peer call. Operators on weird networks override via the flag.
const keyvizFanoutDefaultTimeout = 2 * time.Second

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

const (
	lockResolverEnabledEnvVar = "ELASTICKV_LOCK_RESOLVER_ENABLED"
	fsmCompactorEnabledEnvVar = "ELASTICKV_FSM_COMPACTOR_ENABLED"
)

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

func enabledEnv(name string) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid "+name+"; using default",
			"value", raw,
			"default", true,
		)
		return true
	}
	return enabled
}

func startLockResolverIfEnabled(shardStore *kv.ShardStore, shardGroups map[uint64]*kv.ShardGroup, cleanup *internalutil.CleanupStack) {
	if enabledEnv(lockResolverEnabledEnvVar) {
		lockResolver := kv.NewLockResolver(shardStore, shardGroups, nil)
		cleanup.Add(func() { lockResolver.Close() })
		return
	}
	slog.Info("background lock resolver disabled", "env", lockResolverEnabledEnvVar)
}

func startFSMCompactorIfEnabled(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, readTracker *kv.ActiveTimestampTracker) {
	if enabledEnv(fsmCompactorEnabledEnvVar) {
		compactor := kv.NewFSMCompactor(
			fsmCompactionRuntimes(runtimes),
			kv.WithFSMCompactorActiveTimestampTracker(readTracker),
		)
		eg.Go(func() error {
			return compactor.Run(ctx)
		})
		return
	}
	slog.Info("fsm compactor disabled", "env", fsmCompactorEnabledEnvVar)
}

func run() error {
	cfg, engineType, bootstrapCfg, bootstrap, err := resolveRuntimeInputs()
	if err != nil {
		return err
	}

	var lc net.ListenConfig

	metricsRegistry := monitoring.NewRegistry(*raftId, *myAddr)

	// Factory needs the cold-start observer from the registry so the
	// engine's restoreSnapshotState path can emit
	// elastickv_fsm_cold_start_restore_total / _applied_index_gap
	// (PR #934 round-1 codex P2 closed this plumbing gap — the
	// observer was previously unused because Factory.Create did not
	// carry it through to OpenConfig).
	factory, err := newRaftFactory(engineType, metricsRegistry.ColdStartObserver())
	if err != nil {
		return err
	}

	// Create the shared HLC before building shard groups so every FSM can update
	// physicalCeiling when HLC lease entries are applied to the Raft log.
	clock := kv.NewHLC()

	// Stage 6B-2: construct the shared KEK wrapper + in-memory
	// Keystore once at startup, before any FSM is built. Both are
	// process-wide singletons:
	//
	//   - KEK is exercised at DEK bootstrap (§5.6) and rotation
	//     (§5.2) by every shard's applier; one wrapper per process
	//     is what the file-mode check + kek.FileWrapper invariants
	//     assume.
	//   - Keystore is the in-memory map of (key_id → DEK bytes) the
	//     storage cipher (§6.2, wired in Stage 6D) and the
	//     EncryptionApplier (Stage 6A/6B) both read from. Sharing
	//     one instance across shards keeps post-bootstrap DEKs
	//     visible to every shard's storage cipher.
	//
	// Both are nil-safe in the applier path: WithKEK / WithKeystore
	// are only attached to the applier when --kekFile is non-empty
	// (else the applier stays in the Stage 6A posture where
	// ApplyBootstrap / ApplyRotation return ErrKEKNotConfigured).
	kekWrapper, err := loadKEKAfterPreNonceStartupGuards(cfg)
	if err != nil {
		return err
	}
	keystore := encryption.NewKeystore()
	redisApplyObserver := adapter.NewRedisApplyObserver()

	// Stage 6D-6c: buildShardGroupsWithEncryptionWiring assembles the
	// storage-envelope write-path wiring (cipher + deterministic nonce
	// factory + the process-shared StateCache) before opening any
	// shard store, then constructs the shard groups with it. The
	// wiring hydrates the keystore and bumps the §4.1 local_epoch when
	// a storage DEK is already active on disk (restart path); on a
	// pre-bootstrap binary the per-Put gate stays cleartext until a
	// runtime Bootstrap + EnableStorageEnvelope flips it.
	runtimes, shardGroups, encWiring, err := buildShardGroupsWithEncryptionWiring(
		*raftId,
		*raftDir,
		cfg.groups,
		cfg.defaultGroup,
		cfg.multi,
		bootstrap,
		bootstrapCfg,
		factory,
		func(groupID uint64) kv.ProposalObserver {
			return metricsRegistry.RaftProposalObserver(groupID)
		},
		clock,
		kekWrapper,
		keystore,
		*encryptionSidecarPath,
		*encryptionEnabled,
		cfg.engine,
		redisApplyObserver,
	)
	if err = chainEncryptionStartupGuard(
		err,
		runtimes,
		cfg.defaultGroup,
		*encryptionSidecarPath,
		*encryptionEnabled,
	); err != nil {
		return err
	}

	// Record the active FSM apply sync mode so operators can see on the
	// /metrics endpoint which durability posture this node is running in.
	// The label is resolved per-pebbleStore from ELASTICKV_FSM_SYNC_MODE
	// in NewPebbleStore; read it off the first constructed store (all
	// shards share the same env and therefore the same label).
	recordFSMApplySyncMode(metricsRegistry, runtimes)

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
	startLockResolverIfEnabled(shardStore, shardGroups, &cleanup)
	sampler := buildKeyVizSampler()
	coordinate := kv.NewShardedCoordinator(cfg.engine, shardGroups, cfg.defaultGroup, clock, shardStore).
		WithLeaseReadObserver(metricsRegistry.LeaseReadObserver()).
		WithSampler(keyVizSamplerForCoordinator(sampler)).
		WithKeyVizLabelsEnabled(*keyvizLabelsEnabled).
		WithAllShardGroups(dataGroupIDs(cfg.groups)...).
		WithPartitionResolver(buildSQSPartitionResolver(cfg.sqsFifoPartitionMap))
	if err := configureCoordinatorTSO(coordinate); err != nil {
		return err
	}

	// SQS HT-FIFO §8 leadership-refusal: install per-group
	// observers that step the local node down via
	// TransferLeadership when it acquires (or already holds)
	// leadership of a Raft group hosting a partitioned FIFO
	// queue while the binary lacks the htfifo capability. The
	// composite deregister flows through cleanup; it's a no-op
	// when no group hosts a partitioned queue or when the
	// binary advertises htfifo (the steady-state production
	// case post-PR-4-B-3b).
	leadershipRefusalDeregister := installSQSLeadershipRefusalAcrossGroups(
		ctx, runtimes, cfg.sqsFifoPartitionMap,
		sqsAdvertisesHTFIFO(), slog.Default())
	cleanup.Add(leadershipRefusalDeregister)
	eg, runCtx := errgroup.WithContext(ctx)
	startRaftEngineLifecycleWatchers(runCtx, eg, runtimes)
	// setupDistributionCatalog + the Stage 7a process-start registration
	// gate are bundled so run() has a single startup-fault path: a
	// registry-read / behind-epoch failure fails the process
	// synchronously here, BEFORE the gRPC servers serve, so writes never
	// run with no registration gate installed.
	distCatalog, err := setupDistributionAndRegistration(
		runCtx, eg, runtimes, cfg.engine,
		coordinate, shardGroups[cfg.defaultGroup], encWiring, *raftId, *encryptionSidecarPath)
	if err != nil {
		cancel()
		return err
	}
	// Seed AFTER setupDistributionCatalog so the sampler picks up the
	// catalog-assigned RouteIDs. EnsureCatalogSnapshot inside
	// setupDistributionCatalog applies a snapshot back into the engine
	// with durable non-zero RouteIDs; seeding earlier would register
	// the placeholder zero IDs from buildEngine and Observe would miss
	// every dispatched mutation.
	seedKeyVizRoutes(sampler, cfg.engine)

	eg.Go(func() error {
		return runDistributionCatalogWatcher(runCtx, distCatalog, cfg.engine)
	})
	startKeyVizFlusher(runCtx, eg, sampler)
	startKeyVizLeaderTermPublisher(runCtx, eg, sampler, runtimes)
	startMemoryWatchdog(runCtx, eg, cancel)
	distServer := adapter.NewDistributionServer(
		cfg.engine,
		distCatalog,
		adapter.WithDistributionCoordinator(coordinate),
		adapter.WithDistributionActiveTimestampTracker(readTracker),
		adapter.WithDistributionFilesystemObserver(metricsRegistry.FileSystemObserver()),
	)
	startMonitoringCollectors(runCtx, metricsRegistry, runtimes, clock)
	startFSMCompactorIfEnabled(runCtx, eg, runtimes, readTracker)

	// Stage 7c §3.1: build the encryption-aware
	// MembershipChangeInterceptor here where the concrete
	// *kv.ShardedCoordinator and *kv.ShardGroup are available. Returns
	// nil when encryption is not wired (no StateCache or no default
	// group), in which case raftadmin.Server skips the pre-step.
	encryptionConfChangeInterceptor := newEncryptionPreRegister(
		coordinate, shardGroups[cfg.defaultGroup], encWiring.cache, *encryptionSidecarPath, etcdraftengine.DeriveNodeID)
	defaultRuntime := findDefaultGroupRuntime(runtimes, cfg.defaultGroup)
	rotateOnStartupDeregister, waitRotateOnStartup := installEncryptionRotateOnStartup(
		runCtx,
		*encryptionRotateOnStartup,
		defaultRuntime,
		postCutoverProposerForRuntime(defaultRuntime, shardGroups),
		*encryptionSidecarPath,
		kekWrapper,
		encWiring.raftEnvelope,
		etcdraftengine.DeriveNodeID(*raftId),
		encWiring.epoch,
		encWiring.raftEpoch,
		slog.Default(),
	)
	cleanup.Add(rotateOnStartupDeregister)
	if err := startServersAfterStartupRotation(waitRotateOnStartup, serversInput{
		ctx: runCtx, eg: eg, cancel: cancel, lc: &lc,
		runtimes: runtimes, shardGroups: shardGroups, bootstrapServers: bootstrapCfg.adminSeed(cfg.defaultGroup),
		shardStore: shardStore, coordinate: coordinate,
		distServer: distServer, readTracker: readTracker,
		metricsRegistry: metricsRegistry, cfg: cfg,
		redisApplyObserver:              redisApplyObserver,
		cleanup:                         &cleanup,
		encWiring:                       encWiring,
		keyvizSampler:                   sampler,
		encryptionConfChangeInterceptor: encryptionConfChangeInterceptor,
	}); err != nil {
		return err
	}
	startLeaderBalanceScheduler(
		runCtx,
		eg,
		runtimes,
		leaderBalanceConfigFromFlags(*raftId, cfg.defaultGroup, cfg.sqsFifoPartitionMap, metricsRegistry.Registerer()),
	)

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "failed to serve")
	}
	return nil
}

func startRaftEngineLifecycleWatchers(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime) {
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		engine := rt.snapshotEngine()
		lifecycle, ok := engine.(raftengine.Lifecycle)
		if !ok {
			continue
		}
		done := lifecycle.Done()
		if done == nil {
			continue
		}
		groupID := rt.spec.id
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			case <-done:
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				if err := lifecycle.Err(); err != nil {
					return errors.Wrapf(err, "raft group %d engine stopped", groupID)
				}
				return nil
			}
		})
	}
}

type filesystemStartConfig struct {
	mountPoint string
	clientID   string
	rootMode   uint32
	rootUID    uint32
	rootGID    uint32
}

func filesystemStartConfigFromFlags() (filesystemStartConfig, error) {
	config := filesystemStartConfig{mountPoint: strings.TrimSpace(*filesystemMount)}
	if config.mountPoint == "" {
		return config, nil
	}
	if *filesystemRootMode > 0o7777 || *filesystemRootUID > math.MaxUint32 || *filesystemRootGID > math.MaxUint32 {
		return filesystemStartConfig{}, errors.New("filesystem root mode, uid, or gid is out of range")
	}
	config.clientID = strings.TrimSpace(*filesystemClientID)
	if config.clientID == "" {
		config.clientID = strings.TrimSpace(*raftId)
	}
	if config.clientID == "" {
		return filesystemStartConfig{}, errors.New("filesystem client ID is required")
	}
	config.rootMode = uint32(*filesystemRootMode)
	config.rootUID = uint32(*filesystemRootUID)
	config.rootGID = uint32(*filesystemRootGID)
	return config, nil
}

func startFilesystemIfEnabled(
	ctx context.Context,
	eg *errgroup.Group,
	cleanup *internalutil.CleanupStack,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	observer monitoring.FileSystemObserver,
) error {
	config, err := filesystemStartConfigFromFlags()
	if err != nil {
		return err
	}
	if config.mountPoint == "" {
		return nil
	}
	if eg == nil || cleanup == nil {
		return errors.New("filesystem lifecycle is required")
	}
	service, err := filesystem.NewService(
		shardStore,
		coordinate,
		filesystem.WithCapacity(*filesystemCapacity),
		filesystem.WithMaxFiles(*filesystemMaxFiles),
		filesystem.WithOperationalObserver(observer),
	)
	if err != nil {
		return errors.Wrap(err, "create filesystem service")
	}
	if err := service.InitializeRoot(
		ctx,
		config.rootMode,
		config.rootUID,
		config.rootGID,
	); err != nil {
		return errors.Wrap(err, "initialize filesystem root")
	}
	server, serveDone, err := mountFilesystemService(service, config)
	if err != nil {
		return err
	}
	installFilesystemServerLifecycle(ctx, eg, cleanup, server, serveDone, config.mountPoint)
	startFilesystemPlacementCollector(ctx, eg, service, *filesystemPlacementScanInterval)
	startFilesystemLeaseReaper(ctx, eg, service, *filesystemLeaseReapInterval)
	slog.Info("filesystem FUSE mounted", "mount_point", config.mountPoint, "client_id", config.clientID)
	return nil
}

func mountFilesystemService(
	service *filesystem.Service,
	config filesystemStartConfig,
) (*fuseadapter.Server, <-chan struct{}, error) {
	frontend := fuseadapter.New(service, []byte(config.clientID))
	server, err := fuseadapter.Mount(config.mountPoint, frontend, nil)
	if err != nil {
		frontend.Close()
		return nil, nil, errors.Wrap(err, "mount filesystem frontend")
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		<-serveDone
		return nil, nil, errors.Wrap(err, "wait for filesystem FUSE mount")
	}
	return server, serveDone, nil
}

func installFilesystemServerLifecycle(
	ctx context.Context,
	eg *errgroup.Group,
	cleanup *internalutil.CleanupStack,
	server *fuseadapter.Server,
	serveDone <-chan struct{},
	mountPoint string,
) {
	unmount := sync.OnceValue(server.Unmount)
	cleanup.Add(func() {
		if err := unmount(); err != nil {
			slog.Warn("filesystem FUSE unmount failed", "mount_point", mountPoint, "err", err)
		}
	})
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			if err := unmount(); err != nil {
				slog.WarnContext(ctx, "filesystem FUSE unmount failed", "mount_point", mountPoint, "err", err)
			}
			<-serveDone
			return nil
		case <-serveDone:
			return errors.New("filesystem FUSE server stopped unexpectedly")
		}
	})
}

func startFilesystemPlacementCollector(
	ctx context.Context,
	eg *errgroup.Group,
	service *filesystem.Service,
	interval time.Duration,
) {
	if eg == nil || service == nil || interval <= 0 {
		return
	}
	eg.Go(func() error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if _, err := service.ListFilePlacementStats(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "filesystem placement scan failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
}

type filesystemLeaseReaper interface {
	ReapExpiredOpenHandleLeases(context.Context, int) (filesystem.LeaseReapStats, error)
}

func startFilesystemLeaseReaper(
	ctx context.Context,
	eg *errgroup.Group,
	reaper filesystemLeaseReaper,
	interval time.Duration,
) {
	if eg == nil || reaper == nil || interval <= 0 {
		return
	}
	eg.Go(func() error {
		return runFilesystemLeaseReaper(ctx, reaper, interval)
	})
}

func runFilesystemLeaseReaper(
	ctx context.Context,
	reaper filesystemLeaseReaper,
	interval time.Duration,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		stats, err := reaper.ReapExpiredOpenHandleLeases(ctx, 0)
		if err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "filesystem lease reaper failed", "err", err)
		}
		if stats.ExpiredRefs > 0 || stats.OrphanedInodesGCed > 0 {
			slog.InfoContext(ctx, "filesystem lease reaper reclaimed state",
				"expired_refs", stats.ExpiredRefs,
				"orphaned_inodes_gced", stats.OrphanedInodesGCed,
			)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func resolveRuntimeInputs() (runtimeConfig, raftEngineType, raftBootstrapConfig, bool, error) {
	if *raftId == "" {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, errors.New("flag --raftId is required")
	}

	engineType, err := parseRaftEngineType(*raftEngineName)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}

	cfg, err := parseRuntimeConfig(*myAddr, *redisAddr, *s3Addr, *dynamoAddr, *sqsAddr, *raftGroups, *shardRanges, *raftRedisMap, *raftS3Map, *raftDynamoMap, *raftSqsMap, *sqsFifoPartitionMap)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}

	bootstrapCfg, err := resolveRaftPeerConfig(
		*raftId,
		cfg.groups,
		*raftBootstrapMembers,
		*raftGroupPeers,
		*raftJoinMembers,
		*raftJoinAsLearner,
		*raftBootstrap,
	)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}

	return cfg, engineType, bootstrapCfg, *raftBootstrap || bootstrapCfg.anyBootstrapServers(), nil
}

type raftBootstrapConfig struct {
	legacyServers []raftengine.Server
	groupServers  map[uint64][]raftengine.Server
	joinServers   []raftengine.Server
}

func (c raftBootstrapConfig) anyBootstrapServers() bool {
	return len(c.legacyServers) > 0 || len(c.groupServers) > 0
}

func (c raftBootstrapConfig) serversForGroup(groupID uint64) []raftengine.Server {
	if len(c.joinServers) != 0 {
		return cloneRaftServers(c.joinServers)
	}
	if len(c.groupServers) != 0 {
		return cloneRaftServers(c.groupServers[groupID])
	}
	return cloneRaftServers(c.legacyServers)
}

func (c raftBootstrapConfig) bootstrapsGroup(groupID uint64) bool {
	if len(c.groupServers) != 0 {
		return len(c.groupServers[groupID]) > 0
	}
	return len(c.legacyServers) > 0
}

func (c raftBootstrapConfig) bootstrapSeedForGroup(groupID uint64) []raftengine.Server {
	if len(c.groupServers) == 0 {
		return nil
	}
	return cloneRaftServers(c.groupServers[groupID])
}

func (c raftBootstrapConfig) adminSeed(defaultGroup uint64) []raftengine.Server {
	if len(c.joinServers) != 0 {
		return cloneRaftServers(c.joinServers)
	}
	if len(c.groupServers) != 0 {
		return cloneRaftServers(c.groupServers[defaultGroup])
	}
	return cloneRaftServers(c.legacyServers)
}

func cloneRaftServers(in []raftengine.Server) []raftengine.Server {
	if len(in) == 0 {
		return nil
	}
	return append([]raftengine.Server(nil), in...)
}

type runtimeConfig struct {
	groups              []groupSpec
	defaultGroup        uint64
	engine              *distribution.Engine
	leaderRedis         map[string]string
	leaderS3            map[string]string
	leaderDynamo        map[string]string
	leaderSQS           map[string]string
	sqsFifoPartitionMap map[string]sqsFifoQueueRouting
	multi               bool
}

func parseRuntimeConfig(myAddr, redisAddr, s3Addr, dynamoAddr, sqsAddr, raftGroups, shardRanges, raftRedisMap, raftS3Map, raftDynamoMap, raftSqsMap, sqsFifoPartitionMapRaw string) (runtimeConfig, error) {
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

	sqsFifoPartitionMap, err := buildSQSFifoPartitionMap(groups, sqsFifoPartitionMapRaw)
	if err != nil {
		return runtimeConfig{}, err
	}

	return runtimeConfig{
		groups:              groups,
		defaultGroup:        defaultGroup,
		engine:              engine,
		leaderRedis:         leaderRedis,
		leaderS3:            leaderS3,
		leaderDynamo:        leaderDynamo,
		leaderSQS:           leaderSQS,
		sqsFifoPartitionMap: sqsFifoPartitionMap,
		multi:               dataGroupsNeedMultiDirs(groups),
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

// buildSQSPartitionResolver flattens the operator-supplied partition
// map into the {queue → []groupID} shape adapter consumes and
// returns a ResolveGroup-capable resolver. Returns nil on an empty
// map so the coordinator's resolver field stays unset on a non-
// partitioned cluster — kv.ShardRouter.WithPartitionResolver(nil)
// is a documented no-op, so the request hot path keeps the existing
// engine-only dispatch.
//
// Return type is the kv.PartitionResolver interface, NOT the
// concrete *adapter.SQSPartitionResolver, because Go wraps a typed
// nil pointer into a NON-NIL interface value when the function
// signature is the concrete type. With a concrete return type, a
// non-partitioned cluster would carry a non-nil interface whose
// underlying pointer is nil, the resolver-first short-circuit
// `s.partitionResolver != nil` would always pass, and every request
// would pay an extra ResolveGroup call (which the nil-receiver
// guard makes safe but not free). The interface return type makes
// the untyped `nil` propagate as a true nil interface.
//
// The group-reference parsing here cannot fail in practice because
// parseSQSFifoGroupList already canonicalized each entry as a
// uint64 string at flag-parse time; the conversion is repeated
// defensively so a future caller that bypasses parseSQSFifoGroupList
// (e.g. a test seeding the map programmatically) gets a clear panic
// instead of a silent route-to-group-zero.
func buildSQSPartitionResolver(partitionMap map[string]sqsFifoQueueRouting) kv.PartitionResolver {
	r := buildSQSPartitionResolverConcrete(partitionMap)
	if r == nil {
		// Defensive typed-nil → untyped-nil interface conversion.
		// The doc on this function explains the typed-nil hazard:
		// a non-nil interface wrapping a nil concrete pointer
		// would defeat kv.ShardRouter's `s.partitionResolver !=
		// nil` short-circuit and cost every request an extra
		// nil-receiver ResolveGroup call.
		return nil
	}
	return r
}

// buildSQSPartitionResolverConcrete returns the concrete
// *adapter.SQSPartitionResolver so the SQS server can install it
// via WithSQSPartitionResolver and reuse the routing map for the
// CreateQueue capability gate's coverage check (Codex P1 review
// on PR #734, round 2). Returns nil when partitionMap is empty —
// callers that need the kv.PartitionResolver interface must go
// through buildSQSPartitionResolver to avoid the typed-nil
// interface trap.
func buildSQSPartitionResolverConcrete(partitionMap map[string]sqsFifoQueueRouting) *adapter.SQSPartitionResolver {
	if len(partitionMap) == 0 {
		return nil
	}
	flat := make(map[string][]uint64, len(partitionMap))
	for queue, routing := range partitionMap {
		ids := make([]uint64, 0, len(routing.groups))
		for _, groupRef := range routing.groups {
			id, err := strconv.ParseUint(groupRef, 10, 64)
			if err != nil {
				// parseSQSFifoGroupList canonicalized this; a
				// non-uint64 string here means a programmer skipped
				// the validator. Panic loudly rather than silently
				// route to group 0.
				panic(errors.Wrapf(err,
					"queue %q: bypassed group-ref canonicalisation, %q is not uint64",
					queue, groupRef))
			}
			ids = append(ids, id)
		}
		flat[queue] = ids
	}
	return adapter.NewSQSPartitionResolver(flat)
}

// buildSQSFifoPartitionMap parses and validates the
// --sqsFifoPartitionMap flag against the configured Raft groups.
// Extracted from parseRuntimeConfig so that function stays under the
// cyclop ceiling once the SQS HT-FIFO config plumbing landed.
func buildSQSFifoPartitionMap(groups []groupSpec, raw string) (map[string]sqsFifoQueueRouting, error) {
	parsed, err := parseSQSFifoPartitionMap(raw)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse sqs fifo partition map")
	}
	if len(parsed) == 0 {
		return parsed, nil
	}
	if err := validateSQSFifoPartitionMapNoDedicatedTSOGroup(parsed); err != nil {
		return nil, errors.Wrapf(err, "invalid sqs fifo partition map")
	}
	groupIDs := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		groupIDs[strconv.FormatUint(g.id, 10)] = struct{}{}
	}
	if err := validateSQSFifoPartitionMap(parsed, groupIDs); err != nil {
		return nil, errors.Wrapf(err, "invalid sqs fifo partition map")
	}
	return parsed, nil
}

func validateSQSFifoPartitionMapNoDedicatedTSOGroup(partitionMap map[string]sqsFifoQueueRouting) error {
	for _, queue := range slices.Sorted(maps.Keys(partitionMap)) {
		routing := partitionMap[queue]
		for partition, group := range routing.groups {
			if group == strconv.FormatUint(dedicatedTSORaftGroupID, 10) {
				return errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
					"queue %q partition %d: group %q is reserved for TSO",
					queue, partition, group)
			}
		}
	}
	return nil
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
	ErrRaftGroupPeersMutuallyExclusive    = errors.New("flags --raftBootstrapMembers and --raftGroupPeers are mutually exclusive")
	ErrRaftGroupPeersUnknownGroup         = errors.New("flag --raftGroupPeers references unknown raft group")
	ErrRaftGroupPeersMissingGroup         = errors.New("flag --raftGroupPeers must include every raft group")
	ErrRaftGroupPeersMissingLocalNode     = errors.New("flag --raftGroupPeers group must include local --raftId")
	ErrRaftGroupPeersLocalAddrMismatch    = errors.New("flag --raftGroupPeers local address must match local raft group address")
	ErrRaftGroupPeersTooFewVoters         = errors.New("flag --raftGroupPeers group must contain at least two voters")
	ErrRaftGroupPeersHeterogeneous        = errors.New("flag --raftGroupPeers requires identical raft IDs across groups")
	ErrNoRaftGroupPeersConfigured         = errors.New("no raft group peers configured")
	ErrJoinMembersRequireSingleGroup      = errors.New("flag --raftJoinMembers requires exactly one raft group")
	ErrJoinMembersRequireLearner          = errors.New("flag --raftJoinMembers requires --raftJoinAsLearner")
	ErrJoinMembersConflictBootstrap       = errors.New("flag --raftJoinMembers cannot be combined with raft bootstrap flags")
	ErrJoinMembersMissingLocalNode        = errors.New("flag --raftJoinMembers must include local --raftId")
	ErrJoinMembersLocalAddrMismatch       = errors.New("flag --raftJoinMembers local address must match local raft group address")
	ErrJoinMembersTooFewMembers           = errors.New("flag --raftJoinMembers must include the local node and at least one existing member")
	ErrNoJoinMembersConfigured            = errors.New("no raft join members configured")
)

const (
	minRaftGroupPeerVoters = 2
	minRaftJoinMembers     = 2
)

func resolveBootstrapConfig(
	raftID string,
	groups []groupSpec,
	bootstrapMembers string,
	groupPeersRaw string,
) (raftBootstrapConfig, error) {
	if strings.TrimSpace(groupPeersRaw) == "" {
		servers, err := resolveBootstrapServers(raftID, groups, bootstrapMembers)
		if err != nil {
			return raftBootstrapConfig{}, err
		}
		return raftBootstrapConfig{legacyServers: servers}, nil
	}
	if strings.TrimSpace(bootstrapMembers) != "" {
		return raftBootstrapConfig{}, errors.WithStack(ErrRaftGroupPeersMutuallyExclusive)
	}
	groupServers, err := resolveRaftGroupPeers(raftID, groups, groupPeersRaw)
	if err != nil {
		return raftBootstrapConfig{}, err
	}
	return raftBootstrapConfig{groupServers: groupServers}, nil
}

func resolveRaftPeerConfig(
	raftID string,
	groups []groupSpec,
	bootstrapMembers string,
	groupPeersRaw string,
	joinMembers string,
	joinAsLearner bool,
	explicitBootstrap bool,
) (raftBootstrapConfig, error) {
	if strings.TrimSpace(joinMembers) == "" {
		return resolveBootstrapConfig(raftID, groups, bootstrapMembers, groupPeersRaw)
	}
	if !joinAsLearner {
		return raftBootstrapConfig{}, errors.WithStack(ErrJoinMembersRequireLearner)
	}
	if explicitBootstrap || strings.TrimSpace(bootstrapMembers) != "" || strings.TrimSpace(groupPeersRaw) != "" {
		return raftBootstrapConfig{}, errors.WithStack(ErrJoinMembersConflictBootstrap)
	}
	servers, err := resolveJoinServers(raftID, groups, joinMembers)
	if err != nil {
		return raftBootstrapConfig{}, err
	}
	return raftBootstrapConfig{joinServers: servers}, nil
}

func resolveJoinServers(raftID string, groups []groupSpec, joinMembers string) ([]raftengine.Server, error) {
	if len(groups) != 1 {
		return nil, errors.WithStack(ErrJoinMembersRequireSingleGroup)
	}
	servers, err := parseRaftBootstrapMembers(joinMembers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse raft join members")
	}
	if len(servers) == 0 {
		return nil, errors.WithStack(ErrNoJoinMembersConfigured)
	}
	if len(servers) < minRaftJoinMembers {
		return nil, errors.WithStack(ErrJoinMembersTooFewMembers)
	}

	localAddr := groups[0].address
	for _, server := range servers {
		if server.ID != raftID {
			continue
		}
		if server.Address != localAddr {
			return nil, errors.Wrapf(ErrJoinMembersLocalAddrMismatch, "expected %q got %q", localAddr, server.Address)
		}
		return servers, nil
	}
	return nil, errors.Wrapf(ErrJoinMembersMissingLocalNode, "raftId=%q", raftID)
}

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

func resolveRaftGroupPeers(raftID string, groups []groupSpec, raw string) (map[uint64][]raftengine.Server, error) {
	parsed, err := parseRaftGroupPeers(raw)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse raft group peers")
	}
	if len(parsed) == 0 {
		return nil, errors.WithStack(ErrNoRaftGroupPeersConfigured)
	}
	groupByID := make(map[uint64]groupSpec, len(groups))
	for _, g := range groups {
		groupByID[g.id] = g
	}
	if err := validateRaftGroupPeerCoverage(parsed, groupByID); err != nil {
		return nil, err
	}
	if err := validateRaftGroupPeerLocalNode(raftID, parsed, groupByID); err != nil {
		return nil, err
	}
	if err := validateRaftGroupPeerHomogeneity(parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

func validateRaftGroupPeerCoverage(parsed map[uint64][]raftengine.Server, groupByID map[uint64]groupSpec) error {
	for _, groupID := range slices.Sorted(maps.Keys(parsed)) {
		if _, ok := groupByID[groupID]; !ok {
			return errors.Wrapf(ErrRaftGroupPeersUnknownGroup, "group %d", groupID)
		}
		if len(parsed[groupID]) < minRaftGroupPeerVoters {
			return errors.Wrapf(ErrRaftGroupPeersTooFewVoters, "group %d", groupID)
		}
	}
	for _, groupID := range slices.Sorted(maps.Keys(groupByID)) {
		if _, ok := parsed[groupID]; !ok {
			return errors.Wrapf(ErrRaftGroupPeersMissingGroup, "group %d", groupID)
		}
	}
	return nil
}

func validateRaftGroupPeerLocalNode(raftID string, parsed map[uint64][]raftengine.Server, groupByID map[uint64]groupSpec) error {
	for _, groupID := range slices.Sorted(maps.Keys(parsed)) {
		localAddr := groupByID[groupID].address
		found := false
		for _, server := range parsed[groupID] {
			if server.ID != raftID {
				continue
			}
			found = true
			if server.Address != localAddr {
				return errors.Wrapf(ErrRaftGroupPeersLocalAddrMismatch,
					"group %d expected %q got %q", groupID, localAddr, server.Address)
			}
		}
		if !found {
			return errors.Wrapf(ErrRaftGroupPeersMissingLocalNode, "group %d raftId=%q", groupID, raftID)
		}
	}
	return nil
}

func validateRaftGroupPeerHomogeneity(parsed map[uint64][]raftengine.Server) error {
	var canonical []string
	var canonicalGroup uint64
	for _, groupID := range slices.Sorted(maps.Keys(parsed)) {
		ids := raftServerIDs(parsed[groupID])
		if canonical == nil {
			canonical = ids
			canonicalGroup = groupID
			continue
		}
		if !slices.Equal(canonical, ids) {
			return errors.Wrapf(ErrRaftGroupPeersHeterogeneous,
				"group %d ids %v differ from group %d ids %v",
				groupID, ids, canonicalGroup, canonical)
		}
	}
	return nil
}

func raftServerIDs(servers []raftengine.Server) []string {
	ids := make([]string, 0, len(servers))
	for _, server := range servers {
		ids = append(ids, server.ID)
	}
	slices.Sort(ids)
	return ids
}

func buildShardGroups(
	raftID string,
	raftDir string,
	groups []groupSpec,
	multi bool,
	bootstrap bool,
	bootstrapCfg raftBootstrapConfig,
	factory raftengine.Factory,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	clock *kv.HLC,
	kekWrapper kek.Wrapper,
	keystore *encryption.Keystore,
	sidecarPath string,
	encWiring encryptionWriteWiring,
	routeEngine *distribution.Engine,
	applyObservers ...kv.ApplyObserver,
) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, error) {
	// Defend the "cache is always non-nil" contract for callers that
	// pass a zero-value encryptionWriteWiring{} (the encryption-off
	// test harnesses). WithStateCache(nil) would otherwise make
	// NewApplier install a private per-applier cache, silently
	// breaking the shared-cache invariant 6D-6c-1 relies on.
	encWiring = encWiring.withDefaultedCache()
	multi = effectiveMultiDataDirs(groups, multi)
	builder := shardGroupBuilder{
		raftID:                   raftID,
		raftDir:                  raftDir,
		multi:                    multi,
		bootstrap:                bootstrap,
		bootstrapCfg:             bootstrapCfg,
		factory:                  factory,
		proposalObserverForGroup: proposalObserverForGroup,
		clock:                    clock,
		kekWrapper:               kekWrapper,
		keystore:                 keystore,
		sidecarPath:              sidecarPath,
		encWiring:                encWiring,
		routeEngine:              routeEngine,
		applyObservers:           applyObservers,
	}
	runtimes := make([]*raftGroupRuntime, 0, len(groups))
	shardGroups := make(map[uint64]*kv.ShardGroup, len(groups))
	for _, g := range groups {
		runtime, sg, err := builder.build(g)
		if err != nil {
			closeRaftGroupRuntimes(runtimes)
			return nil, nil, err
		}
		runtimes = append(runtimes, runtime)
		shardGroups[g.id] = sg
		encWiring.attachRaftEnvelopeGroup(g.id, sg)
	}
	return runtimes, shardGroups, nil
}

type shardGroupBuilder struct {
	raftID                   string
	raftDir                  string
	multi                    bool
	bootstrap                bool
	bootstrapCfg             raftBootstrapConfig
	factory                  raftengine.Factory
	proposalObserverForGroup func(uint64) kv.ProposalObserver
	clock                    *kv.HLC
	kekWrapper               kek.Wrapper
	keystore                 *encryption.Keystore
	sidecarPath              string
	encWiring                encryptionWriteWiring
	routeEngine              *distribution.Engine
	applyObservers           []kv.ApplyObserver
}

func (b shardGroupBuilder) build(group groupSpec) (*raftGroupRuntime, *kv.ShardGroup, error) {
	groupBootstrap, groupBootstrapServers, groupBootstrapSeed := bootstrapSettingsForGroup(b.bootstrapCfg, group.id, b.bootstrap)
	observer := observerForGroup(b.proposalObserverForGroup, group.id)
	if group.id == dedicatedTSORaftGroupID {
		runtime, sg, err := buildDedicatedTSOGroup(
			b.raftID, group, b.raftDir, b.multi, groupBootstrap,
			groupBootstrapServers, groupBootstrapSeed,
			b.factory, b.clock, observer,
		)
		return runtime, sg, errors.Wrap(err, "failed to start dedicated TSO group")
	}
	return b.buildDataGroup(group, groupBootstrap, groupBootstrapServers, groupBootstrapSeed, observer)
}

func (b shardGroupBuilder) buildDataGroup(
	group groupSpec,
	bootstrap bool,
	bootstrapServers []raftengine.Server,
	bootstrapSeed []raftengine.Server,
	proposalObserver kv.ProposalObserver,
) (*raftGroupRuntime, *kv.ShardGroup, error) {
	dir := groupDataDir(b.raftDir, b.raftID, group.id, b.multi)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create fsm store dir for group %d", group.id)
	}
	st, err := store.NewPebbleStore(filepath.Join(dir, "fsm.db"), b.encWiring.pebbleOptions()...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open pebble fsm store for group %d", group.id)
	}
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to construct writer registry for group %d", group.id)
	}
	applierOpts := encryptionApplierOptionsFor(b.kekWrapper, b.keystore, b.sidecarPath, b.encWiring)
	applier, err := encryption.NewApplier(reg, applierOpts...)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to construct encryption applier for group %d", group.id)
	}
	sm := kv.NewKvFSMWithHLC(st, b.clock, fsmOptionsForGroup(applier, b.routeEngine, group.id, b.encWiring, b.applyObservers...)...)
	runtime, err := buildRuntimeForGroup(
		b.raftID, group, b.raftDir, b.multi, bootstrap,
		bootstrapServers, bootstrapSeed,
		st, sm, b.factory, *raftJoinAsLearner,
	)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to start raft group %d", group.id)
	}
	// Every group goes through the wrap-aware proposer so HLC renewals and
	// transactional proposals observe raft-envelope cutovers uniformly.
	sg := &kv.ShardGroup{Engine: runtime.engine, Store: st}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func closeRaftGroupRuntimes(runtimes []*raftGroupRuntime) {
	for _, runtime := range runtimes {
		runtime.Close()
	}
}

// buildDedicatedTSOGroup constructs group 0 with the minimal TSO state machine.
// It intentionally does not open an MVCC store: shard routing validation keeps
// user data out of group 0, while the state machine persists only its ceiling
// and allocation floor in Raft snapshots. The ShardGroup still receives the
// normal proposer wrapper so lease renewals participate in raft-envelope
// cutovers exactly like data-group proposals.
func buildDedicatedTSOGroup(
	raftID string,
	group groupSpec,
	baseDir string,
	multi bool,
	bootstrap bool,
	bootstrapServers []raftengine.Server,
	bootstrapSeed []raftengine.Server,
	factory raftengine.Factory,
	clock *kv.HLC,
	proposalObserver kv.ProposalObserver,
) (*raftGroupRuntime, *kv.ShardGroup, error) {
	sm := kv.NewTSOStateMachine(clock)
	runtime, err := buildRuntimeForGroup(
		raftID, group, baseDir, multi, bootstrap,
		bootstrapServers, bootstrapSeed,
		nil, sm, factory, *raftJoinAsLearner,
	)
	if err != nil {
		return nil, nil, err
	}
	sg := &kv.ShardGroup{Engine: runtime.engine}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func bootstrapSettingsForGroup(
	cfg raftBootstrapConfig,
	groupID uint64,
	explicitBootstrap bool,
) (bool, []raftengine.Server, []raftengine.Server) {
	servers := cfg.serversForGroup(groupID)
	return explicitBootstrap || cfg.bootstrapsGroup(groupID), servers, cfg.bootstrapSeedForGroup(groupID)
}

func fsmOptionsForGroup(applier *encryption.Applier, routeEngine *distribution.Engine, groupID uint64, encWiring encryptionWriteWiring, applyObservers ...kv.ApplyObserver) []kv.FSMOption {
	opts := []kv.FSMOption{
		kv.WithEncryption(applier),
		kv.WithRouteHistory(kv.WrapDistributionEngine(routeEngine), groupID),
	}
	if encWiring.raftEnvelope != nil {
		opts = append(opts, kv.WithCutoverSource(encWiring.raftEnvelope))
	}
	for _, observer := range applyObservers {
		opts = append(opts, kv.WithApplyObserver(observer))
	}
	return opts
}

func observerForGroup(factory func(uint64) kv.ProposalObserver, groupID uint64) kv.ProposalObserver {
	if factory == nil {
		return nil
	}
	return factory(groupID)
}

// proposerForGroup returns the wrap-aware proposer for rt's shard
// group, falling back to the raw engine when shardGroups does not
// have an entry (test fixtures that construct a partial map).
// Centralises the Stage 6E-2c forwarded-write wiring so the
// Internal.Forward receive side and any future wrap-aware
// construction site share the same lookup shape: the leader's
// local LeaderProxy.Commit path is already wrap-aware via
// NewLeaderProxyForShardGroup; this helper extends the same
// guarantee to the Internal.Forward path that follower-forwarded
// writes ride (codex P1 round-1 r2).
func proposerForGroup(rt *raftGroupRuntime, shardGroups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	if sg, ok := shardGroups[rt.spec.id]; ok && sg != nil {
		return sg.Proposer()
	}
	return rt.engine
}

func postCutoverProposerForRuntime(rt *raftGroupRuntime, shardGroups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	if rt == nil {
		return nil
	}
	return proposerForGroup(rt, shardGroups)
}

func appliedIndexForEngine(engine raftengine.Engine) func() uint64 {
	applied, ok := engine.(interface{ AppliedIndex() uint64 })
	if !ok {
		return nil
	}
	return applied.AppliedIndex
}

func loadKEKAfterPreNonceStartupGuards(cfg runtimeConfig) (kek.Wrapper, error) {
	if err := checkEnvelopeCutoverDivergenceBeforeNonceBump(
		*raftId,
		*raftDir,
		cfg.groups,
		cfg.defaultGroup,
		cfg.multi,
		*encryptionSidecarPath,
		*encryptionEnabled,
	); err != nil {
		return nil, err
	}
	return loadKEKAndRunStartupGuards()
}

// loadKEKAndRunStartupGuards loads the file-backed KEK wrapper and
// runs the §9.1 startup-refusal guards (Stage 6C-1) BEFORE
// buildShardGroups constructs any Raft engine or storage state. The
// two operations are paired in a single helper because the guards
// need the loaded KEK to verify each wrapped DEK in the sidecar
// unwraps cleanly under the configured KEK (ErrKEKMismatch), and
// pairing keeps run()'s top-level branch count under the cyclop
// budget.
//
// The triple gate in Stage 6B-2 (encryptionMutatorsEnabled) is
// the RPC-boundary guard; this helper is the process-boundary
// guard. Both layers exist by design — the RPC gate keeps
// unreachable mutator paths unreachable, the startup gate keeps
// misconfigured nodes from booting at all.
//
// Scope of this pre-engine guard layer is documented in
// internal/encryption/startup.go's CheckStartupGuards godoc and in
// docs/design/2026_04_29_partial_data_at_rest_encryption.md. The
// Stage 6C-3 membership/registry guards run next inside
// buildShardGroupsWithEncryptionWiring, still before Raft engine
// startup; the sidecar-behind-raft-log gap guard remains later
// because it needs an opened engine's applied index and scanner.
func loadKEKAndRunStartupGuards() (kek.Wrapper, error) {
	kekWrapper, err := loadKEKWrapperFromFlag()
	if err != nil {
		return nil, err
	}
	if err := encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: *encryptionEnabled,
		KEKConfigured:     *kekFile != "",
		KEK:               kekWrapper,
		SidecarPath:       *encryptionSidecarPath,
	}); err != nil {
		return nil, errors.Wrap(err, "encryption startup guards refused process start")
	}
	return kekWrapper, nil
}

// loadKEKWrapperFromFlag constructs the file-backed KEK wrapper
// from the --kekFile flag, returning nil if the flag is empty.
// Returns the kek.Wrapper interface rather than the concrete
// *kek.FileWrapper so the call site (buildShardGroups → applier)
// stays decoupled from the file-mode provider — Stage 9 KMS
// providers (AWS KMS, GCP KMS, Vault) will satisfy the same
// interface and slot in without rewriting the dispatch site.
func loadKEKWrapperFromFlag() (kek.Wrapper, error) {
	if *kekFile == "" {
		return nil, nil
	}
	w, err := kek.NewFileWrapper(*kekFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load KEK from %s", *kekFile)
	}
	return w, nil
}

// applierOptionsFor assembles the variadic ApplierOption slice
// for encryption.NewApplier based on which Stage 6B-2 dependencies
// the operator has wired at startup. Each nil/empty input
// suppresses its option, leaving the applier in the Stage 6A
// posture for that axis. Extracted from buildShardGroups so the
// per-shard loop stays under the cyclop complexity budget.
//
// kekWrapper is typed as encryption.KEKUnwrapper (the
// applier-side narrow interface) rather than the wider
// kek.Wrapper so the helper stays decoupled from the wrap-side
// path. Any kek.Wrapper satisfies encryption.KEKUnwrapper
// structurally because both declare Unwrap with the same
// signature.
func applierOptionsFor(kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore, sidecarPath string) []encryption.ApplierOption {
	const maxOpts = 3
	opts := make([]encryption.ApplierOption, 0, maxOpts)
	if kekWrapper != nil {
		opts = append(opts, encryption.WithKEK(kekWrapper))
	}
	if keystore != nil {
		opts = append(opts, encryption.WithKeystore(keystore))
	}
	if sidecarPath != "" {
		opts = append(opts, encryption.WithSidecarPath(sidecarPath))
	}
	return opts
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

func recordFSMApplySyncMode(reg *monitoring.Registry, runtimes []*raftGroupRuntime) {
	if label := fsmApplySyncModeLabelFromRuntimes(runtimes); label != "" {
		reg.SetFSMApplySyncMode(label)
	}
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
// dashboard. Engines that do not satisfy the interface are skipped
// silently; their groups simply won't contribute to
// elastickv_raft_dispatch_* metrics.
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
	readTracker        *kv.ActiveTimestampTracker
	metricsRegistry    *monitoring.Registry
	cfg                runtimeConfig
	encWiring          encryptionWriteWiring
	redisApplyObserver *adapter.RedisApplyObserver
	cleanup            *internalutil.CleanupStack
	// keyvizSampler is the in-memory key visualizer sampler, or nil
	// when --keyvizEnabled is false. Threaded into setupAdminService
	// so AdminServer.GetKeyVizMatrix can serve snapshots; the
	// coordinator already has its own copy from
	// `WithSampler(...)` higher up in run().
	keyvizSampler *keyviz.MemSampler
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
	adminServer, adminGRPCOpts, err := setupAdminService(*raftId, *myAddr, in.runtimes, in.bootstrapServers, in.keyvizSampler)
	if err != nil {
		return err
	}
	// roleStore + connCache are gated on *adminEnabled. With admin
	// disabled, building either is wasted work AND a security
	// regression risk: a non-empty -adminFullAccessKeys flag would
	// otherwise still flip forwardDeps.readyForRegistration() to
	// true, registering the leader-side gRPC AdminForward service
	// and re-exposing the table-write surface a follower-direct
	// admin call could reach (P1/Major review on #648).
	// The HTTP admin listener already short-circuits in
	// prepareAdminFromFlags when *adminEnabled is false; the gRPC path
	// must do the same.
	var (
		roleStore admin.RoleStore
		connCache *kv.GRPCConnCache
	)
	if *adminEnabled {
		roleStore = roleStoreFromFlags(parseCSV(*adminFullAccessKeys), parseCSV(*adminReadOnlyAccessKeys))
		// connCache is shared between the follower-side LeaderForwarder
		// (built inside prepareAdminFromFlags) and any future bridge that
		// dials the leader's gRPC ports. Keeping a single instance per
		// process means the two paths re-use TLS / HTTP/2 connections
		// rather than each maintaining a parallel pool. The shutdown
		// goroutine drains the cache on context cancellation so the
		// accumulated HTTP/2 connections are not leaked when the
		// process exits gracefully (Claude review on #648).
		connCache = &kv.GRPCConnCache{}
		cache := connCache
		in.eg.Go(func() error {
			<-in.ctx.Done()
			if err := cache.Close(); err != nil {
				return errors.Wrap(err, "close admin gRPC connection cache")
			}
			return nil
		})
	}
	publicKVGate := &startupPublicKVGate{}
	installHLCLeaseRenewalBlocker(in.coordinate, waitRotateOnStartup.BlockMutators)
	adapterCoordinate := startupGatedCoordinator{
		inner: in.coordinate,
		gate:  publicKVGate,
	}
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
		adminServer:        adminServer,
		adminGRPCOpts:      adminGRPCOpts,
		redisAddress:       *redisAddr,
		leaderRedis:        in.cfg.leaderRedis,
		pubsubRelay:        adapter.NewRedisPubSubRelay(),
		readTracker:        in.readTracker,
		encWiring:          in.encWiring,
		redisApplyObserver: in.redisApplyObserver,
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
	startHLCLeaseRenewal(in.ctx, in.eg, in.coordinate)
	publicKVGate.markReady()
	if err := runner.startPublicServices(); err != nil {
		return err
	}
	runner.startAdminHTTP()
	return startFilesystemIfEnabled(
		in.ctx,
		in.eg,
		in.cleanup,
		in.shardStore,
		in.coordinate,
		in.metricsRegistry.FileSystemObserver(),
	)
}

const raftJoinReadyPollInterval = 100 * time.Millisecond

func preparePublicServicesAfterRaftJoinReady(
	ctx context.Context,
	runtimes []*raftGroupRuntime,
	localID string,
	enabled bool,
	prepare func() error,
) error {
	if err := waitForRaftJoinReady(ctx, runtimes, localID, enabled); err != nil {
		return errors.Wrap(err, "wait for fresh raft join before binding public services")
	}
	return prepare()
}

func waitForRaftJoinReady(ctx context.Context, runtimes []*raftGroupRuntime, localID string, enabled bool) error {
	if !enabled {
		return nil
	}
	ticker := time.NewTicker(raftJoinReadyPollInterval)
	defer ticker.Stop()
	for {
		ready, err := raftJoinRuntimesReady(ctx, runtimes, localID)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

func raftJoinRuntimesReady(ctx context.Context, runtimes []*raftGroupRuntime, localID string) (bool, error) {
	if len(runtimes) == 0 {
		return false, nil
	}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.engine == nil {
			return false, nil
		}
		configuration, err := runtime.engine.Configuration(ctx)
		if err != nil {
			return false, errors.Wrapf(err, "read raft group %d join configuration", runtime.spec.id)
		}
		if !configurationContainsMember(configuration, localID) {
			return false, nil
		}
		status := runtime.engine.Status()
		if status.Leader.ID == "" || status.PendingConfChange || status.AppliedIndex < status.CommitIndex {
			return false, nil
		}
	}
	return true, nil
}

func configurationContainsMember(configuration raftengine.Configuration, localID string) bool {
	for _, server := range configuration.Servers {
		if server.ID == localID && (server.Suffrage == "learner" || server.Suffrage == "voter") {
			return true
		}
	}
	return false
}

func configureCoordinatorTSO(coordinate *kv.ShardedCoordinator) error {
	if !*tsoEnabled {
		return nil
	}
	// Group 0 is reserved for TSO state, but data-shard leaders must keep
	// issuing timestamps locally until a TSO-leader redirect path exists.
	tso, err := kv.NewLocalTSOAllocator(coordinate)
	if err != nil {
		return errors.Wrap(err, "configure tso allocator")
	}
	batch, err := kv.NewBatchAllocator(tso, *tsoBatchSize)
	if err != nil {
		return errors.Wrap(err, "configure tso batch allocator")
	}
	coordinate.WithTSOAllocator(batch)
	return nil
}

type hlcLeaseRenewalBlocker interface {
	SetHLCLeaseRenewalBlocker(func() bool)
}

type hlcLeaseRenewalRunner interface {
	RunHLCLeaseRenewal(context.Context)
}

func installHLCLeaseRenewalBlocker(coordinate kv.Coordinator, blocked func() bool) {
	if coordinate == nil || blocked == nil {
		return
	}
	if installer, ok := coordinate.(hlcLeaseRenewalBlocker); ok {
		installer.SetHLCLeaseRenewalBlocker(blocked)
	}
}

func startHLCLeaseRenewal(ctx context.Context, eg *errgroup.Group, coordinate kv.Coordinator) {
	if eg == nil || coordinate == nil {
		return
	}
	runner, ok := coordinate.(hlcLeaseRenewalRunner)
	if !ok {
		return
	}
	eg.Go(func() error {
		runner.RunHLCLeaseRenewal(ctx)
		return nil
	})
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

type startupGatedCoordinator struct {
	inner kv.Coordinator
	gate  *startupPublicKVGate
}

var _ kv.Coordinator = (*startupGatedCoordinator)(nil)
var _ kv.LeaseReadableCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.AllGroupsLeaseReadableCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.GroupRoutableCoordinator = (*startupGatedCoordinator)(nil)

func (c startupGatedCoordinator) Dispatch(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if c.gate != nil && c.gate.blocked() {
		return nil, status.Error(codes.Unavailable, "startup rotation has not completed") //nolint:wrapcheck // Preserve the gRPC status for adapters.
	}
	return c.inner.Dispatch(ctx, reqs) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) IsLeader() bool {
	return c.inner.IsLeader()
}

func (c startupGatedCoordinator) VerifyLeader(ctx context.Context) error {
	return c.inner.VerifyLeader(ctx) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) RaftLeader() string {
	return c.inner.RaftLeader()
}

func (c startupGatedCoordinator) IsLeaderForKey(key []byte) bool {
	return c.inner.IsLeaderForKey(key)
}

func (c startupGatedCoordinator) VerifyLeaderForKey(ctx context.Context, key []byte) error {
	return c.inner.VerifyLeaderForKey(ctx, key) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) RaftLeaderForKey(key []byte) string {
	return c.inner.RaftLeaderForKey(key)
}

func (c startupGatedCoordinator) Clock() *kv.HLC {
	return c.inner.Clock()
}

func (c startupGatedCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	return kv.LeaseReadThrough(c.inner, ctx) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	return kv.LeaseReadForKeyThrough(c.inner, ctx, key) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) LeaseReadAllGroups(ctx context.Context) error {
	return kv.LeaseReadAllGroupsThrough(c.inner, ctx) //nolint:wrapcheck // Pass through coordinator errors unchanged.
}

func (c startupGatedCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	if router, ok := c.inner.(kv.GroupRoutableCoordinator); ok {
		return router.EngineGroupIDForKey(key)
	}
	return 0
}

type startupPublicKVGate struct {
	ready        atomic.Bool
	blockMutator func() bool
}

func (g *startupPublicKVGate) markReady() {
	if g != nil {
		g.ready.Store(true)
	}
}

func (g *startupPublicKVGate) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if g != nil && info != nil && startupRotationGatedMethod(info.FullMethod) && g.blocked() {
		// Return a raw gRPC status so clients and retry policy see Unavailable.
		//nolint:wrapcheck
		return nil, status.Error(codes.Unavailable, "startup rotation has not completed")
	}
	return handler(ctx, req)
}

func (g *startupPublicKVGate) blocked() bool {
	if g == nil {
		return false
	}
	if !g.ready.Load() {
		return true
	}
	return g.blockMutator != nil && g.blockMutator()
}

func startupRotationGatedMethod(fullMethod string) bool {
	switch fullMethod {
	case pb.Internal_Forward_FullMethodName,
		pb.AdminForward_Forward_FullMethodName,
		pb.Distribution_SplitRange_FullMethodName,
		pb.RaftAdmin_AddVoter_FullMethodName,
		pb.RaftAdmin_AddLearner_FullMethodName,
		pb.RaftAdmin_PromoteLearner_FullMethodName,
		pb.RaftAdmin_RemoveServer_FullMethodName,
		pb.RaftAdmin_TransferLeadership_FullMethodName,
		pb.EncryptionAdmin_BootstrapEncryption_FullMethodName,
		pb.EncryptionAdmin_RotateDEK_FullMethodName,
		pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		pb.EncryptionAdmin_ResyncSidecar_FullMethodName,
		pb.EncryptionAdmin_EnableStorageEnvelope_FullMethodName,
		pb.EncryptionAdmin_EnableRaftEnvelope_FullMethodName:
		return true
	default:
		return strings.HasPrefix(fullMethod, "/RawKV/") ||
			strings.HasPrefix(fullMethod, "/TransactionalKV/")
	}
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
	srv.SetCapability(adapter.S3BlobOffloadCapabilityName, adapter.S3BlobOffloadLocalCapability())
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
func startMonitoringCollectors(ctx context.Context, reg *monitoring.Registry, runtimes []*raftGroupRuntime, clock *kv.HLC) {
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
	if obs := reg.HLCObserver(); obs != nil && clock != nil {
		obs.Start(ctx, clock, raftMetricsObserveInterval)
	}
}

// startSQSDepthObserver wires the SQS adapter (when enabled on this
// node) into the monitoring registry's SQSObserver so the
// elastickv_sqs_queue_messages gauges start updating. Mirrors the
// Raft / Redis pattern: the source is plugged in once after startup,
// then the observer owns the ticker. nil sqsServer (e.g.
// --sqsAddress empty on this node) is a no-op.
//
// The thin adapter exists because monitoring.SQSQueueDepth and
// adapter.SQSQueueDepth are intentionally distinct types — having
// the adapter import monitoring would invert the dependency
// direction (every adapter would then know about Prometheus).
// Conversion is a fixed 4-field copy and a shape mismatch surfaces
// at compile time here, not at runtime on the metrics path.
func startSQSDepthObserver(ctx context.Context, reg *monitoring.Registry, sqsServer *adapter.SQSServer) {
	if reg == nil || sqsServer == nil {
		return
	}
	if observer := reg.SQSObserver(); observer != nil {
		observer.Start(ctx, sqsDepthSourceAdapter{inner: sqsServer}, 0)
	}
}

// sqsDepthSourceAdapter bridges *adapter.SQSServer (which returns
// []adapter.SQSQueueDepth) to monitoring.SQSDepthSource (which
// expects []monitoring.SQSQueueDepth). Same shape both sides; the
// loop is a fixed-size copy.
type sqsDepthSourceAdapter struct {
	inner interface {
		SnapshotQueueDepths(context.Context) ([]adapter.SQSQueueDepth, bool)
	}
}

func (a sqsDepthSourceAdapter) SnapshotQueueDepths(ctx context.Context) ([]monitoring.SQSQueueDepth, bool) {
	if a.inner == nil {
		// Empty-but-OK: nothing to emit. Mirrors the
		// follower / nil-receiver case of the underlying source.
		return nil, true
	}
	snaps, ok := a.inner.SnapshotQueueDepths(ctx)
	if !ok {
		// Propagate skip-tick verbatim so the observer leaves
		// existing gauges alone on a transient scan failure.
		return nil, false
	}
	if snaps == nil {
		return nil, true
	}
	out := make([]monitoring.SQSQueueDepth, len(snaps))
	for i, s := range snaps {
		out[i] = monitoring.SQSQueueDepth{
			Queue:      s.Queue,
			Visible:    s.Visible,
			NotVisible: s.NotVisible,
			Delayed:    s.Delayed,
		}
	}
	return out, true
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
	shardGroups map[uint64]*kv.ShardGroup,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	distServer *adapter.DistributionServer,
	relay *adapter.RedisPubSubRelay,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	adminServer *adapter.AdminServer,
	adminGRPCOpts adminGRPCInterceptors,
	forwardDeps adminForwardServerDeps,
	confChangeInterceptor internalraftadmin.MembershipChangeInterceptor,
	encWiring encryptionWriteWiring,
) error {
	forwardLogger := slog.Default().With(slog.String("component", "admin"))
	// extraOptsCap reserves slots for the unary + stream admin interceptor
	// options appended below. Sized as a constant so the magic-number
	// linter does not complain.
	const extraOptsCap = 2
	enableMutators := encryptionMutatorsEnabled()
	encryptionCapabilityFanout := buildEncryptionCapabilityFanout(ctx, eg, runtimes, enableMutators)
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
		trx := kv.NewTransactionWithProposer(proposerForGroup(rt, shardGroups), kv.WithProposalObserver(observerForGroup(proposalObserverForGroup, rt.spec.id)))
		grpcSvc := adapter.NewGRPCServer(shardStore, coordinate)
		pb.RegisterRawKVServer(gs, grpcSvc)
		pb.RegisterTransactionalKVServer(gs, grpcSvc)
		pb.RegisterInternalServer(gs, adapter.NewInternalWithEngine(
			trx,
			rt.engine,
			coordinate.Clock(),
			relay,
			internalTimestampOptions(coordinate)...,
		))
		pb.RegisterDistributionServer(gs, distServer)
		if adminServer != nil {
			pb.RegisterAdminServer(gs, adminServer)
		}
		// full_node_id MUST be per-node-stable, not per-shard.
		// rt.spec.id is the Raft group id which every replica of
		// the same group shares; using it as full_node_id makes
		// every node return the same CapabilityReport value and
		// BootstrapEncryption's writer-batch uniqueness validation
		// (adapter/encryption_admin.go validateWriterBatchUniqueness)
		// rejects the bootstrap with "duplicate full_node_id".
		// Derive a per-node uint64 from --raftId via the canonical
		// FNV-1a hash already used by raftengine for peer ids
		// (etcd.DeriveNodeID), so every node in the cluster reports
		// a stable, distinct value. Codex r1 P1 on PR #760.
		// Stage 6B-2 mutator gate is resolved once above the
		// per-shard loop. Each shard's own engine remains the raw
		// Proposer + LeaderView for the cutover marker, while
		// ShardGroup.Proposer() supplies the wrap-aware post-cutover
		// path for normal admin entries.
		runtimeMutators, runtimeEncryptionEngine := encryptionAdminWiringForGroup(
			rt.spec.id,
			enableMutators,
			rt.engine,
		)
		registerEncryptionAdminServer(
			gs,
			etcdraftengine.DeriveNodeID(*raftId),
			*encryptionSidecarPath,
			runtimeMutators,
			runtimeEncryptionEngine,
			encryptionCapabilityFanout,
			adapter.WithEncryptionAdminLatestAppliedIndex(appliedIndexForEngine(rt.engine)),
			adapter.WithEncryptionAdminPostCutoverProposer(proposerForGroup(rt, shardGroups)),
			adapter.WithEncryptionAdminCutoverBarrier(encWiring.raftEnvelope.barrier()),
		)
		registerAdminForwardServer(gs, forwardDeps, forwardLogger)
		rt.registerGRPC(gs)
		// Stage 7c §3.1: pass the encryption-aware pre-register hook
		// (nil when encryption is not wired); raftadmin.Server invokes
		// it before AddVoter/AddLearner propose the conf-change.
		internalraftadmin.RegisterOperationalServicesWithInterceptor(ctx, gs, rt.engine, []string{"RawKV"}, confChangeInterceptor)
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

func internalTimestampOptions(coordinate kv.Coordinator) []adapter.InternalOption {
	if alloc, ok := coordinate.(kv.TimestampAllocator); ok {
		return []adapter.InternalOption{adapter.WithInternalTimestampAllocator(alloc)}
	}
	return nil
}

func prepareRedisServer(ctx context.Context, lc *net.ListenConfig, redisAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderRedis map[string]string, relay *adapter.RedisPubSubRelay, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker, redisApplyObserver *adapter.RedisApplyObserver) (*adapter.RedisServer, *adapter.DeltaCompactor, net.Listener, error) {
	redisL, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to listen on %s", redisAddr)
	}
	deltaCompactor := adapter.NewDeltaCompactor(shardStore, coordinate)
	redisServer := adapter.NewRedisServer(redisL, redisAddr, shardStore, coordinate, leaderRedis, relay,
		adapter.WithRedisActiveTimestampTracker(readTracker),
		adapter.WithRedisRequestObserver(metricsRegistry.RedisObserver()),
		adapter.WithLuaObserver(metricsRegistry.LuaObserver()),
		adapter.WithLuaFastPathObserver(metricsRegistry.LuaFastPathObserver()),
		adapter.WithRedisCompactor(deltaCompactor),
		adapter.WithLuaPoolMaxIdle(*redisLuaMaxIdleStates),
		adapter.WithRedisApplyObserver(redisApplyObserver),
	)
	// Wire the bounded Lua VM pool into Prometheus. The metrics
	// (hits/misses/drops/idle/max_idle) are read at scrape time via
	// CounterFunc / GaugeFunc, so the EVAL hot path stays
	// observability-free. A registration error degrades observability
	// only — keep running and surface via slog so the operator can
	// notice on the next dashboard load rather than seeing a crash
	// loop here.
	if err := redisServer.RegisterLuaPoolMetrics(metricsRegistry.Registerer()); err != nil {
		slog.Warn("failed to register lua pool metrics; pool counters will be invisible in Prometheus", "err", err)
	}
	return redisServer, deltaCompactor, redisL, nil
}

func runRedisServer(ctx context.Context, eg *errgroup.Group, redisServer *adapter.RedisServer, deltaCompactor *adapter.DeltaCompactor) {
	if redisServer == nil {
		return
	}
	if deltaCompactor != nil {
		eg.Go(func() error { return deltaCompactor.Run(ctx) })
	}
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
}

func newDynamoDBServer(shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderDynamo map[string]string, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker) *adapter.DynamoDBServer {
	return adapter.NewDynamoDBServer(
		nil,
		shardStore,
		coordinate,
		adapter.WithDynamoDBActiveTimestampTracker(readTracker),
		adapter.WithDynamoDBRequestObserver(metricsRegistry.DynamoDBObserver()),
		adapter.WithDynamoDBLeaderMap(leaderDynamo),
	)
}

func bindDynamoDBServer(ctx context.Context, lc *net.ListenConfig, dynamoAddr string, dynamoServer *adapter.DynamoDBServer) (net.Listener, error) {
	if dynamoServer == nil {
		return nil, errors.New("dynamodb server is not prepared")
	}
	dynamoL, err := lc.Listen(ctx, "tcp", dynamoAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", dynamoAddr)
	}
	dynamoServer.SetListener(dynamoL)
	return dynamoL, nil
}

func runDynamoDBServer(ctx context.Context, eg *errgroup.Group, dynamoServer *adapter.DynamoDBServer) {
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
}

func startPprofServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, pprofAddr string, pprofToken string) error {
	pprofServer, pprofL, bindAddr, err := preparePprofServer(ctx, lc, pprofAddr, pprofToken)
	if err != nil {
		return err
	}
	runPreparedPprofServer(ctx, eg, pprofServer, pprofL, bindAddr)
	return nil
}

func preparePprofServer(ctx context.Context, lc *net.ListenConfig, pprofAddr string, pprofToken string) (*http.Server, net.Listener, string, error) {
	pprofAddr = strings.TrimSpace(pprofAddr)
	if pprofAddr == "" {
		return nil, nil, "", nil
	}
	if _, _, err := net.SplitHostPort(pprofAddr); err != nil {
		return nil, nil, "", errors.Wrapf(err, "invalid pprofAddress %q; expected host:port", pprofAddr)
	}
	if monitoring.AddressRequiresToken(pprofAddr) && strings.TrimSpace(pprofToken) == "" {
		return nil, nil, "", errors.New("pprofToken is required when pprofAddress is not loopback")
	}
	pprofL, err := lc.Listen(ctx, "tcp", pprofAddr)
	if err != nil {
		return nil, nil, "", errors.Wrapf(err, "failed to listen on %s", pprofAddr)
	}
	pprofServer := monitoring.NewPprofServer(pprofToken)
	return pprofServer, pprofL, pprofAddr, nil
}

func runPreparedPprofServer(ctx context.Context, eg *errgroup.Group, pprofServer *http.Server, pprofL net.Listener, pprofAddr string) {
	if pprofServer == nil || pprofL == nil {
		return
	}
	eg.Go(monitoring.PprofShutdownTask(ctx, pprofServer, pprofAddr))
	eg.Go(monitoring.PprofServeTask(pprofServer, pprofL, pprofAddr))
}

func startMetricsServer(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, metricsAddr string, metricsToken string, handler http.Handler) error {
	metricsServer, metricsL, bindAddr, err := prepareMetricsServer(ctx, lc, metricsAddr, metricsToken, handler)
	if err != nil {
		return err
	}
	runPreparedMetricsServer(ctx, eg, metricsServer, metricsL, bindAddr)
	return nil
}

func prepareMetricsServer(ctx context.Context, lc *net.ListenConfig, metricsAddr string, metricsToken string, handler http.Handler) (*http.Server, net.Listener, string, error) {
	metricsAddr = strings.TrimSpace(metricsAddr)
	if metricsAddr == "" || handler == nil {
		return nil, nil, "", nil
	}
	if _, _, err := net.SplitHostPort(metricsAddr); err != nil {
		return nil, nil, "", errors.Wrapf(err, "invalid metricsAddress %q; expected host:port", metricsAddr)
	}
	if monitoring.AddressRequiresToken(metricsAddr) && strings.TrimSpace(metricsToken) == "" {
		return nil, nil, "", errors.New("metricsToken is required when metricsAddress is not loopback")
	}
	metricsL, err := lc.Listen(ctx, "tcp", metricsAddr)
	if err != nil {
		return nil, nil, "", errors.Wrapf(err, "failed to listen on %s", metricsAddr)
	}
	metricsServer := monitoring.NewMetricsServer(handler, metricsToken)
	return metricsServer, metricsL, metricsAddr, nil
}

func runPreparedMetricsServer(ctx context.Context, eg *errgroup.Group, metricsServer *http.Server, metricsL net.Listener, metricsAddr string) {
	if metricsServer == nil || metricsL == nil {
		return
	}
	eg.Go(monitoring.MetricsShutdownTask(ctx, metricsServer, metricsAddr))
	eg.Go(monitoring.MetricsServeTask(metricsServer, metricsL, metricsAddr))
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
	// EnsureCatalogSnapshot may Save through the direct (non-raft) write
	// path. When the §7.1 storage envelope is active and this load's
	// writer registration has not yet committed, that Save fails closed
	// with store.ErrWriterNotRegistered (Stage 7a-2). retryUntilRegistered
	// retries the bootstrap until the registration goroutine — armed
	// before this call in setupDistributionAndRegistration — commits and
	// the gate clears. The common cases (populated catalog → no-op Save,
	// or pre-cutover → cleartext Save) never hit the gate and return on
	// the first attempt.
	//
	// Idempotency requirement: the retry re-invokes EnsureCatalogSnapshot
	// from scratch on each ErrWriterNotRegistered, so it MUST be
	// re-entrant — on the populated-catalog path it is a version-unchanged
	// no-op Save (no mutation, no nonce), so re-running it is safe.
	if err := retryUntilRegistered(ctx, "distribution catalog bootstrap", func() error {
		_, e := distribution.EnsureCatalogSnapshot(ctx, distCatalog, engine)
		return errors.Wrap(e, "ensure catalog snapshot")
	}); err != nil {
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
	ctx                             context.Context
	lc                              *net.ListenConfig
	eg                              *errgroup.Group
	cancel                          context.CancelFunc
	runtimes                        []*raftGroupRuntime
	shardGroups                     map[uint64]*kv.ShardGroup
	shardStore                      *kv.ShardStore
	coordinate                      kv.Coordinator
	distServer                      *adapter.DistributionServer
	adminServer                     *adapter.AdminServer
	adminGRPCOpts                   adminGRPCInterceptors
	redisAddress                    string
	leaderRedis                     map[string]string
	pubsubRelay                     *adapter.RedisPubSubRelay
	readTracker                     *kv.ActiveTimestampTracker
	redisApplyObserver              *adapter.RedisApplyObserver
	encWiring                       encryptionWriteWiring
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
	adminGRPCOpts := r.adminGRPCOpts
	if r.publicKVGate != nil {
		adminGRPCOpts.unary = append(adminGRPCOpts.unary, r.publicKVGate.unaryInterceptor)
	}
	forwardDeps := adminForwardServerDeps{
		tables:  newDynamoTablesSource(r.dynamoServer),
		buckets: newBucketsSource(r.s3Server),
		roles:   r.roleStore,
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
	); err != nil {
		return r.startupFailure(err)
	}
	return nil
}

func (r *runtimeServerRunner) prepareAdminForwardServers() error {
	r.dynamoServer = newDynamoDBServer(r.shardStore, r.coordinate, r.leaderDynamo, r.metricsRegistry, r.readTracker)
	s3Server, err := newS3Server(
		r.s3Address, r.shardStore, r.coordinate, r.leaderS3, r.s3Region,
		r.s3CredsFile, r.s3PathStyleOnly, r.readTracker,
		r.metricsRegistry.S3PutAdmissionObserver(),
		r.metricsRegistry.S3BlobOffloadObserver(),
	)
	if err != nil {
		return err
	}
	r.s3Server = s3Server
	return nil
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
		r.leaderSQS, r.sqsRegion, r.sqsCredsFile,
		r.sqsPartitionResolver, r.sqsPartitionObserver,
	)
	if err != nil {
		return err
	}
	r.sqsServer = sqsServer
	r.sqsListener = sqsListener

	metricsServer, metricsListener, metricsAddr, err := prepareMetricsServer(
		r.ctx, r.lc, r.metricsAddress, r.metricsToken, r.metricsRegistry.Handler(),
	)
	if err != nil {
		return err
	}
	r.metricsServer = metricsServer
	r.metricsListener = metricsListener
	r.metricsBindAddr = metricsAddr

	pprofServer, pprofListener, pprofAddr, err := preparePprofServer(r.ctx, r.lc, r.pprofAddress, r.pprofToken)
	if err != nil {
		return err
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
	runS3Server(r.ctx, r.eg, r.s3Server)
	r.s3Listener = nil
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
		KeyBucketsPerRoute:     *keyvizKeyBucketsPerRoute,
		KeyVizLabelsEnabled:    *keyvizLabelsEnabled,
		HotKeysEnabled:         *keyvizHotKeysEnabled,
		HotKeysPerRoute:        *keyvizHotKeysPerRoute,
		HotKeysSampleRate:      *keyvizHotKeysSampleRate,
		HotKeysQueueSize:       *keyvizHotKeysQueueSize,
		HotKeysMaxKeyLen:       *keyvizHotKeysMaxKeyLen,
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
		s.RegisterRoute(r.RouteID, r.Start, r.End, r.GroupID)
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
	// Hot-key drill-down aggregator: a separate goroutine on the same
	// keyvizStep cadence. RunHotKeysAggregator is a no-op block when
	// HotKeysEnabled is false, so calling it unconditionally is safe
	// and keeps the startup wiring uniform regardless of the flag.
	eg.Go(func() error {
		keyviz.RunHotKeysAggregator(ctx, s)
		return nil
	})
}

// startKeyVizLeaderTermPublisher polls each Raft group's current term
// at the sampler's flush cadence and publishes it via
// MemSampler.SetLeaderTerm so subsequent column flushes stamp
// MatrixRow.LeaderTerm. The poll cadence is the same as the flush
// step because every flush column should observe a fresh term —
// publishing more often costs RLocks for no benefit; publishing less
// often opens a window where the column inherits a stale term from
// the previous flush.
//
// Skip the goroutine entirely when the sampler is disabled or when
// no runtimes are wired (single-process tests / cmd/client). With no
// publisher running, MatrixRow.LeaderTerm stays zero and the fan-out
// aggregator falls back to the legacy max-merge — no behavior change
// versus PR #709.
func startKeyVizLeaderTermPublisher(ctx context.Context, eg *errgroup.Group, s *keyviz.MemSampler, runtimes []*raftGroupRuntime) {
	if s == nil || len(runtimes) == 0 {
		return
	}
	eg.Go(func() error {
		step := s.Step()
		if step <= 0 {
			step = keyviz.DefaultStep
		}
		t := time.NewTicker(step)
		defer t.Stop()
		// Publish once immediately so the very first flush column sees
		// a non-zero term — without this, the column built between
		// startup and the first ticker fire would carry LeaderTerm=0
		// for every group, which the fan-out merge interprets as the
		// legacy max-merge fallback.
		publishLeaderTerms(s, runtimes)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				publishLeaderTerms(s, runtimes)
			}
		}
	})
}

// groupTermSnapshot pairs a Raft group ID with the term observed
// from its engine at one publish moment. Pulled out as its own type
// so publishLeaderTermsFromSnapshots can be tested without a real
// raftengine.Engine fake (the interface is too wide to mock cheaply
// for a unit test of this 5-line publication step).
type groupTermSnapshot struct {
	groupID uint64
	term    uint64
}

func publishLeaderTerms(s *keyviz.MemSampler, runtimes []*raftGroupRuntime) {
	snaps := make([]groupTermSnapshot, 0, len(runtimes))
	for _, rt := range runtimes {
		// snapshotEngine takes engineMu.RLock so a concurrent
		// rt.Close() (which clears rt.engine while holding the
		// write lock) cannot race the publisher's read. On
		// startup-error paths that fire cleanup before
		// joining all goroutines, this lock prevents the
		// race-detector failure and the undefined-behavior
		// nil-pointer dereference Codex round-1/round-2 P2
		// flagged on PR #720.
		engine := rt.snapshotEngine()
		if engine == nil {
			continue
		}
		snaps = append(snaps, groupTermSnapshot{groupID: rt.spec.id, term: engine.Status().Term})
	}
	publishLeaderTermsFromSnapshots(s, snaps)
}

// publishLeaderTermsFromSnapshots applies a precomputed
// (groupID, term) set to the sampler. Split out of
// publishLeaderTerms so unit tests can exercise the publish step
// without standing up a full raftengine.Engine.
func publishLeaderTermsFromSnapshots(s *keyviz.MemSampler, snaps []groupTermSnapshot) {
	if s == nil {
		return
	}
	for _, sn := range snaps {
		s.SetLeaderTerm(sn.groupID, sn.term)
	}
}
