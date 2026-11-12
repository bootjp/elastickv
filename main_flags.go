package main

import (
	"flag"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/leaderbalance"
	"github.com/bootjp/elastickv/internal/serverwiring"
)

const (
	filesystemStartupRetryBudget   = 90 * time.Second
	filesystemStartupRetryInterval = 250 * time.Millisecond
	heartbeatTimeout               = 200 * time.Millisecond
	electionTimeout                = 2000 * time.Millisecond
	raftMetricsObserveInterval     = 5 * time.Second
	dirPerm                        = raftDirPerm

	etcdTickInterval      = 10 * time.Millisecond
	etcdHeartbeatMinTicks = 1
	etcdElectionMinTicks  = 2
	etcdMaxSizePerMsg     = 1 << 20
	etcdMaxInflightMsg    = 1024
	defaultTSOBatchSize   = 256
	defaultTSOReload      = 5 * time.Second
	defaultBackupTTL      = 30 * time.Minute
	defaultBackupMaxTTL   = time.Hour
	defaultBackupDeadline = 5 * time.Second
	defaultBackupHeadroom = 1000
	defaultBackupPageSize = 1024
	defaultBackupPinLimit = 4

	defaultFilesystemRootMode              = 0o755
	defaultFilesystemPlacementScanInterval = 30 * time.Second
	defaultFilesystemLeaseReapInterval     = 30 * time.Second
	splitMigrationCapabilityProbeTimeout   = 2 * time.Second
)

var (
	myAddr                          = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr                       = flag.String("redisAddress", "localhost:6379", "TCP host+port for redis")
	dynamoAddr                      = flag.String("dynamoAddress", "localhost:8000", "TCP host+port for DynamoDB-compatible API")
	s3Addr                          = flag.String("s3Address", "", "TCP host+port for S3-compatible API; empty to disable")
	s3Region                        = flag.String("s3Region", "us-east-1", "S3 signing region")
	s3CredsFile                     = flag.String("s3CredentialsFile", "", "Path to a JSON file containing static S3 credentials")
	s3PathStyleOnly                 = flag.Bool("s3PathStyleOnly", true, "Only accept path-style S3 requests")
	sqsAddr                         = flag.String("sqsAddress", "", "TCP host+port for SQS-compatible API; empty to disable")
	sqsPublicEndpoint               = flag.String("sqsPublicEndpoint", "", "Public HTTP(S) origin used in SQS queue URLs behind a proxy; empty derives URLs from the request")
	sqsRegion                       = flag.String("sqsRegion", "us-east-1", "SQS signing region")
	sqsCredsFile                    = flag.String("sqsCredentialsFile", "", "Path to a JSON file containing static SQS credentials")
	metricsAddr                     = flag.String("metricsAddress", "localhost:9090", "TCP host+port for Prometheus metrics")
	metricsToken                    = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	pprofAddr                       = flag.String("pprofAddress", "localhost:6060", "TCP host+port for pprof debug endpoints; empty to disable")
	pprofToken                      = flag.String("pprofToken", "", "Bearer token for pprof; required for non-loopback pprofAddress")
	filesystemMount                 = flag.String("filesystemMount", "", "FUSE mount point for the Elastickv filesystem; empty to disable")
	filesystemClientID              = flag.String("filesystemClientID", "", "Stable open-handle lease client ID for the FUSE mount; empty uses raftId")
	filesystemStrict                = flag.Bool("filesystemStrict", false, "Use the operation journal for supported FUSE mutations")
	filesystemStrictRecover         = flag.Bool("filesystemStrictRecover", false, "Fence and recover a previous strict FUSE session for the same client ID; use only after the previous mount has stopped")
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
	tsoEnabled                      = flag.Bool("tsoEnabled", false, "Commit the one-way cutover marker and issue coordinator-owned persistence timestamps through the dedicated TSO leader when group 0 is configured")
	tsoShadowEnabled                = flag.Bool("tsoShadowEnabled", false, "Serialize legacy HLC issuance through the dedicated TSO; fail closed on TSO errors and switch to TSO values after cutover")
	tsoPhaseDEnabled                = flag.Bool("tsoPhaseDEnabled", false, "Close the centralized-TSO compatibility window after every group-0 member understands the M7 marker")
	tsoBatchSize                    = flag.Int("tsoBatchSize", defaultTSOBatchSize, "Timestamp batch size used by TSO cutover and shadow validation")
	tsoModeFile                     = flag.String("tsoModeFile", "", "Path to a runtime-reloadable TSO mode file containing legacy, shadow, cutover, or phase-d")
	tsoModeReloadInterval           = flag.Duration("tsoModeReloadInterval", defaultTSOReload, "Polling interval for tsoModeFile")
	commitTSFence                   = flag.Bool("commitTSFence", false, "Reserved for replicated catalog bootstrap; activation is rejected until the startup catalog no longer writes outside Raft")
	readWatermarkAdvance            = flag.Bool("readWatermarkAdvance", false, "Raise a lagging group's commit watermark before a cross-group snapshot read so late one-phase writes cannot land below the snapshot; enable only after every node runs a binary that understands the advance entry")
	leaderBalance                   = flag.Bool("leaderBalance", false, "Enable automatic count-based Raft-group leader balancing on the default-group leader")
	leaderBalanceInterval           = flag.Duration("leaderBalanceInterval", leaderbalance.DefaultInterval, "Interval between leader-balance scheduler evaluations")
	leaderBalanceGroupCooldown      = flag.Duration("leaderBalanceGroupCooldown", leaderbalance.DefaultGroupCooldown, "Minimum time before the scheduler can move the same raft group again")
	leaderBalanceGlobalCooldown     = flag.Duration("leaderBalanceGlobalCooldown", leaderbalance.DefaultGlobalCooldown, "Minimum time between any two automatic leadership transfers")
	leaderBalanceStartupGrace       = flag.Duration("leaderBalanceStartupGrace", 0, "Grace period after acquiring default-group leadership before issuing transfers; 0 uses max(interval, global cooldown)")
	leaderBalanceImbalanceThreshold = flag.Int("leaderBalanceImbalanceThreshold", leaderbalance.DefaultImbalanceThreshold, "Minimum leader-count spread required before balancing")
	leaderBalanceMaxTargetLag       = flag.Uint64("leaderBalanceMaxTargetLag", leaderbalance.DefaultMaxTargetLag, "Maximum target lag in raft log entries for gated automatic leadership transfers; 0 requires the target to match the leader's last log index")
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
	adminTokenFile                 = flag.String("adminTokenFile", "", "Path to the read-only Admin bearer token; does not authorize split migration")
	adminInsecureNoAuth            = flag.Bool("adminInsecureNoAuth", false, "Register the Admin gRPC service without bearer-token authentication; development only")
	splitMigrationTokenFile        = flag.String("splitMigrationTokenFile", "", "Path to a dedicated cluster-shared migration controller/node token, distinct from Admin and S3 peer tokens")
	splitMigrationPeerAuthEnforced = flag.Bool("splitMigrationPeerAuthEnforced", false, "Require TLS peer Forward credentials and enable migration only after every node sends the migration token")
	grpcTLSCAFile                  = flag.String("grpcTLSCAFile", "", "CA certificates for verifying peer gRPC TLS; requires grpcTLSCertFile and grpcTLSKeyFile")
	grpcTLSCertFile                = flag.String("grpcTLSCertFile", "", "PEM certificate enabling TLS on group gRPC listeners and peer clients")
	grpcTLSKeyFile                 = flag.String("grpcTLSKeyFile", "", "PEM private key for group gRPC TLS")
	s3BlobPeerTokenFile            = flag.String("s3BlobPeerTokenFile", "", "Path to the cluster-shared bearer token for peer S3 chunkblob RPCs")
	backupDefaultTTL               = flag.Duration("backupDefaultTTL", defaultBackupTTL, "Default live-backup pin TTL")
	backupMaxTTL                   = flag.Duration("backupMaxTTL", defaultBackupMaxTTL, "Maximum live-backup pin TTL per renewal")
	backupBeginDeadline            = flag.Duration("backupBeginDeadline", defaultBackupDeadline, "Deadline for live-backup peer gates and pin fan-out")
	backupSnapshotHeadroomEntries  = flag.Uint64(
		"backupSnapshotHeadroomEntries", defaultBackupHeadroom,
		"Minimum Raft entries remaining before the next snapshot when a live backup begins",
	)
	backupScanPageSize  = flag.Int("backupScanPageSize", defaultBackupPageSize, "Count-only scan page size used by live-backup baselines")
	backupMaxActivePins = flag.Int("backupMaxActivePins", defaultBackupPinLimit, "Maximum concurrent logical live-backup pins")

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
	// AND of --encryption-enabled and a successfully loaded KEK source.
	// Setting --encryptionSidecarPath ALONE no longer enables
	// mutators; the operator must explicitly opt in to encryption
	// AND supply a KEK source. With either gate condition false,
	// registerEncryptionAdminServer omits the Proposer + LeaderView
	// options and every mutator short-circuits at the gRPC boundary
	// with FailedPrecondition before any Raft proposal is created.
	encryptionSidecarPath = flag.String("encryptionSidecarPath", "", "§5.1 keys.json path; enables read-only EncryptionAdmin capability probing. Mutating RPCs additionally require --encryption-enabled and one loaded KEK source.")

	// Stage 6B-2: cluster-wide encryption opt-in flag. The mutating
	// EncryptionAdmin RPCs (BootstrapEncryption, RotateDEK,
	// RegisterEncryptionWriter) become reachable only when this
	// flag is set AND exactly one configured KEK source loads successfully.
	// Default off; pre-Stage-6 clusters and operators who have
	// not yet committed to encryption are unaffected.
	encryptionEnabled = flag.Bool("encryption-enabled", false, "§6.5 opt-in to encryption-mutating EncryptionAdmin RPCs. Requires exactly one KEK source from --kekFile, --kekUri, or ELASTICKV_KEK_BASE64. Default off.")

	// Stage 6F: operator-requested DEK rotation at boot. The flag is
	// intentionally a request, not a guarantee: only the leader of the
	// default encryption Raft group proposes the rotation; followers
	// keep the request in memory and fire it only if they acquire
	// leadership during this process uptime.
	encryptionRotateOnStartup = flag.Bool("encryption-rotate-on-startup", false, "§6.5 request a one-shot DEK rotation after this node becomes leader of the default Raft group. Safe for rolling restarts: followers keep the request in memory and only fire if they acquire leadership during this process uptime.")

	// KEK sources. The KEK never appears in elastickv's
	// data dir; it is held externally and exercised only at process
	// boot and at DEK bootstrap/rotation per §5.1. Empty disables KEK loading; the
	// applier's ApplyBootstrap and ApplyRotation paths then return
	// ErrKEKNotConfigured at apply time, which is masked at the
	// RPC boundary by the mutator gate documented above.
	kekFile = flag.String("kekFile", "", "§5.1 KEK file path (32 raw bytes, owner-only mode). When set, the file-backed kek.Wrapper is constructed at startup and threaded into the §6.3 EncryptionApplier so ApplyBootstrap and ApplyRotation can KEK-unwrap.")
	kekURI  = flag.String("kekUri", "", "§5.1 remote KEK URI: aws-kms://<key-arn>, gcp-kms://<crypto-key-resource>, or vault-transit://<mount>/<key>. Mutually exclusive with --kekFile and ELASTICKV_KEK_BASE64.")

	// Key visualizer sampler and auto-split flags. Both groups are shared
	// with the cmd/server demo through internal/serverwiring. The sampler
	// runs entirely in-memory on each node, feeds
	// AdminServer.GetKeyVizMatrix, and is disabled unless --keyvizEnabled
	// or --autoSplit is set.
	keyVizFlags    = serverwiring.RegisterKeyVizFlags(flag.CommandLine)
	autoSplitFlags = serverwiring.RegisterAutoSplitFlags(flag.CommandLine)

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
