package main

import (
	"flag"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/serverwiring"
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

// serverFlags holds the command-line flags of the server, grouped by
// area. Each area's struct registers its own flags and lives next to the
// wiring that consumes it. Only main reads cliFlags; run() hands each
// area its own values, so helpers never read flag state.
//
// Areas not listed here are still package-level flag globals and move
// into serverFlags one area at a time (root package decomposition, M2).
type serverFlags struct {
	healthAddress   string
	filesystem      filesystemFlags
	leaderBalance   leaderBalanceFlags
	tso             tsoFlags
	snapshotOffload snapshotOffloadFlags
	adminHTTP       adminHTTPFlags
	backup          backupFlags
	// keyViz and autoSplit are shared with the cmd/server demo through
	// internal/serverwiring. The sampler runs entirely in-memory on each
	// node, feeds AdminServer.GetKeyVizMatrix, and is disabled unless
	// --keyvizEnabled or --autoSplit is set.
	keyViz    *serverwiring.KeyVizFlags
	autoSplit *serverwiring.AutoSplitFlags
}

func registerServerFlags(fs *flag.FlagSet) *serverFlags {
	f := &serverFlags{}
	fs.StringVar(&f.healthAddress, "healthAddress", "", "private HTTP health listener (empty disables); exposes /livez and /readyz")
	f.filesystem.register(fs)
	f.leaderBalance.register(fs)
	f.tso.register(fs)
	f.snapshotOffload.register(fs)
	f.adminHTTP.register(fs)
	f.backup.register(fs)
	f.keyViz = serverwiring.RegisterKeyVizFlags(fs)
	f.autoSplit = serverwiring.RegisterAutoSplitFlags(fs)
	return f
}

// cliFlags is bound to the process command line. Only main() reads it.
var cliFlags = registerServerFlags(flag.CommandLine)

var (
	myAddr                = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr             = flag.String("redisAddress", "localhost:6379", "TCP host+port for redis")
	dynamoAddr            = flag.String("dynamoAddress", "localhost:8000", "TCP host+port for DynamoDB-compatible API")
	s3Addr                = flag.String("s3Address", "", "TCP host+port for S3-compatible API; empty to disable")
	s3Region              = flag.String("s3Region", "us-east-1", "S3 signing region")
	s3CredsFile           = flag.String("s3CredentialsFile", "", "Path to a JSON file containing static S3 credentials")
	s3PathStyleOnly       = flag.Bool("s3PathStyleOnly", true, "Only accept path-style S3 requests")
	sqsAddr               = flag.String("sqsAddress", "", "TCP host+port for SQS-compatible API; empty to disable")
	sqsPublicEndpoint     = flag.String("sqsPublicEndpoint", "", "Public HTTP(S) origin used in SQS queue URLs behind a proxy; empty derives URLs from the request")
	sqsRegion             = flag.String("sqsRegion", "us-east-1", "SQS signing region")
	sqsCredsFile          = flag.String("sqsCredentialsFile", "", "Path to a JSON file containing static SQS credentials")
	metricsAddr           = flag.String("metricsAddress", "localhost:9090", "TCP host+port for Prometheus metrics")
	metricsToken          = flag.String("metricsToken", "", "Bearer token for Prometheus metrics; required for non-loopback metricsAddress")
	pprofAddr             = flag.String("pprofAddress", "localhost:6060", "TCP host+port for pprof debug endpoints; empty to disable")
	pprofToken            = flag.String("pprofToken", "", "Bearer token for pprof; required for non-loopback pprofAddress")
	raftId                = flag.String("raftId", "", "Node id used by Raft")
	raftEngineName        = flag.String("raftEngine", string(raftEngineEtcd), "Raft engine implementation (etcd)")
	raftDir               = flag.String("raftDataDir", "data/", "Raft data dir")
	redisLuaMaxIdleStates = flag.Int("redisLuaMaxIdleStates", adapter.DefaultLuaPoolMaxIdle, "Maximum number of idle *lua.LState instances retained by the Redis Lua VM pool. Each state holds ~200 KiB; lower values reduce steady-state memory at the cost of more allocations under burst, higher values absorb bursts at the cost of memory floor. Non-positive values clamp to the default.")
	raftBootstrap         = flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	raftBootstrapMembers  = flag.String("raftBootstrapMembers", "", "Comma-separated bootstrap raft members (raftID=host:port,...)")
	raftGroupPeers        = flag.String("raftGroupPeers", "", "Semicolon-separated per-group bootstrap members (groupID=raftID@host:port,...)")
	raftJoinMembers       = flag.String("raftJoinMembers", "", "Comma-separated raft members used only for transport discovery while this fresh node joins an existing single-group cluster (raftID=host:port,...); requires --raftJoinAsLearner")
	raftJoinAsLearner     = flag.Bool("raftJoinAsLearner", false, "Local node expects to join an existing cluster as a learner; if a post-apply ConfState lists this node as a voter instead, an ERROR-level alarm fires (the node keeps running -- the flag is an operator alarm, not a consensus veto). See docs/design/2026_04_26_implemented_raft_learner.md §4.5.")
	commitTSFence         = flag.Bool("commitTSFence", false, "Reserved for replicated catalog bootstrap; activation is rejected until the startup catalog no longer writes outside Raft")
	readWatermarkAdvance  = flag.Bool("readWatermarkAdvance", false, "Raise a lagging group's commit watermark before a cross-group snapshot read so late one-phase writes cannot land below the snapshot; enable only after every node runs a binary that understands the advance entry")
	raftGroups            = flag.String("raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	shardRanges           = flag.String("shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	raftRedisMap          = flag.String("raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
	raftS3Map             = flag.String("raftS3Map", "", "Map of Raft address to S3 address (raftAddr=s3Addr,...)")
	raftDynamoMap         = flag.String("raftDynamoMap", "", "Map of Raft address to DynamoDB address (raftAddr=dynamoAddr,...)")
	raftSqsMap            = flag.String("raftSqsMap", "", "Map of Raft address to SQS address (raftAddr=sqsAddr,...)")
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
)
