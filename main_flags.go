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
	nodeAuth        nodeAuthFlags
	encryption      encryptionFlags
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
	f.nodeAuth.register(fs)
	f.encryption.register(fs)
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
)
