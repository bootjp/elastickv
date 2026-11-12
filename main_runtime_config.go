package main

import (
	"flag"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

var errInvalidLiveBackupConfig = errors.New("invalid live backup configuration")

// raftFlags identify this node in the Raft cluster: its address, Raft ID,
// engine and data directory.
type raftFlags struct {
	address string
	id      string
	engine  string
	dataDir string
}

func (f *raftFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.address, "address", "localhost:50051", "TCP host+port for this node")
	fs.StringVar(&f.id, "raftId", "", "Node id used by Raft")
	fs.StringVar(&f.engine, "raftEngine", string(raftEngineEtcd), "Raft engine implementation (etcd)")
	fs.StringVar(&f.dataDir, "raftDataDir", "data/", "Raft data dir")
}

// topologyFlags describe the cluster layout: the Raft groups, the shard
// ranges they own, the Raft-to-protocol address maps used to proxy to a
// leader, and the HT-FIFO partition map.
//
// sqsFifoPartitionMap is distinct from sqsMap (which maps raftAddr=sqsAddr
// for the proxyToLeader endpoint resolution). Its grammar is
// `queue.fifo:N=group_0,...,group_{N-1}` with multiple queues separated by
// `;` (Phase 3.D §5). Leaving it empty means no FIFO queue is partitioned
// and the legacy single-partition layout applies to every queue. The §11
// PR 2 dormancy gate rejects PartitionCount > 1 on CreateQueue regardless of
// this flag, so populating it has no effect on production traffic until
// PR 5 of the rollout lands.
type topologyFlags struct {
	groups              string
	shardRanges         string
	redisMap            string
	s3Map               string
	dynamoMap           string
	sqsMap              string
	sqsFifoPartitionMap string
}

func (f *topologyFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.groups, "raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	fs.StringVar(&f.shardRanges, "shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	fs.StringVar(&f.redisMap, "raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
	fs.StringVar(&f.s3Map, "raftS3Map", "", "Map of Raft address to S3 address (raftAddr=s3Addr,...)")
	fs.StringVar(&f.dynamoMap, "raftDynamoMap", "", "Map of Raft address to DynamoDB address (raftAddr=dynamoAddr,...)")
	fs.StringVar(&f.sqsMap, "raftSqsMap", "", "Map of Raft address to SQS address (raftAddr=sqsAddr,...)")
	fs.StringVar(&f.sqsFifoPartitionMap, "sqsFifoPartitionMap", "", "HT-FIFO partition map (queue.fifo:N=group_0,...,group_{N-1};...)")
}

func resolveValidatedRuntimeInputs(flags *serverFlags) (runtimeConfig, raftEngineType, raftBootstrapConfig, bool, error) {
	if err := validateCommitTSFenceStartup(flags.consistency.commitTSFence, flags.bootstrap.peerConfigDir != ""); err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
	cfg, engineType, bootstrapCfg, bootstrap, err := resolveRuntimeInputs(flags.raft, flags.topology, flags.listeners, flags.bootstrap)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
	backup := flags.backup
	err = validateLiveBackupConfig(
		backup.defaultTTL, backup.maxTTL, backup.beginDeadline,
		backup.snapshotHeadroomEntries, backup.scanPageSize, backup.maxActivePins,
	)
	return cfg, engineType, bootstrapCfg, bootstrap, err
}

func validateLiveBackupConfig(
	defaultTTL time.Duration,
	maxTTL time.Duration,
	beginDeadline time.Duration,
	snapshotHeadroom uint64,
	scanPageSize int,
	maxActivePins int,
) error {
	const minTTL = time.Minute
	switch {
	case defaultTTL < minTTL:
		return errors.Wrapf(errInvalidLiveBackupConfig, "backupDefaultTTL must be at least %s", minTTL)
	case maxTTL < minTTL:
		return errors.Wrapf(errInvalidLiveBackupConfig, "backupMaxTTL must be at least %s", minTTL)
	case defaultTTL > maxTTL:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupDefaultTTL must not exceed backupMaxTTL")
	case beginDeadline <= 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupBeginDeadline must be positive")
	case snapshotHeadroom == 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupSnapshotHeadroomEntries must be positive")
	case scanPageSize <= 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupScanPageSize must be positive")
	case maxActivePins <= 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupMaxActivePins must be positive")
	default:
		return nil
	}
}

func resolveRuntimeInputs(raft raftFlags, topology topologyFlags, listeners listenerFlags, bootstrap raftBootstrapFlags) (runtimeConfig, raftEngineType, raftBootstrapConfig, bool, error) {
	if raft.id == "" {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, errors.New("flag --raftId is required")
	}

	engineType, err := parseRaftEngineType(raft.engine)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}

	cfg, err := parseRuntimeConfig(raft.address, listeners.redisAddress, listeners.s3Address, listeners.dynamoAddress, listeners.sqsAddress,
		topology.groups, topology.shardRanges, topology.redisMap, topology.s3Map, topology.dynamoMap, topology.sqsMap, topology.sqsFifoPartitionMap)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
	cfg.s3BlobBackfillConfig, err = adapter.S3BlobBackfillConfigFromEnv()
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, errors.WithStack(err)
	}

	bootstrapCfg, err := resolveRaftPeerConfig(
		raft.id,
		cfg.groups,
		bootstrap.bootstrapMembers,
		bootstrap.groupPeers,
		bootstrap.joinMembers,
		bootstrap.joinAsLearner,
		bootstrap.bootstrap,
	)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
	bootstrapCfg.peerConfigDir = bootstrap.peerConfigDir
	bootstrapCfg.joinAsLearner = bootstrap.joinAsLearner

	return cfg, engineType, bootstrapCfg, bootstrap.bootstrap || bootstrapCfg.anyBootstrapServers(), nil
}

type runtimeConfig struct {
	groups               []groupSpec
	defaultGroup         uint64
	engine               *distribution.Engine
	leaderRedis          map[string]string
	leaderS3             map[string]string
	leaderDynamo         map[string]string
	leaderSQS            map[string]string
	sqsFifoPartitionMap  map[string]sqsFifoQueueRouting
	s3BlobBackfillConfig adapter.S3BlobBackfillConfig
	multi                bool
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
	// An absent HTTP map selects the coordinator's gRPC follower path. A
	// self-only map cannot resolve a remote leader and rejects every request.
	if strings.TrimSpace(raftDynamoMap) == "" {
		return nil, nil
	}
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
