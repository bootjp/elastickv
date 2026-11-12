package main

import (
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

func resolveValidatedRuntimeInputs(backup backupFlags, listeners listenerFlags) (runtimeConfig, raftEngineType, raftBootstrapConfig, bool, error) {
	if err := validateCommitTSFenceStartup(*commitTSFence, *raftBootstrapPeerConfigDir != ""); err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
	cfg, engineType, bootstrapCfg, bootstrap, err := resolveRuntimeInputs(listeners)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
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

func resolveRuntimeInputs(listeners listenerFlags) (runtimeConfig, raftEngineType, raftBootstrapConfig, bool, error) {
	if *raftId == "" {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, errors.New("flag --raftId is required")
	}

	engineType, err := parseRaftEngineType(*raftEngineName)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}

	cfg, err := parseRuntimeConfig(*myAddr, listeners.redisAddress, listeners.s3Address, listeners.dynamoAddress, listeners.sqsAddress, *raftGroups, *shardRanges, *raftRedisMap, *raftS3Map, *raftDynamoMap, *raftSqsMap, *sqsFifoPartitionMap)
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, err
	}
	cfg.s3BlobBackfillConfig, err = adapter.S3BlobBackfillConfigFromEnv()
	if err != nil {
		return runtimeConfig{}, "", raftBootstrapConfig{}, false, errors.WithStack(err)
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
